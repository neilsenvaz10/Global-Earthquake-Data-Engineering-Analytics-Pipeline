"""Airflow-independent planning, execution, quality and durable success state.

The DAG calls this module in the application venv so Airflow's dependency pins are
isolated. Only plan/result file paths cross XCom; source records remain on disk.
"""
import argparse
import hashlib
import json
import logging
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import text

from .config import load_settings
from .ingestion.windowing import parse_utc
from .logging_config import configure_logging
from .monitoring.run_metrics import empty_counts, reconcile
from .pipeline import run_backfill
from .storage.analytics import query_analytics
from .storage.database import create_database_engine
from .storage.loader import COUNT_COLUMNS, failure_detail, load_staging
from .storage.migrations import require_migrations
from .storage.replay import prepare_checkpoint
from .storage.staging import atomic_json, load_state

PIPELINE_LOCK = 81743003
log = logging.getLogger(__name__)


def now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0)


def incremental_window(target, checkpoint=None, *, coverage_start=None, overlap_hours=72,
                       bootstrap_hours=6, step_hours=6):
    if any(not math.isfinite(n) or n <= 0 or n > 168 for n in (overlap_hours, bootstrap_hours, step_hours)):
        raise ValueError("Overlap, bootstrap and catch-up step must be in (0,168] hours")
    target = parse_utc(target)
    if checkpoint is None:
        return target - timedelta(hours=bootstrap_hours), target
    checkpoint = parse_utc(checkpoint)
    if checkpoint > target:
        raise ValueError("Checkpoint is ahead of the requested ingestion horizon")
    end = min(target, checkpoint + timedelta(hours=step_hours))
    start = max(parse_utc(coverage_start), checkpoint - timedelta(hours=overlap_hours))
    if start >= end:
        raise ValueError("No nonempty incremental range available")
    return start, end


def plan_run(settings, engine, execution_key, *, end=None, start=None, minimum_magnitude=None,
             simulate_failure_once=False):
    require_migrations(engine)
    magnitude = settings.minimum_magnitude if minimum_magnitude is None else float(minimum_magnitude)
    if not math.isfinite(magnitude) or not execution_key or len(execution_key) > 500:
        raise ValueError("Invalid magnitude or execution key")
    manual = start is not None
    if manual and end is None:
        raise ValueError("Explicit start requires explicit end")
    if simulate_failure_once and (not manual or os.getenv("ALLOW_FAILURE_SIMULATION", "false").lower() != "true"):
        raise ValueError("Failure simulation requires an explicit manual window and ALLOW_FAILURE_SIMULATION=true")
    scope = hashlib.sha256(json.dumps([settings.api_url, magnitude, settings.event_type]).encode()).hexdigest()[:16]
    name = "quaketrack_earthquake_ingestion:" + scope
    directory = settings.staging_dir / "workflows" / hashlib.sha256(execution_key.encode()).hexdigest()
    plan_path = directory / "plan.json"
    if plan_path.exists():
        existing = json.loads(plan_path.read_text(encoding="utf-8"))
        if existing["execution_key"] != execution_key:
            raise ValueError("Execution plan identity mismatch")
        return plan_path
    delay = float(os.getenv("INGESTION_DELAY_MINUTES", "10"))
    if not math.isfinite(delay) or not 0 <= delay <= 1440:
        raise ValueError("Ingestion delay must be 0..1440 minutes")
    target = parse_utc(end) if end else now_utc()
    if not manual:
        target -= timedelta(minutes=delay)
    if target > now_utc():
        raise ValueError("Ingestion end cannot be in the future")
    with engine.connect() as connection:
        state = connection.execute(text("SELECT * FROM pipeline_state WHERE pipeline_name=:name"), {"name": name}).mappings().first()
    if manual:
        left, right = parse_utc(start), target
        if left >= right or right - left > timedelta(hours=24):
            raise ValueError("Manual verification windows must be positive and at most 24 hours")
    else:
        left, right = incremental_window(target, state["last_successful_window_end"] if state else None,
            coverage_start=state["coverage_start"] if state else None,
            overlap_hours=float(os.getenv("INCREMENTAL_LOOKBACK_HOURS", "72")),
            bootstrap_hours=float(os.getenv("INCREMENTAL_BOOTSTRAP_HOURS", "6")),
            step_hours=float(os.getenv("INCREMENTAL_STEP_HOURS", "6")))
    plan = {"execution_key": execution_key, "run_id": str(uuid5(NAMESPACE_URL, execution_key)),
            "pipeline_name": name, "start": left.isoformat(), "end": right.isoformat(),
            "manual": manual, "minimum_magnitude": magnitude, "event_type": settings.event_type,
            "directory": str(directory.resolve()), "simulate_failure_once": simulate_failure_once}
    atomic_json(plan_path, plan)
    return plan_path


def _counts_from_database(engine, directory):
    # Earliest successful observation per window preserves insert/update counts
    # when a later retry skips chunks or replays an uncertain completed commit.
    with engine.connect() as connection:
        rows = connection.execute(text("""SELECT DISTINCT ON (c.checkpoint_key, c.input_sha256) c.*
            FROM ingestion_chunks c JOIN ingestion_runs r ON r.run_id=c.run_id
            WHERE r.staging_path=:path AND c.status='success'
            ORDER BY c.checkpoint_key,c.input_sha256,c.completed_at,c.chunk_id"""), {"path": str(directory)}).mappings().all()
    counts = empty_counts()
    for row in rows:
        for name, column in COUNT_COLUMNS.items():
            counts[name] += row[column]
    counts["eligible"] = counts["unique_records"] + counts["deduplicated"]
    reconcile(counts)
    return counts, rows


def check_quality(engine, directory, plan):
    root = Path(directory)
    state = load_state(root)
    expected = empty_counts()
    for checkpoint in state["checkpoints"].values():
        records, _, _, _ = prepare_checkpoint(checkpoint, replay=True)
        for name in expected:
            expected[name] += checkpoint["counts"][name]
        with engine.connect() as connection:
            for record in records:
                stored = connection.execute(text("SELECT source_updated_at FROM earthquakes WHERE event_id=:id"), {"id": record["event_id"]}).first()
                if stored is None:
                    raise ValueError("Quality: an accepted event is missing from PostgreSQL")
                if record["source_updated_at"] and (stored[0] is None or stored[0] < parse_utc(record["source_updated_at"])):
                    raise ValueError("Quality: stored event revision regressed")
    counts, chunks = _counts_from_database(engine, root.resolve())
    for name in ("fetched", "accepted", "rejected", "filtered", "deduplicated", "unique_records"):
        if counts[name] != expected[name]:
            raise ValueError("Quality: extraction and committed database counts differ: " + name)
    if len(chunks) != len(state["checkpoints"]):
        raise ValueError("Quality: not all extraction windows have a DB success checkpoint")
    checks = query_analytics(engine, "integrity", plan["start"], plan["end"])[0]
    if any(checks.values()):
        raise ValueError("Quality: database integrity checks failed")
    if not chunks:
        raise ValueError("Quality: no successful database window, including empty source windows")
    latest_commit = max(chunk["completed_at"] for chunk in chunks)
    if now_utc() - latest_commit > timedelta(hours=24):
        raise ValueError("Quality: committed load is older than 24 hours; refresh the source")
    return {"status": "passed", "counts": counts, "checks": checks, "chunks": len(chunks),
            "coverage_lag_seconds": max(0, (now_utc() - parse_utc(plan["end"])).total_seconds()),
            "historical_verification": plan["manual"]}


def execute_plan(settings, engine, plan_path, *, client=None):
    plan = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    require_migrations(engine)
    root = Path(plan["directory"]) / "staging"
    run_id = plan["run_id"]
    stage = "extract"
    result_path = Path(plan["directory"]) / "result.json"
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as lock:
        acquired = lock.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": PIPELINE_LOCK}).scalar_one()
        if not acquired:
            raise RuntimeError("Another ingestion workflow is active; retry later")
        try:
            with engine.begin() as connection:
                existing = connection.execute(text("SELECT status FROM ingestion_runs WHERE run_id=:id"), {"id": run_id}).first()
                if existing and existing[0] == "success":
                    return {"run_id": run_id, "status": "success", "duplicate_execution": True, "result_path": str(result_path)}
                connection.execute(text("""INSERT INTO ingestion_runs
                    (run_id,job_type,status,window_start,window_end,staging_path,pipeline_name,execution_key,stage,quality_status)
                    VALUES (:id,'incremental','running',:start,:end,:path,:name,:key,'extract','pending')
                    ON CONFLICT (run_id) DO UPDATE SET status='running',finished_at=NULL,stage='extract',quality_status='pending',error_message=NULL"""),
                    {"id": run_id, "start": parse_utc(plan["start"]), "end": parse_utc(plan["end"]),
                     "path": str(root), "name": plan["pipeline_name"], "key": plan["execution_key"]})
            try:
                report = run_backfill(settings, plan["start"], plan["end"], minimum_magnitude=plan["minimum_magnitude"],
                                      output_dir=root, resume=True, client=client)
                stage = "load"
                _set_stage(engine, run_id, stage)
                loaded = load_staging(engine, root, resume=True, scope_to_staging=True)
                stage = "quality"
                _set_stage(engine, run_id, stage)
                quality = check_quality(engine, root, plan)
                if plan["simulate_failure_once"]:
                    marker = Path(plan["directory"]) / "controlled_failure.marker"
                    if not marker.exists():
                        marker.write_text("Explicit manual recovery verification; no data was modified.\n", encoding="utf-8")
                        raise RuntimeError("Controlled quality-stage failure; retry this execution")
                counts = quality["counts"]
                with engine.begin() as connection:
                    if not plan["manual"]:
                        state = connection.execute(text("SELECT * FROM pipeline_state WHERE pipeline_name=:name FOR UPDATE"), {"name": plan["pipeline_name"]}).mappings().first()
                        if state and parse_utc(plan["start"]) > state["last_successful_window_end"]:
                            raise ValueError("Checkpoint cannot advance across an unprocessed gap")
                        connection.execute(text("""INSERT INTO pipeline_state
                            (pipeline_name,coverage_start,last_successful_window_end,last_run_id)
                            VALUES(:name,:start,:end,:id) ON CONFLICT(pipeline_name) DO UPDATE SET
                            last_successful_window_end=GREATEST(pipeline_state.last_successful_window_end,EXCLUDED.last_successful_window_end),
                            updated_at=clock_timestamp(),last_run_id=EXCLUDED.last_run_id"""),
                            {"name": plan["pipeline_name"], "start": parse_utc(plan["start"]), "end": parse_utc(plan["end"]), "id": run_id})
                    # Child loader counts have already committed. Wrapper success and
                    # automatic coverage advance happen atomically AFTER quality.
                    values = {column: counts[name] for name, column in COUNT_COLUMNS.items()}
                    assignments = ",".join(column + "=:" + column for column in values)  # fixed internal allowlist
                    connection.execute(text("UPDATE ingestion_runs SET " + assignments + ",status='success',quality_status='passed',stage='complete',finished_at=clock_timestamp(),child_run_id=:child,chunks_completed=:chunks,source_response_count=:responses WHERE run_id=:id"),
                        {**values, "id": run_id, "child": loaded["run_id"], "chunks": quality["chunks"],
                         "responses": sum(len(c["archives"]) for c in load_state(root)["checkpoints"].values())})
                result = {"run_id": run_id, "status": "success", "counts": counts, "quality": quality,
                          "start": plan["start"], "end": plan["end"], "manual": plan["manual"],
                          "extraction_http_attempts_this_try": report["http_attempts"], "child_run_id": loaded["run_id"]}
                atomic_json(result_path, result)
                log.info("%s", json.dumps(result))
                return {"run_id": run_id, "status": "success", "result_path": str(result_path)}
            except BaseException as error:
                column = {"extract": "api_failures", "load": "database_failures", "quality": "quality_failures"}[stage]
                try:
                    with engine.begin() as connection:
                        connection.execute(text("UPDATE ingestion_runs SET status='failed',quality_status=:quality,stage=:stage,finished_at=clock_timestamp(),error_message=:error," + column + "=" + column + "+1 WHERE run_id=:id"),
                            {"id": run_id, "stage": stage, "quality": "failed" if stage == "quality" else "pending",
                             "error": failure_detail(error)})
                except Exception:
                    log.error("Could not persist failure status for workflow %s", run_id)
                atomic_json(Path(plan["directory"]) / "failure.json", {"run_id": run_id, "stage": stage, "error_type": type(error).__name__})
                raise
        finally:
            lock.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": PIPELINE_LOCK})


def _set_stage(engine, run_id, stage):
    with engine.begin() as connection:
        connection.execute(text("UPDATE ingestion_runs SET stage=:stage WHERE run_id=:id"), {"id": run_id, "stage": stage})


def main(argv=None):
    parser = argparse.ArgumentParser(description="Airflow application subprocess interface")
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--execution-key", required=True)
    plan.add_argument("--start")
    plan.add_argument("--end")
    plan.add_argument("--min-magnitude", type=float)
    plan.add_argument("--simulate-failure-once", action="store_true")
    run = commands.add_parser("run")
    run.add_argument("--plan", required=True)
    failure = commands.add_parser("fail")
    failure.add_argument("--execution-key", required=True)
    args = parser.parse_args(argv)
    settings = load_settings()
    configure_logging(settings.log_level)
    engine = create_database_engine(settings)
    try:
        if args.command == "plan":
            path = plan_run(settings, engine, args.execution_key, start=args.start, end=args.end,
                            minimum_magnitude=args.min_magnitude, simulate_failure_once=args.simulate_failure_once)
            print(json.dumps({"plan_path": str(path)}))
        elif args.command == "run":
            print(json.dumps(execute_plan(settings, engine, args.plan)))
        else:
            with engine.begin() as connection:
                connection.execute(text("UPDATE ingestion_runs SET status='failed',finished_at=clock_timestamp(),error_message='Airflow task failed or timed out' WHERE execution_key=:key AND status='running'"), {"key": args.execution_key})
            print(json.dumps({"status": "failure_recorded"}))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
