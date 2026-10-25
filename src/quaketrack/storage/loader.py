"""Explicit Phase 2 -> PostgreSQL loading, with distinct durable DB checkpoints."""
from pathlib import Path
from datetime import datetime
import logging
from uuid import UUID, uuid4

from sqlalchemy import JSON, func, select, text, update

from ..ingestion.windowing import parse_utc
from ..monitoring.run_metrics import empty_counts, reconcile
from .migrations import require_migrations
from .replay import prepare_checkpoint, window_bounds
from .repository import WRITER_LOCK, reflect_tables, upsert_events
from .staging import load_state, staging_lock

COUNT_COLUMNS = {"fetched": "records_fetched", "accepted": "records_accepted",
                 "rejected": "records_rejected", "filtered": "records_filtered",
                 "deduplicated": "records_deduplicated", "unique_records": "records_unique",
                 "inserted": "records_inserted", "updated": "records_updated", "unchanged": "records_unchanged"}


def _count_columns(counts):
    return {column: counts[name] for name, column in COUNT_COLUMNS.items()}


def failure_detail(error):
    """Keep PostgreSQL's primary error/constraint without SQL or bound values."""
    original = getattr(error, "orig", None)
    diagnostic = getattr(original, "diag", None)
    message = getattr(diagnostic, "message_primary", None)
    constraint = getattr(diagnostic, "constraint_name", None)
    if message:
        return f"{type(error).__name__}: {message}" + (f" ({constraint})" if constraint else "")
    return f"{type(error).__name__}: {error}" if isinstance(error, (ValueError, KeyError)) else type(error).__name__


def load_staging(engine, staging_dir, *, replay=False, resume=False, batch_size=500, scope_to_staging=False):
    """Each successful window is one transaction, including events and provenance.

    A fresh run row records every load invocation, including repeated loads. Resume
    consults only successful PostgreSQL chunks, never the extraction checkpoint.
    """
    if not 1 <= batch_size <= 1000:
        raise ValueError("batch_size must be 1..1000")
    require_migrations(engine)
    root = Path(staging_dir).resolve()
    if not (root / "state.json").is_file():
        raise ValueError("No Phase 2 state.json in staging directory")
    tables = reflect_tables(engine)
    runs, chunks = tables["ingestion_runs"], tables["ingestion_chunks"]
    raw_table, rejected_table = tables["raw_archives"], tables["rejected_events"]
    run_id = uuid4()
    counts = empty_counts()
    report = {"run_id": str(run_id), "mode": "replay" if replay else "staged",
              "counts": counts, "chunks_completed": 0, "chunks_skipped": 0, "source_response_count": 0}
    with staging_lock(root):
        state = load_state(root)
        checkpoints = state["checkpoints"]
        if not checkpoints:
            raise ValueError("Staging has no completed extraction windows")
        bounds = {key: window_bounds(value) for key, value in checkpoints.items()}
        with engine.begin() as connection:
            connection.execute(runs.insert().values(run_id=run_id, job_type="replay" if replay else "backfill",
                status="running", staging_path=str(root), window_start=min(b[0] for b in bounds.values()),
                window_end=max(b[1] for b in bounds.values())))
        current_chunk = None
        try:
            for key, checkpoint in sorted(checkpoints.items(), key=lambda item: (bounds[item[0]][0], item[0])):
                start, end = bounds[key]
                current_chunk = {"chunk_id": uuid4(), "run_id": run_id, "checkpoint_key": key,
                    "input_sha256": "0" * 64, "source_run_id": UUID(checkpoint["run_id"]),
                    "window_start": start, "window_end": end, "status": "running", "attempt_count": 1}
                records, rejected, archives, digest = prepare_checkpoint(checkpoint, replay=replay)
                current_chunk["input_sha256"] = digest
                current_chunk["archive_path"] = archives[0]["reference"]["path"]
                current_chunk["payload_sha256"] = archives[0]["reference"]["sha256"]
                skipped = False
                with engine.begin() as connection:
                    connection.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": WRITER_LOCK})
                    prior_query = select(func.count()).select_from(chunks).where(
                        chunks.c.checkpoint_key == key, chunks.c.input_sha256 == digest,
                        chunks.c.status == "success")
                    if scope_to_staging:
                        prior_query = prior_query.where(chunks.c.run_id.in_(
                            select(runs.c.run_id).where(runs.c.staging_path == str(root))))
                    prior = connection.execute(prior_query).scalar_one()
                    current_chunk["attempt_count"] = connection.execute(select(func.count()).select_from(chunks).where(
                        chunks.c.checkpoint_key == key)).scalar_one() + 1
                    if resume and prior:
                        skipped = True
                        connection.execute(update(runs).where(runs.c.run_id == run_id).values(
                            chunks_skipped=report["chunks_skipped"] + 1))
                    else:
                        connection.execute(chunks.insert().values(**current_chunk))
                        archive_ids = {}
                        for archive in archives:
                            reference, metadata, features = archive["reference"], archive["metadata"], archive["features"]
                            archive_id = uuid4()
                            archive_ids[reference["path"]] = archive_id
                            connection.execute(raw_table.insert().values(archive_id=archive_id, run_id=run_id,
                                chunk_id=current_chunk["chunk_id"], source_run_id=UUID(metadata["run_id"]),
                                archive_path=reference["path"], metadata_path=reference["metadata_path"],
                                payload_sha256=reference["sha256"], source=metadata["source"],
                                request_url=metadata["request_url"], request_parameters=metadata["parameters"],
                                fetched_at=datetime.fromisoformat(metadata["fetched_at"].replace("Z", "+00:00")),
                                http_status=metadata["status_code"], attempt=metadata["attempt"],
                                record_count=None if features is None else len(features)))
                        for rejection in rejected:
                            source_id = rejection.get("source_event_id")
                            connection.execute(rejected_table.insert().values(rejection_id=uuid4(), run_id=run_id,
                                chunk_id=current_chunk["chunk_id"], archive_id=archive_ids[rejection["archive"]["path"]],
                                source_event_id=source_id if isinstance(source_id, str) else None,
                                reason_code=rejection["reason_code"], reason_detail=rejection["reason"],
                                raw_event_json=JSON.NULL if rejection["raw_event"] is None else rejection["raw_event"],
                                rejected_at=datetime.fromisoformat(rejection["ingested_at"].replace("Z", "+00:00"))))
                        actions = upsert_events(connection, records, run_id, batch_size=batch_size, table=tables["earthquakes"])
                        if actions["deduplicated"]:
                            raise ValueError("Staged normalized file unexpectedly contains duplicate IDs")
                        batch_counts = {**checkpoint["counts"], **{k: actions[k] for k in ("inserted", "updated", "unchanged")}}
                        reconcile(batch_counts)
                        connection.execute(update(chunks).where(chunks.c.chunk_id == current_chunk["chunk_id"]).values(
                            status="success", completed_at=func.clock_timestamp(), **_count_columns(batch_counts)))
                        total = {name: counts[name] + batch_counts[name] for name in counts}
                        connection.execute(update(runs).where(runs.c.run_id == run_id).values(
                            **_count_columns(total), chunks_completed=report["chunks_completed"] + 1,
                            source_response_count=report["source_response_count"] + len(archives)))
                # Update the returned counters only after the database commit succeeds.
                if skipped:
                    report["chunks_skipped"] += 1
                else:
                    counts.update(total)
                    report["chunks_completed"] += 1
                    report["source_response_count"] += len(archives)
                current_chunk = None
            with engine.begin() as connection:
                connection.execute(update(runs).where(runs.c.run_id == run_id).values(
                    status="success", finished_at=func.clock_timestamp()))
            report["status"] = "success"
            return report
        except BaseException as error:
            try:
                with engine.begin() as connection:
                    if current_chunk is not None:
                        connection.execute(chunks.insert().values(**{**current_chunk, "status": "failed",
                            "error_message": failure_detail(error), "completed_at": func.clock_timestamp()}))
                    connection.execute(update(runs).where(runs.c.run_id == run_id).values(
                        status="interrupted" if isinstance(error, (KeyboardInterrupt, SystemExit)) else "failed",
                        finished_at=func.clock_timestamp(), error_message=failure_detail(error)))
            except Exception:
                logging.getLogger(__name__).error("Could not persist failure status for database run %s", run_id)
            raise
