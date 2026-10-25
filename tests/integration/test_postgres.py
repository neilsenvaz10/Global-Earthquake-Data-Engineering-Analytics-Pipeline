"""PostgreSQL integration tests. Synthetic events stay in a freshly created test DB."""
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import psycopg
from psycopg import sql
import pytest
import requests
from sqlalchemy import MetaData, Table, func, select, text
from sqlalchemy.exc import IntegrityError, DBAPIError

from quaketrack.config import load_settings
from quaketrack.ingestion.usgs_client import USGSClient
from quaketrack.pipeline import run_backfill
from quaketrack.storage.analytics import QUERIES, query_analytics
from quaketrack.storage.database import create_database_engine, database_identity
from quaketrack.storage.loader import load_staging
from quaketrack.storage.migrations import MIGRATION_DIR, apply_migrations, require_migrations
from quaketrack.storage.repository import upsert_events
from quaketrack.storage.staging import load_state
from quaketrack.transforms.normalizer import normalize_feature

pytestmark = pytest.mark.database
START, END = "2020-01-01", "2020-01-03"
EPOCH = 1577836800000


def feature(event_id="mock-a", at=EPOCH, updated=EPOCH + 1000, mag=3.5, depth=12):
    return {"type": "Feature", "id": event_id, "properties": {
        "time": at, "updated": updated, "mag": mag, "magType": "ml", "type": "earthquake"},
        "geometry": {"type": "Point", "coordinates": [120.5, -10.25, depth]}}


@pytest.fixture(scope="session")
def isolated_engine(request):
    if not request.config.getoption("--database-tests"):
        pytest.skip("Use --database-tests to create and run against a fresh isolated database")
    settings = load_settings()
    name = "quaketrack_test_" + uuid4().hex
    assert name != settings.db_name and name.startswith("quaketrack_test_")
    # Never reuse an existing test DB, and never DROP/CREATE the development DB.
    with psycopg.connect(host=settings.db_host, port=settings.db_port, dbname=settings.db_name,
                         user=settings.db_user, password=settings.db_password, autocommit=True, connect_timeout=5) as admin:
        assert admin.execute("SELECT current_database()").fetchone()[0] == settings.db_name
        if admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone():
            pytest.fail("Generated test database already exists; refusing to reuse it")
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    engine = create_database_engine(replace(settings, db_name=name))
    assert database_identity(engine)["database"] == name
    apply_migrations(engine)
    report_path = Path("data/staging/phase3-test-db.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps({"database": name, "port": settings.db_port,
                                      "created_by_tests": True}, indent=2), encoding="utf-8")
    yield engine
    engine.dispose()


@pytest.fixture
def db(isolated_engine):
    # Only synthetic data in THIS session's freshly created database is reset.
    assert database_identity(isolated_engine)["database"].startswith("quaketrack_test_")
    with isolated_engine.begin() as connection:
        connection.exec_driver_sql("TRUNCATE pipeline_state, rejected_events, raw_archives, earthquakes, ingestion_chunks, ingestion_runs")
    return isolated_engine


def create_run(engine):
    run_id = uuid4()
    with engine.begin() as connection:
        connection.execute(text("""INSERT INTO ingestion_runs(run_id,job_type,status,window_start,window_end,staging_path)
            VALUES(:id,'backfill','running','2020-01-01','2020-01-03','mock-only')"""), {"id": run_id})
    return run_id


def stage(tmp_path, rows_per_window):
    settings = replace(load_settings(), archive_dir=tmp_path / "raw", staging_dir=tmp_path / "stage",
                       quarantine_dir=tmp_path / "quarantine", max_retries=0, minimum_magnitude=-10)
    responses = []
    for rows in rows_per_window:
        response = requests.Response()
        response.status_code = 200
        response.url = settings.api_url + "?mock-only=true"
        response._content = json.dumps({"type": "FeatureCollection", "features": rows,
                                       "metadata": {"count": len(rows)}}).encode()
        responses.append(response)
    client = USGSClient(settings, retries=0, sleep=Mock())
    client.session.get = Mock(side_effect=responses)
    end = "2020-01-02" if len(rows_per_window) == 1 else END
    run_backfill(settings, START, end, client=client)
    return settings


def test_migrations_repeat_and_checksum_guard(db, tmp_path):
    run_id = create_run(db)
    with db.begin() as connection:
        upsert_events(connection, [normalize_feature(feature())], run_id)
    assert apply_migrations(db)["applied"] == []
    require_migrations(db)
    with db.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM schema_migrations")).scalar_one() == len(list(MIGRATION_DIR.glob("*.sql")))
        assert connection.execute(text("SELECT COUNT(*) FROM earthquakes")).scalar_one() == 1
    for source in MIGRATION_DIR.glob("*.sql"):
        (tmp_path / source.name).write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    changed = tmp_path / "001_create_ingestion_runs.sql"
    changed.write_text(changed.read_text() + "\n-- changed applied file\n")
    with pytest.raises(ValueError, match="Applied migration changed"):
        apply_migrations(db, tmp_path)


def test_migration_transaction_rolls_back_pending_files(db, tmp_path):
    for source in MIGRATION_DIR.glob("*.sql"):
        (tmp_path / source.name).write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    (tmp_path / "005_bad.sql").write_text("CREATE TABLE mock_should_rollback(id INT); SELECT 1/0;")
    with pytest.raises(DBAPIError):
        apply_migrations(db, tmp_path)
    with db.connect() as connection:
        assert connection.execute(text("SELECT to_regclass('public.mock_should_rollback')")).scalar_one() is None
        assert connection.execute(text("SELECT COUNT(*) FROM schema_migrations")).scalar_one() == len(list(MIGRATION_DIR.glob("*.sql")))


@pytest.mark.parametrize("bad", [
    {"event_id": " "}, {"occurred_at": None}, {"latitude": 91}, {"longitude": -181},
    {"magnitude": float("nan")}, {"magnitude": float("inf")}, {"depth_km": float("-inf")},
    {"last_run_id": uuid4()}, {"event_type": ""}, {"source": ""}])
def test_database_constraints(db, bad):
    run_id = create_run(db)
    table = Table("earthquakes", MetaData(), autoload_with=db)
    values = dict(event_id="mock-constraint", occurred_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
                  latitude=0, longitude=0, event_type="earthquake", source="USGS", last_run_id=run_id)
    values.update(bad)
    with pytest.raises(IntegrityError):
        with db.begin() as connection:
            connection.execute(table.insert().values(**values))


def test_insert_duplicate_update_stale_and_unchanged_timestamps(db):
    first_run = create_run(db)
    record = normalize_feature(feature(updated=1000, mag=-1, depth=-2))
    with db.begin() as connection:
        actions = upsert_events(connection, [record, record], first_run)
    assert actions == {"inserted": 1, "updated": 0, "unchanged": 0, "deduplicated": 1}
    with db.connect() as connection:
        initial = dict(connection.execute(text("SELECT * FROM earthquakes")).mappings().one())
    second_run = create_run(db)
    with db.begin() as connection:
        actions = upsert_events(connection, [record], second_run)
    assert actions["unchanged"] == 1
    with db.connect() as connection:
        assert dict(connection.execute(text("SELECT * FROM earthquakes")).mappings().one()) == initial
    newer = normalize_feature(feature(updated=2000, mag=4, depth=20))
    with db.begin() as connection:
        assert upsert_events(connection, [newer], second_run)["updated"] == 1
    for stale in (record, normalize_feature(feature(updated=None, mag=9))):
        with db.begin() as connection:
            assert upsert_events(connection, [stale], second_run)["unchanged"] == 1
    with db.connect() as connection:
        actual = connection.execute(text("SELECT * FROM earthquakes")).mappings().one()
        assert actual["magnitude"] == 4 and actual["depth_km"] == 20
        assert actual["first_ingested_at"] == initial["first_ingested_at"]
        assert actual["last_run_id"] == second_run


def test_missing_update_fallback_null_measurements_and_sql_injection(db):
    run_id = create_run(db)
    raw = feature("mock'; DROP TABLE earthquakes; --", updated=None, mag=None, depth=None)
    record = normalize_feature(raw)
    with db.begin() as connection:
        assert upsert_events(connection, [record], run_id)["inserted"] == 1
        assert upsert_events(connection, [record], run_id)["unchanged"] == 1
    record["source_updated_at"] = "2020-01-01T00:00:01.000+00:00"
    with db.begin() as connection:
        assert upsert_events(connection, [record], run_id)["updated"] == 1
    with db.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM earthquakes")).scalar_one() == 1


def test_batches_and_rollback_all_batches(db):
    run_id = create_run(db)
    records = [normalize_feature(feature("mock-" + str(i))) for i in range(503)]
    with pytest.raises(DBAPIError):
        with db.begin() as connection:
            assert upsert_events(connection, records, run_id, batch_size=100)["inserted"] == 503
            connection.execute(text("SELECT 1/0"))
    with db.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM earthquakes")).scalar_one() == 0
    with db.begin() as connection:
        assert upsert_events(connection, records, run_id, batch_size=100)["inserted"] == 503


def test_concurrent_load_counts_and_uniqueness(db):
    runs = [create_run(db), create_run(db)]
    record = normalize_feature(feature())
    def load(run):
        with db.begin() as connection:
            return upsert_events(connection, [record], run)
    with ThreadPoolExecutor(max_workers=2) as pool:
        actions = list(pool.map(load, runs))
    assert sum(c["inserted"] for c in actions) == 1
    assert sum(c["unchanged"] for c in actions) == 1


def test_staging_load_metadata_quarantine_replay_and_resume(db, tmp_path, monkeypatch):
    bad = feature("mock-bad")
    bad["geometry"]["coordinates"][0] = 200
    settings = stage(tmp_path, [[feature(), feature(), bad, None]])
    first = load_staging(db, settings.staging_dir)
    assert first["counts"]["inserted"] == 1 and first["counts"]["rejected"] == 2
    assert first["counts"]["deduplicated"] == 1
    with db.connect() as connection:
        run = connection.execute(text("SELECT * FROM ingestion_runs WHERE run_id=:id"), {"id": first["run_id"]}).mappings().one()
        assert run["status"] == "success" and run["records_fetched"] == 4
        assert run["records_inserted"] == 1 and run["records_rejected"] == 2
        archives = connection.execute(text("SELECT * FROM raw_archives")).mappings().all()
        assert len(archives) == 1 and archives[0]["http_status"] == 200
        assert archives[0]["request_parameters"]["format"] == "geojson"
        assert len(archives[0]["payload_sha256"]) == 64
        rejections = connection.execute(text("SELECT raw_event_json, reason_code FROM rejected_events")).mappings().all()
        assert len(rejections) == 2 and any(r["raw_event_json"] is None for r in rejections)
    # Prove replay needs no HTTP or normalized/quarantine output files.
    checkpoint = next(iter(load_state(settings.staging_dir)["checkpoints"].values()))
    for artifact in checkpoint["artifacts"]:
        Path(artifact["path"]).unlink()  # Synthetic outputs in this test's temp dir only.
    monkeypatch.setattr(requests.Session, "get", Mock(side_effect=AssertionError("network forbidden")))
    repeated = load_staging(db, settings.staging_dir, replay=True)
    assert repeated["counts"]["inserted"] == 0 and repeated["counts"]["unchanged"] == 1
    skipped = load_staging(db, settings.staging_dir, replay=True, resume=True)
    assert skipped["chunks_skipped"] == 1 and skipped["chunks_completed"] == 0
    assert query_analytics(db, "integrity", START, END)[0] == dict(
        duplicate_ids=0, invalid_events=0, orphan_events=0, inconsistent_runs=0, incomplete_successful_chunks=0)


def test_failed_window_has_no_database_checkpoint_and_retries(db, tmp_path, monkeypatch):
    import quaketrack.storage.loader as loader
    settings = stage(tmp_path, [[feature()]])
    original = loader.upsert_events
    def fail_after_insert(connection, records, run_id, **kwargs):
        original(connection, records, run_id, **kwargs)
        connection.execute(text("SELECT 1/0"))
    monkeypatch.setattr(loader, "upsert_events", fail_after_insert)
    with pytest.raises(DBAPIError):
        load_staging(db, settings.staging_dir)
    with db.connect() as connection:
        for table in ("earthquakes", "raw_archives", "rejected_events"):
            assert connection.execute(select(func.count()).select_from(
                Table(table, MetaData(), autoload_with=connection))).scalar_one() == 0
        assert connection.execute(text("SELECT status FROM ingestion_runs")).scalar_one() == "failed"
        assert connection.execute(text("SELECT status FROM ingestion_chunks")).scalar_one() == "failed"
    monkeypatch.setattr(loader, "upsert_events", original)
    report = load_staging(db, settings.staging_dir, resume=True)
    assert report["chunks_skipped"] == 0 and report["counts"]["inserted"] == 1


def test_analytical_queries_exact_counts_and_empty_ranges(db):
    run_id = create_run(db)
    records = [normalize_feature(feature("mock-a", mag=-1, depth=-2)),
               normalize_feature(feature("mock-b", at=EPOCH + 3600000, mag=None, depth=None)),
               normalize_feature(feature("mock-c", at=EPOCH + 86400000, mag=6, depth=20)),
               normalize_feature(feature("mock-end", at=EPOCH + 2 * 86400000, mag=9, depth=30))]
    with db.begin() as connection:
        upsert_events(connection, records, run_id)
    overview = query_analytics(db, "overview", START, END)[0]
    assert overview["event_count"] == 3 and overview["known_magnitudes"] == 2
    assert overview["known_depths"] == 2 and overview["average_depth_km"] == 9
    assert overview["maximum_depth_km"] == 20
    assert [r["event_count"] for r in query_analytics(db, "daily_counts", START, END)] == [2, 1]
    assert query_analytics(db, "monthly_counts", START, END)[0]["event_count"] == 3
    buckets = {r["magnitude_range"]: r["event_count"] for r in query_analytics(db, "magnitude_distribution", START, END)}
    assert buckets == {"<0": 1, "Unknown": 1, "6+": 1}
    assert query_analytics(db, "strongest_events", START, END, limit=1)[0]["event_id"] == "mock-c"
    assert query_analytics(db, "recent_activity", START, END, limit=1)[0]["event_id"] == "mock-c"
    assert sum(r["event_count"] for r in query_analytics(db, "geographic_distribution", START, END)) == 3
    assert query_analytics(db, "geographic_distribution", START, END, min_latitude=0) == []
    trends = query_analytics(db, "magnitude_trends", START, END)
    assert trends[0]["mean_reported_magnitude"] == -1 and trends[0]["known_magnitudes"] == 1
    for query in QUERIES:
        query_analytics(db, query, START, END)
    assert query_analytics(db, "overview", "2021-01-01", "2021-01-02")[0]["event_count"] == 0


def test_backfill_database_failure_keeps_extraction_and_can_resume(db, tmp_path, monkeypatch, capsys):
    import quaketrack.cli as cli
    settings = replace(load_settings(), archive_dir=tmp_path / "raw", staging_dir=tmp_path / "stage",
                       quarantine_dir=tmp_path / "quarantine", max_retries=0)
    response = requests.Response()
    response.status_code = 200
    response.url = settings.api_url + "?mock-only=true"
    response._content = json.dumps({"type": "FeatureCollection", "features": [feature()]}).encode()
    monkeypatch.setattr(requests.Session, "get", Mock(return_value=response))
    monkeypatch.setattr(cli, "load_settings", lambda: settings)
    monkeypatch.setattr(cli, "create_database_engine", lambda unused: db)
    monkeypatch.setattr(cli, "load_staging", Mock(side_effect=RuntimeError("mock DB outage")))
    args = ["backfill", "--start", START, "--end", "2020-01-02", "--load-database"]
    assert cli.main(args) == 1
    assert len(load_state(settings.staging_dir)["checkpoints"]) == 1
    monkeypatch.setattr(cli, "load_staging", load_staging)
    monkeypatch.setattr(requests.Session, "get", Mock(side_effect=AssertionError("no HTTP during resume")))
    assert cli.main([*args, "--resume"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["windows_skipped"] == 1 and report["http_attempts"] == 0
    assert report["database_load"]["counts"]["inserted"] == 1


def test_database_resume_rejects_raw_corruption(db, tmp_path):
    settings = stage(tmp_path, [[feature()]])
    load_staging(db, settings.staging_dir)
    checkpoint = next(iter(load_state(settings.staging_dir)["checkpoints"].values()))
    Path(checkpoint["archives"][0]["path"]).write_bytes(b"mock corruption")
    with pytest.raises(ValueError, match="checksum"):
        load_staging(db, settings.staging_dir, resume=True)
    with db.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM earthquakes")).scalar_one() == 1


def test_interrupted_database_load_resumes_only_failed_window(db, tmp_path, monkeypatch):
    import quaketrack.storage.loader as loader
    settings = stage(tmp_path, [[feature()], [feature("mock-day2", at=EPOCH + 86400000)]])
    original = loader.upsert_events
    calls = 0
    def interrupted(connection, records, run_id, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise KeyboardInterrupt()
        return original(connection, records, run_id, **kwargs)
    monkeypatch.setattr(loader, "upsert_events", interrupted)
    with pytest.raises(KeyboardInterrupt):
        load_staging(db, settings.staging_dir)
    with db.connect() as connection:
        assert connection.execute(text("SELECT status FROM ingestion_runs")).scalar_one() == "interrupted"
        assert connection.execute(text("SELECT COUNT(*) FROM earthquakes")).scalar_one() == 1
    monkeypatch.setattr(loader, "upsert_events", original)
    report = load_staging(db, settings.staging_dir, resume=True)
    assert report["chunks_skipped"] == 1 and report["chunks_completed"] == 1
    assert report["counts"]["inserted"] == 1


def workflow_settings(tmp_path):
    return replace(load_settings(), archive_dir=tmp_path / "raw", staging_dir=tmp_path / "workflow",
                   quarantine_dir=tmp_path / "quarantine", max_retries=0)


def workflow_client(settings, rows=None, error=None):
    client = USGSClient(settings, retries=0, sleep=Mock())
    response = requests.Response()
    response.status_code = 200
    response.url = settings.api_url + "?mock-only=true"
    response._content = json.dumps({"type": "FeatureCollection", "features": rows or []}).encode()
    client.session.get = Mock(side_effect=error) if error else Mock(return_value=response)
    return client


def test_workflow_success_checkpoint_and_duplicate_execution(db, tmp_path):
    from quaketrack.orchestration import plan_run, execute_plan
    settings = workflow_settings(tmp_path)
    plan = plan_run(settings, db, "mock-auto", end="2020-01-01T06:10:00Z")
    assert execute_plan(settings, db, plan, client=workflow_client(settings, [feature()]))["status"] == "success"
    with db.connect() as c:
        state = c.execute(text("SELECT * FROM pipeline_state")).mappings().one()
        assert state["last_successful_window_end"] == datetime(2020,1,1,6,tzinfo=timezone.utc)
        parent = c.execute(text("SELECT * FROM ingestion_runs WHERE execution_key='mock-auto'")).mappings().one()
        assert parent["status"] == "success" and parent["quality_status"] == "passed"
        assert parent["records_inserted"] == 1
    client = workflow_client(settings, error=AssertionError("duplicate execution cannot fetch"))
    assert execute_plan(settings, db, plan, client=client)["duplicate_execution"]


def test_workflow_api_failure_then_retry_and_empty_success(db, tmp_path):
    from quaketrack.orchestration import plan_run, execute_plan
    settings = workflow_settings(tmp_path)
    plan = plan_run(settings, db, "mock-api-retry", end="2020-01-01T06:10:00Z")
    with pytest.raises(requests.Timeout):
        execute_plan(settings, db, plan, client=workflow_client(settings, error=requests.Timeout()))
    with db.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM pipeline_state")).scalar_one() == 0
        assert c.execute(text("SELECT api_failures FROM ingestion_runs WHERE execution_key='mock-api-retry'")).scalar_one() == 1
    execute_plan(settings, db, plan, client=workflow_client(settings, []))
    with db.connect() as c:
        parent = c.execute(text("SELECT * FROM ingestion_runs WHERE execution_key='mock-api-retry'")).mappings().one()
        assert parent["status"] == "success" and parent["records_fetched"] == 0
        assert c.execute(text("SELECT COUNT(*) FROM pipeline_state")).scalar_one() == 1


def test_quality_failure_does_not_advance_and_retry_reuses_data(db, tmp_path, monkeypatch):
    import quaketrack.orchestration as workflow
    settings = workflow_settings(tmp_path)
    plan = workflow.plan_run(settings, db, "mock-quality-retry", end="2020-01-01T06:10:00Z")
    original = workflow.check_quality
    monkeypatch.setattr(workflow, "check_quality", Mock(side_effect=ValueError("mock quality failure")))
    with pytest.raises(ValueError):
        workflow.execute_plan(settings, db, plan, client=workflow_client(settings, [feature()]))
    with db.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM pipeline_state")).scalar_one() == 0
        assert c.execute(text("SELECT quality_failures FROM ingestion_runs WHERE execution_key='mock-quality-retry'")).scalar_one() == 1
    monkeypatch.setattr(workflow, "check_quality", original)
    workflow.execute_plan(settings, db, plan, client=workflow_client(settings, error=AssertionError("must reuse archive")))
    with db.connect() as c:
        assert c.execute(text("SELECT records_inserted FROM ingestion_runs WHERE execution_key='mock-quality-retry'")).scalar_one() == 1


def test_workflow_database_failure_recovery(db, tmp_path, monkeypatch):
    import quaketrack.orchestration as workflow
    settings = workflow_settings(tmp_path)
    plan = workflow.plan_run(settings, db, "mock-db-retry", end="2020-01-01T06:10:00Z")
    original = workflow.load_staging
    monkeypatch.setattr(workflow, "load_staging", Mock(side_effect=RuntimeError("mock database outage")))
    with pytest.raises(RuntimeError):
        workflow.execute_plan(settings, db, plan, client=workflow_client(settings, [feature()]))
    with db.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM pipeline_state")).scalar_one() == 0
        assert c.execute(text("SELECT database_failures FROM ingestion_runs WHERE execution_key='mock-db-retry'")).scalar_one() == 1
    monkeypatch.setattr(workflow, "load_staging", original)
    workflow.execute_plan(settings, db, plan, client=workflow_client(settings, error=AssertionError("must reuse extraction")))


def test_workflow_manual_no_automatic_checkpoint_and_bad_archive(db, tmp_path):
    from quaketrack.orchestration import plan_run, execute_plan, check_quality
    settings = workflow_settings(tmp_path)
    plan = plan_run(settings, db, "mock-manual", start=START, end="2020-01-02")
    execute_plan(settings, db, plan, client=workflow_client(settings, [feature()]))
    with db.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM pipeline_state")).scalar_one() == 0
    data = json.loads(Path(plan).read_text())
    root = Path(data["directory"]) / "staging"
    checkpoint = next(iter(load_state(root)["checkpoints"].values()))
    Path(checkpoint["archives"][0]["path"]).write_bytes(b"mock corruption")
    with pytest.raises(ValueError, match="checksum"):
        check_quality(db, root, data)


def test_identical_responses_in_separate_workflows_keep_provenance(db, tmp_path):
    from quaketrack.orchestration import plan_run, execute_plan
    settings = workflow_settings(tmp_path)
    for key in ("mock-identical-first", "mock-identical-second"):
        plan = plan_run(settings, db, key, start=START, end="2020-01-02")
        assert execute_plan(settings, db, plan, client=workflow_client(settings, [feature()]))["status"] == "success"
    with db.connect() as connection:
        rows = connection.execute(text("SELECT records_inserted,records_unchanged FROM ingestion_runs WHERE execution_key IS NOT NULL ORDER BY started_at")).all()
        assert rows == [(1,0),(0,1)]


def test_controlled_failure_once_recovers_without_http(db, tmp_path, monkeypatch):
    from quaketrack.orchestration import plan_run, execute_plan
    settings = workflow_settings(tmp_path)
    monkeypatch.setenv("ALLOW_FAILURE_SIMULATION", "true")
    plan = plan_run(settings, db, "mock-failure-once", start=START, end="2020-01-02", simulate_failure_once=True)
    with pytest.raises(RuntimeError, match="Controlled quality-stage"):
        execute_plan(settings, db, plan, client=workflow_client(settings, [feature()]))
    assert execute_plan(settings, db, plan, client=workflow_client(settings, error=AssertionError("no refetch")))["status"] == "success"
    with db.connect() as connection:
        assert connection.execute(text("SELECT quality_failures FROM ingestion_runs WHERE execution_key='mock-failure-once'")).scalar_one() == 1
