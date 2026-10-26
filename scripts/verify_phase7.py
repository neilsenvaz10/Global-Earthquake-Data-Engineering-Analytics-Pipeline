"""Read-only post-backfill evidence: preservation, coverage, quality, Spark and UI.

Requires this checkout's preflight snapshot and completed approved run artifacts.
No ingestion is started; permission probes use WHERE false and cannot alter rows.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os

import requests
from sqlalchemy.exc import DBAPIError
from streamlit.testing.v1 import AppTest

from quaketrack.config import ROOT, load_settings
from quaketrack.dashboard.app import PAGES, cached, get_data
from quaketrack.dashboard.data import DashboardData, Filters, dashboard_engine, utc_label
from quaketrack.dashboard.presentation import complete_days
from quaketrack.dashboard.security import load_dashboard_settings
from quaketrack.medallion import GOLD_VIEWS
from quaketrack.spark_demo import compare_results
from quaketrack.storage.analytics import query_analytics
from quaketrack.storage.database import create_database_engine
from quaketrack.storage.migrations import require_migrations

OUT = ROOT / "data/staging/phase7"


def read_report(name):
    lines = (OUT / (name + ".log")).read_text(encoding="utf-8-sig").splitlines()
    first = next(index for index, line in enumerate(lines) if line.startswith("{"))
    return json.loads("\n".join(lines[first:]))


def main():
    engine = dashboard_engine(load_settings())
    try:
        require_migrations(engine)
        data = DashboardData(engine)
        before = json.loads((OUT / "before.json").read_text(encoding="utf-8"))
        preflight = json.loads((OUT / "preflight.json").read_text(encoding="utf-8"))
        assert preflight["approval_status"].startswith("approved by user")
        original = {row["event_id"]: row for row in before["events"]}
        current = json.loads(json.dumps(data.fetch("SELECT * FROM earthquakes ORDER BY event_id"), default=str))
        by_id = {row["event_id"]: row for row in current}
        assert len(current) == len(by_id) and all(by_id[key] == row for key, row in original.items())
        current_state = json.loads(json.dumps(data.fetch("SELECT * FROM pipeline_state ORDER BY pipeline_name"), default=str))
        assert current_state == before["pipeline_state"], "Automatic checkpoints changed"
        archives = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in (ROOT / "data/raw").rglob("*") if path.is_file()}
        assert all(archives.get(path) == sha for path, sha in before["archives"].items())
        full, repeat = read_report("backfill"), read_report("repeat")
        load = full["database_load"]
        assert full["status"] == load["status"] == "success"
        assert load["counts"]["updated"] == 0 and len(current) == len(original) + load["counts"]["inserted"]
        assert repeat["database_load"]["counts"]["inserted"] == repeat["database_load"]["counts"]["updated"] == 0
        assert repeat["database_load"]["counts"]["unchanged"] == repeat["database_load"]["counts"]["fetched"]
        chunks = data.fetch("""SELECT window_start,window_end,status,records_fetched FROM ingestion_chunks
            WHERE run_id=:run ORDER BY window_start""", {"run": load["run_id"]})
        start, end = datetime.fromisoformat(full["start"]), datetime.fromisoformat(full["end"])
        assert len(chunks) == 90 and sum(row["records_fetched"] for row in chunks) == load["counts"]["fetched"]
        for index, row in enumerate(chunks):
            assert row["status"] == "success" and row["window_start"] == start + timedelta(days=index)
            assert row["window_end"] == start + timedelta(days=index+1)
        integrity = query_analytics(engine, "integrity", start, end)
        assert all(value == 0 for row in integrity for value in row.values())
        quality = json.loads((OUT / "demo/quality-report.json").read_text(encoding="utf-8"))
        assert quality["quality"]["total_records_checked"] == len(current) and quality["quality"]["invalid_records"] == 0
        assert quality["layers"]["silver_source_matches"] == len(current)
        spark_path = max((OUT / "spark").glob("run-*/spark-report.json"), key=lambda path: path.stat().st_mtime)
        spark = json.loads(spark_path.read_text(encoding="utf-8"))
        sha = hashlib.sha256((OUT / "demo/silver-snapshot.json").read_bytes()).hexdigest()
        assert spark["input_sha256"] == quality["snapshot_sha256"] == sha
        actual_gold = {name: json.loads(json.dumps(data.fetch(f"SELECT * FROM {name} ORDER BY 1"), default=str)) for name in GOLD_VIEWS}
        assert spark["postgres_parity"] and compare_results(spark["results"], actual_gold)
        reader_settings = load_dashboard_settings()
        reader = dashboard_engine(reader_settings)
        try:
            windows = DashboardData(reader).coverage_windows(Filters(start,end))
            assert complete_days(windows,Filters(start,end)).get("4") == 90
            assert DashboardData(reader).coverage()["total"] == len(current)
        finally:
            reader.dispose()
        probe = create_database_engine(reader_settings)
        denied = []
        try:
            with probe.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
                connection.exec_driver_sql("SET default_transaction_read_only=off")
                for operation, statement in (("INSERT", "INSERT INTO earthquakes(event_id) SELECT 'phase7-probe' WHERE false"),
                    ("UPDATE", "UPDATE earthquakes SET magnitude=magnitude WHERE false"), ("DELETE", "DELETE FROM earthquakes WHERE false")):
                    try:
                        connection.exec_driver_sql(statement)
                    except DBAPIError as error:
                        assert error.orig.sqlstate == "42501"
                        denied.append(operation)
                    else:
                        raise AssertionError("Reader unexpectedly accepted " + operation)
        finally:
            probe.dispose()
        base = "http://localhost:18080"
        health = requests.get(base + "/api/v2/monitor/health", timeout=10)
        health.raise_for_status()
        assert all(health.json()[key]["status"] == "healthy" for key in ("metadatabase","scheduler","dag_processor"))
        auth = requests.post(base + "/auth/token", json={"username":"admin","password":os.environ["AIRFLOW_ADMIN_PASSWORD"]},timeout=10)
        auth.raise_for_status()
        dag = requests.get(base + "/api/v2/dags/quaketrack_earthquake_ingestion",headers={"Authorization":"Bearer " + auth.json()["access_token"]},timeout=10)
        dag.raise_for_status()
        assert dag.json()["is_paused"] is True
        coverage = data.coverage()
        checked = datetime.now(timezone.utc).isoformat()
        summary = dict(checked_at=checked,dataset=dict(total=coverage["total"],last_ingested_at=utc_label(coverage["last_ingested_at"]),
            latest_source_update=utc_label(coverage["latest_source_update"])),raw_integrity=f"{len(archives)} files checked; {len(before['archives'])} originals preserved",
            contract_compliance=quality["quality"]["contract_compliance_percentage"],spark_parity="passed over equivalent snapshot")
        (OUT / "validation").mkdir(parents=True,exist_ok=True)
        (OUT / "validation/summary.json").write_text(json.dumps(summary,indent=2),encoding="utf-8")
        cached.clear()
        get_data.clear()
        app = AppTest.from_file(str(ROOT / "dashboard/app.py"),default_timeout=45).run()
        pages = []
        for page in PAGES:
            app.radio(key="page").set_value(page).run()
            assert not app.exception and not app.error, page
            pages.append(dict(page=page,charts=len(app.get("plotly_chart")),tables=len(app.dataframe)))
        app.radio(key="page").set_value("Explorer").run()
        app.text_input(key="location").set_value("Japan").run()
        matches = data.overview(Filters.dates(coverage["first_event"].date(),coverage["last_event"].date(),location="Japan"))["total"]
        assert any(f"{matches:,} matching events" in value.value for value in app.caption)
        assert len(app.get("download_button")) == 1
        app.button(key="reset_filters").click().run()
        assert app.text_input(key="location").value == "" and not app.exception
        report = dict(checked_at=checked,coverage=coverage,backfill_counts=load["counts"],repeat_counts=repeat["database_load"]["counts"],
            successful_daily_windows=len(chunks),preserved_original_events=len(original),preserved_original_files=len(before["archives"]),
            raw_files=len(archives),integrity=integrity,quality=quality["quality"],spark_parity=True,spark_rows=spark["input_rows"],
            spark_elapsed_seconds=spark["elapsed_seconds"],gold=actual_gold,dag_is_paused=True,automatic_checkpoints_unchanged=True,
            denied_reader_operations=denied,pages=pages,japan_matches=matches)
        (OUT / "verification.json").write_text(json.dumps(report,default=str,indent=2),encoding="utf-8")
        print(json.dumps({key:value for key,value in report.items() if key != "gold"},default=str,indent=2))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
