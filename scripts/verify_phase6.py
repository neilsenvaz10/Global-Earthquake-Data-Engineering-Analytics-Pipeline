"""Read-only final preservation, role security, Gold/Spark and scheduler checks.

The three DML probes have WHERE false, so even a misconfigured role cannot change
data. Read-only mode is explicitly disabled for these probes to test real grants.
"""
import hashlib
import json
import os
from pathlib import Path

import requests
from sqlalchemy.exc import DBAPIError

from quaketrack.config import ROOT, load_settings
from quaketrack.dashboard.data import DashboardData, dashboard_engine
from quaketrack.dashboard.security import load_dashboard_settings
from quaketrack.medallion import GOLD_VIEWS
from quaketrack.spark_demo import compare_results
from quaketrack.storage.database import create_database_engine
from quaketrack.storage.migrations import require_migrations

OUT = ROOT / "data/staging/phase6"


def main():
    reader_settings = load_dashboard_settings()
    engine = dashboard_engine(reader_settings)
    try:
        require_migrations(engine)
        data = DashboardData(engine)
        before = json.loads((OUT / "before.json").read_text(encoding="utf-8"))
        records = json.loads(json.dumps(data.fetch("SELECT * FROM earthquakes ORDER BY event_id"), default=str))
        assert records == before["events"], "Original event rows changed"
        current = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                   for path in (ROOT / "data/raw").rglob("*") if path.is_file()}
        assert current == before["archives"], "Raw file inventory or contents changed"
        role = data.fetch("""SELECT current_user AS username,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls
            FROM pg_roles WHERE rolname=current_user""")[0]
        assert not any(value for key, value in role.items() if key != "username")
        actual_gold = {name: json.loads(json.dumps(data.fetch(f"SELECT * FROM {name} ORDER BY 1"), default=str)) for name in GOLD_VIEWS}
        reports = sorted((OUT / "spark").glob("run-*/spark-report.json"), key=lambda path: path.stat().st_mtime)
        assert reports, "Run the optional Spark demonstration first"
        spark = json.loads(reports[-1].read_text(encoding="utf-8"))
        snapshot_sha = hashlib.sha256((OUT / "demo/silver-snapshot.json").read_bytes()).hexdigest()
        quality = json.loads((OUT / "demo/quality-report.json").read_text(encoding="utf-8"))
        assert spark["input_sha256"] == quality["snapshot_sha256"] == snapshot_sha
        assert spark["postgres_parity"] and compare_results(spark["results"], actual_gold)
        # Normal connection (not dashboard_engine) deliberately bypasses the client
        # transaction guard. The database role must still deny all permanent DML.
        probe = create_database_engine(reader_settings)
        denied = []
        try:
            with probe.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
                connection.exec_driver_sql("SET default_transaction_read_only=off")
                for operation, statement in (("INSERT", "INSERT INTO earthquakes(event_id) SELECT 'phase6-permission-probe' WHERE false"),
                    ("UPDATE", "UPDATE earthquakes SET magnitude=magnitude WHERE false"),
                    ("DELETE", "DELETE FROM earthquakes WHERE false")):
                    try:
                        connection.exec_driver_sql(statement)
                    except DBAPIError as error:
                        assert error.orig.sqlstate == "42501", "Expected insufficient_privilege"
                        denied.append(operation)
                    else:
                        raise AssertionError("Dashboard role unexpectedly accepted " + operation)
        finally:
            probe.dispose()
        writer = dashboard_engine(load_settings())  # Read-only inspection of existing ETL permissions.
        try:
            existing = DashboardData(writer).fetch("""SELECT current_user AS username,
                has_table_privilege(current_user,'earthquakes','INSERT') AS can_insert,
                has_table_privilege(current_user,'earthquakes','UPDATE') AS can_update,
                has_table_privilege(current_user,'earthquakes','DELETE') AS can_delete""")[0]
            assert all(value for key, value in existing.items() if key != "username")
        finally:
            writer.dispose()
        health = requests.get("http://localhost:18080/api/v2/monitor/health", timeout=10)
        health.raise_for_status()
        assert all(health.json()[key]["status"] == "healthy" for key in ("metadatabase", "scheduler", "dag_processor"))
        auth = requests.post("http://localhost:18080/auth/token", json={"username": "admin",
                             "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]}, timeout=10)
        auth.raise_for_status()
        status = requests.get("http://localhost:18080/api/v2/dags/quaketrack_earthquake_ingestion",
                              headers={"Authorization": "Bearer " + auth.json()["access_token"]}, timeout=10)
        status.raise_for_status()
        assert status.json()["is_paused"] is True
        assert requests.get("http://localhost:8501/_stcore/health", timeout=10).status_code == 200
        report = dict(preserved_events=len(records), preserved_raw_files=len(current), reader=role,
            denied_operations=denied, denial_sqlstate="42501", read_only_guard_bypassed_for_permission_test=True,
            etl_privileges=existing, gold_views=actual_gold, spark_parity=True, spark_input_rows=spark["input_rows"],
            airflow_healthy=True, dag_is_paused=True, dashboard_health_http=200)
        (OUT / "verification.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
