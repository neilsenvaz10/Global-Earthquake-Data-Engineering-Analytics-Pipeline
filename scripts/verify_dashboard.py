"""Read-only live verification and preservation manifest (never fabricates events).

Run once with --snapshot before a bounded DAG execution, then without it afterwards.
Evidence is local and ignored under data/staging/phase5/.
"""
import argparse
import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text
from streamlit.testing.v1 import AppTest

from quaketrack.config import ROOT, load_settings
from quaketrack.dashboard.app import PAGES, cached, get_data
from quaketrack.dashboard.data import DashboardData, Filters, dashboard_engine
from quaketrack.storage.analytics import query_analytics
from quaketrack.storage.migrations import require_migrations

OUT = ROOT / "data/staging/phase5"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", action="store_true")
    args = parser.parse_args()
    settings = load_settings()
    engine = dashboard_engine(settings)
    try:
        require_migrations(engine)
        data = DashboardData(engine)
        rows = data.fetch("SELECT * FROM earthquakes ORDER BY event_id")
        serialized = json.loads(json.dumps(rows, default=str))
        archives = {str(path.relative_to(ROOT)): digest(path) for path in settings.archive_dir.rglob("*") if path.is_file()}
        OUT.mkdir(parents=True, exist_ok=True)
        if args.snapshot:
            path = OUT / "before.json"
            if path.exists():
                raise RuntimeError("Preservation snapshot already exists; refusing to overwrite it")
            path.write_text(json.dumps(dict(events=serialized, archives=archives), indent=2), encoding="utf-8")
            print(json.dumps(dict(snapshot=str(path), events=len(rows), archive_files=len(archives))))
            return
        before = json.loads((OUT / "before.json").read_text(encoding="utf-8"))
        by_id = {row["event_id"]: row for row in serialized}
        assert all(by_id.get(row["event_id"]) == row for row in before["events"]), "Pre-existing event rows changed"
        assert all(archives.get(path) == sha for path, sha in before["archives"].items()), "Pre-existing raw artifact changed"
        assert len(rows) == len(by_id)
        coverage = data.coverage()
        filters = Filters.dates(coverage["first_event"].date(), coverage["last_event"].date())
        summary = data.overview(filters)
        assert summary["total"] == len(rows)
        assert sum(row["events"] for row in data.timeline(filters)) == len(rows)
        assert len(list(csv.DictReader(io.StringIO(data.export(filters).decode("utf-8-sig"))))) == len(rows)
        direct = query_analytics(engine, "overview", filters.start, filters.end)[0]
        assert direct["event_count"] == summary["total"]
        # Uses the normal PostgreSQL-backed entry point, never mocks the data layer.
        cached.clear()
        get_data.clear()
        app = AppTest.from_file(str(ROOT / "dashboard/app.py"), default_timeout=30).run()
        checks = []
        for page in PAGES:
            app.radio(key="page").set_value(page).run()
            assert not app.exception and not app.error, f"Page failed: {page}"
            checks.append(dict(page=page, plotly_charts=len(app.get("plotly_chart")), tables=len(app.dataframe)))
        app.radio(key="page").set_value("Explorer").run()
        app.text_input(key="location").set_value("Japan").run()
        japan = data.overview(Filters.dates(coverage["first_event"].date(), coverage["last_event"].date(), location="Japan"))["total"]
        assert any(f"{japan:,} matching events" in item.value for item in app.caption)
        app.text_input(key="location").set_value("no-such-location-phase5-verification").run()
        assert any("No events match" in item.value for item in app.info)
        integrity = query_analytics(engine, "integrity", filters.start, filters.end)
        assert all(value == 0 for row in integrity for value in row.values()), integrity
        report = dict(checked_at=datetime.now(timezone.utc), coverage=coverage, overview=summary,
            preserved_events=len(before["events"]), preserved_archive_files=len(before["archives"]),
            current_archive_files=len(archives), page_checks=checks, csv_rows=len(rows),
            japan_filter_matches=japan, integrity=integrity, workflows=data.monitoring()["workflows"])
        (OUT / "verification.json").write_text(json.dumps(report, default=str, indent=2), encoding="utf-8")
        print(json.dumps({k: v for k, v in report.items() if k != "workflows"}, default=str, indent=2))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
