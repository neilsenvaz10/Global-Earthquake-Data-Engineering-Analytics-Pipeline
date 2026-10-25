"""Read-only quality/layer report and a bounded Silver snapshot for optional Spark."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from sqlalchemy import text

from .config import ROOT
from .dashboard.data import dashboard_engine
from .dashboard.security import load_dashboard_settings
from .governance import quality_report
from .medallion import GOLD_VIEWS, verify_layers
from .storage.repository import EVENT_FIELDS


def prepare(output_dir, *, verify_archives=False):
    engine = dashboard_engine(load_dashboard_settings())
    try:
        with engine.connect().execution_options(isolation_level="REPEATABLE READ") as connection:
            with connection.begin():
                connection.exec_driver_sql("SET TRANSACTION READ ONLY")
                records = [dict(row) for row in connection.execute(text(
                    f"SELECT {','.join(EVENT_FIELDS)},last_run_id FROM earthquakes ORDER BY event_id LIMIT 10001")).mappings()]
                if len(records) > 10000:
                    raise ValueError("Spark demo snapshot limited to 10,000 current records; choose a reviewed smaller input")
                expected = {view: [dict(row) for row in connection.execute(text(f"SELECT * FROM {view} ORDER BY 1")).mappings()]
                            for view in GOLD_VIEWS}
                identity = connection.exec_driver_sql("SELECT current_user").scalar_one()
        # Same repeatable-read snapshot underlies Silver and Gold comparisons.
        report = quality_report(records, require_provenance=True)
        layers = verify_layers(records, ROOT / "data/raw") if verify_archives else None
        payload = dict(created_at=datetime.now(timezone.utc), input_stage="persisted Silver snapshot",
                       database_user=identity, records=records, expected_postgresql=expected)
        raw = json.dumps(payload, default=lambda value: value.isoformat() if hasattr(value, "isoformat") else str(value),
                         allow_nan=False, indent=2).encode("utf-8")
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        # Never overwrite an existing demonstration snapshot or report.
        for name in ("silver-snapshot.json", "quality-report.json"):
            if (output_dir / name).exists():
                raise ValueError("Output exists; use a fresh --output-dir to preserve evidence")
        with (output_dir / "silver-snapshot.json").open("xb") as stream:
            stream.write(raw)
        evidence = dict(quality=report, layers=layers, snapshot_sha256=hashlib.sha256(raw).hexdigest(),
                        database_user=identity, generated_at=payload["created_at"])
        with (output_dir / "quality-report.json").open("x", encoding="utf-8") as stream:
            json.dump(evidence, stream, default=str, allow_nan=False, indent=2)
        print(json.dumps(evidence, default=str, indent=2))
        return evidence
    finally:
        engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(ROOT / "data/staging/phase6/demo"))
    parser.add_argument("--verify-archives", action="store_true", help="Read local Bronze bytes and compare Silver to normalized source")
    args = parser.parse_args()
    prepare(args.output_dir, verify_archives=args.verify_archives)


if __name__ == "__main__":
    main()
