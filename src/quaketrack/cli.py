"""Run with python -m quaketrack.cli backfill --help."""
import argparse
import json
import logging

from .config import load_settings
from .logging_config import configure_logging
from .pipeline import run_backfill


def main(argv=None):
    parser = argparse.ArgumentParser(description="QuakeTrack historical ETL to local staging (Phase 2)")
    commands = parser.add_subparsers(dest="command", required=True)
    backfill = commands.add_parser("backfill")
    backfill.add_argument("--start", required=True, help="Inclusive UTC date or ISO8601 timestamp")
    backfill.add_argument("--end", required=True, help="Exclusive UTC date or ISO8601 timestamp")
    backfill.add_argument("--min-magnitude", type=float)
    backfill.add_argument("--window-days", type=float)
    backfill.add_argument("--output-dir", type=str, help="Staging directory; raw/quarantine use configuration")
    backfill.add_argument("--resume", action="store_true", help="Verify and skip committed matching windows")
    backfill.add_argument("--max-windows", type=int, help="Stop after N newly processed windows; status=limited")
    args = parser.parse_args(argv)
    try:
        settings = load_settings()
        configure_logging(settings.log_level)
        report = run_backfill(settings, args.start, args.end, minimum_magnitude=args.min_magnitude,
                              window_days=args.window_days, output_dir=args.output_dir,
                              resume=args.resume, max_windows=args.max_windows)
        print(json.dumps(report, indent=2))
        return 0
    except KeyboardInterrupt:
        logging.error("ETL interrupted; rerun with --resume to continue committed windows")
        return 130
    except Exception as error:
        logging.error("ETL failed (%s): %s", type(error).__name__, error)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
