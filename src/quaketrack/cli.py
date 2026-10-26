"""Run with python -m quaketrack.cli backfill --help."""
import argparse
import json
import logging

from .config import load_settings
from .logging_config import configure_logging
from .pipeline import run_backfill
from .storage.database import create_database_engine, database_identity
from .storage.loader import load_staging
from .storage.migrations import apply_migrations


def main(argv=None):
    parser = argparse.ArgumentParser(description="QuakeTrack historical ETL and PostgreSQL persistence")
    commands = parser.add_subparsers(dest="command", required=True)
    backfill = commands.add_parser("backfill")
    backfill.add_argument("--start", required=True, help="Inclusive UTC date or ISO8601 timestamp")
    backfill.add_argument("--end", required=True, help="Exclusive UTC date or ISO8601 timestamp")
    backfill.add_argument("--min-magnitude", type=float)
    backfill.add_argument("--window-days", type=float)
    backfill.add_argument("--output-dir", type=str, help="Staging directory; raw/quarantine use configuration")
    backfill.add_argument("--resume", action="store_true", help="Verify and skip committed matching windows")
    backfill.add_argument("--max-windows", type=int, help="Stop after N newly processed windows; status=limited")
    backfill.add_argument("--load-database", action="store_true", help="Load completed staging windows into an explicitly migrated database")
    backfill.add_argument("--preserve-existing", action="store_true", help="With --load-database, keep all existing event rows unchanged even if USGS has newer revisions")
    commands.add_parser("migrate", help="Apply reviewed SQL migrations explicitly")
    for name in ("load-staging", "replay"):
        command = commands.add_parser(name, help="Load Phase 2 artifacts" if name == "load-staging" else "Replay verified raw archives offline into PostgreSQL")
        command.add_argument("--staging-dir")
        command.add_argument("--resume", action="store_true", help="Skip successful matching PostgreSQL chunks")
        command.add_argument("--batch-size", type=int, default=500)
    analytics = commands.add_parser("analytics", help="Execute a parameterized analytical query")
    from .storage.analytics import QUERIES
    analytics.add_argument("query", choices=QUERIES)
    analytics.add_argument("--start", required=True)
    analytics.add_argument("--end", required=True)
    analytics.add_argument("--limit", type=int, default=20)
    for name, default in (("min-latitude", -90), ("max-latitude", 90), ("min-longitude", -180), ("max-longitude", 180)):
        analytics.add_argument("--" + name, type=float, default=default)
    args = parser.parse_args(argv)
    try:
        settings = load_settings()
        configure_logging(settings.log_level)
        if args.command == "backfill":
            report = run_backfill(settings, args.start, args.end, minimum_magnitude=args.min_magnitude,
                                  window_days=args.window_days, output_dir=args.output_dir,
                                  resume=args.resume, max_windows=args.max_windows)
            if args.load_database:
                engine = create_database_engine(settings)
                try:
                    report["database_load"] = load_staging(engine, args.output_dir or settings.staging_dir,
                                                          resume=args.resume, preserve_existing=args.preserve_existing)
                finally:
                    engine.dispose()
        else:
            engine = create_database_engine(settings)
            try:
                if args.command == "migrate":
                    report = {"database": database_identity(engine), "migrations": apply_migrations(engine)}
                elif args.command in ("load-staging", "replay"):
                    report = load_staging(engine, args.staging_dir or settings.staging_dir,
                                          resume=args.resume, replay=args.command == "replay", batch_size=args.batch_size)
                else:
                    from .storage.analytics import query_analytics
                    report = query_analytics(engine, args.query, args.start, args.end, limit=args.limit,
                        min_latitude=args.min_latitude, max_latitude=args.max_latitude,
                        min_longitude=args.min_longitude, max_longitude=args.max_longitude)
            finally:
                engine.dispose()
        print(json.dumps(report, indent=2, default=str))
        return 0
    except KeyboardInterrupt:
        logging.error("ETL interrupted; rerun with --resume to continue committed windows")
        return 130
    except Exception as error:
        logging.error("ETL failed (%s): %s", type(error).__name__, error)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
