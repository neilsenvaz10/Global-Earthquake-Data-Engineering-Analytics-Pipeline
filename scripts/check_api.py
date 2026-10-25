"""Run from the repository root after pip install -e ."""
import argparse
import logging
from quaketrack.config import load_settings
from quaketrack.logging_config import configure_logging
from quaketrack.source_check import check_api

def main():
    parser = argparse.ArgumentParser(description="Archive at most ten real USGS events")
    parser.add_argument("--start", default="2026-10-01")
    parser.add_argument("--end", default="2026-10-02")
    args = parser.parse_args()
    try:
        settings = load_settings()
        configure_logging(settings.log_level)
        directory, summary = check_api(settings, args.start, args.end)
        logging.info("USGS check succeeded: %s records archived at %s", summary["count"], directory)
        logging.info("Null magnitudes=%s; null depths=%s; event types=%s",
                     summary["null_magnitudes"], summary["null_depths"], summary["event_types"])
        return 0
    except Exception as exc:
        logging.error("USGS check failed (%s). Check network, dates and configuration.", type(exc).__name__)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
