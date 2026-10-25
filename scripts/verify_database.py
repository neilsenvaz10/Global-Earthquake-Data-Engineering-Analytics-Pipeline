"""Verify SELECT 1 without creating or modifying tables."""
import logging
from quaketrack.config import load_settings
from quaketrack.logging_config import configure_logging
from quaketrack.storage.database import verify_database

def main():
    try:
        settings = load_settings()
        configure_logging(settings.log_level)
        if not verify_database(settings):
            raise RuntimeError("Unexpected SELECT 1 result")
        logging.info("PostgreSQL SELECT 1 succeeded")
        return 0
    except Exception as exc:
        logging.error("PostgreSQL check failed (%s). Check credentials, host, port and service health.", type(exc).__name__)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
