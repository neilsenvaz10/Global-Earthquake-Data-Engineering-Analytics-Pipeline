"""Central environment configuration; process variables override the root .env."""
import logging
import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import URL

ROOT = Path(__file__).resolve().parents[2]

@dataclass(frozen=True)
class Settings:
    api_url: str
    minimum_magnitude: float
    event_type: str
    timeout: float
    archive_dir: Path
    log_level: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str = field(repr=False)

    @property
    def database_url(self):
        if not self.db_password:
            raise ValueError("Set POSTGRES_PASSWORD in .env or the environment")
        return URL.create("postgresql+psycopg", username=self.db_user,
                          password=self.db_password, host=self.db_host,
                          port=self.db_port, database=self.db_name)

def load_settings(env_file=ROOT / ".env"):
    load_dotenv(env_file, override=False)
    timeout = float(os.getenv("USGS_REQUEST_TIMEOUT_SECONDS", "30"))
    magnitude = float(os.getenv("USGS_MIN_MAGNITUDE", "2.5"))
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    url = os.getenv("USGS_API_BASE_URL", "https://earthquake.usgs.gov/fdsnws/event/1/query")
    if url != "https://earthquake.usgs.gov/fdsnws/event/1/query":
        raise ValueError("Phase 1 requires the official USGS HTTPS query endpoint")
    if not math.isfinite(timeout) or timeout <= 0 or not math.isfinite(magnitude):
        raise ValueError("Timeout must be positive and magnitude must be finite")
    if not 1 <= port <= 65535 or level not in logging.getLevelNamesMapping():
        raise ValueError("Invalid PostgreSQL port or LOG_LEVEL")
    archive = Path(os.getenv("RAW_ARCHIVE_DIR", "data/raw"))
    return Settings(url, magnitude, os.getenv("USGS_EVENT_TYPE", "earthquake"),
                    timeout, archive if archive.is_absolute() else ROOT / archive, level,
                    os.getenv("POSTGRES_HOST", "localhost"), port,
                    os.getenv("POSTGRES_DB", "quaketrack"),
                    os.getenv("POSTGRES_USER", "quaketrack_app"),
                    os.getenv("POSTGRES_PASSWORD", ""))
