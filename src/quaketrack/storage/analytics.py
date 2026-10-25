"""Named SQL files with bound parameters, half-open UTC ranges and bounded rows."""
import math

from sqlalchemy import text

from ..config import ROOT
from ..ingestion.windowing import parse_utc

QUERY_DIR = ROOT / "sql/analytics"
QUERIES = ("overview", "daily_counts", "monthly_counts", "magnitude_distribution", "strongest_events",
           "geographic_distribution", "recent_activity", "magnitude_trends", "freshness", "run_summaries", "integrity")


def query_analytics(engine, query, start, end, *, limit=20, min_latitude=-90, max_latitude=90,
                    min_longitude=-180, max_longitude=180):
    start, end = parse_utc(start), parse_utc(end)
    if query not in QUERIES or start >= end or not 1 <= limit <= 1000:
        raise ValueError("Invalid query, date range or limit (1..1000)")
    bounds = (min_latitude, max_latitude, min_longitude, max_longitude)
    if (not all(math.isfinite(value) for value in bounds) or not -90 <= min_latitude <= max_latitude <= 90
        or not -180 <= min_longitude <= max_longitude <= 180):
        raise ValueError("Invalid geographic bounding box")
    parameters = dict(start=start, end=end, limit=limit, min_latitude=min_latitude, max_latitude=max_latitude,
                      min_longitude=min_longitude, max_longitude=max_longitude)
    with engine.begin() as connection:
        connection.exec_driver_sql("SET TRANSACTION READ ONLY")
        return [dict(row) for row in connection.execute(text((QUERY_DIR / (query + ".sql")).read_text(encoding="utf-8")), parameters).mappings()]
