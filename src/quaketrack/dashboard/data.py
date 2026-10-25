"""Shared bound filters and bounded, server-side analytics. No DDL or writes."""
import csv
import io
import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone

from sqlalchemy import text

from ..storage.database import create_database_engine

UTC = timezone.utc
CHART_LIMIT = 5000
EXPORT_LIMIT = 10000
EVENT_COLUMNS = ("event_id", "occurred_at", "magnitude", "magnitude_type", "depth_km",
                 "latitude", "longitude", "place_description", "event_status", "source_url")


@dataclass(frozen=True)
class Filters:
    start: datetime
    end: datetime
    min_magnitude: float | None = None
    max_magnitude: float | None = None
    min_depth: float | None = None
    max_depth: float | None = None
    location: str = ""
    include_unknown: bool = True

    def __post_init__(self):
        if any(d.tzinfo is None or d.utcoffset() is None for d in (self.start, self.end)):
            raise ValueError("Date boundaries must have a timezone")
        if self.start >= self.end:
            raise ValueError("Start date must not be after end date")
        for low, high in ((self.min_magnitude, self.max_magnitude), (self.min_depth, self.max_depth)):
            if any(v is not None and not math.isfinite(v) for v in (low, high)):
                raise ValueError("Measurement bounds must be finite")
            if low is not None and high is not None and low > high:
                raise ValueError("Minimum must not exceed maximum")
        if len(self.location) > 200:
            raise ValueError("Location search is limited to 200 characters")

    @classmethod
    def dates(cls, start: date, end: date, **kwargs):
        """Both UI dates inclusive; SQL uses [start midnight, next midnight) UTC."""
        return cls(datetime.combine(start, time.min, UTC),
                   datetime.combine(end + timedelta(days=1), time.min, UTC), **kwargs)

    def sql(self):
        clauses = ["event_type = 'earthquake'", "occurred_at >= :start", "occurred_at < :end"]
        params = {"start": self.start.astimezone(UTC), "end": self.end.astimezone(UTC)}
        for column, low, high in (("magnitude", self.min_magnitude, self.max_magnitude),
                                  ("depth_km", self.min_depth, self.max_depth)):
            bounds = []
            for suffix, value, operator in (("min", low, ">="), ("max", high, "<=")):
                if value is not None:
                    key = column + "_" + suffix
                    bounds.append(f"{column} {operator} :{key}")
                    params[key] = value
            if bounds:
                predicate = " AND ".join(bounds)
                clauses.append(f"({column} IS NULL OR ({predicate}))" if self.include_unknown else f"({predicate})")
            elif not self.include_unknown:
                clauses.append(f"{column} IS NOT NULL")
        if self.location.strip():
            # Treat user wildcards literally, while keeping SQL values bound.
            escaped = self.location.strip().replace("!", "!!").replace("%", "!%").replace("_", "!_")
            clauses.append("place_description ILIKE :location ESCAPE '!'")
            params["location"] = "%" + escaped + "%"
        return " AND ".join(clauses), params


def dashboard_engine(settings):
    # Server default protects even an accidental write outside fetch().
    engine = create_database_engine(settings)
    from sqlalchemy import event

    @event.listens_for(engine, "connect")
    def set_readonly(connection, _):
        with connection.cursor() as cursor:
            cursor.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        connection.commit()

    return engine


class DashboardData:
    def __init__(self, engine):
        self.engine = engine

    def fetch(self, statement, parameters=None):
        with self.engine.begin() as connection:
            connection.exec_driver_sql("SET TRANSACTION READ ONLY")
            return [dict(row) for row in connection.execute(text(statement), parameters or {}).mappings()]

    def coverage(self):
        summary = self.fetch("""SELECT COUNT(*) AS total, MIN(occurred_at) AS first_event,
            MAX(occurred_at) AS last_event, COUNT(DISTINCT (occurred_at AT TIME ZONE 'UTC')::date) AS days,
            MIN(magnitude) AS min_magnitude, MAX(magnitude) AS max_magnitude,
            MIN(depth_km) AS min_depth, MAX(depth_km) AS max_depth,
            MAX(last_ingested_at) AS last_ingested_at, CURRENT_TIMESTAMP AS queried_at,
            (SELECT MAX(finished_at) FROM pipeline_health WHERE status='success') AS last_successful_workflow
            FROM earthquakes WHERE event_type='earthquake'""")[0]
        summary["source_filters"] = self.fetch("""SELECT DISTINCT
            request_parameters->>'minmagnitude' AS minimum_magnitude,
            request_parameters->>'eventtype' AS event_type
            FROM raw_archives ORDER BY minimum_magnitude, event_type LIMIT 100""")
        return summary

    def overview(self, filters):
        where, params = filters.sql()
        return self.fetch(f"""SELECT COUNT(*) AS total, MAX(magnitude) AS max_magnitude,
            AVG(magnitude) AS avg_magnitude, AVG(depth_km) AS avg_depth,
            COUNT(*) FILTER (WHERE magnitude IS NULL) AS unknown_magnitude,
            COUNT(*) FILTER (WHERE depth_km IS NULL) AS unknown_depth,
            MAX(occurred_at) AS latest_event, MAX(source_updated_at) AS latest_source_update,
            MAX(last_ingested_at) AS last_ingested_at FROM earthquakes WHERE {where}""", params)[0]

    def timeline(self, filters, interval="day"):
        if interval not in ("day", "week", "month"):
            raise ValueError("Invalid timeline interval")
        where, params = filters.sql()
        params["interval"] = interval
        return self.fetch(f"""SELECT date_trunc(:interval, occurred_at AT TIME ZONE 'UTC') AS period,
            COUNT(*) AS events FROM earthquakes WHERE {where} GROUP BY 1 ORDER BY 1""", params)

    def distribution(self, filters, measure="magnitude"):
        if measure not in ("magnitude", "depth_km"):
            raise ValueError("Invalid distribution measure")
        where, params = filters.sql()
        # Adapt width for broad selections: at most ~42 bins, including unknown.
        width = 0.5 if measure == "magnitude" else 25
        params["width"] = width
        return self.fetch(f"""WITH selected AS (SELECT {measure} AS value FROM earthquakes WHERE {where}),
            bounds AS (SELECT GREATEST(:width, COALESCE(ceil((MAX(value)-MIN(value)) / (40.0 * :width)) * :width,
                :width)) AS bin_width FROM selected)
            SELECT floor(value / bin_width) * bin_width AS bin_start, bin_width, COUNT(*) AS events
            FROM selected CROSS JOIN bounds GROUP BY 1, 2 ORDER BY 1 NULLS LAST""", params)

    def events(self, filters, *, sort="Newest", limit=50, offset=0):
        orders = {"Newest": "occurred_at DESC", "Oldest": "occurred_at ASC",
                  "Source update": "source_updated_at DESC NULLS LAST, occurred_at DESC",
                  "Magnitude": "magnitude DESC NULLS LAST, occurred_at DESC",
                  "Depth": "depth_km DESC NULLS LAST, occurred_at DESC"}
        if sort not in orders or not 1 <= limit <= EXPORT_LIMIT or not 0 <= offset <= 10_000_000:
            raise ValueError("Invalid sort or page bounds")
        where, params = filters.sql()
        params.update(limit=limit, offset=offset)
        return self.fetch(f"SELECT {', '.join(EVENT_COLUMNS)} FROM earthquakes WHERE {where} "
                          f"ORDER BY {orders[sort]}, event_id LIMIT :limit OFFSET :offset", params)

    def export(self, filters, sort="Newest"):
        if self.overview(filters)["total"] > EXPORT_LIMIT:
            raise ValueError(f"Narrow the filters to at most {EXPORT_LIMIT:,} events before exporting")
        # One bounded snapshot query; limit+1 detects growth after the count query.
        where, params = filters.sql()
        orders = {"Newest": "occurred_at DESC", "Oldest": "occurred_at ASC",
                  "Magnitude": "magnitude DESC NULLS LAST, occurred_at DESC",
                  "Depth": "depth_km DESC NULLS LAST, occurred_at DESC"}
        if sort not in orders:
            raise ValueError("Invalid sort")
        rows = self.fetch(f"SELECT {', '.join(EVENT_COLUMNS)} FROM earthquakes WHERE {where} "
                          f"ORDER BY {orders[sort]}, event_id LIMIT {EXPORT_LIMIT + 1}", params)
        if len(rows) > EXPORT_LIMIT:
            raise ValueError("Dataset changed; narrow filters and export again")
        return csv_bytes(rows)

    def monitoring(self):
        return {
            "workflows": self.fetch("SELECT * FROM pipeline_health ORDER BY started_at DESC LIMIT 50"),
            "last_success": self.fetch("""SELECT MAX(finished_at) AS finished_at FROM ingestion_runs
                WHERE status='success' AND execution_key IS NOT NULL""")[0]["finished_at"],
            "loads": self.fetch("""SELECT run_id, job_type, status, started_at, finished_at,
                records_fetched, records_inserted, records_updated, records_unchanged, records_rejected
                FROM ingestion_runs WHERE execution_key IS NULL ORDER BY started_at DESC LIMIT 50"""),
            "state": self.fetch("SELECT * FROM pipeline_state ORDER BY pipeline_name LIMIT 100"),
        }


def utc_label(value):
    if value is None:
        return "Unknown"
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return str(value)


def csv_bytes(rows):
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=EVENT_COLUMNS)
    writer.writeheader()
    writer.writerows({key: utc_label(row.get(key)) if isinstance(row.get(key), datetime)
                     else row.get(key) for key in EVENT_COLUMNS} for row in rows)
    return buffer.getvalue().encode("utf-8-sig")


def freshness(value, now=None):
    if value is None:
        return "Unknown"
    now = now or datetime.now(UTC)
    if value.tzinfo is None or now.tzinfo is None:
        raise ValueError("Freshness requires timezone-aware timestamps")
    seconds = (now - value).total_seconds()
    if seconds < 0:
        return "Timestamp is in the future; check clocks"
    return f"{seconds / 3600:.1f} hours ago"
