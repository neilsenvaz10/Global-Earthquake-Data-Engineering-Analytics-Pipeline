"""Parameterized PostgreSQL batches; uniqueness and revision checks live in SQL."""
from uuid import UUID

from sqlalchemy import MetaData, Table, func, or_, select, text
from sqlalchemy.dialects.postgresql import insert

from ..ingestion.windowing import parse_utc
from ..transforms.deduplicator import deduplicate
from ..transforms.validator import coordinates, number, text as validate_text

WRITER_LOCK = 81743002
EVENT_FIELDS = ("event_id", "occurred_at", "source_updated_at", "magnitude", "magnitude_type",
                "depth_km", "latitude", "longitude", "place_description", "event_type", "event_status",
                "source", "network", "source_code", "source_ids", "source_networks", "source_url", "detail_url")


def reflect_tables(engine):
    metadata = MetaData()
    metadata.reflect(bind=engine, only=["earthquakes", "ingestion_runs", "ingestion_chunks",
                                       "raw_archives", "rejected_events"])
    return metadata.tables


def validated_record(record):
    """Check normalized inputs at the persistence boundary, including file inputs."""
    result = {name: record.get(name) for name in EVENT_FIELDS}
    for name in ("event_id", "event_type", "source"):
        result[name] = validate_text(result[name], name, required=True)
    for name in ("magnitude", "depth_km", "latitude", "longitude"):
        result[name] = number(result[name], name, required=name in ("latitude", "longitude"))
    coordinates(result["longitude"], result["latitude"])
    for name in ("occurred_at", "source_updated_at"):
        if result[name] is None and name == "source_updated_at":
            continue
        if result[name] is None:
            raise ValueError("occurred_at is required")
        result[name] = parse_utc(result[name]).isoformat(timespec="milliseconds")
    for name in set(EVENT_FIELDS) - {"event_id", "event_type", "source", "occurred_at",
                                     "source_updated_at", "magnitude", "depth_km", "latitude", "longitude"}:
        result[name] = validate_text(result[name], name)
    return result


def upsert_events(connection, records, run_id, *, batch_size=500, table=None, preserve_existing=False):
    """Caller owns the transaction. All batches roll back together on failure.

    Insert with DO NOTHING RETURNING, then revision-aware upsert conflicts. This
    avoids relying on PostgreSQL's internal xmax column to classify row actions.
    """
    if not 1 <= batch_size <= 1000:
        raise ValueError("batch_size must be 1..1000")
    connection.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": WRITER_LOCK})
    if table is None:
        table = Table("earthquakes", MetaData(), autoload_with=connection)
    normalized, duplicate_count = deduplicate([validated_record(record) for record in records])
    counts = {"inserted": 0, "updated": 0, "unchanged": 0, "deduplicated": duplicate_count}
    for offset in range(0, len(normalized), batch_size):
        rows = []
        for record in normalized[offset:offset + batch_size]:
            row = {**record, "last_run_id": UUID(str(run_id))}
            row["occurred_at"] = parse_utc(row["occurred_at"])
            if row["source_updated_at"] is not None:
                row["source_updated_at"] = parse_utc(row["source_updated_at"])
            rows.append(row)
        statement = insert(table).values(rows).on_conflict_do_nothing(index_elements=[table.c.event_id])
        inserted = set(connection.execute(statement.returning(table.c.event_id)).scalars())
        conflicts = [row for row in rows if row["event_id"] not in inserted]
        updated = []
        if conflicts and not preserve_existing:
            incoming = insert(table).values(conflicts)
            updates = {name: getattr(incoming.excluded, name) for name in EVENT_FIELDS if name != "event_id"}
            updates.update(last_run_id=incoming.excluded.last_run_id, last_ingested_at=func.clock_timestamp())
            statement = incoming.on_conflict_do_update(
                index_elements=[table.c.event_id], set_=updates,
                where=incoming.excluded.source_updated_at.is_not(None) & or_(
                    table.c.source_updated_at.is_(None), incoming.excluded.source_updated_at > table.c.source_updated_at))
            updated = connection.execute(statement.returning(table.c.event_id)).scalars().all()
        counts["inserted"] += len(inserted)
        counts["updated"] += len(updated)
        counts["unchanged"] += len(conflicts) - len(updated)
    return counts
