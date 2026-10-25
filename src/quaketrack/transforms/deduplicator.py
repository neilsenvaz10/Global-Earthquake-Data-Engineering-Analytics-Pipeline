"""Revision precedence shared by batch deduplication and future persistence."""
import json


def prefer_incoming(stored: dict, incoming: dict) -> bool:
    old, new = stored["source_updated_at"], incoming["source_updated_at"]
    # UTC ISO strings have a fixed representation and chronological ordering.
    return new is not None and (old is None or new > old)


def deduplicate(records: list[dict]) -> tuple[list[dict], int]:
    latest = {}
    # Canonical sorting makes equal-revision conflicts deterministic within a batch.
    for record in sorted(records, key=lambda r: json.dumps(r, sort_keys=True)):
        old = latest.get(record["event_id"])
        if old is None or prefer_incoming(old, record):
            latest[record["event_id"]] = record
    return [latest[key] for key in sorted(latest)], len(records) - len(latest)
