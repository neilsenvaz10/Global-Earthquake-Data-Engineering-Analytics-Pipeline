"""Validation failures retain the source record and its archive provenance."""
from datetime import datetime, timezone


def quarantine_entry(raw, error, run_id, archive):
    return {"run_id": run_id, "source": "USGS", "source_event_id": raw.get("id") if isinstance(raw, dict) else None,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "reason_code": "invalid_" + error.field, "reason": str(error),
            "archive": archive, "raw_event": raw}
