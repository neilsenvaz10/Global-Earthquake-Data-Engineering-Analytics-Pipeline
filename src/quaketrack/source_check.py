"""One fixed-window proof of connectivity, not the Phase 2 extraction client."""
import hashlib
import json
from datetime import datetime, timezone
from uuid import uuid4
import requests

def check_api(settings, start="2026-10-01", end="2026-10-02", output_dir=None):
    start_dt, end_dt = (datetime.fromisoformat(v.replace("Z", "+00:00")) for v in (start, end))
    start_dt = start_dt.replace(tzinfo=start_dt.tzinfo or timezone.utc)
    end_dt = end_dt.replace(tzinfo=end_dt.tzinfo or timezone.utc)
    if start_dt >= end_dt:
        raise ValueError("Start must precede end")
    params = dict(format="geojson", starttime=start, endtime=end,
                  minmagnitude=settings.minimum_magnitude, eventtype=settings.event_type,
                  orderby="time-asc", limit=10)
    response = requests.get(settings.api_url, params=params, timeout=settings.timeout)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or payload.get("type") != "FeatureCollection" or not isinstance(payload.get("features"), list):
        raise ValueError("USGS response is not a GeoJSON FeatureCollection")
    features = payload["features"]
    if any(not isinstance(f, dict) or not isinstance(f.get("properties"), dict)
           or not isinstance(f.get("geometry"), dict)
           or not isinstance(f["geometry"].get("coordinates"), list)
           or len(f["geometry"]["coordinates"]) < 3 for f in features):
        raise ValueError("Unexpected USGS feature structure")
    run_id = str(uuid4())
    directory = (output_dir or settings.archive_dir) / run_id
    directory.mkdir(parents=True, exist_ok=False)
    raw = response.content
    (directory / "response.geojson").write_bytes(raw)
    summary = {
        "count": len(features),
        "null_magnitudes": sum(f["properties"].get("mag") is None for f in features),
        "null_depths": sum(f["geometry"]["coordinates"][2] is None for f in features),
        "event_types": sorted({str(f["properties"].get("type")) for f in features}),
        "first_record": features[0] if features else None,
    }
    metadata = dict(run_id=run_id, source="USGS", source_url=settings.api_url,
                    request_url=response.url, parameters=params,
                    fetched_at=datetime.now(timezone.utc).isoformat(),
                    payload_sha256=hashlib.sha256(raw).hexdigest(),
                    archive_path="response.geojson", record_count=len(features),
                    sample_limit=10, complete_window=False, inspection=summary)
    (directory / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return directory, summary
