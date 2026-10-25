"""Logical layers: existing raw files, current normalized records, ordinary views."""
import hashlib
import json

from .storage.repository import validated_record
from .transforms.normalizer import normalize_feature

GOLD_VIEWS = ("gold_daily_earthquakes", "gold_magnitude_distribution",
              "gold_monthly_earthquakes", "gold_depth_by_magnitude")


def verify_layers(records, archive_root):
    candidates = {}
    count = 0
    for metadata_path in archive_root.rglob("metadata.json"):
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        payload = metadata_path.parent / "response.geojson"
        raw = payload.read_bytes()
        if hashlib.sha256(raw).hexdigest() != metadata["payload_sha256"]:
            raise ValueError("Bronze payload checksum mismatch")
        if not all(metadata.get(key) for key in ("source", "run_id", "fetched_at", "parameters")):
            raise ValueError("Bronze source provenance is incomplete")
        count += 1
        try:
            source = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            continue  # Preserved HTTP error bodies are not Silver inputs.
        if not isinstance(source, dict):
            continue
        for feature in source.get("features", []):
            try:
                row = validated_record(normalize_feature(feature))
            except (ValueError, TypeError):
                continue  # Invalid source records cannot validate Silver output.
            candidates.setdefault(row["event_id"], []).append(row)
    if count != sum(1 for _ in archive_root.rglob("response.geojson")):
        raise ValueError("A Bronze response has no metadata")
    matched = sum(validated_record(row) in candidates.get(row["event_id"], []) for row in records)
    if matched != len(records):
        raise ValueError("Silver rows do not match validated archived source representations")
    return dict(bronze_responses_verified=count, silver_records_checked=len(records), silver_source_matches=matched,
                note="Source comparison uses valid normalized representations; raw retry/sample overlaps are not unique event counts.")
