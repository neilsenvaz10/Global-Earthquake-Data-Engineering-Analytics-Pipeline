"""Exclusive archive creation and checksum verification for resume/replay."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def archive_response(root: Path, run_id: str, response, params: dict, attempt: int) -> dict:
    chunk_key = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()[:16]
    directory = root / run_id / chunk_key / str(uuid4())
    directory.mkdir(parents=True, exist_ok=False)
    raw = response.content
    with (directory / "response.geojson").open("xb") as stream:
        stream.write(raw)
    digest = hashlib.sha256(raw).hexdigest()
    metadata = {"run_id": run_id, "source": "USGS", "parameters": params,
                "request_url": response.url, "status_code": response.status_code,
                "attempt": attempt, "fetched_at": datetime.now(timezone.utc).isoformat(),
                "payload_sha256": digest, "archive_path": "response.geojson"}
    with (directory / "metadata.json").open("x", encoding="utf-8") as stream:
        json.dump(metadata, stream, indent=2, allow_nan=False)
    return {"path": str((directory / "response.geojson").resolve()), "sha256": digest,
            "metadata_path": str((directory / "metadata.json").resolve())}


def verify_archive(reference: dict):
    path = Path(reference["path"])
    if hashlib.sha256(path.read_bytes()).hexdigest() != reference["sha256"]:
        raise ValueError(f"Raw archive checksum mismatch: {path}")
    metadata = json.loads(Path(reference["metadata_path"]).read_text(encoding="utf-8"))
    if metadata["payload_sha256"] != reference["sha256"]:
        raise ValueError(f"Raw metadata checksum mismatch: {path}")
