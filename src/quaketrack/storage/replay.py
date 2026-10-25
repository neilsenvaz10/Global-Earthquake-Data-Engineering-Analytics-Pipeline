"""Read verified Phase 2 checkpoints; replay requires no network or staged outputs."""
import hashlib
import json
from pathlib import Path

import requests

from ..ingestion.raw_archive import verify_archive
from ..ingestion.usgs_client import parse_response
from ..ingestion.windowing import MILLISECOND, parse_utc
from ..transforms.deduplicator import deduplicate
from ..transforms.normalizer import normalize_feature
from ..transforms.quarantine import quarantine_entry
from ..transforms.validator import ValidationError


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def window_bounds(checkpoint):
    params = [read_json(ref["metadata_path"])["parameters"] for ref in checkpoint["archives"]]
    if not params:
        raise ValueError("Checkpoint has no archived source response")
    return min(parse_utc(p["starttime"]) for p in params), max(parse_utc(p["endtime"]) for p in params) + MILLISECOND


def prepare_checkpoint(checkpoint, *, replay=False):
    archives = []
    for reference in checkpoint["archives"]:
        verify_archive(reference)
        metadata = read_json(reference["metadata_path"])
        if metadata["run_id"] != checkpoint["run_id"]:
            raise ValueError("Archive source run does not match its checkpoint")
        raw = Path(reference["path"]).read_bytes()
        response = requests.Response()
        response.status_code = metadata["status_code"]
        response._content = raw
        features = parse_response(response) if response.status_code in (200, 204) else None
        archives.append({"reference": reference, "metadata": metadata, "features": features})
    if replay:
        eligible, rejected = [], []
        counts = dict(fetched=0, accepted=0, rejected=0, filtered=0)
        for archive in archives:
            features, reference, metadata = archive["features"], archive["reference"], archive["metadata"]
            params = metadata["parameters"]
            if features is None or len(features) >= int(params["limit"]):
                continue  # Error attempts and capped parents are not processed inputs.
            left, right = parse_utc(params["starttime"]), parse_utc(params["endtime"]) + MILLISECOND
            counts["fetched"] += len(features)
            for feature in features:
                try:
                    record = normalize_feature(feature)
                except ValidationError as error:
                    rejected.append(quarantine_entry(feature, error, checkpoint["run_id"], reference))
                    counts["rejected"] += 1
                    continue
                counts["accepted"] += 1
                if (record["event_type"] != params["eventtype"] or not left <= parse_utc(record["occurred_at"]) < right
                    or (record["magnitude"] is not None and record["magnitude"] < float(params["minmagnitude"]))):
                    counts["filtered"] += 1
                else:
                    eligible.append(record)
        records, counts["deduplicated"] = deduplicate(eligible)
        counts["unique_records"] = len(records)
        counts["eligible"] = len(eligible)
        for name, value in counts.items():
            if checkpoint["counts"][name] != value:
                raise ValueError(f"Replay count differs from extraction checkpoint: {name}")
    else:
        for artifact in checkpoint["artifacts"]:
            if hashlib.sha256(Path(artifact["path"]).read_bytes()).hexdigest() != artifact["sha256"]:
                raise ValueError("Staged artifact checksum mismatch; use replay to rebuild from verified raw archives")
        records, rejected = (read_json(artifact["path"]) for artifact in checkpoint["artifacts"])
        if not isinstance(records, list) or not isinstance(rejected, list):
            raise ValueError("Staged records and quarantine must be arrays")
        if len(records) != checkpoint["counts"]["unique_records"] or len(rejected) != checkpoint["counts"]["rejected"]:
            raise ValueError("Staged record counts do not match checkpoint")
    digest_input = {"records": records, "rejections": [
        {key: item[key] for key in ("raw_event", "reason_code", "reason")} for item in rejected],
        "payloads": [a["reference"]["sha256"] for a in archives]}
    digest = hashlib.sha256(json.dumps(digest_input, sort_keys=True, allow_nan=False).encode()).hexdigest()
    return records, rejected, archives, digest
