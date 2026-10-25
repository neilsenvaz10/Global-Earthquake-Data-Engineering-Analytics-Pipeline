"""Historical extraction -> immutable raw -> validation -> local staging.

Each complete top-level window commits its catalog and checkpoint together. Failed
windows can leave orphan archives/artifacts but cannot advance committed progress.
"""
import hashlib
import json
import logging
import math
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from .ingestion.raw_archive import archive_response, verify_archive
from .ingestion.usgs_client import USGSClient, parse_response
from .ingestion.windowing import parse_utc, source_times, split_window, windows
from .monitoring.run_metrics import empty_counts, reconcile, utc_now
from .storage.staging import atomic_json, load_state, staging_lock
from .transforms.deduplicator import deduplicate, prefer_incoming
from .transforms.normalizer import normalize_feature
from .transforms.quarantine import quarantine_entry
from .transforms.validator import ValidationError

log = logging.getLogger(__name__)


def extract_window(client, settings, start, end, magnitude, references, run_id):
    params = {"format": "geojson", **source_times(start, end), "minmagnitude": str(magnitude).removesuffix(".0"),
              "eventtype": settings.event_type, "orderby": "time-asc", "limit": settings.request_limit}

    def archive(response, query, attempt):
        reference = archive_response(settings.archive_dir, run_id, response, query, attempt)
        references.append(reference)
        return reference

    response, reference = client.get(params, archive)
    features = parse_response(response)
    if len(features) >= settings.request_limit:
        # Preserve the capped parent as evidence, but process only complete leaves.
        for left, right in split_window(start, end):
            yield from extract_window(client, settings, left, right, magnitude, references, run_id)
    else:
        yield features, reference, start, end


def _checkpoint_key(start, end, magnitude, settings):
    request = {"start": start.isoformat(), "end": end.isoformat(), "minimum_magnitude": magnitude,
               "event_type": settings.event_type, "url": settings.api_url, "contract_version": 1}
    return hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()


def run_backfill(settings, start, end, *, minimum_magnitude=None, window_days=None,
                 output_dir=None, resume=False, max_windows=None, client=None):
    start, end = parse_utc(start), parse_utc(end)
    magnitude = settings.minimum_magnitude if minimum_magnitude is None else minimum_magnitude
    days = settings.window_days if window_days is None else window_days
    if not math.isfinite(magnitude) or not math.isfinite(days) or not 0 < days <= 366:
        raise ValueError("Magnitude must be finite and window days in (0, 366]")
    # Validate the full requested range even when only part of it will run.
    size = timedelta(days=days)
    next(windows(start, end, size))
    if max_windows is not None and max_windows < 1:
        raise ValueError("max_windows must be positive")
    root = Path(output_dir or settings.staging_dir).resolve()
    run_id = str(uuid4())
    report = {"run_id": run_id, "job_type": "backfill", "started_at": utc_now(),
              "status": "running", "start": start.isoformat(), "end": end.isoformat(),
              "minimum_magnitude": magnitude, "event_type": settings.event_type,
              "window_days": days, "resume": resume, "counts": empty_counts(),
              "windows_completed": 0, "windows_skipped": 0, "source_responses": 0,
              "http_attempts": 0, "archives": [], "batches": []}
    owned_client = client is None
    client = client or USGSClient(settings, retries=settings.max_retries, backoff=settings.retry_backoff)
    initial_attempts = client.attempts
    try:
        with staging_lock(root):
            run_dir = root / "runs" / run_id
            run_dir.mkdir(parents=True, exist_ok=False)
            report_path = run_dir / "report.json"
            report["report_path"] = str(report_path)
            atomic_json(report_path, report)
            try:
                state = load_state(root)
                complete = True
                for window_start, window_end in windows(start, end, size):
                    key = _checkpoint_key(window_start, window_end, magnitude, settings)
                    checkpoint = state["checkpoints"].get(key)
                    if resume and checkpoint:
                        for reference in checkpoint["archives"]:
                            verify_archive(reference)
                        for artifact in checkpoint["artifacts"]:
                            if hashlib.sha256(Path(artifact["path"]).read_bytes()).hexdigest() != artifact["sha256"]:
                                raise ValueError("Staging artifact checksum mismatch; cannot safely resume")
                        report["windows_skipped"] += 1
                        continue
                    if max_windows is not None and report["windows_completed"] >= max_windows:
                        complete = False
                        break
                    batch_dir = run_dir / key
                    batch_dir.mkdir()
                    counts = empty_counts()
                    eligible, rejected, references = [], [], []
                    try:
                        for features, reference, left, right in extract_window(
                                client, settings, window_start, window_end, magnitude, references, run_id):
                            counts["fetched"] += len(features)
                            for feature in features:
                                try:
                                    record = normalize_feature(feature)
                                except ValidationError as error:
                                    rejected.append(quarantine_entry(feature, error, run_id, reference))
                                    counts["rejected"] += 1
                                    continue
                                counts["accepted"] += 1
                                occurred = parse_utc(record["occurred_at"])
                                if (record["event_type"] != settings.event_type or not left <= occurred < right
                                    or (record["magnitude"] is not None and record["magnitude"] < magnitude)):
                                    counts["filtered"] += 1
                                    continue
                                eligible.append(record)
                        counts["eligible"] = len(eligible)
                        records, counts["deduplicated"] = deduplicate(eligible)
                        counts["unique_records"] = len(records)
                        catalog = dict(state["catalog"])
                        for record in records:
                            old = catalog.get(record["event_id"])
                            if old is None:
                                counts["inserted"] += 1
                                catalog[record["event_id"]] = record
                            elif prefer_incoming(old, record):
                                counts["updated"] += 1
                                catalog[record["event_id"]] = record
                            else:
                                counts["unchanged"] += 1
                        reconcile(counts)
                        normalized_path = batch_dir / "normalized.json"
                        quarantine_path = settings.quarantine_dir / run_id / (key + ".json")
                        atomic_json(normalized_path, records)
                        atomic_json(quarantine_path, rejected)
                        artifacts = [{"path": str(p.resolve()), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                                     for p in (normalized_path, quarantine_path)]
                        checkpoint = {"run_id": run_id, "completed_at": utc_now(), "counts": counts,
                                      "archives": references, "artifacts": artifacts}
                        updated_state = {**state, "catalog": catalog,
                                         "checkpoints": {**state["checkpoints"], key: checkpoint}}
                        # Catalog changes and resume checkpoint become visible together.
                        atomic_json(root / "state.json", updated_state)
                        state = updated_state
                        report["windows_completed"] += 1
                        report["batches"].append({"key": key, **checkpoint})
                        for name, value in counts.items():
                            report["counts"][name] += value
                        log.info("Committed UTC window %s -> %s: %s", window_start, window_end, counts)
                    finally:
                        report["archives"].extend(references)
                    atomic_json(report_path, report)
                report["status"] = "success" if complete else "limited"
                report["catalog_records"] = len(state["catalog"])
            except BaseException as error:
                report["status"] = "interrupted" if isinstance(error, (KeyboardInterrupt, SystemExit)) else "failed"
                report["error"] = {"type": type(error).__name__, "message": str(error)}
                raise
            finally:
                report["finished_at"] = utc_now()
                report["http_attempts"] = client.attempts - initial_attempts
                report["source_responses"] = len(report["archives"])
                atomic_json(report_path, report)
    finally:
        if owned_client:
            client.close()
    return report
