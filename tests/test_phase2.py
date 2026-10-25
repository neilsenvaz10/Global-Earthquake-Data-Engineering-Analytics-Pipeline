"""Phase 2 tests use synthetic records and mocked HTTP only; never live USGS."""
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

from quaketrack.cli import main
from quaketrack.config import load_settings
from quaketrack.ingestion.raw_archive import verify_archive
from quaketrack.ingestion.usgs_client import USGSClient, retry_delay
from quaketrack.ingestion.windowing import parse_utc, source_times, split_window, windows
from quaketrack.pipeline import run_backfill
from quaketrack.storage.staging import load_state, staging_lock
from quaketrack.transforms.deduplicator import deduplicate, prefer_incoming
from quaketrack.transforms.normalizer import normalize_feature
from quaketrack.transforms.validator import ValidationError

START = "2020-01-01"
END = "2020-01-02"
EPOCH = 1577836800000


def feature(event_id="mock-a", at=EPOCH, updated=EPOCH + 1000):
    return {"type": "Feature", "id": event_id,
            "properties": {"time": at, "updated": updated, "mag": 3.5,
                           "magType": "ml", "place": "Mock region", "type": "earthquake",
                           "status": "reviewed", "net": "mock", "code": "a",
                           "ids": ",mock-a,", "sources": ",mock,",
                           "url": "https://example.invalid/mock-a", "detail": None},
            "geometry": {"type": "Point", "coordinates": [120.5, -10.25, 12.0]}}


def response(features=None, *, status=200, content=None, headers=None):
    result = requests.Response()
    result.status_code = status
    result.url = "https://earthquake.usgs.gov/fdsnws/event/1/query?mock=true"
    result.headers.update(headers or {})
    result._content = content if content is not None else json.dumps({
        "type": "FeatureCollection", "metadata": {"count": len(features or [])},
        "features": features or []}).encode()
    return result


@pytest.fixture
def settings(monkeypatch, tmp_path):
    import os
    for key in list(os.environ):
        if key.startswith(("USGS_", "POSTGRES_", "BACKFILL_")) or key in (
                "RAW_ARCHIVE_DIR", "STAGING_DIR", "QUARANTINE_DIR", "LOG_LEVEL"):
            monkeypatch.delenv(key)
    result = load_settings(tmp_path / "absent.env")
    return replace(result, archive_dir=tmp_path / "raw", staging_dir=tmp_path / "stage",
                   quarantine_dir=tmp_path / "quarantine", max_retries=0)


def client_for(settings, results):
    client = USGSClient(settings, retries=settings.max_retries, sleep=Mock())
    client.session.get = Mock(side_effect=results)
    return client


def execute(settings, client, **kwargs):
    return run_backfill(settings, START, END, client=client, **kwargs)


def test_windows_boundaries_and_offset():
    start = parse_utc("2020-02-28T05:30:00+05:30")
    end = parse_utc("2020-03-02")
    result = list(windows(start, end, timedelta(days=1)))
    assert len(result) == 3  # leap day
    assert result[0][0] == parse_utc("2020-02-28")
    assert all(a[1] == b[0] for a, b in zip(result, result[1:]))
    assert result[-1][1] == end
    query = source_times(*result[0])
    assert query["endtime"] == "2020-02-28T23:59:59.999"


@pytest.mark.parametrize("start,end,seconds", [(END, START, 1), (START, START, 1),
    (START, END, 0), (START, END, -1), (START, END, 0.0001)])
def test_invalid_windows(start, end, seconds):
    with pytest.raises(ValueError):
        list(windows(parse_utc(start), parse_utc(end), timedelta(seconds=seconds)))


def test_millisecond_precision_and_split():
    with pytest.raises(ValueError):
        parse_utc("2020-01-01T00:00:00.000001Z")
    start, end = parse_utc(START), parse_utc(END)
    left, right = split_window(start, end)
    assert left[0] == start and left[1] == right[0] and right[1] == end
    with pytest.raises(ValueError, match="cannot guarantee completeness"):
        split_window(start, start + timedelta(milliseconds=1))


def test_normalization_and_nulls():
    raw = feature()
    actual = normalize_feature(raw)
    assert actual["longitude"] == 120.5 and actual["latitude"] == -10.25
    assert actual["occurred_at"] == "2020-01-01T00:00:00.000+00:00"
    assert actual["source_updated_at"] == "2020-01-01T00:00:01.000+00:00"
    assert actual["magnitude_type"] == "ml" and actual["event_status"] == "reviewed"
    assert actual["source"] == "USGS" and actual["network"] == "mock"
    assert actual == normalize_feature(raw)
    raw["properties"] = {"time": EPOCH, "type": "earthquake"}
    raw["geometry"]["coordinates"] = [-180, 90]
    result = normalize_feature(raw)
    assert result["depth_km"] is None and result["magnitude"] is None
    assert result["source_updated_at"] is None and result["place_description"] is None


def test_negative_values_and_epoch_zero():
    raw = feature(at=0, updated=0)
    raw["properties"]["mag"] = -1.5
    raw["geometry"]["coordinates"][2] = -2.0
    result = normalize_feature(raw)
    assert result["magnitude"] == -1.5 and result["depth_km"] == -2
    assert result["source_updated_at"] == "1970-01-01T00:00:00.000+00:00"


@pytest.mark.parametrize("path,value", [
    (("id",), " "), (("id",), None), (("id",), 123),
    (("properties", "time"), None), (("properties", "time"), "bad"),
    (("properties", "time"), 1e30), (("properties", "time"), 1.5),
    (("properties", "time"), True), (("properties", "updated"), "bad"),
    (("properties", "mag"), "bad"), (("properties", "mag"), "nan"),
    (("properties", "mag"), True), (("properties", "place"), {}),
    (("geometry", "coordinates"), [181, 0, 0]),
    (("geometry", "coordinates"), [0, -91, 0]),
    (("geometry", "coordinates"), [None, 0, 0]),
    (("geometry", "coordinates"), [0, 0, "bad"]),
    (("geometry", "coordinates"), [0, 0, float("inf")]),
    (("geometry", "type"), "Polygon"), (("properties",), None),
])
def test_invalid_records(path, value):
    raw = feature()
    target = raw
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(ValidationError):
        normalize_feature(raw)


@pytest.mark.parametrize("raw", [None, [], "bad", 0, {}])
def test_malformed_feature(raw):
    with pytest.raises(ValidationError):
        normalize_feature(raw)


def test_revision_precedence_and_deterministic_ties():
    old = normalize_feature(feature(updated=1000))
    new = normalize_feature(feature(updated=2000))
    missing = normalize_feature(feature(updated=None))
    assert prefer_incoming(old, new)
    assert not prefer_incoming(new, old)
    assert prefer_incoming(missing, old)
    assert not prefer_incoming(old, missing)
    assert not prefer_incoming(missing, missing)
    assert deduplicate([old, new, old])[0] == [new]
    assert deduplicate([old, new, old])[1] == 2
    tie = {**new, "magnitude": 9.0}
    assert deduplicate([new, tie]) == deduplicate([tie, new])


def test_retry_timeout_and_server_error(settings, tmp_path):
    client = client_for(replace(settings, max_retries=2), [
        requests.Timeout(), response(status=503, headers={"Retry-After": "2"}), response([])])
    report = execute(settings, client)
    assert report["status"] == "success" and report["http_attempts"] == 3
    assert client.sleep.call_count == 2
    assert client.sleep.call_args.args[0] >= 2
    assert len(report["archives"]) == 2  # timeout had no response body
    assert client.session.get.call_args.kwargs["timeout"] == settings.timeout


def test_retry_budget_and_permanent_errors(settings):
    client = client_for(replace(settings, max_retries=1), [response(status=429), response(status=429)])
    with pytest.raises(requests.HTTPError):
        execute(settings, client)
    assert client.attempts == 2
    client = client_for(settings, [response(status=400)])
    with pytest.raises(requests.HTTPError):
        execute(settings, client)
    assert client.attempts == 1
    assert not (settings.staging_dir / "state.json").exists()


def test_retry_after_date_and_excessive_wait(settings):
    from email.utils import format_datetime
    future = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=20))
    assert retry_delay(future, 0, 0) >= 18
    client = client_for(replace(settings, max_retries=1), [response(status=429, headers={"Retry-After": "600"})])
    with pytest.raises(RuntimeError, match="retry the run later"):
        execute(settings, client)
    client.sleep.assert_not_called()


@pytest.mark.parametrize("content", [b"not json", b'[]', b'{"type":"FeatureCollection","features":{}}',
    b'{"type":"FeatureCollection","features":[],"metadata":{"count":2}}',
    b'{"type":"FeatureCollection","features":[],"metadata":[]}',
    b'{"type":"FeatureCollection","features":[NaN]}'])
def test_bad_response_archived_and_no_commit(settings, content):
    client = client_for(settings, [response(content=content)])
    with pytest.raises(ValueError):
        execute(settings, client)
    assert not (settings.staging_dir / "state.json").exists()
    assert next(settings.archive_dir.rglob("response.geojson")).read_bytes() == content
    report = json.loads(next(settings.staging_dir.rglob("report.json")).read_text())
    assert report["status"] == "failed" and report["source_responses"] == 1


def test_empty_response(settings):
    client = client_for(settings, [response(status=204, content=b"")])
    report = execute(settings, client)
    assert report["counts"]["fetched"] == 0
    assert report["windows_completed"] == 1 and report["status"] == "success"


def test_adaptive_split_no_truncation(settings):
    settings = replace(settings, request_limit=2)
    a, b = feature("mock-a"), feature("mock-b", at=EPOCH + 12 * 3600 * 1000)
    client = client_for(settings, [response([a, b]), response([a]), response([b])])
    report = execute(settings, client)
    assert report["http_attempts"] == 3
    assert report["counts"]["fetched"] == 2 and report["counts"]["inserted"] == 2
    calls = client.session.get.call_args_list
    assert calls[1].kwargs["params"]["endtime"] == "2020-01-01T11:59:59.999"
    assert calls[2].kwargs["params"]["starttime"] == "2020-01-01T12:00:00"
    assert len(report["archives"]) == 3


def test_unsplittable_fails_instead_of_truncating(settings):
    settings = replace(settings, request_limit=1)
    client = client_for(settings, [response([feature()])])
    with pytest.raises(ValueError, match="cannot guarantee completeness"):
        run_backfill(settings, START, "2020-01-01T00:00:00.001Z", client=client)
    assert not (settings.staging_dir / "state.json").exists()


def test_quarantine_filter_dedup_counts_and_raw_integrity(settings):
    invalid = feature("mock-bad")
    invalid["geometry"]["coordinates"][0] = 181
    other = feature("mock-blast")
    other["properties"]["type"] = "quarry blast"
    rows = [feature(), feature(updated=EPOCH + 2000), invalid, None, other,
            feature("mock-outside", at=EPOCH + 86400000)]
    client = client_for(settings, [response(rows)])
    report = execute(settings, client)
    counts = report["counts"]
    assert counts == dict(fetched=6, accepted=4, rejected=2, filtered=2, eligible=2,
                         deduplicated=1, unique_records=1, inserted=1, updated=0, unchanged=0)
    for reference in report["archives"]:
        verify_archive(reference)
    quarantine = json.loads(next(settings.quarantine_dir.rglob("*.json")).read_text())
    assert len(quarantine) == 2
    assert quarantine[0]["raw_event"] == invalid
    assert quarantine[0]["reason_code"] == "invalid_longitude"
    assert quarantine[0]["run_id"] == report["run_id"]
    assert quarantine[0]["source"] == "USGS" and quarantine[0]["ingested_at"]
    assert quarantine[1]["raw_event"] is None


def test_repeat_newer_older_and_missing_revision(settings):
    reports = []
    for update, magnitude in [(1000, 3), (1000, 3), (2000, 4), (1000, 8), (None, 9)]:
        row = feature(updated=update)
        row["properties"]["mag"] = magnitude
        reports.append(execute(settings, client_for(settings, [response([row])])))
    assert [r["counts"]["inserted"] for r in reports] == [1, 0, 0, 0, 0]
    assert [r["counts"]["updated"] for r in reports] == [0, 0, 1, 0, 0]
    assert [r["counts"]["unchanged"] for r in reports] == [0, 1, 0, 1, 1]
    catalog = load_state(settings.staging_dir)["catalog"]
    assert len(catalog) == 1 and catalog["mock-a"]["magnitude"] == 4
    assert len(list(settings.archive_dir.rglob("response.geojson"))) == 5


def test_interrupt_then_resume_only_uncommitted_window(settings):
    client = client_for(settings, [response([feature()]), KeyboardInterrupt()])
    with pytest.raises(KeyboardInterrupt):
        run_backfill(settings, START, "2020-01-03", client=client)
    state = load_state(settings.staging_dir)
    assert len(state["checkpoints"]) == 1 and len(state["catalog"]) == 1
    first_report = json.loads(next(settings.staging_dir.rglob("report.json")).read_text())
    assert first_report["status"] == "interrupted"
    second = client_for(settings, [response([feature("mock-day2", at=EPOCH + 86400000)])])
    report = run_backfill(settings, START, "2020-01-03", client=second, resume=True)
    assert report["windows_skipped"] == 1 and report["windows_completed"] == 1
    assert second.attempts == 1 and report["catalog_records"] == 2
    skipped = run_backfill(settings, START, "2020-01-03", client=client_for(settings, []), resume=True)
    assert skipped["windows_skipped"] == 2 and skipped["http_attempts"] == 0


def test_limited_run_resume_and_config_identity(settings):
    first = run_backfill(settings, START, "2020-01-03", max_windows=1,
                         client=client_for(settings, [response([])]))
    assert first["status"] == "limited" and first["windows_completed"] == 1
    second = run_backfill(settings, START, "2020-01-03", resume=True,
                          client=client_for(settings, [response([])]))
    assert second["windows_skipped"] == 1 and second["windows_completed"] == 1
    different = execute(settings, client_for(settings, [response([])]), resume=True, minimum_magnitude=4)
    assert different["windows_skipped"] == 0


@pytest.mark.parametrize("target", ["raw", "metadata", "normalized", "quarantine"])
def test_resume_refuses_damaged_artifacts(settings, target):
    first = execute(settings, client_for(settings, [response([feature()])]))
    batch = first["batches"][0]
    paths = {"raw": batch["archives"][0]["path"], "metadata": batch["archives"][0]["metadata_path"],
             "normalized": batch["artifacts"][0]["path"], "quarantine": batch["artifacts"][1]["path"]}
    Path(paths[target]).write_text("{}")
    with pytest.raises((ValueError, KeyError)):
        execute(settings, client_for(settings, []), resume=True)


def test_failed_state_commit_does_not_advance_checkpoint(settings, monkeypatch):
    import quaketrack.pipeline as pipeline
    original = pipeline.atomic_json
    def fail_commit(path, value):
        if path.name == "state.json":
            raise OSError("mock interrupted atomic commit")
        original(path, value)
    monkeypatch.setattr(pipeline, "atomic_json", fail_commit)
    with pytest.raises(OSError):
        execute(settings, client_for(settings, [response([feature()])]))
    assert not (settings.staging_dir / "state.json").exists()
    monkeypatch.setattr(pipeline, "atomic_json", original)
    report = execute(settings, client_for(settings, [response([feature()])]), resume=True)
    assert report["counts"]["inserted"] == 1


def test_writer_lock_released(settings):
    with staging_lock(settings.staging_dir):
        with pytest.raises(RuntimeError, match="Another ETL"):
            with staging_lock(settings.staging_dir):
                pass
    with staging_lock(settings.staging_dir):
        pass


def test_invalid_newer_revision_cannot_replace_valid(settings):
    execute(settings, client_for(settings, [response([feature(updated=1000)])]))
    bad = feature(updated=2000)
    bad["properties"]["mag"] = "not-a-number"
    report = execute(settings, client_for(settings, [response([bad])]))
    assert report["counts"]["rejected"] == 1 and report["counts"]["updated"] == 0
    assert load_state(settings.staging_dir)["catalog"]["mock-a"]["magnitude"] == 3.5


def test_atomic_replace_failure_keeps_old_file(tmp_path, monkeypatch):
    import quaketrack.storage.staging as staging
    path = tmp_path / "state.json"
    staging.atomic_json(path, {"old": True})
    monkeypatch.setattr(staging.os, "replace", Mock(side_effect=OSError("mock rename failure")))
    with pytest.raises(OSError):
        staging.atomic_json(path, {"new": True})
    assert json.loads(path.read_text()) == {"old": True}
    assert not list(tmp_path.glob("*.tmp"))


def test_partial_window_and_nonrounded_threshold(settings):
    start = parse_utc(START)
    end = parse_utc("2020-01-02T06:00:00Z")
    generated = list(windows(start, end, timedelta(days=1)))
    assert generated[-1] == (parse_utc(END), end)
    client = client_for(settings, [response([])])
    execute(settings, client, minimum_magnitude=2.123456789)
    assert client.session.get.call_args.kwargs["params"]["minmagnitude"] == "2.123456789"


@pytest.mark.parametrize("key,value", [("USGS_MAX_RETRIES", "-1"), ("USGS_MAX_RETRIES", "11"),
    ("USGS_REQUEST_LIMIT", "0"), ("USGS_REQUEST_LIMIT", "20001"),
    ("USGS_RETRY_BACKOFF_SECONDS", "nan"), ("BACKFILL_WINDOW_DAYS", "0")])
def test_phase2_configuration_validation(settings, monkeypatch, tmp_path, key, value):
    monkeypatch.setenv(key, value)
    with pytest.raises(ValueError):
        load_settings(tmp_path / "absent")


def test_cli_failure_and_arguments(settings, monkeypatch, capsys):
    import quaketrack.cli as cli
    monkeypatch.setattr(cli, "load_settings", lambda: settings)
    assert main(["backfill", "--start", END, "--end", START]) == 1
    def fake_run(*args, **kwargs):
        assert kwargs["resume"] and kwargs["minimum_magnitude"] == 4
        assert kwargs["window_days"] == 0.5 and kwargs["max_windows"] == 1
        return {"status": "limited"}
    monkeypatch.setattr(cli, "run_backfill", fake_run)
    assert main(["backfill", "--start", START, "--end", END, "--resume", "--min-magnitude", "4",
                 "--window-days", ".5", "--max-windows", "1"]) == 0
    assert '"limited"' in capsys.readouterr().out
