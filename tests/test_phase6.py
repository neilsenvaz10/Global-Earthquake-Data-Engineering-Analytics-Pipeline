"""Processed contracts and configuration; no database or Spark required."""
from copy import deepcopy
from uuid import uuid4

import pytest

from quaketrack.governance import load_contract, quality_report, validate_contract
from quaketrack.storage.repository import EVENT_FIELDS
from quaketrack.transforms.normalizer import normalize_feature
from quaketrack.dashboard.security import load_dashboard_settings


def record():
    return normalize_feature({"type": "Feature", "id": "test-only", "properties": {
        "time": 1577836800000, "updated": 1577836801000, "mag": -1, "type": "earthquake"},
        "geometry": {"type": "Point", "coordinates": [120, -10, -2]}})


def test_contract_matches_normalizer_and_preserves_negative_null_values():
    row = record()
    before = deepcopy(row)
    assert set(load_contract()["fields"]) == set(EVENT_FIELDS)
    assert validate_contract(row) == []
    assert row == before and row["magnitude"] == -1 and row["depth_km"] == -2
    row.update(magnitude=None, depth_km=None, source_updated_at=None)
    assert validate_contract(row) == []


@pytest.mark.parametrize("field,value,code", [("event_id", "", "required"), ("occurred_at", None, "required"),
    ("source", None, "required"), ("latitude", 91, "coordinate"), ("longitude", -181, "coordinate"),
    ("latitude", float("nan"), "coordinate"), ("magnitude", float("inf"), "invalid"),
    ("magnitude", "4.5", "invalid"), ("depth_km", True, "invalid"),
    ("occurred_at", "2020-01-01T00:00:00", "invalid"),
    ("occurred_at", "2020-01-01T01:00:00+01:00", "invalid"),
    ("occurred_at", "2020-01-01T00:00:00.000001Z", "invalid")])
def test_contract_invalid_processed_values(field, value, code):
    row = record()
    row[field] = value
    assert any(error["field"] == field and error["code"] == code for error in validate_contract(row))


def test_quality_report_denominator_duplicates_and_multiple_errors():
    rows = [record(), record(), {**record(), "event_id": "other", "latitude": 91, "source": None}]
    before = deepcopy(rows)
    report = quality_report(rows)
    assert report["total_records_checked"] == 3
    assert report["valid_records"] == 0 and report["invalid_records"] == 3
    assert report["duplicate_event_ids"] == 1 and report["duplicate_records"] == 1
    assert report["coordinate_validation_failures"] == 1 and report["missing_required_values"] == 1
    assert report["contract_compliance_percentage"] == 0 and rows == before
    assert quality_report([])["contract_compliance_percentage"] is None
    assert quality_report([record()])["contract_compliance_percentage"] == 100


def test_provenance_is_required_only_for_persisted_scope():
    assert validate_contract(record()) == []
    assert validate_contract(record(), require_provenance=True)[0]["code"] == "provenance"
    assert validate_contract({**record(), "last_run_id": str(uuid4())}, require_provenance=True) == []
    assert quality_report([None])["invalid_records"] == 1


def test_dashboard_configuration_never_falls_back_to_etl_credentials(monkeypatch):
    monkeypatch.setenv("DASHBOARD_DB_USER", "")
    monkeypatch.setenv("DASHBOARD_DB_PASSWORD", "")
    with pytest.raises(ValueError, match="Dashboard credentials"):
        load_dashboard_settings()
    monkeypatch.setenv("DASHBOARD_DB_USER", "quaketrack_reader_test")
    monkeypatch.setenv("DASHBOARD_DB_PASSWORD", "synthetic-test-secret")
    settings = load_dashboard_settings()
    assert settings.db_user == "quaketrack_reader_test" and settings.db_password == "synthetic-test-secret"
    assert "synthetic-test-secret" not in repr(settings)


def test_bronze_preservation_and_silver_source_consistency(tmp_path):
    import hashlib
    import json
    import requests
    from quaketrack.ingestion.raw_archive import archive_response
    from quaketrack.medallion import verify_layers
    feature = {"type": "Feature", "id": "bronze-test-only", "properties": {
        "time": 1577836800000, "type": "earthquake", "mag": -1},
        "geometry": {"type": "Point", "coordinates": [120, -10, -2]}}
    response = requests.Response()
    response.status_code = 200
    response.url = "https://earthquake.usgs.gov/test-only"
    response._content = json.dumps({"features": [feature]}).encode()
    reference = archive_response(tmp_path, str(uuid4()), response, {"test_only": True}, 1)
    from pathlib import Path
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in tmp_path.rglob('*') if path.is_file()}
    result = verify_layers([normalize_feature(feature)], tmp_path)
    assert result["bronze_responses_verified"] == result["silver_source_matches"] == 1
    assert before == {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in before}
    with pytest.raises(ValueError, match="Silver rows"):
        verify_layers([{**normalize_feature(feature), "magnitude": 9}], tmp_path)
    Path(reference["path"]).write_bytes(b"synthetic fixture tamper")
    with pytest.raises(ValueError, match="checksum"):
        verify_layers([], tmp_path)
