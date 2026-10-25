"""Offline tests: all HTTP responses here are explicitly synthetic mocks."""
import hashlib
import json
import logging
from dataclasses import replace
from unittest.mock import MagicMock
import pytest
import requests
from quaketrack.config import load_settings
from quaketrack.logging_config import configure_logging
from quaketrack.source_check import check_api
from quaketrack.storage.database import verify_database

@pytest.fixture
def settings(monkeypatch, tmp_path):
    import os
    for key in list(os.environ):
        if key.startswith(("USGS_", "POSTGRES_")) or key in ("RAW_ARCHIVE_DIR", "LOG_LEVEL"):
            monkeypatch.delenv(key)
    return load_settings(tmp_path / "missing.env")

def test_defaults_and_no_secret_repr(settings):
    assert settings.minimum_magnitude == 2.5
    assert settings.timeout == 30
    assert "private-value" not in repr(replace(settings, db_password="private-value"))
    with pytest.raises(ValueError, match="POSTGRES_PASSWORD"):
        _ = settings.database_url

def test_env_precedence(settings, monkeypatch, tmp_path):
    env = tmp_path / ".env"
    env.write_text("POSTGRES_PORT=5433\nPOSTGRES_DB=from_file\n")
    monkeypatch.setenv("POSTGRES_PORT", "5434")
    result = load_settings(env)
    assert result.db_port == 5434
    assert result.db_name == "from_file"

@pytest.mark.parametrize("key,value", [("POSTGRES_PORT", "0"), ("LOG_LEVEL", "BOGUS"),
    ("USGS_REQUEST_TIMEOUT_SECONDS", "nan"), ("USGS_REQUEST_TIMEOUT_SECONDS", "0"),
    ("USGS_MIN_MAGNITUDE", "inf"), ("USGS_API_BASE_URL", "http://example.com")])
def test_invalid_settings(settings, monkeypatch, tmp_path, key, value):
    monkeypatch.setenv(key, value)
    with pytest.raises(ValueError):
        load_settings(tmp_path / "absent")

def test_database_url_escapes_password(settings):
    url = replace(settings, db_password="p@ss:/?#").database_url
    assert url.password == "p@ss:/?#"
    assert "p@ss" not in str(url)

def mock_response(monkeypatch, payload):
    response = MagicMock()
    response.json.return_value = payload
    response.content = json.dumps(payload).encode()
    response.url = "https://earthquake.usgs.gov/fdsnws/event/1/query?mock=true"
    get = MagicMock(return_value=response)
    monkeypatch.setattr("quaketrack.source_check.requests.get", get)
    return response, get

@pytest.mark.parametrize("features", [[], [{"id": "mock-only", "properties": {
    "mag": None, "type": "earthquake", "time": 0},
    "geometry": {"coordinates": [10, 20, None]}}]])
def test_archive(settings, monkeypatch, tmp_path, features):
    response, get = mock_response(monkeypatch, {"type": "FeatureCollection", "features": features})
    directory, summary = check_api(settings, output_dir=tmp_path)
    raw = (directory / "response.geojson").read_bytes()
    metadata = json.loads((directory / "metadata.json").read_text())
    assert raw == response.content
    assert metadata["payload_sha256"] == hashlib.sha256(raw).hexdigest()
    assert metadata["record_count"] == len(features)
    assert metadata["complete_window"] is False
    assert summary["null_magnitudes"] == len(features)
    assert get.call_args.kwargs["timeout"] == 30
    assert get.call_args.kwargs["params"]["limit"] == 10
    second, _ = check_api(settings, output_dir=tmp_path)
    assert second != directory

@pytest.mark.parametrize("payload", [{}, [], {"type": "FeatureCollection", "features": [{}]}])
def test_bad_schema_not_archived(settings, monkeypatch, tmp_path, payload):
    mock_response(monkeypatch, payload)
    with pytest.raises(ValueError):
        check_api(settings, output_dir=tmp_path)
    assert not list(tmp_path.iterdir())

@pytest.mark.parametrize("error", [requests.Timeout(), requests.HTTPError(), ValueError("bad JSON")])
def test_source_failure(settings, monkeypatch, tmp_path, error):
    response, get = mock_response(monkeypatch, {})
    if isinstance(error, requests.Timeout):
        get.side_effect = error
    elif isinstance(error, requests.HTTPError):
        response.raise_for_status.side_effect = error
    else:
        response.json.side_effect = error
    with pytest.raises(type(error)):
        check_api(settings, output_dir=tmp_path)
    assert not list(tmp_path.iterdir())

def test_reversed_dates(settings, monkeypatch, tmp_path):
    _, get = mock_response(monkeypatch, {})
    with pytest.raises(ValueError):
        check_api(settings, "2026-10-02", "2026-10-01", tmp_path)
    get.assert_not_called()

@pytest.mark.parametrize("failed", [False, True])
def test_database_read_only_and_disposed(settings, monkeypatch, failed):
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.scalar_one.return_value = 1
    if failed:
        engine.connect.side_effect = RuntimeError("mock unavailable")
    monkeypatch.setattr("quaketrack.storage.database.create_engine", MagicMock(return_value=engine))
    configured = replace(settings, db_password="mock-password")
    if failed:
        with pytest.raises(RuntimeError):
            verify_database(configured)
    else:
        assert verify_database(configured)
        assert str(connection.execute.call_args.args[0]) == "SELECT 1"
    engine.dispose.assert_called_once()

def test_logging_utc(settings):
    configure_logging()
    formatter = logging.getLogger().handlers[0].formatter
    import time
    assert formatter.converter is time.gmtime
