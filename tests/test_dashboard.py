"""Offline presentation and semantic checks; no development database access."""
import csv
import io
from datetime import date, datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest
from sqlalchemy.exc import OperationalError
from streamlit.testing.v1 import AppTest

from quaketrack.config import ROOT
from quaketrack.dashboard import charts
from quaketrack.dashboard.app import PAGES, cached
from quaketrack.dashboard.data import Filters, csv_bytes, freshness

UTC = timezone.utc
NOW = datetime(2020, 1, 2, tzinfo=UTC)


def test_inclusive_dates_and_utc_offsets():
    filters = Filters.dates(date(2020, 1, 1), date(2020, 1, 1))
    assert filters.end - filters.start == timedelta(days=1)
    shifted = Filters(NOW.astimezone(timezone(timedelta(hours=5, minutes=30))), NOW + timedelta(days=1))
    assert shifted.sql()[1]["start"] == NOW


@pytest.mark.parametrize("kwargs", [dict(start=NOW, end=NOW), dict(start=NOW.replace(tzinfo=None), end=NOW),
    dict(start=NOW, end=NOW + timedelta(days=1), min_magnitude=3, max_magnitude=-1),
    dict(start=NOW, end=NOW + timedelta(days=1), min_depth=float("nan")),
    dict(start=NOW, end=NOW + timedelta(days=1), min_depth=1, max_depth=-1),
    dict(start=NOW, end=NOW + timedelta(days=1), location="a" * 201)])
def test_invalid_filters(kwargs):
    with pytest.raises(ValueError):
        Filters(**kwargs)


def test_filters_preserve_negatives_and_bind_literal_search():
    filters = Filters(NOW, NOW + timedelta(days=1), min_magnitude=-2, max_magnitude=-1,
                      min_depth=-3, max_depth=-1, location="%' OR 1=1 --_!")
    sql, params = filters.sql()
    assert "OR 1=1" not in sql
    assert params["magnitude_min"] == -2 and params["depth_km_min"] == -3
    assert params["location"] == "%!%' OR 1=1 --!_!!%"
    assert "IS NULL OR" in sql
    assert "IS NULL OR" not in Filters(NOW, NOW + timedelta(days=1), include_unknown=False).sql()[0]


def test_csv_utc_nulls_quoting_and_negatives():
    raw = csv_bytes([dict(event_id="a", occurred_at=NOW, magnitude=-1.2, depth_km=-2,
                          place_description='A, "B"\nC')])
    row = list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))))[0]
    assert row["occurred_at"] == "2020-01-02T00:00:00Z"
    assert row["magnitude"] == "-1.2" and row["depth_km"] == "-2"
    assert row["magnitude_type"] == "" and row["place_description"] == 'A, "B"\nC'
    assert len(list(csv.DictReader(io.StringIO(csv_bytes([]).decode("utf-8-sig"))))) == 0
    assert "2020-01-02T00:00:00.123000Z" in csv_bytes([dict(occurred_at=NOW + timedelta(milliseconds=123))]).decode("utf-8-sig")


def test_freshness():
    assert freshness(None, NOW) == "Unknown"
    assert freshness(NOW - timedelta(hours=2), NOW) == "2.0 hours ago"
    assert "future" in freshness(NOW + timedelta(seconds=1), NOW)
    with pytest.raises(ValueError):
        freshness(NOW.replace(tzinfo=None), NOW)


def test_chart_negative_null_and_invalid_coordinates():
    base = dict(occurred_at=NOW, magnitude=-1, depth_km=-2, latitude=0, longitude=0, place_description=None)
    rows = [base, {**base, "magnitude": None}, {**base, "latitude": 91}, {**base, "longitude": None}]
    figure = charts.earthquake_map(rows)
    assert len(figure.data) == 2
    assert figure.data[0].marker.size == (8,)
    assert figure.data[0].marker.color == (-1,)
    assert figure.data[0].customdata[0][2] == -2
    assert len(charts.scatter(rows).data[0].x) == 3
    assert charts.distribution([dict(bin_start=-1, events=1), dict(bin_start=None, events=2)], "magnitude").data[0].x == ("-1 to <-0.5", "Unknown")
    assert len(charts.earthquake_map([]).data) == 0
    assert len(charts.scatter([]).data[0].x) == 0


def sample_data(total=1):
    data = Mock()
    data.coverage.return_value = dict(total=total, first_event=NOW if total else None,
        last_event=NOW if total else None, min_magnitude=-1, max_magnitude=5, min_depth=-2, max_depth=40,
        days=1, source_filters=[dict(minimum_magnitude="-2", event_type="earthquake")])
    data.overview.return_value = dict(total=total, max_magnitude=-1, avg_magnitude=-1,
        avg_depth=-2, unknown_magnitude=0, unknown_depth=0, latest_event=NOW,
        latest_source_update=None, last_ingested_at=NOW)
    data.events.return_value = [dict(event_id="test-only", occurred_at=NOW, magnitude=-1,
        magnitude_type=None, depth_km=-2, latitude=1, longitude=2, place_description=None,
        event_status=None, source_url="https://earthquake.usgs.gov/earthquakes/eventpage/test-only")]
    data.timeline.return_value = [dict(period=NOW, events=total)] if total else []
    data.distribution.return_value = [dict(bin_start=-1, events=total)] if total else []
    data.export.return_value = csv_bytes(data.events.return_value)
    data.monitoring.return_value = dict(workflows=[], state=[], loads=[], last_success=None)
    return data


@pytest.mark.parametrize("page", PAGES)
@pytest.mark.parametrize("total", [0, 1])
def test_all_pages_and_empty_states(page, total):
    cached.clear()
    with patch("quaketrack.dashboard.app.get_data", return_value=sample_data(total)):
        app = AppTest.from_file(str(ROOT / "dashboard/app.py"), default_timeout=20).run()
        app.radio(key="page").set_value(page).run()
        assert not app.exception
        if total and page == "Explorer":
            assert len(app.get("download_button")) == 1
        if page == "Pipeline Monitoring":
            assert any("not live Airflow" in item.value for item in app.info)


def test_application_database_failure_is_safe():
    cached.clear()
    data = sample_data()
    data.coverage.side_effect = OperationalError("SECRET_DSN", {}, Exception("SECRET_PASSWORD"))
    with patch("quaketrack.dashboard.app.get_data", return_value=data):
        app = AppTest.from_file(str(ROOT / "dashboard/app.py")).run()
    assert not app.exception and "Database unavailable" in app.error[0].value
    assert "SECRET" not in app.error[0].value


def test_monitoring_metrics_use_workflow_parent_without_sensitive_error():
    cached.clear()
    data = sample_data()
    data.monitoring.return_value.update(workflows=[dict(status="success", duration_seconds=2.5,
        quality_status="passed", execution_key="test-only", started_at=NOW,
        records_fetched=12, records_inserted=1, records_updated=2, records_unchanged=9,
        records_rejected=0, error_message="SECRET_PASSWORD")], last_success=NOW)
    with patch("quaketrack.dashboard.app.get_data", return_value=data):
        app = AppTest.from_file(str(ROOT / "dashboard/app.py")).run()
        app.radio(key="page").set_value("Pipeline Monitoring").run()
    assert not app.exception
    metrics = {item.label: item.value for item in app.metric}
    assert metrics["Extracted"] == "12" and metrics["Unchanged"] == "9"
    assert "error_message" not in app.dataframe[0].value.columns


def test_application_filters_change_queries_and_validation():
    cached.clear()
    data = sample_data()
    with patch("quaketrack.dashboard.app.get_data", return_value=data):
        app = AppTest.from_file(str(ROOT / "dashboard/app.py")).run()
        app.text_input(key="location").set_value("Japan").run()
        assert data.overview.call_args.args[0].location == "Japan"
        app.checkbox(key="use_depth").check().run()
        app.number_input(key="min_depth").set_value(100).run()
        assert not app.exception and "Minimum" in app.error[0].value
