"""Real optional Spark tests; skipped on the normal host without PySpark."""
import json
from uuid import uuid4

import pytest

pytest.importorskip("pyspark")
from pyspark.sql.types import TimestampType, DoubleType
from quaketrack.spark_demo import session, schema, dataframe, aggregates, compare_results
from quaketrack.transforms.normalizer import normalize_feature


@pytest.fixture(scope="module")
def spark():
    instance = session()
    instance.sparkContext.setLogLevel("ERROR")
    yield instance
    instance.stop()


@pytest.fixture(scope="module")
def source():
    rows = []
    for i, (mag, depth) in enumerate(((-1, -2), (0, None), (2, 10), (4, 25), (6, 100), (None, 0))):
        row = normalize_feature({"type": "Feature", "id": "spark-test-" + str(i),
            "properties": {"time": 1577836800000 + (86400000 if i == 4 else 0), "type": "earthquake", "mag": mag},
            "geometry": {"type": "Point", "coordinates": [120, -10, depth]}})
        rows.append({**row, "last_run_id": str(uuid4())})
    rows.append({**rows[0], "event_id": "spark-test-blast", "event_type": "quarry blast"})
    return rows


@pytest.fixture(scope="module")
def results(spark, source):
    return {name: [json.loads(json.dumps(row.asDict(), default=str)) for row in frame.collect()]
            for name, frame in aggregates(dataframe(spark, source)).items()}


def test_explicit_schema_and_local_mode(spark, source):
    frame = dataframe(spark, source)
    assert frame.schema == schema() and not frame.schema["event_id"].nullable
    assert isinstance(frame.schema["occurred_at"].dataType, TimestampType)
    assert isinstance(frame.schema["magnitude"].dataType, DoubleType)
    assert frame.schema["magnitude"].nullable and frame.schema["source_updated_at"].nullable
    assert frame.count() == 7 and spark.sparkContext.master == "local[2]"
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_transformations_filter_categories_negatives_and_unknowns(results):
    categories = {row["magnitude_category"]: row["event_count"] for row in results["gold_magnitude_distribution"]}
    assert categories == {"<0": 1, "0-<2": 1, "2-<4": 1, "4-<6": 1, "6+": 1, "unknown": 1}
    depths = {row["magnitude_category"]: row for row in results["gold_depth_by_magnitude"]}
    assert depths["<0"]["average_depth_km"] == -2
    assert depths["0-<2"]["average_depth_km"] is None


def test_daily_aggregations_and_spark_sql(results):
    daily = sorted(results["gold_daily_earthquakes"], key=lambda row: row["event_day"])
    assert [row["event_count"] for row in daily] == [5, 1]
    assert daily[0]["known_magnitudes"] == 4 and daily[0]["average_magnitude"] == 1.25
    monthly = results["gold_monthly_earthquakes"][0]
    assert monthly["event_month"] == "2020-01-01" and monthly["event_count"] == 6
    assert monthly["average_magnitude"] == 2.2 and monthly["average_depth_km"] == 26.6


def test_empty_schema_and_invalid_inputs(spark, source):
    assert dataframe(spark, []).schema == schema()
    assert all(frame.count() == 0 for frame in aggregates(dataframe(spark, [])).values())
    with pytest.raises(ValueError, match="compliant Silver"):
        dataframe(spark, [source[0], source[0]])


def test_parity_compares_values_independently_of_collation(results):
    reordered = {name: list(reversed(rows)) for name, rows in results.items()}
    assert compare_results(results, reordered)
    changed = json.loads(json.dumps(results))
    changed["gold_daily_earthquakes"][0]["event_count"] += 1
    with pytest.raises(ValueError, match="mismatch"):
        compare_results(changed, results)
