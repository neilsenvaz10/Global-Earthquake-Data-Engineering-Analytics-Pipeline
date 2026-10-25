"""Optional local[2] Spark job; never imported by normal ingestion or dashboard."""
import argparse
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import time
from uuid import uuid4

from .governance import quality_report


def session():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master("local[2]").appName("QuakeTrack optional transformation demo")
        .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false").config("spark.driver.memory", "512m").getOrCreate())


def schema():
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
    return StructType([StructField("event_id", StringType(), False),
        StructField("occurred_at", TimestampType(), False), StructField("source_updated_at", TimestampType(), True),
        StructField("magnitude", DoubleType(), True), StructField("depth_km", DoubleType(), True),
        StructField("latitude", DoubleType(), False), StructField("longitude", DoubleType(), False),
        StructField("event_type", StringType(), False)])


def dataframe(spark, records):
    check = quality_report(records, require_provenance=True)
    if check["invalid_records"]:
        raise ValueError("Spark accepts only compliant Silver input; inspect the quality report")
    rows = []
    for record in records:
        row = {name: record.get(name) for name in schema().fieldNames()}
        for field in ("occurred_at", "source_updated_at"):
            if row[field] is not None:
                row[field] = datetime.fromisoformat(str(row[field]).replace("Z", "+00:00"))
        for field in ("magnitude", "depth_km", "latitude", "longitude"):
            if row[field] is not None:
                row[field] = float(row[field])
        rows.append(row)
    return spark.createDataFrame(rows, schema=schema())


def aggregates(df):
    from pyspark.sql import functions as F
    selected = df.select("event_id", "occurred_at", "source_updated_at", "magnitude", "depth_km", "event_type")
    selected = selected.filter(F.col("event_type") == "earthquake")
    selected = selected.withColumn("event_day", F.to_date("occurred_at"))
    selected = selected.withColumn("magnitude_category", F.when(F.col("magnitude").isNull(), "unknown")
        .when(F.col("magnitude") < 0, "<0").when(F.col("magnitude") < 2, "0-<2")
        .when(F.col("magnitude") < 4, "2-<4").when(F.col("magnitude") < 6, "4-<6").otherwise("6+"))
    daily = selected.groupBy("event_day").agg(F.count("*").alias("event_count"),
        F.count("magnitude").alias("known_magnitudes"), F.avg("magnitude").alias("average_magnitude"),
        F.max("magnitude").alias("maximum_magnitude"), F.count("depth_km").alias("known_depths"),
        F.avg("depth_km").alias("average_depth_km"))
    categories = selected.groupBy("magnitude_category").agg(F.count("*").alias("event_count"))
    depths = selected.groupBy("magnitude_category").agg(F.count("*").alias("event_count"),
        F.count("depth_km").alias("known_depths"), F.avg("depth_km").alias("average_depth_km"))
    selected.createOrReplaceTempView("silver_earthquakes")
    # Spark SQL demonstrates the same monthly aggregation as the PostgreSQL view.
    monthly = df.sparkSession.sql("""SELECT CAST(date_trunc('month',occurred_at) AS DATE) AS event_month,
        COUNT(*) AS event_count, AVG(magnitude) AS average_magnitude,
        MAX(magnitude) AS maximum_magnitude, AVG(depth_km) AS average_depth_km
        FROM silver_earthquakes GROUP BY 1""")
    return dict(gold_daily_earthquakes=daily, gold_magnitude_distribution=categories,
                gold_monthly_earthquakes=monthly, gold_depth_by_magnitude=depths)


def compare_results(actual, expected):
    if set(actual) != set(expected):
        raise ValueError("Aggregation dataset names differ")
    for name, reference in expected.items():
        group = "event_day" if name == "gold_daily_earthquakes" else "event_month" if name == "gold_monthly_earthquakes" else "magnitude_category"
        rows = sorted(actual[name], key=lambda row: str(row[group]))
        reference = sorted(reference, key=lambda row: str(row[group]))
        if len(rows) != len(reference):
            raise ValueError(f"Aggregation row count differs: {name}")
        for a, b in zip(rows, reference):
            if set(a) != set(b):
                raise ValueError(f"Aggregation columns differ: {name}")
            for key in a:
                left, right = a[key], b[key]
                equal = math.isclose(left, right, rel_tol=1e-10, abs_tol=1e-10) if isinstance(left, (float, int)) and isinstance(right, (float, int)) else left == right
                if not equal:
                    raise ValueError(f"Spark/PostgreSQL mismatch: {name}.{key}")
    return True


def run(input_path, output_dir):
    raw = Path(input_path).read_bytes()
    payload = json.loads(raw)
    if len(payload["records"]) > 10000:
        raise ValueError("Spark demonstration input exceeds 10,000 rows")
    started = time.perf_counter()
    spark = session()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = dataframe(spark, payload["records"])
        frames = aggregates(df)
        # collect/count are actions; SQL aggregates stay bounded by the input cap.
        actual = {name: [json.loads(json.dumps(row.asDict(), default=str)) for row in frame.orderBy(frame.columns[0]).collect()]
                  for name, frame in frames.items()}
        compare_results(actual, payload["expected_postgresql"])
        report = dict(spark_version=spark.version, master=spark.sparkContext.master, timezone="UTC",
            schema=df.schema.jsonValue(), input_rows=df.count(), postgres_parity=True, results=actual,
            input_sha256=hashlib.sha256(raw).hexdigest(), elapsed_seconds=round(time.perf_counter()-started, 3))
        destination = Path(output_dir) / ("run-" + uuid4().hex)
        destination.mkdir(parents=True, exist_ok=False)
        (destination / "spark-report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps({**report, "report_path": str(destination / "spark-report.json")}, indent=2))
        return report
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    run(args.input, args.output_dir)


if __name__ == "__main__":
    main()
