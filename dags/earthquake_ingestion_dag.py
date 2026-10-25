"""Three tasks, file-based handoff, isolated application dependencies."""
import json
import os
import subprocess
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

DAG_ID = "quaketrack_earthquake_ingestion"
APP_PYTHON = "/opt/quaketrack-venv/bin/python"


def app(*args):
    completed = subprocess.run([APP_PYTHON, "-m", "quaketrack.orchestration", *args],
                               check=False, text=True, capture_output=True)
    # Application logging is on stderr, with no credentials/configuration dump.
    if completed.stderr:
        print(completed.stderr)
    if completed.returncode:
        raise RuntimeError("Application stage failed; see preceding task logs")
    return json.loads(completed.stdout.strip().splitlines()[-1])


def record_failure(context):
    try:
        app("fail", "--execution-key", DAG_ID + ":" + context["run_id"])
    except Exception:
        print("Could not record Airflow failure in application DB; inspect task logs")


def determine_window(**context):
    conf = context["dag_run"].conf or {}
    allowed = {"start", "end", "min_magnitude", "simulate_failure_once"}
    if set(conf) - allowed:
        raise ValueError("Unknown DAG configuration key")
    args = ["plan", "--execution-key", DAG_ID + ":" + context["run_id"]]
    for key in ("start", "end", "min_magnitude"):
        if conf.get(key) is not None:
            args.extend(["--" + key.replace("_", "-"), str(conf[key])])
    if "end" not in conf and context.get("data_interval_end"):
        args.extend(["--end", context["data_interval_end"].isoformat()])
    if conf.get("simulate_failure_once") is True:
        args.append("--simulate-failure-once")
    return app(*args)["plan_path"]


def ingest_and_check(**context):
    path = context["ti"].xcom_pull(task_ids="determine_window")
    return app("run", "--plan", path)


def record_results(**context):
    result = context["ti"].xcom_pull(task_ids="ingest_and_check")
    if result["status"] != "success":
        raise ValueError("Ingestion did not finish successfully")
    print(json.dumps(result))
    return {"run_id": result["run_id"], "status": "success"}


schedule = os.getenv("PIPELINE_SCHEDULE", "0 * * * *")
with DAG(dag_id=DAG_ID, schedule=None if schedule.lower() == "none" else schedule,
         start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), catchup=False,
         max_active_runs=1, max_active_tasks=1, is_paused_upon_creation=True,
         dagrun_timeout=timedelta(hours=2), tags=["quaketrack", "earthquakes"],
         default_args={"retries": 2, "retry_delay": timedelta(seconds=int(os.getenv("AIRFLOW_TASK_RETRY_SECONDS", "60"))),
                       "retry_exponential_backoff": True, "max_retry_delay": timedelta(minutes=5),
                       "execution_timeout": timedelta(minutes=90)}) as dag:
    window = PythonOperator(task_id="determine_window", python_callable=determine_window)
    ingest = PythonOperator(task_id="ingest_and_check", python_callable=ingest_and_check,
                            on_failure_callback=record_failure, on_retry_callback=record_failure)
    result = PythonOperator(task_id="record_results", python_callable=record_results)
    window >> ingest >> result
