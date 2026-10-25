"""Inside Airflow: one six-hour DAG test, with paused-state guards before and after.

docker compose exec -T airflow python /workspace/scripts/verify_phase5_airflow.py
This performs ingestion; it is an explicit verification command, not a unit test.
"""
import json
import argparse
import os
from pathlib import Path
import subprocess
from datetime import datetime, timezone

import requests


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-run", help="Verify an existing run ID without executing another run")
    args = parser.parse_args()
    base = "http://localhost:8080"
    dag_id = "quaketrack_earthquake_ingestion"
    token = requests.post(base + "/auth/token", json={"username": "admin",
                          "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]}, timeout=10)
    token.raise_for_status()
    headers = {"Authorization": "Bearer " + token.json()["access_token"]}

    def dag():
        response = requests.get(base + "/api/v2/dags/" + dag_id, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()

    assert dag()["is_paused"] is True, "Expected a paused DAG; refusing to change its state"
    assert os.getenv("ALLOW_FAILURE_SIMULATION", "false").lower() == "false"
    logical_date = datetime.now(timezone.utc).isoformat()
    conf = dict(start="2026-10-01T00:00:00Z", end="2026-10-01T06:00:00Z", min_magnitude=4)
    out = Path("/workspace/data/staging/phase5")
    out.mkdir(parents=True, exist_ok=True)
    if args.check_run:
        output = (out / "airflow-test.log").read_text(encoding="utf-8")
        returncode = 0
    else:
        result = subprocess.run(["airflow", "dags", "test", dag_id, logical_date,
                                 "--use-executor", "--conf", json.dumps(conf)], capture_output=True, text=True, timeout=360)
        output = result.stdout + result.stderr
        returncode = result.returncode
    # Never retain credentials even if an upstream dependency logs configuration.
    secrets = [os.environ.get(key, "") for key in ("POSTGRES_PASSWORD", "AIRFLOW_ADMIN_PASSWORD",
                "AIRFLOW_METADATA_PASSWORD", "AIRFLOW__API_AUTH__JWT_SECRET", "AIRFLOW__CORE__FERNET_KEY")]
    exposed = any(secret and secret in output for secret in secrets)
    for secret in secrets:
        if secret:
            output = output.replace(secret, "[REDACTED]")
    (out / "airflow-test.log").write_text(output, encoding="utf-8")
    assert dag()["is_paused"] is True, "DAG paused state changed unexpectedly"
    response = requests.get(base + "/api/v2/dags/" + dag_id + "/dagRuns",
                            params={"limit": 100, "order_by": "-run_after"}, headers=headers, timeout=10)
    response.raise_for_status()
    runs = response.json()["dag_runs"]
    matched = [run for run in runs if run.get("conf") == conf and
               (run["dag_run_id"] == args.check_run if args.check_run else
                run.get("logical_date") and datetime.fromisoformat(run["logical_date"].replace("Z", "+00:00")) == datetime.fromisoformat(logical_date))]
    assert returncode == 0 and matched and matched[0]["state"] == "success", "DAG test failed; inspect redacted local log"
    assert not exposed, "An upstream log contained a credential (redacted in saved output)"
    report = dict(is_paused=True, failure_simulation=False, conf=conf,
                  dag_run_id=matched[0]["dag_run_id"], state=matched[0]["state"],
                  execution="airflow dags test --use-executor (LocalExecutor)", credential_log_scan="passed")
    (out / "airflow-verification.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
