"""Airflow *_CMD config helper: stdout is consumed by Airflow, never log it."""
import os
from urllib.parse import quote

password = os.environ.get("AIRFLOW_METADATA_PASSWORD")
if not password:
    raise SystemExit("Missing AIRFLOW_METADATA_PASSWORD; run setup_airflow.py")
print("postgresql+psycopg2://airflow:" + quote(password, safe="") + "@airflow-db:5432/airflow", end="")
