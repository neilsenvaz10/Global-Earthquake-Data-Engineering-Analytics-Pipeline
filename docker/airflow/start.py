"""Prepare local authentication without printing passwords, then start Airflow."""
import json
import os
from pathlib import Path
from urllib.parse import quote

for key in ("AIRFLOW_ADMIN_PASSWORD", "AIRFLOW_METADATA_PASSWORD", "AIRFLOW__API_AUTH__JWT_SECRET", "AIRFLOW__CORE__FERNET_KEY"):
    if not os.getenv(key):
        raise SystemExit("Missing local Airflow secret: " + key + "; run scripts/setup_airflow.py")

password_file = Path("/opt/airflow/auth_passwords.json")
password_file.write_text(json.dumps({"admin": os.environ["AIRFLOW_ADMIN_PASSWORD"]}), encoding="utf-8")
password_file.chmod(0o600)
password = quote(os.environ["AIRFLOW_METADATA_PASSWORD"], safe="")
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"postgresql+psycopg2://airflow:{password}@airflow-db:5432/airflow"
os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"] = str(password_file)
os.execvp("airflow", ["airflow", "standalone"])
