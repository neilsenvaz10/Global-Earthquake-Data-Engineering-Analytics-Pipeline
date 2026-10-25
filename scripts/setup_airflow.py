"""Append missing local Airflow secrets to ignored .env; never print values."""
import base64
import secrets
from pathlib import Path

from dotenv import dotenv_values

root = Path(__file__).resolve().parents[1]
path = root / ".env"
if not path.exists():
    raise SystemExit("Create .env from .env.example and configure PostgreSQL first")
values = dotenv_values(path)
generators = {"AIRFLOW_METADATA_PASSWORD": lambda: secrets.token_urlsafe(32),
              "AIRFLOW_ADMIN_PASSWORD": lambda: secrets.token_urlsafe(24),
              "AIRFLOW_JWT_SECRET": lambda: secrets.token_urlsafe(48),
              "AIRFLOW_FERNET_KEY": lambda: base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()}
content = path.read_text(encoding="utf-8")
for name, generate in generators.items():
    if not values.get(name):
        content += "\n" + name + "=" + generate()
        print("Configured " + name)
path.write_text(content.rstrip() + "\n", encoding="utf-8")
