"""Create a separate SELECT-only login, keeping existing ETL privileges intact."""
from dataclasses import replace
import os
import secrets

from dotenv import dotenv_values
from sqlalchemy import text

from quaketrack.config import ROOT, load_settings
from quaketrack.dashboard.data import dashboard_engine
from quaketrack.dashboard.security import ensure_dashboard_role
from quaketrack.storage.database import create_database_engine
from quaketrack.storage.migrations import require_migrations


def main():
    path = ROOT / ".env"
    if not path.exists():
        raise ValueError("Configure the local .env first")
    settings = load_settings()
    role = os.getenv("DASHBOARD_DB_USER") or "quaketrack_dashboard"
    password = os.getenv("DASHBOARD_DB_PASSWORD")
    engine = create_database_engine(settings)
    try:
        require_migrations(engine)
        with engine.connect() as connection:
            exists = connection.execute(text("SELECT 1 FROM pg_roles WHERE rolname=:name"), {"name": role}).scalar()
        if exists and not password:
            raise ValueError("Existing dashboard role requires its existing password; it will not be rotated")
        password = password or secrets.token_urlsafe(32)
        result = ensure_dashboard_role(engine, role, password)
        reader = dashboard_engine(replace(settings, db_user=role, db_password=password))
        try:
            with reader.connect() as connection:
                assert connection.exec_driver_sql("SELECT current_user").scalar_one() == role
                connection.exec_driver_sql("SELECT COUNT(*) FROM earthquakes").scalar_one()
        except Exception:
            raise RuntimeError("Dashboard login verification failed; existing passwords were not changed") from None
        finally:
            reader.dispose()
        # Persist generated keys only. Preserve unrelated keys and existing credentials.
        current = dotenv_values(path)
        additions = {"DASHBOARD_DB_USER": role, "DASHBOARD_DB_PASSWORD": password}
        content = path.read_text(encoding="utf-8")
        for key, value in additions.items():
            if not current.get(key):
                lines = content.splitlines()
                replacement = key + "=" + value
                if any(line.startswith(key + "=") for line in lines):
                    lines = [replacement if line.startswith(key + "=") else line for line in lines]
                    content = "\n".join(lines) + "\n"
                else:
                    content = content.rstrip() + "\n" + replacement + "\n"
        path.write_text(content, encoding="utf-8")
        print(f"Dashboard role ready: {result['role']}; created={result['created']}; no password rotation; SELECT only")
    finally:
        engine.dispose()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Do not print driver errors or CREATE ROLE statements containing passwords.
        raise SystemExit(f"Dashboard setup failed ({type(error).__name__}); check configuration and administrator permissions") from None
