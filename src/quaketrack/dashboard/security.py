"""Explicit dashboard credentials and additive, repeatable least-privilege setup."""
from dataclasses import replace
import os
import re

from psycopg import sql

from ..config import load_settings

HEALTH_COLUMNS = ("run_id", "execution_key", "pipeline_name", "status", "stage", "quality_status",
    "started_at", "finished_at", "duration_seconds", "window_start", "window_end", "records_fetched",
    "records_inserted", "records_updated", "records_unchanged", "records_rejected", "api_failures",
    "database_failures", "quality_failures", "last_successful_window_end", "last_successful_ingestion_at",
    "coverage_lag_seconds")
READ_COLUMNS = {
    "earthquakes": None, "schema_migrations": None, "pipeline_state": None,
    "raw_archives": ("request_parameters", "chunk_id"), "pipeline_health": HEALTH_COLUMNS,
    "ingestion_chunks": ("status", "window_start", "window_end", "chunk_id"),
    "ingestion_runs": ("run_id", "job_type", "status", "started_at", "finished_at", "execution_key",
                       "records_fetched", "records_inserted", "records_updated", "records_unchanged", "records_rejected"),
    "gold_daily_earthquakes": None, "gold_monthly_earthquakes": None,
    "gold_magnitude_distribution": None, "gold_depth_by_magnitude": None,
}


def load_dashboard_settings(env_file=None):
    settings = load_settings() if env_file is None else load_settings(env_file)
    user = os.getenv("DASHBOARD_DB_USER", "")
    password = os.getenv("DASHBOARD_DB_PASSWORD", "")
    if not user or not password:
        raise ValueError("Dashboard credentials are missing; run scripts/setup_dashboard.py")
    if user == settings.db_user:
        raise ValueError("Dashboard and ETL must use separate database users")
    return replace(settings, db_user=user, db_password=password)


def ensure_dashboard_role(engine, role, password):
    """Never revoke privileges or rotate an existing role's password.

    An existing powerful role is refused rather than modified. Errors are sanitized
    because CREATE ROLE contains a password literal in the database protocol.
    """
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", role) or not password or role == engine.url.username:
        raise ValueError("Use a distinct lowercase dashboard role and a nonempty password")
    connection = engine.raw_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls,rolcanlogin FROM pg_roles WHERE rolname=%s", (role,))
            existing = cursor.fetchone()
            if existing:
                if any(existing[:5]) or not existing[5]:
                    raise ValueError("Existing role has unsafe attributes; choose a separate role")
                cursor.execute("SELECT 1 FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=%s)", (role,))
                if cursor.fetchone():
                    raise ValueError("Existing role has memberships; refusing to change it")
                cursor.execute("""SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                    WHERE n.nspname='public' AND c.relkind IN ('r','v','m','p') AND
                    (c.relowner=(SELECT oid FROM pg_roles WHERE rolname=%s) OR
                    has_table_privilege(%s,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')) LIMIT 1""", (role, role))
                if cursor.fetchone():
                    raise ValueError("Existing role has ownership or write privileges; refusing to revoke them")
            else:
                cursor.execute(sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD {}").format(sql.Identifier(role), sql.Literal(password)))
            cursor.execute("SELECT has_schema_privilege(%s,'public','CREATE'),has_database_privilege(%s,current_database(),'CREATE')", (role, role))
            if any(cursor.fetchone()):
                raise ValueError("Dashboard role can create permanent objects; inspect grants before proceeding")
            cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(engine.url.database), sql.Identifier(role)))
            cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(role)))
            for table, columns in READ_COLUMNS.items():
                selection = sql.SQL("") if columns is None else sql.SQL(" ({})").format(sql.SQL(",").join(map(sql.Identifier, columns)))
                cursor.execute(sql.SQL("GRANT SELECT{} ON public.{} TO {}").format(selection, sql.Identifier(table), sql.Identifier(role)))
            cursor.execute(sql.SQL("ALTER ROLE {} SET default_transaction_read_only=on").format(sql.Identifier(role)))
        connection.commit()
        return dict(role=role, created=existing is None, tables_or_views=len(READ_COLUMNS), password_rotated=False)
    except ValueError:
        connection.rollback()
        raise
    except Exception:
        connection.rollback()
        raise RuntimeError("Dashboard role setup failed; inspect database availability and administrator permissions") from None
    finally:
        connection.close()
