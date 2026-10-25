"""Explicit, checksum-tracked SQL migrations, all pending files in one transaction."""
import hashlib
from pathlib import Path

from sqlalchemy import text

from ..config import ROOT

MIGRATION_DIR = ROOT / "sql/migrations"
MIGRATION_LOCK = 81743001


def apply_migrations(engine, directory: Path = MIGRATION_DIR):
    files = sorted(directory.glob("[0-9][0-9][0-9]_*.sql"))
    if not files:
        raise ValueError("No migration files found")
    names = [path.name for path in files]
    if len({name[:3] for name in names}) != len(names):
        raise ValueError("Migration version numbers must be unique")
    applied = []
    with engine.begin() as connection:
        connection.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": MIGRATION_LOCK})
        connection.exec_driver_sql("""CREATE TABLE IF NOT EXISTS schema_migrations (
            filename TEXT PRIMARY KEY, sha256 TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
        existing = dict(connection.execute(text("SELECT filename, sha256 FROM schema_migrations")).all())
        if set(existing) - set(names):
            raise ValueError("Database has migrations missing from this checkout")
        pending = []
        for path in files:
            raw = path.read_text(encoding="utf-8").encode("utf-8")
            digest = hashlib.sha256(raw).hexdigest()
            if path.name in existing:
                if existing[path.name] != digest:
                    raise ValueError(f"Applied migration changed: {path.name}")
            else:
                pending.append((path, raw, digest))
        if pending and existing and min(path.name for path, _, _ in pending) < max(existing):
            raise ValueError("Cannot insert a migration before an already applied version")
        for path, raw, digest in pending:
            connection.exec_driver_sql(raw.decode("utf-8"))
            connection.execute(text("INSERT INTO schema_migrations(filename, sha256) VALUES (:name, :sha)"),
                               {"name": path.name, "sha": digest})
            applied.append(path.name)
    return {"applied": applied, "current": names}


def require_migrations(engine):
    """Fail clearly on an unmigrated or incompatible DB; no DDL here."""
    with engine.connect() as connection:
        exists = connection.execute(text("SELECT to_regclass('public.schema_migrations')")).scalar_one()
        if exists is None:
            raise ValueError("Database is not migrated; run the migrate command first")
        rows = dict(connection.execute(text("SELECT filename, sha256 FROM schema_migrations")).all())
    expected = {p.name: hashlib.sha256(p.read_text(encoding="utf-8").encode("utf-8")).hexdigest()
                for p in MIGRATION_DIR.glob("[0-9][0-9][0-9]_*.sql")}
    if not expected or rows != expected:
        raise ValueError("Database migration checksums differ; review and run migrate")
