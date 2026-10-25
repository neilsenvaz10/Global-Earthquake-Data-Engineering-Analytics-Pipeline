# QuakeTrack

Global Earthquake Data Engineering & Analytics Pipeline. PROJECT.md is the full
specification. **Phase 1 is implemented and verified.** The API sample and PostgreSQL SELECT 1
checks succeeded. No ETL, schema, Airflow or dashboard exists yet.

## Local setup (PowerShell)

Use Python 3.11â€“3.14 and Docker Desktop with Linux containers, or an existing
PostgreSQL server. This implementation was tested with Python 3.14.6.

```powershell
python -m venv .venv
# If venv fails at ensurepip, the environment may still exist; bootstrap with:
# python -m pip --python .venv install pip
.\.venv\Scripts\python -m pip install -e ".[dev]"
Copy-Item .env.example .env
```

A local ignored `.env` was generated during verification with a random password
and `POSTGRES_PORT=55432` because Windows rejected port 5432. Keep that file for
this initialized database. On a fresh clone, copy the template, choose an available
port (55432 if 5432 is unavailable), and set a locally chosen `POSTGRES_PASSWORD` before starting PostgreSQL.
Never commit `.env`. No password is provided by this repository. Existing process
environment variables take precedence over `.env`. No activation is required.
If venv lacks pip, use `python -m pip --python .venv install -e ".[dev]"`.
For the exact resolved Phase 1 dependencies, run
`.\.venv\Scripts\python -m pip install -r requirements-lock.txt`, then install
the local package with `python -m pip install --no-deps -e .` using the venv Python.

```powershell
# Start Docker Desktop first, then:
docker info
docker compose up -d postgres
docker compose ps
.\.venv\Scripts\python scripts/verify_database.py
.\.venv\Scripts\python scripts/check_api.py --start 2026-10-01 --end 2026-10-02
.\.venv\Scripts\python -m pytest -q
```

The database check executes SELECT 1 only. API and database commands exit 1 on
failure and 0 on success. The API check requires internet access; pytest is offline
and uses explicitly mocked HTTP responses. The API check requests at most ten real
events and writes `data/raw/<run UUID>/response.geojson` plus `metadata.json`.
An empty source result is valid. The sample is not a full historical ingestion.

For an existing PostgreSQL server, skip Compose and set POSTGRES_HOST, POSTGRES_PORT,
POSTGRES_DB, POSTGRES_USER and POSTGRES_PASSWORD in `.env`. Inside future containers,
the database host will be `postgres`; local Python uses `localhost` by default.
Compose binds the database only to local loopback and persists data in a named volume.
Changing `.env` passwords after database initialization does not change stored roles;
use PostgreSQL administration to change existing credentials.

```powershell
# Stop services while retaining the database volume:
docker compose down
```

## Structure and documentation

- `src/quaketrack/`: configuration, UTC logging, sample source check, DB check.
- `scripts/`: executable connectivity commands.
- `tests/`: offline configuration, archive, failure and SQL smoke tests.
- [Implementation plan](docs/implementation_plan.md): five phases, audit and decisions.
- [Source contract v1](docs/source_contract.md): field semantics and API constraints.
- [Phase 1 report](docs/phase1_report.md): executed checks and remaining blockers.

Phase 2, after explicit approval, adds historical windowing, retries, normalization,
validation, quarantine and deduplication. No Git staging, commits or branches are
performed as part of this implementation.

If sandbox temporary-directory permissions prevent pytest from running, create
`.pytest_tmp` and use `python -m pytest -q --basetemp=.pytest_tmp/local` with the
venv Python. Use a fresh subdirectory for each run in restricted environments.
