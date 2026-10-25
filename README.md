# QuakeTrack

Global Earthquake Data Engineering & Analytics Pipeline. `PROJECT.md` is the primary
specification. **Phases 1–5 are implemented and locally verified.** Historical extraction, validation,
quarantine, repeatable staging, PostgreSQL loading, migrations and analytical SQL
work. Airflow orchestration, incremental coverage and monitoring are verified.
The Streamlit dashboard provides five PostgreSQL-backed pages with shared filters and CSV export.

## Local setup (PowerShell)

Use Python 3.11-3.14. Tested here with Python 3.14.6. Airflow runs separately in its Python 3.12 Linux container.

```powershell
python -m venv .venv
# If venv fails at ensurepip but creates the environment, bootstrap with:
# python -m pip --python .venv install pip
.\.venv\Scripts\python -m pip install -e ".[dev,dashboard]"
# Fresh clones only; preserve an existing .env:
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

The existing ignored `.env` has a generated password and `POSTGRES_PORT=55432`.
Keep this file for the initialized database. For a fresh clone, choose a password
and keep POSTGRES_PORT=55432 before starting PostgreSQL. Process environment variables override
`.env`. Never commit credentials. Relative configured data paths resolve against
the project root; a CLI `--output-dir` resolves against the current working directory.

For the resolved dependency set, install `requirements-lock.txt`, then install
this local package with `--no-deps -e .`, using the venv Python. If venv has no pip,
use `python -m pip --python .venv install -e ".[dev]"` from the host.

## Phase 1 connectivity checks

```powershell
# Start Docker Desktop with Linux containers first:
docker compose up -d --wait postgres
.\.venv\Scripts\python scripts/verify_database.py
.\.venv\Scripts\python scripts/check_api.py --start 2026-10-01 --end 2026-10-02
```

The database check runs SELECT 1 without creating tables. The API connectivity
sample is capped at ten events; its metadata labels it as an incomplete window.
It is separate from Phase 2's complete-window extraction. Both commands exit 1
on failure. Phase 2 itself does not require a running database or DB credentials.

## Phase 2 historical ETL

A bounded real-data demonstration (one day, magnitude 4+, two half-day windows):

```powershell
.\.venv\Scripts\python -m quaketrack.cli backfill --start 2026-10-01 --end 2026-10-02 --min-magnitude 4 --window-days 0.5 --output-dir data/staging/demo
# Verify archived artifacts and skip already committed matching windows:
.\.venv\Scripts\python -m quaketrack.cli backfill --start 2026-10-01 --end 2026-10-02 --min-magnitude 4 --window-days 0.5 --output-dir data/staging/demo --resume
```

Dates/times are UTC by default; explicit offsets are converted to UTC. Start is
inclusive and end is exclusive. Millisecond-aligned timestamps are supported. USGS
uses inclusive bounds, so each API request ends one millisecond before the next
window. An exact response cap triggers smaller requests, never silent truncation.
At an unsplittable one-millisecond cap, the command fails visibly.

A limited run processes complete windows; it is not a dry run and does write data:

```powershell
.\.venv\Scripts\python scripts/run_backfill.py --start 2026-10-01 --end 2026-10-03 --min-magnitude 4 --max-windows 1 --output-dir data/staging/limited-demo
# Continue with the same output directory and options plus --resume.
```

A limited run reports `status=limited` and exits 0; failures exit 1, keyboard
interrupts exit 130. Choose `--max-windows` to bound initial work rather than
claiming a full backfill. `--window-days` defaults to 1; minimum magnitude defaults
to 2.5. Requests default to a 1,000-record cap, 30-second timeout and three retries.
Inspect `.env.example` for window, retry, archive, quarantine and staging settings.
A new output directory gives a separate local catalog; the same directory preserves
cross-run deduplication. Without `--resume`, matching windows are fetched again to
observe source revisions. With it, verified completed windows are reused and do
not refresh the source. Changing filters or window boundaries requires new work.

### Outputs

- `data/raw/<run_id>/<request_hash>/<response_id>/`: original response bytes and
  request metadata, UTC fetch time, HTTP status, retry attempt and SHA-256.
- `data/quarantine/<run_id>/<window_key>.json`: invalid original records with
  validation reason, timestamp, run ID and source archive reference.
- `<output-dir>/state.json`: local catalog keyed by USGS ID and committed window
  checkpoints, replaced atomically together after staging artifacts are written.
- `<output-dir>/runs/<run_id>/report.json`: status, committed counts, archives,
  request attempts, skipped windows, errors and timestamps.
- `<output-dir>/runs/<run_id>/<window_key>/normalized.json`: deduplicated normalized
  records for that window. `state.json` contains the current unique catalog.

The report's inserted/updated/unchanged counters refer to **local staging**, not
PostgreSQL. Null measurements stay null; negative magnitudes/depths are permitted.
Known newer source updates win. Missing/older/equal update timestamps cannot replace
an existing record. Details and tie policies are in [ETL operations](docs/etl_operations.md).

## Tests and verification

```powershell
New-Item -ItemType Directory -Force .pytest_tmp | Out-Null
.\.venv\Scripts\python -m pytest -q --basetemp=.pytest_tmp/local-check
.\.venv\Scripts\python -m pip check
.\.venv\Scripts\python -m compileall -q src scripts tests
```

Use a fresh basetemp subdirectory in restricted environments. Tests are offline;
all artificial records are explicitly labeled test mocks. Live verification fetched
37 valid records for the example range; repeat and resume checks passed. This is
not evidence that a 12-month backfill has run. See [Phase 2 report](docs/phase2_report.md).

## Phase 3 PostgreSQL persistence

Apply explicit migrations, then load the already verified Phase 2 sample:

```powershell
.\.venv\Scripts\python -m quaketrack.cli migrate
.\.venv\Scripts\python -m quaketrack.cli load-staging --staging-dir data/staging/phase2-live
# Database resume checks DB success independently of extraction checkpoints:
.\.venv\Scripts\python -m quaketrack.cli load-staging --staging-dir data/staging/phase2-live --resume
# Rebuild and load from raw archives with no API requests:
.\.venv\Scripts\python -m quaketrack.cli replay --staging-dir data/staging/phase2-live
.\.venv\Scripts\python -m quaketrack.cli analytics overview --start 2026-10-01 --end 2026-10-02
```

For a fresh clone, run a Phase 2 backfill first; ignored sample files are not shipped.
To stage and load in one command after migration:

```powershell
.\.venv\Scripts\python -m quaketrack.cli backfill --start 2026-10-01 --end 2026-10-02 --min-magnitude 4 --window-days 0.5 --output-dir data/staging/demo --load-database
```

Add --resume to skip verified extraction and successful DB chunks independently.
Without it, repeated DB loading reports unchanged events and keeps their existing
timestamps. Newer valid source revisions update rows; stale revisions cannot regress
them. Loading is transactional per window. No schema is created on application startup.
`state.json` and raw archives are needed for replay; normalized output files are not.

Integration tests explicitly create an isolated test DB; development is never reset:

```powershell
.\.venv\Scripts\python -m pytest --database-tests -q --basetemp=.pytest_tmp/db-check
```

The role needs CREATEDB to run those tests. Phase 4 originally passed 125 tests. The Phase 5 totals and verification evidence are in the final report. Real development
loading inserted 37 events; repeat and offline replay each reported 37 unchanged.

See [database schema and operations](docs/database.md) for tables, constraints,
indexes, count definitions, all query names, test isolation and recovery. The
[Phase 3 report](docs/phase3_report.md) contains the executed verification evidence.

## Database service and later phases

Keep PostgreSQL on localhost:55432 in this checkout. Compose binds only loopback
and persists its named volume. `docker compose down` stops it without deleting data.
Changing `.env` does not rotate existing PostgreSQL passwords. Use administration
commands for role changes. For an existing database, configure its host/port and
credentials instead of starting Compose. Future containers will use host `postgres`.

Phases 1–5 are complete for the bounded local college demonstration.
No ML or prediction is included.

- [Implementation plan](docs/implementation_plan.md)
- [Source contract](docs/source_contract.md)
- [ETL operations and recovery](docs/etl_operations.md)
- [Phase 1 report](docs/phase1_report.md)
- [Phase 2 report](docs/phase2_report.md)

- [Database schema and operations](docs/database.md)
- [Phase 3 report](docs/phase3_report.md)

## Phase 4 Airflow

The pinned Linux Airflow 3.3.2 runtime uses LocalExecutor and its own PostgreSQL
metadata DB. The application DB stays on localhost:55432. The UI is available at
http://localhost:18080 (admin; password is AIRFLOW_ADMIN_PASSWORD in local .env).

```powershell
.\.venv\Scripts\python scripts/setup_airflow.py
.\.venv\Scripts\python -m quaketrack.cli migrate
docker compose --profile airflow build airflow
docker compose --profile airflow up -d airflow
# Keep the DAG paused. Do not enable recurring ingestion without explicit approval.
# Stop Airflow services without deleting any volumes or stopping the app DB:
docker compose --profile airflow stop airflow airflow-db
```

The DAG is left paused after verification; its hourly schedule is configured.
catchup=False prevents historical scheduler backfill. First automatic coverage is
6 hours, followed by a configurable overlap up to 72 hours and bounded catch-up.
Only successful PostgreSQL loading PLUS post-load quality advances pipeline_state.
Manual historical test windows do not change automatic coverage. Failure simulation
is disabled. Do not delete existing database volumes.

See [Airflow operations](docs/airflow.md) for bounded manual triggering, task logs,
retries, restart safety, all settings and troubleshooting. Six real DAG runs succeeded;
one controlled quality failure recovered on its second attempt without refetching.
Three current real events were inserted, bringing the total to 40; the original
37 were unchanged. Application tests passed 125 cases plus 3 real DAG checks.

- [Phase 4 report](docs/phase4_report.md)

## Phase 5 dashboard and local deployment

Open [QuakeTrack](http://localhost:8501) and [Airflow](http://localhost:18080).
The application database remains PostgreSQL on `localhost:55432`. Docker Desktop
must use Linux containers. Preserve the existing `.env` and named volumes.

After the environment setup above, start the complete local demo:

```powershell
docker compose up -d --wait postgres
.\.venv\Scripts\python -m quaketrack.cli migrate
.\.venv\Scripts\python scripts/setup_airflow.py
docker compose --profile airflow build airflow
docker compose --profile airflow up -d --wait airflow
docker compose --profile dashboard build dashboard
docker compose --profile dashboard up -d --wait dashboard
```

The dashboard profile does not start Airflow. Airflow has its own metadata database;
earthquakes and application monitoring remain in the original application database.
The DAG remains **paused**; no recurring ingestion is enabled by these instructions
on this verified checkout. A fresh DAG is created paused. An existing installation
retains its saved pause state, so inspect it in Airflow before a demonstration.

For development, run Streamlit directly on Windows instead of the dashboard container:

```powershell
docker compose --profile dashboard stop dashboard
.\.venv\Scripts\python -m streamlit run dashboard/app.py
```

Do not run both on port 8501. If another application occupies it, pass
`--server.port=8502` to Python or set `STREAMLIT_PORT=8502` in `.env` for Docker.
Airflow admin credentials stay in the local `.env`; the dashboard does not receive them.
The dashboard container is built from source, so rebuild it after code changes.

Navigation includes Overview, Earthquake Map, Trends, Explorer, and Pipeline
Monitoring. Shared date, magnitude, depth and literal location filters run in SQL.
Both date inputs are inclusive UTC dates; the SQL end is the next midnight,
exclusive. Numeric bounds are inclusive. Unknown magnitude/depth are included
by default, even with active bounds; uncheck the explicit option to require both
measurements. Valid negative measurements are preserved. Location is free text,
not a country assignment. Explorer offers deterministic sorting, pages of
25/50/100 rows, and a UTF-8 CSV of every matching row up to 10,000 events.
Selections above that limit must be narrowed before export. Map and scatter
use the newest 5,000 matches with visible limits. Server-side frequency and
adaptive histograms aggregate all matches, including an unknown bin.

Connections reuse Phase 1 settings and a managed SQLAlchemy pool. Dashboard
connections default to read-only transactions, and each query explicitly starts
read-only. Writes are rejected in integration tests. No startup migrations or
ingestion are performed. Query results expire after 60 seconds; Refresh data
clears them immediately. Credentials are never rendered. Friendly DB failure and
empty-selection states never substitute sample data. Map tiles need internet.

Operational history comes from `pipeline_health`, `ingestion_runs` and
`pipeline_state`; it is global rather than earthquake-filtered. Workflow parents
and child persistence loads are separate. Database history cannot establish live
scheduler status; the page says so and links to Airflow. Event/source-update age,
record-ingestion age, successful-workflow age and checkpoint lag have distinct
meanings. An unchanged replay does not rewrite an event's ingestion timestamp.

The verified database contains **40 real events on two dates**, October 1 and 25,
2026, from requests with magnitude thresholds 4 and 5. It is sufficient for a
functional demo, not a complete history or reliable frequency inference. Blank
periods do not establish zero earthquakes. See the report for a proposed 30-day
dataset; it has **not** been ingested.

Verification commands:

```powershell
.\.venv\Scripts\python -m pytest --database-tests -q --basetemp=.pytest_tmp/final-check
.\.venv\Scripts\python -m pip check
docker compose exec -T airflow python /workspace/scripts/verify_airflow_dag.py
# Explicitly executes one six-hour historical DAG run using LocalExecutor:
docker compose exec -T airflow python /workspace/scripts/verify_phase5_airflow.py
# This checkout already has a preservation snapshot from before Phase 5 verification:
.\.venv\Scripts\python scripts/verify_dashboard.py
```

`--database-tests` creates a fresh `quaketrack_test_<uuid>` database and only resets
synthetic fixtures there. It requires CREATEDB and leaves that test DB for inspection.
The live verification script uses read-only connections, checks original event rows
and archive hashes against ignored `data/staging/phase5/before.json`, opens all five
pages with AppTest, checks real filters and CSV, and compares KPIs to existing SQL.
For a fresh environment, capture `scripts/verify_dashboard.py --snapshot` before
your own bounded verification. It refuses to overwrite a previous snapshot. A
later legitimate source revision may change event rows and fail this strict
preservation check; inspect that change rather than replacing evidence blindly.

Stop safely (no volumes are removed):

```powershell
docker compose --profile dashboard stop dashboard
docker compose --profile airflow stop airflow airflow-db
docker compose stop postgres
```

Stop a host Streamlit process with Ctrl+C. Never run `down -v` or delete raw
archives. The original `requirements-lock.txt` remains the core dependency set;
`requirements-dashboard-lock.txt` records the resolved Windows Python 3.14 demo
environment. For that exact environment, install it with `pip install -r`, then
install the local package using `pip install --no-deps -e .`. Docker resolves
platform-specific transitive wheels from the pinned direct dependencies.

- [Final report and verification evidence](docs/phase5_report.md)
- [4–5 minute demonstration](docs/DEMO_GUIDE.md)
- [Viva questions and answers](docs/VIVA_QA.md)
