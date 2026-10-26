# QuakeTrack

Global Earthquake Data Engineering & Analytics Pipeline. `PROJECT.md` is the primary
specification. **Phases 1–5 are implemented and locally verified.** Historical extraction, validation,
quarantine, repeatable staging, PostgreSQL loading, migrations and analytical SQL
work. Airflow orchestration, incremental coverage and monitoring are verified.
The Streamlit dashboard provides five PostgreSQL-backed pages with shared filters and CSV export.
Phase 6 adds logical Bronze–Silver–Gold layers, four SQL views, a processed-event
contract and quality reporting, a dedicated dashboard reader, and an optional local
PySpark demonstration. The normal ETL remains Python; Spark is isolated from it.

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
.\.venv\Scripts\python scripts/setup_dashboard.py
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

Connections reuse Phase 1 host/database settings and a managed SQLAlchemy pool.
Dashboard credentials are separate `DASHBOARD_DB_USER` / `DASHBOARD_DB_PASSWORD`
values: there is no fallback to ETL credentials. The dedicated role has SELECT
permissions only on required tables/views or columns, without superuser flags,
ownership or memberships. Writes are denied even when transaction read-only mode
is disabled in verification. Connections also default to read-only transactions,
and each query explicitly starts read-only. No startup migrations or
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

## Phase 6: syllabus demonstrations

The existing `data/raw` files are **Bronze**; normalized staging and the current
`earthquakes` table are **Silver**. Four ordinary SQL views are **Gold**:
`gold_daily_earthquakes`, `gold_magnitude_distribution`, `gold_monthly_earthquakes`
and `gold_depth_by_magnitude`. No files are moved or duplicated into new production
layers. Views aggregate current earthquake-type rows; null measurements stay
unknown, negative values remain valid, and all dates are UTC. This is a logical
medallion layout, not Delta Lake, Iceberg or Hudi.

For an existing checkout, apply the additive migration and configure the reader:

```powershell
docker compose up -d --wait postgres
.\.venv\Scripts\python -m quaketrack.cli migrate
.\.venv\Scripts\python scripts/setup_dashboard.py
docker compose --profile dashboard build dashboard
docker compose --profile dashboard up -d --wait dashboard
```

`setup_dashboard.py` creates `quaketrack_dashboard` with a generated password in
ignored `.env`, or reuses its existing credentials without rotation. It grants only
the required reads, omits sensitive audit columns, and refuses an existing powerful
role rather than revoking privileges. ETL/Airflow continue to use `POSTGRES_USER` /
`POSTGRES_PASSWORD`; Streamlit receives only `DASHBOARD_DB_USER` /
`DASHBOARD_DB_PASSWORD`. Run setup again after reviewed migrations add a view the
reader needs. New views are not granted automatically. Default PostgreSQL PUBLIC
temporary-table privileges are not revoked; permanent project tables remain
protected by ACLs. The existing bootstrap ETL role was not demoted or changed.

The formal contract is `contracts/earthquake.v1.json`. It describes all 18
normalized fields, source revisions, UTC/millisecond timestamps, optional values,
uniqueness and provenance. Validation reuses existing domain functions. Processed
numeric strings fail this contract even though raw-source normalization can convert
them. The report denominator is every **processed Silver row supplied**, including
duplicates, rather than every raw API feature fetched. All rows in a duplicate-ID
group fail dataset uniqueness; an empty input has no compliance percentage.

Generate a bounded, read-only Silver snapshot, SQL references and quality report:

```powershell
.\.venv\Scripts\python -m quaketrack.phase6 --verify-archives
```

The verified checkout already has these files under `data/staging/phase6/demo/`.
The command refuses to overwrite them. For a later independent snapshot, pass a
fresh `--output-dir`, such as `data/staging/phase6/demo-next`; Spark can read it using
a matching bind mount. No earthquake rows or ingestion checkpoints are changed.
The optional artifact is capped at 10,000 records. Silver and Gold are exported
within the same repeatable-read transaction, so the parity denominator is identical.
`--verify-archives` checks every local response checksum and verifies that each
Silver row matches a valid normalized archived representation. It requires the
local ignored source archive files; a fresh clone must ingest a small sample first.

Optional Spark runs on Python 3.12 + OpenJDK 17 in Docker, using pinned
**PySpark 4.1.3**, `local[2]`, UTC, two shuffle partitions and no exposed Spark UI.
Java was absent from this Windows PATH; Docker supplies it without a host install.
Neither normal ingestion nor dashboard startup installs or imports PySpark.

```powershell
docker compose --profile spark build spark
docker compose --profile spark run --rm spark
docker compose --profile spark run --rm spark python -m pytest tests/spark -q -p no:cacheprovider
```

The Spark job reads `/inputs/silver-snapshot.json` from the default demo mount,
creates an explicit typed DataFrame, demonstrates `select`, `filter`, `withColumn`,
`groupBy`, aggregates and Spark SQL, and compares four outputs to PostgreSQL.
It writes a new `run-<uuid>/spark-report.json` under `data/staging/phase6/spark`.
Each run preserves older reports. Collect/count are actions; the transformations
build lazy plans. The container receives **no database or Airflow credentials**.
This is single-machine teaching code, not a cluster or a performance improvement
over PostgreSQL for 40 rows. The first image build downloads Java/Spark and takes
several minutes; repeated executions reuse it. The native-Hadoop warning can occur
for this local job; no Hadoop service or cluster is deployed.

To use a fresh snapshot directory without editing Compose:

```powershell
docker compose --profile spark run --rm -v "${PWD}/data/staging/phase6/demo-next:/inputs:ro" spark
```

After the initial snapshot and Spark run, verify preservation, live Gold parity,
real reader INSERT/UPDATE/DELETE denial, ETL privileges and the paused DAG:

```powershell
.\.venv\Scripts\python scripts/verify_phase6.py
.\.venv\Scripts\python scripts/verify_dashboard.py
.\.venv\Scripts\python -m pytest --database-tests -q --basetemp=.pytest_tmp/phase6-check
docker compose exec -T airflow python /workspace/scripts/verify_airflow_dag.py
```

The Phase 6 verifier expects this checkout's ignored `before.json` preservation
manifest and default `demo` snapshot. It is a local evidence checker, not a required
startup step in fresh clones. Normal host pytest skips the optional Spark module;
the five actual Spark tests run in its separate container. DB tests create a new
isolated database and reader role, retained for inspection. No production rows are
modified by tests. See the report for exact results and limits.

- [Phase 6 implementation and verification](docs/phase6_report.md)
- [Syllabus mapping](docs/SYLLABUS_MAPPING.md)

## Phase 7: expanded history and redesigned dashboard

The approved 90-day backfill loaded **3,951 source observations** for
`[2026-07-27, 2026-10-25)` UTC at magnitude ≥4.0. It inserted **3,914** events and
left 37 overlapping events unchanged, bringing the database to **3,954** events
across **91 event dates**. The three original October 25 observations remain
outside the approved 90 complete days. All 40 original complete rows and all
48 original raw files are preserved. No additional day was silently added when
work resumed on October 26. Airflow remains paused on port 18080.

The navy/teal interface includes responsive KPI cards, compact filters with Reset
and Refresh, balanced Plotly panels, a compact recent-events table, optional
technical Explorer columns, pagination and lossless filtered CSV. The Map page
has a source-event inspector and an **Offline coordinate view** toggle. That
fallback uses longitude/latitude axes without any external map tiles. Coordinate
density cells are labeled by degrees; no countries are inferred from place text.

Coverage comes from successful PostgreSQL chunk metadata, merged separately by
source magnitude threshold. It distinguishes event dates from fully loaded UTC
days. Histograms/counts use all selected rows; map/scatter remain capped at 5,000.
Missing periods are not automatically filled with zeros. Amber timeline bars mark
partial calendar periods or unverified coverage at the source threshold. Monitoring distinguishes
current SQL metadata from dated archive/contract/Spark verification evidence,
and shows whether the recorded dataset summary still matches current data.

Update an existing deployment after pulling these changes:

```powershell
.\.venv\Scripts\python scripts/setup_dashboard.py
docker compose --profile dashboard build dashboard
docker compose --profile dashboard up -d --wait dashboard
```

The setup adds only SELECT access to chunk IDs, successful chunk windows/status
and the raw archive chunk link. These non-sensitive fields are necessary to verify
coverage; archive paths, quarantine and error details remain unreadable. No write
permissions or password changes are introduced. The dashboard receives the small
validation summary through a read-only bind mount, not raw archives or ETL secrets.

The new **opt-in** `--preserve-existing` backfill option keeps existing PostgreSQL
rows unchanged even when a newer source revision arrives. It applies when used
with `--load-database`; normal ingestion retains its original revision-aware upsert
policy. The approved run used this flag to respect preservation of the original
40 rows. Its `unchanged` counter describes unchanged database rows; it does not
prove every incoming representation had an identical source revision.

Full results and source counts are in [the Phase 7 report](docs/phase7_report.md)
and [the reviewed preflight](docs/phase7_preflight.md). The completed run has a
fresh staging directory at `data/staging/phase7-90d-mag4`. To check resume of that
already approved range without refetching successful windows:

```powershell
.\.venv\Scripts\python -m quaketrack.cli backfill --start 2026-07-27 --end 2026-10-25 --min-magnitude 4 --window-days 1 --max-windows 90 --output-dir data/staging/phase7-90d-mag4 --load-database --preserve-existing --resume
```

The job logs separate staging inserts from PostgreSQL inserts. A separate repeat
of July 27 fetched 60 observations and reported all 60 unchanged in PostgreSQL.
Do not delete named volumes or source archives to repeat a demonstration.

Verification with the current expanded snapshot:

```powershell
.\.venv\Scripts\python scripts/verify_phase7.py
.\.venv\Scripts\python -m pytest --database-tests -q --basetemp=.pytest_tmp/phase7-check
docker compose exec -T airflow python /workspace/scripts/verify_airflow_dag.py
docker compose --profile spark run --rm spark python -m pytest tests/spark -q -p no:cacheprovider
docker compose --profile spark run --rm -v "${PWD}/data/staging/phase7/demo:/inputs:ro" -v "${PWD}/data/staging/phase7/spark:/outputs" spark
```

Phase 6's 40-row snapshot/report remains historical evidence. Use the Phase 7
snapshot and verifier for current Spark parity; the old strict Phase 6 point-in-time
verifier deliberately detects dataset expansion rather than silently treating an
old snapshot as current. Phase 7 evidence is local and ignored under
`data/staging/phase7`; fresh clones need their own approved ingestion and snapshot.
To prepare a new independent snapshot without overwriting evidence, run
`python -m quaketrack.phase6 --output-dir <fresh-directory> --verify-archives`
with the venv Python and bind that directory to Spark's `/inputs`.

Phase 7 publication was subsequently authorized by the user. Services retain ports
55432/18080/8501. If Docker Desktop is stopped, run `docker desktop start`, then
the normal Compose startup commands; existing named volumes restore the data.
