# Airflow operations (Phase 4)

## Runtime architecture

Airflow 3.3.2 with Python 3.12 runs in its pinned official Linux image. A separate
application virtual environment isolates the existing pinned application libraries
from Airflow dependencies. LocalExecutor runs locally in the Airflow container;
`airflow standalone` starts its API server, scheduler, DAG processor and triggerer.
This is a local academic deployment, not a hardened public hosted service. There
are no Celery workers, Redis, Kubernetes or external brokers.

The optional Compose `airflow` profile adds two services:

- airflow-db: PostgreSQL 16 metadata DB, own user/password and named volume,
  not published on a host port. It never hosts earthquake application tables.
- airflow: web UI on localhost:18080, with persistent /opt/airflow state/logs and
  read-only mounts for DAGs, application code, SQL and helper scripts. ./data is a
  shared writable mount for archives, staging plans/results and quarantine.

The existing PostgreSQL 17 service/volume remain intact on localhost:55432. Containers
use host postgres and internal port 5432; this does not change its host port. No
existing volume is removed. Airflow health checks inspect metadata, scheduler and
DAG-processor status, not just whether the HTTP server responds.

## First startup

Use Docker Desktop with Linux containers and at least 4 GB available to Airflow;
this verification used a Docker engine with about 8 GB. Keep the existing .env and
POSTGRES_PASSWORD. On a fresh checkout, copy .env.example and configure the application
password first. SQL migrations are explicit; starting Airflow does not change the
application schema. Run from the repository root:

```powershell
.\.venv\Scripts\python scripts/setup_airflow.py
.\.venv\Scripts\python -m quaketrack.cli migrate
docker compose --profile airflow build airflow
docker compose --profile airflow up -d airflow
docker compose --profile airflow ps
```

setup_airflow.py appends only missing random Airflow metadata/admin passwords, JWT
secret and Fernet key to ignored .env. It prints key names, never values. No demo
password is hardcoded. The helper preserves application configuration and existing
secrets. Airflow's SimpleAuthManager password file is pre-created with mode 0600 so
startup need not generate and print credentials. Airflow metadata URLs are assembled
using URL encoding; do not print the airflow_connection.py helper's output.

Open http://localhost:18080. Username: admin. Read AIRFLOW_ADMIN_PASSWORD from your
local ignored .env to log in. This development auth manager is restricted to loopback;
production auth/deployment is not claimed. Web port is configurable via AIRFLOW_WEB_PORT.
Windows rejected 8080 during verification; 18080 worked.

## DAG and schedule

DAG ID: quaketrack_earthquake_ingestion.

```text
determine_window -> ingest_and_check -> record_results
```

The middle task reuses the complete extraction/normalization/quarantine/deduplication,
DB loading and quality functions. Combining these avoids rewriting the Phase 2 ETL.
Tasks call the application venv via subprocess; stderr appears in task logs, while
stdout returns only a plan/result identifier. No record arrays are passed via XCom.

Default PIPELINE_SCHEDULE is hourly (`0 * * * *`) per PROJECT.md. Set `0 */6 * * *`
for six-hourly scheduling or `None` for manual-only execution. catchup=False prevents
historical scheduler backfill, max_active_runs=1/max_active_tasks=1 serialize runs,
and an application-wide PostgreSQL session advisory lock prevents competing workflow
writers. Existing repository transaction locks protect DB batches as before.

The DAG starts paused and is **left paused after verification**, with its hourly
schedule configured. Enable automatic ingestion deliberately:

```powershell
docker compose exec airflow airflow dags unpause quaketrack_earthquake_ingestion
# Disable future automatic launches:
docker compose exec airflow airflow dags pause quaketrack_earthquake_ingestion
```

The CLI prints the previous pause value before applying its update; inspect the UI
for the applied state. Airflow task defaults: 2 retries after the first attempt,
60-second base delay, exponential backoff capped at 5 minutes, 90-minute task timeout
and 2-hour DAG timeout. USGS request retries/timeouts remain independently configured.
Avoid assuming the small retry delay used for verification is the final runtime value.

## Manual execution

To inspect a small historical range, configure manual-only scheduling while unpaused:
set PIPELINE_SCHEDULE=None in .env and recreate only Airflow. Then run:

```powershell
docker compose --profile airflow up -d airflow
docker compose exec airflow airflow dags unpause quaketrack_earthquake_ingestion
docker compose exec airflow airflow dags trigger quaketrack_earthquake_ingestion --run-id my-bounded-demo --conf '{"start":"2026-10-01T00:00:00Z","end":"2026-10-01T06:00:00Z","min_magnitude":4}'
```

Use a unique run ID for a new observation. Explicit manual windows must be positive,
at most 24 hours and not in the future. They do not advance automatic coverage.
With no explicit start/end, a manually triggered run exercises the same durable
incremental selector as scheduled execution. min_magnitude may override the default;
watermarks are separated by source/magnitude/event-type scope.

The complete pipeline was verified with actual queued/scheduled-by-Airflow tasks,
not just `dag.test()`. Tests of manual windows fetched 12 real historical events
already stored in PostgreSQL, so counts were 0 inserted/0 updated/12 unchanged.
Two automatically selected incremental windows inserted 3 current real events,
then reported all 3 unchanged on the second overlapping run.

## Incremental selection and completeness

A pipeline_state row stores the coverage start and last successful window end. This
is not MAX(event occurred_at): occurrence time cannot capture source revisions or
legitimately empty periods. Timestamps are UTC and inherited half-open windows
preserve millisecond boundaries.

On first use of a scope, bootstrap only the last 6 hours, ending 10 minutes behind
the current/scheduled horizon. This bounds startup work; it does not claim a complete
historical catalog. After success, fetch from max(initial coverage start, previous
successful end minus 72 hours) to min(target horizon, previous end plus 6 hours).
Thus overlap expands gradually and never backfills before the initial coverage
start. Downtime catch-up advances in bounded steps rather than jumping across gaps.
Configure INCREMENTAL_BOOTSTRAP_HOURS, INCREMENTAL_LOOKBACK_HOURS,
INCREMENTAL_STEP_HOURS and INGESTION_DELAY_MINUTES as documented in .env.example.

Each Airflow run has a stable execution key and UUID plus its own staging directory.
The plan persists before extraction; retries reuse it, verified archives and the
same completed extraction windows. DB resume is scoped to this workflow's staging
directory, so a cached response from a different workflow cannot skip its provenance.
The DB catalog remains globally idempotent. Duplicate execution of an already
successful execution key returns its recorded success without new API calls.

A load run can finish DB persistence before quality. Only the parent workflow's
quality-gated success and pipeline_state advance share the final transaction.
Failures never advance automatic coverage. Previously persisted events remain safe
and retries cannot overwrite newer source revisions with old responses. The same
occurrence-time window can pick up source_updated_at corrections. Revisions outside
the overlap, deleted source events and pre-bootstrap history require deliberate
historical replay/reconciliation; no complete-worldwide or real-time guarantee is made.

## Quality and monitoring

After DB loading, verify source/archive checksums and raw replay, accepted/rejected/
filtered/deduplicated counts against committed chunks, presence of accepted event
IDs, non-regressing source revisions, global PK/required-field/coordinate/FK checks,
connectivity, recent committed-load freshness and coverage lag. Zero records is valid
if the source request and empty DB window commit succeeded. A stale (>24-hour) reused
commit is rejected; start a fresh run to refresh the source rather than advancing
coverage from stale evidence.

The pipeline_health view exposes parent workflow identity/status/stage, start/end,
duration, quality status, fetched/inserted/updated/unchanged/rejected counts, failure
counters and automatic last success/coverage lag. api_failures/database_failures/
quality_failures count failed workflow stage attempts. Individual retryable HTTP
responses and attempt numbers remain in raw metadata/task logs; successful request
retries do not necessarily increment a workflow-stage failure counter.

Example application checks:

```powershell
.\.venv\Scripts\python -m quaketrack.cli analytics integrity --start 2026-10-01 --end 2026-10-02
# In the container, use /opt/quaketrack-venv/bin/python for application commands.
docker compose exec postgres psql -U quaketrack_app -d quaketrack -c 'SELECT execution_key,status,quality_status,records_fetched,records_inserted,records_updated,records_unchanged FROM pipeline_health ORDER BY started_at DESC;'
```

ingestion_runs retains child DB load observations as well as parent workflow rows;
use pipeline_health for end-to-end counts, rather than summing parent and child rows.
A failed parent may have successful child commits; this never implies overall success.
App data artifacts are under data/staging/workflows/<execution-key hash>. They include
plan.json, result.json/failure.json and staging/runs/<id>/report.json. Historical
Windows path references remain host-specific; current container quality checks inspect
the current workflow's shared archives, not unrelated old host paths.

## Logs, failure recovery and restarts

In the Airflow UI, open a DAG run, select a task, then Logs. CLI states and service logs:

```powershell
docker compose exec airflow airflow dags list-runs quaketrack_earthquake_ingestion -o json
docker compose exec airflow airflow tasks states-for-dag-run quaketrack_earthquake_ingestion my-bounded-demo -o json
docker compose logs --tail 100 airflow
```

Task logs persist in the airflow_home volume under /opt/airflow/logs. Credentials
are not dumped by the application; a scan of actual container logs found no configured
secret values. Never publish .env, auth files or full unreviewed config dumps.

USGS timeouts/429/5xx use existing bounded retries. A malformed collection fails
with archived evidence; malformed records are quarantined. A DB outage or transaction
error leaves the pipeline checkpoint unchanged. Airflow retries the same plan safely.
Timeout/failure callbacks best-effort mark an unfinished parent failed. If the DB is
unreachable, Airflow task logs are the available failure record until recovery.
After retries are exhausted, inspect the error and clear the failed task plus downstream
record_results in the UI. Do not clear unrelated successful runs. An OS-killed worker
releases the advisory lock; its uncommitted work cannot become a success checkpoint.

An explicitly gated manual-only test flag simulate_failure_once exists for recovery
verification. It is rejected unless ALLOW_FAILURE_SIMULATION=true and an explicit
bounded historical window is supplied. It fails after loading/quality, writes a marker
and allows the next retry to proceed. It does not alter earthquake source data. The
flag was used successfully in a real DAG run (ingestion attempt 2 succeeded with zero
additional HTTP requests), then ALLOW_FAILURE_SIMULATION was restored to false.
Leave this flag disabled in normal use.

```powershell
# Stop only the new services; preserve application DB and all named volumes:
docker compose --profile airflow stop airflow airflow-db
# Start them again:
docker compose --profile airflow up -d airflow
```

Do not use down -v or delete/recreate the application PostgreSQL volume. Restarting
Airflow was verified to preserve all six DAG runs, its pause state and all 40 current
earthquakes. The original 37 event rows remained byte-for-byte identical when
serialized for comparison; only 3 new real events were added.

Troubleshooting: if a host port is denied, choose another AIRFLOW_WEB_PORT; do not
change 55432. If metadata authentication fails after changing a password, changing
.env alone does not rotate an existing PostgreSQL role. Missing secrets require
setup_airflow.py before profile startup. DAG import errors appear in Airflow, and
missing application migrations require the explicit migrate command. Changed
schedule/source environment values require recreating Airflow, not deleting volumes.

## Verification commands

```powershell
.\.venv\Scripts\python -m pytest --database-tests -q --basetemp=.pytest_tmp/phase4-check
docker compose exec airflow python /workspace/scripts/verify_airflow_dag.py
```

The application suite passed 125 cases (96 offline, 29 isolated DB cases); the real
Airflow interpreter passed 3 additional DAG checks. No host Airflow installation is
required. The DB test fixture was extended only to account for the new migration and
to reset its own newly created test DB's pipeline_state between cases.

References: https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/docker-compose/index.html
and https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/auth-manager/simple/index.html.
