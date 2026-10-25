# Phase 4 completion report

Date: 2026-10-25 (Asia/Calcutta). **Phase 4 is complete and verified.**
Airflow 3.3.2 is running at http://localhost:18080. The hourly DAG is configured
and left paused after verification; automatic ingestion starts when it is unpaused.
Failure simulation is disabled. The application PostgreSQL port remains 55432.
No Phase 5 dashboard/ML was implemented. No Git mutation was performed.

## Audit and baseline

Read PROJECT.md, README, Phase 1/2/3 reports, database operations and the existing
ETL/configuration/persistence/Compose/test architecture. Verified the repository
was clean. Baseline full suite: **110 passed in 30.79s**, including isolated DB tests.
The existing PostgreSQL container was healthy. Existing application code, SQL
migrations 001-003, raw archives and application credentials were preserved.

## Architecture and configuration

Pinned official image: apache/airflow:3.3.2-python3.12. The build used base digest
sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1.
The official Airflow 3.3.2 prerequisites/docs were checked before implementation.
Airflow 2 was considered but not selected because its lifecycle has ended.

Airflow standalone with LocalExecutor runs API server, scheduler, DAG processor and
triggerer inside one local service. An isolated PostgreSQL 16 metadata service has
its own user, random password and named volume; it never stores application tables.
The existing PostgreSQL 17 service and postgres_data volume were not replaced,
reset or removed. Source code/SQL/DAGs/scripts are mounted read-only; data is shared
for durable archival/staging. Airflow home/auth/log state persists in its own volume.

The application runs in a separate Python 3.12 venv inside the image, retaining its
pinned requests/SQLAlchemy/psycopg/dotenv stack. Airflow dependencies do not replace
host or application packages. No host package was installed for Phase 4. No broker,
Celery worker, Redis or Kubernetes was added. Docker memory was about 8 GB.

Airflow secrets were generated into ignored .env without printing their values.
A pre-created password file avoids generated credential logging. The admin password
is the local AIRFLOW_ADMIN_PASSWORD; no shared demo password exists. Host UI port
18080 binds only loopback. HTTP reachability returned 200; authentication returned
201 and an authenticated DAG API request returned 200. Tokens/passwords were not
printed. A scan of actual service logs found no configured secret values.

## DAG and incremental workflow

DAG: quaketrack_earthquake_ingestion.

`determine_window -> ingest_and_check -> record_results`

The middle task calls the existing extraction/archival, transformation, validation,
quarantine, deduplication and PostgreSQL loading, then performs quality checks.
Combining these steps avoids rewriting the completed ETL. XCom carries only plan
paths, result paths, run IDs and status, not event datasets. Application stderr is
visible in task logs, including failure details; stdout contains lightweight JSON.

Configuration: hourly `0 * * * *`, catchup=False, paused on creation, one active DAG
run and task, two retries after the initial try, exponential backoff from 60 seconds
capped at 5 minutes, task timeout 90 minutes, DAG timeout 2 hours. During bounded
verification, scheduling was temporarily None and retry delay 5 seconds. Final
configuration was restored to hourly/60 seconds with the DAG paused. No clock-driven
hourly tick is claimed; actual manual triggers ran through the live scheduler/executor.
The default schedule can be changed to six-hourly or manual-only in .env.

The Airflow-independent orchestration module plans from pipeline_state rather than
MAX(event time). New source/filter scopes bootstrap a six-hour range, ending ten
minutes behind the horizon. Subsequent runs overlap by up to 72 hours, bounded by
the initial coverage start, and move the end forward by at most six hours. UTC,
half-open boundaries and source_updated_at revision precedence are inherited from
Phase 2/3. Delayed catch-up cannot jump over an unprocessed gap. No large historical
backfill runs on startup. Explicit manual ranges are capped at 24 hours and never
advance automatic coverage.

Migration 004 adds non-destructive monitoring columns, permits incremental job types,
creates pipeline_state and exposes pipeline_health. It widens a CHECK constraint in
a transaction without deleting/recreating application tables. Existing migration
files remain unchanged. Stable execution keys/UUIDs and per-workflow staging survive
retries; session advisory locking protects against duplicate workflow writers.

Success and automatic coverage advancement share the final transaction AFTER
PostgreSQL persistence and quality checks. A successful child DB load is distinct
from end-to-end parent workflow success. Failed quality never advances pipeline_state.
Committed chunk counters survive retries, including uncertain commits/partial loads.
DB resume is explicitly scoped to the workflow staging directory; direct Phase 3
loading retains its previous default behavior.

## Monitoring and quality

pipeline_health tracks workflow/DAG identity, current stage/status, UTC start/finish,
duration, fetched/inserted/updated/unchanged/rejected counts, API/database/quality
stage failures and automatic coverage freshness. Child load/run/archive/rejection
metadata remains available. Use parent workflow rows for aggregate end-to-end counts
instead of summing parent and child observations.

Quality checks verify archived source hashes and replay, event ID presence,
non-regressing stored revisions, source-versus-committed-window counts, expected DB
checkpoints, global duplicate/required-field/coordinate/FK/counter integrity,
connectivity, recent committed-load freshness and coverage lag. A legitimate empty
query is successful only when its empty DB window committed. Invalid JSON fails
with preserved evidence; invalid records remain quarantined. Old unrelated Windows
archive paths are not treated as container paths; current-run references are verified.

API timeouts/429/5xx reuse existing bounded request retries. DB transactions roll
back on failure. Task timeout/retry callbacks best-effort record a failed parent;
if DB access is unavailable, Airflow logs remain the recovery evidence. Failed task
runs can be cleared with their downstream result task after fixing the cause.
No success checkpoint is inferred from extraction alone or a failed child operation.

## Tests executed

- Baseline: 110 passed in 30.79s.
- Initial Phase 4 full suite: 123 passed in 38.12s.
- Final full suite after a live-discovered resume fix: **125 passed in 37.82s**.
  This is 96 offline cases plus 29 cases in a freshly created isolated test DB.
- Final default run: **96 passed, 29 intentionally skipped in 3.16s**.
- Actual Airflow interpreter/DAG import checks: **3 passed** (final execution 4.127s).
- compileall succeeded for src/scripts/dags/tests; pip check found no broken host
  requirements; git diff --check passed with ordinary line-ending notices only.

Coverage includes actual DAG imports/dependencies/schedule/retries, bounded bootstrap,
overlap and gap-free catch-up, UTC boundaries, invalid settings, duplicate execution,
checkpoint behavior, source/DB failure recovery, empty results, quality failure before
checkpoint advance, raw corruption refusal, identical responses in separate workflows
and one-time controlled failure recovery without additional HTTP. Existing Phase 1-3
assertions remain; only migration-count assumptions, the next test migration number
and owned-test-DB cleanup were adjusted for the new schema.

## Actual Airflow verification results

Six manually triggered DAG runs reached **success**, with all 18 task instances
successful. Source requests went to real USGS, not production fixtures. A six-hour
historical query on October 1 at magnitude 4+ returned 12 already-stored events;
loading correctly left them unchanged. Two automatic-window-selector runs at
magnitude 5+ fetched three current real events and advanced a durable coverage scope.

| Airflow run ID | Final status | Fetched | Inserted | Updated | Unchanged | Quality failure attempts |
|---|---|---:|---:|---:|---:|---:|
| phase4-live-first | success | 12 | 0 | 0 | 12 | 0 |
| phase4-live-repeat | success | 12 | 0 | 0 | 12 | 0 |
| phase4-live-recovery | success | 12 | 0 | 0 | 12 | 4 |
| phase4-incremental-first | success | 3 | 3 | 0 | 0 | 0 |
| phase4-recovery-final | success | 12 | 0 | 0 | 12 | 1 |
| phase4-incremental-second | success | 3 | 0 | 0 | 3 | 0 |

The clean controlled recovery run phase4-recovery-final deliberately failed once
after persistence/quality. The ingest task succeeded on **attempt 2**, with zero
HTTP requests on its successful retry. All 12 records remained unchanged. The
simulation was explicitly enabled only for a bounded manual window and then disabled.

The earlier phase4-live-recovery exposed a real integration bug: a cached identical
response reused a DB checkpoint belonging to another staging directory, leaving
that workflow without matching provenance counts. The quality gate failed correctly.
The fix scopes orchestration DB resume to its own staging path, with a new regression
test. Only that run's failed ingestion and downstream task were cleared; its source
archives were retained. It ultimately succeeded on attempt 5, preserving its four
quality-failure observations (three bug detections and one controlled failure).
No unresolved failure remains; a separate clean recovery run verifies the intended
one-failure/one-retry behavior.

The automatic coverage scope now has:

- coverage_start: 2026-10-25 10:15:58+00:00
- last_successful_window_end: 2026-10-25 16:20:30+00:00
- last_run_id: 30f96b17-bf67-52e6-8d6c-0d29676e8203

The second automatic plan began at/before the first end and advanced the end. All
six workflows passed archive/replay/count/integrity checks again after execution.
Development now contains **40 events**: the original 37 plus 3 real current events.
All original 37 rows, including ingestion timestamps/provenance, remained identical
to the Phase 3 snapshot; SHA-256 is
4e87424bd6dfedb65e83f2fad8b7bbb46b18a7b23f41dd8c80451c23f3764e9d.
No original raw file or event was edited to fabricate revisions/failure cases.

Airflow was recreated with its persistent volumes during verification. All six run
histories, metadata, credentials, pause state and all 40 events survived. Final UI,
scheduler, DAG processor, metadata DB and application DB checks were healthy.
Scheduling is configured and paused; no scheduled future work was left enabled.

## Environment issues encountered and resolved

Windows rejected port 8080; using UI port 18080 resolved it without touching 55432.
An early CLI metadata connection included a newline from the *_CMD URL helper;
removing that trailing newline fixed CLI DB access. The UI process used the correct
URL and its metadata database was never reset. Initial real recovery testing found
the checkpoint-scope issue described above; it was corrected and verified in tests
and actual scheduler retries. No approval request or destructive infrastructure
change was necessary: only new metadata/home volumes were created, and no existing
volume was removed.

## Files created/modified

Created:
- .dockerignore
- dags/earthquake_ingestion_dag.py
- docker/airflow/Dockerfile and docker/airflow/start.py
- scripts/setup_airflow.py, scripts/airflow_connection.py, scripts/verify_airflow_dag.py
- sql/migrations/004_add_pipeline_monitoring.sql
- src/quaketrack/orchestration.py
- tests/test_phase4.py
- docs/airflow.md and docs/phase4_report.md

Modified:
- docker-compose.yml: optional services, new volumes, health checks and configuration.
- .env.example: secure Airflow template, source/window/schedule settings; default app
  port now matches the required 55432 (actual existing port never changed).
- src/quaketrack/storage/loader.py: optional workflow-scoped DB resume.
- tests/integration/test_postgres.py: new recovery tests and migration-aware fixtures.
- README.md and docs/implementation_plan.md.

Ignored local .env gained generated Airflow secrets and settings; its application
password/port were preserved. Docker images installed Airflow plus an isolated
application environment; no host dependency pins/lock were changed. Runtime evidence
is retained under data/staging/phase4-*.json/.txt and per-workflow plan/result/failure/
staging files. Airflow task logs are in its persistent home volume.

## Commands executed

```powershell
.\.venv\Scripts\python -m pytest --database-tests -q --basetemp=.pytest_tmp/phase4-baseline --tb=short
docker pull apache/airflow:3.3.2-python3.12
.\.venv\Scripts\python scripts/setup_airflow.py
docker compose --profile airflow build airflow
.\.venv\Scripts\python -m quaketrack.cli migrate
docker compose --profile airflow up -d airflow
docker compose exec airflow python /workspace/scripts/verify_airflow_dag.py
docker compose exec airflow airflow dags list -o json
docker compose exec airflow airflow dags unpause quaketrack_earthquake_ingestion
docker compose exec airflow airflow dags trigger quaketrack_earthquake_ingestion --run-id phase4-live-first --conf '{"start":"2026-10-01T00:00:00Z","end":"2026-10-01T06:00:00Z","min_magnitude":4}'
# The same bounded window was triggered under distinct repeat/recovery run IDs.
docker compose exec airflow airflow dags trigger quaketrack_earthquake_ingestion --run-id phase4-incremental-first --conf '{"min_magnitude":5}'
# A second run with a new ID exercised overlap and forward checkpoint progress.
docker compose exec airflow airflow dags list-runs quaketrack_earthquake_ingestion -o json
docker compose exec airflow airflow tasks states-for-dag-run quaketrack_earthquake_ingestion phase4-recovery-final -o json
docker compose exec airflow airflow dags pause quaketrack_earthquake_ingestion
# After restoring hourly config and disabling failure simulation:
docker compose --profile airflow up -d airflow
.\.venv\Scripts\python -m pytest --database-tests -q --basetemp=.pytest_tmp/phase4-final --tb=short
.\.venv\Scripts\python -m pytest -q --basetemp=.pytest_tmp/phase4-offline-final --tb=short
.\.venv\Scripts\python -m compileall -q src scripts dags tests
.\.venv\Scripts\python -m pip check
```

Additional executed checks used read-only PostgreSQL identities/health queries,
actual Airflow metadata task attempt counts, post-restart DAG pause state, authenticated
HTTP requests, a no-secret-value log scan, per-workflow check_quality calls, durable
state comparison and a full original-event snapshot comparison. The failed run was
cleared through the pinned Airflow task-clear API for exactly its two affected tasks.
No development table was truncated or dropped, and no database volume was deleted.

## Known limitations and Phase 5 prerequisites

This is a local standalone Airflow deployment with development authentication, not
public production infrastructure. The configured hourly schedule was validated but
left paused; no clock-triggered hourly run is claimed. Empty/retry edge cases use
explicit isolated fixtures; the reported real observations are genuine source data.

Initial coverage is intentionally six hours, not a full historical catalog. Corrections
outside the 72-hour overlap, source deletion and pre-bootstrap data require deliberate
reconciliation. Absolute archive paths remain execution-environment-specific; preserve
shared data for container retries. A long outage may leave stale running parent rows;
callbacks are best effort and new runs/checkpoints remain safe, while broader operator
status auditing is manual. A source commit reused after 24 hours fails freshness and
requires a fresh run. API failure counters measure failed stage attempts; individual
HTTP attempts/statuses are in archives and task logs. Parent/child counters must not
be summed indiscriminately.

Phase 5 can use earthquakes, analytical queries, pipeline_health and source/provenance
metadata for its PostgreSQL-backed dashboard. It still needs filters/maps/charts,
empty/error handling, display freshness labels, end-to-end UI tests and demonstration
artifacts. **Phase 5 was not started.** See airflow.md for exact startup/shutdown,
manual triggers, login, schedule enablement, log access and troubleshooting.
