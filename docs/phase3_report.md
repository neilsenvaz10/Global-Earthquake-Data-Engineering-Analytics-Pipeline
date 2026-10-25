# Phase 3 completion report

Date: 2026-10-25 (Asia/Calcutta). **Phase 3 is complete and verified.**
Scope: PostgreSQL schema, migrations, transactional loading, provenance, replay,
analytical SQL and integrity verification. No Airflow, dashboard or ML was added.
No files were staged, committed, pushed or branched during this phase.

## Audit and baseline

Read PROJECT.md, both prior phase reports, README, configuration, ingestion,
normalization/validation/deduplication, staging, CLI, tests and Compose configuration.
The working tree was clean. Baseline: **88 tests passed in 1.88s**; pip check passed.
The verified development DB was quaketrack, role quaketrack_app, PostgreSQL 17.11,
client localhost:55432 (server/container port 5432). Public application tables were
empty before migration. Existing .env, credentials, port and Compose were unchanged.
No package was installed or dependency version changed.

## Schema and migrations

Reviewable SQL migrations follow the blueprint:

1. 001_create_ingestion_runs.sql: operational run identities, UTC timestamps,
   requested loaded range, status, source/action/rejection counters and errors.
2. 002_create_earthquakes.sql: USGS ID primary key, UTC occurrence/source timestamps,
   magnitude/type, depth, coordinate bounds, place, event type/status, source details,
   URLs, first/last-ingested timestamps and last accepted run foreign key.
3. 003_create_operational_tables.sql: ingestion_chunks, raw_archives and
   rejected_events, with archive/run/chunk provenance and persistence checkpoints.

The runner tracks canonical SQL checksums in schema_migrations and serializes
migration application with a PostgreSQL advisory transaction lock. All pending SQL
and tracker inserts share one transaction. Repeat application is a no-op; edited,
missing or out-of-order tracked migrations fail visibly. No destructive statement
or automatic schema recreation is provided. Checksums normalize line endings:

- 001_create_ingestion_runs.sql: `322cabfc882e13ce39f3c373b8fc9552bd25cc82983520aa83d78748e16d3202`
- 002_create_earthquakes.sql: `27299111d5a7d8632c17da7c3f8c59903f8291c2b943950d5a4f3d7f066e7f99`
- 003_create_operational_tables.sql: `1d3dea8bf129b6b90d97e29709626c5031673779e0d9f04525dff03b2652b4a8`

Required/nonempty fields, finite measurements/timestamps, coordinate bounds,
SHA-256 formats, status values, positive attempts, date ordering and count equations
are enforced in PostgreSQL. Null magnitude/depth and valid negative values are
allowed. Foreign keys protect event-to-run and archive/chunk/run/rejection relations.
Raw response bodies remain in the existing filesystem archive; only references and
metadata are stored. Invalid individual features retain their JSONB source record,
reason and archive foreign key as specified by PROJECT.md.

Indexes cover occurrence time, magnitude, event type/time, source updates, run
history and metadata foreign-key joins. A partial successful-chunk index supports
DB resume. Coordinate indexes and PostGIS are not justified by the measured sample;
coordinate queries currently scan the small catalog. See database.md for the full
schema, entity diagram, field semantics and index rationale.

## Loading and integration

A repository accepts validated Phase 2 records, deduplicates a batch by event ID
and validates the normalized persistence boundary. PostgreSQL INSERT ON CONFLICT
and conditional revision updates enforce uniqueness and precedence even on reruns.
RETURNING row IDs, rather than xmax internals, supplies accurate action counts.
A default 500-record batch bounds statement size; each window wraps all its batches,
provenance, final counters and checkpoint success in one transaction.

Newer known source_updated_at replaces older/missing stored revisions. Stale,
missing or equal incoming revisions leave the existing event row fully untouched,
including first/last ingestion timestamps and last_run_id. A known incoming timestamp
can replace a stored null timestamp. Invalid newer records cannot overwrite valid data.

`load-staging` verifies and reads normalized/quarantine artifacts; `replay` rebuilds
records from verified archived responses with no HTTP and no dependency on staged
normalized/quarantine outputs. Both consume a Phase 2 state manifest. Replay checks
source counters and reruns the same normalization, validation, filtering and dedup.
Capped parents and error responses remain metadata observations, not duplicated input.

`backfill --load-database` integrates the existing CLI without changing local staging.
Extraction checkpoints remain independent. A failed DB load can be retried with
--resume without repeating successful extraction. Database resume trusts only
committed successful chunks with matching window/input fingerprints, not staging
success. Failed transactions roll back; a separate transaction records failed status.
Earlier successful windows remain durable. Killed processes can be retried safely;
Phase 4 will handle operational auditing of stale running statuses.

## Automated verification

Full suite with explicit database tests: **110 passed in 30.19s**.
This includes the original 88 tests and **22 PostgreSQL integration cases**. An
initial full pass had 107 tests; three additional DB recovery/integrity checks were
added and the final 110-test run passed. Final default offline run:
**88 passed, 22 skipped in 2.44s** (DB tests intentionally require --database-tests).
Compilation and dependency checks passed. git diff --check passed; only normal
Windows line-ending notices were emitted.

The final test database was `quaketrack_test_e22f9312e6a14f81b725fcaa33f0c437`, created fresh for that invocation on port 55432. An earlier invocation also
created its own isolated DB. Tests never reused, dropped or reset development or
any pre-existing DB. Only synthetic rows in each session-owned test DB were truncated
between test cases. These isolated DBs are retained for inspection.

Tests verify migration repeatability, preservation of rows, checksum drift detection,
transactional rollback of failed SQL migrations, required/numeric/coordinate/FK
constraints, negative/null values, new inserts, controlled newer updates, stale and
missing revision precedence, unchanged row timestamps, duplicate prevention,
parameterized event IDs, 503-row multi-batch loading, rollback after batch insertion,
concurrent writers and exact action counts, run/archive/quarantine metadata,
raw replay without HTTP or staged outputs, idempotency, corrupted archive refusal,
failed DB checkpoint retry, backfill integration after a DB failure and a two-window
interrupted DB load that resumes only its uncommitted window.
Controlled event updates occurred only in explicitly synthetic isolated fixtures.
No real development event was modified to simulate a source revision.

## Real-data loading and idempotency

Used the existing **37 real events** in data/staging/phase2-live: October 1, 2026
UTC, magnitude 4+, in two half-day windows. No new USGS request was required.
Original source archives were read only. All normalized source fields were compared
against stored rows and matched exactly, including timestamp precision, coordinates,
measurements, type/status, source details and URLs.

| Operation | Database run ID | Fetched | Inserted | Updated | Unchanged | Completed / skipped chunks |
|---|---|---:|---:|---:|---:|---:|
| Initial staged load | 4161d8d8-ac5a-4352-8bb6-eb7200f22e7b | 37 | 37 | 0 | 0 | 2 / 0 |
| Repeated staged load | 2a54eaf3-d366-45e8-bc5a-a67f8edbe852 | 37 | 0 | 0 | 37 | 2 / 0 |
| Database resume | af98a195-8ef2-4d57-8c77-84fe9f6cf0dd | 0 | 0 | 0 | 0 | 0 / 2 |
| Offline raw replay | 8831904b-c6c1-48eb-aefc-2c1fdc7ed60f | 37 | 0 | 0 | 37 | 2 / 0 |

All four DB runs finished successfully. Repeat and offline replay left every event
row identical to the first-load snapshot, including ingestion timestamps and last
accepted run reference. Snapshot SHA-256:
`4e87424bd6dfedb65e83f2fad8b7bbb46b18a7b23f41dd8c80451c23f3764e9d`.
Database resume skipped both chunks; its source/action counters were zero.
Raw replay was executed with requests.Session.get replaced by a function that
would raise on use; **zero HTTP calls occurred**.

Final development counts at verification: earthquakes=37, ingestion_runs=4,
ingestion_chunks=6, raw_archives=6, rejected_events=0, schema_migrations=3. Repeated
observations create provenance rows, not duplicate logical earthquakes. The sample
has no invalid features; actual quarantine insertion/references were verified using
invalid coordinate and null-feature mocks in the isolated test database.

Verification artifacts (ignored local files):

- data/staging/phase3-migrations.json and phase3-migrations-repeat.json.
- data/staging/phase3-first-load.json, phase3-repeat-load.json,
  phase3-resume-load.json and phase3-raw-replay.json.
- data/staging/phase3-event-snapshot.json.
- data/staging/phase3-verification.json (source comparison, joins, all SQL results).
- data/staging/phase3-test-db.json (isolated database identity).

## Analytical SQL verification

All **11 queries** executed successfully against the real dataset and were also
exercised with controlled values in isolated tests:

- overview: total 37; known magnitudes/depths 37; average depth
  68.01427027027026 km; maximum depth 637.906 km; maximum magnitude 5.3.
- daily_counts and monthly_counts: each reconciled to 37 for the selected period.
- magnitude_distribution: bucket counts summed to 37; fixture tests also covered
  null, negative and 6+ buckets.
- strongest_events: top magnitude matched the source maximum 5.3, with source IDs,
  occurrence time, magnitude type and coordinates retained.
- geographic_distribution: 10-degree coordinate-cell counts summed to 37, with
  optional bounding-box filtering verified. No country was inferred from place text.
- recent_activity: deterministic latest-first results within the selected period.
- magnitude_trends: UTC daily counts, known-magnitude denominator, mean and maximum.
- freshness: separate event/source/accepted-row/load timestamps were populated.
- run_summaries: recorded actual run action counts and resume skips.
- integrity: duplicate IDs=0, invalid events=0, orphan event references=0,
  inconsistent run counts=0, incomplete successful chunks=0.

Archive/run/chunk foreign-key joins also matched every stored metadata reference.
Read-only CLI overview, strongest_events and integrity commands were executed in
addition to the programmatic all-query verification. Aggregate date bounds are
half-open UTC; null-depth denominators and unknown-magnitude behavior are explicit.

## Files created

- sql/migrations/001_create_ingestion_runs.sql
- sql/migrations/002_create_earthquakes.sql
- sql/migrations/003_create_operational_tables.sql
- sql/analytics/overview.sql, daily_counts.sql, monthly_counts.sql,
  magnitude_distribution.sql, strongest_events.sql, geographic_distribution.sql,
  recent_activity.sql, magnitude_trends.sql, freshness.sql, run_summaries.sql,
  integrity.sql.
- src/quaketrack/storage/migrations.py
- src/quaketrack/storage/repository.py
- src/quaketrack/storage/replay.py
- src/quaketrack/storage/loader.py
- src/quaketrack/storage/analytics.py
- tests/conftest.py and tests/integration/test_postgres.py.
- docs/database.md and docs/phase3_report.md.

Files modified: src/quaketrack/cli.py, src/quaketrack/storage/database.py,
pyproject.toml (test marker only), README.md and docs/implementation_plan.md.
PROJECT.md, .env, Compose, dependency pins/lock, raw archives, Phase 1/2 test files
and local extraction/staging implementation were not modified.

## Commands executed

Read-only inspection used Get-Content, rg --files, git status --short,
git diff --stat/--check and Python/SQL identity queries. Migrations, loading and SQL
commands used the configured credentials without printing secrets:

```powershell
.\.venv\Scripts\python -m pytest -q --basetemp=.pytest_tmp/phase3-baseline --tb=short
.\.venv\Scripts\python -m pytest --database-tests -q --basetemp=.pytest_tmp/phase3-db-first --tb=short
.\.venv\Scripts\python -m pytest --database-tests -q --basetemp=.pytest_tmp/phase3-db-final --tb=short
.\.venv\Scripts\python -m quaketrack.cli migrate
.\.venv\Scripts\python -m quaketrack.cli load-staging --staging-dir data/staging/phase2-live
# The same load command was executed again for idempotency.
.\.venv\Scripts\python -m quaketrack.cli load-staging --staging-dir data/staging/phase2-live --resume
# migrate was rerun: applied=[] and event data remained unchanged.
.\.venv\Scripts\python -m quaketrack.cli analytics overview --start 2026-10-01 --end 2026-10-02
.\.venv\Scripts\python -m quaketrack.cli analytics strongest_events --start 2026-10-01 --end 2026-10-02 --limit 3
.\.venv\Scripts\python -m quaketrack.cli analytics integrity --start 2026-10-01 --end 2026-10-02
.\.venv\Scripts\python scripts/verify_database.py
.\.venv\Scripts\python -m pytest -q --basetemp=.pytest_tmp/phase3-final-offline --tb=short
.\.venv\Scripts\python -m compileall -q src scripts tests
.\.venv\Scripts\python -m pip check
```

Additional executed Python verification called load_staging(replay=True) with HTTP
forbidden, prepare_checkpoint(replay=True), database_identity, query_analytics for
all named queries, source-field comparison, full event snapshot comparison and
metadata join checks. stdout summaries were saved to the ignored files listed above.
PostgreSQL activity was inspected while the first integration run took longer than
expected; no database blocker or failed test was found.

## Known limitations and Phase 4 prerequisites

No mandatory Phase 3 criterion remains unverified, and no unresolved failure remains.
This verifies one bounded real day, not a 12-month backfill or throughput target.
Local Phase 2 state still has in-memory JSON/absolute-path portability limits; replay
needs its state manifest and original raw files. SQL assets are loaded from the
supported editable repository checkout, not a standalone packaged wheel.

Source updates without reliable updated timestamps cannot replace existing data;
source deletions are not reconciled automatically. A full-window transaction is
atomic, while earlier successful windows survive later failures. A forced process
kill can leave a stale running run; safe retry is supported but status auditing and
automated DB/network retry are later operational work. The DB user needs CREATEDB
for opt-in tests, and retained test DBs may be cleaned up manually when no longer
needed. There are no region/country administrative totals without reference data.

Before Phase 4: choose a supported Airflow/container Python version, extend run job
type/status constraints as needed via new migrations, add hourly window selection
and configurable lookback, orchestration retries/timeouts, stale-run monitoring,
reconciliation and operational recovery. The database success checkpoints and
transactional provenance are ready for that integration. **Phase 4 was not started.**
