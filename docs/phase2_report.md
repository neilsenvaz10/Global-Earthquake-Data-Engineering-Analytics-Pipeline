# Phase 2 completion report

Date: 2026-10-25, Asia/Calcutta. Scope: historical extraction and local ETL staging.
**Status: complete and verified.** No Phase 3 schema/loader, Airflow DAG, dashboard,
ML, or Git mutation was implemented/performed. PostgreSQL remains on port 55432.

## Initial audit and reused components

Read PROJECT.md, docs/phase1_report.md, README, all existing source/scripts and
smoke tests. The working tree was clean before changes. Phase 1 configuration,
UTC logging, requests, SQLAlchemy connectivity and the sample checker were reused.
The PostgreSQL SELECT 1 check succeeded before implementation after granting the
execution tool network access. The initial restricted attempt could not connect.
No existing .env credential or port was changed; no new dependency was required.

## Implementation and architecture

CLI -> bounded UTC windows -> retry-aware USGS client -> immutable raw archive ->
normalization/validation -> quarantine or eligible records -> revision-based
batch deduplication -> atomic local catalog + checkpoint -> structured run report.

- Start/end dates, minimum magnitude, window days, output directory, resume and
  maximum new windows are configurable through the CLI; operational defaults are
  centralized in the existing configuration module and .env.example.
- Half-open UTC windows are converted to inclusive USGS bounds at millisecond
  precision. A full response cap causes recursive splitting, not truncated input.
  Unsplittable capped intervals fail without advancing their checkpoint.
- HTTP timeouts, connection errors and transient status codes receive bounded
  exponential retries with jitter and Retry-After handling. All received response
  bodies, including failed HTTP attempts, are archived without overwriting files.
- Normalization retains source identity, timestamps, magnitude/type, depth,
  coordinates, place, event type/status, source network/code/IDs and relevant URLs.
  Null optional values remain null. Invalid present values are quarantined.
- Negative magnitudes/depths and epoch-zero timestamps are valid. Coordinates,
  finite numbers, required fields and integral epoch milliseconds are validated.
- USGS ID controls deduplication. Known newer valid source update metadata wins;
  older/missing updates cannot regress a stored version. Equal revision conflicts
  use documented deterministic batch ties and stored-version precedence across runs.
- A local staging catalog supports cross-run inserted/updated/unchanged counts.
  Catalog updates and completed checkpoints share one atomic state-file replacement.
  File locking prevents overlapping staging writers and is released on process exit.
- Resume verifies raw payload, metadata checksum consistency and staged artifact
  hashes before skipping a committed matching window. Failed windows remain retryable.
- Reports reconcile accepted/rejected, filtering, deduplication and catalog actions.
  Committed counts exclude failed-window attempts; their archived responses remain
  in the report. See etl_operations.md for the exact count equations.

## Files created

- src/quaketrack/cli.py
- src/quaketrack/pipeline.py
- src/quaketrack/ingestion/__init__.py
- src/quaketrack/ingestion/windowing.py
- src/quaketrack/ingestion/usgs_client.py
- src/quaketrack/ingestion/raw_archive.py
- src/quaketrack/transforms/__init__.py
- src/quaketrack/transforms/normalizer.py
- src/quaketrack/transforms/validator.py
- src/quaketrack/transforms/deduplicator.py
- src/quaketrack/transforms/quarantine.py
- src/quaketrack/storage/staging.py
- src/quaketrack/monitoring/__init__.py
- src/quaketrack/monitoring/run_metrics.py
- scripts/run_backfill.py
- tests/test_phase2.py
- docs/etl_operations.md
- docs/phase2_report.md

## Files modified

.env.example, .gitignore, README.md, pyproject.toml (description only),
src/quaketrack/config.py, src/quaketrack/__init__.py (description),
docs/source_contract.md and docs/implementation_plan.md.
PROJECT.md, the original Phase 1 scripts and tests, existing .env, Compose database
configuration and dependency lock remain unchanged. Raw, quarantine and staging
artifacts are ignored by Git. No dependencies were installed for Phase 2.

## Commands executed and test outcomes

Read-only repository/environment inspection: Get-Content, rg --files,
git status --short, git diff --stat, git diff --check, git check-ignore,
venv Python --version, docker compose ps, and the existing database checker.
Official USGS API documentation was rechecked before extraction implementation.

Automated checks (all executed successfully after implementation):

```powershell
.\.venv\Scripts\python -m pytest -q --basetemp=.pytest_tmp/phase2-final --tb=short
# 88 passed in 2.13s
.\.venv\Scripts\python -m pytest tests/test_phase1.py -q --basetemp=.pytest_tmp/phase1-regression-final --tb=short
# 21 passed in 0.73s
.\.venv\Scripts\python -m pytest tests/test_phase2.py -q --basetemp=.pytest_tmp/phase2-regression-final --tb=short
# 67 passed in 1.55s
.\.venv\Scripts\python -m compileall -q src scripts tests
.\.venv\Scripts\python -m pip check
# No broken requirements found.
.\.venv\Scripts\python -m quaketrack.cli backfill --help
.\.venv\Scripts\python scripts/verify_database.py
# PostgreSQL SELECT 1 succeeded, configured port 55432.
.\.venv\Scripts\python scripts/check_api.py
# Phase 1 check still succeeds: 10 real records, new immutable archive.
```

New tests cover leap-day/partial/adjacent windows, timezone conversion, end bounds,
sub-millisecond rejection, field mapping, nulls, epoch zero, malformed optional and
required values, invalid coordinates/timestamps, negative values, duplicate IDs,
newer/older/missing updates, equal-revision deterministic ties, invalid newer
candidates, retries, Retry-After seconds/date, retry exhaustion and permanent errors,
invalid JSON/schema, 204 empty responses, adaptive splitting, unsplittable caps,
quarantine provenance, count reconciliation, repeated execution, interruption,
limited-run continuation, damaged raw/metadata/normalized/quarantine files,
failed atomic replacement, failed checkpoint commit, writer locking and CLI options.
All artificial data is explicitly marked as offline mocks.

Live ingestion commands (stdout redirected to ignored JSON summary files):

```powershell
New-Item -ItemType Directory -Force data/staging | Out-Null
.\.venv\Scripts\python -m quaketrack.cli backfill --start 2026-10-01 --end 2026-10-02 --min-magnitude 4 --window-days 0.5 --output-dir data/staging/phase2-live
# Repeat the exact same command to observe staging idempotency.
.\.venv\Scripts\python -m quaketrack.cli backfill --start 2026-10-01 --end 2026-10-02 --min-magnitude 4 --window-days 0.5 --output-dir data/staging/phase2-live --resume
.\.venv\Scripts\python -m quaketrack.cli backfill --start 2026-10-01 --end 2026-10-02 --min-magnitude 4 --window-days 0.5 --output-dir data/staging/phase2-resume-check --max-windows 1
.\.venv\Scripts\python -m quaketrack.cli backfill --start 2026-10-01 --end 2026-10-02 --min-magnitude 4 --window-days 0.5 --output-dir data/staging/phase2-resume-check --resume
```

## Real-data results and retained failures

Only **2026-10-01 00:00 UTC through 2026-10-02 00:00 UTC, magnitude 4+** was verified.
The final unique local catalog contains **37** real earthquakes: 19 in the first
half-day and 18 in the second. All 37 normalized, none were quarantined or filtered,
and no within-window duplicate IDs appeared. This says nothing about a 12-month
backfill. Quarantine/duplicate/revision edge cases are proven by mocked tests,
not fabricated production data.

Two early runs received transient USGS 504 responses. Retry exhaustion correctly
failed without committing the affected window. One failed before any commit; the
next retained its first 19 records but did not mark the second half-day complete.
A diagnostic request succeeded with the documented implicit-UTC format, which was
adopted at the API boundary. This is not proof that timezone offsets caused 504s;
the source also returned a later transient 504 with the new format. Subsequent
runs completed. These failures are retained in raw archives and run reports.
The first output-redirection attempt failed before launch because its directory
was absent; creating data/staging resolved it.

| Run ID | Status | Completed / skipped windows | HTTP attempts | Fetched (committed) | Inserted / updated / unchanged |
|---|---|---:|---:|---:|---:|
| 04ccef72-8dd2-445e-b9f0-6a0bf67a6aec | failed | 0 / 0 | 4 | 0 | 0 / 0 / 0 |
| 0d81aae3-183c-4c0b-8fdf-482088d4752c | failed | 1 / 0 | 5 | 19 | 19 / 0 / 0 |
| 2a756017-e993-4845-b669-1efc11af0d48 | success | 2 / 0 | 2 | 37 | 0 / 0 / 37 |
| 8cdd38b9-21fb-4d5d-8f77-c7d01b58c95d | success | 2 / 0 | 2 | 37 | 18 / 0 / 19 |
| be522ea1-d02f-43c5-aba4-8fe7fcc6d955 | success | 0 / 2 | 0 | 0 | 0 / 0 / 0 |
| 90354928-4736-4c16-8548-b4c8928d0fae | limited | 1 / 0 | 1 | 19 | 19 / 0 / 0 |
| db20ef27-ee85-4da4-b017-529e74e0fd43 | success | 1 / 1 | 1 | 18 | 18 / 0 / 0 |

The full repeat inserted 0, updated 0, left all 37 unchanged and retained 37 unique
IDs. The completed-run resume skipped two windows with zero HTTP attempts. The
separate limited run inserted 19; continuation skipped one verified window and
inserted the remaining 18, also reaching 37. Interrupted-run recovery and rollback
at a failed atomic commit were additionally verified in automated tests.

## Artifact integrity and replay checks

A separate offline Python verification inspected every run report, called
verify_archive for its references, reconciled its counters, checked each committed
artifact hash, re-normalized/deduplicated archived source bytes and compared the
result exactly with committed normalized records. All records also passed the
expected date-range and magnitude checks. Results are saved locally in
`data/staging/phase2-verification.json`:

- phase2-live: 5 reports, 13 HTTP response archives verified, 2 checkpoints,
  37 normalized/catalog records, 0 quarantined.
- phase2-resume-check: 2 reports, 2 HTTP response archives verified, 2 checkpoints,
  37 normalized/catalog records, 0 quarantined.
- The original Phase 1 archive checksum still matches its metadata.

The first ad hoc replay diagnostic used Windows' default text encoding and failed
on a UTF-8 source label; explicitly reading UTF-8 resolved it. Application archive
and metadata readers already use bytes/UTF-8. The successful rerun verified all
artifacts. No integrity failure remains unresolved.

## Limitations and Phase 3 handoff

- Local state is an in-memory JSON staging catalog rewritten per complete window.
  This suits the current demonstration, not unmeasured large-scale backfills.
- Raw references use absolute paths; relocating files can break resume references.
- Extraction observes a mutable catalog, not a point-in-time snapshot. Later source
  revisions/deletions need reconciliation; resume is not a source refresh.
- A full one-millisecond source interval fails rather than attempting unstable
  offset pagination. Long Retry-After delays fail visibly for later manual retry.
- Counter definitions distinguish accepted-before-filtering, eligible, within-window
  duplicates and catalog actions; no PostgreSQL insert count is claimed.
- There was no real null/invalid/duplicate measurement in this bounded sample;
  offline tests verify those behaviors.
- Phase 3 must add schema/migrations, transactional PostgreSQL upserts, constraints,
  run/chunk/rejection provenance, indexes, analytics SQL and database integration
  tests, including how extra retained source fields fit the blueprint schema.
- Airflow scheduling and operational DB checkpoints remain Phase 4; dashboards
  remain Phase 5. No Phase 3 work was started.

All mandatory Phase 2 criteria have verification evidence. No unresolved blocker
remains. No staging, commit, push, merge, branch creation or other Git mutation
was performed during Phase 2.
