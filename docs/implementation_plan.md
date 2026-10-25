# Implementation plan and initial audit

The initial repository contained only PROJECT.md (untracked) and a one-line README.md.
No application, environment template, dependencies, tests, or deployment existed.
PROJECT.md remains the source of truth and is unchanged.

1. **Phase 1 (completed):** package and local environment, centralized environment configuration,
   UTC logging, bounded historical connectivity sample with provenance, read-only PostgreSQL
   check, database-only Compose service, offline smoke tests, source contract and setup guide.
2. **Phase 2 (completed):** reusable extractor with bounded retries,
   historical windows, replayable archives, normalization, validation, quarantine,
   deterministic deduplication and unit tests. No database loading yet.
3. **Phase 3 (completed):** migrations, transactional revision-aware upserts, indexes, analytical
   queries, integrity and repeat-load integration tests.
4. **Phase 4 (completed):** Airflow deployment in a supported Linux/container Python runtime,
   hourly incremental updates, 72-hour lookback, checkpoints, run metrics and recovery.
5. **Phase 5:** PostgreSQL-backed Streamlit/Plotly dashboard, end-to-end testing,
   reproducible deployment, screenshots and demo documentation.

## Decisions and environment risks

- Create files progressively; no empty DAG, dashboard, migration or ETL placeholders.
- Use requests and SQLAlchemy/psycopg; defer Pandas, Airflow, Streamlit and Plotly
  installation until their implementation phases. Their compatibility must be checked then.
- Python 3.14.6 is installed; `py` reports no registered Python. Commands use `python`.
- Docker CLI 29.8.2 and Compose v5.5.1 exist, but the Linux engine is stopped.
  No psql executable is on PATH. Docker Desktop was subsequently started and
  PostgreSQL connectivity passed; see phase1_report.md for the resolved blockers.
- PostgreSQL 17 is selected for the database-only Compose service. No application image
  or Airflow setup is needed in Phase 1. The image tag receives maintenance patches.
- Source proof of concept performs one request, with a timeout and ten-event cap.
  Full retry/pagination, raw-layer recovery and transformation belong to Phase 2.
- No database schema is created by the connectivity test; it runs only SELECT 1.
- Database passwords have no fallback. Compose refuses missing/empty passwords.
- Relative archive paths resolve against the project root; process variables override .env.
- Future backfill defaults (12 months), lookback (72 hours), retries and schedule
  will be added when used; Phase 1 does not pretend these settings already run a pipeline.

## Phase 2 verification

Phase 2 was explicitly authorized and is complete. It reuses Phase 1 configuration,
logging and dependencies, adds local staging for repeat/resume checks, and leaves
PostgreSQL loading for Phase 3. See phase2_report.md for 88 passing tests and bounded
real-data results. Phase 3 was subsequently approved and completed; see phase3_report.md. Phase 4
still requires explicit approval.

## Phase 3 verification

Added explicit SQL migrations, PostgreSQL repository/loading, offline raw replay,
separate DB checkpoints and analytical queries. All 110 tests passed, including
22 tests in a new isolated database. The 37 real Phase 2 events were loaded and
verified in development. No Airflow or dashboard was implemented.

## Phase 4 verification

Pinned Airflow 3.3.2 with isolated metadata DB and LocalExecutor is running. All six
bounded live DAG runs succeeded. The suite passed 125 application/DB cases plus
3 real DAG checks. Scope-specific durable incremental coverage, quality-gated
checkpoint advancement, retry recovery and persistent restart were verified.
The hourly DAG is left paused and failure simulation is disabled. Phase 5 was
subsequently authorized; see phase5_report.md for the dashboard and final checks.

## Phase 5 verification

Five Streamlit/Plotly views now share read-only, parameterized database filters.
The final report records automated tests, live page checks, a bounded DAG test,
preservation hashes, actual coverage and local deployment commands. No large
backfill or unattended ingestion was enabled. Changes remain uncommitted.
