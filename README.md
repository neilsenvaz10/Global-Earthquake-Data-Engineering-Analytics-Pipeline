# QuakeTrack

Global Earthquake Data Engineering & Analytics Pipeline. `PROJECT.md` is the primary
specification. **Phases 1 and 2 are implemented.** Historical extraction, validation,
quarantine and repeatable local staging work. PostgreSQL loading, migrations,
Airflow and the Streamlit dashboard belong to later phases.

## Local setup (PowerShell)

Use Python 3.11-3.14. Tested here with Python 3.14.6. No new dependencies are needed
for Phase 2; the existing pinned dependencies support the implementation.

```powershell
python -m venv .venv
# If venv fails at ensurepip but creates the environment, bootstrap with:
# python -m pip --python .venv install pip
.\.venv\Scripts\python -m pip install -e ".[dev]"
# Fresh clones only; preserve an existing .env:
Copy-Item .env.example .env
```

The existing ignored `.env` has a generated password and `POSTGRES_PORT=55432`.
Keep this file for the initialized database. For a fresh clone, choose a password
and available port before starting PostgreSQL. Process environment variables override
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

## Database and later phases

Keep PostgreSQL on localhost:55432 in this checkout. Compose binds only loopback
and persists its named volume. `docker compose down` stops it without deleting data.
Changing `.env` does not rotate existing PostgreSQL passwords. Use administration
commands for role changes. For an existing database, configure its host/port and
credentials instead of starting Compose. Future containers will use host `postgres`.

Phase 3 requires approval and adds migrations, transactional PostgreSQL loading,
revision-aware upserts, indexes and analytical SQL. Phase 4 adds Airflow; Phase 5
adds the dashboard. No ML or prediction is included.

- [Implementation plan](docs/implementation_plan.md)
- [Source contract](docs/source_contract.md)
- [ETL operations and recovery](docs/etl_operations.md)
- [Phase 1 report](docs/phase1_report.md)
- [Phase 2 report](docs/phase2_report.md)
