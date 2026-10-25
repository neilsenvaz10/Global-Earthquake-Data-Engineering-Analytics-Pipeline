# QuakeTrack — Global Earthquake Data Engineering & Analytics Pipeline

> **Project type:** Academic Data Engineering mini-project  
> **Version:** 1.0 — implementation blueprint  
> **Status:** Planning; no implementation is claimed in this document  
> **Scope:** Global historical and regularly updated earthquake-event analytics, **not** earthquake prediction or emergency alerting

---

## 1. Executive summary

**QuakeTrack** is an end-to-end data engineering system that retrieves earthquake event records from the **United States Geological Survey (USGS) Earthquake Catalog API**, preserves source data, cleans and validates records, incrementally loads them into PostgreSQL, schedules repeatable jobs, and serves an interactive Streamlit analytics dashboard.

The system should answer questions such as:

- How many cataloged earthquake events occurred during a selected period?
- How are events distributed by magnitude, depth, time, and geographical area?
- Where are recorded epicenters concentrated on a world map?
- What are the largest-magnitude recorded events in a chosen window?
- How fresh is the local database, and did the latest ingestion run succeed?
- How many source records were fetched, accepted, rejected, updated, or deduplicated?

The **engineering work**, not simply the charts, is the core contribution: reproducible ingestion, source preservation, idempotent upserts, temporal correctness, operational visibility, tests, and repeatable deployment.

### 1.1 One-sentence problem statement

Earthquake catalog data is available online, but turning frequently updated event feeds into a trustworthy, queryable, historical analytics dataset requires automated ingestion, validation, deduplication, persistent storage, scheduling, and monitoring.

### 1.2 Proposed solution

Build a five-phase pipeline:

1. **Setup and source integration** — environment, API proof of concept, source contract.
2. **Ingestion and transformation** — resilient fetching, raw storage, normalization, validation.
3. **Database and analytical modeling** — PostgreSQL schema, upserts, analytical views.
4. **Automation and monitoring** — scheduled incremental loads, retry/recovery, run metrics.
5. **Dashboard, testing and delivery** — interactive insights, verification, Dockerized demo, report.

### 1.3 Why this is a Data Engineering project

QuakeTrack demonstrates **extract–transform–load (ETL)**, API integration, raw-to-curated data layers, schema design, data quality rules, incremental processing, idempotency, scheduling, observability, analytical SQL, and reproducible deployment. A prediction model is neither necessary nor in scope.

---

## 2. Goals, non-goals, and success criteria

### 2.1 Primary goals

| ID | Goal | Evidence required |
|---|---|---|
| G1 | Retrieve earthquake events from a real public source | Successful API request and archived source response |
| G2 | Clean and standardize the source records | Unit tests and normalized examples |
| G3 | Persist historical events in PostgreSQL | SQL counts and schema inspection |
| G4 | Prevent duplicate logical events | Repeated-load test shows unchanged unique-event count |
| G5 | Update changed events correctly | Same USGS ID with newer source update timestamp is reflected in database |
| G6 | Run scheduled updates automatically | Airflow DAG execution history and logs |
| G7 | Provide useful analytics | Working dashboard with map, trends, filters, and KPIs |
| G8 | Make the project demonstrable and reproducible | Setup guide, configuration example, tests, screenshots, and demo script |

### 2.2 Non-goals

- No earthquake prediction, forecasting, early-warning system, or tsunami prediction.
- No claim that the dataset contains **every** earthquake worldwide; detection, catalog inclusion, and reporting completeness vary.
- No claim that an earthquake's magnitude is a direct measure of damage or human impact.
- No guaranteed real-time feed, public-safety service-level agreement, or emergency notification capability.
- No required paid API, large distributed cluster, Spark deployment, or ML training.
- No assumption that a USGS textual `place` label is a verified country field.
- No scraping of third-party websites when the public API suffices.

### 2.3 Definition of done

The project is complete when a fresh developer environment can run the documented setup; a historical backfill loads real USGS events; repeated runs remain idempotent; a later incremental run ingests new and updated events; Airflow displays run status; the Streamlit dashboard queries PostgreSQL; and the verification suite passes. The final report must distinguish implemented functionality from proposed enhancements.

---

## 3. Audience and use cases

**Primary users:** course evaluator, student developer, analyst exploring seismic-event distributions.

**Example user stories:**

- As an analyst, I want to filter by date range and magnitude to compare event frequencies.
- As an analyst, I want a world map with epicenter coordinates so I can inspect spatial clusters.
- As a reviewer, I want to run the pipeline twice and see that events are not duplicated.
- As a reviewer, I want to see that changed USGS event records update existing rows.
- As an operator, I want run logs and error counts so failures can be diagnosed.
- As a user, I want to see the database's last successful refresh timestamp.

---

## 4. Scope and implementation assumptions

### 4.1 Geographic and time coverage

- **Geography:** worldwide, limited to records returned by the selected USGS query.
- **Historical baseline:** a **configurable 12-month backfill** is the initial recommended scope; expand only after measuring data volume and query behavior.
- **Magnitude policy:** configurable minimum magnitude, with **2.5** as the initial suggested setting to keep data volumes manageable. This is a project filtering choice, **not** a scientific threshold for what constitutes an earthquake.
- **Incremental frequency:** **hourly** as an initial target, subject to source policies and actual needs.
- **Recheck window:** re-fetch the **previous 72 hours** on each incremental run to capture revisions; make this configurable and add a wider periodic reconciliation if necessary.
- **Time zone:** store all timestamps in **UTC**; allow presentation conversions only when explicitly labeled.

These defaults are design proposals, not guarantees about USGS coverage, API limits, or event-update timing.

### 4.2 Important source-data caveats

USGS event records may be updated after initial publication. An event ID should be treated as the logical identity; a newer `updated` timestamp indicates a changed source representation. Magnitude and depth can be null. The `place` string is descriptive, not a normalized country code. Different magnitude scales and catalog completeness can complicate comparisons. Event counts depend on magnitude threshold, source availability, date range, and query filters. A recent-event dashboard is **not** a substitute for authoritative hazard information.

### 4.3 Scope control

**Required MVP:** one API source, one PostgreSQL database, one DAG, one dashboard, meaningful tests, documented setup.

**Optional stretch work:** daily aggregate materialized views, country enrichment using a properly licensed geospatial dataset, second data source, advanced spatial queries/PostGIS, deployment to a hosted server, source-to-target reconciliation reports.

Do not implement optional work until the core pipeline is correct.

---

## 5. System architecture

```text
                      USGS Earthquake Catalog API
                                 |
                         HTTPS / GeoJSON
                                 |
                                 v
                   [Python Extraction Client]
                   pagination / chunking / retries
                                 |
                    +------------+------------+
                    |                         |
                    v                         v
          [Raw JSON Archive]           [Run Metadata]
          immutable source files       timestamps, counts,
          + request parameters         status, errors
                    |
                    v
            [Validation + Transform]
            normalize timestamps, numeric values,
            source IDs, coordinate order, quality rules
                    |
             +------+-------+
             |              |
             v              v
        [Valid Rows]   [Rejected Rows]
             |         quarantine + reason
             v
      [PostgreSQL Curated Tables]
      idempotent INSERT ... ON CONFLICT
             |
             v
       [SQL Analytical Views]
             |
             v
       [Streamlit Dashboard]
       KPIs | Trends | Map | Tables

       [Apache Airflow Scheduler]
                 |
        orchestrates extraction,
        transform, load, quality checks
```

### 5.1 Component responsibilities

| Component | Responsibility | Proposed technology |
|---|---|---|
| Source | Published event data | USGS FDSN Event API |
| Extractor | Query, chunk, retry, record provenance | Python `requests` or `httpx` |
| Raw layer | Preserve returned payloads and request metadata | Local JSON/GeoJSON files |
| Transformer | Convert nested source records into typed rows | Python + Pandas (optional for batch processing) |
| Validator | Enforce domain and schema checks | Python functions + tests |
| Curated store | Upsert event records and log runs | PostgreSQL |
| Orchestrator | Run scheduled jobs, retries, dependencies | Apache Airflow |
| Analytics | Query aggregates and event rows | PostgreSQL SQL |
| Dashboard | Display filters, KPIs, maps and trends | Streamlit + Plotly |
| Local deployment | Reproducible services | Docker Compose |

### 5.2 Why not use Spark?

For the initial API data volume and a college mini-project, Python/Pandas + PostgreSQL is simpler and easier to verify. Spark adds operational overhead without necessarily improving the learning outcome. Consider it only if measured data scale justifies distributed processing.

---

## 6. Data source specification

### 6.1 Primary source

**USGS Earthquake Catalog API:**

- Documentation: https://earthquake.usgs.gov/fdsnws/event/1/
- Query endpoint: `https://earthquake.usgs.gov/fdsnws/event/1/query`
- Preferred format: `geojson`
- Example request (illustrative; validate parameters and source availability during Phase 1):

```http
GET https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2026-10-01&endtime=2026-10-02&minmagnitude=2.5&orderby=time-asc
```

Typical GeoJSON structure:

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "id": "example-event-id",
      "properties": {
        "mag": 4.5,
        "place": "Example region",
        "time": 1790812800000,
        "updated": 1790816400000,
        "url": "https://earthquake.usgs.gov/",
        "type": "earthquake"
      },
      "geometry": {
        "type": "Point",
        "coordinates": [140.2, 35.6, 32.0]
      }
    }
  ]
}
```

**All IDs and values in this example are fictional.** GeoJSON coordinate order is **longitude, latitude, depth**. USGS event times are commonly expressed as epoch milliseconds; convert explicitly to UTC and verify actual field definitions against the source documentation.

### 6.2 Proposed source fields

| Source field | Meaning | Destination |
|---|---|---|
| `feature.id` | Stable event identifier | `earthquakes.event_id` |
| `properties.time` | Event occurrence time | `occurred_at` |
| `properties.updated` | Last source update | `source_updated_at` |
| `properties.mag` | Reported magnitude | `magnitude` |
| `properties.place` | Human-readable region | `place_description` |
| `properties.type` | Event type | `event_type` |
| `properties.url` | USGS event URL | `source_url` |
| `geometry.coordinates[0]` | Longitude | `longitude` |
| `geometry.coordinates[1]` | Latitude | `latitude` |
| `geometry.coordinates[2]` | Depth in km | `depth_km` |

### 6.3 API extraction strategy

1. Read start/end dates, minimum magnitude, and optional event type from configuration.
2. Split a long historical range into **small non-overlapping time windows** (e.g., days or weeks), rather than assuming one giant request will succeed.
3. Handle the source's response-size and result-limit behavior using documented query parameters and smaller windows; verify whether `limit`/`offset` are suitable before implementing pagination.
4. Archive each successful response with query parameters, fetch time, source URL (without secrets), response hash, and run ID.
5. Apply retry with exponential backoff and jitter for transient network failures and supported retryable status codes; respect `Retry-After` if supplied.
6. Fail a chunk clearly after a bounded retry budget; do not silently skip it.
7. Keep checkpoint state so interrupted historical backfills can resume without losing completed chunks.
8. Filter the source to earthquake-type events if the desired scope is strictly earthquakes; do not inadvertently count quarry blasts or other event types as earthquakes.
9. Test the handling of zero results, invalid JSON, timeouts, duplicate records, and corrected source events.

### 6.4 Provenance

Each batch should retain: `run_id`, source name, request start/end, fetch timestamp, payload hash, record count, and archive path. Curated rows should retain the last ingestion run ID and source update time.

---

## 7. Database design

### 7.1 Data layers

**Raw:** source responses stored as immutable files; sufficient to replay a transformation without re-querying the API.

**Curated:** normalized event rows in PostgreSQL, one current row per USGS event ID.

**Analytics:** SQL views or materialized aggregates for dashboard queries; optional until query performance requires them.

### 7.2 Core tables

#### `earthquakes`

| Column | PostgreSQL type | Rules |
|---|---|---|
| `event_id` | TEXT | Primary key; USGS event identifier |
| `occurred_at` | TIMESTAMPTZ | Not null; UTC semantics |
| `source_updated_at` | TIMESTAMPTZ | Nullable if absent, but source expectation checked |
| `magnitude` | DOUBLE PRECISION | Nullable; do not invent values |
| `depth_km` | DOUBLE PRECISION | Nullable; retain legitimate negative depths if source allows them |
| `latitude` | DOUBLE PRECISION | Between -90 and 90 |
| `longitude` | DOUBLE PRECISION | Between -180 and 180 |
| `place_description` | TEXT | Nullable; unstructured source description |
| `event_type` | TEXT | Preserve event classification |
| `source_url` | TEXT | Nullable |
| `first_ingested_at` | TIMESTAMPTZ | Default current timestamp |
| `last_ingested_at` | TIMESTAMPTZ | Last accepted ingest time |
| `last_run_id` | UUID | Reference to ingestion run |

#### `ingestion_runs`

`run_id`, `job_type` (`backfill`/`incremental`/`reconcile`), `started_at`, `finished_at`, `status`, `window_start`, `window_end`, `records_fetched`, `records_inserted`, `records_updated`, `records_unchanged`, `records_rejected`, `error_message`, `source_response_count`.

#### `ingestion_chunks`

`chunk_id`, `run_id`, `window_start`, `window_end`, `status`, `attempt_count`, `archive_path`, `payload_sha256`, `records_fetched`, `completed_at`, `error_message`.

#### `rejected_events`

`rejection_id`, `run_id`, `source_event_id`, `reason_code`, `reason_detail`, `raw_event_json`, `rejected_at`.

#### Optional `pipeline_state`

`pipeline_name`, `last_successful_window_end`, `updated_at`. Advance a checkpoint only after the corresponding chunk is durably committed and quality checks pass.

### 7.3 Example PostgreSQL DDL

```sql
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id UUID PRIMARY KEY,
    job_type TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    records_fetched INTEGER NOT NULL DEFAULT 0,
    records_inserted INTEGER NOT NULL DEFAULT 0,
    records_updated INTEGER NOT NULL DEFAULT 0,
    records_unchanged INTEGER NOT NULL DEFAULT 0,
    records_rejected INTEGER NOT NULL DEFAULT 0,
    source_response_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS earthquakes (
    event_id TEXT PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL,
    source_updated_at TIMESTAMPTZ,
    magnitude DOUBLE PRECISION,
    depth_km DOUBLE PRECISION,
    latitude DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    place_description TEXT,
    event_type TEXT,
    source_url TEXT,
    first_ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_run_id UUID REFERENCES ingestion_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_eq_occurred_at ON earthquakes (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_eq_magnitude ON earthquakes (magnitude);
CREATE INDEX IF NOT EXISTS idx_eq_event_type ON earthquakes (event_type);
```

**Implementation note:** additional DDL for `ingestion_chunks`, `rejected_events`, and `pipeline_state` belongs in migrations. A single source event may appear in multiple raw archives; only the curated `earthquakes` table must have one row per logical event.

### 7.4 Upsert policy

- Use `event_id` as the unique conflict key.
- Insert unseen events.
- Update an existing event when the incoming `source_updated_at` is newer than the stored value, with a documented fallback for missing update timestamps.
- Do **not** overwrite newer curated data with stale API responses.
- Record inserted/updated/unchanged counts separately.
- Use transactions so a failed batch does not leave an ambiguous partial state.
- A second identical run should insert **zero new events** and should not alter source-data fields.
- Keep event occurrence time separate from the time it was ingested.

### 7.5 Analytical SQL examples

**Daily event counts:**

```sql
SELECT DATE(occurred_at AT TIME ZONE 'UTC') AS event_day,
       COUNT(*) AS event_count
FROM earthquakes
WHERE event_type = 'earthquake'
GROUP BY 1
ORDER BY 1;
```

**Magnitude buckets (illustrative):**

```sql
SELECT CASE
         WHEN magnitude IS NULL THEN 'Unknown'
         WHEN magnitude < 2 THEN '<2'
         WHEN magnitude < 4 THEN '2–<4'
         WHEN magnitude < 6 THEN '4–<6'
         ELSE '6+'
       END AS magnitude_bucket,
       COUNT(*) AS event_count
FROM earthquakes
WHERE event_type = 'earthquake'
GROUP BY 1
ORDER BY 1;
```

**Largest recorded events:**

```sql
SELECT event_id, occurred_at, magnitude, depth_km, place_description
FROM earthquakes
WHERE event_type = 'earthquake' AND magnitude IS NOT NULL
ORDER BY magnitude DESC, occurred_at DESC
LIMIT 20;
```

### 7.6 Geographic caution

Avoid claiming exact **country-wise** totals from substring matching against `place_description`. The MVP should provide a world map, coordinate-based filtering, and source place labels. Country analytics require an explicit geographic-enrichment step (e.g., licensed boundaries and a point-in-polygon operation), with ocean events handled separately.

---

## 8. ETL specification

### 8.1 Extract

Input: UTC time window and filters. Output: raw source response plus fetch metadata. Validate HTTP status and response format. Track errors, retries, and payload hashes.

### 8.2 Transform

1. Parse each GeoJSON feature and its nested `properties`/`geometry`.
2. Normalize `event_id` to nonempty text.
3. Convert epoch-millisecond timestamps to timezone-aware UTC datetimes.
4. Read coordinates in the correct `[longitude, latitude, depth]` order.
5. Convert numeric fields safely, preserving null values.
6. Validate latitude and longitude bounds.
7. Preserve `place_description` as a source label rather than a normalized country.
8. Keep only the configured event types for the curated analytics dataset.
9. Distinguish **rejected invalid rows** from **legitimate rows with missing optional values**.
10. Deduplicate within a batch by event ID, preferring the newest source update timestamp.
11. Emit structured validation outcomes and counts.

### 8.3 Load

Load in bounded batches. Use parameterized SQL or an established PostgreSQL driver/ORM. Upsert transactionally. Persist run metrics. Mark a window complete only after successful database commit and checks.

### 8.4 Example transformation contract

```python
# Pseudocode — contract illustration, not complete implementation

def normalize_feature(feature: dict) -> dict:
    event_id = require_nonempty_string(feature.get("id"))
    props = feature["properties"]
    coords = feature["geometry"]["coordinates"]
    longitude, latitude = float(coords[0]), float(coords[1])
    depth = float(coords[2]) if len(coords) > 2 and coords[2] is not None else None
    validate_coordinates(latitude, longitude)
    return {
        "event_id": event_id,
        "occurred_at": epoch_ms_to_utc(props["time"]),
        "source_updated_at": epoch_ms_to_utc(props["updated"]) if props.get("updated") else None,
        "magnitude": optional_float(props.get("mag")),
        "depth_km": depth,
        "latitude": latitude,
        "longitude": longitude,
        "place_description": props.get("place"),
        "event_type": props.get("type"),
        "source_url": props.get("url"),
    }
```

### 8.5 Data quality checks

| Check | Expected behavior |
|---|---|
| Missing event ID | Reject and quarantine |
| Missing occurrence time | Reject and quarantine |
| Invalid coordinates | Reject and quarantine |
| Null magnitude | Retain event; display as unknown or exclude only from magnitude-specific metrics |
| Null depth | Retain event; display as unknown |
| Duplicate event ID within payload | Resolve deterministically and count |
| Repeated historical load | No duplicate database rows |
| Older update arrives after newer one | Do not regress curated row |
| Non-earthquake event | Exclude from earthquake-only analytics, or explicitly segregate |
| Empty valid API response | Successful zero-record chunk, not a pipeline failure |
| API/network failure | Retry within limits, then fail with actionable logs |
| Unexpected source schema | Reject affected records, record error, and flag the run |
| Count reconciliation | `fetched = accepted source rows + rejected rows`, with dedup/filtered counts tracked separately |

---

## 9. Phase-by-phase implementation plan

# PHASE 1 — Project Setup and Source Integration

### Objective

Create a reproducible development environment, confirm API connectivity, establish source assumptions, and freeze the initial data contract before implementing the full ETL.

### Tasks

1. Initialize project folder and Python virtual environment.
2. Create `pyproject.toml` or `requirements.txt` with pinned compatible dependencies.
3. Add `.env.example` and configuration loader; keep secrets out of source control.
4. Start PostgreSQL locally or through Docker Compose.
5. Read and document the USGS API's request parameters, response fields, result limits, and usage expectations.
6. Implement a small command-line API connectivity test.
7. Fetch a small, fixed historical window and archive the returned GeoJSON.
8. Inspect records with null magnitudes, depth values, event types, and timestamps.
9. Define a versioned normalized record contract and initial configuration defaults.
10. Write `README.md` with installation and first-run instructions.

### Deliverables

- Working Python environment.
- PostgreSQL connection test.
- USGS API proof of concept.
- Example archived raw response (small sample; ensure licensing/provenance is noted).
- Configuration template and documented source contract.
- Initial folder structure and setup guide.

### Acceptance criteria

- A fresh environment can run the API test.
- The script produces an archived response with fetch metadata.
- Timestamp and coordinate conventions are documented.
- The project can connect to PostgreSQL.
- No credentials or environment-specific secrets are committed.

### Suggested effort

**1–2 working days**, depending on environment readiness.

---

# PHASE 2 — Data Ingestion and ETL Pipeline

### Objective

Build a reusable pipeline that can retrieve a configured time range, preserve raw data, transform nested GeoJSON into normalized records, and report quality issues.

### Tasks

1. Implement a typed API client with request timeout and bounded retries.
2. Split historical backfill into manageable time chunks.
3. Persist raw responses with deterministic archive names and checksums.
4. Implement source parsing and field normalization.
5. Convert epoch milliseconds to UTC datetimes.
6. Validate event IDs, coordinates, and required fields.
7. Handle optional fields such as magnitude and depth without fabricating values.
8. Filter configured event types.
9. Deduplicate within each batch by event ID and source update time.
10. Create a quarantine format for rejected rows.
11. Track fetched, transformed, rejected, filtered, and deduplicated counts.
12. Add unit tests for normal and malformed records.
13. Provide a CLI such as `python -m quaketrack.cli backfill --start ... --end ...` (to be implemented).

### Deliverables

- API extraction module.
- Raw archive writer.
- Normalizer and validator modules.
- Quarantine/rejection output.
- Historical backfill command.
- Unit test suite and sample fixtures.

### Acceptance criteria

- A configured historical range can be fetched in chunks.
- Correctly parsed rows match the source values and coordinate order.
- Missing optional fields do not crash the job.
- Invalid rows are reported, not silently discarded.
- Transient failures are retried; permanent failures are visible.
- Same raw payload produces deterministic normalized records.

### Suggested effort

**2–3 working days**.

---

# PHASE 3 — PostgreSQL Database and Analytical Modeling

### Objective

Persist a reliable curated earthquake catalog and expose useful, testable SQL analytics.

### Tasks

1. Create migration scripts for `ingestion_runs`, `earthquakes`, `ingestion_chunks`, and `rejected_events`.
2. Implement transactional inserts and conflict-aware upserts.
3. Enforce uniqueness by source event ID.
4. Implement update precedence using source `updated` timestamps.
5. Store ingestion provenance and quality counts.
6. Create indexes for time and magnitude queries.
7. Load the initial historical backfill.
8. Build SQL queries/views for daily counts, magnitude buckets, largest events, and depth distribution.
9. Verify event totals against source responses and filtering rules.
10. Add integration tests using a disposable PostgreSQL test database.
11. Test repeated loads, newer updates, older updates, and partial failures.
12. Document the entity relationships and table dictionary.

### Deliverables

- SQL migrations.
- PostgreSQL loader and upsert logic.
- Curated historical dataset.
- Analytics queries/views.
- Schema diagram and data dictionary.
- Database integration tests.

### Acceptance criteria

- Historical events persist in PostgreSQL.
- No duplicate event IDs exist.
- Re-running the same load does not increase unique-event count.
- Newer source revisions update the correct event.
- Older source revisions cannot overwrite newer records.
- Analytics SQL returns consistent, explainable results.
- Failed batch transactions do not leave partially committed data.

### Suggested effort

**2–3 working days**.

---

# PHASE 4 — Airflow Automation, Incremental Updates and Monitoring

### Objective

Turn the manual ETL into a repeatable, scheduled, observable process that can recover from common failures.

### Tasks

1. Set up Airflow using an appropriate supported version and local deployment configuration.
2. Create an Airflow DAG with extract, transform, load, and quality-check tasks.
3. Configure hourly incremental ingestion (initial proposal).
4. Re-fetch a configurable lookback window to capture corrected events.
5. Implement durable checkpointing and backfill resume behavior.
6. Add task retries, timeouts, and useful failure messages.
7. Record run status, record counts, durations, and failure reasons in PostgreSQL.
8. Implement one active writer policy or database locking to avoid overlapping loads corrupting run state.
9. Provide a manual trigger option for demonstration.
10. Test repeated DAG runs and simulate one recoverable API failure.
11. Add a basic pipeline-health query/view for the dashboard.
12. Document operational recovery: retry, replay from raw archive, rerun a date range.

### Suggested DAG flow

```text
start
  |
compute_window
  |
fetch_and_archive
  |
normalize_and_validate
  |
transactional_upsert
  |
quality_and_reconciliation_checks
  |
record_success_and_advance_checkpoint
  |
end
```

On failure, record the failed status and **do not advance the successful checkpoint**.

### Deliverables

- Airflow DAG.
- Incremental load configuration.
- Retry and recovery logic.
- Run-history tables and operational logs.
- Pipeline-health metrics.
- Airflow execution screenshots for the final report.

### Acceptance criteria

- DAG runs successfully when manually triggered.
- Scheduled execution can be enabled and observed.
- Repeated overlapping windows do not duplicate events.
- A source event update is reflected after re-ingestion.
- A simulated failure is visible and recoverable.
- Latest successful refresh time is queryable.

### Suggested effort

**2–3 working days**.

---

# PHASE 5 — Streamlit Dashboard, End-to-End Testing and Delivery

### Objective

Create a clear analytics experience, verify the full pipeline, and package the project for academic evaluation.

### Tasks

1. Create a Streamlit dashboard that reads **PostgreSQL**, not raw CSV files.
2. Add date-range, magnitude, and depth filters.
3. Show total recorded events, largest recorded magnitude, average depth for non-null rows, and last successful refresh time.
4. Add daily/weekly time-series chart, magnitude distribution, and depth distribution.
5. Add an interactive epicenter map with marker sizing/coloring chosen carefully and labeled.
6. Add a sortable recent-events table and source links.
7. Clearly label unknown magnitude/depth values and the active dataset filters.
8. Provide pipeline health panel: last run, success/failure, duration, fetched/inserted/updated/rejected counts.
9. Add empty-state, error-state, and database-unavailable handling.
10. Add caching or query limits to prevent unnecessarily expensive dashboard queries.
11. Perform end-to-end tests from API to dashboard.
12. Verify startup with documented commands or Docker Compose.
13. Produce architecture diagram, ER diagram, screenshots, test results, and final report.
14. Rehearse a 3–5 minute demonstration.

### Dashboard sections

**Overview** — key metrics, latest refresh, selected date range.  
**World Map** — recorded epicenters with hover details.  
**Magnitude Analysis** — histogram/buckets and strongest events.  
**Temporal Trends** — event counts by day/week/month.  
**Depth Analysis** — distribution and depth-vs-magnitude scatter (optional).  
**Data Pipeline Health** — recent runs, status, errors, row counts.

### Deliverables

- Functional Streamlit dashboard.
- End-to-end test evidence.
- Docker/local startup instructions.
- Final report, screenshots, and demo walkthrough.
- Limitations and future-work section.

### Acceptance criteria

- Dashboard filters change database-backed results correctly.
- Map points match stored latitude and longitude.
- Metric denominators and missing-value handling are transparent.
- Latest refresh indicator reflects actual successful ingestion.
- Reviewer can run the project from documented instructions.
- Demo shows an actual ingestion run and an idempotent rerun.

### Suggested effort

**2–3 working days**.

---

## 10. Milestone summary and dependencies

| Phase | Focus | Depends on | Exit gate | Suggested duration |
|---|---|---|---|---|
| 1 | Setup + USGS source contract | None | Sample fetch archived | 1–2 days |
| 2 | Extraction + transformation | Phase 1 | ETL unit tests pass | 2–3 days |
| 3 | PostgreSQL + SQL analytics | Phase 2 | Idempotent backfill verified | 2–3 days |
| 4 | Airflow + monitoring | Phase 3 | Scheduled incremental DAG verified | 2–3 days |
| 5 | Dashboard + release | Phases 3–4 | End-to-end demo passes | 2–3 days |

**Estimated total:** approximately **9–14 working days** for a student comfortable with Python/SQL. This is a planning estimate, not a guaranteed completion time. Airflow/Docker setup can extend the timeline.

**Recommended order:** finish and test each phase before adding the next. Phase 5's UI design can be sketched early, but the final dashboard should read only the actual curated database.

---

## 11. Proposed repository structure

```text
quaketrack/
├── PROJECT.md
├── README.md
├── .gitignore
├── .env.example
├── pyproject.toml                  # or requirements.txt
├── docker-compose.yml
├── Dockerfile                      # optional app image
├── config/
│   └── settings.example.yaml
├── data/
│   ├── raw/                        # downloaded GeoJSON; generally gitignored
│   ├── quarantine/                 # rejected records; generally gitignored
│   └── samples/                    # tiny safe fixtures
├── sql/
│   ├── migrations/
│   │   ├── 001_create_ingestion_runs.sql
│   │   ├── 002_create_earthquakes.sql
│   │   └── 003_create_operational_tables.sql
│   └── analytics/
│       ├── daily_counts.sql
│       ├── magnitude_distribution.sql
│       └── strongest_events.sql
├── src/
│   └── quaketrack/
│       ├── __init__.py
│       ├── config.py
│       ├── cli.py
│       ├── ingestion/
│       │   ├── usgs_client.py
│       │   ├── windowing.py
│       │   └── raw_archive.py
│       ├── transforms/
│       │   ├── normalizer.py
│       │   ├── validator.py
│       │   └── deduplicator.py
│       ├── storage/
│       │   ├── database.py
│       │   ├── repository.py
│       │   └── checkpoints.py
│       └── monitoring/
│           └── run_metrics.py
├── dags/
│   └── earthquake_ingestion_dag.py
├── dashboard/
│   ├── app.py
│   ├── queries.py
│   └── charts.py
├── tests/
│   ├── unit/
│   │   ├── test_normalizer.py
│   │   ├── test_validator.py
│   │   └── test_windowing.py
│   ├── integration/
│   │   ├── test_database_upsert.py
│   │   └── test_pipeline_replay.py
│   └── fixtures/
│       └── sample_usgs_geojson.json
├── scripts/
│   ├── check_api.py
│   ├── run_backfill.py
│   └── verify_database.py
└── docs/
    ├── architecture.md
    ├── data_dictionary.md
    ├── operations.md
    ├── test_plan.md
    ├── demo_script.md
    └── screenshots/
```

**Important:** this is a proposed structure; files should be created progressively as each phase is implemented, not as empty placeholders simply to claim completion.

---

## 12. Configuration and environment

Example `.env.example` (do not put real passwords in it):

```dotenv
# Source
USGS_API_BASE_URL=https://earthquake.usgs.gov/fdsnws/event/1/query
USGS_MIN_MAGNITUDE=2.5
USGS_EVENT_TYPE=earthquake
USGS_REQUEST_TIMEOUT_SECONDS=30
USGS_MAX_RETRIES=3

# Backfill / incremental
BACKFILL_START_DATE=2025-10-01
BACKFILL_END_DATE=2026-10-01
INCREMENTAL_LOOKBACK_HOURS=72
PIPELINE_SCHEDULE=0 * * * *

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=quaketrack
POSTGRES_USER=quaketrack_app
POSTGRES_PASSWORD=change_me_locally

# Application
LOG_LEVEL=INFO
RAW_ARCHIVE_DIR=./data/raw
QUARANTINE_DIR=./data/quarantine
```

Dates above are **illustrative defaults** and should be changed for the actual run. Keep runtime values in environment variables; do not hard-code credentials or machine-specific paths. For Docker Compose, the database hostname inside containers may differ from `localhost`.

---

## 13. Automation and operational behavior

### 13.1 Historical backfill

- Accept explicit `start` and `end` UTC dates.
- Split into time chunks and process them sequentially or with conservative bounded concurrency.
- Save each successful source response and mark its chunk complete.
- On restart, skip verified completed chunks or safely replay them.
- Use idempotent upserts so retries cannot create duplicates.

### 13.2 Incremental update

- Trigger on an hourly schedule.
- Fetch a sliding lookback window (proposed 72 hours) so recently corrected events can be updated.
- Upsert by event ID and source update timestamp.
- Record `last_successful_refresh` only after quality checks and commit.
- Optionally run a weekly wider reconciliation for late revisions outside the lookback window.

### 13.3 Logging and metrics

Every run should record:

- Run ID, job type, UTC start/end, status, duration.
- Requested source window and query filters.
- HTTP attempts and failures.
- Fetched features, filtered features, rejected rows, deduplicated rows.
- Database inserted, updated, unchanged counts.
- Archive paths and payload hashes.
- Final error details when unsuccessful.

### 13.4 Recovery scenarios

| Failure | Expected handling |
|---|---|
| API timeout | Retry with backoff, then fail visibly |
| API rate limiting | Respect source guidance and retry delays |
| Database unavailable | Roll back transaction and retry later |
| Invalid record | Quarantine row, record reason, continue under documented quality policy |
| Bad response format | Fail chunk and preserve diagnostic metadata |
| Airflow worker interrupted | Re-run safely using checkpoints and idempotent upserts |
| Duplicate scheduled trigger | Prevent overlapping writer conflicts or serialize safely |
| Dashboard DB error | Display friendly error without fabricating data |

---

## 14. Dashboard design specification

### 14.1 Global controls

- Start and end date.
- Minimum/maximum magnitude (include explicit handling for unknown magnitude).
- Minimum/maximum depth, if selected.
- Optional bounding box/region selection for advanced users.
- Refresh query action and visible data-as-of timestamp.

### 14.2 KPI cards

1. **Recorded earthquake events:** count of filtered `event_type='earthquake'` rows.
2. **Maximum recorded magnitude:** maximum non-null magnitude in filtered data.
3. **Average recorded depth:** mean of non-null depth values, clearly labeled.
4. **Latest successful pipeline refresh:** from operational run metadata.

### 14.3 Charts

- Time series of counts by UTC day/week/month.
- Magnitude histogram or clearly defined bins.
- Depth histogram.
- Scatterplot of magnitude versus depth (optional).
- Top 20 strongest events table.
- World map of epicenter points.
- Pipeline-run history and processing counts.

### 14.4 Map considerations

- Map **longitude** to x and **latitude** to y; do not reverse them.
- Use clustering, aggregation, or result limits when too many points would overload the browser.
- Explain that dense clusters can reflect both seismic activity and reporting/detection characteristics.
- Do not label epicenters as exact damage locations.

### 14.5 Dashboard quality

- Consistent UTC labels and units (`km` for depth).
- No fabricated example KPIs in production view.
- Show active magnitude and date filters prominently.
- Handle no-data ranges gracefully.
- Avoid misleading comparisons between periods with different coverage or magnitude thresholds.

---

## 15. Testing strategy

### 15.1 Unit tests

- Parse valid source features.
- Convert epoch milliseconds to UTC accurately.
- Verify `[longitude, latitude, depth]` ordering.
- Handle null magnitude and depth.
- Reject invalid coordinates and missing IDs.
- Deduplicate same-ID rows correctly.
- Verify time-window boundaries do not unintentionally omit or double-count events.

### 15.2 Integration tests

- Apply migrations to a disposable PostgreSQL instance.
- Load fixture events and compare stored values.
- Load the same fixture twice; assert one row per event ID.
- Load a newer source revision; assert update.
- Load an older revision afterward; assert no regression.
- Simulate a failed transaction; assert no partial committed batch.
- Validate run metrics and checkpoint behavior.

### 15.3 End-to-end tests

- Fetch a small, fixed source window.
- Archive raw data.
- Transform and load.
- Query aggregate views.
- Trigger a second ingestion of the same window.
- Verify stable unique-event counts.
- Open dashboard and verify selected KPIs against direct SQL results.

### 15.4 Sample acceptance checklist

- [ ] Fresh installation instructions work.
- [ ] USGS connectivity is demonstrated.
- [ ] Raw payload and provenance are saved.
- [ ] Transformation handles nulls and invalid records.
- [ ] PostgreSQL migrations apply cleanly.
- [ ] Historical backfill completes for the chosen test window.
- [ ] Re-running ingestion creates no duplicate event IDs.
- [ ] Newer revisions are updated and stale revisions ignored.
- [ ] Airflow DAG executes and logs outcomes.
- [ ] Dashboard reads live database tables/views.
- [ ] Data freshness and pipeline health are visible.
- [ ] All required tests pass and results are recorded.
- [ ] Final demo can be reproduced.

---

## 16. Performance and reliability

### 16.1 Initial expectations

The system should run on a typical development laptop with Docker Desktop or a local Python/PostgreSQL setup. Do not promise fixed throughput or response times before measuring actual workload. Keep historical backfills chunked, index frequently filtered columns, and cap dashboard result sizes.

### 16.2 Measurable performance indicators

- Extraction time per source window.
- Events processed per minute.
- Number of successful versus failed pipeline runs.
- Percentage of source rows rejected by validation.
- Upsert throughput.
- Dashboard query latency for selected time windows.
- Age of last successful data refresh.

Record actual measurements during Phase 5 and compare them to the baseline; do not invent benchmark numbers.

### 16.3 Operational safeguards

- Bound request concurrency and retries.
- Set network and database timeouts.
- Use transactions for batch writes.
- Preserve raw data for replay.
- Do not silently advance checkpoints after failed loads.
- Keep source-update timestamps to resolve revisions.
- Back up PostgreSQL before destructive schema changes.

---

## 17. Security, privacy, and responsible use

- USGS public earthquake events are not personal-user records; avoid introducing unrelated personal information.
- Keep database credentials in local environment variables or secrets management, not in Git.
- Use least-privilege database accounts where practical.
- Parameterize SQL and validate user-selected filters.
- Limit dashboard database access to read-only queries if deploying publicly.
- Respect source usage guidance and attribution requirements.
- Clearly state that this is an **educational analytics application**, not a safety-critical earthquake alert or prediction service.
- If hosted publicly, protect operational dashboards and prevent unrestricted expensive queries.

---

## 18. Risks, constraints, and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| USGS query returns too many results | Timeout/failed backfill | Use smaller date chunks and validate API limits |
| API schema or availability changes | Broken ingestion | Isolate source adapter; use fixtures and error handling |
| Source event revisions | Stale/incorrect analytics | Re-fetch lookback and upsert by update timestamp |
| Null magnitudes or depths | Misleading averages/charts | Preserve nulls and label metric denominators |
| Incorrect coordinate ordering | Wrong map positions | Explicit coordinate-order tests |
| Overlap with classmates' disaster projects | Topic approval risk | Emphasize earthquake-only ETL, revision handling, and pipeline observability; seek approval |
| Airflow setup complexity | Timeline slip | Finish manual CLI pipeline first, then add orchestration |
| Data gaps or varying detection thresholds | Invalid scientific conclusions | State dataset/filter limitations and avoid causal claims |
| Expensive dashboard queries | Slow demo | Add indexes, date filters, result limits, and caching |
| Unclear source-to-database counts | Difficult evaluation | Record filtering, dedup, rejection, insert, and update counts separately |

---

## 19. Academic evaluation mapping

| Academic criterion | Evidence to show |
|---|---|
| Problem definition | Problem statement and data-flow diagram |
| Data sources | USGS API response and source documentation |
| ETL | Extraction script, transformation tests, raw/curated examples |
| Database | PostgreSQL schema, keys, indexes, SQL queries |
| Automation | Airflow DAG and execution history |
| Data quality | Validation rules, rejection examples, idempotency tests |
| Analytics | Streamlit dashboard and SQL-backed charts |
| Monitoring | Run metrics, refresh timestamp, failure example |
| Deployment | Setup guide and repeatable start commands |
| Documentation | Project plan, architecture, ER diagram, test report |

---

## 20. Demonstration plan (approximately 4 minutes)

**0:00–0:30 — Problem and architecture**  
Explain scattered/updated event records and show the USGS → ETL → PostgreSQL → Airflow → dashboard diagram.

**0:30–1:10 — Real source and raw data**  
Show a small USGS GeoJSON response and the archived raw payload.

**1:10–2:00 — ETL and database**  
Run a manual pipeline or trigger Airflow; show normalized records and PostgreSQL row counts.

**2:00–2:30 — Idempotency**  
Run the same window again and demonstrate no duplicate event IDs; show inserted versus unchanged counts.

**2:30–3:30 — Analytics dashboard**  
Filter by dates/magnitude, inspect map, daily trends, and largest events.

**3:30–4:00 — Automation and monitoring**  
Show DAG run history, last successful refresh, and a summary of quality checks.

**Demo reliability tip:** preload a verified historical sample for visualizations, then run a small live query separately. This avoids relying on a large network download during the presentation. Label any preloaded data honestly.

---

## 21. Suggested final report structure

1. Title page and abstract.
2. Introduction and motivation.
3. Problem statement.
4. Objectives and scope.
5. Existing data source and limitations.
6. Proposed architecture and data flow.
7. Tools and technologies.
8. Database schema and ER diagram.
9. ETL workflow and data quality rules.
10. Airflow orchestration and monitoring.
11. Dashboard implementation and screenshots.
12. Testing methodology and observed results.
13. Challenges and mitigations.
14. Conclusion and future scope.
15. References and appendix (commands, configuration, selected SQL).

---

## 22. Possible viva questions and short answers

**Q1. Why is this a Data Engineering project?**  
Because it focuses on ingestion, transformation, storage, data quality, incremental updates, orchestration, and analytics—not predictive modeling.

**Q2. What is ETL?**  
Extract data from USGS, transform it into validated structured rows, and load it into PostgreSQL.

**Q3. Why PostgreSQL?**  
It provides relational integrity, SQL analytics, indexes, transactions, and conflict-aware upserts.

**Q4. Why use Airflow?**  
To schedule tasks, track dependencies, retry failures, and inspect execution history.

**Q5. How do you prevent duplicates?**  
Use the USGS event ID as a primary key and idempotent upserts.

**Q6. What if USGS corrects an earthquake's magnitude?**  
Re-fetch recent events and update a stored event only when the source revision is newer.

**Q7. Why keep raw data?**  
For auditing, reproducibility, debugging, and replay without downloading again.

**Q8. What happens when the API is unavailable?**  
Bounded retries, visible failure status, and no successful-checkpoint advancement.

**Q9. Is this real-time?**  
It is scheduled near-current analytics, not a guaranteed real-time safety service.

**Q10. Can this predict earthquakes?**  
No. The scope is descriptive analytics and reliable data processing.

**Q11. Why can earthquake counts be misleading?**  
Detection thresholds, reporting coverage, selected magnitude filters, and catalog updates affect observed counts.

**Q12. How is this different from a static CSV dashboard?**  
It automatically fetches source updates, maintains raw and curated layers, handles revisions and failures, records run metrics, and refreshes database-backed analytics.

---

## 23. Future enhancements (out of scope for initial submission)

- Spatial indexing with PostGIS for fast bounding-box and radius queries.
- Region/country enrichment with documented geospatial boundaries and licensing.
- A second authoritative earthquake catalog for source comparison, with careful event-identity reconciliation.
- Long-term reconciliation to capture source updates outside the hourly lookback.
- Materialized daily/weekly aggregate tables for larger histories.
- Pipeline notifications on repeated failures.
- Hosted read-only dashboard with appropriate security and query limits.
- Data lineage visualization and more formal data contracts.

These enhancements should **not** be described as implemented unless actually built and tested.

---

## 24. Final implementation principles

1. **Correctness before complexity.** Build a working single-source pipeline before adding features.
2. **Source truth before charts.** Never fabricate observations or assume missing values.
3. **UTC everywhere internally.** Clearly label displayed time zones.
4. **Idempotency is mandatory.** Re-running a job must be safe.
5. **Source revisions matter.** The same earthquake can be updated after first publication.
6. **Raw data is evidence.** Archive it with provenance and checksums.
7. **Quality is observable.** Count accepted, rejected, filtered, inserted, updated, and unchanged records.
8. **No fake real-time claims.** Show the actual last successful refresh.
9. **Avoid unnecessary technology.** Python, PostgreSQL, Airflow, and Streamlit are sufficient.
10. **Demonstrate the pipeline, not just the dashboard.** The strongest demo shows ingestion, database state, repeatability, and analytics together.

---

## 25. References and starting resources

- USGS FDSN Event Web Service: https://earthquake.usgs.gov/fdsnws/event/1/
- USGS Earthquake Catalog: https://earthquake.usgs.gov/earthquakes/search/
- PostgreSQL documentation: https://www.postgresql.org/docs/
- Apache Airflow documentation: https://airflow.apache.org/docs/
- Streamlit documentation: https://docs.streamlit.io/
- Plotly Python documentation: https://plotly.com/python/
- Docker Compose documentation: https://docs.docker.com/compose/

**Document status:** detailed project specification and phased build plan. API behavior, dependency versions, deployment commands, data volume, and performance must be validated during implementation; no live API call, database creation, or code execution is claimed by this document.
