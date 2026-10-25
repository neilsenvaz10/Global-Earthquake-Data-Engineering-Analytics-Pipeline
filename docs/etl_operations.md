# Phase 2 ETL architecture and recovery

## Flow and module responsibilities

`cli.py` -> `pipeline.py` -> `ingestion/windowing.py` and `usgs_client.py` ->
`raw_archive.py` -> `transforms/normalizer.py` + `validator.py` -> quarantine ->
`deduplicator.py` -> `storage/staging.py`; `monitoring/run_metrics.py` reconciles
counts and the pipeline writes a report for every started run.

Phase 1 configuration/logging and requests are reused. Pandas is optional under
PROJECT.md and adds no benefit for these small per-record transformations, so no
new dependency is introduced. There are no PostgreSQL event tables or loaders.

## Extraction and boundary policy

The CLI requires explicit start/end values, normalizes offsets to UTC and rejects
sub-millisecond boundaries. Internal windows are half-open [start,end). USGS bounds
are inclusive; request endtime is end minus one millisecond. Requests use documented
implicit-UTC strings; normalized timestamps/reports keep explicit +00:00 offsets.
The API still queries only earthquake-type records by default.

Each request sets a configurable cap (default 1,000; valid 1..20,000). A response
at or above that cap is archived, then replaced for processing by two smaller time
windows. This repeats until every processed response is below the cap. Even an
exactly full response is split conservatively. A one-millisecond capped interval
fails: no completeness claim, no checkpoint. Pagination is deliberately avoided.
Only complete leaf responses count as fetched records; capped parents are evidence,
not additional logical input. Parent/error responses still count in source_responses.

The source catalog is mutable, not a snapshot. Splitting and half-open windows
avoid deterministic boundary loss/duplication; records revised or deleted while
fetching require later reconciliation. No global completeness guarantee is made.

## Retry policy

Retry timeouts, connection errors, HTTP 408, 429, 500, 502, 503 and 504. Three retries
means at most four attempts per request. Other HTTP errors fail immediately. Delay
is exponential backoff plus jitter, taking at least the Retry-After seconds or HTTP
date if supplied. A delay over five minutes fails visibly and tells the operator to
retry later rather than violating Retry-After. Retry budget exhaustion fails the
window. Invalid JSON or collection schema fails after the raw response is archived.
Malformed individual features are quarantined without stopping valid siblings.
Every received HTTP response, including an error body, gets an immutable archive.
Timeouts with no body have only an attempt count and run error, not a fictitious file.
The file extension is .geojson even for an HTTP error; inspect status_code first.

## Normalization and validation

Required: Feature object, nonempty event ID, properties object, Point geometry,
finite numeric longitude/latitude within their geographic bounds, valid occurrence
time and a nonempty event type. Optional measurements and source attributes can be
missing/null. Present but malformed optional values are rejected, not silently
replaced with null. Numeric strings are accepted; booleans, NaN/infinity and invalid
numbers are rejected. Strict JSON rejects nonstandard bare NaN/Infinity constants
at response level. Valid negative magnitude and depth are retained.

Timestamps are integral epoch milliseconds converted explicitly to UTC ISO8601 with
millisecond precision. Epoch zero is valid. Invalid non-null updated timestamps are
rejected, because revision precedence cannot rely on them. Source strings are
preserved as labels; place descriptions are not country codes. No values are imputed.
Records outside the requested leaf window, below a known magnitude threshold or
of another event type are counted as filtered, separately from invalid records.
Quarantine includes the original parsed feature, reason code/detail, USGS source,
run ID, ingestion time and raw archive reference. The original byte representation
is retained in the archive even when JSON parsing is impossible.

## Revision and deduplication policy

USGS event ID is the key. Within a complete top-level window, select the newest
valid source_updated_at. Known timestamps outrank missing timestamps. For conflicting
equal/missing revisions in a batch, the lexicographically first canonical JSON record
wins; input order cannot change that result. This is a deterministic fallback, not
evidence of source revision order. Across windows/runs an existing record wins a tie
or a missing incoming update timestamp. A known incoming timestamp can replace a
stored missing timestamp. Invalid newer candidates are quarantined and never win.
Pure `prefer_incoming` can be used to specify Phase 3's upsert tests.

The catalog persists across runs in the same output directory. This is temporary
Phase 2 staging persistence, not a substitute for Phase 3 PostgreSQL integrity.
Local state contains one current record per ID. Per-window normalized files may
contain older versions and must not be concatenated as a unique global catalog.

## Counter definitions

Counters describe newly committed windows in the current invocation. Resumed/skipped
windows have separate counters and do not inflate extracted or unchanged counts.

- fetched = accepted + rejected (quarantined).
- accepted = eligible + filtered (accepted means structurally/domain valid).
- eligible = unique_records + deduplicated (duplicates within that top-level window).
- unique_records = inserted + updated + unchanged in the local staging catalog.

Repeated IDs across different windows are resolved against the catalog and counted
as unchanged/updated, not as within-window deduplicated. Therefore unique_records
summed over a run is a sum of per-window unique counts, not necessarily final catalog
size. catalog_records is the actual number of current unique event IDs.
Failed-window rows are not included in committed counts; their response archives
are still listed in the failed run's report. A failed run can retain earlier complete
windows. These counts do not claim any PostgreSQL rows were inserted.

## Commit, interruption and resume

A nonblocking OS lock serializes writers for each staging directory and releases on
process exit, including crashes; the .writer.lock file can remain harmlessly present.
Do not point independent staging catalogs at a shared hand-edited state file.

For each top-level window: fetch/archive, normalize/validate, write immutable-run
normalized and quarantine files, compute hashes, then atomically replace state.json
with BOTH the new catalog and its completed checkpoint. The replacement uses a
same-directory temporary file, flush/fsync and os.replace. A pre-commit failure
cannot advance that checkpoint; orphan raw/normalized/quarantine files are retained
as evidence. Reports are also replaced atomically. This provides process-interruption
recovery on a normal local filesystem, not a guarantee against hardware corruption.

`--resume` checks the window/filter/contract fingerprint, raw and metadata hashes,
and normalized/quarantine artifact hashes before skipping a window. If verification
fails, the run fails visibly. Restore the verified files or rerun without --resume
to fetch that window again. Never ignore a damaged archive to claim a safe resume.
Changing the window size can reduce checkpoint reuse, but catalog dedup remains safe.
Without --resume, the source is fetched again and newer valid revisions can be staged.
Use a new output directory only when intentionally starting an independent catalog.

`--max-windows N` commits N new complete windows then reports limited if work remains.
It makes real requests and writes data. Continue with identical bounds/settings,
the same output directory and --resume. Keyboard interrupts exit 130; ordinary
failures exit 1. Reports are under <output-dir>/runs/<run_id>/report.json. Lock or
argument failures before a run starts are reported on the console only.

## Limits and Phase 3 handoff

State/catalog is read and rewritten in memory per top-level window. This is designed
for a college mini-project, not multi-million-event storage. Request windows are
bounded; total catalog memory is not. Archive references use absolute paths; moving
a checkout can break resume references. Restore paths or fetch into a new staging
catalog after relocation. Keep raw archives when relocating or backing up.

Phase 3 must add schema/migrations, constraints, transactional upserts, ingestion
provenance, indexes, analytical queries and disposable-DB integration tests. Decide
how the extra source attributes (magnitude_type, status, network, URLs) fit the
blueprint schema. Airflow checkpoints and scheduling remain Phase 4. No checkpoints
or reports in Phase 2 imply a successful database load.
