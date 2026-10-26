# Phase 7: historical expansion and dashboard redesign

Verified October 26, 2026. The two deliverables are complete:

| Deliverable | Status |
|---|---|
| Historical expansion | Complete after explicit user approval; 90 successful daily source windows verified |
| Dashboard redesign | Complete; all five pages, regression checks, browser CSV export and responsive inspection verified |

Phase 6 was committed as `04b0e4f` (`Add Spark demo and dashboard read-only role`). Phase 7 implementation was verified before staging or publication; the user subsequently authorized committing and pushing the code on the existing `main` branch. No branches, database resets, archive replacements or schedule changes were made. Phase 1–6 reports and evidence remain intact.

## Preflight and approval

The baseline was checked before changes: 40 real events on two event dates, 48 raw files representing 24 responses, valid migrations 001–005, unchanged incremental checkpoints, zero integrity violations and a paused Airflow DAG. Baseline validation passed 170 host tests, five tests in the actual Spark container and three checks in the actual Airflow container.

The [reviewed preflight](phase7_preflight.md) was prepared October 25 at 17:55 UTC. The then-current 90 complete UTC days were **July 27 through October 24, 2026**, represented as `[2026-07-27T00:00:00Z, 2026-10-25T00:00:00Z)`. The source was the official USGS FDSN API, global, earthquake type, minimum magnitude **4.0**.

One bounded count request estimated **3,951** observations; a five-feature sample supported storage planning. The proposal estimated **90 daily extraction requests** before retries or adaptive splits, **3–15 minutes**, and a **100 MiB** storage budget. Available workspace storage was 403.11 GiB; the PostgreSQL volume reported 935,879,732 KiB free. The configured page cap was 1,000; capped results use the existing adaptive splitting logic.

The user explicitly answered **“Approve the proposed 90-day backfill”** after receiving that proposal. The full download and persistence followed approval. Work resumed on October 26 without shifting the approved scope or downloading another day. No further approval is pending for this completed run.

## Historical ingestion results

The existing extraction, retry, adaptive splitting, immutable archive, normalization, validation, quarantine, deduplication and transactional loading modules were reused. An opt-in `--preserve-existing` flag prevents conflicting IDs from changing existing database rows, even if a newer source revision arrives. This was necessary to preserve the original complete rows. Normal ingestion still uses its original revision-aware update policy. The flag never replaces newer stored revisions with older data.

```powershell
.\.venv\Scripts\python -m quaketrack.cli backfill --start 2026-07-27 --end 2026-10-25 --min-magnitude 4 --window-days 1 --max-windows 90 --output-dir data/staging/phase7-90d-mag4 --load-database --preserve-existing
```

| Measure | Actual result |
|---|---:|
| Full-run elapsed time | 63.913 seconds |
| Successful contiguous daily windows / extraction requests | 90 / 90 |
| Fetched / accepted / unique IDs | 3,951 / 3,951 / 3,951 |
| PostgreSQL inserted / updated / unchanged | 3,914 / 0 / 37 |
| Rejected / filtered / duplicate input records | 0 / 0 / 0 |
| Total database events / distinct IDs | 3,954 / 3,954 |
| Distinct event dates | 91 |
| Original complete rows preserved | 40 of 40 |
| Original raw-file SHA-256 values preserved | 48 of 48 |
| Current raw inventory after repeat | 230 files / 115 responses |

The database count reconciles as `40 + 3,914 = 3,954`. Thirty-seven original October 1 events overlapped the approved range; three original October 25 observations were retained outside it. `unchanged` describes unchanged PostgreSQL rows under the preservation policy; it does not assert identical incoming revision timestamps.

The full database load ID is `caed0dff-67eb-456d-91b2-9cbc346ff0c5`. A separate bounded repeat of `[2026-07-27, 2026-07-28)` fetched **60** events and reported **0 inserted, 0 updated, 60 unchanged**, with no rejects. Its database load ID is `d1396b8f-a394-4401-bda9-6279b58a9f7e`. Automatic `pipeline_state` rows match the preflight snapshot exactly.

Earliest stored event: `2026-07-27T00:50:52.453Z`. Latest: `2026-10-25T11:25:57.101Z`. Current maximum magnitude is **7.8**, average magnitude **4.565612** and average depth **69.810569 km**. All current rows have magnitude and depth; the UI continues to support unknown and negative measurements.

All 90 intended daily source windows were verified as successful, contiguous and bounded at UTC midnight. This establishes loaded window coverage for the selected source threshold. It does not establish comprehensive global earthquake history, eliminate future USGS revisions or make October 25 complete. Partial July and October calendar months are explicitly marked in monthly charts.

## Dashboard changes

The application retains Streamlit, Plotly and server-side PostgreSQL queries. Deep navy surfaces, teal accents, amber coverage notices, consistent typography and bordered panels improve hierarchy without introducing another frontend framework. CSS targets owned presentation markup and Streamlit's documented container keys.

| Page | Result |
|---|---|
| Overview | Five responsive KPI cards, daily activity, magnitude distribution, coordinate density and an eight-row recent-events table with Explorer navigation |
| Earthquake Map | Actual source coordinates, magnitude-scaled markers, pan/zoom, event selector and official source link; an offline longitude/latitude view works without map tiles |
| Trends | Day/week/month frequency with UTC labels, partial-period flags, magnitude/depth distributions, measurement scatter and strongest observations |
| Explorer | Six readable default columns, optional technical columns, sorting, 25/50/100-row pagination and an independent full filtered CSV download |
| Pipeline Monitoring | Separate workflow/load history, latest successful load, real Gold availability and SELECT privileges, checkpoints and explicitly dated validation evidence |

The sidebar has shared UTC dates, optional magnitude/depth limits, a literal place-label search, unknown-measurement control, Reset and Refresh. Clearing a required date gives a validation message. Pagination clamps when filters shrink the result set. Queries are cached for 60 seconds; Refresh clears cached results. Source bounds and database load time are distinguished from event time.

Coverage merges successful chunk intervals separately by source threshold. Charts plot observed periods without inventing zeros; amber denotes a partial calendar period or unverified coverage. Ten-degree coordinate cells are not country counts. Monitoring labels archive/contract/Spark results as historical checks and detects a changed dataset summary rather than claiming a new live scan.

The existing dashboard role gained only SELECT access to chunk status/window/ID and the raw-archive chunk link, needed to derive truthful coverage. Archive paths, quarantine payloads and error details remain excluded. No password rotation or write privileges were introduced. A small sanitized evidence summary is mounted read-only into the dashboard; writer credentials and raw archives are not mounted there.

## PostgreSQL, governance, Gold and Spark verification

`scripts/verify_phase7.py` checks the approved artifacts and live database without ingesting data. It verifies original complete rows, raw-file hashes, checkpoints, source-to-database counts, all 90 chunks, Gold parity, reader permission denial, Airflow health/pause and all five real-database Streamlit pages.

- Integrity query: **zero** duplicate IDs, invalid events, orphan events, inconsistent runs and incomplete successful chunks.
- Data contract: **3,954 valid of 3,954**, **100% compliance**, no duplicate or missing required fields and no coordinate violations.
- Bronze/Silver reconciliation: **115 archived response checksums verified**, **3,954 source-to-Silver matches**.
- Four current Gold views remain available: daily events, monthly events, magnitude distribution and depth by magnitude.
- Actual PySpark 4.1.3 execution processed **3,954 rows in 39.129 seconds**; all four equivalent PostgreSQL aggregates matched. Spark had no database credentials.
- Reader INSERT, UPDATE and DELETE probes all failed with PostgreSQL **SQLSTATE 42501**, even after disabling the session's read-only transaction default. Probes used `WHERE false` and could not change event rows.

The equivalent Silver snapshot SHA-256 is `b98fb6ba12103e69e6c5cabec5fd2638f43430c69ffd420d6e464cb91bec0f7a`. Current snapshot/contract output is in `data/staging/phase7/demo/`; Spark output is in `data/staging/phase7/spark/run-c630704b6a2f4ba8b55fb3c96a13c215/`.

Airflow's metadatabase, scheduler and DAG processor reported healthy through its authenticated API. The earthquake DAG remains paused, with its schedule and port **18080** unchanged. PostgreSQL remains on **55432**, dashboard on **8501**. Docker Desktop had stopped during the interruption between sessions; restarting it and existing Compose services recovered the named-volume data without reset. All three services were healthy after recovery.

## Automated and browser validation

| Validation | Result |
|---|---|
| Full host suite, including isolated PostgreSQL integration tests | **181 passed, 1 skipped**, 57.02 seconds |
| Actual Spark container tests | **5 passed**, 26.17 seconds |
| Actual Airflow container DAG checks | **3 passed**, 4.803 seconds |
| Final focused dashboard checks after monthly tick-label adjustment | **9 passed**, 4.34 seconds |
| Final read-only Phase 7 verifier | Passed with all five pages, 274 Japan matches, preservation, permissions and service checks |

There are **186 passing pytest cases across host and Spark**, plus three Airflow checks. The host skip is the optional Spark module, separately exercised in its container. Focused reruns are not added to that total. New integration checks cover preservation versus ordinary revision updates and bounded coverage/density SQL. UI regressions cover overlapping/gapped windows, UTC offsets, partial periods, offline coordinates, stale evidence, Reset, Refresh, Explorer navigation/pagination and cleared dates.

Compilation, dependency consistency (`pip check`) and `git diff --check` passed. A final scan of changed text files, recent PostgreSQL/Airflow/dashboard logs and ingestion/Spark/verification logs found no matches for the six configured credential values checked. No files are staged. Windows sandbox restrictions blocked Docker access and Streamlit temporary files during initial final-check attempts; the same checks passed with the approved local execution permissions.

Browser inspection used the actual Docker dashboard at **1920×1080**, **640×900** and **390×844**. Desktop cards, charts and tables were inspected; the main content had no horizontal overflow at those widths. Small-screen cards reflowed, the sidebar opened as an overlay, and Map navigation/offline view remained usable. Tables retain their own scrolling on narrow screens. Map tiles rendered, zoom worked and the selected event showed actual magnitude/depth/coordinates and its USGS link.

The browser's **Download filtered CSV** button downloaded **274 Japan matches**, all with matching source place labels and ten technical columns. The first timestamp retained precision as `2026-10-24T23:59:36.599000Z`. Reset restored all 3,954 matches; Refresh updated query time. Monitoring showed the repeat's 60 unchanged events, all four Gold views and matching dated validation evidence.

Screenshots:

- [Overview, desktop](images/phase7/overview-desktop.png)
- [Map with basemap](images/phase7/map-desktop.png) and [offline map](images/phase7/map-offline.png)
- [Monthly trends](images/phase7/trends-desktop.png)
- [Filtered Explorer](images/phase7/explorer-desktop.png)
- [Pipeline Monitoring](images/phase7/monitoring-desktop.png)
- [Overview, 640-pixel width](images/phase7/overview-small.png), [sidebar](images/phase7/sidebar-small.png), [small map](images/phase7/map-small.png) and [390-pixel map](images/phase7/map-mobile.png)

## Files and reproducibility

Created `src/quaketrack/dashboard/presentation.py`, `scripts/verify_phase7.py`, `tests/test_phase7.py`, this report, `docs/phase7_preflight.md` and the screenshot directory. Updated dashboard app/charts/data/security, `.streamlit/config.toml`, `docker-compose.yml`, the CLI/loader/repository preservation option, dashboard fixtures and PostgreSQL integration tests. Updated README and `docs/DEMO_GUIDE.md` for startup, the already completed approved scope and a 4–5 minute demonstration.

Machine-readable evidence is local and Git-ignored under `data/staging/phase7/`: `before.json`, `preflight.json`, `backfill.log`, `backfill-execution.json`, `repeat.log`, `verification.json`, `final-verification.log`, `demo/`, `spark/` and `validation/summary.json`. Raw responses remain under `data/raw/`; staging is separate at `data/staging/phase7-90d-mag4` and `data/staging/phase7-repeat`.

```powershell
.\.venv\Scripts\python scripts/verify_phase7.py
.\.venv\Scripts\python -m pytest --database-tests -q --basetemp=.pytest_tmp/phase7-check
docker compose exec -T airflow python /workspace/scripts/verify_airflow_dag.py
docker compose --profile spark run --rm spark python -m pytest tests/spark -q -p no:cacheprovider
docker compose --profile spark run --rm -v "${PWD}/data/staging/phase7/demo:/inputs:ro" -v "${PWD}/data/staging/phase7/spark:/outputs" spark
```

The Phase 7 verifier requires the local preflight and run evidence; a fresh clone needs its own ingestion and snapshot. The strict Phase 6 verifier describes the former 40-row point in time and intentionally detects the expanded dataset. Its historical evidence was not overwritten.

## Limits and remaining actions

No implementation or ingestion approval remains pending. The dashboard is available at `http://localhost:8501`; Airflow remains at `http://localhost:18080`. The user subsequently authorized publication of the code; local datasets, credentials and ignored evidence remain outside Git.

The database is a USGS catalog subset at documented thresholds, not a complete census or real-time feed. Source event IDs are the deduplication key; distinct IDs can describe closely related observations. No country boundaries or country inference are provided. Preserving existing rows intentionally leaves their representations untouched; normal revision processing remains available outside preservation mode.

Map and measurement scatter are capped at the newest 5,000 filtered matches; aggregate charts use all matches. CSV export is capped at 10,000 matches with an explicit narrowing message. Online basemap tiles require internet; the offline coordinate view does not. A dataset-summary match supports dated evidence reuse but is not a new exhaustive integrity scan. Scheduler state must be checked in Airflow for a live answer. No deployment beyond the existing local Docker services was performed.
