# Phase 7 historical backfill preflight

Prepared October 25, 2026 at 17:55 UTC. Initial status was awaiting approval.
**The user subsequently approved this exact proposal in chat, and the backfill
completed successfully.** See `phase7_report.md` for actual results and verification.
The range was calculated from the current UTC day rather than hardcoded:
90 complete days, **July 27 through October 24, 2026**, represented as
`[2026-07-27T00:00:00Z, 2026-10-25T00:00:00Z)`.

| Item | Actual inspection / estimate |
|---|---|
| Source and filters | Official USGS FDSN API; global; earthquake type; magnitude ≥4.0 |
| Source count estimate | **3,951** returned by the bounded count endpoint |
| API estimation work already performed | One count request and one five-feature sample; no ingestion/persistence |
| Planned extraction workload | **90** daily requests before retries/adaptive splits |
| Existing configured request cap / USGS maximum | 1,000 / 20,000 events per request; capped responses are split, never silently truncated |
| Measured count / five-feature request time | 0.892 / 0.798 seconds |
| Estimated execution time | **3–15 minutes**, a planning estimate rather than a throughput guarantee |
| Estimated raw feature size | Mean 740 bytes in five-feature sample; about 2.79 MiB before response/metadata overhead |
| Planning storage budget | **100 MiB** for raw, staged, normalized, database/index and audit overhead; retries/revisions add storage |
| Workspace free space | **403.11 GiB** |
| PostgreSQL volume available space | **935,879,732 KiB** reported inside the container |
| Database identity | `quaketrack`, PostgreSQL 17.11, existing `quaketrack_app` writer; host port remains 55432 |
| Existing events / event dates | **40 / 2**: October 1 and October 25 |
| Existing archive inventory | **48 files**, 24 response/metadata pairs; all payload hashes verified |
| Silver-to-source comparison | All 40 rows match valid normalized archived representations |
| Integrity checks | Zero duplicate IDs, invalid rows, orphan rows, inconsistent runs or incomplete successful chunks |
| Migrations | 001–005 valid; exact checksums saved in local preflight evidence |
| Airflow | Authenticated API reports the earthquake DAG paused; schedule unchanged |
| Baseline tests | 170 host tests, 5 actual Spark tests and 3 actual Airflow DAG checks passed |

The original complete rows, raw-file SHA-256 inventory and incremental checkpoints
are preserved in ignored `data/staging/phase7/before.json`. Full machine-readable
preflight evidence is in `data/staging/phase7/preflight.json`.

Use the existing bounded ETL, retries, adaptive splitting, archive writer,
normalizer, validation/quarantine, deduplication and transactional loader. A fresh
staging directory separates these filters from prior runs. Extraction resume skips
verified completed windows; database resume checks committed matching chunks.
Existing tests cover repeated-load idempotency, source revision precedence, failed
transaction rollback and resume. No second ingestion implementation is proposed.

The raw source count is not a net-insert prediction: 37 original October 1 events
overlap the proposed period, while the three October 25 events fall outside it and
must remain. Source revisions and newly added historical observations may change
the count. Existing upserts can accept newer source revisions, but the user's
instruction requires the original 40 rows to remain unchanged. Before execution,
the loader must guard those IDs or stop for review of a changed representation;
it must not silently rewrite original rows.

No recurring ingestion will be enabled, and manual historical work must not
advance automatic incremental checkpoints. No tables, schemas, volumes or archives
will be reset or overwritten. After approval, verify source/DB count reconciliation,
all completed windows, quarantine, archive hashes, original rows, Gold views,
contracts and Spark parity over the same expanded snapshot. A missing timeline
period must not be displayed as zero without complete-window evidence.

Reviewed CLI invocation, executed only after user approval and testing the new
opt-in original-row preservation guard:

```powershell
.\.venv\Scripts\python -m quaketrack.cli backfill --start 2026-07-27 --end 2026-10-25 --min-magnitude 4 --window-days 1 --max-windows 90 --output-dir data/staging/phase7-90d-mag4 --load-database --preserve-existing
```

Explicit approval was required by the Phase 7 prompt before the full 90-day
download and persistence and was received. Dashboard work proceeded independently. If approval
comes on a later UTC date, recheck the current 90-complete-day range and count
before running it rather than silently changing the approved scope.
