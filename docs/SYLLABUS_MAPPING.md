# QuakeTrack syllabus mapping

No numbered institutional syllabus was supplied. The module names below are
topic groups from the Phase 6 request, not invented official unit numbers.

| Topic/module group | Implemented evidence | Scope |
|---|---|---|
| Data engineering fundamentals | USGS source, bounded extraction, normalization, PostgreSQL persistence | Existing Python ETL; Pandas is used by the dashboard stack, not the core ingestion algorithm |
| APIs and ingestion reliability | Timeouts, retries, UTC window splitting, immutable responses, resume | Existing Phases 1–2; no streaming broker |
| Modern storage organization | Bronze `data/raw`; Silver normalized staging/current table; four Gold SQL views | New logical medallion mapping; local files + PostgreSQL, not a lakehouse table format |
| Relational storage and analytical SQL | Keys, foreign keys, migrations, revision-aware upserts, daily/monthly/category views | Existing schema plus migration 005; ordinary views avoid refresh jobs |
| Distributed-processing fundamentals | PySpark 4.1.3 explicit DataFrame schema; lazy select/filter/withColumn/groupBy; count/collect actions | New optional `local[2]` job; JVM and two local execution threads, no multi-machine cluster |
| Spark SQL | Temporary Silver view and monthly SQL aggregate | New; results compared with PostgreSQL on one repeatable-read snapshot |
| Data quality and contracts | Versioned JSON schema description, shared validators, required/optional fields, null/negative policies, duplicate groups | New processed-row contract/report, not a raw-response acceptance-rate calculation |
| Governance and lineage | Raw SHA-256, source request metadata, source-update policy, run IDs, quarantine separation, reproducibility checks | Existing lineage plus explicit contract and Bronze-to-Silver verification |
| Database access control | Separate dashboard login, schema usage, selected table/column reads, actual DML denials | New role; no superuser, ownership or group memberships; ETL privileges preserved |
| Workflow orchestration | Airflow DAG dependencies, retries, quality-gated incremental coverage | Existing Phase 4; scheduler/DAG state checked and DAG left paused |
| Analytics and visualization | PostgreSQL-backed Streamlit pages and Plotly charts | Existing Phase 5, regression-tested with the new reader credentials |
| Testing and reproducibility | Offline/DB tests, actual local Spark tests, SQL parity, preservation manifests | Development records and raw archives remain unchanged |

## What this does not cover

Kafka, Flink, Debezium/CDC, distributed Hadoop services, MLlib, cloud object
storage, cloud hosting, Delta Lake, Apache Iceberg and Apache Hudi are not
implemented. Spark ships client libraries internally; that is not a Hadoop
cluster deployment. Bronze–Silver–Gold terminology alone does not create a
transactional lakehouse, schema evolution engine, streaming system or data catalog.

## A short Phase 6 viva demonstration

1. Show the architecture in the Phase 6 report and one existing response/hash.
2. Show the current Silver table and the four Gold views; explain nulls and UTC.
3. Open the JSON contract and generated quality report: 40/40 valid processed rows.
4. Run `docker compose --profile spark run --rm spark`; show `local[2]`, the explicit
   schema, daily counts 37/3, monthly count 40 and `postgres_parity: true`.
5. Run `python scripts/verify_phase6.py` using the venv Python. Explain why the
   probes disable the transaction guard yet still receive SQLSTATE 42501.
6. Open Streamlit and confirm that analytics still work with the dedicated reader.

The actual institution may organize these topics differently. Use its official
syllabus to assign final module numbers; the implementation evidence stays the same.
