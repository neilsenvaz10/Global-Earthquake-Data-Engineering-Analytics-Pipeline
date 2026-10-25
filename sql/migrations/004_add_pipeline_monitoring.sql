ALTER TABLE ingestion_runs DROP CONSTRAINT ingestion_runs_job_type_check;
ALTER TABLE ingestion_runs ADD CONSTRAINT ingestion_runs_job_type_check
    CHECK (job_type IN ('backfill', 'replay', 'incremental'));
ALTER TABLE ingestion_runs ADD COLUMN pipeline_name TEXT;
ALTER TABLE ingestion_runs ADD COLUMN execution_key TEXT UNIQUE;
ALTER TABLE ingestion_runs ADD COLUMN stage TEXT;
ALTER TABLE ingestion_runs ADD COLUMN quality_status TEXT CHECK (quality_status IN ('pending', 'passed', 'failed'));
ALTER TABLE ingestion_runs ADD COLUMN api_failures INTEGER NOT NULL DEFAULT 0 CHECK (api_failures >= 0);
ALTER TABLE ingestion_runs ADD COLUMN database_failures INTEGER NOT NULL DEFAULT 0 CHECK (database_failures >= 0);
ALTER TABLE ingestion_runs ADD COLUMN quality_failures INTEGER NOT NULL DEFAULT 0 CHECK (quality_failures >= 0);
ALTER TABLE ingestion_runs ADD COLUMN child_run_id UUID REFERENCES ingestion_runs(run_id);

CREATE TABLE pipeline_state (
    pipeline_name TEXT PRIMARY KEY,
    coverage_start TIMESTAMPTZ NOT NULL,
    last_successful_window_end TIMESTAMPTZ NOT NULL CHECK (last_successful_window_end > coverage_start),
    last_run_id UUID NOT NULL REFERENCES ingestion_runs(run_id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE VIEW pipeline_health AS
SELECT r.run_id, r.execution_key, r.pipeline_name, r.status, r.stage, r.quality_status,
       r.started_at, r.finished_at, EXTRACT(EPOCH FROM (COALESCE(r.finished_at, NOW()) - r.started_at)) AS duration_seconds,
       r.window_start, r.window_end, r.records_fetched, r.records_inserted, r.records_updated,
       r.records_unchanged, r.records_rejected, r.api_failures, r.database_failures, r.quality_failures,
       r.error_message, s.last_successful_window_end, s.updated_at AS last_successful_ingestion_at,
       EXTRACT(EPOCH FROM (NOW() - s.last_successful_window_end)) AS coverage_lag_seconds
FROM ingestion_runs r LEFT JOIN pipeline_state s ON s.pipeline_name = r.pipeline_name
WHERE r.execution_key IS NOT NULL;
