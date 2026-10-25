CREATE TABLE ingestion_chunks (
    chunk_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES ingestion_runs(run_id),
    checkpoint_key TEXT NOT NULL CHECK (checkpoint_key ~ '^[0-9a-f]{64}$'),
    input_sha256 TEXT NOT NULL CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    source_run_id UUID NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL CHECK (window_end > window_start),
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    attempt_count INTEGER NOT NULL CHECK (attempt_count > 0),
    records_fetched BIGINT NOT NULL DEFAULT 0 CHECK (records_fetched >= 0),
    records_accepted BIGINT NOT NULL DEFAULT 0 CHECK (records_accepted >= 0),
    records_rejected BIGINT NOT NULL DEFAULT 0 CHECK (records_rejected >= 0),
    records_filtered BIGINT NOT NULL DEFAULT 0 CHECK (records_filtered >= 0),
    records_deduplicated BIGINT NOT NULL DEFAULT 0 CHECK (records_deduplicated >= 0),
    records_unique BIGINT NOT NULL DEFAULT 0 CHECK (records_unique >= 0),
    records_inserted BIGINT NOT NULL DEFAULT 0 CHECK (records_inserted >= 0),
    records_updated BIGINT NOT NULL DEFAULT 0 CHECK (records_updated >= 0),
    records_unchanged BIGINT NOT NULL DEFAULT 0 CHECK (records_unchanged >= 0),
    archive_path TEXT,
    payload_sha256 TEXT CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    UNIQUE (chunk_id, run_id),
    CHECK (records_fetched = records_accepted + records_rejected),
    CHECK (records_accepted = records_filtered + records_deduplicated + records_unique),
    CHECK (records_unique = records_inserted + records_updated + records_unchanged),
    CHECK (status <> 'success' OR completed_at IS NOT NULL)
);
CREATE INDEX idx_chunks_run_id ON ingestion_chunks (run_id);
CREATE INDEX idx_chunks_checkpoint ON ingestion_chunks (checkpoint_key, input_sha256) WHERE status = 'success';

CREATE TABLE raw_archives (
    archive_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES ingestion_runs(run_id),
    chunk_id UUID NOT NULL REFERENCES ingestion_chunks(chunk_id),
    source_run_id UUID NOT NULL,
    archive_path TEXT NOT NULL,
    metadata_path TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    source TEXT NOT NULL,
    request_url TEXT NOT NULL,
    request_parameters JSONB NOT NULL CHECK (jsonb_typeof(request_parameters) = 'object'),
    fetched_at TIMESTAMPTZ NOT NULL,
    http_status INTEGER NOT NULL CHECK (http_status BETWEEN 100 AND 599),
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    record_count INTEGER CHECK (record_count >= 0),
    UNIQUE (chunk_id, archive_path),
    UNIQUE (archive_id, chunk_id, run_id),
    FOREIGN KEY (chunk_id, run_id) REFERENCES ingestion_chunks(chunk_id, run_id)
);
CREATE INDEX idx_archives_run_id ON raw_archives (run_id);

CREATE TABLE rejected_events (
    rejection_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES ingestion_runs(run_id),
    chunk_id UUID NOT NULL,
    archive_id UUID NOT NULL,
    source_event_id TEXT,
    reason_code TEXT NOT NULL,
    reason_detail TEXT NOT NULL,
    raw_event_json JSONB NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (archive_id, chunk_id, run_id) REFERENCES raw_archives(archive_id, chunk_id, run_id)
);
CREATE INDEX idx_rejected_run_id ON rejected_events (run_id);
