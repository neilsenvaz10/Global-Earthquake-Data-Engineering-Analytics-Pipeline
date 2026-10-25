CREATE TABLE earthquakes (
    event_id TEXT PRIMARY KEY CHECK (length(btrim(event_id)) > 0),
    occurred_at TIMESTAMPTZ NOT NULL CHECK (isfinite(occurred_at)),
    source_updated_at TIMESTAMPTZ CHECK (isfinite(source_updated_at)),
    magnitude DOUBLE PRECISION CHECK (magnitude NOT IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)),
    magnitude_type TEXT,
    depth_km DOUBLE PRECISION CHECK (depth_km NOT IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)),
    latitude DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    place_description TEXT,
    event_type TEXT NOT NULL CHECK (length(btrim(event_type)) > 0),
    event_status TEXT,
    source TEXT NOT NULL CHECK (length(btrim(source)) > 0),
    network TEXT,
    source_code TEXT,
    source_ids TEXT,
    source_networks TEXT,
    source_url TEXT,
    detail_url TEXT,
    first_ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_run_id UUID NOT NULL REFERENCES ingestion_runs(run_id)
);
CREATE INDEX idx_eq_occurred_at ON earthquakes (occurred_at DESC);
CREATE INDEX idx_eq_magnitude ON earthquakes (magnitude);
CREATE INDEX idx_eq_event_type_time ON earthquakes (event_type, occurred_at DESC);
CREATE INDEX idx_eq_source_updated_at ON earthquakes (source_updated_at DESC);
