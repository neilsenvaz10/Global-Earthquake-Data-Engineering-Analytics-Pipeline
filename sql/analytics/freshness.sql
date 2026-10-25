SELECT MAX(occurred_at) AS latest_event_at, MAX(source_updated_at) AS latest_source_update_at,
       MAX(last_ingested_at) AS latest_accepted_event_ingestion_at,
       (SELECT MAX(finished_at) FROM ingestion_runs WHERE status = 'success' AND chunks_completed > 0) AS latest_successful_load_at
FROM earthquakes
WHERE event_type = 'earthquake' AND occurred_at >= :start AND occurred_at < :end;
