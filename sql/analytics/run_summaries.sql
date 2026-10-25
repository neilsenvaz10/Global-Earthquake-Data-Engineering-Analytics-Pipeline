SELECT run_id, job_type, started_at, finished_at, status, window_start, window_end,
       records_fetched, records_inserted, records_updated, records_unchanged,
       records_rejected, source_response_count, chunks_completed, chunks_skipped, error_message
FROM ingestion_runs WHERE window_start < :end AND window_end > :start
ORDER BY started_at DESC, run_id LIMIT :limit;
