SELECT
  (SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM earthquakes) AS duplicate_ids,
  (SELECT COUNT(*) FROM earthquakes WHERE occurred_at IS NULL OR event_id IS NULL
    OR latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180) AS invalid_events,
  (SELECT COUNT(*) FROM earthquakes e LEFT JOIN ingestion_runs r ON r.run_id = e.last_run_id
    WHERE r.run_id IS NULL) AS orphan_events,
  (SELECT COUNT(*) FROM ingestion_runs WHERE records_fetched <> records_accepted + records_rejected
    OR records_unique <> records_inserted + records_updated + records_unchanged) AS inconsistent_runs,
  (SELECT COUNT(*) FROM ingestion_chunks WHERE status = 'success'
    AND completed_at IS NULL) AS incomplete_successful_chunks;
