SELECT CAST(occurred_at AT TIME ZONE 'UTC' AS DATE) AS event_day, COUNT(*) AS event_count
FROM earthquakes
WHERE event_type = 'earthquake' AND occurred_at >= :start AND occurred_at < :end
GROUP BY 1 ORDER BY 1;
