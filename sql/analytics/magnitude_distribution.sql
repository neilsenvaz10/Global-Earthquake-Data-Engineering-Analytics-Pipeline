SELECT CASE WHEN magnitude IS NULL THEN 'Unknown' WHEN magnitude < 0 THEN '<0'
       WHEN magnitude < 2 THEN '0-<2' WHEN magnitude < 4 THEN '2-<4'
       WHEN magnitude < 6 THEN '4-<6' ELSE '6+' END AS magnitude_range,
       COUNT(*) AS event_count
FROM earthquakes
WHERE event_type = 'earthquake' AND occurred_at >= :start AND occurred_at < :end
GROUP BY 1 ORDER BY 1;
