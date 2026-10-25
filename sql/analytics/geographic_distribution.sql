-- Reliable coordinate-based 10-degree cells, not inferred countries or named regions.
-- A point on +90/+180 belongs to the final cell, not an extra cell past the bound.
SELECT LEAST(FLOOR((latitude + 90) / 10), 17) * 10 - 90 AS latitude_cell_start,
       LEAST(FLOOR((longitude + 180) / 10), 35) * 10 - 180 AS longitude_cell_start,
       COUNT(*) AS event_count
FROM earthquakes
WHERE event_type = 'earthquake' AND occurred_at >= :start AND occurred_at < :end
  AND latitude BETWEEN :min_latitude AND :max_latitude
  AND longitude BETWEEN :min_longitude AND :max_longitude
GROUP BY 1, 2 ORDER BY 1, 2;
