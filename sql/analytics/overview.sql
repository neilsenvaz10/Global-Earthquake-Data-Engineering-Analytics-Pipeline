SELECT COUNT(*) AS event_count, COUNT(magnitude) AS known_magnitudes,
       COUNT(depth_km) AS known_depths, AVG(depth_km) AS average_depth_km,
       MAX(depth_km) AS maximum_depth_km, MAX(magnitude) AS maximum_magnitude
FROM earthquakes
WHERE event_type = 'earthquake' AND occurred_at >= :start AND occurred_at < :end;
