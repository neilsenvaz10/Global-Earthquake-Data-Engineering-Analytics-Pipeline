SELECT event_id, occurred_at, magnitude, magnitude_type, depth_km, latitude, longitude, place_description, source_url
FROM earthquakes
WHERE event_type = 'earthquake' AND occurred_at >= :start AND occurred_at < :end AND magnitude IS NOT NULL
ORDER BY magnitude DESC, occurred_at DESC, event_id LIMIT :limit;
