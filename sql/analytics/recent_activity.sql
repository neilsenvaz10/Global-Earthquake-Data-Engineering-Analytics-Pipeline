-- Recent within the requested historical period, relative to its exclusive end.
SELECT event_id, occurred_at, magnitude, magnitude_type, depth_km, place_description, source_url
FROM earthquakes
WHERE event_type = 'earthquake' AND occurred_at >= :start AND occurred_at < :end
ORDER BY occurred_at DESC, event_id LIMIT :limit;
