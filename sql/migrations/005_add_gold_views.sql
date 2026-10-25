-- Logical Gold layer. Ordinary views always reflect committed Silver rows.
CREATE VIEW gold_daily_earthquakes AS
SELECT (occurred_at AT TIME ZONE 'UTC')::date AS event_day,
       COUNT(*) AS event_count, COUNT(magnitude) AS known_magnitudes,
       AVG(magnitude) AS average_magnitude, MAX(magnitude) AS maximum_magnitude,
       COUNT(depth_km) AS known_depths, AVG(depth_km) AS average_depth_km
FROM earthquakes WHERE event_type = 'earthquake' GROUP BY 1;

CREATE VIEW gold_magnitude_distribution AS
SELECT CASE WHEN magnitude IS NULL THEN 'unknown'
            WHEN magnitude < 0 THEN '<0'
            WHEN magnitude < 2 THEN '0-<2'
            WHEN magnitude < 4 THEN '2-<4'
            WHEN magnitude < 6 THEN '4-<6' ELSE '6+' END AS magnitude_category,
       COUNT(*) AS event_count
FROM earthquakes WHERE event_type = 'earthquake' GROUP BY 1;

CREATE VIEW gold_monthly_earthquakes AS
SELECT date_trunc('month', occurred_at AT TIME ZONE 'UTC')::date AS event_month,
       COUNT(*) AS event_count, AVG(magnitude) AS average_magnitude,
       MAX(magnitude) AS maximum_magnitude, AVG(depth_km) AS average_depth_km
FROM earthquakes WHERE event_type = 'earthquake' GROUP BY 1;

CREATE VIEW gold_depth_by_magnitude AS
SELECT CASE WHEN magnitude IS NULL THEN 'unknown'
            WHEN magnitude < 0 THEN '<0'
            WHEN magnitude < 2 THEN '0-<2'
            WHEN magnitude < 4 THEN '2-<4'
            WHEN magnitude < 6 THEN '4-<6' ELSE '6+' END AS magnitude_category,
       COUNT(*) AS event_count, COUNT(depth_km) AS known_depths,
       AVG(depth_km) AS average_depth_km
FROM earthquakes WHERE event_type = 'earthquake' GROUP BY 1;
