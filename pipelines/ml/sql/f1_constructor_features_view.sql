-- Feature view: per-season constructor aggregate stats for F1 ML pipelines.
-- Aggregates results by constructor and year with reliability, performance metrics.
CREATE OR REPLACE VIEW `data-feeder-lcd.curated.f1_constructor_features_v` AS
SELECT
  c.constructorId,
  c.name AS constructor,
  c.nationality,
  ra.year,
  COUNT(*) AS race_entries,
  SUM(r.points) AS total_points,
  SUM(CASE WHEN r.positionOrder = 1 THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN r.positionOrder <= 3 THEN 1 ELSE 0 END) AS podiums,
  SUM(CASE WHEN s.status = 'Finished' OR s.status LIKE '+%' THEN 1 ELSE 0 END) AS finishes,
  COUNT(*) - SUM(CASE WHEN s.status = 'Finished' OR s.status LIKE '+%' THEN 1 ELSE 0 END) AS retirements,
  ROUND(SAFE_DIVIDE(SUM(CASE WHEN s.status = 'Finished' OR s.status LIKE '+%' THEN 1 ELSE 0 END), COUNT(*)), 3) AS reliability_pct,
  ROUND(AVG(r.positionOrder), 2) AS avg_finish_position,
  ROUND(AVG(r.grid), 2) AS avg_grid_position,
  ROUND(AVG(r.grid - r.positionOrder), 2) AS avg_positions_gained,
  ROUND(SAFE_DIVIDE(SUM(r.points), COUNT(DISTINCT ra.raceId)), 2) AS points_per_race,
  ROUND(SAFE_DIVIDE(SUM(CASE WHEN r.positionOrder = 1 THEN 1 ELSE 0 END), COUNT(DISTINCT ra.raceId)), 3) AS win_rate,
  ROUND(SAFE_DIVIDE(SUM(CASE WHEN r.positionOrder <= 3 THEN 1 ELSE 0 END), COUNT(DISTINCT ra.raceId)), 3) AS podium_rate
FROM `data-feeder-lcd.curated.results` r
JOIN `data-feeder-lcd.curated.races` ra ON r.raceId = ra.raceId
JOIN `data-feeder-lcd.curated.constructors` c ON r.constructorId = c.constructorId
JOIN `data-feeder-lcd.curated.status` s ON r.statusId = s.statusId
WHERE r.grid > 0
GROUP BY c.constructorId, c.name, c.nationality, ra.year
HAVING COUNT(*) >= 5;
