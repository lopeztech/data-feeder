-- Feature view: per-race driver features for F1 ML pipelines.
-- Joins results, qualifying, races, circuits, drivers, constructors, status.
CREATE OR REPLACE VIEW `data-feeder-lcd.curated.f1_driver_features_v` AS
SELECT
  r.resultId,
  r.raceId,
  ra.year,
  ra.round,
  ra.name AS race_name,
  ci.circuitRef AS circuit,
  ci.country AS circuit_country,
  r.driverId,
  d.driverRef AS driver,
  CONCAT(d.forename, ' ', d.surname) AS driver_name,
  d.nationality AS driver_nationality,
  r.constructorId,
  c.name AS constructor,
  r.grid,
  r.positionOrder AS finish_position,
  r.points,
  r.laps,
  SAFE_CAST(r.milliseconds AS INT64) AS race_time_ms,
  SAFE_CAST(r.fastestLapSpeed AS FLOAT64) AS fastest_lap_speed,
  SAFE_CAST(r.rank AS INT64) AS fastest_lap_rank,
  r.statusId,
  s.status AS finish_status,
  CASE WHEN s.status = 'Finished' OR s.status LIKE '+%' THEN 1 ELSE 0 END AS finished,
  q.position AS quali_position,
  -- Derived features
  r.grid - r.positionOrder AS positions_gained,
  CASE WHEN r.positionOrder <= 3 THEN 1 ELSE 0 END AS podium,
  CASE WHEN r.positionOrder = 1 THEN 1 ELSE 0 END AS win
FROM `data-feeder-lcd.curated.results` r
JOIN `data-feeder-lcd.curated.races` ra ON r.raceId = ra.raceId
JOIN `data-feeder-lcd.curated.circuits` ci ON ra.circuitId = ci.circuitId
JOIN `data-feeder-lcd.curated.drivers` d ON r.driverId = d.driverId
JOIN `data-feeder-lcd.curated.constructors` c ON r.constructorId = c.constructorId
JOIN `data-feeder-lcd.curated.status` s ON r.statusId = s.statusId
LEFT JOIN `data-feeder-lcd.curated.qualifying` q ON r.raceId = q.raceId AND r.driverId = q.driverId
WHERE r.grid > 0;
