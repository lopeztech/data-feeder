-- Feature view: per-pitstop features for F1 strategy analysis.
-- Joins pit_stops with races, circuits, drivers, results, constructors.
CREATE OR REPLACE VIEW `data-feeder-lcd.curated.f1_pitstop_features_v` AS
SELECT
  p.raceId,
  p.driverId,
  p.stop AS stop_number,
  p.lap AS stop_lap,
  p.duration AS stop_duration_sec,
  p.milliseconds AS stop_duration_ms,
  ra.year,
  ra.name AS race_name,
  ci.circuitRef AS circuit,
  d.driverRef AS driver,
  c.name AS constructor,
  r.grid,
  r.positionOrder AS finish_position,
  r.grid - r.positionOrder AS positions_gained,
  r.laps AS total_laps,
  ROUND(SAFE_DIVIDE(p.lap, r.laps), 3) AS stop_race_pct,
  -- Pit stop context
  CASE WHEN p.stop = 1 THEN 1 ELSE 0 END AS is_first_stop,
  CASE WHEN p.duration < 25 THEN 1 ELSE 0 END AS fast_stop,
  CASE WHEN p.duration > 35 THEN 1 ELSE 0 END AS slow_stop
FROM `data-feeder-lcd.curated.pit_stops` p
JOIN `data-feeder-lcd.curated.races` ra ON p.raceId = ra.raceId
JOIN `data-feeder-lcd.curated.circuits` ci ON ra.circuitId = ci.circuitId
JOIN `data-feeder-lcd.curated.drivers` d ON p.driverId = d.driverId
JOIN `data-feeder-lcd.curated.results` r ON p.raceId = r.raceId AND p.driverId = r.driverId
JOIN `data-feeder-lcd.curated.constructors` c ON r.constructorId = c.constructorId
WHERE p.duration > 0 AND p.duration < 120 AND r.laps > 0;
