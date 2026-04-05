CREATE TABLE IF NOT EXISTS `data-feeder-lcd.curated.pipeline_runs` (
  run_id STRING,
  pipeline_name STRING,
  timestamp TIMESTAMP,
  metric_name STRING,
  metric_value FLOAT64
)
OPTIONS (
  description = 'Centralized metrics log for all ML pipeline runs'
);
