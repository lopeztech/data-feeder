CREATE TABLE IF NOT EXISTS `data-feeder-lcd.curated.pipeline_runs` (
  run_id STRING NOT NULL,
  pipeline_name STRING NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  metric_name STRING NOT NULL,
  metric_value FLOAT64 NOT NULL
)
OPTIONS (
  description = 'Centralized metrics log for all ML pipeline runs'
);
