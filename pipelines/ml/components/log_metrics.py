"""Shared helper: log pipeline metrics to BigQuery pipeline_runs table."""

from kfp import dsl


@dsl.component(
    base_image="python:3.11-slim",
    packages_to_install=[
        "google-cloud-bigquery==3.30.0", "db-dtypes==1.3.1",
        "protobuf>=4.21.1,<5", "urllib3>=1.26,<2",
    ],
)
def log_metrics(
    metrics_artifact: dsl.Input[dsl.Artifact],
    project_id: str,
    region: str,
    bq_dataset: str,
    pipeline_name: str,
):
    """Read a metrics JSON artifact and write each numeric value to pipeline_runs in BigQuery."""
    import json
    import uuid
    from datetime import datetime, timezone
    from google.cloud import bigquery

    with open(metrics_artifact.path) as f:
        metrics = json.load(f)

    run_id = str(uuid.uuid4())[:8]
    timestamp = datetime.now(timezone.utc).isoformat()

    def flatten(obj: dict, prefix: str = "") -> list[tuple[str, float]]:
        """Flatten nested dicts into (dotted_key, value) pairs."""
        items = []
        for k, v in obj.items():
            key = f"{prefix}{k}" if not prefix else f"{prefix}.{k}"
            if isinstance(v, (int, float)):
                items.append((key, float(v)))
            elif isinstance(v, dict):
                items.extend(flatten(v, key))
        return items

    flat = flatten(metrics)
    rows = [
        {
            "run_id": run_id,
            "pipeline_name": pipeline_name,
            "timestamp": timestamp,
            "metric_name": name,
            "metric_value": value,
        }
        for name, value in flat
    ]

    if not rows:
        print("No numeric metrics to log")
        return

    client = bigquery.Client(project=project_id, location=region)
    table_id = f"{project_id}.{bq_dataset}.pipeline_runs"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("pipeline_name", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("metric_name", "STRING"),
            bigquery.SchemaField("metric_value", "FLOAT64"),
        ],
        write_disposition="WRITE_APPEND",
    )

    client.load_table_from_json(rows, table_id, job_config=job_config).result()
    print(f"Logged {len(rows)} metrics for {pipeline_name} (run {run_id})")
