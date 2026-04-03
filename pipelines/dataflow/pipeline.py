"""Dataflow Flex Template: Silver-to-Gold BigQuery Loader.

Reads validated Parquet files from GCS Silver bucket and loads them into
BigQuery staging (WRITE_TRUNCATE) and curated (WRITE_APPEND) datasets
with automatic clustering detection and day partitioning.
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery, firestore, pubsub_v1, storage
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Arrow type string prefix → BigQuery type for clustering detection
_ARROW_TO_BQ: dict[str, str] = {
    'string': 'STRING', 'large_string': 'STRING', 'utf8': 'STRING',
    'int64': 'INT64', 'int32': 'INT64', 'int16': 'INT64', 'int8': 'INT64',
    'uint64': 'INT64', 'uint32': 'INT64',
    'float64': 'NUMERIC', 'float32': 'NUMERIC', 'double': 'NUMERIC',
    'decimal128': 'NUMERIC',
    'timestamp': 'TIMESTAMP', 'date32': 'DATE', 'date64': 'DATE',
}

_CLUSTER_TYPES = {'STRING', 'INT64', 'NUMERIC', 'TIMESTAMP', 'DATETIME', 'DATE'}


class LoaderOptions(PipelineOptions):
    """Pipeline options for the Silver-to-Gold loader."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--silver_gcs_path', required=True,
            help='GCS path to Silver Parquet file (gs://bucket/path)')
        parser.add_argument(
            '--target_bq_table', required=True,
            help='Target BigQuery table name')
        parser.add_argument(
            '--job_id', required=True,
            help='Pipeline job ID for Firestore tracking')
        parser.add_argument(
            '--project_id', required=True,
            help='GCP project ID')
        parser.add_argument(
            '--firestore_database', default='data-feeder',
            help='Firestore database ID')
        parser.add_argument(
            '--bq_staging_dataset', default='staging',
            help='BigQuery staging dataset')
        parser.add_argument(
            '--bq_curated_dataset', default='curated',
            help='BigQuery curated dataset')
        parser.add_argument(
            '--dataset_location', default='australia-southeast1',
            help='BigQuery dataset location')
        parser.add_argument(
            '--pipeline_failed_topic', default='pipeline-failed',
            help='Pub/Sub topic for failure notifications')


def _parse_gcs_path(gcs_path: str) -> tuple[str, str]:
    """Parse gs://bucket/path into (bucket, object_path)."""
    match = re.match(r'^gs://([^/]+)/(.+)$', gcs_path)
    if not match:
        raise ValueError(f'Invalid GCS path: {gcs_path}')
    return match.group(1), match.group(2)


def _sanitize_table_name(name: str) -> str:
    """Sanitize a string for use as a BigQuery table name."""
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    sanitized = re.sub(r'_+', '_', sanitized).strip('_')
    if sanitized and sanitized[0].isdigit():
        sanitized = f't_{sanitized}'
    return sanitized[:128] or 'unnamed_table'


def _detect_clustering_fields(arrow_schema, max_fields: int = 4) -> list[str]:
    """Detect columns suitable for BigQuery clustering from an Arrow schema."""
    id_fields: list[str] = []
    ts_fields: list[str] = []
    other_fields: list[str] = []

    for i in range(len(arrow_schema)):
        field = arrow_schema.field(i)
        arrow_type = str(field.type).split('[')[0].lower()
        bq_type = _ARROW_TO_BQ.get(arrow_type)
        if not bq_type or bq_type not in _CLUSTER_TYPES:
            continue

        name_lower = field.name.lower()
        if 'id' in name_lower or name_lower.endswith('_key'):
            id_fields.append(field.name)
        elif bq_type in ('TIMESTAMP', 'DATETIME', 'DATE'):
            ts_fields.append(field.name)
        else:
            other_fields.append(field.name)

    return (id_fields + ts_fields + other_fields)[:max_fields]


def _read_parquet_schema(gcs_path: str):
    """Read Parquet schema from GCS without loading the full file."""
    import io
    bucket_name, object_path = _parse_gcs_path(gcs_path)
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(object_path)
    buf = io.BytesIO()
    blob.download_to_file(buf)
    buf.seek(0)
    return pq.ParquetFile(buf).schema_arrow


def _ensure_dataset(bq_client: bigquery.Client, dataset_id: str, location: str):
    """Ensure a BigQuery dataset exists, creating it if needed."""
    ds = bigquery.Dataset(f'{bq_client.project}.{dataset_id}')
    ds.location = location
    bq_client.create_dataset(ds, exists_ok=True)


def _update_firestore(job_id: str, status: str, project_id: str,
                      database: str, extra: Optional[dict] = None):
    """Update Firestore job document status."""
    db = firestore.Client(project=project_id, database=database)
    update: dict = {
        'status': status,
        'updated_at': datetime.now(timezone.utc).isoformat(),
    }
    if extra:
        update.update(extra)
    db.collection('jobs').document(job_id).update(update)
    logger.info('Updated job %s → %s', job_id, status)


def _publish_failure(project_id: str, topic: str, job_id: str,
                     dataset: str, error: str, silver_path: str):
    """Publish failure message to pipeline-failed topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic)
    publisher.publish(topic_path, json.dumps({
        'jobId': job_id,
        'dataset': dataset,
        'error': error,
        'silverPath': silver_path,
    }).encode('utf-8'))
    logger.info('Published failure for job %s to %s', job_id, topic)


class _CountElementsFn(beam.DoFn):
    """Counts rows and estimates bytes via Beam metrics."""

    def __init__(self):
        self.rows = Metrics.counter('loader', 'rows_processed')
        self.bytes = Metrics.counter('loader', 'bytes_written')
        self.errors = Metrics.counter('loader', 'error_count')

    def process(self, element):
        self.rows.inc()
        self.bytes.inc(len(json.dumps(element, default=str).encode('utf-8')))
        yield element


def run():
    """Main entry point for the Dataflow Flex Template."""
    opts = LoaderOptions()
    loader = opts.view_as(LoaderOptions)

    silver_gcs_path = loader.silver_gcs_path
    target_bq_table = loader.target_bq_table
    job_id = loader.job_id
    project_id = loader.project_id
    firestore_db = loader.firestore_database
    staging_dataset = loader.bq_staging_dataset
    curated_dataset = loader.bq_curated_dataset
    dataset_location = loader.dataset_location
    failed_topic = loader.pipeline_failed_topic

    table_name = _sanitize_table_name(target_bq_table)
    staging_table = f'{project_id}:{staging_dataset}.{table_name}'
    curated_table = f'{project_id}:{curated_dataset}.{table_name}'

    logger.info('Loader pipeline starting: job=%s silver=%s staging=%s curated=%s',
                job_id, silver_gcs_path, staging_table, curated_table)

    try:
        # Pre-pipeline setup: ensure datasets and detect clustering
        bq_client = bigquery.Client(project=project_id, location=dataset_location)
        _ensure_dataset(bq_client, staging_dataset, dataset_location)
        _ensure_dataset(bq_client, curated_dataset, dataset_location)

        arrow_schema = _read_parquet_schema(silver_gcs_path)
        clustering_fields = _detect_clustering_fields(arrow_schema)
        logger.info('Clustering fields: %s', clustering_fields)

        curated_bq_params: dict = {
            'timePartitioning': {'type': 'DAY'},
        }
        if clustering_fields:
            curated_bq_params['clustering'] = {'fields': clustering_fields}

        # Run Beam pipeline
        with beam.Pipeline(options=opts) as p:
            rows = (
                p
                | 'ReadSilverParquet' >> beam.io.ReadFromParquet(silver_gcs_path)
                | 'CountAndPassThrough' >> beam.ParDo(_CountElementsFn())
            )

            rows | 'WriteToStaging' >> WriteToBigQuery(
                staging_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )

            rows | 'WriteToCurated' >> WriteToBigQuery(
                curated_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                additional_bq_parameters=curated_bq_params,
            )

        # Post-pipeline: get row count from staging and update Firestore
        staging_ref = f'{project_id}.{staging_dataset}.{table_name}'
        result = list(bq_client.query(
            f'SELECT COUNT(*) AS cnt FROM `{staging_ref}`'
        ).result())
        loaded_rows = result[0].cnt if result else 0

        _update_firestore(
            job_id=job_id,
            status='LOADED',
            project_id=project_id,
            database=firestore_db,
            extra={
                'bq_table': f'{curated_dataset}.{table_name}',
                'stats.loaded': loaded_rows,
            },
        )

        logger.info('Job %s completed: %d rows loaded', job_id, loaded_rows)

    except Exception as exc:
        logger.error('Pipeline failed for job %s: %s', job_id, exc)

        try:
            _update_firestore(
                job_id=job_id, status='FAILED',
                project_id=project_id, database=firestore_db,
                extra={'error': str(exc)},
            )
        except Exception as fs_err:
            logger.error('Firestore update failed: %s', fs_err)

        try:
            _publish_failure(
                project_id=project_id, topic=failed_topic,
                job_id=job_id, dataset=target_bq_table,
                error=str(exc), silver_path=silver_gcs_path,
            )
        except Exception as pub_err:
            logger.error('Failure publish failed: %s', pub_err)

        raise


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()
