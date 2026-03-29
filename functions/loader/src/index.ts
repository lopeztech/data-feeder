import { cloudEvent, CloudEvent } from '@google-cloud/functions-framework';
import { BigQuery } from '@google-cloud/bigquery';
import { Firestore } from '@google-cloud/firestore';
import { PubSub } from '@google-cloud/pubsub';
import type { ValidationCompleteMessage, MessagePublishedData } from './types.js';

const bigquery = new BigQuery();
const firestore = new Firestore({
  databaseId: process.env.FIRESTORE_DATABASE || 'data-feeder',
});
const pubsub = new PubSub();

const BQ_DATASET = process.env.BQ_CURATED_DATASET || 'curated';
const PIPELINE_FAILED_TOPIC = process.env.PIPELINE_FAILED_TOPIC || 'pipeline-failed';

const FORMAT_MAP: Record<string, string> = {
  'text/csv': 'CSV',
  'application/json': 'NEWLINE_DELIMITED_JSON',
  'application/x-ndjson': 'NEWLINE_DELIMITED_JSON',
};

cloudEvent('loader', async (event: CloudEvent<MessagePublishedData>) => {
  const messageData = event.data?.message?.data;
  if (!messageData) {
    console.error('No message data in event');
    return;
  }

  let msg: ValidationCompleteMessage;
  try {
    msg = JSON.parse(Buffer.from(messageData, 'base64').toString('utf-8'));
  } catch (err) {
    console.error('Failed to parse validation-complete message:', err);
    return;
  }

  const { jobId, dataset, silverPath, contentType } = msg;

  if (!jobId || !silverPath) {
    console.warn('Missing jobId or silverPath in message, skipping');
    return;
  }

  const jobRef = firestore.collection('jobs').doc(jobId);

  try {
    // Idempotency guard: only process TRANSFORMING jobs
    const jobDoc = await jobRef.get();
    if (!jobDoc.exists) {
      console.warn(`Job ${jobId} not found, skipping`);
      return;
    }

    const currentStatus = jobDoc.data()?.status;
    if (currentStatus !== 'TRANSFORMING') {
      console.warn(`Job ${jobId} status is ${currentStatus}, expected TRANSFORMING — skipping`);
      return;
    }

    // Sanitize table name for BigQuery
    const tableName = (jobDoc.data()?.bq_table || dataset)
      .replace(/[^a-zA-Z0-9_]/g, '_')
      .replace(/^_+/, '')
      .substring(0, 128);

    // Determine BQ source format
    const sourceFormat = FORMAT_MAP[contentType] || 'CSV';

    // Use BQ load job directly from GCS — handles schema auto-detection
    const [job] = await bigquery.dataset(BQ_DATASET).table(tableName).load(
      silverPath,
      {
        sourceFormat,
        autodetect: true,
        writeDisposition: 'WRITE_APPEND',
        createDisposition: 'CREATE_IF_NEEDED',
        timePartitioning: { type: 'DAY' },
        skipLeadingRows: sourceFormat === 'CSV' ? 1 : 0,
      },
    );

    const status = job.status;
    if (status?.errors && status.errors.length > 0) {
      throw new Error(`BQ load errors: ${status.errors.map(e => e.message).join('; ')}`);
    }

    // Get row count from load job statistics
    const loadedRows = Number(job.statistics?.load?.outputRows ?? 0);

    // Update Firestore to LOADED
    await jobRef.update({
      status: 'LOADED',
      bq_table: `${BQ_DATASET}.${tableName}`,
      stats: {
        total_records: jobDoc.data()?.stats?.total_records ?? loadedRows,
        valid: jobDoc.data()?.stats?.valid ?? loadedRows,
        rejected: 0,
        loaded: loadedRows,
      },
      updated_at: new Date().toISOString(),
    });

    console.log(`Job ${jobId}: loaded ${loadedRows} rows → ${BQ_DATASET}.${tableName}`);
  } catch (err) {
    console.error(`Job ${jobId}: loader error:`, err);

    try {
      await jobRef.update({
        status: 'FAILED',
        error: err instanceof Error ? err.message : 'Unknown loader error',
        updated_at: new Date().toISOString(),
      });
    } catch { /* best effort */ }

    try {
      await pubsub.topic(PIPELINE_FAILED_TOPIC).publishMessage({
        json: {
          jobId,
          dataset,
          error: err instanceof Error ? err.message : 'Unknown loader error',
          silverPath,
        },
      });
    } catch { /* best effort */ }

    throw err; // Let Pub/Sub retry
  }
});
