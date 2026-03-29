import { cloudEvent, CloudEvent } from '@google-cloud/functions-framework';
import { BigQuery } from '@google-cloud/bigquery';
import { Storage } from '@google-cloud/storage';
import { Firestore } from '@google-cloud/firestore';
import { PubSub } from '@google-cloud/pubsub';
import { parseFile } from './parsers.js';
import type { ValidationCompleteMessage, MessagePublishedData } from './types.js';

const bigquery = new BigQuery();
const storage = new Storage();
const firestore = new Firestore({
  databaseId: process.env.FIRESTORE_DATABASE || 'data-feeder',
});
const pubsub = new PubSub();

const BQ_DATASET = process.env.BQ_CURATED_DATASET || 'curated';
const PIPELINE_FAILED_TOPIC = process.env.PIPELINE_FAILED_TOPIC || 'pipeline-failed';

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

    // Parse gs:// path
    const pathMatch = silverPath.match(/^gs:\/\/([^/]+)\/(.+)$/);
    if (!pathMatch) {
      throw new Error(`Invalid Silver path: ${silverPath}`);
    }
    const [, bucketName, objectName] = pathMatch;
    const filename = objectName.split('/').pop() ?? objectName;

    // Download file from Silver
    const [buffer] = await storage.bucket(bucketName).file(objectName).download();

    // Parse into rows
    const { rows } = parseFile(buffer, contentType, filename);

    if (rows.length === 0) {
      throw new Error('No rows to load after parsing');
    }

    // Sanitize table name for BigQuery (alphanumeric + underscores only)
    const tableName = (jobDoc.data()?.bq_table || dataset)
      .replace(/[^a-zA-Z0-9_]/g, '_')
      .replace(/^_+/, '')
      .substring(0, 128);

    const tableRef = bigquery.dataset(BQ_DATASET).table(tableName);

    // Insert rows — BigQuery streaming insert handles schema auto-detection
    // for CREATE_IF_NEEDED tables
    const [tableExists] = await tableRef.exists();

    if (!tableExists) {
      // Create table with schema inferred from first row
      const schema = Object.keys(rows[0]).map(name => ({
        name: name.replace(/[^a-zA-Z0-9_]/g, '_'),
        type: inferBqType(rows[0][name]),
        mode: 'NULLABLE' as const,
      }));

      await bigquery.dataset(BQ_DATASET).createTable(tableName, {
        schema: { fields: schema },
        timePartitioning: { type: 'DAY' },
      });
    }

    // Normalize column names in rows to match BQ requirements
    const normalizedRows = rows.map(row => {
      const normalized: Record<string, unknown> = {};
      for (const [key, value] of Object.entries(row)) {
        normalized[key.replace(/[^a-zA-Z0-9_]/g, '_')] = value;
      }
      return normalized;
    });

    // Insert in batches of 500 (BQ streaming insert limit is 10k but smaller is safer)
    const BATCH_SIZE = 500;
    let loadedCount = 0;

    for (let i = 0; i < normalizedRows.length; i += BATCH_SIZE) {
      const batch = normalizedRows.slice(i, i + BATCH_SIZE);
      await tableRef.insert(batch, { ignoreUnknownValues: true });
      loadedCount += batch.length;
    }

    // Update Firestore to LOADED
    await jobRef.update({
      status: 'LOADED',
      bq_table: `${BQ_DATASET}.${tableName}`,
      stats: {
        total_records: jobDoc.data()?.stats?.total_records ?? rows.length,
        valid: jobDoc.data()?.stats?.valid ?? rows.length,
        rejected: 0,
        loaded: loadedCount,
      },
      updated_at: new Date().toISOString(),
    });

    console.log(`Job ${jobId}: loaded ${loadedCount} rows → ${BQ_DATASET}.${tableName}`);
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

function inferBqType(value: unknown): string {
  if (value === null || value === undefined) return 'STRING';
  if (typeof value === 'number') return Number.isInteger(value) ? 'INT64' : 'FLOAT64';
  if (typeof value === 'boolean') return 'BOOL';
  if (typeof value === 'string') {
    if (/^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?/.test(value)) return 'TIMESTAMP';
    return 'STRING';
  }
  return 'STRING';
}
