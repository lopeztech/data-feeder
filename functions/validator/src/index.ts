import { cloudEvent, CloudEvent } from '@google-cloud/functions-framework';
import { Storage } from '@google-cloud/storage';
import { Firestore } from '@google-cloud/firestore';
import { PubSub } from '@google-cloud/pubsub';
import { parse } from 'csv-parse/sync';
import { stringify } from 'csv-stringify/sync';
import { validate } from './validators.js';
import { maskPii } from './pii.js';
import type { GcsNotification, MessagePublishedData } from './types.js';

const storage = new Storage();
const firestore = new Firestore({
  databaseId: process.env.FIRESTORE_DATABASE || 'data-feeder',
});
const pubsub = new PubSub();

const RAW_BUCKET = process.env.GCS_RAW_BUCKET || 'data-feeder-lcd-raw';
const SILVER_BUCKET = process.env.GCS_SILVER_BUCKET || 'data-feeder-lcd-staging';
const REJECTED_BUCKET = process.env.GCS_REJECTED_BUCKET || 'data-feeder-lcd-rejected';
const VALIDATION_COMPLETE_TOPIC = process.env.VALIDATION_COMPLETE_TOPIC || 'validation-complete';
const PIPELINE_FAILED_TOPIC = process.env.PIPELINE_FAILED_TOPIC || 'pipeline-failed';

cloudEvent('validator', async (event: CloudEvent<MessagePublishedData>) => {
  // Decode Pub/Sub message
  const messageData = event.data?.message?.data;
  if (!messageData) {
    console.error('No message data in event');
    return; // Ack — nothing to retry
  }

  let notification: GcsNotification;
  try {
    notification = JSON.parse(Buffer.from(messageData, 'base64').toString('utf-8'));
  } catch (err) {
    console.error('Failed to parse GCS notification:', err);
    return; // Ack — malformed message, don't retry
  }

  const objectName = notification.name;
  const contentType = notification.contentType;
  const jobId = notification.metadata?.jobId;

  if (!jobId) {
    console.warn(`No jobId in metadata for object ${objectName}, skipping`);
    return; // Ack — can't process without jobId
  }

  const jobRef = firestore.collection('jobs').doc(jobId);
  const filename = objectName.split('/').pop() ?? objectName;

  try {
    // Idempotency guard: only process if status is UPLOADING
    const jobDoc = await jobRef.get();
    if (!jobDoc.exists) {
      console.warn(`Job ${jobId} not found in Firestore, skipping`);
      return;
    }

    const currentStatus = jobDoc.data()?.status;
    if (currentStatus !== 'UPLOADING') {
      console.warn(`Job ${jobId} status is ${currentStatus}, expected UPLOADING — skipping`);
      return;
    }

    // Update status to VALIDATING
    await jobRef.update({
      status: 'VALIDATING',
      updated_at: new Date().toISOString(),
    });

    // Download file from Bronze
    const [buffer] = await storage.bucket(RAW_BUCKET).file(objectName).download();

    // Validate
    const result = validate(buffer, contentType, filename);

    if (result.valid) {
      // Mask PII and write to Silver
      const maskedBuffer = maskAndSerialize(buffer, contentType, filename, result.columns);

      await storage
        .bucket(SILVER_BUCKET)
        .file(objectName)
        .save(maskedBuffer.data, { contentType });

      // Update Firestore
      const piiInfo = maskedBuffer.piiMasked > 0
        ? ` (PII masked: ${maskedBuffer.piiColumns.map(c => c.column).join(', ')})`
        : '';

      await jobRef.update({
        status: 'TRANSFORMING',
        silver_path: `gs://${SILVER_BUCKET}/${objectName}`,
        pii_masked: maskedBuffer.piiColumns,
        stats: {
          total_records: result.totalRecords,
          valid: result.totalRecords,
          rejected: 0,
          loaded: 0,
        },
        updated_at: new Date().toISOString(),
      });

      // Publish validation-complete
      await pubsub.topic(VALIDATION_COMPLETE_TOPIC).publishMessage({
        json: {
          jobId,
          dataset: notification.metadata?.dataset,
          silverPath: `gs://${SILVER_BUCKET}/${objectName}`,
          totalRecords: result.totalRecords,
          contentType,
        },
      });

      console.log(`Job ${jobId}: validated ${result.totalRecords} records → Silver${piiInfo}`);
    } else {
      // Copy to Rejected bucket
      await storage
        .bucket(RAW_BUCKET)
        .file(objectName)
        .copy(storage.bucket(REJECTED_BUCKET).file(objectName));

      // Update Firestore
      await jobRef.update({
        status: 'REJECTED',
        error: result.error,
        stats: {
          total_records: 0,
          valid: 0,
          rejected: 0,
          loaded: 0,
        },
        updated_at: new Date().toISOString(),
      });

      // Publish pipeline-failed
      await pubsub.topic(PIPELINE_FAILED_TOPIC).publishMessage({
        json: {
          jobId,
          dataset: notification.metadata?.dataset,
          error: result.error,
          bronzePath: `gs://${RAW_BUCKET}/${objectName}`,
        },
      });

      console.log(`Job ${jobId}: rejected — ${result.error}`);
    }
  } catch (err) {
    // Transient error — update Firestore to FAILED if possible, then throw to retry
    console.error(`Job ${jobId}: validation error:`, err);
    try {
      await jobRef.update({
        status: 'FAILED',
        error: err instanceof Error ? err.message : 'Unknown validation error',
        updated_at: new Date().toISOString(),
      });
    } catch {
      // Firestore update itself failed — still throw to retry
    }
    throw err; // Let Pub/Sub retry
  }
});

function getExtension(filename: string): string {
  return filename.split('.').pop()?.toLowerCase() ?? '';
}

function maskAndSerialize(
  buffer: Buffer,
  contentType: string,
  filename: string,
  columns?: string[],
): { data: Buffer; piiMasked: number; piiColumns: { column: string; type: string }[] } {
  const ext = getExtension(filename);

  // Binary formats — no PII masking, pass through
  if (['parquet', 'avro'].includes(ext)) {
    return { data: buffer, piiMasked: 0, piiColumns: [] };
  }

  // Parse into rows
  let rows: Record<string, string>[];
  const cols = columns ?? [];

  if (ext === 'csv' || contentType === 'text/csv') {
    rows = parse(buffer, { columns: true, skip_empty_lines: true, relax_column_count: true }) as Record<string, string>[];
  } else if (ext === 'json' || contentType === 'application/json') {
    const parsed = JSON.parse(buffer.toString('utf-8'));
    rows = (Array.isArray(parsed) ? parsed : [parsed]) as Record<string, string>[];
  } else if (ext === 'ndjson' || contentType === 'application/x-ndjson') {
    rows = buffer.toString('utf-8').split('\n').filter(l => l.trim()).map(l => JSON.parse(l)) as Record<string, string>[];
  } else {
    return { data: buffer, piiMasked: 0, piiColumns: [] };
  }

  if (rows.length === 0) {
    return { data: buffer, piiMasked: 0, piiColumns: [] };
  }

  const effectiveCols = cols.length > 0 ? cols : Object.keys(rows[0]);
  const { maskedRows, report } = maskPii(rows, effectiveCols);

  // Serialize back to original format
  let data: Buffer;
  if (ext === 'csv' || contentType === 'text/csv') {
    data = Buffer.from(stringify(maskedRows, { header: true, columns: effectiveCols }));
  } else if (ext === 'ndjson' || contentType === 'application/x-ndjson') {
    data = Buffer.from(maskedRows.map(r => JSON.stringify(r)).join('\n') + '\n');
  } else {
    data = Buffer.from(JSON.stringify(maskedRows, null, 2));
  }

  return { data, piiMasked: report.totalMasked, piiColumns: report.maskedColumns };
}
