import { cloudEvent, CloudEvent } from '@google-cloud/functions-framework';
import { Storage } from '@google-cloud/storage';
import { Firestore } from '@google-cloud/firestore';
import { PubSub } from '@google-cloud/pubsub';
import { parse } from 'csv-parse/sync';
import crypto from 'crypto';

import { validate } from './validators.js';
import { maskPii } from './pii.js';
import { transformRows } from './schema.js';
import type { GcsNotification, MessagePublishedData, RejectedRecord } from './types.js';

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
const FILE_UPLOADED_TOPIC = process.env.FILE_UPLOADED_TOPIC || 'file-uploaded';
const ZIP_EXTS = new Set(['csv', 'json', 'ndjson', 'parquet', 'avro']);
const ZIP_CT: Record<string, string> = { csv: 'text/csv', json: 'application/json', ndjson: 'application/x-ndjson', parquet: 'application/octet-stream', avro: 'application/octet-stream' };

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

    // Handle ZIP archives
    if (getExtension(filename) === 'zip' || contentType === 'application/zip') {
      await handleZipArchive(buffer, objectName, jobId, jobRef, notification);
      return;
    }

    // Validate file structure
    const result = validate(buffer, contentType, filename);

    if (!result.valid) {
      // Entire file is structurally invalid — reject it wholesale
      await rejectFile(objectName, buffer, jobRef, jobId, notification, result.error ?? 'Validation failed');
      return;
    }

    // For binary formats (Parquet/Avro), pass through unchanged (already typed)
    const ext = getExtension(filename);
    if (['parquet', 'avro'].includes(ext)) {
      await storage.bucket(SILVER_BUCKET).file(objectName).save(buffer, { contentType });

      await jobRef.update({
        status: 'TRANSFORMING',
        silver_path: `gs://${SILVER_BUCKET}/${objectName}`,
        stats: { total_records: result.totalRecords, valid: result.totalRecords, rejected: 0, loaded: 0 },
        updated_at: new Date().toISOString(),
      });

      await pubsub.topic(VALIDATION_COMPLETE_TOPIC).publishMessage({
        json: {
          jobId,
          dataset: notification.metadata?.dataset,
          silverPath: `gs://${SILVER_BUCKET}/${objectName}`,
          totalRecords: result.totalRecords,
          contentType,
        },
      });

      console.log(`Job ${jobId}: binary format passed through → Silver`);
      return;
    }

    // Parse text formats into rows for transformation
    const rows = parseTextRows(buffer, contentType, ext);
    const columns = result.columns ?? Object.keys(rows[0] ?? {});

    // Step 1: PII masking (before type casting so we work on strings)
    const { maskedRows, report: piiReport } = maskPii(rows, columns);

    // Step 2: Type casting, null standardization, deduplication, per-record validation
    const { validRows, rejectedRows, schema } = transformRows(maskedRows, columns);

    if (validRows.length === 0) {
      // All records rejected
      await rejectFile(objectName, buffer, jobRef, jobId, notification,
        `All ${rejectedRows.length} records failed type validation`);
      return;
    }

    // Step 3: Write rejected records to rejection bucket with metadata
    if (rejectedRows.length > 0) {
      await writeRejectionManifest(objectName, rejectedRows, jobId);
    }

    // Step 4: Serialize valid rows to CSV and write to Silver
    const { stringify } = await import('csv-stringify/sync');
    const silverObjectName = objectName.replace(/\.[^.]+$/, '.csv');
    const silverBuffer = Buffer.from(
      stringify(validRows, { header: true, columns: columns }),
    );

    await storage
      .bucket(SILVER_BUCKET)
      .file(silverObjectName)
      .save(silverBuffer, { contentType: 'text/csv' });

    const piiInfo = piiReport.totalMasked > 0
      ? ` (PII masked: ${piiReport.maskedColumns.map(c => c.column).join(', ')})`
      : '';

    const dedupCount = rows.length - validRows.length - rejectedRows.length;

    await jobRef.update({
      status: 'TRANSFORMING',
      silver_path: `gs://${SILVER_BUCKET}/${silverObjectName}`,
      pii_masked: piiReport.maskedColumns,
      schema: schema.map(s => ({ name: s.name, type: s.type })),
      stats: {
        total_records: rows.length,
        valid: validRows.length,
        rejected: rejectedRows.length,
        duplicates_removed: dedupCount,
        loaded: 0,
      },
      updated_at: new Date().toISOString(),
    });

    // Publish validation-complete
    await pubsub.topic(VALIDATION_COMPLETE_TOPIC).publishMessage({
      json: {
        jobId,
        dataset: notification.metadata?.dataset,
        silverPath: `gs://${SILVER_BUCKET}/${silverObjectName}`,
        totalRecords: validRows.length,
        contentType: 'text/csv',
      },
    });

    console.log(
      `Job ${jobId}: ${rows.length} records → ${validRows.length} valid, ` +
      `${rejectedRows.length} rejected, ${dedupCount} deduped → Silver (Parquet/Snappy)${piiInfo}`
    );
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

/**
 * Parse text-format buffers (CSV, JSON, NDJSON) into row arrays.
 */
function parseTextRows(
  buffer: Buffer,
  contentType: string,
  ext: string,
): Record<string, string>[] {
  if (ext === 'csv' || contentType === 'text/csv') {
    return parse(buffer, { columns: true, skip_empty_lines: true, relax_column_count: true }) as Record<string, string>[];
  }
  if (ext === 'json' || contentType === 'application/json') {
    const parsed = JSON.parse(buffer.toString('utf-8'));
    return (Array.isArray(parsed) ? parsed : [parsed]) as Record<string, string>[];
  }
  if (ext === 'ndjson' || contentType === 'application/x-ndjson') {
    return buffer.toString('utf-8').split('\n').filter(l => l.trim()).map(l => JSON.parse(l)) as Record<string, string>[];
  }
  return [];
}

/**
 * Reject an entire file: copy to rejected bucket, update Firestore, publish failure.
 */
async function rejectFile(
  objectName: string,
  _buffer: Buffer,
  jobRef: FirebaseFirestore.DocumentReference,
  jobId: string,
  notification: GcsNotification,
  error: string,
): Promise<void> {
  await storage
    .bucket(RAW_BUCKET)
    .file(objectName)
    .copy(storage.bucket(REJECTED_BUCKET).file(objectName));

  await jobRef.update({
    status: 'REJECTED',
    error,
    stats: { total_records: 0, valid: 0, rejected: 0, loaded: 0 },
    updated_at: new Date().toISOString(),
  });

  await pubsub.topic(PIPELINE_FAILED_TOPIC).publishMessage({
    json: {
      jobId,
      dataset: notification.metadata?.dataset,
      error,
      bronzePath: `gs://${RAW_BUCKET}/${objectName}`,
    },
  });

  console.log(`Job ${jobId}: rejected — ${error}`);
}

/**
 * Write rejected records and their errors to the rejection bucket as a NDJSON manifest.
 */
async function writeRejectionManifest(
  objectName: string,
  rejectedRows: RejectedRecord[],
  jobId: string,
): Promise<void> {
  const manifestPath = objectName.replace(/\.[^.]+$/, '_rejected.ndjson');
  const manifest = rejectedRows.map(r => JSON.stringify({
    job_id: jobId,
    errors: r.errors,
    original_row: r.row,
    rejected_at: new Date().toISOString(),
  })).join('\n') + '\n';

  await storage
    .bucket(REJECTED_BUCKET)
    .file(manifestPath)
    .save(Buffer.from(manifest), { contentType: 'application/x-ndjson' });

  console.log(`Job ${jobId}: wrote ${rejectedRows.length} rejected records to ${REJECTED_BUCKET}/${manifestPath}`);
}

async function handleZipArchive(
  buffer: Buffer, objectName: string, parentJobId: string,
  parentJobRef: FirebaseFirestore.DocumentReference, notification: GcsNotification,
): Promise<void> {
  const AdmZip = (await import('adm-zip')).default;
  const zip = new AdmZip(buffer);
  const entries = zip.getEntries().filter(e => {
    if (e.isDirectory) return false;
    const n = e.entryName.split('/').pop() ?? '';
    return !n.startsWith('.') && !n.startsWith('__') && ZIP_EXTS.has(getExtension(n));
  });

  if (entries.length === 0) {
    await parentJobRef.update({ status: 'REJECTED', error: 'ZIP contains no supported files', updated_at: new Date().toISOString() });
    console.log(`Job ${parentJobId}: ZIP rejected — no supported files`);
    return;
  }

  const childJobIds: string[] = [];
  const basePath = objectName.replace(/[^/]+$/, '');

  for (const entry of entries) {
    const data = entry.getData();
    const name = entry.entryName.split('/').pop() ?? entry.entryName;
    const ct = ZIP_CT[getExtension(name)] || 'application/octet-stream';
    const childId = crypto.randomUUID();
    const ds = name.replace(/\.[^.]+$/, '').toLowerCase().replace(/[\s-]+/g, '_');
    const path = `${basePath}${childId}/${name}`;
    const upBy = notification.metadata?.uploadedBy ?? '';

    await storage.bucket(RAW_BUCKET).file(path).save(data, {
      contentType: ct, metadata: { dataset: ds, jobId: childId, uploadedBy: upBy, parentJobId },
    });

    const now = new Date().toISOString();
    await firestore.collection('jobs').doc(childId).set({
      job_id: childId, dataset: ds, filename: name, file_size_bytes: data.length,
      content_type: ct, status: 'UPLOADING', uploaded_by: upBy,
      created_at: now, updated_at: now, bronze_path: `gs://${RAW_BUCKET}/${path}`,
      silver_path: null, bq_table: ds, description: `Extracted from ${notification.name}`,
      parent_job_id: parentJobId, stats: { total_records: 0, valid: 0, rejected: 0, loaded: 0 }, error: null,
    });

    await pubsub.topic(FILE_UPLOADED_TOPIC).publishMessage({
      json: { kind: 'storage#object', name: path, bucket: RAW_BUCKET, contentType: ct,
        size: String(data.length), metadata: { dataset: ds, jobId: childId, uploadedBy: upBy } },
    });

    childJobIds.push(childId);
    console.log(`Job ${parentJobId}: extracted ${name} → child ${childId}`);
  }

  await parentJobRef.update({
    status: 'LOADED', child_jobs: childJobIds,
    stats: { total_records: entries.length, valid: entries.length, rejected: 0, loaded: 0 },
    updated_at: new Date().toISOString(),
  });
  console.log(`Job ${parentJobId}: ZIP → ${childJobIds.length} child jobs`);
}
