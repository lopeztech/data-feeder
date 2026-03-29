import { cloudEvent, CloudEvent } from '@google-cloud/functions-framework';
import { Storage } from '@google-cloud/storage';
import { Firestore } from '@google-cloud/firestore';
import { PubSub } from '@google-cloud/pubsub';
import { validate } from './validators.js';
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
      // Copy to Silver bucket
      await storage
        .bucket(RAW_BUCKET)
        .file(objectName)
        .copy(storage.bucket(SILVER_BUCKET).file(objectName));

      // Update Firestore
      await jobRef.update({
        status: 'TRANSFORMING',
        silver_path: `gs://${SILVER_BUCKET}/${objectName}`,
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

      console.log(`Job ${jobId}: validated ${result.totalRecords} records → Silver`);
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
