import { cloudEvent, CloudEvent } from '@google-cloud/functions-framework';
import { google } from 'googleapis';
import { Firestore } from '@google-cloud/firestore';
import type { ValidationCompleteMessage, MessagePublishedData } from './types.js';

const PROJECT_ID = process.env.GCP_PROJECT || 'data-feeder-lcd';
const REGION = process.env.GCP_REGION || 'australia-southeast1';
const TEMPLATE_PATH = process.env.DATAFLOW_TEMPLATE_PATH || '';
const TEMP_LOCATION = process.env.DATAFLOW_TEMP_LOCATION || `gs://${PROJECT_ID}-dataflow-temp/tmp`;
const FIRESTORE_DATABASE = process.env.FIRESTORE_DATABASE || 'data-feeder';

const firestore = new Firestore({ databaseId: FIRESTORE_DATABASE });

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

  const { jobId, dataset, silverPath } = msg;

  if (!jobId || !silverPath) {
    console.warn('Missing jobId or silverPath in message, skipping');
    return;
  }

  // Idempotency guard: only process TRANSFORMING jobs
  const jobRef = firestore.collection('jobs').doc(jobId);
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

  const tableName = jobDoc.data()?.bq_table || dataset;

  console.log(`Launching Dataflow job for ${jobId}: ${silverPath} → ${tableName}`);

  const auth = new google.auth.GoogleAuth({
    scopes: ['https://www.googleapis.com/auth/cloud-platform'],
  });
  const dataflow = google.dataflow({ version: 'v1b3', auth });

  await dataflow.projects.locations.flexTemplates.launch({
    projectId: PROJECT_ID,
    location: REGION,
    requestBody: {
      launchParameter: {
        jobName: `loader-${jobId.replace(/[^a-z0-9-]/gi, '-').toLowerCase()}`,
        containerSpecGcsPath: TEMPLATE_PATH,
        parameters: {
          silver_gcs_path: silverPath,
          target_bq_table: tableName,
          job_id: jobId,
          project_id: PROJECT_ID,
        },
        environment: {
          numWorkers: 1,
          maxWorkers: 20,
          tempLocation: TEMP_LOCATION,
        },
      },
    },
  });

  console.log(`Dataflow job launched for ${jobId}`);
});
