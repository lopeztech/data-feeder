import { http } from '@google-cloud/functions-framework';
import { Storage } from '@google-cloud/storage';
import { Firestore } from '@google-cloud/firestore';
import { PubSub } from '@google-cloud/pubsub';
import { BigQuery } from '@google-cloud/bigquery';
import { parse } from 'csv-parse/sync';
import crypto from 'crypto';

const storage = new Storage();
const firestore = new Firestore({
  databaseId: process.env.FIRESTORE_DATABASE || 'data-feeder',
});
const pubsub = new PubSub();
const bigquery = new BigQuery();

const RAW_BUCKET = process.env.GCS_RAW_BUCKET || 'data-feeder-lcd-raw';
const SILVER_BUCKET = process.env.GCS_SILVER_BUCKET || 'data-feeder-lcd-staging';
const BQ_CURATED_DATASET = process.env.BQ_CURATED_DATASET || 'curated';
const FILE_UPLOADED_TOPIC = process.env.PUBSUB_FILE_UPLOADED_TOPIC || 'file-uploaded';
const RESUMABLE_THRESHOLD = 5 * 1024 * 1024; // 5 MB

// Allowed origins for CORS
const ALLOWED_ORIGINS = [
  'https://datafeeder.lopezcloud.dev',
  'http://localhost:5173',
];

interface InitBody {
  filename: string;
  contentType: string;
  fileSize: number;
  dataset: string;
  bqTable: string;
  description?: string;
}

function extractUser(authHeader: string | undefined): { uid: string; email: string } {
  if (!authHeader?.startsWith('Bearer ')) {
    return { uid: 'anonymous', email: 'anonymous' };
  }
  try {
    const payload = JSON.parse(
      Buffer.from(authHeader.slice(7).split('.')[1], 'base64url').toString()
    );
    return { uid: payload.sub ?? 'unknown', email: payload.email ?? 'unknown' };
  } catch {
    return { uid: 'unknown', email: 'unknown' };
  }
}

http('uploadApi', (req, res) => {
  // CORS
  const origin = req.headers.origin ?? '';
  if (ALLOWED_ORIGINS.includes(origin)) {
    res.set('Access-Control-Allow-Origin', origin);
  }
  res.set('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.set('Access-Control-Max-Age', '3600');

  if (req.method === 'OPTIONS') {
    res.status(204).send('');
    return;
  }

  // Route: POST /init
  if (req.method === 'POST' && req.path === '/init') {
    handleInit(req, res);
    return;
  }

  // Route: GET /jobs
  if (req.method === 'GET' && req.path === '/jobs') {
    handleListJobs(res);
    return;
  }

  // Route: POST /:uploadId/retrigger
  const retriggerMatch = req.path.match(/^\/([a-f0-9-]+)\/retrigger$/);
  if (req.method === 'POST' && retriggerMatch) {
    handleRetrigger(retriggerMatch[1], res);
    return;
  }

  // Route: GET /:uploadId/preview
  const previewMatch = req.path.match(/^\/([a-f0-9-]+)\/preview$/);
  if (req.method === 'GET' && previewMatch) {
    handlePreview(previewMatch[1], res);
    return;
  }

  // Route: DELETE /:uploadId
  const deleteMatch = req.path.match(/^\/([a-f0-9-]+)$/);
  if (req.method === 'DELETE' && deleteMatch) {
    handleDelete(deleteMatch[1], res);
    return;
  }

  // Route: GET /:uploadId/status
  const statusMatch = req.path.match(/^\/([a-f0-9-]+)\/status$/);
  if (req.method === 'GET' && statusMatch) {
    handleStatus(statusMatch[1], res);
    return;
  }

  res.status(404).json({ error: 'Not found' });
});

async function handleInit(
  req: { body: unknown; headers: { authorization?: string } },
  res: { status: (code: number) => { json: (data: unknown) => void } },
) {
  try {
    const body = req.body as InitBody;

    if (!body.filename || !body.dataset || !body.fileSize) {
      res.status(400).json({ error: 'Missing required fields: filename, dataset, fileSize' });
      return;
    }

    const user = extractUser(req.headers.authorization);
    const jobId = crypto.randomUUID();
    const objectPath = `${body.dataset}/${jobId}/${body.filename}`;
    const contentType = body.contentType || 'application/octet-stream';
    const isResumable = body.fileSize > RESUMABLE_THRESHOLD;

    const bucket = storage.bucket(RAW_BUCKET);
    const file = bucket.file(objectPath);

    let signedUrl: string;

    if (isResumable) {
      const [uri] = await file.createResumableUpload({
        metadata: {
          contentType,
          metadata: {
            dataset: body.dataset,
            jobId,
            uploadedBy: user.email,
          },
        },
      });
      signedUrl = uri;
    } else {
      const [url] = await file.getSignedUrl({
        version: 'v4',
        action: 'write',
        expires: Date.now() + 15 * 60 * 1000,
        contentType,
      });
      signedUrl = url;
    }

    const now = new Date().toISOString();
    const jobDoc = {
      job_id: jobId,
      dataset: body.dataset,
      filename: body.filename,
      file_size_bytes: body.fileSize,
      content_type: contentType,
      status: 'UPLOADING',
      uploaded_by: user.email,
      created_at: now,
      updated_at: now,
      bronze_path: `gs://${RAW_BUCKET}/${objectPath}`,
      silver_path: null,
      bq_table: body.bqTable || body.dataset,
      description: body.description || null,
      stats: { total_records: 0, valid: 0, rejected: 0, loaded: 0 },
      error: null,
    };

    await firestore.collection('jobs').doc(jobId).set(jobDoc);

    res.status(200).json({
      uploadId: jobId,
      signedUrl,
      objectPath,
      uploadType: isResumable ? 'resumable' : 'simple',
    });
  } catch (err) {
    console.error('Upload init error:', err);
    res.status(500).json({ error: 'Failed to initialize upload' });
  }
}

async function handleStatus(
  uploadId: string,
  res: { status: (code: number) => { json: (data: unknown) => void } },
) {
  try {
    const doc = await firestore.collection('jobs').doc(uploadId).get();
    if (!doc.exists) {
      res.status(404).json({ error: 'Job not found' });
      return;
    }
    res.status(200).json(doc.data());
  } catch (err) {
    console.error('Status fetch error:', err);
    res.status(500).json({ error: 'Failed to fetch job status' });
  }
}

async function handleDelete(
  jobId: string,
  res: { status: (code: number) => { json: (data: unknown) => void } },
) {
  try {
    const docRef = firestore.collection('jobs').doc(jobId);
    const doc = await docRef.get();

    if (!doc.exists) {
      res.status(404).json({ error: 'Job not found' });
      return;
    }

    await docRef.delete();
    res.status(200).json({ deleted: true, jobId });
  } catch (err) {
    console.error('Delete error:', err);
    res.status(500).json({ error: 'Failed to delete job' });
  }
}

const PREVIEW_ROWS = 10;

function parseGcsPath(gsPath: string): { bucket: string; object: string } | null {
  const m = gsPath.match(/^gs:\/\/([^/]+)\/(.+)$/);
  return m ? { bucket: m[1], object: m[2] } : null;
}

function sampleGcsFile(buffer: Buffer, contentType: string): Record<string, unknown>[] {
  const text = buffer.slice(0, 256 * 1024).toString('utf-8');

  if (contentType === 'text/csv' || contentType.includes('csv')) {
    const records = parse(text, {
      columns: true,
      skip_empty_lines: true,
      to: PREVIEW_ROWS,
      relax_column_count: true,
    }) as Record<string, unknown>[];
    return records;
  }

  if (contentType === 'application/x-ndjson' || contentType.includes('ndjson')) {
    return text.split('\n').filter(l => l.trim()).slice(0, PREVIEW_ROWS).map(l => JSON.parse(l));
  }

  if (contentType === 'application/json' || contentType.includes('json')) {
    const parsed = JSON.parse(text);
    const arr = Array.isArray(parsed) ? parsed : [parsed];
    return arr.slice(0, PREVIEW_ROWS);
  }

  return [];
}

async function handlePreview(
  jobId: string,
  res: { status: (code: number) => { json: (data: unknown) => void } },
) {
  try {
    const doc = await firestore.collection('jobs').doc(jobId).get();
    if (!doc.exists) {
      res.status(404).json({ error: 'Job not found' });
      return;
    }

    const job = doc.data()!;
    const ct = (job.content_type as string) || 'text/csv';
    const result: {
      bronze: Record<string, unknown>[] | null;
      silver: Record<string, unknown>[] | null;
      curated: Record<string, unknown>[] | null;
    } = { bronze: null, silver: null, curated: null };

    // Bronze preview
    if (job.bronze_path) {
      try {
        const gcs = parseGcsPath(job.bronze_path as string);
        if (gcs) {
          const [buf] = await storage.bucket(gcs.bucket).file(gcs.object).download({ start: 0, end: 256 * 1024 });
          result.bronze = sampleGcsFile(buf, ct);
        }
      } catch { /* file may not exist */ }
    }

    // Silver preview
    if (job.silver_path) {
      try {
        const gcs = parseGcsPath(job.silver_path as string);
        if (gcs) {
          const [buf] = await storage.bucket(gcs.bucket).file(gcs.object).download({ start: 0, end: 256 * 1024 });
          result.silver = sampleGcsFile(buf, ct);
        }
      } catch { /* file may not exist */ }
    }

    // Curated preview (BigQuery)
    if (job.bq_table && job.status === 'LOADED') {
      try {
        const tableName = (job.bq_table as string).includes('.')
          ? (job.bq_table as string)
          : `${BQ_CURATED_DATASET}.${job.bq_table}`;
        const [rows] = await bigquery.query({
          query: `SELECT * FROM \`${tableName}\` LIMIT ${PREVIEW_ROWS}`,
        });
        result.curated = rows;
      } catch { /* table may not exist yet */ }
    }

    res.status(200).json(result);
  } catch (err) {
    console.error('Preview error:', err);
    res.status(500).json({ error: 'Failed to fetch preview' });
  }
}

const RETRIGGERABLE_STATUSES = ['UPLOADING', 'FAILED', 'REJECTED', 'TRANSFORMING'];

async function handleRetrigger(
  jobId: string,
  res: { status: (code: number) => { json: (data: unknown) => void } },
) {
  try {
    const docRef = firestore.collection('jobs').doc(jobId);
    const doc = await docRef.get();

    if (!doc.exists) {
      res.status(404).json({ error: 'Job not found' });
      return;
    }

    const job = doc.data()!;

    if (!RETRIGGERABLE_STATUSES.includes(job.status)) {
      res.status(409).json({ error: `Cannot retrigger job with status ${job.status}` });
      return;
    }

    // Verify file exists in Bronze
    const bronzePath = (job.bronze_path as string).replace(`gs://${RAW_BUCKET}/`, '');
    const [exists] = await storage.bucket(RAW_BUCKET).file(bronzePath).exists();
    if (!exists) {
      res.status(410).json({ error: 'Source file no longer exists in Bronze bucket' });
      return;
    }

    // Reset job status to UPLOADING atomically
    await docRef.update({
      status: 'UPLOADING',
      error: null,
      silver_path: null,
      stats: { total_records: 0, valid: 0, rejected: 0, loaded: 0 },
      updated_at: new Date().toISOString(),
    });

    // Publish synthetic GCS notification to trigger validator
    await pubsub.topic(FILE_UPLOADED_TOPIC).publishMessage({
      json: {
        kind: 'storage#object',
        name: bronzePath,
        bucket: RAW_BUCKET,
        contentType: job.content_type,
        size: String(job.file_size_bytes),
        metadata: {
          dataset: job.dataset,
          jobId,
          uploadedBy: job.uploaded_by,
        },
      },
    });

    res.status(200).json({ status: 'retriggered', jobId });
  } catch (err) {
    console.error('Retrigger error:', err);
    res.status(500).json({ error: 'Failed to retrigger job' });
  }
}

async function handleListJobs(
  res: { status: (code: number) => { json: (data: unknown) => void } },
) {
  try {
    const snapshot = await firestore
      .collection('jobs')
      .orderBy('created_at', 'desc')
      .limit(100)
      .get();
    const jobs = snapshot.docs.map(doc => doc.data());
    res.status(200).json(jobs);
  } catch (err) {
    console.error('List jobs error:', err);
    res.status(500).json({ error: 'Failed to list jobs' });
  }
}
