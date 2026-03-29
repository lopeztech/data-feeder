import { Router, Request, Response } from 'express';
import { Storage } from '@google-cloud/storage';
import { Firestore } from '@google-cloud/firestore';
import crypto from 'crypto';

const router = Router();

const storage = new Storage();
const firestore = new Firestore({
  databaseId: process.env.FIRESTORE_DATABASE || '(default)',
});

const RAW_BUCKET = process.env.GCS_RAW_BUCKET || 'data-feeder-lcd-raw';
const RESUMABLE_THRESHOLD = 5 * 1024 * 1024; // 5 MB

interface InitBody {
  filename: string;
  contentType: string;
  fileSize: number;
  dataset: string;
  bqTable: string;
  description?: string;
}

function extractUser(req: Request): { uid: string; email: string } {
  // The frontend sends the Google ID token as Bearer token.
  // For now, decode the JWT payload (signature was verified by Google on the client).
  // A production setup would verify the token server-side.
  const auth = req.headers.authorization;
  if (!auth?.startsWith('Bearer ')) {
    return { uid: 'anonymous', email: 'anonymous' };
  }
  try {
    const payload = JSON.parse(
      Buffer.from(auth.slice(7).split('.')[1], 'base64url').toString()
    );
    return { uid: payload.sub ?? 'unknown', email: payload.email ?? 'unknown' };
  } catch {
    return { uid: 'unknown', email: 'unknown' };
  }
}

router.post('/init', async (req: Request, res: Response) => {
  try {
    const body = req.body as InitBody;

    if (!body.filename || !body.dataset || !body.fileSize) {
      res.status(400).json({ error: 'Missing required fields: filename, dataset, fileSize' });
      return;
    }

    const user = extractUser(req);
    const jobId = crypto.randomUUID();
    const objectPath = `${body.dataset}/${jobId}/${body.filename}`;
    const contentType = body.contentType || 'application/octet-stream';
    const isResumable = body.fileSize > RESUMABLE_THRESHOLD;

    const bucket = storage.bucket(RAW_BUCKET);
    const file = bucket.file(objectPath);

    let signedUrl: string;

    if (isResumable) {
      // Generate a resumable upload URI
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
      // Generate a v4 signed URL for a simple PUT
      const [url] = await file.getSignedUrl({
        version: 'v4',
        action: 'write',
        expires: Date.now() + 15 * 60 * 1000, // 15 minutes
        contentType,
      });
      signedUrl = url;
    }

    // Create Firestore job document
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

    res.json({
      uploadId: jobId,
      signedUrl,
      objectPath,
      uploadType: isResumable ? 'resumable' : 'simple',
    });
  } catch (err) {
    console.error('Upload init error:', err);
    res.status(500).json({ error: 'Failed to initialize upload' });
  }
});

router.get('/:uploadId/status', async (req: Request<{ uploadId: string }>, res: Response) => {
  try {
    const doc = await firestore.collection('jobs').doc(req.params.uploadId).get();
    if (!doc.exists) {
      res.status(404).json({ error: 'Job not found' });
      return;
    }
    res.json(doc.data());
  } catch (err) {
    console.error('Status fetch error:', err);
    res.status(500).json({ error: 'Failed to fetch job status' });
  }
});

export { router };
