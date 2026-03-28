import { auth } from './firebase';

export const RESUMABLE_THRESHOLD = 5 * 1024 * 1024; // 5 MB
const CHUNK_SIZE = 8 * 1024 * 1024; // 8 MB per chunk

export interface InitUploadRequest {
  filename: string;
  contentType: string;
  fileSize: number;
  dataset: string;
  bqTable: string;
  description?: string;
}

export interface InitUploadResponse {
  uploadId: string;
  signedUrl: string;
  objectPath: string;
  uploadType: 'simple' | 'resumable';
}

async function getAuthToken(): Promise<string | null> {
  return auth?.currentUser?.getIdToken() ?? null;
}

export async function initUpload(req: InitUploadRequest): Promise<InitUploadResponse> {
  const token = await getAuthToken();
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch('/api/uploads/init', {
    method: 'POST',
    headers,
    body: JSON.stringify(req),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`Upload init failed (${res.status}): ${text}`);
  }
  return res.json() as Promise<InitUploadResponse>;
}

export async function getUploadStatus(uploadId: string): Promise<unknown> {
  const token = await getAuthToken();
  const headers: Record<string, string> = {};
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const res = await fetch(`/api/uploads/${encodeURIComponent(uploadId)}/status`, { headers });
  if (!res.ok) throw new Error(`Status fetch failed (${res.status})`);
  return res.json();
}

/**
 * Simple PUT upload for files <= RESUMABLE_THRESHOLD.
 * The signedUrl is a GCS v4 signed URL that accepts a single PUT.
 */
export function simpleUploadToGCS(
  signedUrl: string,
  file: File,
  onProgress: (percent: number) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();

    xhr.upload.addEventListener('progress', (e) => {
      if (e.lengthComputable) onProgress(Math.round((e.loaded / e.total) * 100));
    });
    xhr.addEventListener('load', () => {
      if (xhr.status >= 200 && xhr.status < 300) resolve();
      else reject(new Error(`GCS upload failed: ${xhr.status} ${xhr.statusText}`));
    });
    xhr.addEventListener('error', () => reject(new Error('GCS upload network error')));
    xhr.addEventListener('abort', () => reject(new Error('GCS upload aborted')));

    xhr.open('PUT', signedUrl);
    xhr.setRequestHeader('Content-Type', file.type || 'application/octet-stream');
    xhr.send(file);
  });
}

/**
 * Resumable (chunked) upload for files > RESUMABLE_THRESHOLD.
 * The sessionUri is a GCS resumable upload session URI returned by the backend
 * after it initiates the session. Chunks are sent with Content-Range headers
 * per the GCS resumable upload protocol (HTTP 308 = chunk accepted, continue).
 */
export function resumableUploadToGCS(
  sessionUri: string,
  file: File,
  onProgress: (percent: number) => void,
): Promise<void> {
  return new Promise((resolve, reject) => {
    let offset = 0;

    function uploadChunk() {
      const end = Math.min(offset + CHUNK_SIZE, file.size);
      const chunk = file.slice(offset, end);
      const contentRange = `bytes ${offset}-${end - 1}/${file.size}`;

      const xhr = new XMLHttpRequest();

      xhr.upload.addEventListener('progress', (e) => {
        if (e.lengthComputable) {
          onProgress(Math.round(((offset + e.loaded) / file.size) * 100));
        }
      });

      xhr.addEventListener('load', () => {
        if (xhr.status === 308) {
          // Resume Incomplete — GCS accepted chunk, advance offset from Range header
          const range = xhr.getResponseHeader('Range');
          offset = range ? parseInt(range.split('-')[1], 10) + 1 : end;
          uploadChunk();
        } else if (xhr.status >= 200 && xhr.status < 300) {
          onProgress(100);
          resolve();
        } else {
          reject(new Error(`Resumable upload failed at byte ${offset}: ${xhr.status} ${xhr.statusText}`));
        }
      });

      xhr.addEventListener('error', () => reject(new Error('Resumable upload network error')));
      xhr.addEventListener('abort', () => reject(new Error('Resumable upload aborted')));

      xhr.open('PUT', sessionUri);
      xhr.setRequestHeader('Content-Range', contentRange);
      xhr.setRequestHeader('Content-Type', file.type || 'application/octet-stream');
      xhr.send(chunk);
    }

    uploadChunk();
  });
}
