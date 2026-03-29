import { useCallback, useState } from 'react';
import { useDropzone } from 'react-dropzone';
import { useAuth } from '../context/AuthContext';
import {
  initUpload,
  simpleUploadToGCS,
  resumableUploadToGCS,
  RESUMABLE_THRESHOLD,
} from '../lib/uploadService';

const ACCEPTED_FORMATS: Record<string, string[]> = {
  'text/csv': ['.csv'],
  'application/json': ['.json'],
  'application/x-ndjson': ['.ndjson'],
  'application/octet-stream': ['.parquet', '.avro'],
};

const FORMAT_LABELS: Record<string, string> = {
  csv: 'CSV',
  json: 'JSON',
  ndjson: 'NDJSON',
  parquet: 'Parquet',
  avro: 'Avro',
};

const TYPE_COLORS: Record<string, string> = {
  string: 'bg-gray-100 text-gray-600',
  number: 'bg-blue-100 text-blue-700',
  boolean: 'bg-purple-100 text-purple-700',
  date: 'bg-green-100 text-green-700',
  null: 'bg-yellow-100 text-yellow-700',
};

const LARGE_FILE_THRESHOLD = 500 * 1024 * 1024; // 500 MB — UI warning threshold

type ColumnType = 'string' | 'number' | 'boolean' | 'date' | 'null';

interface SchemaPreview {
  columns: string[];
  types: Record<string, ColumnType>;
  rows: string[][];
}

function formatBytes(bytes: number) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1_048_576) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1_073_741_824) return `${(bytes / 1_048_576).toFixed(1)} MB`;
  return `${(bytes / 1_073_741_824).toFixed(2)} GB`;
}

function getExtension(filename: string) {
  return filename.split('.').pop()?.toLowerCase() ?? '';
}

function detectType(values: string[]): ColumnType {
  const nonEmpty = values.filter(v => v !== '' && v.toLowerCase() !== 'null');
  if (nonEmpty.length === 0) return 'null';
  if (nonEmpty.every(v => /^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(v))) return 'number';
  if (nonEmpty.every(v => v.toLowerCase() === 'true' || v.toLowerCase() === 'false')) return 'boolean';
  if (nonEmpty.every(v => /^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?/.test(v))) return 'date';
  return 'string';
}

function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      inQuotes = !inQuotes;
    } else if (ch === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += ch;
    }
  }
  result.push(current.trim());
  return result;
}

async function inferSchema(file: File): Promise<SchemaPreview | null> {
  const ext = getExtension(file.name);
  if (!['csv', 'json', 'ndjson'].includes(ext)) return null;

  const text = await file.slice(0, 512 * 1024).text();

  if (ext === 'csv') {
    const lines = text.split('\n').map(l => l.trimEnd()).filter(l => l.length > 0);
    if (lines.length < 2) return null;
    const columns = parseCSVLine(lines[0]);
    const rows = lines.slice(1, 11).map(l => parseCSVLine(l));
    const types: Record<string, ColumnType> = {};
    columns.forEach((col, i) => {
      types[col] = detectType(rows.map(r => r[i] ?? ''));
    });
    return { columns, types, rows };
  }

  if (ext === 'json') {
    try {
      const parsed = JSON.parse(text);
      const arr: Record<string, unknown>[] = (Array.isArray(parsed) ? parsed : [parsed]).slice(0, 10);
      if (arr.length === 0 || typeof arr[0] !== 'object' || arr[0] === null) return null;
      const columns = Object.keys(arr[0]);
      const rows = arr.map(obj => columns.map(c => String((obj as Record<string, unknown>)[c] ?? '')));
      const types: Record<string, ColumnType> = {};
      columns.forEach((col, i) => { types[col] = detectType(rows.map(r => r[i])); });
      return { columns, types, rows };
    } catch {
      return null;
    }
  }

  if (ext === 'ndjson') {
    try {
      const lines = text.split('\n').map(l => l.trim()).filter(l => l.length > 0).slice(0, 10);
      const arr = lines.map(l => JSON.parse(l) as Record<string, unknown>);
      if (arr.length === 0) return null;
      const columns = Object.keys(arr[0]);
      const rows = arr.map(obj => columns.map(c => String(obj[c] ?? '')));
      const types: Record<string, ColumnType> = {};
      columns.forEach((col, i) => { types[col] = detectType(rows.map(r => r[i])); });
      return { columns, types, rows };
    } catch {
      return null;
    }
  }

  return null;
}

export default function UploadPage() {
  const { user } = useAuth();
  const isGuest = user?.role === 'guest';

  const [file, setFile] = useState<File | null>(null);
  const [dataset, setDataset] = useState('');
  const [datasetTouched, setDatasetTouched] = useState(false);
  const [description, setDescription] = useState('');
  const [bqTable, setBqTable] = useState('');
  const [bqTableTouched, setBqTableTouched] = useState(false);
  const [schemaFile, setSchemaFile] = useState<File | null>(null);
  const [preview, setPreview] = useState<SchemaPreview | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [progress, setProgress] = useState<number | null>(null);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [uploadId, setUploadId] = useState<string | null>(null);
  const [submitted, setSubmitted] = useState(false);

  // Run schema inference when a new file is selected
  const handleFileChange = useCallback((newFile: File | null) => {
    setFile(newFile);
    if (!newFile) { setPreview(null); setPreviewLoading(false); return; }
    // Auto-populate dataset and BQ table from filename (strip extension, lowercase, underscores)
    const baseName = newFile.name.replace(/\.[^.]+$/, '').toLowerCase().replace(/[\s-]+/g, '_');
    if (!datasetTouched) setDataset(baseName);
    if (!bqTableTouched) setBqTable(baseName);
    setPreviewLoading(true);
    inferSchema(newFile).then(result => {
      setPreview(result);
      setPreviewLoading(false);
    });
  }, [datasetTouched, bqTableTouched]);

  const onDrop = useCallback((accepted: File[]) => {
    if (accepted.length > 0) handleFileChange(accepted[0]);
  }, [handleFileChange]);

  const { getRootProps, getInputProps, isDragActive, fileRejections } = useDropzone({
    onDrop,
    accept: ACCEPTED_FORMATS,
    maxFiles: 1,
    disabled: isGuest,
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!file || !dataset || isGuest) return;

    setProgress(0);
    setUploadError(null);

    try {
      const init = await initUpload({
        filename: file.name,
        contentType: file.type || 'application/octet-stream',
        fileSize: file.size,
        dataset,
        bqTable: bqTable || dataset,
        description: description || undefined,
      });

      setUploadId(init.uploadId);

      if (init.uploadType === 'resumable') {
        await resumableUploadToGCS(init.signedUrl, file, setProgress);
      } else {
        await simpleUploadToGCS(init.signedUrl, file, setProgress);
      }

      setSubmitted(true);
    } catch (err) {
      setUploadError(err instanceof Error ? err.message : 'Upload failed. Please try again.');
      setProgress(null);
    }
  };

  const handleReset = () => {
    handleFileChange(null); setDataset(''); setDatasetTouched(false); setDescription(''); setBqTable('');
    setBqTableTouched(false); setSchemaFile(null); setPreview(null);
    setProgress(null); setUploadError(null); setUploadId(null); setSubmitted(false);
  };

  const ext = file ? getExtension(file.name) : null;
  const isLargeFile = file ? file.size > LARGE_FILE_THRESHOLD : false;
  const effectiveBqTable = bqTable || dataset;

  if (submitted) {
    return (
      <div className="p-4 sm:p-8 max-w-2xl mx-auto flex flex-col items-center justify-center min-h-[60vh]">
        <div className="w-16 h-16 bg-green-100 rounded-full flex items-center justify-center mb-4">
          <svg className="w-8 h-8 text-green-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
          </svg>
        </div>
        <h2 className="text-xl font-semibold text-gray-900">Upload queued!</h2>
        <p className="text-gray-500 mt-2 text-sm text-center">
          <strong>{file?.name}</strong> has been uploaded to the Bronze zone.<br />
          The validation and transformation pipeline has been triggered.
        </p>
        <div className="mt-6 bg-gray-50 rounded-xl p-4 w-full text-xs text-gray-500 font-mono space-y-1">
          {uploadId && <p className="text-gray-400">job: <span className="text-gray-600">{uploadId}</span></p>}
          <p>→ Bronze: <span className="text-orange-600">gs://data-feeder-bronze/raw/{dataset}/</span></p>
          <p>→ Silver: validating schema &amp; type-casting…</p>
          <p>→ Gold: <span className="text-gray-400">pending</span> curated.{effectiveBqTable}</p>
        </div>
        <button onClick={handleReset} className="mt-6 px-4 py-2 bg-brand-600 text-white rounded-lg text-sm hover:bg-brand-700 transition">
          Upload another file
        </button>
      </div>
    );
  }

  return (
    <div className="p-4 sm:p-8 max-w-3xl mx-auto">
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">Upload Data</h1>
        <p className="text-gray-500 mt-1 text-sm">
          Files are uploaded directly to GCS Bronze zone and trigger the ingestion pipeline.
        </p>
      </div>

      {isGuest && (
        <div className="mb-6 p-4 bg-amber-50 border border-amber-200 rounded-xl flex gap-3">
          <svg className="w-5 h-5 text-amber-500 flex-shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M12 9v2m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
          </svg>
          <div>
            <p className="text-sm font-medium text-amber-800">Guest mode — uploads disabled</p>
            <p className="text-xs text-amber-600 mt-0.5">Sign in with Google to upload files and trigger real pipeline runs.</p>
          </div>
        </div>
      )}

      <form onSubmit={handleSubmit} className="space-y-6">
        {/* Drop zone */}
        <div
          {...getRootProps()}
          className={`border-2 border-dashed rounded-xl p-10 text-center transition cursor-pointer
            ${isGuest ? 'opacity-40 cursor-not-allowed border-gray-200' : ''}
            ${isDragActive ? 'border-brand-500 bg-brand-50' : 'border-gray-300 hover:border-brand-400 hover:bg-gray-50'}
            ${file ? 'border-green-400 bg-green-50' : ''}
          `}
        >
          <input {...getInputProps()} />
          {file ? (
            <div className="space-y-2">
              <div className="inline-flex items-center justify-center w-12 h-12 bg-green-100 rounded-xl">
                <svg className="w-6 h-6 text-green-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
              </div>
              <p className="font-medium text-gray-900">{file.name}</p>
              <p className="text-sm text-gray-500">
                {ext && FORMAT_LABELS[ext] ? FORMAT_LABELS[ext] : ext?.toUpperCase()} &middot; {formatBytes(file.size)}
              </p>
              <button
                type="button"
                onClick={(e) => { e.stopPropagation(); handleFileChange(null); }}
                className="text-xs text-red-500 hover:text-red-700 mt-1"
              >
                Remove
              </button>
            </div>
          ) : (
            <div className="space-y-2">
              <div className="inline-flex items-center justify-center w-12 h-12 bg-gray-100 rounded-xl">
                <svg className="w-6 h-6 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                    d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                </svg>
              </div>
              <p className="text-gray-600 font-medium">
                {isDragActive ? 'Drop the file here' : 'Drag & drop a file, or click to browse'}
              </p>
              <p className="text-xs text-gray-400">CSV, JSON, NDJSON, Parquet, Avro</p>
            </div>
          )}
        </div>

        {fileRejections.length > 0 && (
          <div className="flex items-start gap-2 p-3 bg-red-50 border border-red-200 rounded-lg">
            <svg className="w-4 h-4 text-red-500 flex-shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
            <p className="text-sm text-red-700">
              File rejected: {fileRejections[0].errors[0].message}. Supported formats: CSV, JSON, NDJSON, Parquet, Avro.
            </p>
          </div>
        )}

        {isLargeFile && (
          <div className="flex items-start gap-2 p-3 bg-amber-50 border border-amber-200 rounded-lg">
            <svg className="w-4 h-4 text-amber-500 flex-shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
            </svg>
            <p className="text-sm text-amber-800">
              <strong>Large file ({formatBytes(file!.size)})</strong> — this will use the GCS resumable (chunked) upload path.
            </p>
          </div>
        )}

        {/* Schema preview */}
        {file && ['csv', 'json', 'ndjson'].includes(ext ?? '') && (
          <div className="border border-gray-200 rounded-xl overflow-hidden">
            <div className="flex items-center justify-between px-4 py-2 bg-gray-50 border-b border-gray-200">
              <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Data preview</p>
              {preview && (
                <p className="text-xs text-gray-400">{preview.columns.length} columns · up to 10 rows</p>
              )}
            </div>
            {previewLoading && (
              <div className="flex items-center justify-center py-8">
                <div className="w-5 h-5 border-2 border-brand-600 border-t-transparent rounded-full animate-spin" />
              </div>
            )}
            {!previewLoading && preview && (
              <div className="overflow-x-auto">
                <table className="min-w-full text-xs">
                  <thead>
                    <tr className="bg-gray-50">
                      {preview.columns.map(col => (
                        <th key={col} className="px-3 py-2 text-left font-medium text-gray-600 whitespace-nowrap border-b border-gray-200">
                          <div className="flex items-center gap-1.5">
                            <span>{col}</span>
                            <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${TYPE_COLORS[preview.types[col]] ?? TYPE_COLORS.string}`}>
                              {preview.types[col]}
                            </span>
                          </div>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {preview.rows.map((row, i) => (
                      <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                        {row.map((cell, j) => (
                          <td key={j} className="px-3 py-1.5 text-gray-700 whitespace-nowrap max-w-[200px] truncate border-b border-gray-100">
                            {cell}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            {!previewLoading && !preview && (
              <p className="text-xs text-gray-400 px-4 py-3">Could not parse file for preview.</p>
            )}
          </div>
        )}

        {/* Metadata */}
        <div className="space-y-4">
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Dataset name <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                value={dataset}
                onChange={e => { const v = e.target.value.toLowerCase().replace(/\s+/g, '_'); setDataset(v); setDatasetTouched(true); if (!bqTableTouched) setBqTable(v); }}
                placeholder="e.g. sales_orders"
                disabled={isGuest}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-brand-500 disabled:opacity-40"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Target BigQuery table <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                value={bqTable}
                onChange={e => { setBqTable(e.target.value.toLowerCase().replace(/\s+/g, '_')); setBqTableTouched(true); }}
                placeholder={dataset || 'e.g. sales_orders'}
                disabled={isGuest}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-brand-500 disabled:opacity-40"
              />
              {effectiveBqTable && (
                <p className="text-xs text-gray-400 mt-1">
                  Destination: <code className="text-blue-600">curated.{effectiveBqTable}</code>
                </p>
              )}
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Description</label>
            <textarea
              rows={2}
              value={description}
              onChange={e => setDescription(e.target.value)}
              placeholder="Optional: describe what this data contains"
              disabled={isGuest}
              className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-brand-500 disabled:opacity-40 resize-none"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Expected schema <span className="text-gray-400 font-normal">(optional — JSON file)</span>
            </label>
            <div className="flex items-center gap-3">
              <label className={`flex items-center gap-2 px-3 py-2 border border-gray-300 rounded-lg text-sm cursor-pointer hover:bg-gray-50 transition ${isGuest ? 'opacity-40 pointer-events-none' : ''}`}>
                <svg className="w-4 h-4 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15.172 7l-6.586 6.586a2 2 0 102.828 2.828l6.414-6.586a4 4 0 00-5.656-5.656l-6.415 6.585a6 6 0 108.486 8.486L20.5 13" />
                </svg>
                {schemaFile ? schemaFile.name : 'Attach schema file'}
                <input
                  type="file"
                  accept=".json"
                  className="sr-only"
                  disabled={isGuest}
                  onChange={e => setSchemaFile(e.target.files?.[0] ?? null)}
                />
              </label>
              {schemaFile && (
                <button type="button" onClick={() => setSchemaFile(null)} className="text-xs text-red-500 hover:text-red-700">
                  Remove
                </button>
              )}
            </div>
          </div>
        </div>

        {/* Pipeline preview */}
        {file && dataset && (
          <div className="bg-gray-50 border border-gray-200 rounded-xl p-4 space-y-3">
            <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Pipeline preview</p>
            <div className="flex items-center gap-2 text-xs text-gray-600 flex-wrap">
              <span className="px-2 py-1 bg-orange-100 text-orange-700 rounded font-medium">Bronze</span>
              <span className="text-gray-400">→</span>
              <span className="px-2 py-1 bg-blue-100 text-blue-700 rounded font-medium">Silver staging</span>
              <span className="text-gray-400">→</span>
              <span className="px-2 py-1 bg-green-100 text-green-700 rounded font-medium">Gold</span>
              <span className="text-gray-400">→</span>
              <code className="text-blue-600">curated.{effectiveBqTable || dataset}</code>
            </div>
            <div className="text-xs text-gray-400 font-mono space-y-0.5">
              <p>gs://data-feeder-bronze/<span className="text-orange-600">{dataset}/</span>{file.name}</p>
              <p>gs://data-feeder-silver/<span className="text-blue-600">{dataset}/</span> ← schema validation &amp; type-casting</p>
              <p>gs://data-feeder-gold/<span className="text-green-600">{dataset}/</span> → BigQuery <span className="text-blue-600">curated.{effectiveBqTable || dataset}</span></p>
            </div>
          </div>
        )}

        {/* Progress */}
        {progress !== null && (
          <div>
            <div className="flex justify-between text-xs text-gray-500 mb-1">
              <span>{file && file.size > RESUMABLE_THRESHOLD ? 'Chunked upload to GCS Bronze zone…' : 'Uploading to GCS Bronze zone…'}</span>
              <span>{progress}%</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2">
              <div className="bg-brand-600 h-2 rounded-full transition-all duration-200" style={{ width: `${progress}%` }} />
            </div>
          </div>
        )}

        {uploadError && (
          <div className="flex items-start gap-2 p-3 bg-red-50 border border-red-200 rounded-lg">
            <svg className="w-4 h-4 text-red-500 flex-shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
            <p className="text-sm text-red-700">{uploadError}</p>
          </div>
        )}

        <button
          type="submit"
          disabled={!file || !dataset || isGuest || progress !== null}
          className="w-full py-3 bg-brand-600 text-white rounded-lg font-medium text-sm hover:bg-brand-700 transition disabled:opacity-40 disabled:cursor-not-allowed"
        >
          {isGuest ? 'Sign in with Google to upload' : 'Upload & trigger pipeline'}
        </button>
      </form>
    </div>
  );
}
