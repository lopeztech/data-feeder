import { useState, useEffect, useCallback, useMemo } from 'react';
import { useAuth } from '../context/AuthContext';
import { MOCK_JOBS } from '../data/mockJobs';
import { listJobs, retriggerJob, fetchPreview, deleteJob, bulkDeleteJobs } from '../lib/uploadService';
import type { StagePreview } from '../lib/uploadService';
import { PipelineJob, JobStatus } from '../types';

const STATUS_STYLES: Record<JobStatus, { bg: string; text: string; dot: string; label: string }> = {
  UPLOADING:    { bg: 'bg-blue-50',   text: 'text-blue-700',   dot: 'bg-blue-400',   label: 'Uploading' },
  VALIDATING:   { bg: 'bg-yellow-50', text: 'text-yellow-700', dot: 'bg-yellow-400', label: 'Validating' },
  TRANSFORMING: { bg: 'bg-purple-50', text: 'text-purple-700', dot: 'bg-purple-400', label: 'Transforming' },
  LOADED:       { bg: 'bg-green-50',  text: 'text-green-700',  dot: 'bg-green-500',  label: 'Loaded' },
  FAILED:       { bg: 'bg-red-50',    text: 'text-red-700',    dot: 'bg-red-500',    label: 'Failed' },
  REJECTED:     { bg: 'bg-orange-50', text: 'text-orange-700', dot: 'bg-orange-400', label: 'Rejected' },
};

const IN_PROGRESS: JobStatus[] = ['UPLOADING', 'VALIDATING', 'TRANSFORMING'];

function formatBytes(bytes: number) {
  if (bytes < 1_048_576) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1_048_576).toFixed(1)} MB`;
}

function formatDate(iso: string) {
  return new Date(iso).toLocaleString(undefined, {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
  });
}

function StatusBadge({ status }: { status: JobStatus }) {
  const s = STATUS_STYLES[status];
  return (
    <span className={`inline-flex items-center gap-1.5 px-2 py-1 rounded-full text-xs font-medium ${s.bg} ${s.text}`}>
      <span className={`w-1.5 h-1.5 rounded-full ${s.dot} ${IN_PROGRESS.includes(status) ? 'animate-pulse' : ''}`} />
      {s.label}
    </span>
  );
}

const RETRIGGERABLE: JobStatus[] = ['UPLOADING', 'VALIDATING', 'FAILED', 'REJECTED', 'TRANSFORMING'];

const PIPELINE_STAGES = [
  { key: 'UPLOADING', label: 'Upload', sublabel: 'Browser → GCS Bronze', color: 'gray' },
  { key: 'VALIDATING', label: 'Validate', sublabel: 'Schema check + PII mask', color: 'orange' },
  { key: 'TRANSFORMING', label: 'Transform', sublabel: 'Silver → BigQuery', color: 'blue' },
  { key: 'LOADED', label: 'Loaded', sublabel: 'Available in BigQuery', color: 'green' },
] as const;

const STATUS_ORDER: Record<string, number> = {
  UPLOADING: 0, VALIDATING: 1, TRANSFORMING: 2, LOADED: 3, FAILED: -1, REJECTED: -1,
};

function PipelineStepper({ job }: { job: PipelineJob }) {
  const currentIdx = STATUS_ORDER[job.status] ?? -1;
  const isFailed = job.status === 'FAILED' || job.status === 'REJECTED';

  return (
    <div>
      <div className="flex items-center justify-between mb-2">
        <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide">Pipeline progress</p>
        {IN_PROGRESS.includes(job.status) && (
          <span className="inline-flex items-center gap-1.5 text-xs text-brand-600 font-medium">
            <span className="w-1.5 h-1.5 rounded-full bg-brand-500 animate-pulse" />
            Processing
          </span>
        )}
      </div>

      {/* Stepper */}
      <div className="flex items-start gap-0">
        {PIPELINE_STAGES.map((stage, idx) => {
          const done = currentIdx > idx;
          const active = currentIdx === idx && !isFailed;
          const upcoming = currentIdx < idx;
          const failedAtStage = isFailed && currentIdx === idx;

          return (
            <div key={stage.key} className="flex-1 flex flex-col items-center relative">
              {/* Connector line */}
              {idx > 0 && (
                <div className={`absolute top-3 right-1/2 w-full h-0.5 -translate-y-1/2 transition-colors duration-500 ${
                  done ? 'bg-green-400' : active ? 'bg-brand-300' : 'bg-gray-200'
                }`} />
              )}

              {/* Circle */}
              <div className={`relative z-10 w-6 h-6 rounded-full flex items-center justify-center transition-all duration-500 ${
                done ? 'bg-green-500' :
                active ? 'bg-brand-600 ring-4 ring-brand-100' :
                failedAtStage ? 'bg-red-500 ring-4 ring-red-100' :
                'bg-gray-200'
              }`}>
                {done ? (
                  <svg className="w-3.5 h-3.5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" />
                  </svg>
                ) : active ? (
                  <div className="w-2 h-2 rounded-full bg-white animate-pulse" />
                ) : failedAtStage ? (
                  <svg className="w-3.5 h-3.5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                ) : (
                  <div className="w-2 h-2 rounded-full bg-gray-400" />
                )}
              </div>

              {/* Label */}
              <p className={`mt-1.5 text-xs font-medium text-center transition-colors duration-500 ${
                done ? 'text-green-700' :
                active ? 'text-brand-700' :
                failedAtStage ? 'text-red-700' :
                upcoming ? 'text-gray-400' : 'text-gray-500'
              }`}>{stage.label}</p>
              <p className={`text-[10px] text-center ${
                done || active ? 'text-gray-500' : 'text-gray-300'
              }`}>{stage.sublabel}</p>

              {/* Path info */}
              {idx === 0 && job.bronze_path && done && (
                <p className="text-[10px] text-orange-500 font-mono truncate max-w-[120px] mt-0.5">{job.bronze_path.split('/').pop()}</p>
              )}
              {idx === 1 && job.silver_path && done && (
                <p className="text-[10px] text-blue-500 font-mono mt-0.5">Silver</p>
              )}
              {idx === 3 && job.bq_table && done && (
                <p className="text-[10px] text-green-600 font-mono mt-0.5">{job.bq_table}</p>
              )}
            </div>
          );
        })}
      </div>

      {/* Failed/Rejected banner */}
      {isFailed && (
        <div className="mt-3 flex items-center gap-2 px-3 py-2 bg-red-50 border border-red-200 rounded-lg">
          <svg className="w-4 h-4 text-red-500 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
          </svg>
          <p className="text-xs text-red-700">
            <span className="font-medium">{job.status}</span>
            {job.error && <span> — {job.error}</span>}
          </p>
        </div>
      )}
    </div>
  );
}

type PreviewTab = 'details' | 'bronze' | 'silver' | 'curated';

function DataTable({ rows }: { rows: Record<string, unknown>[] }) {
  if (rows.length === 0) return <p className="text-xs text-gray-400 p-3">No data</p>;
  const columns = Object.keys(rows[0]);
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-xs">
        <thead>
          <tr className="bg-gray-50">
            {columns.map(col => (
              <th key={col} className="px-3 py-2 text-left font-medium text-gray-600 whitespace-nowrap border-b border-gray-200">{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
              {columns.map(col => (
                <td key={col} className="px-3 py-1.5 text-gray-700 whitespace-nowrap max-w-[200px] truncate border-b border-gray-100">
                  {String(row[col] ?? '')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function JobDetail({ job, onClose, onRetrigger, onDelete }: {
  job: PipelineJob;
  onClose: () => void;
  onRetrigger?: (jobId: string) => void;
  onDelete?: (jobId: string) => void;
}) {
  const [tab, setTab] = useState<PreviewTab>('details');
  const [preview, setPreview] = useState<StagePreview | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);

  const completionPct = job.stats.total_records > 0
    ? Math.round((job.stats.loaded / job.stats.total_records) * 100)
    : 0;
  const canRetrigger = RETRIGGERABLE.includes(job.status) && !!onRetrigger;

  useEffect(() => {
    if (tab !== 'details' && !preview && !previewLoading) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- loading flag before async fetch
      setPreviewLoading(true);
      fetchPreview(job.job_id)
        .then(setPreview)
        .catch(() => setPreview({ bronze: null, silver: null, curated: null }))
        .finally(() => setPreviewLoading(false));
    }
  }, [tab, preview, previewLoading, job.job_id]);

  const tabs: { key: PreviewTab; label: string; available: boolean }[] = [
    { key: 'details', label: 'Details', available: true },
    { key: 'bronze', label: 'Bronze', available: !!job.bronze_path },
    { key: 'silver', label: 'Silver', available: !!job.silver_path },
    { key: 'curated', label: 'Curated', available: job.status === 'LOADED' },
  ];

  return (
    <div className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-3xl max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between p-6 border-b border-gray-100">
          <div>
            <h2 className="font-semibold text-gray-900">{job.filename}</h2>
            <p className="text-xs text-gray-400 font-mono mt-0.5">{job.job_id}</p>
          </div>
          <div className="flex items-center gap-3">
            <StatusBadge status={job.status} />
            <button onClick={onClose} className="p-1.5 hover:bg-gray-100 rounded-lg transition">
              <svg className="w-4 h-4 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
        </div>

        {/* Tabs */}
        <div className="flex gap-1 px-6 pt-4 border-b border-gray-100">
          {tabs.filter(t => t.available).map(t => (
            <button
              key={t.key}
              onClick={() => setTab(t.key)}
              className={`px-3 py-2 text-xs font-medium rounded-t-lg transition ${
                tab === t.key
                  ? 'bg-white text-brand-700 border border-gray-200 border-b-white -mb-px'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>

        <div className="p-6">
          {/* Details tab */}
          {tab === 'details' && (
            <div className="space-y-6">
              {/* Pipeline progress stepper */}
              <PipelineStepper job={job} />

              {/* PII Masking */}
              {job.pii_masked && job.pii_masked.length > 0 && (
                <div className="p-3 bg-purple-50 border border-purple-200 rounded-xl">
                  <p className="text-xs font-semibold text-purple-700 mb-2">PII masked in Silver</p>
                  <div className="flex flex-wrap gap-2">
                    {job.pii_masked.map(({ column, type }) => (
                      <span key={column} className="inline-flex items-center gap-1 px-2 py-1 bg-purple-100 text-purple-700 rounded text-xs font-medium">
                        <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
                        </svg>
                        {column} ({type})
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {/* Stats */}
              {job.stats.total_records > 0 && (
                <div>
                  <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-3">Record stats</p>
                  <div className="grid grid-cols-2 gap-3">
                    {[
                      { label: 'Total', value: job.stats.total_records.toLocaleString(), color: 'text-gray-900' },
                      { label: 'Valid', value: job.stats.valid.toLocaleString(), color: 'text-green-700' },
                      { label: 'Rejected', value: job.stats.rejected.toLocaleString(), color: 'text-orange-600' },
                      { label: 'Loaded', value: job.stats.loaded.toLocaleString(), color: 'text-blue-700' },
                    ].map(({ label, value, color }) => (
                      <div key={label} className="bg-gray-50 rounded-lg p-3">
                        <p className="text-xs text-gray-400">{label}</p>
                        <p className={`text-lg font-semibold ${color} mt-0.5`}>{value}</p>
                      </div>
                    ))}
                  </div>
                  {job.status === 'LOADED' && (
                    <div className="mt-3">
                      <div className="flex justify-between text-xs text-gray-500 mb-1">
                        <span>Success rate</span>
                        <span>{completionPct}%</span>
                      </div>
                      <div className="w-full bg-gray-200 rounded-full h-1.5">
                        <div className="bg-green-500 h-1.5 rounded-full" style={{ width: `${completionPct}%` }} />
                      </div>
                    </div>
                  )}
                </div>
              )}


              {/* Actions */}
              <div className="flex gap-3">
                {canRetrigger && (
                  <button
                    onClick={() => onRetrigger(job.job_id)}
                    className="flex-1 py-2.5 bg-brand-600 text-white rounded-lg text-sm font-medium hover:bg-brand-700 transition"
                  >
                    Re-trigger validation
                  </button>
                )}
                {onDelete && (
                  <button
                    onClick={() => { if (confirm('Delete this job?')) onDelete(job.job_id); }}
                    className="px-4 py-2.5 text-red-600 border border-red-200 rounded-lg text-sm font-medium hover:bg-red-50 transition"
                  >
                    Delete
                  </button>
                )}
              </div>

              {/* Metadata */}
              <div className="text-xs text-gray-400 space-y-1 border-t border-gray-100 pt-4">
                <p>Uploaded by: <span className="text-gray-600">{job.uploaded_by}</span></p>
                <p>Started: <span className="text-gray-600">{formatDate(job.created_at)}</span></p>
                <p>Last updated: <span className="text-gray-600">{formatDate(job.updated_at)}</span></p>
                <p>File size: <span className="text-gray-600">{formatBytes(job.file_size_bytes)}</span></p>
              </div>
            </div>
          )}

          {/* Data preview tabs */}
          {tab !== 'details' && (
            <div className="border border-gray-200 rounded-xl overflow-hidden">
              <div className="px-4 py-2 bg-gray-50 border-b border-gray-200">
                <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
                  {tab === 'bronze' && 'Bronze — Raw upload (first 10 rows)'}
                  {tab === 'silver' && 'Silver — Validated data (first 10 rows)'}
                  {tab === 'curated' && 'Curated — BigQuery table (first 10 rows)'}
                </p>
              </div>
              {previewLoading && (
                <div className="flex items-center justify-center py-8">
                  <div className="w-5 h-5 border-2 border-brand-600 border-t-transparent rounded-full animate-spin" />
                </div>
              )}
              {!previewLoading && preview && (
                preview[tab] && preview[tab]!.length > 0
                  ? <DataTable rows={preview[tab]!} />
                  : <p className="text-xs text-gray-400 p-4">No data available for this stage.</p>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

type SortColumn = 'filename' | 'dataset' | 'status' | 'total_records' | 'created_at';
type SortDirection = 'asc' | 'desc' | null;

const PAGE_SIZE = 20;

export default function JobsPage() {
  const { user } = useAuth();
  const [filter, setFilter] = useState<JobStatus | 'ALL'>('ALL');
  const [datasetFilter, setDatasetFilter] = useState<string>('ALL');
  const [sortColumn, setSortColumn] = useState<SortColumn | null>(null);
  const [sortDirection, setSortDirection] = useState<SortDirection>(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [selected, setSelected] = useState<PipelineJob | null>(null);
  const [liveJobs, setLiveJobs] = useState<PipelineJob[]>([]);
  const [loading, setLoading] = useState(false);
  const [checked, setChecked] = useState<Set<string>>(new Set());
  const isGuest = user?.role === 'guest';

  const fetchJobs = useCallback(async () => {
    setLoading(true);
    try {
      setLiveJobs(await listJobs());
    } catch (err) {
      console.error('Failed to fetch jobs:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  const handleRetrigger = useCallback(async (jobId: string) => {
    try {
      await retriggerJob(jobId);
      setSelected(null);
      setTimeout(() => fetchJobs(), 1500);
    } catch (err) {
      console.error('Retrigger failed:', err);
      alert(err instanceof Error ? err.message : 'Retrigger failed');
    }
  }, [fetchJobs]);

  const handleDelete = useCallback(async (jobId: string) => {
    try {
      await deleteJob(jobId);
      setSelected(null);
      fetchJobs();
    } catch (err) {
      console.error('Delete failed:', err);
      alert(err instanceof Error ? err.message : 'Delete failed');
    }
  }, [fetchJobs]);

  const handleBulkDelete = useCallback(async () => {
    if (checked.size === 0) return;
    if (!confirm(`Delete ${checked.size} job${checked.size > 1 ? 's' : ''}?`)) return;
    try {
      await bulkDeleteJobs(Array.from(checked));
      setChecked(new Set());
      fetchJobs();
    } catch (err) {
      console.error('Bulk delete failed:', err);
      alert(err instanceof Error ? err.message : 'Bulk delete failed');
    }
  }, [checked, fetchJobs]);

  const toggleCheck = useCallback((jobId: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setChecked(prev => {
      const next = new Set(prev);
      if (next.has(jobId)) next.delete(jobId); else next.add(jobId);
      return next;
    });
  }, []);

  const toggleAll = useCallback((filteredJobs: PipelineJob[]) => {
    if (checked.size === filteredJobs.length) {
      setChecked(new Set());
    } else {
      setChecked(new Set(filteredJobs.map(j => j.job_id)));
    }
  }, [checked.size]);

  useEffect(() => {
    if (!isGuest && user) fetchJobs();
  }, [user, isGuest, fetchJobs]);

  // Auto-poll when there are active (in-progress) jobs
  const hasActiveJobs = liveJobs.some(j => IN_PROGRESS.includes(j.status));
  useEffect(() => {
    if (isGuest || !hasActiveJobs) return;
    const interval = setInterval(fetchJobs, 5000);
    return () => clearInterval(interval);
  }, [isGuest, hasActiveJobs, fetchJobs]);

  // Keep selected job in sync with live data
  useEffect(() => {
    if (selected && !isGuest) {
      const updated = liveJobs.find(j => j.job_id === selected.job_id);
      if (updated && updated.updated_at !== selected.updated_at) {
        setSelected(updated);
      }
    }
  }, [liveJobs, selected, isGuest]);

  const jobs = isGuest ? MOCK_JOBS : liveJobs;

  const datasetOptions = useMemo(() => {
    const datasets = Array.from(new Set(jobs.map(j => j.dataset))).sort();
    return datasets;
  }, [jobs]);

  // Reset page when filters or sort change
  const handleStatusFilter = useCallback((s: JobStatus | 'ALL') => {
    setFilter(s);
    setCurrentPage(1);
  }, []);

  const handleDatasetFilter = useCallback((ds: string) => {
    setDatasetFilter(ds);
    setCurrentPage(1);
  }, []);

  const handleSort = useCallback((col: SortColumn) => {
    setCurrentPage(1);
    setSortColumn(prev => {
      if (prev !== col) {
        setSortDirection('asc');
        return col;
      }
      // Cycle: asc -> desc -> null
      setSortDirection(dir => {
        if (dir === 'asc') return 'desc';
        return null;
      });
      if (sortDirection === 'desc') return null;
      return col;
    });
  }, [sortDirection]);

  // Filter -> Sort -> Paginate pipeline
  const filteredJobs = useMemo(() => {
    let result = jobs;
    if (filter !== 'ALL') result = result.filter(j => j.status === filter);
    if (datasetFilter !== 'ALL') result = result.filter(j => j.dataset === datasetFilter);
    return result;
  }, [jobs, filter, datasetFilter]);

  const sortedJobs = useMemo(() => {
    if (!sortColumn || !sortDirection) return filteredJobs;
    const sorted = [...filteredJobs];
    const dir = sortDirection === 'asc' ? 1 : -1;
    sorted.sort((a, b) => {
      let cmp = 0;
      switch (sortColumn) {
        case 'filename':
          cmp = a.filename.localeCompare(b.filename);
          break;
        case 'dataset':
          cmp = a.dataset.localeCompare(b.dataset);
          break;
        case 'status':
          cmp = a.status.localeCompare(b.status);
          break;
        case 'total_records':
          cmp = a.stats.total_records - b.stats.total_records;
          break;
        case 'created_at':
          cmp = a.created_at.localeCompare(b.created_at);
          break;
      }
      return cmp * dir;
    });
    return sorted;
  }, [filteredJobs, sortColumn, sortDirection]);

  const totalPages = Math.max(1, Math.ceil(sortedJobs.length / PAGE_SIZE));
  const safePage = Math.min(currentPage, totalPages);
  const paginatedJobs = useMemo(() => {
    const start = (safePage - 1) * PAGE_SIZE;
    return sortedJobs.slice(start, start + PAGE_SIZE);
  }, [sortedJobs, safePage]);

  const showStart = sortedJobs.length === 0 ? 0 : (safePage - 1) * PAGE_SIZE + 1;
  const showEnd = Math.min(safePage * PAGE_SIZE, sortedJobs.length);

  // Generate page numbers for pagination controls
  const pageNumbers = useMemo(() => {
    const pages: (number | 'ellipsis')[] = [];
    if (totalPages <= 7) {
      for (let i = 1; i <= totalPages; i++) pages.push(i);
    } else {
      pages.push(1);
      if (safePage > 3) pages.push('ellipsis');
      for (let i = Math.max(2, safePage - 1); i <= Math.min(totalPages - 1, safePage + 1); i++) {
        pages.push(i);
      }
      if (safePage < totalPages - 2) pages.push('ellipsis');
      pages.push(totalPages);
    }
    return pages;
  }, [totalPages, safePage]);

  // Sort header helper
  const SortHeader = ({ column, label }: { column: SortColumn; label: string }) => {
    const isActive = sortColumn === column && sortDirection !== null;
    return (
      <th
        onClick={() => handleSort(column)}
        className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide cursor-pointer select-none hover:text-gray-700 transition"
      >
        <span className="inline-flex items-center gap-1">
          {label}
          {isActive && sortDirection === 'asc' && (
            <svg className="w-3 h-3 text-brand-600" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 15l7-7 7 7" /></svg>
          )}
          {isActive && sortDirection === 'desc' && (
            <svg className="w-3 h-3 text-brand-600" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" /></svg>
          )}
          {!isActive && (
            <svg className="w-3 h-3 text-gray-300" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16V4m0 0L3 8m4-4l4 4m6 0v12m0 0l4-4m-4 4l-4-4" /></svg>
          )}
        </span>
      </th>
    );
  };

  return (
    <div className="p-4 sm:p-8">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-xl sm:text-2xl font-bold text-gray-900">Pipeline Jobs</h1>
          <p className="text-gray-500 mt-1 text-sm">
            {user?.role === 'guest' ? 'Viewing demo data — sign in to see live jobs.' : 'Real-time pipeline status.'}
          </p>
        </div>
        <div className="flex items-center gap-2">
          {!isGuest && (
            <button
              onClick={fetchJobs}
              disabled={loading}
              className="p-1.5 hover:bg-gray-100 rounded-lg transition disabled:opacity-40"
              title="Refresh"
            >
              <svg className={`w-4 h-4 text-gray-500 ${loading ? 'animate-spin' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
              </svg>
            </button>
          )}
          {isGuest && (
            <span className="text-xs px-3 py-1.5 bg-amber-100 text-amber-700 rounded-full font-medium">
              Demo data
            </span>
          )}
        </div>
      </div>

      {/* Status filter + dataset filter */}
      <div className="flex gap-2 mb-6 flex-wrap items-center">
        {(['ALL', 'LOADED', 'TRANSFORMING', 'VALIDATING', 'FAILED', 'REJECTED'] as const).map((s) => (
          <button
            key={s}
            onClick={() => handleStatusFilter(s)}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
              filter === s
                ? 'bg-brand-600 text-white'
                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
            }`}
          >
            {s === 'ALL' ? 'All jobs' : STATUS_STYLES[s].label}
          </button>
        ))}
        <select
          value={datasetFilter}
          onChange={(e) => handleDatasetFilter(e.target.value)}
          className="px-3 py-1.5 rounded-lg text-xs font-medium bg-gray-100 text-gray-600 border-0 focus:ring-2 focus:ring-brand-500 cursor-pointer"
        >
          <option value="ALL">All datasets</option>
          {datasetOptions.map((ds) => (
            <option key={ds} value={ds}>{ds}</option>
          ))}
        </select>
      </div>

      {/* Bulk action bar */}
      {!isGuest && checked.size > 0 && (
        <div className="flex items-center gap-3 mb-4 px-4 py-2.5 bg-brand-50 border border-brand-200 rounded-lg">
          <p className="text-sm font-medium text-brand-700 flex-1">
            {checked.size} job{checked.size > 1 ? 's' : ''} selected
          </p>
          <button
            onClick={() => setChecked(new Set())}
            className="text-xs text-gray-500 hover:text-gray-700 transition"
          >
            Clear
          </button>
          <button
            onClick={handleBulkDelete}
            className="px-3 py-1.5 bg-red-600 text-white rounded-lg text-xs font-medium hover:bg-red-700 transition"
          >
            Delete selected
          </button>
        </div>
      )}

      {/* Mobile card list */}
      <div className="md:hidden space-y-3">
        {paginatedJobs.map((job) => (
          <div
            key={job.job_id}
            onClick={() => setSelected(job)}
            className={`bg-white border rounded-xl p-4 active:bg-gray-50 cursor-pointer transition ${checked.has(job.job_id) ? 'border-brand-400 ring-1 ring-brand-200' : 'border-gray-200'}`}
          >
            <div className="flex items-start justify-between gap-3 mb-2">
              {!isGuest && (
                <input
                  type="checkbox"
                  checked={checked.has(job.job_id)}
                  onChange={() => {}}
                  onClick={(e) => toggleCheck(job.job_id, e)}
                  className="mt-1 rounded border-gray-300 text-brand-600 focus:ring-brand-500 flex-shrink-0"
                />
              )}
              <div className="min-w-0 flex-1">
                <p className="font-medium text-gray-900 truncate">{job.filename}</p>
                <p className="text-xs text-gray-400">{job.dataset} · {formatBytes(job.file_size_bytes)}</p>
              </div>
              <StatusBadge status={job.status} />
            </div>
            <div className="flex items-center justify-between text-xs text-gray-400">
              <span>{job.stats.total_records > 0 ? `${job.stats.total_records.toLocaleString()} records` : 'No records'}</span>
              <span>{formatDate(job.created_at)}</span>
            </div>
          </div>
        ))}
        {paginatedJobs.length === 0 && (
          <div className="bg-white border border-gray-200 rounded-xl px-4 py-12 text-center text-gray-400 text-sm">
            No jobs match the selected filters.
          </div>
        )}
      </div>

      {/* Desktop table */}
      <div className="hidden md:block bg-white border border-gray-200 rounded-xl overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-100 bg-gray-50">
              {!isGuest && (
                <th className="px-4 py-3 w-10">
                  <input
                    type="checkbox"
                    checked={paginatedJobs.length > 0 && checked.size === paginatedJobs.length}
                    onChange={() => toggleAll(paginatedJobs)}
                    className="rounded border-gray-300 text-brand-600 focus:ring-brand-500"
                  />
                </th>
              )}
              <SortHeader column="filename" label="File" />
              <SortHeader column="dataset" label="Dataset" />
              <SortHeader column="status" label="Status" />
              <SortHeader column="total_records" label="Records" />
              <SortHeader column="created_at" label="Started" />
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50">
            {paginatedJobs.map((job) => (
              <tr
                key={job.job_id}
                onClick={() => setSelected(job)}
                className={`hover:bg-gray-50 cursor-pointer transition ${checked.has(job.job_id) ? 'bg-brand-50' : ''}`}
              >
                {!isGuest && (
                  <td className="px-4 py-3">
                    <input
                      type="checkbox"
                      checked={checked.has(job.job_id)}
                      onChange={() => {}}
                      onClick={(e) => toggleCheck(job.job_id, e)}
                      className="rounded border-gray-300 text-brand-600 focus:ring-brand-500"
                    />
                  </td>
                )}
                <td className="px-4 py-3">
                  <p className="font-medium text-gray-900 truncate max-w-[200px]">{job.filename}</p>
                  <p className="text-xs text-gray-400">{formatBytes(job.file_size_bytes)}</p>
                </td>
                <td className="px-4 py-3 text-gray-600">{job.dataset}</td>
                <td className="px-4 py-3">
                  <StatusBadge status={job.status} />
                </td>
                <td className="px-4 py-3 text-gray-600">
                  {job.stats.total_records > 0 ? job.stats.total_records.toLocaleString() : '—'}
                </td>
                <td className="px-4 py-3 text-gray-400 text-xs">{formatDate(job.created_at)}</td>
              </tr>
            ))}
            {paginatedJobs.length === 0 && (
              <tr>
                <td colSpan={!isGuest ? 6 : 5} className="px-4 py-12 text-center text-gray-400 text-sm">
                  No jobs match the selected filters.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {sortedJobs.length > PAGE_SIZE && (
        <div className="flex items-center justify-between mt-4 px-1">
          <p className="text-xs text-gray-500">
            Showing {showStart}–{showEnd} of {sortedJobs.length} jobs
          </p>
          <div className="flex items-center gap-1">
            <button
              onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
              disabled={safePage <= 1}
              className="px-2.5 py-1.5 rounded-lg text-xs font-medium bg-gray-100 text-gray-600 hover:bg-gray-200 transition disabled:opacity-40 disabled:cursor-not-allowed"
            >
              Previous
            </button>
            {pageNumbers.map((p, i) =>
              p === 'ellipsis' ? (
                <span key={`ellipsis-${i}`} className="px-1.5 text-xs text-gray-400">...</span>
              ) : (
                <button
                  key={p}
                  onClick={() => setCurrentPage(p)}
                  className={`px-2.5 py-1.5 rounded-lg text-xs font-medium transition ${
                    safePage === p
                      ? 'bg-brand-600 text-white'
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  {p}
                </button>
              )
            )}
            <button
              onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
              disabled={safePage >= totalPages}
              className="px-2.5 py-1.5 rounded-lg text-xs font-medium bg-gray-100 text-gray-600 hover:bg-gray-200 transition disabled:opacity-40 disabled:cursor-not-allowed"
            >
              Next
            </button>
          </div>
        </div>
      )}

      {selected && <JobDetail job={selected} onClose={() => setSelected(null)} onRetrigger={isGuest ? undefined : handleRetrigger} onDelete={isGuest ? undefined : handleDelete} />}
    </div>
  );
}
