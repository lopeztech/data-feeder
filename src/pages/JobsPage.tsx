import { useState } from 'react';
import { useAuth } from '../context/AuthContext';
import { MOCK_JOBS } from '../data/mockJobs';
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

function JobDetail({ job, onClose }: { job: PipelineJob; onClose: () => void }) {
  const completionPct = job.stats.total_records > 0
    ? Math.round((job.stats.loaded / job.stats.total_records) * 100)
    : 0;

  return (
    <div className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-4">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-2xl max-h-[90vh] overflow-y-auto">
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

        <div className="p-6 space-y-6">
          {/* Pipeline stages */}
          <div>
            <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-3">Pipeline stages</p>
            <div className="space-y-3">
              {[
                { label: 'Bronze (Raw)', path: job.bronze_path, done: !!job.bronze_path, color: 'orange' },
                { label: 'Silver (Validated)', path: job.silver_path, done: !!job.silver_path, color: 'blue' },
                { label: 'Gold (Curated)', path: job.bq_table ? `BigQuery → ${job.bq_table}` : null, done: !!job.bq_table, color: 'green' },
              ].map(({ label, path, done, color }) => (
                <div key={label} className="flex items-start gap-3">
                  <div className={`mt-0.5 w-5 h-5 rounded-full flex-shrink-0 flex items-center justify-center ${
                    done ? `bg-${color}-100` : 'bg-gray-100'
                  }`}>
                    {done ? (
                      <svg className={`w-3 h-3 text-${color}-600`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" />
                      </svg>
                    ) : (
                      <div className="w-2 h-2 rounded-full bg-gray-300" />
                    )}
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-gray-700">{label}</p>
                    {path && (
                      <p className="text-xs text-gray-400 font-mono truncate mt-0.5">{path}</p>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>

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

          {/* Error */}
          {job.error && (
            <div className="p-4 bg-red-50 border border-red-200 rounded-xl">
              <p className="text-xs font-semibold text-red-700 mb-1">Error</p>
              <p className="text-sm text-red-600">{job.error}</p>
            </div>
          )}

          {/* Metadata */}
          <div className="text-xs text-gray-400 space-y-1 border-t border-gray-100 pt-4">
            <p>Uploaded by: <span className="text-gray-600">{job.uploaded_by}</span></p>
            <p>Started: <span className="text-gray-600">{formatDate(job.created_at)}</span></p>
            <p>Last updated: <span className="text-gray-600">{formatDate(job.updated_at)}</span></p>
            <p>File size: <span className="text-gray-600">{formatBytes(job.file_size_bytes)}</span></p>
          </div>
        </div>
      </div>
    </div>
  );
}

export default function JobsPage() {
  const { user } = useAuth();
  const [filter, setFilter] = useState<JobStatus | 'ALL'>('ALL');
  const [selected, setSelected] = useState<PipelineJob | null>(null);

  // Guests always see mock data; authenticated users would call the real API
  const jobs = MOCK_JOBS;

  const filtered = filter === 'ALL' ? jobs : jobs.filter((j) => j.status === filter);

  return (
    <div className="p-8">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Pipeline Jobs</h1>
          <p className="text-gray-500 mt-1 text-sm">
            {user?.role === 'guest' ? 'Viewing demo data — sign in to see live jobs.' : 'Real-time pipeline status.'}
          </p>
        </div>
        {user?.role === 'guest' && (
          <span className="text-xs px-3 py-1.5 bg-amber-100 text-amber-700 rounded-full font-medium">
            Demo data
          </span>
        )}
      </div>

      {/* Status filter */}
      <div className="flex gap-2 mb-6 flex-wrap">
        {(['ALL', 'LOADED', 'TRANSFORMING', 'VALIDATING', 'FAILED', 'REJECTED'] as const).map((s) => (
          <button
            key={s}
            onClick={() => setFilter(s)}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
              filter === s
                ? 'bg-brand-600 text-white'
                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
            }`}
          >
            {s === 'ALL' ? 'All jobs' : STATUS_STYLES[s].label}
          </button>
        ))}
      </div>

      {/* Jobs table */}
      <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-100 bg-gray-50">
              <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">File</th>
              <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Dataset</th>
              <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Status</th>
              <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Records</th>
              <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500 uppercase tracking-wide">Started</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50">
            {filtered.map((job) => (
              <tr
                key={job.job_id}
                onClick={() => setSelected(job)}
                className="hover:bg-gray-50 cursor-pointer transition"
              >
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
            {filtered.length === 0 && (
              <tr>
                <td colSpan={5} className="px-4 py-12 text-center text-gray-400 text-sm">
                  No jobs match the selected filter.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {selected && <JobDetail job={selected} onClose={() => setSelected(null)} />}
    </div>
  );
}
