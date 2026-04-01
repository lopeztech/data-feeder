import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { listJobs } from '../lib/uploadService';
import type { PipelineJob } from '../types';
import { MOCK_LINEAGE_JOBS } from '../data/mockClusters';

interface DatasetGroup {
  dataset: string;
  jobs: PipelineJob[];
  totalLoaded: number;
  totalSize: number;
  fileCount: number;
  latestDate: string;
}

function groupJobsByDataset(jobs: PipelineJob[]): DatasetGroup[] {
  const loaded = jobs.filter(j => j.status === 'LOADED' && j.bq_table);
  const map = new Map<string, PipelineJob[]>();
  for (const j of loaded) {
    const key = j.dataset;
    const arr = map.get(key) ?? [];
    arr.push(j);
    map.set(key, arr);
  }
  return Array.from(map.entries())
    .map(([dataset, djobs]) => ({
      dataset,
      jobs: djobs,
      totalLoaded: djobs.reduce((s, j) => s + (j.stats?.loaded ?? 0), 0),
      totalSize: djobs.reduce((s, j) => s + j.file_size_bytes, 0),
      fileCount: djobs.length,
      latestDate: djobs.reduce((latest, j) => j.updated_at > latest ? j.updated_at : latest, ''),
    }))
    .sort((a, b) => b.latestDate.localeCompare(a.latestDate));
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString('en-AU', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' });
}

export default function InsightsListPage() {
  const { user } = useAuth();
  const navigate = useNavigate();
  const isGuest = user?.role === 'guest';
  const [groups, setGroups] = useState<DatasetGroup[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (isGuest) {
      setGroups(groupJobsByDataset(MOCK_LINEAGE_JOBS));
      return;
    }
    setLoading(true);
    listJobs()
      .then(jobs => setGroups(groupJobsByDataset(jobs)))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false));
  }, [isGuest]);

  return (
    <div className="p-4 sm:p-8 max-w-6xl mx-auto">
      <div className="mb-8">
        <h1 className="text-xl sm:text-2xl font-bold text-gray-900">AI Insights</h1>
        <p className="text-gray-500 mt-1 text-sm">
          Select a dataset to view its data lineage and ML clustering insights.
        </p>
      </div>

      {isGuest && (
        <div className="p-4 mb-6 bg-amber-50 border border-amber-200 rounded-xl">
          <p className="text-sm font-medium text-amber-800">You're viewing demo data. Sign in with Google to see live datasets from your pipeline.</p>
        </div>
      )}

      {loading && (
        <div className="flex items-center justify-center py-20">
          <div className="w-8 h-8 border-4 border-brand-600 border-t-transparent rounded-full animate-spin" />
        </div>
      )}

      {error && (
        <div className="p-4 bg-red-50 border border-red-200 rounded-xl">
          <p className="text-sm text-red-700">{error}</p>
        </div>
      )}

      {!loading && !error && groups.length === 0 && (
        <div className="text-center py-20">
          <svg className="w-12 h-12 text-gray-300 mx-auto mb-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
          </svg>
          <p className="text-sm text-gray-500">No loaded datasets yet.</p>
          <p className="text-xs text-gray-400 mt-1">Upload data and run the pipeline to see insights here.</p>
        </div>
      )}

      {!loading && !error && groups.length > 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {groups.map(g => (
            <button
              key={g.dataset}
              onClick={() => navigate(`/insights/${encodeURIComponent(g.dataset)}`)}
              className="text-left bg-white border border-gray-200 rounded-xl p-5 hover:border-brand-300 hover:shadow-md transition-all group"
            >
              <div className="flex items-center gap-3 mb-3">
                <div className="w-9 h-9 rounded-lg bg-brand-50 flex items-center justify-center group-hover:bg-brand-100 transition-colors">
                  <svg className="w-4.5 h-4.5 text-brand-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2 1 3 3 3h10c2 0 3-1 3-3V7c0-2-1-3-3-3H7C5 4 4 5 4 7zM9 12h6M12 9v6" />
                  </svg>
                </div>
                <div className="flex-1 min-w-0">
                  <h3 className="font-semibold text-gray-900 text-sm truncate">{g.dataset}</h3>
                  <p className="text-xs text-gray-400">BigQuery table</p>
                </div>
                <svg className="w-4 h-4 text-gray-300 group-hover:text-brand-500 transition-colors" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
              </div>

              <div className="grid grid-cols-3 gap-3">
                <div>
                  <p className="text-xs text-gray-400">Uploads</p>
                  <p className="text-sm font-semibold text-gray-900">{g.fileCount}</p>
                </div>
                <div>
                  <p className="text-xs text-gray-400">Rows</p>
                  <p className="text-sm font-semibold text-gray-900">{g.totalLoaded.toLocaleString()}</p>
                </div>
                <div>
                  <p className="text-xs text-gray-400">Size</p>
                  <p className="text-sm font-semibold text-gray-900">{formatBytes(g.totalSize)}</p>
                </div>
              </div>

              <p className="text-xs text-gray-400 mt-3">Last updated {formatDate(g.latestDate)}</p>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
