import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { fetchModels, listJobs } from '../lib/uploadService';
import type { ModelInfo } from '../lib/uploadService';
import type { PipelineJob } from '../types';
import { MOCK_MODELS, MOCK_LINEAGE_JOBS } from '../data/mockClusters';

interface ModelCard extends ModelInfo {
  totalUploads: number;
  totalRows: number;
  totalSize: number;
  latestDate: string;
}

function enrichModels(models: ModelInfo[], jobs: PipelineJob[]): ModelCard[] {
  const loadedJobs = jobs.filter(j => j.status === 'LOADED');
  return models.map(m => {
    const relatedJobs = loadedJobs.filter(j => m.sourceTables.includes(j.dataset));
    return {
      ...m,
      totalUploads: relatedJobs.length,
      totalRows: relatedJobs.reduce((s, j) => s + (j.stats?.loaded ?? 0), 0),
      totalSize: relatedJobs.reduce((s, j) => s + j.file_size_bytes, 0),
      latestDate: relatedJobs.reduce((latest, j) => j.updated_at > latest ? j.updated_at : latest, ''),
    };
  });
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatDate(iso: string): string {
  if (!iso) return '-';
  return new Date(iso).toLocaleDateString('en-AU', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' });
}

export default function InsightsListPage() {
  const { user } = useAuth();
  const navigate = useNavigate();
  const isGuest = user?.role === 'guest';
  const [cards, setCards] = useState<ModelCard[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (isGuest) {
      setCards(enrichModels(MOCK_MODELS, MOCK_LINEAGE_JOBS));
      return;
    }
    setLoading(true);
    Promise.all([
      fetchModels(),
      listJobs().catch(() => [] as PipelineJob[]),
    ])
      .then(([models, jobs]) => setCards(enrichModels(models, jobs)))
      .catch(err => setError(err.message))
      .finally(() => setLoading(false));
  }, [isGuest]);

  return (
    <div className="p-4 sm:p-8 max-w-6xl mx-auto">
      <div className="mb-8">
        <h1 className="text-xl sm:text-2xl font-bold text-gray-900">AI Insights</h1>
        <p className="text-gray-500 mt-1 text-sm">
          Select a model to view its data lineage and clustering insights.
        </p>
      </div>

      {isGuest && (
        <div className="p-4 mb-6 bg-amber-50 border border-amber-200 rounded-xl">
          <p className="text-sm font-medium text-amber-800">You're viewing demo data. Sign in with Google to see live models from your pipeline.</p>
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

      {!loading && !error && cards.length === 0 && (
        <div className="text-center py-20">
          <svg className="w-12 h-12 text-gray-300 mx-auto mb-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
          </svg>
          <p className="text-sm text-gray-500">No ML models found.</p>
          <p className="text-xs text-gray-400 mt-1">Upload data and run the ML pipeline to generate cluster models.</p>
        </div>
      )}

      {!loading && !error && cards.length > 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {cards.map(c => (
            <button
              key={c.model}
              onClick={() => navigate(`/insights/${encodeURIComponent(c.model)}`)}
              className="text-left bg-white border border-gray-200 rounded-xl p-5 hover:border-brand-300 hover:shadow-md transition-all group"
            >
              <div className="flex items-center gap-3 mb-3">
                <div className="w-9 h-9 rounded-lg bg-brand-50 flex items-center justify-center group-hover:bg-brand-100 transition-colors">
                  <svg className="w-4.5 h-4.5 text-brand-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                  </svg>
                </div>
                <div className="flex-1 min-w-0">
                  <h3 className="font-semibold text-gray-900 text-sm truncate">{c.model}</h3>
                  <p className="text-xs text-gray-400">{c.clustersTable}</p>
                </div>
                <svg className="w-4 h-4 text-gray-300 group-hover:text-brand-500 transition-colors" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
              </div>

              {/* Source datasets */}
              <div className="mb-3 space-y-1">
                {c.sourceTables.map(st => (
                  <div key={st} className="flex items-center gap-1.5 text-xs text-gray-500">
                    <svg className="w-3 h-3 text-teal-500 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 10h18M3 14h18M3 18h18M3 6h18" />
                    </svg>
                    <span className="truncate">{st}</span>
                  </div>
                ))}
              </div>

              <div className="grid grid-cols-3 gap-3">
                <div>
                  <p className="text-xs text-gray-400">Uploads</p>
                  <p className="text-sm font-semibold text-gray-900">{c.totalUploads}</p>
                </div>
                <div>
                  <p className="text-xs text-gray-400">Rows</p>
                  <p className="text-sm font-semibold text-gray-900">{c.totalRows.toLocaleString()}</p>
                </div>
                <div>
                  <p className="text-xs text-gray-400">Size</p>
                  <p className="text-sm font-semibold text-gray-900">{formatBytes(c.totalSize)}</p>
                </div>
              </div>

              <p className="text-xs text-gray-400 mt-3">Last updated {formatDate(c.latestDate)}</p>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
