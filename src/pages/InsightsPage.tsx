import { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { fetchClusters, listJobs } from '../lib/uploadService';
import type { ClusterSummary, ClusterRecord } from '../lib/uploadService';
import type { PipelineJob } from '../types';
import { MOCK_CLUSTERS, MOCK_CLUSTER_RECORDS, MOCK_LINEAGE_JOBS } from '../data/mockClusters';

interface TableGroup {
  table: string;
  jobs: PipelineJob[];
  totalLoaded: number;
  totalSize: number;
  latestDate: string;
}

function groupByTable(jobs: PipelineJob[]): TableGroup[] {
  const map = new Map<string, PipelineJob[]>();
  for (const j of jobs) {
    const key = j.bq_table ?? j.dataset;
    const arr = map.get(key) ?? [];
    arr.push(j);
    map.set(key, arr);
  }
  return Array.from(map.entries()).map(([table, tableJobs]) => ({
    table,
    jobs: tableJobs,
    totalLoaded: tableJobs.reduce((s, j) => s + (j.stats?.loaded ?? 0), 0),
    totalSize: tableJobs.reduce((s, j) => s + j.file_size_bytes, 0),
    latestDate: tableJobs.reduce((latest, j) => j.updated_at > latest ? j.updated_at : latest, ''),
  }));
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString('en-AU', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' });
}

const CLUSTER_COLORS = [
  { bg: 'bg-blue-50', border: 'border-blue-200', text: 'text-blue-700', badge: 'bg-blue-100 text-blue-700', bar: 'bg-blue-500' },
  { bg: 'bg-green-50', border: 'border-green-200', text: 'text-green-700', badge: 'bg-green-100 text-green-700', bar: 'bg-green-500' },
  { bg: 'bg-purple-50', border: 'border-purple-200', text: 'text-purple-700', badge: 'bg-purple-100 text-purple-700', bar: 'bg-purple-500' },
  { bg: 'bg-orange-50', border: 'border-orange-200', text: 'text-orange-700', badge: 'bg-orange-100 text-orange-700', bar: 'bg-orange-500' },
  { bg: 'bg-pink-50', border: 'border-pink-200', text: 'text-pink-700', badge: 'bg-pink-100 text-pink-700', bar: 'bg-pink-500' },
  { bg: 'bg-cyan-50', border: 'border-cyan-200', text: 'text-cyan-700', badge: 'bg-cyan-100 text-cyan-700', bar: 'bg-cyan-500' },
  { bg: 'bg-amber-50', border: 'border-amber-200', text: 'text-amber-700', badge: 'bg-amber-100 text-amber-700', bar: 'bg-amber-500' },
  { bg: 'bg-red-50', border: 'border-red-200', text: 'text-red-700', badge: 'bg-red-100 text-red-700', bar: 'bg-red-500' },
];

function clusterColor(id: number) {
  return CLUSTER_COLORS[id % CLUSTER_COLORS.length];
}

/** Pick the top N numeric metric keys from clusters for display (skip near-zero averages). */
function pickDisplayMetrics(clusters: ClusterSummary[], maxCount = 4): string[] {
  if (clusters.length === 0) return [];
  const allKeys = new Set<string>();
  for (const c of clusters) {
    for (const k of Object.keys(c.metrics)) allKeys.add(k);
  }
  // Rank by max value across clusters (higher variance = more interesting)
  return Array.from(allKeys)
    .map(k => ({ key: k, max: Math.max(...clusters.map(c => Math.abs(c.metrics[k] ?? 0))) }))
    .filter(e => e.max > 0.01)
    .sort((a, b) => b.max - a.max)
    .slice(0, maxCount)
    .map(e => e.key);
}

function metricLabel(key: string): string {
  return key
    .replace(/^avg_/, '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase());
}

function StatBar({ value, max, color }: { value: number; max: number; color: string }) {
  const pct = max > 0 ? Math.min(100, (value / max) * 100) : 0;
  return (
    <div className="w-full bg-gray-100 rounded-full h-1.5">
      <div className={`h-1.5 rounded-full ${color} transition-all duration-500`} style={{ width: `${pct}%` }} />
    </div>
  );
}

/** Pick string fields from records (for the table columns). */
function pickRecordFields(records: ClusterRecord[], maxCount = 3): string[] {
  if (records.length === 0) return [];
  const allKeys = new Set<string>();
  for (const r of records) {
    for (const [k, v] of Object.entries(r.fields)) {
      if (typeof v === 'string') allKeys.add(k);
    }
  }
  return Array.from(allKeys).slice(0, maxCount);
}

/** Pick numeric fields from records for the table. */
function pickRecordNumericFields(records: ClusterRecord[], maxCount = 4): string[] {
  if (records.length === 0) return [];
  const allKeys = new Set<string>();
  for (const r of records) {
    for (const [k, v] of Object.entries(r.fields)) {
      if (typeof v === 'number') allKeys.add(k);
    }
  }
  return Array.from(allKeys).slice(0, maxCount);
}

export default function InsightsPage() {
  const { dataset } = useParams<{ dataset: string }>();
  const { user } = useAuth();
  const isGuest = user?.role === 'guest';
  const [clusters, setClusters] = useState<ClusterSummary[]>([]);
  const [records, setRecords] = useState<ClusterRecord[]>([]);
  const [lineageJobs, setLineageJobs] = useState<PipelineJob[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedCluster, setExpandedCluster] = useState<number | null>(null);
  const [lineageOpen, setLineageOpen] = useState(true);

  useEffect(() => {
    if (!dataset) return;

    if (isGuest) {
      setClusters(MOCK_CLUSTERS);
      setRecords(MOCK_CLUSTER_RECORDS);
      setLineageJobs(MOCK_LINEAGE_JOBS.filter(j => j.dataset === dataset));
      setLoading(false);
      return;
    }

    setLoading(true);
    Promise.all([
      fetchClusters(dataset),
      listJobs().catch(() => [] as PipelineJob[]),
    ])
      .then(([data, allJobs]) => {
        setClusters(data.clusters);
        setRecords(data.records);
        setLineageJobs(allJobs.filter(j => j.status === 'LOADED' && j.dataset === dataset));
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false));
  }, [dataset, isGuest]);

  const totalRecords = clusters.reduce((sum, c) => sum + c.record_count, 0);
  const displayMetrics = pickDisplayMetrics(clusters);
  const metricMaxes = Object.fromEntries(
    displayMetrics.map(k => [k, Math.max(...clusters.map(c => Math.abs(c.metrics[k] ?? 0)), 1)])
  );

  // Top scoring records across all clusters
  const topRecords = [...records]
    .sort((a, b) => b.score - a.score)
    .slice(0, 10);

  const stringFields = pickRecordFields(records);
  const numericFields = pickRecordNumericFields(records);

  return (
    <div className="p-4 sm:p-8 max-w-6xl mx-auto">
      {/* Back link + header */}
      <div className="mb-8">
        <Link to="/insights" className="inline-flex items-center gap-1 text-sm text-gray-500 hover:text-brand-600 transition mb-3">
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
          </svg>
          All Datasets
        </Link>
        <h1 className="text-xl sm:text-2xl font-bold text-gray-900">{dataset}</h1>
        <p className="text-gray-500 mt-1 text-sm">
          K-Means clustering analysis and data lineage
        </p>
      </div>

      {isGuest && (
        <div className="p-4 mb-6 bg-amber-50 border border-amber-200 rounded-xl">
          <p className="text-sm font-medium text-amber-800">You're viewing demo data. Sign in with Google to see live ML insights from your pipeline.</p>
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
          <p className="text-xs text-red-500 mt-1">The ML pipeline may not have run yet. Upload data and trigger the pipeline first.</p>
        </div>
      )}

      {/* Data Lineage */}
      {!loading && !error && lineageJobs.length > 0 && (() => {
        const groups = groupByTable(lineageJobs);
        const totalUploads = lineageJobs.length;
        return (
          <div className="mb-6 bg-white border border-gray-200 rounded-xl overflow-hidden">
            <button
              onClick={() => setLineageOpen(!lineageOpen)}
              className="w-full flex items-center justify-between px-4 py-3 hover:bg-gray-50 transition-colors"
            >
              <div className="flex items-center gap-2">
                <svg className="w-4 h-4 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2 1 3 3 3h10c2 0 3-1 3-3V7c0-2-1-3-3-3H7C5 4 4 5 4 7zM9 12h6M12 9v6" />
                </svg>
                <span className="text-sm font-semibold text-gray-700">Data Lineage</span>
                <span className="text-xs text-gray-400">{groups.length} {groups.length === 1 ? 'table' : 'tables'} · {totalUploads} uploads</span>
              </div>
              <svg className={`w-4 h-4 text-gray-400 transition-transform ${lineageOpen ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
              </svg>
            </button>

            {lineageOpen && (
              <div className="px-4 pb-4">
                <div className="grid grid-cols-1 md:grid-cols-[1fr_40px_1fr] gap-4 items-center">
                  {/* Source uploads */}
                  <div className="space-y-3">
                    {groups.map(g => (
                      <div key={g.table} className="border border-gray-100 rounded-lg p-3 border-l-4 border-l-teal-500">
                        <div className="flex items-center gap-2 mb-1">
                          <svg className="w-3.5 h-3.5 text-teal-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 10h18M3 14h18M3 18h18M3 6h18" />
                          </svg>
                          <span className="text-xs font-bold text-gray-700">{g.table}</span>
                        </div>
                        <div className="flex flex-wrap gap-x-4 gap-y-0.5 text-xs text-gray-500">
                          <span>{g.jobs.length} {g.jobs.length === 1 ? 'upload' : 'uploads'}</span>
                          <span>{g.totalLoaded.toLocaleString()} rows</span>
                          <span>{formatBytes(g.totalSize)}</span>
                        </div>
                        <p className="text-xs text-gray-400 mt-1">Last: {formatDate(g.latestDate)}</p>
                      </div>
                    ))}
                  </div>

                  {/* Connector arrow */}
                  <div className="hidden md:flex flex-col items-center justify-center gap-1">
                    <div className="w-px flex-1 border-l-2 border-dashed border-gray-200" />
                    <svg className="w-5 h-5 text-gray-300" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
                    </svg>
                    <div className="w-px flex-1 border-l-2 border-dashed border-gray-200" />
                  </div>

                  {/* ML output */}
                  <div className="border border-gray-100 rounded-lg p-3 border-l-4 border-l-brand-500">
                    <div className="flex items-center gap-2 mb-1">
                      <svg className="w-3.5 h-3.5 text-brand-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                      </svg>
                      <span className="text-xs font-bold text-gray-700">K-Means Clustering</span>
                    </div>
                    <div className="flex flex-wrap gap-x-4 gap-y-0.5 text-xs text-gray-500">
                      <span>{clusters.length} clusters</span>
                      <span>{totalRecords.toLocaleString()} records</span>
                    </div>
                    <p className="text-xs text-gray-400 mt-1">Vertex AI</p>
                  </div>
                </div>
              </div>
            )}
          </div>
        );
      })()}

      {!loading && !error && clusters.length > 0 && (
        <div className="space-y-8">
          {/* Overview cards */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
            <div className="bg-white border border-gray-200 rounded-xl p-4">
              <p className="text-xs text-gray-400">Clusters</p>
              <p className="text-2xl font-bold text-gray-900">{clusters.length}</p>
            </div>
            <div className="bg-white border border-gray-200 rounded-xl p-4">
              <p className="text-xs text-gray-400">Total Records</p>
              <p className="text-2xl font-bold text-gray-900">{totalRecords.toLocaleString()}</p>
            </div>
            <div className="bg-white border border-gray-200 rounded-xl p-4">
              <p className="text-xs text-gray-400">Uploads</p>
              <p className="text-2xl font-bold text-gray-900">{lineageJobs.length}</p>
            </div>
            <div className="bg-white border border-gray-200 rounded-xl p-4">
              <p className="text-xs text-gray-400">Top Scored</p>
              <p className="text-2xl font-bold text-brand-700">{topRecords.length}</p>
            </div>
          </div>

          {/* Cluster cards */}
          <div>
            <h2 className="text-lg font-semibold text-gray-900 mb-4">Clusters</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {clusters.map(c => {
                const color = clusterColor(c.cluster_id);
                const clusterRecords = records.filter(r => r.cluster_id === c.cluster_id);
                const isExpanded = expandedCluster === c.cluster_id;

                return (
                  <div key={c.cluster_id} className={`border rounded-xl overflow-hidden ${color.border} ${color.bg}`}>
                    <button
                      onClick={() => setExpandedCluster(isExpanded ? null : c.cluster_id)}
                      className="w-full p-4 text-left"
                    >
                      <div className="flex items-center justify-between mb-3">
                        <div className="flex items-center gap-2">
                          <span className={`px-2 py-0.5 rounded-full text-xs font-bold ${color.badge}`}>
                            C{c.cluster_id}
                          </span>
                          <h3 className={`font-semibold ${color.text}`}>
                            {c.label ?? `Cluster ${c.cluster_id}`}
                          </h3>
                        </div>
                        <span className="text-xs text-gray-500">{c.record_count} records</span>
                      </div>

                      <div className="space-y-2">
                        {displayMetrics.map(key => (
                          <div key={key} className="flex items-center gap-2">
                            <span className="text-xs text-gray-500 w-24 truncate">{metricLabel(key)}</span>
                            <StatBar value={c.metrics[key] ?? 0} max={metricMaxes[key]} color={color.bar} />
                            <span className="text-xs font-medium text-gray-700 w-10 text-right">
                              {(c.metrics[key] ?? 0) % 1 === 0
                                ? c.metrics[key]
                                : (c.metrics[key] ?? 0).toFixed(1)}
                            </span>
                          </div>
                        ))}
                      </div>
                    </button>

                    {/* Expanded: top records */}
                    {isExpanded && clusterRecords.length > 0 && (
                      <div className="border-t border-gray-200/50 bg-white/50">
                        <div className="px-4 py-2 bg-gray-50/50">
                          <p className="text-xs font-semibold text-gray-500 uppercase">Top Records</p>
                        </div>
                        <div className="divide-y divide-gray-100">
                          {clusterRecords.map(r => (
                            <div key={r.record_id} className="px-4 py-2 flex items-center gap-3">
                              <div className="flex-1 min-w-0">
                                <p className="text-sm font-medium text-gray-900 truncate">{r.label}</p>
                                <p className="text-xs text-gray-400">
                                  {Object.entries(r.fields)
                                    .filter(([, v]) => typeof v === 'string')
                                    .map(([, v]) => v)
                                    .join(' · ')}
                                </p>
                              </div>
                              <span className="text-xs font-medium text-brand-700">{r.score.toFixed(3)}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>

          {/* Top records table */}
          {topRecords.length > 0 && (
            <div>
              <h2 className="text-lg font-semibold text-gray-900 mb-2">Top Scored Records</h2>
              <p className="text-sm text-gray-500 mb-4">
                Records with the highest proximity score to their cluster centroid.
              </p>
              <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-gray-100 bg-gray-50">
                        <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500">Record</th>
                        <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500">Cluster</th>
                        {stringFields.map(f => (
                          <th key={f} className="text-left px-4 py-3 text-xs font-semibold text-gray-500">{metricLabel(f)}</th>
                        ))}
                        {numericFields.map(f => (
                          <th key={f} className="text-right px-4 py-3 text-xs font-semibold text-gray-500">{metricLabel(f)}</th>
                        ))}
                        <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500">Score</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-50">
                      {topRecords.map(r => {
                        const color = clusterColor(r.cluster_id);
                        return (
                          <tr key={r.record_id} className="hover:bg-gray-50">
                            <td className="px-4 py-3">
                              <p className="font-medium text-gray-900">{r.label}</p>
                            </td>
                            <td className="px-4 py-3">
                              <span className={`px-2 py-0.5 rounded-full text-xs font-bold ${color.badge}`}>
                                C{r.cluster_id}
                              </span>
                            </td>
                            {stringFields.map(f => (
                              <td key={f} className="px-4 py-3 text-gray-600">{r.fields[f] ?? '-'}</td>
                            ))}
                            {numericFields.map(f => (
                              <td key={f} className="px-4 py-3 text-right text-gray-600">{r.fields[f] ?? '-'}</td>
                            ))}
                            <td className="px-4 py-3 text-right">
                              <span className="font-bold text-brand-700">{r.score.toFixed(3)}</span>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {!loading && !error && clusters.length === 0 && (
        <div className="text-center py-20">
          <svg className="w-12 h-12 text-gray-300 mx-auto mb-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
          </svg>
          <p className="text-sm text-gray-500">No clustering insights yet for this dataset.</p>
          <p className="text-xs text-gray-400 mt-1">Run the ML pipeline on <span className="font-medium">{dataset}</span> to generate cluster analysis.</p>
        </div>
      )}
    </div>
  );
}
