import { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { fetchClusters, fetchAnomalies, fetchPredictions, fetchProfile, listJobs } from '../lib/uploadService';
import type { ClusterSummary, ClusterRecord, AnomalyData, PredictionData, ProfileData, ModelType } from '../lib/uploadService';
import type { PipelineJob } from '../types';
import { MOCK_CLUSTERS, MOCK_CLUSTER_RECORDS, MOCK_LINEAGE_JOBS, MOCK_MODELS, MOCK_ANOMALY_DATA, MOCK_PREDICTION_DATA } from '../data/mockClusters';
import { ClusterDistributionChart, ClusterMetricsRadar, AnomalyScoreDistribution, AnomalyBreakdownPie, PredictionScatterChart, ResidualDistributionChart, ProfileBarChart } from '../components/InsightCharts';

// ── Shared helpers ──

interface TableGroup { table: string; jobs: PipelineJob[]; totalLoaded: number; totalSize: number; latestDate: string }

function groupByTable(jobs: PipelineJob[]): TableGroup[] {
  const map = new Map<string, PipelineJob[]>();
  for (const j of jobs) { const k = j.bq_table ?? j.dataset; map.set(k, [...(map.get(k) ?? []), j]); }
  return Array.from(map.entries()).map(([table, js]) => ({
    table, jobs: js,
    totalLoaded: js.reduce((s, j) => s + (j.stats?.loaded ?? 0), 0),
    totalSize: js.reduce((s, j) => s + j.file_size_bytes, 0),
    latestDate: js.reduce((l, j) => j.updated_at > l ? j.updated_at : l, ''),
  }));
}

function formatBytes(b: number) { return b < 1024 ? `${b} B` : b < 1048576 ? `${(b/1024).toFixed(1)} KB` : `${(b/1048576).toFixed(1)} MB`; }
function formatDate(iso: string) { return new Date(iso).toLocaleDateString('en-AU', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }); }
function metricLabel(key: string) { return key.replace(/^avg_/, '').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()); }

const TYPE_LABELS: Record<ModelType, string> = { clusters: 'K-Means Clustering', anomalies: 'Anomaly Detection', predictions: 'Prediction Model', profile: 'Profile Analysis' };

import { getNarrative } from '../lib/narratives';
import type { Narrative } from '../lib/narratives';

function NarrativeSection({ narrative, reportUrl }: { narrative: Narrative; reportUrl?: string }) {
  return (
    <div className="bg-gradient-to-br from-brand-50 to-blue-50 border border-brand-200 rounded-xl p-5 mb-6">
      <div className="flex items-start justify-between gap-4 mb-2">
        <h2 className="text-base font-semibold text-gray-900">{narrative.title}</h2>
        {reportUrl && (
          <Link
            to={reportUrl}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-white border border-brand-300 rounded-lg text-xs font-medium text-brand-700 hover:bg-brand-50 transition flex-shrink-0"
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
            </svg>
            Export Report
          </Link>
        )}
      </div>
      <p className="text-sm text-gray-700 mb-4">{narrative.overview}</p>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div>
          <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1">What this data shows</h3>
          <p className="text-sm text-gray-600">{narrative.whatItShows}</p>
        </div>
        <div>
          <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1">Recommended actions</h3>
          <ul className="text-sm text-gray-600 space-y-1">
            {narrative.actions.map((a, i) => (
              <li key={i} className="flex gap-2">
                <span className="text-brand-500 flex-shrink-0 mt-0.5">-</span>
                <span>{a}</span>
              </li>
            ))}
          </ul>
        </div>
      </div>

      {narrative.fineTuning && (
        <div className="mt-4 pt-3 border-t border-brand-200">
          <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-1">Model fine-tuning notes</h3>
          <p className="text-xs text-gray-500">{narrative.fineTuning}</p>
        </div>
      )}
    </div>
  );
}

// ── Cluster helpers ──

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
function clusterColor(id: number) { return CLUSTER_COLORS[id % CLUSTER_COLORS.length]; }

function pickDisplayMetrics(clusters: ClusterSummary[], max = 4) {
  const keys = new Set<string>(); clusters.forEach(c => Object.keys(c.metrics).forEach(k => keys.add(k)));
  return [...keys].map(k => ({ key: k, max: Math.max(...clusters.map(c => Math.abs(c.metrics[k] ?? 0))) }))
    .filter(e => e.max > 0.01).sort((a, b) => b.max - a.max).slice(0, max).map(e => e.key);
}

function StatBar({ value, max, color }: { value: number; max: number; color: string }) {
  return <div className="w-full bg-gray-100 rounded-full h-1.5"><div className={`h-1.5 rounded-full ${color} transition-all duration-500`} style={{ width: `${max > 0 ? Math.min(100, (value/max)*100) : 0}%` }} /></div>;
}

// ── Cluster View ──

function ClustersView({ clusters, records, expandedCluster, setExpandedCluster }: {
  clusters: ClusterSummary[]; records: ClusterRecord[]; expandedCluster: number | null; setExpandedCluster: (v: number | null) => void;
}) {
  const totalRecords = clusters.reduce((s, c) => s + c.record_count, 0);
  const displayMetrics = pickDisplayMetrics(clusters);
  const maxes = Object.fromEntries(displayMetrics.map(k => [k, Math.max(...clusters.map(c => Math.abs(c.metrics[k] ?? 0)), 1)]));
  const topRecords = [...records].sort((a, b) => b.score - a.score).slice(0, 10);

  return (
    <div className="space-y-8">
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <div className="bg-white border border-gray-200 rounded-xl p-4"><p className="text-xs text-gray-400">Clusters</p><p className="text-2xl font-bold text-gray-900">{clusters.length}</p></div>
        <div className="bg-white border border-gray-200 rounded-xl p-4"><p className="text-xs text-gray-400">Total Records</p><p className="text-2xl font-bold text-gray-900">{totalRecords.toLocaleString()}</p></div>
        <div className="bg-white border border-gray-200 rounded-xl p-4"><p className="text-xs text-gray-400">Top Scored</p><p className="text-2xl font-bold text-brand-700">{topRecords.length}</p></div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <ClusterDistributionChart clusters={clusters} />
        <ClusterMetricsRadar clusters={clusters} />
      </div>

      <div>
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Clusters</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {clusters.map(c => {
            const color = clusterColor(c.cluster_id);
            const cr = records.filter(r => r.cluster_id === c.cluster_id);
            const expanded = expandedCluster === c.cluster_id;
            return (
              <div key={c.cluster_id} className={`border rounded-xl overflow-hidden ${color.border} ${color.bg}`}>
                <button onClick={() => setExpandedCluster(expanded ? null : c.cluster_id)} className="w-full p-4 text-left">
                  <div className="flex items-center justify-between mb-3">
                    <div className="flex items-center gap-2">
                      <span className={`px-2 py-0.5 rounded-full text-xs font-bold ${color.badge}`}>C{c.cluster_id}</span>
                      <h3 className={`font-semibold ${color.text}`}>{c.label ?? `Cluster ${c.cluster_id}`}</h3>
                    </div>
                    <span className="text-xs text-gray-500">{c.record_count} records</span>
                  </div>
                  <div className="space-y-2">
                    {displayMetrics.map(key => (
                      <div key={key} className="flex items-center gap-2">
                        <span className="text-xs text-gray-500 w-24 truncate">{metricLabel(key)}</span>
                        <StatBar value={c.metrics[key] ?? 0} max={maxes[key]} color={color.bar} />
                        <span className="text-xs font-medium text-gray-700 w-10 text-right">{(c.metrics[key] ?? 0) % 1 === 0 ? c.metrics[key] : (c.metrics[key] ?? 0).toFixed(1)}</span>
                      </div>
                    ))}
                  </div>
                </button>
                {expanded && cr.length > 0 && (
                  <div className="border-t border-gray-200/50 bg-white/50">
                    <div className="px-4 py-2 bg-gray-50/50"><p className="text-xs font-semibold text-gray-500 uppercase">Top Records</p></div>
                    <div className="divide-y divide-gray-100">
                      {cr.map(r => (
                        <div key={r.record_id} className="px-4 py-2 flex items-center gap-3">
                          <div className="flex-1 min-w-0">
                            <p className="text-sm font-medium text-gray-900 truncate">{r.label}</p>
                            <p className="text-xs text-gray-400">{Object.entries(r.fields).filter(([,v]) => typeof v === 'string').map(([,v]) => v).join(' · ')}</p>
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
    </div>
  );
}

// ── Anomaly View ──

function AnomaliesView({ data }: { data: AnomalyData }) {
  const { summary, records } = data;
  const anomalies = records.filter(r => r.is_anomaly);

  return (
    <div className="space-y-8">
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <div className="bg-white border border-gray-200 rounded-xl p-4"><p className="text-xs text-gray-400">Total Records</p><p className="text-2xl font-bold text-gray-900">{summary.total.toLocaleString()}</p></div>
        <div className="bg-white border border-gray-200 rounded-xl p-4"><p className="text-xs text-gray-400">Anomalies</p><p className="text-2xl font-bold text-amber-600">{summary.anomaly_count}</p></div>
        <div className="bg-white border border-gray-200 rounded-xl p-4"><p className="text-xs text-gray-400">Anomaly Rate</p><p className="text-2xl font-bold text-gray-900">{(summary.anomaly_count / summary.total * 100).toFixed(1)}%</p></div>
        <div className="bg-white border border-gray-200 rounded-xl p-4"><p className="text-xs text-gray-400">Max Score</p><p className="text-2xl font-bold text-gray-900">{summary.max_score}</p></div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <AnomalyScoreDistribution data={data} />
        <AnomalyBreakdownPie data={data} />
      </div>

      <div>
        <h2 className="text-lg font-semibold text-gray-900 mb-2">Anomalous Records</h2>
        <p className="text-sm text-gray-500 mb-4">Records with unusual stat distributions detected by Isolation Forest.</p>
        <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100 bg-gray-50">
                  <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500">Record</th>
                  <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500">Status</th>
                  {records[0] && Object.keys(records[0].fields).filter(k => typeof records[0].fields[k] === 'string').slice(0, 2).map(k => (
                    <th key={k} className="text-left px-4 py-3 text-xs font-semibold text-gray-500">{metricLabel(k)}</th>
                  ))}
                  {records[0] && Object.keys(records[0].fields).filter(k => typeof records[0].fields[k] === 'number').slice(0, 3).map(k => (
                    <th key={k} className="text-right px-4 py-3 text-xs font-semibold text-gray-500">{metricLabel(k)}</th>
                  ))}
                  <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500">Anomaly Score</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {(anomalies.length > 0 ? anomalies : records).slice(0, 20).map(r => {
                  const strFields = Object.entries(r.fields).filter(([,v]) => typeof v === 'string').slice(0, 2);
                  const numFields = Object.entries(r.fields).filter(([,v]) => typeof v === 'number').slice(0, 3);
                  return (
                    <tr key={r.record_id} className="hover:bg-gray-50">
                      <td className="px-4 py-3 font-medium text-gray-900">{r.label}</td>
                      <td className="px-4 py-3">
                        <span className={`px-2 py-0.5 rounded-full text-xs font-bold ${r.is_anomaly ? 'bg-amber-100 text-amber-700' : 'bg-gray-100 text-gray-500'}`}>
                          {r.is_anomaly ? 'Anomaly' : 'Normal'}
                        </span>
                      </td>
                      {strFields.map(([,v]) => <td key={String(v)} className="px-4 py-3 text-gray-600">{String(v)}</td>)}
                      {numFields.map(([k,v]) => <td key={k} className="px-4 py-3 text-right text-gray-600">{v}</td>)}
                      <td className="px-4 py-3 text-right">
                        <span className={`font-bold ${r.is_anomaly ? 'text-amber-700' : 'text-gray-500'}`}>{r.anomaly_score.toFixed(4)}</span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Prediction View ──

function PredictionsView({ data }: { data: PredictionData }) {
  const { summary, records } = data;
  const overRated = records.filter(r => r.residual > 0).slice(0, 10);
  const underRated = records.filter(r => r.residual < 0).slice(0, 10);

  return (
    <div className="space-y-8">
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <div className="bg-white border border-gray-200 rounded-xl p-4"><p className="text-xs text-gray-400">R² (approx)</p><p className="text-2xl font-bold text-gray-900">{summary.r2_approx}</p></div>
        <div className="bg-white border border-gray-200 rounded-xl p-4"><p className="text-xs text-gray-400">MAE</p><p className="text-2xl font-bold text-gray-900">{summary.mae}</p></div>
        <div className="bg-white border border-gray-200 rounded-xl p-4"><p className="text-xs text-gray-400">RMSE</p><p className="text-2xl font-bold text-gray-900">{summary.rmse}</p></div>
        <div className="bg-white border border-gray-200 rounded-xl p-4"><p className="text-xs text-gray-400">Total Records</p><p className="text-2xl font-bold text-gray-900">{summary.total.toLocaleString()}</p></div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <PredictionScatterChart data={data} />
        <ResidualDistributionChart data={data} />
      </div>

      {/* Over-rated (model predicts higher than actual) */}
      {overRated.length > 0 && (
        <div>
          <h2 className="text-lg font-semibold text-gray-900 mb-2">Over-Rated</h2>
          <p className="text-sm text-gray-500 mb-4">Model predicts a higher rating than actual — may be underperforming relative to stats.</p>
          <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-100 bg-gray-50">
                    <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500">Record</th>
                    <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500">Position</th>
                    <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500">Predicted</th>
                    <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500">Actual</th>
                    <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500">Residual</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50">
                  {overRated.map(r => (
                    <tr key={r.record_id} className="hover:bg-gray-50">
                      <td className="px-4 py-3 font-medium text-gray-900">{r.label}</td>
                      <td className="px-4 py-3 text-gray-600">{r.position ?? '-'}</td>
                      <td className="px-4 py-3 text-right text-gray-600">{r.predicted_rating}</td>
                      <td className="px-4 py-3 text-right text-gray-600">{r.actual_rating}</td>
                      <td className="px-4 py-3 text-right"><span className="font-bold text-green-600">+{r.residual.toFixed(4)}</span></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* Under-rated */}
      {underRated.length > 0 && (
        <div>
          <h2 className="text-lg font-semibold text-gray-900 mb-2">Under-Rated</h2>
          <p className="text-sm text-gray-500 mb-4">Model predicts a lower rating than actual — may be overperforming relative to stats.</p>
          <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-100 bg-gray-50">
                    <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500">Record</th>
                    <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500">Position</th>
                    <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500">Predicted</th>
                    <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500">Actual</th>
                    <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500">Residual</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-50">
                  {underRated.map(r => (
                    <tr key={r.record_id} className="hover:bg-gray-50">
                      <td className="px-4 py-3 font-medium text-gray-900">{r.label}</td>
                      <td className="px-4 py-3 text-gray-600">{r.position ?? '-'}</td>
                      <td className="px-4 py-3 text-right text-gray-600">{r.predicted_rating}</td>
                      <td className="px-4 py-3 text-right text-gray-600">{r.actual_rating}</td>
                      <td className="px-4 py-3 text-right"><span className="font-bold text-red-600">{r.residual.toFixed(4)}</span></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ── Profile View ──

function ProfileView({ data }: { data: ProfileData }) {
  const numericCols = data.columns.filter(c => ['FLOAT64', 'INT64', 'NUMERIC'].includes(c.type));
  const stringCols = data.columns.filter(c => c.type === 'STRING');
  const labelCol = stringCols[0]?.name;

  return (
    <div className="space-y-6">
      <ProfileBarChart data={data} />

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <div className="bg-white border border-gray-200 rounded-xl p-4">
          <p className="text-xs text-gray-400">Records</p>
          <p className="text-2xl font-bold text-gray-900">{data.total}</p>
        </div>
        <div className="bg-white border border-gray-200 rounded-xl p-4">
          <p className="text-xs text-gray-400">Columns</p>
          <p className="text-2xl font-bold text-gray-900">{data.columns.length}</p>
        </div>
        <div className="bg-white border border-gray-200 rounded-xl p-4">
          <p className="text-xs text-gray-400">Table</p>
          <p className="text-sm font-semibold text-gray-700 truncate">{data.outputTable}</p>
        </div>
        <div className="bg-white border border-gray-200 rounded-xl p-4">
          <p className="text-xs text-gray-400">Numeric Fields</p>
          <p className="text-2xl font-bold text-gray-900">{numericCols.length}</p>
        </div>
      </div>

      <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-100 bg-gray-50">
          <h2 className="text-sm font-semibold text-gray-700">Data</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100 bg-gray-50">
                {data.columns.map(col => (
                  <th key={col.name} className="text-left px-4 py-2 text-xs font-semibold text-gray-500 whitespace-nowrap">
                    {col.name}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {data.records.map((row, i) => (
                <tr key={labelCol ? String(row[labelCol]) : i} className="hover:bg-gray-50">
                  {data.columns.map(col => (
                    <td key={col.name} className="px-4 py-2 text-gray-700 whitespace-nowrap max-w-[200px] truncate">
                      {typeof row[col.name] === 'number'
                        ? (Number(row[col.name]) % 1 !== 0 ? Number(row[col.name]).toFixed(4) : String(row[col.name]))
                        : String(row[col.name] ?? '')}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

// ── Main Page ──

export default function InsightsPage() {
  const { type, model } = useParams<{ type: string; model: string }>();
  const modelType = (type as ModelType) || 'clusters';
  const { user } = useAuth();
  const isGuest = user?.role === 'guest';

  const [clusterData, setClusterData] = useState<{ clusters: ClusterSummary[]; records: ClusterRecord[] } | null>(null);
  const [anomalyData, setAnomalyData] = useState<AnomalyData | null>(null);
  const [predictionData, setPredictionData] = useState<PredictionData | null>(null);
  const [profileData, setProfileData] = useState<ProfileData | null>(null);
  const [sourceTables, setSourceTables] = useState<string[]>([]);
  const [lineageJobs, setLineageJobs] = useState<PipelineJob[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedCluster, setExpandedCluster] = useState<number | null>(null);
  const [lineageOpen, setLineageOpen] = useState(true);

  useEffect(() => {
    if (!model) return;

    if (isGuest) {
      const mockModel = MOCK_MODELS.find(m => m.model === model && m.type === modelType);
      const sources = mockModel?.sourceTables ?? [];
      // eslint-disable-next-line react-hooks/set-state-in-effect -- sync mock data for guest mode
      setSourceTables(sources);
      setLineageJobs(MOCK_LINEAGE_JOBS.filter(j => sources.includes(j.dataset)));
      if (modelType === 'clusters') setClusterData({ clusters: MOCK_CLUSTERS, records: MOCK_CLUSTER_RECORDS });
      else if (modelType === 'anomalies') setAnomalyData(MOCK_ANOMALY_DATA);
      else if (modelType === 'predictions') setPredictionData(MOCK_PREDICTION_DATA);
      setLoading(false);
      return;
    }

    setLoading(true);
    const dataFetch = modelType === 'clusters'
      ? fetchClusters(model).then(d => { setClusterData({ clusters: d.clusters, records: d.records }); setSourceTables(d.sourceTables); })
      : modelType === 'anomalies'
      ? fetchAnomalies(model).then(d => { setAnomalyData(d); setSourceTables(d.sourceTables); })
      : modelType === 'profile'
      ? fetchProfile(model).then(d => { setProfileData(d); setSourceTables(d.sourceTables); })
      : fetchPredictions(model).then(d => { setPredictionData(d); setSourceTables(d.sourceTables); });

    Promise.all([dataFetch, listJobs().catch(() => [] as PipelineJob[])])
      .then(([, allJobs]) => {
        setLineageJobs(allJobs.filter(j => j.status === 'LOADED'));
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false));
  }, [model, modelType, isGuest]);

  // Filter lineage jobs to source tables once both are loaded
  const filteredLineage = lineageJobs.filter(j => sourceTables.includes(j.dataset));
  const hasData = clusterData || anomalyData || predictionData || profileData;

  return (
    <div className="p-4 sm:p-8 max-w-6xl mx-auto">
      <div className="mb-8">
        <Link to="/insights" className="inline-flex items-center gap-1 text-sm text-gray-500 hover:text-brand-600 transition mb-3">
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
          </svg>
          All Models
        </Link>
        <h1 className="text-xl sm:text-2xl font-bold text-gray-900">{model}</h1>
        <p className="text-gray-500 mt-1 text-sm">
          {TYPE_LABELS[modelType]} — {sourceTables.length} source {sourceTables.length === 1 ? 'table' : 'tables'}
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
      {!loading && !error && filteredLineage.length > 0 && (() => {
        const groups = groupByTable(filteredLineage);
        return (
          <div className="mb-6 bg-white border border-gray-200 rounded-xl overflow-hidden">
            <button onClick={() => setLineageOpen(!lineageOpen)} className="w-full flex items-center justify-between px-4 py-3 hover:bg-gray-50 transition-colors">
              <div className="flex items-center gap-2">
                <svg className="w-4 h-4 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2 1 3 3 3h10c2 0 3-1 3-3V7c0-2-1-3-3-3H7C5 4 4 5 4 7zM9 12h6M12 9v6" /></svg>
                <span className="text-sm font-semibold text-gray-700">Data Lineage</span>
                <span className="text-xs text-gray-400">{groups.length} {groups.length === 1 ? 'table' : 'tables'} · {filteredLineage.length} uploads</span>
              </div>
              <svg className={`w-4 h-4 text-gray-400 transition-transform ${lineageOpen ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" /></svg>
            </button>
            {lineageOpen && (
              <div className="px-4 pb-4">
                <div className="grid grid-cols-1 md:grid-cols-[1fr_40px_1fr] gap-4 items-center">
                  <div className="space-y-3">
                    {groups.map(g => (
                      <div key={g.table} className="border border-gray-100 rounded-lg p-3 border-l-4 border-l-teal-500">
                        <div className="flex items-center gap-2 mb-1">
                          <svg className="w-3.5 h-3.5 text-teal-600" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 10h18M3 14h18M3 18h18M3 6h18" /></svg>
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
                  <div className="hidden md:flex flex-col items-center justify-center gap-1">
                    <div className="w-px flex-1 border-l-2 border-dashed border-gray-200" />
                    <svg className="w-5 h-5 text-gray-300" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" /></svg>
                    <div className="w-px flex-1 border-l-2 border-dashed border-gray-200" />
                  </div>
                  <div className="border border-gray-100 rounded-lg p-3 border-l-4 border-l-brand-500">
                    <div className="flex items-center gap-2 mb-1">
                      <svg className="w-3.5 h-3.5 text-brand-600" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" /></svg>
                      <span className="text-xs font-bold text-gray-700">{TYPE_LABELS[modelType]}</span>
                    </div>
                    <p className="text-xs text-gray-400 mt-1">Vertex AI</p>
                  </div>
                </div>
              </div>
            )}
          </div>
        );
      })()}

      {/* Model-specific view */}
      {!loading && !error && hasData && (
        <>
          <NarrativeSection
            narrative={getNarrative(model ?? '', modelType, profileData?.outputTable)}
            reportUrl={`/insights/${encodeURIComponent(type ?? '')}/${encodeURIComponent(model ?? '')}/report`}
          />
          {clusterData && <ClustersView clusters={clusterData.clusters} records={clusterData.records} expandedCluster={expandedCluster} setExpandedCluster={setExpandedCluster} />}
          {anomalyData && <AnomaliesView data={anomalyData} />}
          {predictionData && <PredictionsView data={predictionData} />}
          {profileData && <ProfileView data={profileData} />}
        </>
      )}

      {!loading && !error && !hasData && (
        <div className="text-center py-20">
          <svg className="w-12 h-12 text-gray-300 mx-auto mb-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
          </svg>
          <p className="text-sm text-gray-500">No insights yet for this model.</p>
          <p className="text-xs text-gray-400 mt-1">Run the ML pipeline on <span className="font-medium">{model}</span> first.</p>
        </div>
      )}
    </div>
  );
}
