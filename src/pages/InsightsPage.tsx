import { useState, useEffect } from 'react';
import { useAuth } from '../context/AuthContext';
import { fetchClusters, listJobs } from '../lib/uploadService';
import type { ClusterSummary, ClusterPlayer } from '../lib/uploadService';
import type { PipelineJob } from '../types';
import { MOCK_CLUSTERS, MOCK_CLUSTER_PLAYERS, MOCK_LINEAGE_JOBS } from '../data/mockClusters';

const ML_SOURCE_TABLES = ['all_player_stats', 'all_player_profiles'];
const SOURCE_COLORS: Record<string, { border: string; bg: string; icon: string }> = {
  all_player_stats: { border: 'border-l-teal-500', bg: 'bg-teal-50', icon: 'text-teal-600' },
  all_player_profiles: { border: 'border-l-indigo-500', bg: 'bg-indigo-50', icon: 'text-indigo-600' },
};

function filterLineageJobs(jobs: PipelineJob[]): PipelineJob[] {
  return jobs.filter(j =>
    j.status === 'LOADED' &&
    j.bq_table &&
    ML_SOURCE_TABLES.some(t => j.bq_table!.includes(t))
  );
}

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
    const key = ML_SOURCE_TABLES.find(t => j.bq_table?.includes(t)) ?? j.bq_table ?? 'unknown';
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

function inferClusterRole(c: ClusterSummary): string {
  if (c.avg_saves > 2) return 'Goalkeepers';
  if (c.avg_tackles > 10 && c.avg_interceptions > 5) return 'Defensive Anchors';
  if (c.avg_goals > 5 && c.avg_assists > 3) return 'Goal Threats';
  if (c.avg_assists > 4) return 'Creative Playmakers';
  if (c.avg_tackles > 8) return 'Ball Winners';
  if (c.avg_minutes > 2000 && c.avg_rating > 6.5) return 'Reliable Starters';
  if (c.avg_goals > 3) return 'Attacking Contributors';
  return `Cluster ${c.cluster_id}`;
}

function StatBar({ value, max, color }: { value: number; max: number; color: string }) {
  const pct = max > 0 ? Math.min(100, (value / max) * 100) : 0;
  return (
    <div className="w-full bg-gray-100 rounded-full h-1.5">
      <div className={`h-1.5 rounded-full ${color} transition-all duration-500`} style={{ width: `${pct}%` }} />
    </div>
  );
}

export default function InsightsPage() {
  const { user } = useAuth();
  const isGuest = user?.role === 'guest';
  const [clusters, setClusters] = useState<ClusterSummary[]>(isGuest ? MOCK_CLUSTERS : []);
  const [players, setPlayers] = useState<ClusterPlayer[]>(isGuest ? MOCK_CLUSTER_PLAYERS : []);
  const [lineageJobs, setLineageJobs] = useState<PipelineJob[]>(isGuest ? MOCK_LINEAGE_JOBS : []);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedCluster, setExpandedCluster] = useState<number | null>(null);
  const [lineageOpen, setLineageOpen] = useState(true);

  useEffect(() => {
    if (isGuest) return;
    setLoading(true);
    Promise.all([fetchClusters(), listJobs().catch(() => [] as PipelineJob[])])
      .then(([data, allJobs]) => {
        setClusters(data.clusters);
        setPlayers(data.players);
        setLineageJobs(filterLineageJobs(allJobs));
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false));
  }, [isGuest]);

  const totalPlayers = clusters.reduce((sum, c) => sum + c.player_count, 0);
  const maxGoals = Math.max(...clusters.map(c => c.avg_goals), 1);
  const maxAssists = Math.max(...clusters.map(c => c.avg_assists), 1);
  const maxTackles = Math.max(...clusters.map(c => c.avg_tackles), 1);
  const maxSaves = Math.max(...clusters.map(c => c.avg_saves), 1);

  // Hidden impact: high impact score but low conventional stats (goals/assists)
  const hiddenImpact = players
    .filter(p => p.impact_score > 0.5)
    .sort((a, b) => b.impact_score - a.impact_score)
    .slice(0, 10);

  return (
    <div className="p-4 sm:p-8 max-w-6xl mx-auto">
      <div className="mb-8">
        <h1 className="text-xl sm:text-2xl font-bold text-gray-900">Vertex AI Insights</h1>
        <p className="text-gray-500 mt-1 text-sm">
          K-Means clustering analysis — player role identification and hidden impact detection
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
      {!loading && !error && clusters.length > 0 && lineageJobs.length > 0 && (() => {
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
                <span className="text-xs text-gray-400">{groups.length} datasets · {totalUploads} uploads</span>
              </div>
              <svg className={`w-4 h-4 text-gray-400 transition-transform ${lineageOpen ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
              </svg>
            </button>

            {lineageOpen && (
              <div className="px-4 pb-4">
                <div className="grid grid-cols-1 md:grid-cols-[1fr_40px_1fr] gap-4 items-center">
                  {/* Source datasets */}
                  <div className="space-y-3">
                    {groups.map(g => {
                      const colors = SOURCE_COLORS[g.table] ?? { border: 'border-l-gray-400', bg: 'bg-gray-50', icon: 'text-gray-500' };
                      return (
                        <div key={g.table} className={`border border-gray-100 rounded-lg p-3 border-l-4 ${colors.border}`}>
                          <div className="flex items-center gap-2 mb-1">
                            <svg className={`w-3.5 h-3.5 ${colors.icon}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
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
                      );
                    })}
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
                      <span>{totalPlayers.toLocaleString()} players</span>
                    </div>
                    <p className="text-xs text-gray-400 mt-1">Vertex AI · curated.player_clusters</p>
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
              <p className="text-xs text-gray-400">Total Players</p>
              <p className="text-2xl font-bold text-gray-900">{totalPlayers.toLocaleString()}</p>
            </div>
            <div className="bg-white border border-gray-200 rounded-xl p-4">
              <p className="text-xs text-gray-400">Avg Rating</p>
              <p className="text-2xl font-bold text-gray-900">
                {(clusters.reduce((s, c) => s + c.avg_rating * c.player_count, 0) / totalPlayers).toFixed(2)}
              </p>
            </div>
            <div className="bg-white border border-gray-200 rounded-xl p-4">
              <p className="text-xs text-gray-400">Hidden Impact</p>
              <p className="text-2xl font-bold text-brand-700">{hiddenImpact.length}</p>
            </div>
          </div>

          {/* Cluster cards */}
          <div>
            <h2 className="text-lg font-semibold text-gray-900 mb-4">Player Clusters</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {clusters.map(c => {
                const color = clusterColor(c.cluster_id);
                const role = inferClusterRole(c);
                const clusterPlayers = players.filter(p => p.cluster_id === c.cluster_id);
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
                          <h3 className={`font-semibold ${color.text}`}>{role}</h3>
                        </div>
                        <span className="text-xs text-gray-500">{c.player_count} players</span>
                      </div>

                      <div className="space-y-2">
                        <div className="flex items-center gap-2">
                          <span className="text-xs text-gray-500 w-16">Goals</span>
                          <StatBar value={c.avg_goals} max={maxGoals} color={color.bar} />
                          <span className="text-xs font-medium text-gray-700 w-8 text-right">{c.avg_goals}</span>
                        </div>
                        <div className="flex items-center gap-2">
                          <span className="text-xs text-gray-500 w-16">Assists</span>
                          <StatBar value={c.avg_assists} max={maxAssists} color={color.bar} />
                          <span className="text-xs font-medium text-gray-700 w-8 text-right">{c.avg_assists}</span>
                        </div>
                        <div className="flex items-center gap-2">
                          <span className="text-xs text-gray-500 w-16">Tackles</span>
                          <StatBar value={c.avg_tackles} max={maxTackles} color={color.bar} />
                          <span className="text-xs font-medium text-gray-700 w-8 text-right">{c.avg_tackles}</span>
                        </div>
                        <div className="flex items-center gap-2">
                          <span className="text-xs text-gray-500 w-16">Saves</span>
                          <StatBar value={c.avg_saves} max={maxSaves} color={color.bar} />
                          <span className="text-xs font-medium text-gray-700 w-8 text-right">{c.avg_saves}</span>
                        </div>
                      </div>

                      <div className="flex items-center justify-between mt-3 pt-2 border-t border-gray-200/50">
                        <span className="text-xs text-gray-500">Avg rating: <span className="font-medium text-gray-700">{c.avg_rating}</span></span>
                        <span className="text-xs text-gray-500">Impact: <span className="font-medium text-gray-700">{c.avg_impact_score}</span></span>
                      </div>
                    </button>

                    {/* Expanded: top players */}
                    {isExpanded && clusterPlayers.length > 0 && (
                      <div className="border-t border-gray-200/50 bg-white/50">
                        <div className="px-4 py-2 bg-gray-50/50">
                          <p className="text-xs font-semibold text-gray-500 uppercase">Top Players</p>
                        </div>
                        <div className="divide-y divide-gray-100">
                          {clusterPlayers.map(p => (
                            <div key={p.player_id} className="px-4 py-2 flex items-center gap-3">
                              <div className="flex-1 min-w-0">
                                <p className="text-sm font-medium text-gray-900 truncate">{p.name}</p>
                                <p className="text-xs text-gray-400">{p.position} · {p.league}</p>
                              </div>
                              <div className="flex items-center gap-3 text-xs text-gray-600">
                                <span>{p.goals}G</span>
                                <span>{p.assists}A</span>
                                <span>{p.tackles}T</span>
                                <span className="font-medium text-brand-700">{p.impact_score.toFixed(3)}</span>
                              </div>
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

          {/* Hidden impact players */}
          {hiddenImpact.length > 0 && (
            <div>
              <h2 className="text-lg font-semibold text-gray-900 mb-2">Hidden Impact Players</h2>
              <p className="text-sm text-gray-500 mb-4">
                Players with high impact scores — closest to their cluster centroid, representing the archetypal player for their role.
              </p>
              <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-gray-100 bg-gray-50">
                        <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500">Player</th>
                        <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500">Cluster</th>
                        <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500">Position</th>
                        <th className="text-left px-4 py-3 text-xs font-semibold text-gray-500">League</th>
                        <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500">Goals</th>
                        <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500">Assists</th>
                        <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500">Rating</th>
                        <th className="text-right px-4 py-3 text-xs font-semibold text-gray-500">Impact</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-50">
                      {hiddenImpact.map(p => {
                        const color = clusterColor(p.cluster_id);
                        return (
                          <tr key={p.player_id} className="hover:bg-gray-50">
                            <td className="px-4 py-3">
                              <p className="font-medium text-gray-900">{p.name}</p>
                            </td>
                            <td className="px-4 py-3">
                              <span className={`px-2 py-0.5 rounded-full text-xs font-bold ${color.badge}`}>
                                C{p.cluster_id}
                              </span>
                            </td>
                            <td className="px-4 py-3 text-gray-600">{p.position}</td>
                            <td className="px-4 py-3 text-gray-600">{p.league}</td>
                            <td className="px-4 py-3 text-right text-gray-600">{p.goals}</td>
                            <td className="px-4 py-3 text-right text-gray-600">{p.assists}</td>
                            <td className="px-4 py-3 text-right text-gray-600">{p.rating}</td>
                            <td className="px-4 py-3 text-right">
                              <span className="font-bold text-brand-700">{p.impact_score.toFixed(3)}</span>
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
    </div>
  );
}
