import { useState, useEffect } from 'react';
import { useAuth } from '../context/AuthContext';
import { fetchClusters } from '../lib/uploadService';
import type { ClusterSummary, ClusterPlayer } from '../lib/uploadService';

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
  const [clusters, setClusters] = useState<ClusterSummary[]>([]);
  const [players, setPlayers] = useState<ClusterPlayer[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedCluster, setExpandedCluster] = useState<number | null>(null);

  useEffect(() => {
    if (isGuest) return;
    setLoading(true);
    fetchClusters()
      .then(data => {
        setClusters(data.clusters);
        setPlayers(data.players);
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
        <div className="p-4 bg-amber-50 border border-amber-200 rounded-xl">
          <p className="text-sm font-medium text-amber-800">Sign in with Google to view ML insights.</p>
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
