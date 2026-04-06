import { useState, useEffect } from 'react';
import { useAuth } from '../context/AuthContext';
import { fetchNFLTeamAnalysis } from '../lib/nflService';
import { MOCK_NFL_ANALYSIS } from '../data/mockNFLTeams';
import type { NFLTeamAnalysisData, NFLTeamRanking } from '../types/nflTeams';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
  ResponsiveContainer,
} from 'recharts';

const COLORS = ['#3b82f6', '#22c55e', '#a855f7', '#f97316', '#ec4899', '#06b6d4', '#f59e0b', '#ef4444'];

const PILLAR_LABELS: Record<string, string> = {
  pillar_winning: 'Winning',
  pillar_offence: 'Offence',
  pillar_defence: 'Defence',
  pillar_efficiency: 'Efficiency',
};

function formatFeature(name: string): string {
  return name
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase())
    .replace('Pct', '%')
    .replace('Opp', 'Opponent')
    .replace('Yds', 'Yards')
    .replace('Mov', 'Margin of Victory');
}

export default function NFLTeamsPage() {
  const { user } = useAuth();
  const isGuest = user?.role === 'guest';
  const [data, setData] = useState<NFLTeamAnalysisData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedTeam, setSelectedTeam] = useState<NFLTeamRanking | null>(null);

  useEffect(() => {
    if (isGuest) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- sync mock data for guest mode
      setData(MOCK_NFL_ANALYSIS);
      // eslint-disable-next-line react-hooks/set-state-in-effect -- sync mock data for guest mode
      setSelectedTeam(MOCK_NFL_ANALYSIS.rankings[0]);
      return;
    }
    setLoading(true);
    fetchNFLTeamAnalysis()
      .then(result => {
        setData(result);
        if (result.rankings.length > 0) setSelectedTeam(result.rankings[0]);
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false));
  }, [isGuest]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="w-8 h-8 border-4 border-brand-600 border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-4 sm:p-8 max-w-6xl mx-auto">
        <div className="p-4 bg-red-50 border border-red-200 rounded-xl">
          <p className="text-sm text-red-700">{error}</p>
        </div>
      </div>
    );
  }

  if (!data) return null;

  const best = data.rankings[0];
  const topSeasons = data.seasons
    .sort((a, b) => b.dominance_score - a.dominance_score)
    .slice(0, 10);

  // Radar data for the selected team's pillar breakdown
  const selectedSeasons = selectedTeam
    ? data.seasons.filter(s => s.team === selectedTeam.team)
    : [];
  const avgPillars = selectedSeasons.length > 0
    ? Object.keys(PILLAR_LABELS).map(key => ({
        pillar: PILLAR_LABELS[key],
        value: Math.round(
          (selectedSeasons.reduce((sum, s) => sum + (s[key as keyof typeof s] as number), 0) /
            selectedSeasons.length) * 100
        ) / 100,
      }))
    : [];

  // Normalize radar for display (shift so min is 0-ish)
  const radarMin = Math.min(...avgPillars.map(p => p.value), 0);
  const radarData = avgPillars.map(p => ({
    ...p,
    normalized: Math.round((p.value - radarMin + 0.5) * 100) / 100,
  }));

  return (
    <div className="p-4 sm:p-8 max-w-6xl mx-auto">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-xl sm:text-2xl font-bold text-gray-900">NFL Team Analysis</h1>
        <p className="text-gray-500 mt-1 text-sm">
          Composite dominance scoring across {data.rankings.length} teams &middot; Era-adjusted z-scores
        </p>
      </div>

      {isGuest && (
        <div className="p-4 mb-6 bg-amber-50 border border-amber-200 rounded-xl">
          <p className="text-sm font-medium text-amber-800">
            You're viewing demo data. Sign in with Google to see live analysis from your pipeline.
          </p>
        </div>
      )}

      {/* Best Team Hero Card */}
      <div className="mb-8 bg-gradient-to-r from-brand-600 to-brand-700 rounded-2xl p-6 text-white">
        <p className="text-xs font-medium text-brand-200 uppercase tracking-wider mb-1">Best Overall Team (2003-2023)</p>
        <h2 className="text-2xl sm:text-3xl font-bold mb-4">{best.team}</h2>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
          <div>
            <p className="text-xs text-brand-200">Composite Score</p>
            <p className="text-lg font-bold">{best.composite_score.toFixed(3)}</p>
          </div>
          <div>
            <p className="text-xs text-brand-200">Avg Win %</p>
            <p className="text-lg font-bold">{(best.avg_win_pct * 100).toFixed(1)}%</p>
          </div>
          <div>
            <p className="text-xs text-brand-200">Record</p>
            <p className="text-lg font-bold">{best.total_wins}-{best.total_losses}</p>
          </div>
          <div>
            <p className="text-xs text-brand-200">Best Season</p>
            <p className="text-lg font-bold">{best.best_season_year}</p>
          </div>
        </div>
      </div>

      {/* Rankings Table + Team Detail */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
        {/* Rankings List */}
        <div className="lg:col-span-2 bg-white border border-gray-200 rounded-xl overflow-hidden">
          <div className="px-5 py-4 border-b border-gray-100">
            <h3 className="text-sm font-semibold text-gray-700">All-Time Dominance Rankings</h3>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  <th className="px-4 py-3">#</th>
                  <th className="px-4 py-3">Team</th>
                  <th className="px-4 py-3 text-right">Score</th>
                  <th className="px-4 py-3 text-right">Win %</th>
                  <th className="px-4 py-3 text-right hidden sm:table-cell">W-L</th>
                  <th className="px-4 py-3 text-right hidden md:table-cell">Peak</th>
                  <th className="px-4 py-3 text-right hidden md:table-cell">Best Year</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {data.rankings.map(team => (
                  <tr
                    key={team.team}
                    onClick={() => setSelectedTeam(team)}
                    className={`cursor-pointer transition hover:bg-brand-50 ${
                      selectedTeam?.team === team.team ? 'bg-brand-50' : ''
                    }`}
                  >
                    <td className="px-4 py-3 font-semibold text-gray-400">{team.rank}</td>
                    <td className="px-4 py-3 font-medium text-gray-900">{team.team}</td>
                    <td className="px-4 py-3 text-right font-mono text-gray-700">{team.composite_score.toFixed(3)}</td>
                    <td className="px-4 py-3 text-right text-gray-600">{(team.avg_win_pct * 100).toFixed(1)}%</td>
                    <td className="px-4 py-3 text-right text-gray-500 hidden sm:table-cell">{team.total_wins}-{team.total_losses}</td>
                    <td className="px-4 py-3 text-right font-mono text-gray-500 hidden md:table-cell">{team.peak_dominance.toFixed(3)}</td>
                    <td className="px-4 py-3 text-right text-gray-500 hidden md:table-cell">{team.best_season_year}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Selected Team Radar */}
        <div className="bg-white border border-gray-200 rounded-xl p-5">
          <h3 className="text-sm font-semibold text-gray-700 mb-1">
            {selectedTeam?.team ?? 'Select a team'}
          </h3>
          <p className="text-xs text-gray-400 mb-4">Average pillar scores across all seasons</p>
          {radarData.length > 0 && (
            <ResponsiveContainer width="100%" height={250}>
              <RadarChart data={radarData}>
                <PolarGrid stroke="#e5e7eb" />
                <PolarAngleAxis dataKey="pillar" tick={{ fontSize: 12, fill: '#6b7280' }} />
                <PolarRadiusAxis tick={false} axisLine={false} />
                <Radar
                  name={selectedTeam?.team}
                  dataKey="normalized"
                  stroke="#3b82f6"
                  fill="#3b82f6"
                  fillOpacity={0.2}
                />
                <Tooltip formatter={(v: number) => v.toFixed(2)} />
              </RadarChart>
            </ResponsiveContainer>
          )}
          {selectedTeam && (
            <div className="grid grid-cols-2 gap-3 mt-4">
              <div>
                <p className="text-xs text-gray-400">Pts Scored/G</p>
                <p className="text-sm font-semibold">{selectedTeam.avg_points_per_game.toFixed(1)}</p>
              </div>
              <div>
                <p className="text-xs text-gray-400">Pts Allowed/G</p>
                <p className="text-sm font-semibold">{selectedTeam.avg_points_opp_per_game.toFixed(1)}</p>
              </div>
              <div>
                <p className="text-xs text-gray-400">Elite Seasons</p>
                <p className="text-sm font-semibold">{selectedTeam.seasons_top_8}</p>
              </div>
              <div>
                <p className="text-xs text-gray-400">Peak Score</p>
                <p className="text-sm font-semibold">{selectedTeam.peak_dominance.toFixed(3)}</p>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Top Seasons Chart */}
      <div className="bg-white border border-gray-200 rounded-xl p-5 mb-8">
        <h3 className="text-sm font-semibold text-gray-700 mb-1">Top 10 Individual Seasons</h3>
        <p className="text-xs text-gray-400 mb-4">Highest single-season dominance scores with pillar breakdown</p>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart
            data={topSeasons.map(s => ({
              label: `${s.team.split(' ').pop()} '${String(s.year).slice(2)}`,
              Winning: Math.round(s.pillar_winning * 100) / 100,
              Offence: Math.round(s.pillar_offence * 100) / 100,
              Defence: Math.round(s.pillar_defence * 100) / 100,
              Efficiency: Math.round(s.pillar_efficiency * 100) / 100,
            }))}
            layout="vertical"
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
            <XAxis type="number" tick={{ fontSize: 12 }} />
            <YAxis dataKey="label" type="category" width={110} tick={{ fontSize: 11 }} />
            <Tooltip />
            <Legend />
            <Bar dataKey="Winning" stackId="a" fill={COLORS[0]} />
            <Bar dataKey="Offence" stackId="a" fill={COLORS[1]} />
            <Bar dataKey="Defence" stackId="a" fill={COLORS[2]} />
            <Bar dataKey="Efficiency" stackId="a" fill={COLORS[3]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Dominance Drivers */}
      <div className="bg-white border border-gray-200 rounded-xl p-5 mb-8">
        <h3 className="text-sm font-semibold text-gray-700 mb-1">What Makes a Team Elite?</h3>
        <p className="text-xs text-gray-400 mb-4">
          Feature importance from GradientBoosting classifier &mdash; which stats most separate elite teams from the rest
        </p>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart
            data={data.drivers.slice(0, 10).map(d => ({
              feature: formatFeature(d.feature_name),
              importance: Math.round(d.importance * 10000) / 100,
            }))}
            layout="vertical"
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
            <XAxis type="number" tick={{ fontSize: 12 }} unit="%" />
            <YAxis dataKey="feature" type="category" width={160} tick={{ fontSize: 11 }} />
            <Tooltip formatter={(v: number) => `${v.toFixed(1)}%`} />
            <Bar dataKey="importance" fill="#3b82f6" radius={[0, 4, 4, 0]} />
          </BarChart>
        </ResponsiveContainer>

        {/* Driver details table */}
        <div className="mt-6 overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                <th className="px-3 py-2">#</th>
                <th className="px-3 py-2">Feature</th>
                <th className="px-3 py-2 text-right">Importance</th>
                <th className="px-3 py-2 text-right">Elite Avg</th>
                <th className="px-3 py-2 text-right">League Avg</th>
                <th className="px-3 py-2 text-right">Advantage</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {data.drivers.slice(0, 10).map(d => (
                <tr key={d.feature_name} className="hover:bg-gray-50">
                  <td className="px-3 py-2 text-gray-400">{d.rank}</td>
                  <td className="px-3 py-2 font-medium text-gray-700">{formatFeature(d.feature_name)}</td>
                  <td className="px-3 py-2 text-right font-mono text-gray-600">
                    {(d.importance * 100).toFixed(1)}%
                  </td>
                  <td className="px-3 py-2 text-right text-gray-600">{d.elite_mean.toFixed(2)}</td>
                  <td className="px-3 py-2 text-right text-gray-500">{d.league_mean.toFixed(2)}</td>
                  <td className={`px-3 py-2 text-right font-medium ${
                    d.elite_advantage > 0 ? 'text-green-600' : 'text-red-600'
                  }`}>
                    {d.elite_advantage > 0 ? '+' : ''}{d.elite_advantage.toFixed(2)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
