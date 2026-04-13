import { useState, useEffect } from 'react';
import { useAuth } from '../context/AuthContext';
import { fetchNRLTeamAnalysis } from '../lib/nrlService';
import { MOCK_NRL_ANALYSIS } from '../data/mockNRLTeams';
import type { NRLTeamAnalysisData, NRLTeamRanking, NRLRivalry } from '../types/nrlTeams';
import NRLFieldViz from '../components/fields/NRLFieldViz';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
  ResponsiveContainer,
} from 'recharts';

const COLORS = ['#3b82f6', '#22c55e', '#a855f7', '#f97316', '#ec4899', '#06b6d4', '#f59e0b', '#ef4444'];

const ARCHETYPE_COLORS: Record<string, string> = {
  dynasty: 'bg-yellow-100 text-yellow-800',
  contender: 'bg-blue-100 text-blue-700',
  competitive: 'bg-green-100 text-green-700',
  rebuilding: 'bg-gray-100 text-gray-600',
  expansion: 'bg-purple-100 text-purple-700',
};

type Tab = 'rankings' | 'profiles' | 'rivalries' | 'trends';

export default function NRLTeamsPage() {
  const { user } = useAuth();
  const isGuest = user?.role === 'guest';
  const [data, setData] = useState<NRLTeamAnalysisData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedTeam, setSelectedTeam] = useState<NRLTeamRanking | null>(null);
  const [tab, setTab] = useState<Tab>('rankings');

  useEffect(() => {
    if (isGuest) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- sync mock data for guest mode
      setData(MOCK_NRL_ANALYSIS);
      // eslint-disable-next-line react-hooks/set-state-in-effect -- sync mock data for guest mode
      setSelectedTeam(MOCK_NRL_ANALYSIS.rankings[0]);
      return;
    }
    setLoading(true);
    fetchNRLTeamAnalysis()
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
  const selectedProfile = selectedTeam
    ? data.profiles.find(p => p.team === selectedTeam.team)
    : null;
  const selectedRivalries = selectedTeam
    ? data.rivalries.filter(r => r.team === selectedTeam.team).sort((a, b) => b.total_matches - a.total_matches)
    : [];
  const selectedTrends = selectedTeam
    ? data.trends.filter(t => t.team === selectedTeam.team).sort((a, b) => a.window_start - b.window_start)
    : [];

  // Radar data for selected team profile
  const radarFields = [
    { key: 'win_rate', label: 'Win Rate' },
    { key: 'close_game_win_rate', label: 'Clutch' },
    { key: 'bounce_back_rate', label: 'Bounce-back' },
    { key: 'streak_maintenance_rate', label: 'Streaks' },
    { key: 'attack_defense_ratio', label: 'Attack/Def' },
  ] as const;

  const radarData = selectedProfile
    ? radarFields.map(f => ({
        pillar: f.label,
        value: Math.round((selectedProfile[f.key] as number) * 100) / 100,
      }))
    : [];

  const topSeasons = data.seasons
    .sort((a, b) => b.win_rate - a.win_rate)
    .slice(0, 10);

  return (
    <div className="p-4 sm:p-8 max-w-6xl mx-auto">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-xl sm:text-2xl font-bold text-gray-900">NRL Team Analysis</h1>
        <p className="text-gray-500 mt-1 text-sm">
          Composite dominance scoring across {data.rankings.length} teams &middot; 1990&ndash;2025
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
        <p className="text-xs font-medium text-brand-200 uppercase tracking-wider mb-1">Best Overall Team (1990&ndash;2025)</p>
        <h2 className="text-2xl sm:text-3xl font-bold mb-1">{best.team}</h2>
        <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium bg-white/20 text-white mb-4`}>
          {best.archetype_label}
        </span>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
          <div>
            <p className="text-xs text-brand-200">Composite Score</p>
            <p className="text-lg font-bold">{best.composite_score.toFixed(3)}</p>
          </div>
          <div>
            <p className="text-xs text-brand-200">Win Rate</p>
            <p className="text-lg font-bold">{(best.mean_win_rate * 100).toFixed(1)}%</p>
          </div>
          <div>
            <p className="text-xs text-brand-200">Record</p>
            <p className="text-lg font-bold">{best.total_wins}W / {best.total_games}G</p>
          </div>
          <div>
            <p className="text-xs text-brand-200">Peak Season</p>
            <p className="text-lg font-bold">{best.peak_season}</p>
          </div>
        </div>
      </div>

      {/* Field Visualization */}
      <div className="mb-8">
        <NRLFieldViz
          rankings={data.rankings}
          selectedTeam={selectedTeam?.team}
          onSelectTeam={t => setSelectedTeam(t)}
        />
      </div>

      {/* Tab Navigation */}
      <div className="flex gap-2 mb-6 flex-wrap">
        {([
          ['rankings', 'Rankings'],
          ['profiles', 'Team Profiles'],
          ['rivalries', 'Rivalries'],
          ['trends', 'Trends'],
        ] as const).map(([key, label]) => (
          <button
            key={key}
            onClick={() => setTab(key)}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
              tab === key ? 'bg-brand-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {/* Rankings Tab */}
      {tab === 'rankings' && (
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
                    <th className="px-4 py-3 text-right hidden sm:table-cell">Avg Margin</th>
                    <th className="px-4 py-3 text-right hidden md:table-cell">Archetype</th>
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
                      <td className="px-4 py-3 text-right text-gray-600">{(team.mean_win_rate * 100).toFixed(1)}%</td>
                      <td className="px-4 py-3 text-right text-gray-500 hidden sm:table-cell">
                        <span className={team.mean_avg_margin >= 0 ? 'text-green-600' : 'text-red-600'}>
                          {team.mean_avg_margin >= 0 ? '+' : ''}{team.mean_avg_margin.toFixed(1)}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right hidden md:table-cell">
                        <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${ARCHETYPE_COLORS[team.archetype_label] || 'bg-gray-100 text-gray-600'}`}>
                          {team.archetype_label}
                        </span>
                      </td>
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
            <p className="text-xs text-gray-400 mb-4">Tactical profile</p>
            {radarData.length > 0 && (
              <ResponsiveContainer width="100%" height={250}>
                <RadarChart data={radarData}>
                  <PolarGrid stroke="#e5e7eb" />
                  <PolarAngleAxis dataKey="pillar" tick={{ fontSize: 11, fill: '#6b7280' }} />
                  <PolarRadiusAxis tick={false} axisLine={false} />
                  <Radar
                    name={selectedTeam?.team}
                    dataKey="value"
                    stroke="#3b82f6"
                    fill="#3b82f6"
                    fillOpacity={0.2}
                  />
                  <Tooltip formatter={(v: unknown) => Number(v).toFixed(3)} />
                </RadarChart>
              </ResponsiveContainer>
            )}
            {selectedTeam && (
              <div className="grid grid-cols-2 gap-3 mt-4">
                <div>
                  <p className="text-xs text-gray-400">Pts For/G</p>
                  <p className="text-sm font-semibold">{selectedTeam.mean_avg_points_for.toFixed(1)}</p>
                </div>
                <div>
                  <p className="text-xs text-gray-400">Pts Against/G</p>
                  <p className="text-sm font-semibold">{selectedTeam.mean_avg_points_against.toFixed(1)}</p>
                </div>
                <div>
                  <p className="text-xs text-gray-400">Modern Dominance</p>
                  <p className="text-sm font-semibold">{selectedTeam.modern_dominance.toFixed(3)}</p>
                </div>
                <div>
                  <p className="text-xs text-gray-400">Seasons Active</p>
                  <p className="text-sm font-semibold">{selectedTeam.seasons_active}</p>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Top Seasons Chart (shown on rankings tab) */}
      {tab === 'rankings' && (
        <div className="bg-white border border-gray-200 rounded-xl p-5 mb-8">
          <h3 className="text-sm font-semibold text-gray-700 mb-1">Top 10 Individual Seasons</h3>
          <p className="text-xs text-gray-400 mb-4">Highest single-season win rates with scoring breakdown</p>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart
              data={topSeasons.map(s => ({
                label: `${s.team.split(' ').pop()} '${String(s.year).slice(2)}`,
                'Win Rate': Math.round(s.win_rate * 1000) / 10,
                'Avg Margin': Math.round(s.avg_margin * 10) / 10,
              }))}
              layout="vertical"
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
              <XAxis type="number" tick={{ fontSize: 12 }} />
              <YAxis dataKey="label" type="category" width={110} tick={{ fontSize: 11 }} />
              <Tooltip />
              <Legend />
              <Bar dataKey="Win Rate" fill={COLORS[0]} />
              <Bar dataKey="Avg Margin" fill={COLORS[1]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Profiles Tab */}
      {tab === 'profiles' && (
        <div className="space-y-6">
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {data.profiles.map(p => (
              <div
                key={p.team}
                onClick={() => setSelectedTeam(data.rankings.find(r => r.team === p.team) ?? null)}
                className={`bg-white border rounded-xl p-5 cursor-pointer transition hover:border-brand-300 hover:shadow-md ${
                  selectedTeam?.team === p.team ? 'border-brand-400 shadow-md' : 'border-gray-200'
                }`}
              >
                <h4 className="font-semibold text-gray-900 text-sm mb-1">{p.team}</h4>
                <span className="inline-block px-2 py-0.5 rounded-full text-xs font-medium bg-brand-50 text-brand-700 mb-3">
                  {p.playing_style}
                </span>
                <div className="space-y-2 text-xs">
                  <div>
                    <span className="text-gray-400">Strengths: </span>
                    <span className="text-green-700 font-medium">{p.strengths}</span>
                  </div>
                  <div>
                    <span className="text-gray-400">Weaknesses: </span>
                    <span className="text-red-600 font-medium">{p.weaknesses}</span>
                  </div>
                  <div className="grid grid-cols-3 gap-2 pt-2 border-t border-gray-100">
                    <div>
                      <p className="text-gray-400">Close Win</p>
                      <p className="font-semibold text-gray-700">{(p.close_game_win_rate * 100).toFixed(0)}%</p>
                    </div>
                    <div>
                      <p className="text-gray-400">Bounce</p>
                      <p className="font-semibold text-gray-700">{(p.bounce_back_rate * 100).toFixed(0)}%</p>
                    </div>
                    <div>
                      <p className="text-gray-400">Home Dep.</p>
                      <p className="font-semibold text-gray-700">{(p.home_dependency * 100).toFixed(0)}%</p>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Rivalries Tab */}
      {tab === 'rivalries' && (
        <div className="space-y-6">
          {/* Team selector */}
          <div className="flex gap-2 flex-wrap">
            {data.rankings.map(t => (
              <button
                key={t.team}
                onClick={() => setSelectedTeam(t)}
                className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
                  selectedTeam?.team === t.team ? 'bg-brand-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                {t.team.split(' ').pop()}
              </button>
            ))}
          </div>

          {selectedRivalries.length > 0 && (
            <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
              <div className="px-5 py-4 border-b border-gray-100">
                <h3 className="text-sm font-semibold text-gray-700">
                  {selectedTeam?.team} Head-to-Head Records
                </h3>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      <th className="px-4 py-3">Opponent</th>
                      <th className="px-4 py-3 text-right">W-L</th>
                      <th className="px-4 py-3 text-right">Win %</th>
                      <th className="px-4 py-3 text-right">Avg Margin</th>
                      <th className="px-4 py-3 text-right hidden sm:table-cell">Home Win %</th>
                      <th className="px-4 py-3 text-right hidden sm:table-cell">Matches</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {selectedRivalries.map((r: NRLRivalry) => (
                      <tr key={r.opponent} className="hover:bg-gray-50">
                        <td className="px-4 py-3 font-medium text-gray-900">{r.opponent}</td>
                        <td className="px-4 py-3 text-right text-gray-600">{r.wins}-{r.losses}</td>
                        <td className="px-4 py-3 text-right">
                          <span className={r.win_rate >= 0.5 ? 'text-green-600 font-medium' : 'text-red-600'}>
                            {(r.win_rate * 100).toFixed(1)}%
                          </span>
                        </td>
                        <td className="px-4 py-3 text-right">
                          <span className={r.avg_margin >= 0 ? 'text-green-600' : 'text-red-600'}>
                            {r.avg_margin >= 0 ? '+' : ''}{r.avg_margin.toFixed(1)}
                          </span>
                        </td>
                        <td className="px-4 py-3 text-right text-gray-500 hidden sm:table-cell">
                          {r.home_win_rate ? (r.home_win_rate * 100).toFixed(0) + '%' : '-'}
                        </td>
                        <td className="px-4 py-3 text-right text-gray-400 hidden sm:table-cell">{r.total_matches}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Trends Tab */}
      {tab === 'trends' && (
        <div className="space-y-6">
          {/* Team selector */}
          <div className="flex gap-2 flex-wrap">
            {data.rankings.map(t => (
              <button
                key={t.team}
                onClick={() => setSelectedTeam(t)}
                className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
                  selectedTeam?.team === t.team ? 'bg-brand-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                }`}
              >
                {t.team.split(' ').pop()}
              </button>
            ))}
          </div>

          {selectedTrends.length > 0 && (
            <>
              <div className="bg-white border border-gray-200 rounded-xl p-5">
                <div className="flex items-center gap-3 mb-4">
                  <h3 className="text-sm font-semibold text-gray-700">
                    {selectedTeam?.team} — 5-Year Rolling Performance
                  </h3>
                  <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${
                    selectedTrends[selectedTrends.length - 1]?.trajectory === 'improving'
                      ? 'bg-green-100 text-green-700'
                      : selectedTrends[selectedTrends.length - 1]?.trajectory === 'declining'
                      ? 'bg-red-100 text-red-700'
                      : 'bg-gray-100 text-gray-600'
                  }`}>
                    {selectedTrends[selectedTrends.length - 1]?.trajectory}
                  </span>
                </div>
                <ResponsiveContainer width="100%" height={280}>
                  <BarChart data={selectedTrends.map(t => ({
                    window: `${t.window_start}-${String(t.window_end).slice(2)}`,
                    'Win Rate': Math.round(t.avg_win_rate * 1000) / 10,
                    'Close Game WR': Math.round(t.avg_close_game_wr * 1000) / 10,
                    'Bounce-back': Math.round(t.avg_bounce_back * 1000) / 10,
                  }))}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                    <XAxis dataKey="window" tick={{ fontSize: 11 }} />
                    <YAxis tick={{ fontSize: 12 }} unit="%" />
                    <Tooltip formatter={(v: unknown) => `${Number(v).toFixed(1)}%`} />
                    <Legend />
                    <Bar dataKey="Win Rate" fill={COLORS[0]} />
                    <Bar dataKey="Close Game WR" fill={COLORS[1]} />
                    <Bar dataKey="Bounce-back" fill={COLORS[2]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>

              {/* Trend table */}
              <div className="bg-white border border-gray-200 rounded-xl overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        <th className="px-4 py-3">Window</th>
                        <th className="px-4 py-3 text-right">Win Rate</th>
                        <th className="px-4 py-3 text-right">Avg Margin</th>
                        <th className="px-4 py-3 text-right hidden sm:table-cell">Close Game</th>
                        <th className="px-4 py-3 text-right hidden sm:table-cell">Bounce-back</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                      {selectedTrends.map(t => (
                        <tr key={`${t.window_start}-${t.window_end}`} className="hover:bg-gray-50">
                          <td className="px-4 py-3 font-medium text-gray-900">{t.window_start}&ndash;{t.window_end}</td>
                          <td className="px-4 py-3 text-right text-gray-600">{(t.avg_win_rate * 100).toFixed(1)}%</td>
                          <td className="px-4 py-3 text-right">
                            <span className={t.avg_margin >= 0 ? 'text-green-600' : 'text-red-600'}>
                              {t.avg_margin >= 0 ? '+' : ''}{t.avg_margin.toFixed(1)}
                            </span>
                          </td>
                          <td className="px-4 py-3 text-right text-gray-500 hidden sm:table-cell">{(t.avg_close_game_wr * 100).toFixed(1)}%</td>
                          <td className="px-4 py-3 text-right text-gray-500 hidden sm:table-cell">{(t.avg_bounce_back * 100).toFixed(1)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
