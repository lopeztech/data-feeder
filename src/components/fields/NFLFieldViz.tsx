import { useState } from 'react';
import type { NFLTeamRanking } from '../../types/nflTeams';

interface Props {
  rankings: NFLTeamRanking[];
  onSelectTeam?: (team: NFLTeamRanking) => void;
  selectedTeam?: string | null;
}

const FIELD_W = 700;
const FIELD_H = 380;
const PAD = 36;
const ENDZONE_W = 50;
const FIELD_LEFT = PAD;
const FIELD_RIGHT = FIELD_W - PAD;
const FIELD_TOP = PAD;
const FIELD_BOTTOM = FIELD_H - PAD;
const PLAY_W = FIELD_RIGHT - FIELD_LEFT;
const PLAY_H = FIELD_BOTTOM - FIELD_TOP;

function shortName(team: string): string {
  const map: Record<string, string> = {
    'New England Patriots': 'NE',
    'Kansas City Chiefs': 'KC',
    'Green Bay Packers': 'GB',
    'Pittsburgh Steelers': 'PIT',
    'Baltimore Ravens': 'BAL',
    'Indianapolis Colts': 'IND',
    'Seattle Seahawks': 'SEA',
    'Philadelphia Eagles': 'PHI',
    'San Francisco 49ers': 'SF',
    'Denver Broncos': 'DEN',
    'Dallas Cowboys': 'DAL',
    'New Orleans Saints': 'NO',
    'Minnesota Vikings': 'MIN',
    'Buffalo Bills': 'BUF',
    'Tampa Bay Buccaneers': 'TB',
    'Los Angeles Rams': 'LAR',
    'Tennessee Titans': 'TEN',
    'Cincinnati Bengals': 'CIN',
    'Miami Dolphins': 'MIA',
    'Cleveland Browns': 'CLE',
    'Chicago Bears': 'CHI',
    'Las Vegas Raiders': 'LV',
    'Los Angeles Chargers': 'LAC',
    'Arizona Cardinals': 'ARI',
    'Atlanta Falcons': 'ATL',
    'Washington Commanders': 'WSH',
    'Carolina Panthers': 'CAR',
    'Jacksonville Jaguars': 'JAX',
    'New York Giants': 'NYG',
    'New York Jets': 'NYJ',
    'Detroit Lions': 'DET',
    'Houston Texans': 'HOU',
  };
  return map[team] || team.split(' ').pop()?.substring(0, 3).toUpperCase() || '???';
}

export default function NFLFieldViz({ rankings, onSelectTeam, selectedTeam }: Props) {
  const [hoveredTeam, setHoveredTeam] = useState<string | null>(null);

  if (rankings.length === 0) return null;

  // Map teams to field positions
  // X-axis: composite_score → field length (best teams near opponent's end zone at right)
  // Y-axis: offensive vs defensive balance (points_per_game - points_opp_per_game)
  const scores = rankings.map(t => t.composite_score);
  const minScore = Math.min(...scores);
  const maxScore = Math.max(...scores);
  const scoreRange = maxScore - minScore || 1;

  const diffs = rankings.map(t => t.avg_points_per_game - t.avg_points_opp_per_game);
  const minDiff = Math.min(...diffs);
  const maxDiff = Math.max(...diffs);
  const diffRange = maxDiff - minDiff || 1;

  const playableLeft = FIELD_LEFT + ENDZONE_W;
  const playableRight = FIELD_RIGHT - ENDZONE_W;
  const playableW = playableRight - playableLeft;

  const teamPositions = rankings.map((team) => {
    const diff = team.avg_points_per_game - team.avg_points_opp_per_game;
    // X: higher score = closer to right end zone
    const xNorm = (team.composite_score - minScore) / scoreRange;
    const x = playableLeft + 20 + xNorm * (playableW - 40);
    // Y: point differential maps to field height (higher diff = higher on field)
    const yNorm = 1 - (diff - minDiff) / diffRange;
    const y = FIELD_TOP + 20 + yNorm * (PLAY_H - 40);

    return { team, x, y };
  });

  const hovered = hoveredTeam ? rankings.find(t => t.team === hoveredTeam) : null;

  // Yard line positions (every 10 yards across the playable area)
  const yardLines = Array.from({ length: 11 }, (_, i) => ({
    x: playableLeft + (i / 10) * playableW,
    label: i <= 5 ? `${i * 10}` : `${(10 - i) * 10}`,
  }));

  return (
    <div className="bg-white border border-gray-200 rounded-xl p-5">
      <h3 className="text-sm font-semibold text-gray-700 mb-1">Team Landscape</h3>
      <p className="text-xs text-gray-400 mb-3">
        Teams positioned by dominance (right = strongest) and point differential (top = more offensive)
      </p>
      <svg viewBox={`0 0 ${FIELD_W} ${FIELD_H}`} className="w-full" style={{ maxHeight: 380 }}>
        {/* Field background */}
        <rect x={FIELD_LEFT} y={FIELD_TOP} width={PLAY_W} height={PLAY_H} rx={4} fill="#15803d" />

        {/* End zones */}
        <rect x={FIELD_LEFT} y={FIELD_TOP} width={ENDZONE_W} height={PLAY_H} fill="#166534" rx={4} />
        <rect x={FIELD_RIGHT - ENDZONE_W} y={FIELD_TOP} width={ENDZONE_W} height={PLAY_H} fill="#991b1b" rx={4} />

        {/* End zone text */}
        <text x={FIELD_LEFT + ENDZONE_W / 2} y={FIELD_TOP + PLAY_H / 2} textAnchor="middle" dominantBaseline="middle" fill="rgba(255,255,255,0.3)" fontSize={14} fontWeight={700} transform={`rotate(-90, ${FIELD_LEFT + ENDZONE_W / 2}, ${FIELD_TOP + PLAY_H / 2})`}>
          OWN ZONE
        </text>
        <text x={FIELD_RIGHT - ENDZONE_W / 2} y={FIELD_TOP + PLAY_H / 2} textAnchor="middle" dominantBaseline="middle" fill="rgba(255,255,255,0.3)" fontSize={14} fontWeight={700} transform={`rotate(90, ${FIELD_RIGHT - ENDZONE_W / 2}, ${FIELD_TOP + PLAY_H / 2})`}>
          ENDZONE
        </text>

        {/* Yard lines */}
        {yardLines.map(({ x, label }, i) => (
          <g key={i}>
            <line x1={x} y1={FIELD_TOP} x2={x} y2={FIELD_BOTTOM} stroke="rgba(255,255,255,0.25)" strokeWidth={1} />
            {i > 0 && i < 10 && (
              <text x={x} y={FIELD_BOTTOM - 8} textAnchor="middle" fill="rgba(255,255,255,0.3)" fontSize={10} fontWeight={600}>
                {label}
              </text>
            )}
          </g>
        ))}

        {/* Hash marks */}
        {yardLines.map(({ x }, i) => (
          <g key={`hash-${i}`}>
            <line x1={x - 4} y1={FIELD_TOP + PLAY_H * 0.33} x2={x + 4} y2={FIELD_TOP + PLAY_H * 0.33} stroke="rgba(255,255,255,0.2)" strokeWidth={1} />
            <line x1={x - 4} y1={FIELD_TOP + PLAY_H * 0.67} x2={x + 4} y2={FIELD_TOP + PLAY_H * 0.67} stroke="rgba(255,255,255,0.2)" strokeWidth={1} />
          </g>
        ))}

        {/* Axis hint labels */}
        <text x={playableLeft + playableW / 2} y={FIELD_TOP + 14} textAnchor="middle" fill="rgba(255,255,255,0.25)" fontSize={9}>
          OFFENSIVE BALANCE
        </text>
        <text x={playableLeft + playableW / 2} y={FIELD_BOTTOM - 2} textAnchor="middle" fill="rgba(255,255,255,0.25)" fontSize={9}>
          DEFENSIVE BALANCE
        </text>

        {/* Team markers */}
        {teamPositions.map(({ team: t, x, y }) => {
          const isSelected = selectedTeam === t.team;
          const isHovered = hoveredTeam === t.team;
          const r = isSelected || isHovered ? 16 : 12;
          const color = isSelected ? '#f59e0b' : '#3b82f6';

          return (
            <g
              key={t.team}
              style={{ cursor: 'pointer' }}
              onClick={() => onSelectTeam?.(t)}
              onMouseEnter={() => setHoveredTeam(t.team)}
              onMouseLeave={() => setHoveredTeam(null)}
            >
              <circle cx={x} cy={y} r={r + 2} fill="rgba(0,0,0,0.3)" />
              <circle cx={x} cy={y} r={r} fill={color} stroke={isSelected ? '#fff' : 'rgba(255,255,255,0.4)'} strokeWidth={isSelected ? 2.5 : 1} />
              <text x={x} y={y + 1} textAnchor="middle" dominantBaseline="middle" fill="#fff" fontSize={isSelected || isHovered ? 9 : 7.5} fontWeight={700}>
                {shortName(t.team)}
              </text>
            </g>
          );
        })}

        {/* Hover tooltip */}
        {hovered && (() => {
          const pos = teamPositions.find(p => p.team.team === hovered.team);
          if (!pos) return null;
          const tx = Math.min(Math.max(pos.x, 120), FIELD_W - 120);
          const ty = pos.y > FIELD_H / 2 ? pos.y - 55 : pos.y + 30;
          return (
            <g>
              <rect x={tx - 110} y={ty - 8} width={220} height={48} rx={6} fill="rgba(0,0,0,0.85)" />
              <text x={tx} y={ty + 8} textAnchor="middle" fill="#fff" fontSize={11} fontWeight={600}>
                #{hovered.rank} {hovered.team}
              </text>
              <text x={tx} y={ty + 24} textAnchor="middle" fill="rgba(255,255,255,0.7)" fontSize={10}>
                Score: {hovered.composite_score.toFixed(3)} | Win: {(hovered.avg_win_pct * 100).toFixed(0)}% | PPG: {hovered.avg_points_per_game.toFixed(1)}
              </text>
            </g>
          );
        })()}
      </svg>
    </div>
  );
}
