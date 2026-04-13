import { useState } from 'react';
import type { NRLTeamRanking } from '../../types/nrlTeams';

interface Props {
  rankings: NRLTeamRanking[];
  onSelectTeam?: (team: NRLTeamRanking) => void;
  selectedTeam?: string | null;
}

// Field dimensions (proportional to real NRL field 100m x 68m)
const FIELD_W = 680;
const FIELD_H = 440;
const PAD = 40;
const INGOAL = 50; // in-goal area depth
const FIELD_LEFT = PAD;
const FIELD_RIGHT = FIELD_W - PAD;
const FIELD_TOP = PAD;
const FIELD_BOTTOM = FIELD_H - PAD;
const PLAY_W = FIELD_RIGHT - FIELD_LEFT;
const PLAY_H = FIELD_BOTTOM - FIELD_TOP;

function fieldLine(x1: number, y1: number, x2: number, y2: number, dashed = false) {
  return (
    <line
      x1={x1} y1={y1} x2={x2} y2={y2}
      stroke="rgba(255,255,255,0.5)" strokeWidth={1.5}
      strokeDasharray={dashed ? '6,4' : undefined}
    />
  );
}

function shortName(team: string): string {
  const map: Record<string, string> = {
    'Melbourne Storm': 'STO',
    'Sydney Roosters': 'ROO',
    'Penrith Panthers': 'PAN',
    'Brisbane Broncos': 'BRO',
    'Manly Sea Eagles': 'MAN',
    'Canterbury Bulldogs': 'BUL',
    'Cronulla Sharks': 'SHA',
    'South Sydney Rabbitohs': 'SOU',
    'Parramatta Eels': 'PAR',
    'North Queensland Cowboys': 'COW',
    'Canberra Raiders': 'RAI',
    'St George Illawarra Dragons': 'DRA',
    'Wests Tigers': 'TIG',
    'Newcastle Knights': 'KNI',
    'Gold Coast Titans': 'TIT',
    'New Zealand Warriors': 'WAR',
    'Dolphins': 'DOL',
  };
  return map[team] || team.split(' ').pop()?.substring(0, 3).toUpperCase() || '???';
}

const ARCHETYPE_COLORS: Record<string, string> = {
  dynasty: '#eab308',
  contender: '#3b82f6',
  competitive: '#22c55e',
  rebuilding: '#9ca3af',
  expansion: '#a855f7',
  cellar: '#ef4444',
};

export default function NRLFieldViz({ rankings, onSelectTeam, selectedTeam }: Props) {
  const [hoveredTeam, setHoveredTeam] = useState<string | null>(null);

  if (rankings.length === 0) return null;

  // Map teams to field positions
  // Y-axis: composite_score → field length (best teams near the try line at top)
  // X-axis: attack_defense_ratio (mean_avg_points_for / mean_avg_points_against) → field width
  const scores = rankings.map(t => t.composite_score);
  const minScore = Math.min(...scores);
  const maxScore = Math.max(...scores);
  const scoreRange = maxScore - minScore || 1;

  const ratios = rankings.map(t => t.mean_avg_points_for / (t.mean_avg_points_against || 1));
  const minRatio = Math.min(...ratios);
  const maxRatio = Math.max(...ratios);
  const ratioRange = maxRatio - minRatio || 1;

  const teamPositions = rankings.map((team, i) => {
    const ratio = team.mean_avg_points_for / (team.mean_avg_points_against || 1);
    // Y: higher score = closer to top try line (lower y value)
    const yNorm = 1 - (team.composite_score - minScore) / scoreRange;
    const y = FIELD_TOP + INGOAL + yNorm * (PLAY_H - 2 * INGOAL) + 10;
    // X: ratio maps across field width
    const xNorm = (ratio - minRatio) / ratioRange;
    const x = FIELD_LEFT + 30 + xNorm * (PLAY_W - 60);

    return { team, x, y, rank: i + 1 };
  });

  const hovered = hoveredTeam ? rankings.find(t => t.team === hoveredTeam) : null;

  return (
    <div className="bg-white border border-gray-200 rounded-xl p-5">
      <h3 className="text-sm font-semibold text-gray-700 mb-1">Team Landscape</h3>
      <p className="text-xs text-gray-400 mb-3">
        Teams positioned by dominance (top = strongest) and attack/defense ratio (right = more attacking)
      </p>
      <svg viewBox={`0 0 ${FIELD_W} ${FIELD_H}`} className="w-full" style={{ maxHeight: 420 }}>
        {/* Field background */}
        <rect x={FIELD_LEFT} y={FIELD_TOP} width={PLAY_W} height={PLAY_H} rx={4} fill="#15803d" />

        {/* In-goal areas */}
        <rect x={FIELD_LEFT} y={FIELD_TOP} width={PLAY_W} height={INGOAL} fill="#166534" rx={4} />
        <rect x={FIELD_LEFT} y={FIELD_BOTTOM - INGOAL} width={PLAY_W} height={INGOAL} fill="#166534" />

        {/* Field lines */}
        {/* Try lines */}
        {fieldLine(FIELD_LEFT, FIELD_TOP + INGOAL, FIELD_RIGHT, FIELD_TOP + INGOAL)}
        {fieldLine(FIELD_LEFT, FIELD_BOTTOM - INGOAL, FIELD_RIGHT, FIELD_BOTTOM - INGOAL)}

        {/* 10m lines from each try line */}
        {fieldLine(FIELD_LEFT, FIELD_TOP + INGOAL + (PLAY_H - 2 * INGOAL) * 0.143, FIELD_RIGHT, FIELD_TOP + INGOAL + (PLAY_H - 2 * INGOAL) * 0.143, true)}
        {fieldLine(FIELD_LEFT, FIELD_BOTTOM - INGOAL - (PLAY_H - 2 * INGOAL) * 0.143, FIELD_RIGHT, FIELD_BOTTOM - INGOAL - (PLAY_H - 2 * INGOAL) * 0.143, true)}

        {/* 20m lines */}
        {fieldLine(FIELD_LEFT, FIELD_TOP + INGOAL + (PLAY_H - 2 * INGOAL) * 0.286, FIELD_RIGHT, FIELD_TOP + INGOAL + (PLAY_H - 2 * INGOAL) * 0.286, true)}
        {fieldLine(FIELD_LEFT, FIELD_BOTTOM - INGOAL - (PLAY_H - 2 * INGOAL) * 0.286, FIELD_RIGHT, FIELD_BOTTOM - INGOAL - (PLAY_H - 2 * INGOAL) * 0.286, true)}

        {/* 30m lines */}
        {fieldLine(FIELD_LEFT, FIELD_TOP + INGOAL + (PLAY_H - 2 * INGOAL) * 0.429, FIELD_RIGHT, FIELD_TOP + INGOAL + (PLAY_H - 2 * INGOAL) * 0.429, true)}
        {fieldLine(FIELD_LEFT, FIELD_BOTTOM - INGOAL - (PLAY_H - 2 * INGOAL) * 0.429, FIELD_RIGHT, FIELD_BOTTOM - INGOAL - (PLAY_H - 2 * INGOAL) * 0.429, true)}

        {/* Halfway */}
        {fieldLine(FIELD_LEFT, FIELD_TOP + PLAY_H / 2, FIELD_RIGHT, FIELD_TOP + PLAY_H / 2)}

        {/* Centre circle */}
        <circle cx={FIELD_LEFT + PLAY_W / 2} cy={FIELD_TOP + PLAY_H / 2} r={20} fill="none" stroke="rgba(255,255,255,0.3)" strokeWidth={1.5} />

        {/* Zone labels */}
        <text x={FIELD_LEFT + PLAY_W / 2} y={FIELD_TOP + 30} textAnchor="middle" fill="rgba(255,255,255,0.35)" fontSize={11} fontWeight={600}>TRY ZONE</text>
        <text x={FIELD_LEFT + PLAY_W / 2} y={FIELD_BOTTOM - 18} textAnchor="middle" fill="rgba(255,255,255,0.35)" fontSize={11} fontWeight={600}>OWN HALF</text>

        {/* Axis labels */}
        <text x={FIELD_LEFT + 8} y={FIELD_TOP + PLAY_H / 2} textAnchor="middle" fill="rgba(255,255,255,0.3)" fontSize={9} transform={`rotate(-90, ${FIELD_LEFT + 8}, ${FIELD_TOP + PLAY_H / 2})`}>
          DEFENSIVE
        </text>
        <text x={FIELD_RIGHT - 8} y={FIELD_TOP + PLAY_H / 2} textAnchor="middle" fill="rgba(255,255,255,0.3)" fontSize={9} transform={`rotate(90, ${FIELD_RIGHT - 8}, ${FIELD_TOP + PLAY_H / 2})`}>
          ATTACKING
        </text>

        {/* Team markers */}
        {teamPositions.map(({ team: t, x, y }) => {
          const isSelected = selectedTeam === t.team;
          const isHovered = hoveredTeam === t.team;
          const color = ARCHETYPE_COLORS[t.archetype_label] || '#6b7280';
          const r = isSelected || isHovered ? 18 : 14;

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
              <text x={x} y={y + 1} textAnchor="middle" dominantBaseline="middle" fill="#fff" fontSize={isSelected || isHovered ? 9 : 8} fontWeight={700}>
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
              <rect x={tx - 100} y={ty - 8} width={200} height={48} rx={6} fill="rgba(0,0,0,0.85)" />
              <text x={tx} y={ty + 8} textAnchor="middle" fill="#fff" fontSize={11} fontWeight={600}>
                #{hovered.rank} {hovered.team}
              </text>
              <text x={tx} y={ty + 24} textAnchor="middle" fill="rgba(255,255,255,0.7)" fontSize={10}>
                Score: {hovered.composite_score.toFixed(3)} | Win: {(hovered.mean_win_rate * 100).toFixed(0)}% | {hovered.archetype_label}
              </text>
            </g>
          );
        })()}
      </svg>

      {/* Legend */}
      <div className="flex gap-3 mt-3 flex-wrap justify-center">
        {Object.entries(ARCHETYPE_COLORS).map(([label, color]) => (
          <div key={label} className="flex items-center gap-1.5">
            <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: color }} />
            <span className="text-xs text-gray-500 capitalize">{label}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
