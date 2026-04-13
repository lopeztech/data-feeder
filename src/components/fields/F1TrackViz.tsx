import { useState } from 'react';

interface Constructor {
  rank: number;
  constructor: string;
  composite_score: number;
  archetype_label: string;
  mean_win_rate?: number;
  total_wins?: number;
  seasons_active?: number;
}

interface Props {
  constructors: Constructor[];
  onSelectConstructor?: (c: Constructor) => void;
  selectedConstructor?: string | null;
}

const TRACK_W = 700;
const TRACK_H = 420;
const CX = TRACK_W / 2;
const CY = TRACK_H / 2 + 10;

// Track path: a stylised circuit with straights and curves (Melbourne-inspired layout)
function getTrackPath(): string {
  // Outer boundary of a simplified circuit
  return `
    M 130,100
    L 400,80
    Q 500,80 540,130
    L 580,200
    Q 600,240 580,280
    L 520,340
    Q 480,370 420,360
    L 200,350
    Q 140,340 120,290
    L 100,200
    Q 90,150 130,100
    Z
  `;
}

// Points along the track for positioning constructors
function getTrackPoints(count: number): { x: number; y: number; angle: number }[] {
  // Define waypoints around the circuit
  const waypoints = [
    { x: 160, y: 98 },    // Start/finish straight
    { x: 280, y: 86 },
    { x: 400, y: 82 },
    { x: 480, y: 95 },    // Turn 1
    { x: 540, y: 140 },   // Turn 2
    { x: 570, y: 200 },   // Back straight
    { x: 575, y: 260 },
    { x: 540, y: 320 },   // Turn 3
    { x: 480, y: 355 },   // Turn 4
    { x: 400, y: 358 },
    { x: 300, y: 352 },   // Chicane area
    { x: 200, y: 348 },
    { x: 150, y: 320 },   // Turn 5
    { x: 120, y: 270 },
    { x: 108, y: 210 },   // Final turns
    { x: 110, y: 150 },
  ];

  // Interpolate to get `count` evenly spaced points
  const totalWaypoints = waypoints.length;
  const points: { x: number; y: number; angle: number }[] = [];

  for (let i = 0; i < count; i++) {
    const t = (i / count) * totalWaypoints;
    const idx = Math.floor(t);
    const frac = t - idx;
    const p1 = waypoints[idx % totalWaypoints];
    const p2 = waypoints[(idx + 1) % totalWaypoints];
    const x = p1.x + (p2.x - p1.x) * frac;
    const y = p1.y + (p2.y - p1.y) * frac;
    const dx = p2.x - p1.x;
    const dy = p2.y - p1.y;
    const angle = Math.atan2(dy, dx) * (180 / Math.PI);
    points.push({ x, y, angle });
  }

  return points;
}

function shortName(name: string): string {
  const map: Record<string, string> = {
    'Ferrari': 'FER',
    'McLaren': 'MCL',
    'Mercedes': 'MER',
    'Red Bull': 'RBR',
    'Williams': 'WIL',
    'Renault': 'REN',
    'Alpine': 'ALP',
    'Aston Martin': 'AMR',
    'Alfa Romeo': 'ALF',
    'AlphaTauri': 'AT',
    'Haas F1 Team': 'HAS',
    'Racing Point': 'RP',
    'Toro Rosso': 'STR',
    'Force India': 'FI',
    'Sauber': 'SAU',
    'Lotus': 'LOT',
    'Manor': 'MNR',
    'Caterham': 'CAT',
  };
  return map[name] || name.substring(0, 3).toUpperCase();
}

const ARCHETYPE_COLORS: Record<string, string> = {
  dominant: '#eab308',
  competitive: '#3b82f6',
  midfield: '#22c55e',
  backmarker: '#9ca3af',
  occasional: '#a855f7',
};

export default function F1TrackViz({ constructors, onSelectConstructor, selectedConstructor }: Props) {
  const [hoveredConstructor, setHoveredConstructor] = useState<string | null>(null);

  if (constructors.length === 0) return null;

  const trackPoints = getTrackPoints(constructors.length);
  const hovered = hoveredConstructor ? constructors.find(c => c.constructor === hoveredConstructor) : null;

  return (
    <div className="bg-white border border-gray-200 rounded-xl p-5">
      <h3 className="text-sm font-semibold text-gray-700 mb-1">Constructor Grid</h3>
      <p className="text-xs text-gray-400 mb-3">
        Constructors positioned on track by all-time ranking. P1 leads from the start/finish line.
      </p>
      <svg viewBox={`0 0 ${TRACK_W} ${TRACK_H}`} className="w-full" style={{ maxHeight: 400 }}>
        {/* Track surface */}
        <defs>
          <linearGradient id="trackGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#374151" />
            <stop offset="100%" stopColor="#1f2937" />
          </linearGradient>
        </defs>

        {/* Outer track */}
        <path d={getTrackPath()} fill="#374151" stroke="#6b7280" strokeWidth={2} />

        {/* Inner track (creates the road surface) */}
        <path
          d={getTrackPath()}
          fill="none"
          stroke="#4b5563"
          strokeWidth={52}
          strokeLinejoin="round"
          opacity={0.6}
        />

        {/* Track centre line (racing line hint) */}
        <path
          d={getTrackPath()}
          fill="none"
          stroke="rgba(255,255,255,0.08)"
          strokeWidth={1}
          strokeDasharray="8,12"
        />

        {/* Start/finish line */}
        <line x1={160} y1={75} x2={160} y2={120} stroke="#fff" strokeWidth={3} />
        <rect x={148} y={68} width={24} height={8} rx={2} fill="#fff" />
        {/* Chequered pattern */}
        {[0, 1, 2, 3, 4, 5].map(i => (
          <rect key={i} x={148 + i * 4} y={68} width={4} height={4} fill={i % 2 === 0 ? '#000' : '#fff'} />
        ))}
        {[0, 1, 2, 3, 4, 5].map(i => (
          <rect key={`b${i}`} x={148 + i * 4} y={72} width={4} height={4} fill={i % 2 === 1 ? '#000' : '#fff'} />
        ))}

        {/* DRS zone indicator */}
        <line x1={220} y1={82} x2={380} y2={78} stroke="#22c55e" strokeWidth={2} opacity={0.4} />
        <text x={300} y={73} textAnchor="middle" fill="rgba(34,197,94,0.5)" fontSize={8} fontWeight={600}>DRS</text>

        {/* Pit lane */}
        <path d="M 180,120 Q 200,155 280,155 L 420,148 Q 470,145 490,125" fill="none" stroke="rgba(255,255,255,0.15)" strokeWidth={1.5} strokeDasharray="4,4" />
        <text x={330} y={168} textAnchor="middle" fill="rgba(255,255,255,0.2)" fontSize={9}>PIT LANE</text>

        {/* Constructor markers on track */}
        {constructors.map((c, i) => {
          const pt = trackPoints[i];
          if (!pt) return null;
          const isSelected = selectedConstructor === c.constructor;
          const isHovered = hoveredConstructor === c.constructor;
          const color = ARCHETYPE_COLORS[c.archetype_label] || '#6b7280';
          const r = isSelected || isHovered ? 16 : 12;

          return (
            <g
              key={c.constructor}
              style={{ cursor: 'pointer' }}
              onClick={() => onSelectConstructor?.(c)}
              onMouseEnter={() => setHoveredConstructor(c.constructor)}
              onMouseLeave={() => setHoveredConstructor(null)}
            >
              {/* Position number */}
              <text x={pt.x} y={pt.y - r - 4} textAnchor="middle" fill="rgba(255,255,255,0.5)" fontSize={8} fontWeight={600}>
                P{c.rank}
              </text>
              <circle cx={pt.x} cy={pt.y} r={r + 2} fill="rgba(0,0,0,0.4)" />
              <circle cx={pt.x} cy={pt.y} r={r} fill={color} stroke={isSelected ? '#fff' : 'rgba(255,255,255,0.3)'} strokeWidth={isSelected ? 2.5 : 1} />
              <text x={pt.x} y={pt.y + 1} textAnchor="middle" dominantBaseline="middle" fill="#fff" fontSize={isSelected || isHovered ? 9 : 7.5} fontWeight={700}>
                {shortName(c.constructor)}
              </text>
            </g>
          );
        })}

        {/* Hover tooltip */}
        {hovered && (() => {
          const idx = constructors.findIndex(c => c.constructor === hovered.constructor);
          const pt = trackPoints[idx];
          if (!pt) return null;
          const tx = Math.min(Math.max(pt.x, 130), TRACK_W - 130);
          const ty = pt.y > CY ? pt.y - 55 : pt.y + 35;
          return (
            <g>
              <rect x={tx - 110} y={ty - 8} width={220} height={48} rx={6} fill="rgba(0,0,0,0.9)" />
              <text x={tx} y={ty + 8} textAnchor="middle" fill="#fff" fontSize={11} fontWeight={600}>
                P{hovered.rank} {hovered.constructor}
              </text>
              <text x={tx} y={ty + 24} textAnchor="middle" fill="rgba(255,255,255,0.7)" fontSize={10}>
                Score: {hovered.composite_score.toFixed(3)} | {hovered.archetype_label}
                {hovered.total_wins != null ? ` | ${hovered.total_wins} wins` : ''}
              </text>
            </g>
          );
        })()}

        {/* Circuit name */}
        <text x={CX} y={CY + 5} textAnchor="middle" fill="rgba(255,255,255,0.12)" fontSize={28} fontWeight={700}>
          ALL-TIME GRID
        </text>
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
