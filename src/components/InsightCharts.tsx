import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  PieChart, Pie, Cell,
  ScatterChart, Scatter,
  RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
  ResponsiveContainer,
} from 'recharts';
import type { ClusterSummary, AnomalyData, PredictionData, ProfileData } from '../lib/uploadService';

const COLORS = ['#3b82f6', '#22c55e', '#a855f7', '#f97316', '#ec4899', '#06b6d4', '#f59e0b', '#ef4444'];

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const pieLabel = (props: any) => `${props.name} (${((props.percent ?? 0) * 100).toFixed(0)}%)`;

// ── Cluster Charts ──

export function ClusterDistributionChart({ clusters }: { clusters: ClusterSummary[] }) {
  const data = clusters.map(c => ({
    name: c.label ?? `Cluster ${c.cluster_id}`,
    records: c.record_count,
  }));

  return (
    <div className="bg-white border border-gray-200 rounded-xl p-4">
      <h3 className="text-sm font-semibold text-gray-700 mb-3">Cluster Size Distribution</h3>
      <ResponsiveContainer width="100%" height={250}>
        <PieChart>
          <Pie data={data} dataKey="records" nameKey="name" cx="50%" cy="50%" outerRadius={90} label={pieLabel} labelLine={false}>
            {data.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
          </Pie>
          <Tooltip />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}

export function ClusterMetricsRadar({ clusters }: { clusters: ClusterSummary[] }) {
  const metricKeys = [...new Set(clusters.flatMap(c => Object.keys(c.metrics)))];
  const top6 = metricKeys
    .map(k => ({ key: k, range: Math.max(...clusters.map(c => c.metrics[k] ?? 0)) - Math.min(...clusters.map(c => c.metrics[k] ?? 0)) }))
    .sort((a, b) => b.range - a.range)
    .slice(0, 6)
    .map(e => e.key);

  if (top6.length < 3) return null;

  // Normalize to 0-100 for radar
  const maxes = Object.fromEntries(top6.map(k => [k, Math.max(...clusters.map(c => Math.abs(c.metrics[k] ?? 0)), 1)]));
  const data = top6.map(k => {
    const row: Record<string, string | number> = { metric: k.replace('avg_', '') };
    clusters.forEach(c => {
      row[c.label ?? `C${c.cluster_id}`] = Math.round(((c.metrics[k] ?? 0) / maxes[k]) * 100);
    });
    return row;
  });

  return (
    <div className="bg-white border border-gray-200 rounded-xl p-4">
      <h3 className="text-sm font-semibold text-gray-700 mb-3">Cluster Profile Comparison</h3>
      <ResponsiveContainer width="100%" height={300}>
        <RadarChart data={data}>
          <PolarGrid />
          <PolarAngleAxis dataKey="metric" tick={{ fontSize: 10 }} />
          <PolarRadiusAxis tick={false} domain={[0, 100]} />
          {clusters.map((c, i) => (
            <Radar key={c.cluster_id} name={c.label ?? `C${c.cluster_id}`} dataKey={c.label ?? `C${c.cluster_id}`} stroke={COLORS[i % COLORS.length]} fill={COLORS[i % COLORS.length]} fillOpacity={0.15} />
          ))}
          <Legend />
          <Tooltip />
        </RadarChart>
      </ResponsiveContainer>
    </div>
  );
}

// ── Anomaly Charts ──

export function AnomalyScoreDistribution({ data }: { data: AnomalyData }) {
  // Histogram of anomaly scores in buckets
  const buckets = Array.from({ length: 10 }, (_, i) => ({ range: `${(i * 0.1).toFixed(1)}-${((i + 1) * 0.1).toFixed(1)}`, normal: 0, anomaly: 0 }));
  data.records.forEach(r => {
    const idx = Math.min(Math.floor(r.anomaly_score * 10), 9);
    if (r.is_anomaly) buckets[idx].anomaly++;
    else buckets[idx].normal++;
  });

  return (
    <div className="bg-white border border-gray-200 rounded-xl p-4">
      <h3 className="text-sm font-semibold text-gray-700 mb-3">Anomaly Score Distribution</h3>
      <ResponsiveContainer width="100%" height={250}>
        <BarChart data={buckets}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis dataKey="range" tick={{ fontSize: 10 }} />
          <YAxis tick={{ fontSize: 10 }} />
          <Tooltip />
          <Legend />
          <Bar dataKey="normal" name="Normal" fill="#94a3b8" radius={[2, 2, 0, 0]} />
          <Bar dataKey="anomaly" name="Anomaly" fill="#f59e0b" radius={[2, 2, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

export function AnomalyBreakdownPie({ data }: { data: AnomalyData }) {
  const pieData = [
    { name: 'Normal', value: data.summary.total - data.summary.anomaly_count },
    { name: 'Anomaly', value: data.summary.anomaly_count },
  ];

  return (
    <div className="bg-white border border-gray-200 rounded-xl p-4">
      <h3 className="text-sm font-semibold text-gray-700 mb-3">Normal vs Anomalous</h3>
      <ResponsiveContainer width="100%" height={250}>
        <PieChart>
          <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={90} label={pieLabel} labelLine={false}>
            <Cell fill="#94a3b8" />
            <Cell fill="#f59e0b" />
          </Pie>
          <Tooltip />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}

// ── Prediction Charts ──

export function PredictionScatterChart({ data }: { data: PredictionData }) {
  const chartData = data.records.map(r => ({
    actual: r.actual_rating,
    predicted: r.predicted_rating,
    name: r.label,
  }));

  const min = Math.min(...chartData.map(d => Math.min(d.actual, d.predicted)));
  const max = Math.max(...chartData.map(d => Math.max(d.actual, d.predicted)));

  return (
    <div className="bg-white border border-gray-200 rounded-xl p-4">
      <h3 className="text-sm font-semibold text-gray-700 mb-3">Predicted vs Actual</h3>
      <ResponsiveContainer width="100%" height={300}>
        <ScatterChart>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis type="number" dataKey="actual" name="Actual" domain={[min * 0.95, max * 1.05]} tick={{ fontSize: 10 }} label={{ value: 'Actual', position: 'insideBottom', offset: -5, fontSize: 11 }} />
          <YAxis type="number" dataKey="predicted" name="Predicted" domain={[min * 0.95, max * 1.05]} tick={{ fontSize: 10 }} label={{ value: 'Predicted', angle: -90, position: 'insideLeft', fontSize: 11 }} />
          <Tooltip content={({ payload }) => {
            if (!payload?.[0]) return null;
            const d = payload[0].payload as typeof chartData[0];
            return (
              <div className="bg-white border border-gray-200 rounded-lg p-2 shadow text-xs">
                <p className="font-medium">{d.name}</p>
                <p>Actual: {d.actual.toFixed(4)}</p>
                <p>Predicted: {d.predicted.toFixed(4)}</p>
              </div>
            );
          }} />
          <Scatter data={chartData} fill="#3b82f6" fillOpacity={0.6} r={3} />
        </ScatterChart>
      </ResponsiveContainer>
      <p className="text-xs text-gray-400 mt-1 text-center">Points on the diagonal line indicate perfect predictions. Above = over-predicted, below = under-predicted.</p>
    </div>
  );
}

export function ResidualDistributionChart({ data }: { data: PredictionData }) {
  const sorted = [...data.records].sort((a, b) => b.residual - a.residual).slice(0, 20);
  const chartData = sorted.map(r => ({
    name: r.label.length > 15 ? r.label.slice(0, 15) + '...' : r.label,
    residual: parseFloat(r.residual.toFixed(4)),
  }));

  return (
    <div className="bg-white border border-gray-200 rounded-xl p-4">
      <h3 className="text-sm font-semibold text-gray-700 mb-3">Top Residuals (Over vs Under Performers)</h3>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData} layout="vertical">
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis type="number" tick={{ fontSize: 10 }} />
          <YAxis type="category" dataKey="name" tick={{ fontSize: 9 }} width={110} />
          <Tooltip />
          <Bar dataKey="residual" radius={[0, 4, 4, 0]}>
            {chartData.map((d, i) => <Cell key={i} fill={d.residual >= 0 ? '#22c55e' : '#ef4444'} />)}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <p className="text-xs text-gray-400 mt-1 text-center">Green = stats better than outcome (undervalued). Red = outcome better than stats (overvalued).</p>
    </div>
  );
}

// ── Profile Charts ──

export function ProfileBarChart({ data }: { data: ProfileData }) {
  // For optimal_profile: show elite vs league median
  if (data.outputTable.includes('optimal_profile')) {
    const chartData = data.records
      .filter(r => typeof r.gap === 'number')
      .sort((a, b) => Math.abs(Number(b.gap)) - Math.abs(Number(a.gap)))
      .slice(0, 12)
      .map(r => ({
        name: String(r.feature_name ?? '').replace(/_/g, ' '),
        elite: Number(r.elite_median ?? 0),
        league: Number(r.league_median ?? 0),
      }));

    return (
      <div className="bg-white border border-gray-200 rounded-xl p-4">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Elite vs League Average (Top Features by Gap)</h3>
        <ResponsiveContainer width="100%" height={350}>
          <BarChart data={chartData} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis type="number" tick={{ fontSize: 10 }} />
            <YAxis type="category" dataKey="name" tick={{ fontSize: 9 }} width={120} />
            <Tooltip />
            <Legend />
            <Bar dataKey="elite" name="Elite (Top 25%)" fill="#3b82f6" radius={[0, 4, 4, 0]} />
            <Bar dataKey="league" name="League Avg" fill="#d1d5db" radius={[0, 4, 4, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    );
  }

  // For feature_importances: horizontal bar of importance values
  if (data.outputTable.includes('feature_importances')) {
    const chartData = data.records
      .filter(r => String(r.model_type) === 'controllable')
      .sort((a, b) => Number(b.importance) - Number(a.importance))
      .slice(0, 10)
      .map(r => ({
        name: String(r.feature_name ?? '').replace(/_/g, ' '),
        importance: Number(r.importance ?? 0),
      }));

    return (
      <div className="bg-white border border-gray-200 rounded-xl p-4">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Feature Importance (Controllable Model)</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chartData} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis type="number" tick={{ fontSize: 10 }} />
            <YAxis type="category" dataKey="name" tick={{ fontSize: 9 }} width={130} />
            <Tooltip />
            <Bar dataKey="importance" fill="#3b82f6" radius={[0, 4, 4, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    );
  }

  // For positional_value_weights: bar chart by position
  if (data.outputTable.includes('positional_value') || data.outputTable.includes('value_weights')) {
    const posMap = new Map<string, number>();
    data.records.forEach(r => {
      const pos = String(r.position ?? '');
      posMap.set(pos, (posMap.get(pos) ?? 0) + Number(r.positional_value ?? 0));
    });
    const chartData = [...posMap.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(([position, value]) => ({ position, value: parseFloat(value.toFixed(4)) }));

    return (
      <div className="bg-white border border-gray-200 rounded-xl p-4">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Total Positional Value</h3>
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis dataKey="position" tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 10 }} />
            <Tooltip />
            <Bar dataKey="value" name="Positional Value" radius={[4, 4, 0, 0]}>
              {chartData.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    );
  }

  return null;
}
