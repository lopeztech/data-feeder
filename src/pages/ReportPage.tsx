import { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { fetchClusters, fetchAnomalies, fetchPredictions, fetchProfile } from '../lib/uploadService';
import type { ClusterSummary, ClusterRecord, AnomalyData, PredictionData, ProfileData, ModelType } from '../lib/uploadService';
import { MOCK_CLUSTERS, MOCK_CLUSTER_RECORDS, MOCK_ANOMALY_DATA, MOCK_PREDICTION_DATA } from '../data/mockClusters';
import { getNarrative } from '../lib/narratives';

export default function ReportPage() {
  const { type, model } = useParams<{ type: string; model: string }>();
  const modelType = (type as ModelType) || 'clusters';
  const navigate = useNavigate();
  const { user } = useAuth();
  const isGuest = user?.role === 'guest';

  const [clusterData, setClusterData] = useState<{ clusters: ClusterSummary[]; records: ClusterRecord[] } | null>(null);
  const [anomalyData, setAnomalyData] = useState<AnomalyData | null>(null);
  const [predictionData, setPredictionData] = useState<PredictionData | null>(null);
  const [profileData, setProfileData] = useState<ProfileData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!model) return;

    if (isGuest) {
      if (modelType === 'clusters') setClusterData({ clusters: MOCK_CLUSTERS, records: MOCK_CLUSTER_RECORDS });
      else if (modelType === 'anomalies') setAnomalyData(MOCK_ANOMALY_DATA);
      else if (modelType === 'predictions') setPredictionData(MOCK_PREDICTION_DATA);
      setLoading(false);
      return;
    }

    setLoading(true);
    const fetch = modelType === 'clusters'
      ? fetchClusters(model).then(d => setClusterData({ clusters: d.clusters, records: d.records }))
      : modelType === 'anomalies'
      ? fetchAnomalies(model).then(d => setAnomalyData(d))
      : modelType === 'profile'
      ? fetchProfile(model).then(d => setProfileData(d))
      : fetchPredictions(model).then(d => setPredictionData(d));

    fetch
      .catch(err => setError(err.message))
      .finally(() => setLoading(false));
  }, [model, modelType, isGuest]);

  const narrative = getNarrative(model ?? '', modelType, profileData?.outputTable);
  const now = new Date().toLocaleString('en-AU', {
    day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false,
  });

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="w-8 h-8 border-4 border-brand-600 border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-8 max-w-4xl mx-auto">
        <p className="text-red-600">Failed to load report data: {error}</p>
        <button onClick={() => navigate(-1)} className="mt-4 text-sm text-brand-600 hover:underline">Back</button>
      </div>
    );
  }

  return (
    <div className="bg-white min-h-screen">
      {/* Action bar — hidden when printing */}
      <div className="print:hidden sticky top-0 z-10 bg-white border-b border-gray-200 px-6 py-3 flex items-center justify-between">
        <button onClick={() => navigate(-1)} className="inline-flex items-center gap-1 text-sm text-gray-500 hover:text-brand-600 transition">
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
          </svg>
          Back to Insights
        </button>
        <button
          onClick={() => window.print()}
          className="inline-flex items-center gap-2 px-4 py-2 bg-brand-600 text-white rounded-lg text-sm font-medium hover:bg-brand-700 transition"
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
          </svg>
          Export as PDF
        </button>
      </div>

      {/* Report content */}
      <div className="max-w-4xl mx-auto px-8 py-10 print:px-0 print:py-0">
        {/* Header */}
        <div className="border-b-2 border-gray-900 pb-4 mb-8">
          <p className="text-xs text-gray-400 uppercase tracking-widest mb-1">Executive Report</p>
          <h1 className="text-2xl font-bold text-gray-900">{narrative.title}</h1>
          <div className="flex gap-6 mt-2 text-xs text-gray-500">
            <span>Model: <strong className="text-gray-700">{model}</strong></span>
            <span>Generated: <strong className="text-gray-700">{now}</strong></span>
            <span>Source: <strong className="text-gray-700">Data Feeder ML Pipeline</strong></span>
          </div>
        </div>

        {/* Overview */}
        <section className="mb-8">
          <h2 className="text-sm font-bold text-gray-900 uppercase tracking-wide border-b border-gray-200 pb-1 mb-3">Overview</h2>
          <p className="text-sm text-gray-700 leading-relaxed">{narrative.overview}</p>
        </section>

        {/* What it shows */}
        <section className="mb-8">
          <h2 className="text-sm font-bold text-gray-900 uppercase tracking-wide border-b border-gray-200 pb-1 mb-3">Interpretation</h2>
          <p className="text-sm text-gray-700 leading-relaxed">{narrative.whatItShows}</p>
        </section>

        {/* Recommended actions */}
        <section className="mb-8">
          <h2 className="text-sm font-bold text-gray-900 uppercase tracking-wide border-b border-gray-200 pb-1 mb-3">Recommended Actions</h2>
          <ol className="text-sm text-gray-700 space-y-2 list-decimal list-inside">
            {narrative.actions.map((a, i) => <li key={i}>{a}</li>)}
          </ol>
        </section>

        {/* Key findings */}
        <section className="mb-8">
          <h2 className="text-sm font-bold text-gray-900 uppercase tracking-wide border-b border-gray-200 pb-1 mb-3">Key Findings</h2>
          {clusterData && <ClusterFindings clusters={clusterData.clusters} />}
          {anomalyData && <AnomalyFindings data={anomalyData} />}
          {predictionData && <PredictionFindings data={predictionData} />}
          {profileData && <ProfileFindings data={profileData} />}
        </section>

        {/* Data tables */}
        <section className="mb-8">
          <h2 className="text-sm font-bold text-gray-900 uppercase tracking-wide border-b border-gray-200 pb-1 mb-3">Data</h2>
          {clusterData && <ClusterTables clusters={clusterData.clusters} records={clusterData.records} />}
          {anomalyData && <AnomalyTable data={anomalyData} />}
          {predictionData && <PredictionTable data={predictionData} />}
          {profileData && <ProfileTable data={profileData} />}
        </section>

        {/* Fine-tuning notes */}
        {narrative.fineTuning && (
          <section className="mb-8 bg-gray-50 rounded-lg p-4">
            <h2 className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-2">Model Fine-Tuning Notes</h2>
            <p className="text-xs text-gray-600 leading-relaxed">{narrative.fineTuning}</p>
          </section>
        )}

        {/* Footer */}
        <div className="border-t border-gray-200 pt-4 mt-12 text-xs text-gray-400">
          <p>This report was generated by the Data Feeder ML Pipeline. Data sourced from BigQuery curated dataset.</p>
          <p className="mt-1">For questions about this report, contact the data engineering team.</p>
        </div>
      </div>
    </div>
  );
}

// ── Finding components ──

function KV({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="flex justify-between py-1 border-b border-gray-100">
      <span className="text-sm text-gray-500">{label}</span>
      <span className="text-sm font-medium text-gray-900">{typeof value === 'number' ? value.toLocaleString() : value}</span>
    </div>
  );
}

function ClusterFindings({ clusters }: { clusters: ClusterSummary[] }) {
  const total = clusters.reduce((s, c) => s + c.record_count, 0);
  const largest = [...clusters].sort((a, b) => b.record_count - a.record_count)[0];
  return (
    <div className="max-w-md space-y-0">
      <KV label="Total Records" value={total} />
      <KV label="Number of Clusters" value={clusters.length} />
      <KV label="Largest Cluster" value={`Cluster ${largest?.cluster_id} (${largest?.record_count} records)`} />
    </div>
  );
}

function AnomalyFindings({ data }: { data: AnomalyData }) {
  return (
    <div className="max-w-md space-y-0">
      <KV label="Total Records Scanned" value={data.summary.total} />
      <KV label="Anomalies Detected" value={data.summary.anomaly_count} />
      <KV label="Anomaly Rate" value={`${((data.summary.anomaly_count / data.summary.total) * 100).toFixed(1)}%`} />
      <KV label="Average Anomaly Score" value={data.summary.avg_score.toFixed(4)} />
      <KV label="Maximum Anomaly Score" value={data.summary.max_score.toFixed(4)} />
    </div>
  );
}

function PredictionFindings({ data }: { data: PredictionData }) {
  return (
    <div className="max-w-md space-y-0">
      <KV label="Total Records" value={data.summary.total} />
      <KV label="Model R\u00B2 (approx)" value={data.summary.r2_approx.toFixed(4)} />
      <KV label="Mean Absolute Error" value={data.summary.mae.toFixed(4)} />
      <KV label="RMSE" value={data.summary.rmse.toFixed(4)} />
      <KV label="Mean Residual" value={data.summary.mean_residual.toFixed(4)} />
    </div>
  );
}

function ProfileFindings({ data }: { data: ProfileData }) {
  return (
    <div className="max-w-md space-y-0">
      <KV label="Output Table" value={data.outputTable} />
      <KV label="Total Records" value={data.total} />
      <KV label="Columns" value={data.columns.length} />
    </div>
  );
}

// ── Data table components ──

function ReportTable({ headers, rows }: { headers: string[]; rows: (string | number)[][] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs border-collapse">
        <thead>
          <tr>
            {headers.map(h => (
              <th key={h} className="text-left px-2 py-1.5 font-semibold text-gray-600 bg-gray-50 border border-gray-200 whitespace-nowrap">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>
              {row.map((cell, j) => (
                <td key={j} className="px-2 py-1 text-gray-700 border border-gray-200 whitespace-nowrap max-w-[200px] truncate">
                  {typeof cell === 'number' && cell % 1 !== 0 ? cell.toFixed(4) : String(cell ?? '')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ClusterTables({ clusters, records }: { clusters: ClusterSummary[]; records: ClusterRecord[] }) {
  const metricKeys = [...new Set(clusters.flatMap(c => Object.keys(c.metrics)))];
  const total = clusters.reduce((s, c) => s + c.record_count, 0);

  const summaryHeaders = ['Cluster', 'Records', '% of Total', ...metricKeys.map(k => k.replace('avg_', ''))];
  const summaryRows = clusters.map(c => [
    c.cluster_id,
    c.record_count,
    `${((c.record_count / total) * 100).toFixed(1)}%`,
    ...metricKeys.map(k => c.metrics[k] ?? ''),
  ] as (string | number)[]);

  const fieldKeys = records.length > 0 ? Object.keys(records[0].fields) : [];
  const recordHeaders = ['Cluster', 'ID', 'Name', 'Impact Score', ...fieldKeys];
  const recordRows = records.map(r => [
    r.cluster_id, r.record_id, r.label, r.score,
    ...fieldKeys.map(k => r.fields[k] ?? ''),
  ] as (string | number)[]);

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-xs font-semibold text-gray-500 mb-2">Cluster Summary</h3>
        <ReportTable headers={summaryHeaders} rows={summaryRows} />
      </div>
      <div className="break-before-page">
        <h3 className="text-xs font-semibold text-gray-500 mb-2">Top Records by Cluster</h3>
        <ReportTable headers={recordHeaders} rows={recordRows} />
      </div>
    </div>
  );
}

function AnomalyTable({ data }: { data: AnomalyData }) {
  const fieldKeys = data.records.length > 0 ? Object.keys(data.records[0].fields) : [];
  const sorted = [...data.records].sort((a, b) => (b.is_anomaly ? 1 : 0) - (a.is_anomaly ? 1 : 0) || b.anomaly_score - a.anomaly_score);
  const headers = ['ID', 'Name', 'Score', 'Flagged', ...fieldKeys];
  const rows = sorted.slice(0, 50).map(r => [
    r.record_id, r.label, r.anomaly_score, r.is_anomaly ? 'YES' : 'No',
    ...fieldKeys.map(k => r.fields[k] ?? ''),
  ] as (string | number)[]);

  return <ReportTable headers={headers} rows={rows} />;
}

function PredictionTable({ data }: { data: PredictionData }) {
  const sorted = [...data.records].sort((a, b) => b.residual - a.residual);
  const overPerformers = sorted.filter(r => r.residual > 0).slice(0, 15);
  const underPerformers = sorted.filter(r => r.residual < 0).reverse().slice(0, 15);

  const headers = ['ID', 'Name', 'Position', 'Predicted', 'Actual', 'Residual', 'Assessment'];

  const overRows = overPerformers.map(r => [
    r.record_id, r.label, r.position ?? '', r.predicted_rating, r.actual_rating, r.residual,
    'Undervalued - stats exceed outcome',
  ] as (string | number)[]);

  const underRows = underPerformers.map(r => [
    r.record_id, r.label, r.position ?? '', r.predicted_rating, r.actual_rating, r.residual,
    'Overvalued - outcome exceeds stats',
  ] as (string | number)[]);

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-xs font-semibold text-green-700 mb-2">Over-Performers (Undervalued)</h3>
        <ReportTable headers={headers} rows={overRows} />
      </div>
      <div className="break-before-page">
        <h3 className="text-xs font-semibold text-red-700 mb-2">Under-Performers (Overvalued)</h3>
        <ReportTable headers={headers} rows={underRows} />
      </div>
    </div>
  );
}

function ProfileTable({ data }: { data: ProfileData }) {
  const headers = data.columns.map(c => c.name);
  const rows = data.records.map(row =>
    data.columns.map(c => row[c.name] ?? '') as (string | number)[]
  );

  return <ReportTable headers={headers} rows={rows} />;
}
