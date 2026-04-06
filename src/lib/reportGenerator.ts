import type { ClusterSummary, ClusterRecord, AnomalyData, PredictionData, ProfileData } from './uploadService';
import type { Narrative } from './narratives';

function timestamp(): string {
  return new Date().toLocaleString('en-AU', {
    day: '2-digit', month: 'short', year: 'numeric',
    hour: '2-digit', minute: '2-digit', hour12: false,
  });
}

function csvEscape(value: unknown): string {
  const str = String(value ?? '');
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

function csvRow(values: unknown[]): string {
  return values.map(csvEscape).join(',');
}

function downloadCsv(filename: string, content: string) {
  const blob = new Blob([content], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}

function executiveSummary(narrative: Narrative, modelName: string, generatedAt: string): string[] {
  const lines: string[] = [];
  lines.push(csvRow(['EXECUTIVE SUMMARY REPORT']));
  lines.push(csvRow(['Model', modelName]));
  lines.push(csvRow(['Report', narrative.title]));
  lines.push(csvRow(['Generated', generatedAt]));
  lines.push('');
  lines.push(csvRow(['OVERVIEW']));
  lines.push(csvRow([narrative.overview]));
  lines.push('');
  lines.push(csvRow(['WHAT THIS DATA SHOWS']));
  lines.push(csvRow([narrative.whatItShows]));
  lines.push('');
  lines.push(csvRow(['RECOMMENDED ACTIONS']));
  narrative.actions.forEach((a, i) => lines.push(csvRow([`${i + 1}. ${a}`])));
  if (narrative.fineTuning) {
    lines.push('');
    lines.push(csvRow(['MODEL FINE-TUNING NOTES']));
    lines.push(csvRow([narrative.fineTuning]));
  }
  lines.push('');
  return lines;
}

export function downloadClusterReport(
  modelName: string,
  narrative: Narrative,
  clusters: ClusterSummary[],
  records: ClusterRecord[],
) {
  const ts = timestamp();
  const lines = executiveSummary(narrative, modelName, ts);
  const totalRecords = clusters.reduce((s, c) => s + c.record_count, 0);

  // Key findings
  lines.push(csvRow(['KEY FINDINGS']));
  lines.push(csvRow(['Total Records', totalRecords]));
  lines.push(csvRow(['Number of Clusters', clusters.length]));
  const sortedBySize = [...clusters].sort((a, b) => b.record_count - a.record_count);
  lines.push(csvRow(['Largest Cluster', `Cluster ${sortedBySize[0]?.cluster_id} (${sortedBySize[0]?.record_count} records)`]));
  lines.push('');

  // Cluster summary table
  lines.push(csvRow(['CLUSTER ANALYSIS']));
  const metricKeys = [...new Set(clusters.flatMap(c => Object.keys(c.metrics)))];
  lines.push(csvRow(['Cluster ID', 'Records', '% of Total', ...metricKeys.map(k => k.replace('avg_', ''))]));
  for (const c of clusters) {
    lines.push(csvRow([
      c.cluster_id,
      c.record_count,
      `${((c.record_count / totalRecords) * 100).toFixed(1)}%`,
      ...metricKeys.map(k => c.metrics[k]?.toFixed(2) ?? ''),
    ]));
  }
  lines.push('');

  // Top records per cluster
  lines.push(csvRow(['TOP RECORDS BY CLUSTER']));
  const fieldKeys = records.length > 0 ? Object.keys(records[0].fields) : [];
  lines.push(csvRow(['Cluster', 'Record ID', 'Name', 'Impact Score', ...fieldKeys]));
  for (const r of records) {
    lines.push(csvRow([
      r.cluster_id,
      r.record_id,
      r.label,
      r.score.toFixed(4),
      ...fieldKeys.map(k => r.fields[k] ?? ''),
    ]));
  }

  downloadCsv(`${modelName}_cluster_report_${new Date().toISOString().slice(0, 10)}.csv`, lines.join('\n'));
}

export function downloadAnomalyReport(
  modelName: string,
  narrative: Narrative,
  data: AnomalyData,
) {
  const ts = timestamp();
  const lines = executiveSummary(narrative, modelName, ts);

  // Key findings
  lines.push(csvRow(['KEY FINDINGS']));
  lines.push(csvRow(['Total Records Scanned', data.summary.total]));
  lines.push(csvRow(['Anomalies Detected', data.summary.anomaly_count]));
  lines.push(csvRow(['Anomaly Rate', `${((data.summary.anomaly_count / data.summary.total) * 100).toFixed(1)}%`]));
  lines.push(csvRow(['Average Anomaly Score', data.summary.avg_score.toFixed(4)]));
  lines.push(csvRow(['Maximum Anomaly Score', data.summary.max_score.toFixed(4)]));
  lines.push('');

  // Anomaly records
  const anomalies = data.records.filter(r => r.is_anomaly);
  const normal = data.records.filter(r => !r.is_anomaly);

  lines.push(csvRow(['FLAGGED ANOMALIES — Requires Review']));
  const fieldKeys = data.records.length > 0 ? Object.keys(data.records[0].fields) : [];
  lines.push(csvRow(['Record ID', 'Name', 'Anomaly Score', 'Flagged', ...fieldKeys]));
  for (const r of [...anomalies, ...normal].slice(0, 50)) {
    lines.push(csvRow([
      r.record_id,
      r.label,
      r.anomaly_score.toFixed(4),
      r.is_anomaly ? 'YES' : 'No',
      ...fieldKeys.map(k => r.fields[k] ?? ''),
    ]));
  }

  downloadCsv(`${modelName}_anomaly_report_${new Date().toISOString().slice(0, 10)}.csv`, lines.join('\n'));
}

export function downloadPredictionReport(
  modelName: string,
  narrative: Narrative,
  data: PredictionData,
) {
  const ts = timestamp();
  const lines = executiveSummary(narrative, modelName, ts);

  // Key findings
  lines.push(csvRow(['KEY FINDINGS']));
  lines.push(csvRow(['Total Records', data.summary.total]));
  lines.push(csvRow(['Model R² (approx)', data.summary.r2_approx.toFixed(4)]));
  lines.push(csvRow(['Mean Absolute Error', data.summary.mae.toFixed(4)]));
  lines.push(csvRow(['RMSE', data.summary.rmse.toFixed(4)]));
  lines.push(csvRow(['Mean Residual', data.summary.mean_residual.toFixed(4)]));
  lines.push('');

  // Split into over/under performers
  const sorted = [...data.records].sort((a, b) => b.residual - a.residual);
  const overPerformers = sorted.filter(r => r.residual > 0).slice(0, 20);
  const underPerformers = sorted.filter(r => r.residual < 0).reverse().slice(0, 20);

  lines.push(csvRow(['OVER-PERFORMERS — Stats exceed their rating/outcome']));
  lines.push(csvRow(['Record ID', 'Name', 'Position', 'Predicted', 'Actual', 'Residual', 'Interpretation']));
  for (const r of overPerformers) {
    lines.push(csvRow([
      r.record_id,
      r.label,
      r.position ?? '',
      r.predicted_rating.toFixed(4),
      r.actual_rating.toFixed(4),
      r.residual.toFixed(4),
      'Model expects higher outcome — potentially undervalued',
    ]));
  }
  lines.push('');

  lines.push(csvRow(['UNDER-PERFORMERS — Rating/outcome exceeds their stats']));
  lines.push(csvRow(['Record ID', 'Name', 'Position', 'Predicted', 'Actual', 'Residual', 'Interpretation']));
  for (const r of underPerformers) {
    lines.push(csvRow([
      r.record_id,
      r.label,
      r.position ?? '',
      r.predicted_rating.toFixed(4),
      r.actual_rating.toFixed(4),
      r.residual.toFixed(4),
      'Model expects lower outcome — may be overvalued or due for regression',
    ]));
  }

  downloadCsv(`${modelName}_prediction_report_${new Date().toISOString().slice(0, 10)}.csv`, lines.join('\n'));
}

export function downloadProfileReport(
  modelName: string,
  narrative: Narrative,
  data: ProfileData,
) {
  const ts = timestamp();
  const lines = executiveSummary(narrative, modelName, ts);

  // Key findings
  lines.push(csvRow(['KEY FINDINGS']));
  lines.push(csvRow(['Output Table', data.outputTable]));
  lines.push(csvRow(['Total Records', data.total]));
  lines.push(csvRow(['Columns', data.columns.length]));
  lines.push('');

  // Full data table
  lines.push(csvRow(['DATA']));
  lines.push(csvRow(data.columns.map(c => c.name)));
  for (const row of data.records) {
    lines.push(csvRow(data.columns.map(c => {
      const v = row[c.name];
      return typeof v === 'number' && v % 1 !== 0 ? v.toFixed(4) : v;
    })));
  }

  downloadCsv(`${modelName}_${data.outputTable}_report_${new Date().toISOString().slice(0, 10)}.csv`, lines.join('\n'));
}
