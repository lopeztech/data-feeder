import type { ClusterSummary, ClusterRecord, ModelInfo } from '../lib/uploadService';
import type { PipelineJob } from '../types';

export const MOCK_MODELS: ModelInfo[] = [
  {
    model: 'player',
    clustersTable: 'player_clusters',
    idCol: 'player_id',
    scoreCol: 'impact_score',
    sourceTables: ['all_player_stats', 'all_player_profiles'],
  },
];

export const MOCK_CLUSTERS: ClusterSummary[] = [
  {
    cluster_id: 0,
    record_count: 84,
    label: 'Goalkeepers',
    metrics: { avg_goals: 0.42, avg_assists: 0.31, avg_tackles: 2.15, avg_saves: 28.6, avg_rating: 6.58, avg_impact_score: 0.612 },
  },
  {
    cluster_id: 1,
    record_count: 312,
    label: 'Defensive Anchors',
    metrics: { avg_goals: 1.24, avg_assists: 1.56, avg_tackles: 12.8, avg_saves: 0.02, avg_rating: 6.72, avg_impact_score: 0.489 },
  },
  {
    cluster_id: 2,
    record_count: 198,
    label: 'Goal Threats',
    metrics: { avg_goals: 8.7, avg_assists: 5.2, avg_tackles: 3.1, avg_saves: 0.01, avg_rating: 7.14, avg_impact_score: 0.523 },
  },
  {
    cluster_id: 3,
    record_count: 245,
    label: 'Creative Playmakers',
    metrics: { avg_goals: 2.1, avg_assists: 6.8, avg_tackles: 5.6, avg_saves: 0.0, avg_rating: 6.91, avg_impact_score: 0.410 },
  },
  {
    cluster_id: 4,
    record_count: 276,
    label: 'Ball Winners',
    metrics: { avg_goals: 1.8, avg_assists: 2.1, avg_tackles: 9.4, avg_saves: 0.03, avg_rating: 6.65, avg_impact_score: 0.399 },
  },
];

export const MOCK_CLUSTER_RECORDS: ClusterRecord[] = [
  // Cluster 0 — Goalkeepers
  { cluster_id: 0, record_id: 'p-gk-001', label: 'Marc-André ter Stegen', fields: { position: 'GK', league: 'La Liga', goals: 0, assists: 0, tackles: 0, saves: 102, rating: 6.82 }, score: 0.891 },
  { cluster_id: 0, record_id: 'p-gk-002', label: 'Alisson Becker', fields: { position: 'GK', league: 'Premier League', goals: 0, assists: 1, tackles: 0, saves: 98, rating: 6.91 }, score: 0.874 },
  { cluster_id: 0, record_id: 'p-gk-003', label: 'Thibaut Courtois', fields: { position: 'GK', league: 'La Liga', goals: 0, assists: 0, tackles: 0, saves: 84, rating: 6.75 }, score: 0.812 },
  { cluster_id: 0, record_id: 'p-gk-004', label: 'Ederson Moraes', fields: { position: 'GK', league: 'Premier League', goals: 0, assists: 2, tackles: 0, saves: 76, rating: 6.68 }, score: 0.756 },
  { cluster_id: 0, record_id: 'p-gk-005', label: 'Jan Oblak', fields: { position: 'GK', league: 'La Liga', goals: 0, assists: 0, tackles: 0, saves: 91, rating: 6.79 }, score: 0.743 },

  // Cluster 1 — Defensive Anchors
  { cluster_id: 1, record_id: 'p-def-001', label: 'Virgil van Dijk', fields: { position: 'CB', league: 'Premier League', goals: 3, assists: 1, tackles: 42, saves: 0, rating: 7.12 }, score: 0.834 },
  { cluster_id: 1, record_id: 'p-def-002', label: 'Rúben Dias', fields: { position: 'CB', league: 'Premier League', goals: 1, assists: 2, tackles: 38, saves: 0, rating: 7.04 }, score: 0.798 },
  { cluster_id: 1, record_id: 'p-def-003', label: 'Antonio Rüdiger', fields: { position: 'CB', league: 'La Liga', goals: 2, assists: 0, tackles: 35, saves: 0, rating: 6.88 }, score: 0.761 },
  { cluster_id: 1, record_id: 'p-def-004', label: 'William Saliba', fields: { position: 'CB', league: 'Premier League', goals: 2, assists: 1, tackles: 44, saves: 0, rating: 7.08 }, score: 0.723 },
  { cluster_id: 1, record_id: 'p-def-005', label: 'Kim Min-jae', fields: { position: 'CB', league: 'Bundesliga', goals: 1, assists: 0, tackles: 36, saves: 0, rating: 6.94 }, score: 0.691 },

  // Cluster 2 — Goal Threats
  { cluster_id: 2, record_id: 'p-att-001', label: 'Erling Haaland', fields: { position: 'ST', league: 'Premier League', goals: 27, assists: 5, tackles: 4, saves: 0, rating: 7.45 }, score: 0.912 },
  { cluster_id: 2, record_id: 'p-att-002', label: 'Kylian Mbappé', fields: { position: 'LW', league: 'La Liga', goals: 22, assists: 8, tackles: 6, saves: 0, rating: 7.52 }, score: 0.887 },
  { cluster_id: 2, record_id: 'p-att-003', label: 'Harry Kane', fields: { position: 'ST', league: 'Bundesliga', goals: 26, assists: 7, tackles: 3, saves: 0, rating: 7.38 }, score: 0.856 },
  { cluster_id: 2, record_id: 'p-att-004', label: 'Lautaro Martínez', fields: { position: 'ST', league: 'Serie A', goals: 19, assists: 4, tackles: 5, saves: 0, rating: 7.21 }, score: 0.801 },
  { cluster_id: 2, record_id: 'p-att-005', label: 'Viktor Gyökeres', fields: { position: 'ST', league: 'Primeira Liga', goals: 24, assists: 6, tackles: 7, saves: 0, rating: 7.31 }, score: 0.778 },

  // Cluster 3 — Creative Playmakers
  { cluster_id: 3, record_id: 'p-mid-001', label: 'Kevin De Bruyne', fields: { position: 'CAM', league: 'Premier League', goals: 6, assists: 14, tackles: 8, saves: 0, rating: 7.34 }, score: 0.869 },
  { cluster_id: 3, record_id: 'p-mid-002', label: 'Martin Ødegaard', fields: { position: 'CAM', league: 'Premier League', goals: 7, assists: 11, tackles: 10, saves: 0, rating: 7.28 }, score: 0.821 },
  { cluster_id: 3, record_id: 'p-mid-003', label: 'Florian Wirtz', fields: { position: 'CAM', league: 'Bundesliga', goals: 9, assists: 10, tackles: 6, saves: 0, rating: 7.19 }, score: 0.793 },
  { cluster_id: 3, record_id: 'p-mid-004', label: 'Jamal Musiala', fields: { position: 'AM', league: 'Bundesliga', goals: 8, assists: 9, tackles: 7, saves: 0, rating: 7.11 }, score: 0.754 },
  { cluster_id: 3, record_id: 'p-mid-005', label: 'Bruno Fernandes', fields: { position: 'CAM', league: 'Premier League', goals: 5, assists: 12, tackles: 9, saves: 0, rating: 7.06 }, score: 0.712 },

  // Cluster 4 — Ball Winners
  { cluster_id: 4, record_id: 'p-dm-001', label: 'Rodri', fields: { position: 'CDM', league: 'Premier League', goals: 3, assists: 4, tackles: 52, saves: 0, rating: 7.22 }, score: 0.845 },
  { cluster_id: 4, record_id: 'p-dm-002', label: 'Declan Rice', fields: { position: 'CDM', league: 'Premier League', goals: 4, assists: 5, tackles: 48, saves: 0, rating: 7.08 }, score: 0.789 },
  { cluster_id: 4, record_id: 'p-dm-003', label: 'Aurélien Tchouaméni', fields: { position: 'CDM', league: 'La Liga', goals: 2, assists: 2, tackles: 41, saves: 0, rating: 6.92 }, score: 0.734 },
  { cluster_id: 4, record_id: 'p-dm-004', label: 'Moises Caicedo', fields: { position: 'CDM', league: 'Premier League', goals: 1, assists: 3, tackles: 46, saves: 0, rating: 6.85 }, score: 0.698 },
  { cluster_id: 4, record_id: 'p-dm-005', label: 'Amadou Onana', fields: { position: 'CDM', league: 'Premier League', goals: 2, assists: 1, tackles: 39, saves: 0, rating: 6.78 }, score: 0.652 },
];

export const MOCK_LINEAGE_JOBS: PipelineJob[] = [
  {
    job_id: 'mock-stats-001',
    dataset: 'all_player_stats',
    filename: 'epl_player_stats_2025.csv',
    file_size_bytes: 2_840_000,
    status: 'LOADED',
    uploaded_by: 'demo@datafeeder.dev',
    created_at: '2026-03-20T14:22:00Z',
    updated_at: '2026-03-20T14:35:12Z',
    bronze_path: 'gs://demo-raw/all_player_stats/job-mock-stats-001/epl_player_stats_2025.csv',
    silver_path: 'gs://demo-staging/all_player_stats/job-mock-stats-001/data.parquet',
    bq_table: 'curated.all_player_stats',
    stats: { total_records: 842, valid: 839, rejected: 3, loaded: 839 },
    error: null,
  },
  {
    job_id: 'mock-stats-002',
    dataset: 'all_player_stats',
    filename: 'laliga_player_stats_2025.csv',
    file_size_bytes: 1_560_000,
    status: 'LOADED',
    uploaded_by: 'demo@datafeeder.dev',
    created_at: '2026-03-22T09:10:00Z',
    updated_at: '2026-03-22T09:18:45Z',
    bronze_path: 'gs://demo-raw/all_player_stats/job-mock-stats-002/laliga_player_stats_2025.csv',
    silver_path: 'gs://demo-staging/all_player_stats/job-mock-stats-002/data.parquet',
    bq_table: 'curated.all_player_stats',
    stats: { total_records: 476, valid: 476, rejected: 0, loaded: 476 },
    error: null,
  },
  {
    job_id: 'mock-profiles-001',
    dataset: 'all_player_profiles',
    filename: 'player_profiles_combined.json',
    file_size_bytes: 3_210_000,
    status: 'LOADED',
    uploaded_by: 'demo@datafeeder.dev',
    created_at: '2026-03-19T11:05:00Z',
    updated_at: '2026-03-19T11:14:30Z',
    bronze_path: 'gs://demo-raw/all_player_profiles/job-mock-profiles-001/player_profiles_combined.json',
    silver_path: 'gs://demo-staging/all_player_profiles/job-mock-profiles-001/data.parquet',
    bq_table: 'curated.all_player_profiles',
    stats: { total_records: 1_115, valid: 1_115, rejected: 0, loaded: 1_115 },
    error: null,
  },
];
