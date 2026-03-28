export type UserRole = 'google' | 'guest';

export interface AuthUser {
  uid: string;
  email: string | null;
  displayName: string | null;
  photoURL: string | null;
  role: UserRole;
}

export type JobStatus =
  | 'UPLOADING'
  | 'VALIDATING'
  | 'TRANSFORMING'
  | 'LOADED'
  | 'FAILED'
  | 'REJECTED';

export interface JobStats {
  total_records: number;
  valid: number;
  rejected: number;
  loaded: number;
}

export interface PipelineJob {
  job_id: string;
  dataset: string;
  filename: string;
  file_size_bytes: number;
  status: JobStatus;
  uploaded_by: string;
  created_at: string;
  updated_at: string;
  bronze_path: string | null;
  silver_path: string | null;
  bq_table: string | null;
  stats: JobStats;
  error: string | null;
}
