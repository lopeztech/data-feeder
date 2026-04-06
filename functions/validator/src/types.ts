export interface GcsNotification {
  kind: string;
  name: string;
  bucket: string;
  contentType: string;
  size: string;
  metadata?: {
    dataset?: string;
    jobId?: string;
    uploadedBy?: string;
    category?: string;
  };
}

export interface MessagePublishedData {
  message: {
    data: string;
    attributes?: Record<string, string>;
    messageId: string;
    publishTime: string;
  };
  subscription: string;
}

export interface ValidationResult {
  valid: boolean;
  totalRecords: number;
  columns?: string[];
  error?: string;
}

export interface RejectedRecord {
  row: Record<string, unknown>;
  errors: string[];
}
