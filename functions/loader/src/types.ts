export interface ValidationCompleteMessage {
  jobId: string;
  dataset: string;
  silverPath: string;
  totalRecords: number;
  contentType: string;
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
