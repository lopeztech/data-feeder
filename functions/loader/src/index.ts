import { cloudEvent, CloudEvent } from '@google-cloud/functions-framework';
import { BigQuery } from '@google-cloud/bigquery';
import { Storage } from '@google-cloud/storage';
import { Firestore } from '@google-cloud/firestore';
import { PubSub } from '@google-cloud/pubsub';
import type { ValidationCompleteMessage, MessagePublishedData } from './types.js';

const bigquery = new BigQuery();
const gcs = new Storage();
const firestore = new Firestore({
  databaseId: process.env.FIRESTORE_DATABASE || 'data-feeder',
});
const pubsub = new PubSub();

const BQ_DATASET = process.env.BQ_CURATED_DATASET || 'curated';
const BQ_STAGING_DATASET = process.env.BQ_STAGING_DATASET || 'staging';
const PIPELINE_FAILED_TOPIC = process.env.PIPELINE_FAILED_TOPIC || 'pipeline-failed';

const FORMAT_MAP: Record<string, string> = {
  'text/csv': 'CSV',
  'application/json': 'NEWLINE_DELIMITED_JSON',
  'application/x-ndjson': 'NEWLINE_DELIMITED_JSON',
};

cloudEvent('loader', async (event: CloudEvent<MessagePublishedData>) => {
  const messageData = event.data?.message?.data;
  if (!messageData) {
    console.error('No message data in event');
    return;
  }

  let msg: ValidationCompleteMessage;
  try {
    msg = JSON.parse(Buffer.from(messageData, 'base64').toString('utf-8'));
  } catch (err) {
    console.error('Failed to parse validation-complete message:', err);
    return;
  }

  const { jobId, dataset, silverPath, contentType } = msg;

  if (!jobId || !silverPath) {
    console.warn('Missing jobId or silverPath in message, skipping');
    return;
  }

  const jobRef = firestore.collection('jobs').doc(jobId);

  try {
    // Idempotency guard: only process TRANSFORMING jobs
    const jobDoc = await jobRef.get();
    if (!jobDoc.exists) {
      console.warn(`Job ${jobId} not found, skipping`);
      return;
    }

    const currentStatus = jobDoc.data()?.status;
    if (currentStatus !== 'TRANSFORMING') {
      console.warn(`Job ${jobId} status is ${currentStatus}, expected TRANSFORMING — skipping`);
      return;
    }

    // Parse gs:// path
    const pathMatch = silverPath.match(/^gs:\/\/([^/]+)\/(.+)$/);
    if (!pathMatch) {
      throw new Error(`Invalid Silver path: ${silverPath}`);
    }
    const [, bucketName, objectName] = pathMatch;

    // Sanitize table name for BigQuery
    const tableName = (jobDoc.data()?.bq_table || dataset)
      .replace(/[^a-zA-Z0-9_]/g, '_')
      .replace(/^_+/, '')
      .substring(0, 128);

    // Determine BQ source format — Silver files are now Parquet
    const isParquet = objectName.endsWith('.parquet');
    const sourceFormat = isParquet ? 'PARQUET' : (FORMAT_MAP[contentType] || 'CSV');

    const gcsFile = gcs.bucket(bucketName).file(objectName);

    // Step 1: Load into staging with WRITE_TRUNCATE (idempotent per batch)
    await ensureDataset(BQ_STAGING_DATASET);
    const [stagingJob] = await bigquery.dataset(BQ_STAGING_DATASET).table(tableName).load(
      gcsFile,
      {
        sourceFormat,
        autodetect: true,
        writeDisposition: 'WRITE_TRUNCATE',
        createDisposition: 'CREATE_IF_NEEDED',
        timePartitioning: { type: 'DAY' },
        skipLeadingRows: sourceFormat === 'CSV' ? 1 : 0,
      },
    );

    if (stagingJob.status?.errors?.length) {
      throw new Error(`BQ staging load errors: ${stagingJob.status.errors.map(e => e.message).join('; ')}`);
    }

    const loadedRows = Number(stagingJob.statistics?.load?.outputRows ?? 0);

    // Step 2: Detect high-cardinality columns for clustering
    const clusteringFields = await detectClusteringFields(BQ_STAGING_DATASET, tableName);

    // Step 3: Append from staging to curated (Gold) with clustering
    await ensureDataset(BQ_DATASET);
    const curatedTable = bigquery.dataset(BQ_DATASET).table(tableName);
    const [curatedExists] = await curatedTable.exists();

    if (!curatedExists) {
      // First load: load directly into curated with partitioning + clustering
      const loadOpts: Record<string, unknown> = {
        sourceFormat,
        autodetect: true,
        writeDisposition: 'WRITE_TRUNCATE',
        createDisposition: 'CREATE_IF_NEEDED',
        timePartitioning: { type: 'DAY', field: undefined },
        skipLeadingRows: sourceFormat === 'CSV' ? 1 : 0,
      };
      if (clusteringFields.length > 0) {
        loadOpts.clustering = { fields: clusteringFields };
      }
      const [curatedJob] = await curatedTable.load(gcsFile, loadOpts);
      if (curatedJob.status?.errors?.length) {
        throw new Error(`BQ curated load errors: ${curatedJob.status.errors.map((e) => e.message).join('; ')}`);
      }
    } else {
      // Subsequent loads: append from staging to curated
      const stagingRef = `${bigquery.projectId}.${BQ_STAGING_DATASET}.${tableName}`;
      const curatedRef = `${bigquery.projectId}.${BQ_DATASET}.${tableName}`;
      const [appendJob] = await bigquery.createQueryJob({
        query: `INSERT INTO \`${curatedRef}\` SELECT * FROM \`${stagingRef}\``,
      });
      await appendJob.getQueryResults();
    }

    // Step 4: Create Data Catalog tag if first load
    if (!curatedExists) {
      await createCatalogTag(BQ_DATASET, tableName, dataset);
    }

    // Update Firestore to LOADED
    await jobRef.update({
      status: 'LOADED',
      bq_table: `${BQ_DATASET}.${tableName}`,
      stats: {
        total_records: jobDoc.data()?.stats?.total_records ?? loadedRows,
        valid: jobDoc.data()?.stats?.valid ?? loadedRows,
        rejected: jobDoc.data()?.stats?.rejected ?? 0,
        loaded: loadedRows,
      },
      updated_at: new Date().toISOString(),
    });

    console.log(`Job ${jobId}: loaded ${loadedRows} rows → staging.${tableName} → ${BQ_DATASET}.${tableName}`);
  } catch (err) {
    console.error(`Job ${jobId}: loader error:`, err);

    try {
      await jobRef.update({
        status: 'FAILED',
        error: err instanceof Error ? err.message : 'Unknown loader error',
        updated_at: new Date().toISOString(),
      });
    } catch { /* best effort */ }

    try {
      await pubsub.topic(PIPELINE_FAILED_TOPIC).publishMessage({
        json: {
          jobId,
          dataset,
          error: err instanceof Error ? err.message : 'Unknown loader error',
          silverPath,
        },
      });
    } catch { /* best effort */ }

    throw err; // Let Pub/Sub retry
  }
});

/** Ensure a BigQuery dataset exists, creating it if needed. */
async function ensureDataset(datasetId: string): Promise<void> {
  const ds = bigquery.dataset(datasetId);
  const [exists] = await ds.exists();
  if (!exists) {
    await bigquery.createDataset(datasetId, { location: 'australia-southeast1' });
  }
}

/**
 * Detect high-cardinality string/timestamp columns suitable for clustering.
 * Returns up to 4 column names (BigQuery clustering limit).
 */
async function detectClusteringFields(datasetId: string, tableName: string): Promise<string[]> {
  try {
    const [cols] = await bigquery.query({
      query: `
        SELECT column_name, data_type
        FROM \`${datasetId}.INFORMATION_SCHEMA.COLUMNS\`
        WHERE table_name = @tableName
        ORDER BY ordinal_position
      `,
      params: { tableName },
    });

    const candidates: string[] = [];
    for (const col of cols as { column_name: string; data_type: string }[]) {
      // Prefer ID columns, timestamps, and string dimensions for clustering
      const name = col.column_name.toLowerCase();
      const isIdLike = name.endsWith('_id') || name === 'id';
      const isTimestamp = ['TIMESTAMP', 'DATETIME', 'DATE'].includes(col.data_type);
      const isClusterable = ['STRING', 'INT64', 'NUMERIC', 'TIMESTAMP', 'DATETIME', 'DATE'].includes(col.data_type);

      if (isIdLike || isTimestamp) {
        candidates.unshift(col.column_name); // prioritize
      } else if (isClusterable) {
        candidates.push(col.column_name);
      }
    }

    return candidates.slice(0, 4);
  } catch {
    return [];
  }
}

/**
 * Create a Data Catalog tag on the Gold table with pipeline metadata.
 * Best-effort — failures are logged but don't block the pipeline.
 */
async function createCatalogTag(datasetId: string, tableName: string, sourceDataset: string): Promise<void> {
  try {
    const { DataCatalogClient } = await import('@google-cloud/datacatalog');
    const catalog = new DataCatalogClient();

    const projectId = bigquery.projectId;
    const linkedResource = `//bigquery.googleapis.com/projects/${projectId}/datasets/${datasetId}/tables/${tableName}`;

    // Look up the entry
    const [entry] = await catalog.lookupEntry({ linkedResource });
    if (!entry?.name) return;

    // Create or update a tag template (idempotent)
    const tagTemplateName = `projects/${projectId}/locations/australia-southeast1/tagTemplates/data_feeder_gold`;
    let templateExists = false;
    try {
      await catalog.getTagTemplate({ name: tagTemplateName });
      templateExists = true;
    } catch { /* template doesn't exist yet */ }

    if (!templateExists) {
      await catalog.createTagTemplate({
        parent: `projects/${projectId}/locations/australia-southeast1`,
        tagTemplateId: 'data_feeder_gold',
        tagTemplate: {
          displayName: 'Data Feeder Gold Table',
          fields: {
            source_dataset: { displayName: 'Source Dataset', type: { primitiveType: 'STRING' } },
            pipeline_zone: { displayName: 'Pipeline Zone', type: { primitiveType: 'STRING' } },
            created_by_pipeline: { displayName: 'Created By Pipeline', type: { primitiveType: 'BOOL' } },
          },
        },
      });
    }

    // Create tag on the entry
    await catalog.createTag({
      parent: entry.name,
      tag: {
        template: tagTemplateName,
        fields: {
          source_dataset: { stringValue: sourceDataset },
          pipeline_zone: { stringValue: 'gold' },
          created_by_pipeline: { boolValue: true },
        },
      },
    });

    console.log(`Data Catalog tag created for ${datasetId}.${tableName}`);
  } catch (err) {
    console.warn('Data Catalog tagging failed (non-blocking):', err instanceof Error ? err.message : err);
  }
}
