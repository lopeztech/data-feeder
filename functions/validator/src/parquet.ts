import type { ColumnSchema } from './schema.js';

/**
 * Write rows as a Snappy-compressed Parquet buffer.
 * Uses @dsnp/parquetjs for pure-JS Parquet writing.
 */
export async function writeParquet(
  rows: Record<string, unknown>[],
  schema: ColumnSchema[],
): Promise<Buffer> {
  const parquet = await import('@dsnp/parquetjs');

  // Map our inferred types to Parquet types
  const typeMap: Record<string, string> = {
    INT64: 'INT64',
    FLOAT64: 'DOUBLE',
    BOOLEAN: 'BOOLEAN',
    TIMESTAMP: 'TIMESTAMP_MILLIS',
    STRING: 'UTF8',
  };

  // Build parquetjs schema definition
  const schemaDef: Record<string, unknown> = {};
  for (const col of schema) {
    schemaDef[col.name] = {
      type: typeMap[col.type] || 'UTF8',
      compression: 'SNAPPY',
      optional: true,
    };
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const parquetSchema = new parquet.ParquetSchema(schemaDef as any);

  // Write to an in-memory buffer
  const outputChunks: Buffer[] = [];

  const mockStream = {
    write(chunk: unknown) {
      outputChunks.push(Buffer.from(chunk as Uint8Array));
      return true;
    },
    end() {
      // no-op
    },
  };

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const writer = await parquet.ParquetWriter.openStream(parquetSchema, mockStream as any);

  for (const row of rows) {
    const parquetRow: Record<string, unknown> = {};
    for (const col of schema) {
      const val = row[col.name];
      if (val === null || val === undefined) continue; // optional: skip nulls
      if (col.type === 'TIMESTAMP' && typeof val === 'string') {
        parquetRow[col.name] = new Date(val);
      } else {
        parquetRow[col.name] = val;
      }
    }
    await writer.appendRow(parquetRow);
  }

  await writer.close();

  return Buffer.concat(outputChunks);
}
