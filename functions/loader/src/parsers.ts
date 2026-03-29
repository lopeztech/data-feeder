import { parse } from 'csv-parse/sync';

function getExtension(filename: string): string {
  return filename.split('.').pop()?.toLowerCase() ?? '';
}

/**
 * Parse a file buffer into an array of row objects suitable for BigQuery insertion.
 * Returns { rows, columns } where columns are the field names.
 */
export function parseFile(
  buffer: Buffer,
  contentType: string,
  filename: string,
): { rows: Record<string, unknown>[]; columns: string[] } {
  const ext = getExtension(filename);

  if (ext === 'csv' || contentType === 'text/csv') {
    return parseCsv(buffer);
  }
  if (ext === 'json' || contentType === 'application/json') {
    return parseJson(buffer);
  }
  if (ext === 'ndjson' || contentType === 'application/x-ndjson') {
    return parseNdjson(buffer);
  }

  throw new Error(`Unsupported format for BigQuery load: .${ext} (${contentType})`);
}

function parseCsv(buffer: Buffer): { rows: Record<string, unknown>[]; columns: string[] } {
  const records = parse(buffer, {
    columns: true,
    skip_empty_lines: true,
    cast: true,
    cast_date: false,
  }) as Record<string, unknown>[];

  const columns = records.length > 0 ? Object.keys(records[0]) : [];
  return { rows: records, columns };
}

function parseJson(buffer: Buffer): { rows: Record<string, unknown>[]; columns: string[] } {
  const parsed: unknown = JSON.parse(buffer.toString('utf-8'));
  const arr = Array.isArray(parsed) ? parsed : [parsed];
  const rows = arr.filter(
    (r): r is Record<string, unknown> => typeof r === 'object' && r !== null && !Array.isArray(r),
  );
  const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
  return { rows, columns };
}

function parseNdjson(buffer: Buffer): { rows: Record<string, unknown>[]; columns: string[] } {
  const lines = buffer.toString('utf-8').split('\n').filter(l => l.trim().length > 0);
  const rows: Record<string, unknown>[] = [];

  for (const line of lines) {
    const obj: unknown = JSON.parse(line);
    if (typeof obj === 'object' && obj !== null && !Array.isArray(obj)) {
      rows.push(obj as Record<string, unknown>);
    }
  }

  const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
  return { rows, columns };
}
