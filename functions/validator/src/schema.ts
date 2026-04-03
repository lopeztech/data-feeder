import crypto from 'crypto';

/** Inferred column type for schema enforcement. */
export type InferredType = 'INT64' | 'FLOAT64' | 'BOOLEAN' | 'TIMESTAMP' | 'STRING';

export interface ColumnSchema {
  name: string;
  type: InferredType;
}

export interface TransformResult {
  validRows: Record<string, unknown>[];
  rejectedRows: { row: Record<string, unknown>; errors: string[] }[];
  schema: ColumnSchema[];
}

// Patterns for type inference (sampled values must pass these to be considered the type)
const INT_RE = /^-?\d+$/;
const FLOAT_RE = /^-?\d+(\.\d+)?([eE][+-]?\d+)?$/;
const BOOL_RE = /^(true|false|0|1|yes|no)$/i;
const TIMESTAMP_RE = /^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2})?)?/;

// Values treated as null
const NULL_TOKENS = new Set(['', 'null', 'NULL', 'None', 'NONE', 'none', 'NA', 'N/A', 'n/a', 'NaN', 'nan', '#N/A', 'undefined']);

/**
 * Standardize null values: convert common null representations to actual null.
 */
export function standardizeNull(value: unknown): unknown {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string' && NULL_TOKENS.has(value.trim())) return null;
  return value;
}

/**
 * Infer column types by sampling the first N rows.
 */
export function inferSchema(rows: Record<string, unknown>[], columns: string[]): ColumnSchema[] {
  const sampleSize = Math.min(rows.length, 100);
  const sample = rows.slice(0, sampleSize);

  return columns.map(name => {
    const values = sample
      .map(r => standardizeNull(r[name]))
      .filter(v => v !== null)
      .map(v => String(v).trim());

    if (values.length === 0) return { name, type: 'STRING' as InferredType };

    // Check types in order of specificity
    if (values.every(v => BOOL_RE.test(v))) return { name, type: 'BOOLEAN' as InferredType };
    if (values.every(v => INT_RE.test(v))) return { name, type: 'INT64' as InferredType };
    if (values.every(v => FLOAT_RE.test(v))) return { name, type: 'FLOAT64' as InferredType };
    if (values.every(v => TIMESTAMP_RE.test(v))) return { name, type: 'TIMESTAMP' as InferredType };
    return { name, type: 'STRING' as InferredType };
  });
}

/**
 * Cast a value to the inferred type. Returns { value, error } tuple.
 */
function castValue(raw: unknown, schema: ColumnSchema): { value: unknown; error?: string } {
  const normalized = standardizeNull(raw);
  if (normalized === null) return { value: null };

  const str = String(normalized).trim();

  switch (schema.type) {
    case 'INT64': {
      if (!INT_RE.test(str)) return { value: raw, error: `${schema.name}: expected INT64, got "${str}"` };
      return { value: parseInt(str, 10) };
    }
    case 'FLOAT64': {
      if (!FLOAT_RE.test(str)) return { value: raw, error: `${schema.name}: expected FLOAT64, got "${str}"` };
      return { value: parseFloat(str) };
    }
    case 'BOOLEAN': {
      const lower = str.toLowerCase();
      if (['true', '1', 'yes'].includes(lower)) return { value: true };
      if (['false', '0', 'no'].includes(lower)) return { value: false };
      return { value: raw, error: `${schema.name}: expected BOOLEAN, got "${str}"` };
    }
    case 'TIMESTAMP': {
      if (!TIMESTAMP_RE.test(str)) return { value: raw, error: `${schema.name}: expected TIMESTAMP, got "${str}"` };
      return { value: str };
    }
    default:
      return { value: str };
  }
}

/**
 * Transform rows: standardize nulls, cast types, deduplicate.
 * Returns valid rows, rejected rows with error details, and the inferred schema.
 */
export function transformRows(
  rows: Record<string, unknown>[],
  columns: string[],
): TransformResult {
  const schema = inferSchema(rows, columns);

  const validRows: Record<string, unknown>[] = [];
  const rejectedRows: { row: Record<string, unknown>; errors: string[] }[] = [];
  const seenHashes = new Set<string>();

  for (const row of rows) {
    // Deduplicate: hash the row content
    const rowHash = crypto
      .createHash('md5')
      .update(columns.map(c => String(row[c] ?? '')).join('\x00'))
      .digest('hex');

    if (seenHashes.has(rowHash)) continue; // skip duplicates silently
    seenHashes.add(rowHash);

    // Cast each field
    const castRow: Record<string, unknown> = {};
    const errors: string[] = [];

    for (const col of schema) {
      const { value, error } = castValue(row[col.name], col);
      castRow[col.name] = value;
      if (error) errors.push(error);
    }

    if (errors.length > 0) {
      rejectedRows.push({ row, errors });
    } else {
      validRows.push(castRow);
    }
  }

  return { validRows, rejectedRows, schema };
}
