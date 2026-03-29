import { parse } from 'csv-parse/sync';
import type { ValidationResult } from './types.js';

function getExtension(filename: string): string {
  return filename.split('.').pop()?.toLowerCase() ?? '';
}

function validateCsv(buffer: Buffer): ValidationResult {
  try {
    const records = parse(buffer, {
      columns: true,
      skip_empty_lines: true,
      relax_column_count: true,
    }) as Record<string, unknown>[];

    if (records.length === 0) {
      return { valid: false, totalRecords: 0, error: 'CSV file is empty (no data rows)' };
    }

    const columns = Object.keys(records[0]);
    if (columns.length === 0) {
      return { valid: false, totalRecords: 0, error: 'CSV file has no columns' };
    }

    return { valid: true, totalRecords: records.length, columns };
  } catch (err) {
    return { valid: false, totalRecords: 0, error: `CSV parse error: ${err instanceof Error ? err.message : String(err)}` };
  }
}

function validateJson(buffer: Buffer): ValidationResult {
  try {
    const parsed: unknown = JSON.parse(buffer.toString('utf-8'));

    if (Array.isArray(parsed)) {
      if (parsed.length === 0) {
        return { valid: false, totalRecords: 0, error: 'JSON array is empty' };
      }
      const first = parsed[0];
      const columns = typeof first === 'object' && first !== null ? Object.keys(first) : undefined;
      return { valid: true, totalRecords: parsed.length, columns };
    }

    if (typeof parsed === 'object' && parsed !== null) {
      const columns = Object.keys(parsed);
      return { valid: true, totalRecords: 1, columns };
    }

    return { valid: false, totalRecords: 0, error: 'JSON root must be an object or array' };
  } catch (err) {
    return { valid: false, totalRecords: 0, error: `JSON parse error: ${err instanceof Error ? err.message : String(err)}` };
  }
}

function validateNdjson(buffer: Buffer): ValidationResult {
  try {
    const lines = buffer.toString('utf-8').split('\n').filter(l => l.trim().length > 0);

    if (lines.length === 0) {
      return { valid: false, totalRecords: 0, error: 'NDJSON file is empty' };
    }

    let validCount = 0;
    let columns: string[] | undefined;
    const errors: string[] = [];

    for (let i = 0; i < lines.length; i++) {
      try {
        const obj: unknown = JSON.parse(lines[i]);
        if (typeof obj !== 'object' || obj === null || Array.isArray(obj)) {
          errors.push(`Line ${i + 1}: not a JSON object`);
          continue;
        }
        if (!columns) columns = Object.keys(obj);
        validCount++;
      } catch {
        errors.push(`Line ${i + 1}: invalid JSON`);
      }
    }

    if (validCount === 0) {
      return { valid: false, totalRecords: 0, error: `No valid NDJSON records. ${errors[0]}` };
    }

    return { valid: true, totalRecords: validCount, columns };
  } catch (err) {
    return { valid: false, totalRecords: 0, error: `NDJSON read error: ${err instanceof Error ? err.message : String(err)}` };
  }
}

function validateBinary(buffer: Buffer, filename: string): ValidationResult {
  const ext = getExtension(filename);

  if (buffer.length === 0) {
    return { valid: false, totalRecords: 0, error: 'File is empty' };
  }

  if (ext === 'parquet') {
    // Parquet files start with magic bytes "PAR1"
    const magic = buffer.subarray(0, 4).toString('ascii');
    if (magic !== 'PAR1') {
      return { valid: false, totalRecords: 0, error: 'Invalid Parquet file (missing PAR1 magic bytes)' };
    }
    return { valid: true, totalRecords: 0 };
  }

  if (ext === 'avro') {
    // Avro files start with magic bytes "Obj\x01"
    const magic = buffer.subarray(0, 4);
    if (magic[0] !== 0x4f || magic[1] !== 0x62 || magic[2] !== 0x6a || magic[3] !== 0x01) {
      return { valid: false, totalRecords: 0, error: 'Invalid Avro file (missing Obj\\x01 magic bytes)' };
    }
    return { valid: true, totalRecords: 0 };
  }

  return { valid: false, totalRecords: 0, error: `Unsupported binary format: .${ext}` };
}

export function validate(buffer: Buffer, contentType: string, filename: string): ValidationResult {
  const ext = getExtension(filename);

  if (ext === 'csv' || contentType === 'text/csv') {
    return validateCsv(buffer);
  }
  if (ext === 'json' || contentType === 'application/json') {
    return validateJson(buffer);
  }
  if (ext === 'ndjson' || contentType === 'application/x-ndjson') {
    return validateNdjson(buffer);
  }
  if (['parquet', 'avro'].includes(ext) || contentType === 'application/octet-stream') {
    return validateBinary(buffer, filename);
  }

  return { valid: false, totalRecords: 0, error: `Unsupported file format: .${ext} (${contentType})` };
}
