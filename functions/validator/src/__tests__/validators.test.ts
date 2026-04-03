import { describe, it, expect } from 'vitest';
import { validate } from '../validators.js';

describe('validate()', () => {
  describe('CSV', () => {
    it('valid CSV with headers and data rows', () => {
      const csv = 'name,age,city\nAlice,30,Sydney\nBob,25,Melbourne\n';
      const result = validate(Buffer.from(csv), 'text/csv', 'data.csv');
      expect(result.valid).toBe(true);
      expect(result.totalRecords).toBe(2);
      expect(result.columns).toEqual(['name', 'age', 'city']);
    });

    it('empty CSV (headers only, no data rows)', () => {
      const csv = 'name,age,city\n';
      const result = validate(Buffer.from(csv), 'text/csv', 'data.csv');
      expect(result.valid).toBe(false);
      expect(result.totalRecords).toBe(0);
      expect(result.error).toMatch(/empty/i);
    });

    it('malformed CSV with unclosed quote', () => {
      const csv = 'name,age\n"Alice,30\n';
      const result = validate(Buffer.from(csv), 'text/csv', 'data.csv');
      expect(result.valid).toBe(false);
      expect(result.error).toMatch(/parse error/i);
    });
  });

  describe('JSON', () => {
    it('valid JSON array of objects', () => {
      const json = JSON.stringify([
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ]);
      const result = validate(Buffer.from(json), 'application/json', 'data.json');
      expect(result.valid).toBe(true);
      expect(result.totalRecords).toBe(2);
      expect(result.columns).toEqual(['id', 'name']);
    });

    it('valid single JSON object', () => {
      const json = JSON.stringify({ id: 1, name: 'Alice', score: 99 });
      const result = validate(Buffer.from(json), 'application/json', 'data.json');
      expect(result.valid).toBe(true);
      expect(result.totalRecords).toBe(1);
      expect(result.columns).toEqual(['id', 'name', 'score']);
    });

    it('empty JSON array', () => {
      const result = validate(Buffer.from('[]'), 'application/json', 'data.json');
      expect(result.valid).toBe(false);
      expect(result.totalRecords).toBe(0);
      expect(result.error).toMatch(/empty/i);
    });

    it('invalid JSON string', () => {
      const result = validate(Buffer.from('{not valid json'), 'application/json', 'data.json');
      expect(result.valid).toBe(false);
      expect(result.error).toMatch(/parse error/i);
    });

    it('primitive root (string)', () => {
      const result = validate(Buffer.from('"hello"'), 'application/json', 'data.json');
      expect(result.valid).toBe(false);
      expect(result.error).toMatch(/root must be.*object or array/i);
    });
  });

  describe('NDJSON', () => {
    it('valid multi-line NDJSON', () => {
      const ndjson = '{"id":1,"name":"Alice"}\n{"id":2,"name":"Bob"}\n{"id":3,"name":"Carol"}\n';
      const result = validate(Buffer.from(ndjson), 'application/x-ndjson', 'data.ndjson');
      expect(result.valid).toBe(true);
      expect(result.totalRecords).toBe(3);
      expect(result.columns).toEqual(['id', 'name']);
    });

    it('empty NDJSON', () => {
      const result = validate(Buffer.from(''), 'application/x-ndjson', 'data.ndjson');
      expect(result.valid).toBe(false);
      expect(result.totalRecords).toBe(0);
      expect(result.error).toMatch(/empty/i);
    });

    it('mixed valid and invalid lines counts only valid records', () => {
      const ndjson = '{"id":1}\nnot json\n{"id":2}\n[1,2,3]\n{"id":3}\n';
      const result = validate(Buffer.from(ndjson), 'application/x-ndjson', 'data.ndjson');
      expect(result.valid).toBe(true);
      expect(result.totalRecords).toBe(3);
    });
  });

  describe('Parquet', () => {
    it('buffer starting with PAR1 magic bytes', () => {
      const buf = Buffer.concat([Buffer.from('PAR1'), Buffer.alloc(100, 0)]);
      const result = validate(buf, 'application/octet-stream', 'data.parquet');
      expect(result.valid).toBe(true);
    });

    it('buffer with wrong magic bytes', () => {
      const buf = Buffer.from('XXXX' + '\0'.repeat(100));
      const result = validate(buf, 'application/octet-stream', 'data.parquet');
      expect(result.valid).toBe(false);
      expect(result.error).toMatch(/PAR1/);
    });
  });

  describe('Avro', () => {
    it('buffer starting with Obj\\x01 magic bytes', () => {
      const buf = Buffer.concat([Buffer.from([0x4f, 0x62, 0x6a, 0x01]), Buffer.alloc(100, 0)]);
      const result = validate(buf, 'application/octet-stream', 'data.avro');
      expect(result.valid).toBe(true);
    });

    it('buffer with wrong magic bytes', () => {
      const buf = Buffer.from('XXXX' + '\0'.repeat(100));
      const result = validate(buf, 'application/octet-stream', 'data.avro');
      expect(result.valid).toBe(false);
      expect(result.error).toMatch(/Obj/);
    });
  });

  describe('Binary edge cases', () => {
    it('empty buffer for parquet', () => {
      const result = validate(Buffer.alloc(0), 'application/octet-stream', 'data.parquet');
      expect(result.valid).toBe(false);
      expect(result.error).toMatch(/empty/i);
    });
  });

  describe('Unsupported format', () => {
    it('unknown extension returns unsupported error', () => {
      const result = validate(Buffer.from('hello'), 'application/zip', 'data.zip');
      expect(result.valid).toBe(false);
      expect(result.error).toMatch(/unsupported/i);
    });
  });

  describe('Content-type fallback', () => {
    it('filename without known extension uses contentType for CSV', () => {
      const csv = 'col1,col2\nval1,val2\n';
      const result = validate(Buffer.from(csv), 'text/csv', 'data');
      expect(result.valid).toBe(true);
      expect(result.totalRecords).toBe(1);
      expect(result.columns).toEqual(['col1', 'col2']);
    });
  });
});
