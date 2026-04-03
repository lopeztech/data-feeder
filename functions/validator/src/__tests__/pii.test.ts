import { describe, it, expect } from 'vitest';
import { maskPii } from '../pii.js';

describe('maskPii()', () => {
  describe('Column name detection', () => {
    it('detects "email" column and masks with sha256@masked.invalid', () => {
      const rows = [
        { email: 'alice@example.com', score: '10' },
        { email: 'bob@example.com', score: '20' },
      ];
      const { maskedRows, report } = maskPii(rows, ['email', 'score']);

      expect(report.maskedColumns).toContainEqual({ column: 'email', type: 'email' });
      expect(report.totalMasked).toBe(2);
      expect(maskedRows[0].email).toMatch(/@masked\.invalid$/);
      expect(maskedRows[0].email).not.toBe('alice@example.com');
      // score should be unchanged
      expect(maskedRows[0].score).toBe('10');
    });

    it('detects "phone_number" column and masks as ***-***-XXXX', () => {
      const rows = [
        { phone_number: '+1-555-123-4567', id: '1' },
      ];
      const { maskedRows, report } = maskPii(rows, ['phone_number', 'id']);

      expect(report.maskedColumns).toContainEqual({ column: 'phone_number', type: 'phone' });
      expect(maskedRows[0].phone_number).toMatch(/^\*{3}-\*{3}-\d{4}$/);
    });

    it('detects "ssn" column and masks as ***-**-XXXX', () => {
      const rows = [
        { ssn: '123-45-6789', name: 'Test' },
      ];
      const { maskedRows, report } = maskPii(rows, ['ssn', 'name']);

      expect(report.maskedColumns).toContainEqual({ column: 'ssn', type: 'ssn' });
      expect(maskedRows[0].ssn).toMatch(/^\*{3}-\*{2}-\d{4}$/);
    });

    it('detects "first_name" column and masks as initial + ***', () => {
      const rows = [
        { first_name: 'Alice', id: '1' },
      ];
      const { maskedRows, report } = maskPii(rows, ['first_name', 'id']);

      expect(report.maskedColumns).toContainEqual({ column: 'first_name', type: 'name' });
      expect(maskedRows[0].first_name).toBe('A***');
    });
  });

  describe('Value pattern detection', () => {
    it('detects email values even when column is not named as PII', () => {
      // Need >50% match rate on up to 20-row sample
      const rows = Array.from({ length: 10 }, (_, i) => ({
        contact: `user${i}@example.com`,
        data: `value${i}`,
      }));
      const { maskedRows, report } = maskPii(rows, ['contact', 'data']);

      expect(report.maskedColumns).toContainEqual({ column: 'contact', type: 'email' });
      expect(maskedRows[0].contact).toMatch(/@masked\.invalid$/);
      // data column should be unchanged
      expect(maskedRows[0].data).toBe('value0');
    });
  });

  describe('No PII', () => {
    it('returns original rows unchanged when no PII is found', () => {
      const rows = [
        { score: '100', level: '5', status: 'active' },
        { score: '200', level: '10', status: 'inactive' },
      ];
      const { maskedRows, report } = maskPii(rows, ['score', 'level', 'status']);

      expect(report.maskedColumns).toEqual([]);
      expect(report.totalMasked).toBe(0);
      // Should be the same reference (no copy) since no PII detected
      expect(maskedRows).toBe(rows);
    });
  });

  describe('Empty values', () => {
    it('PII column with empty strings are not masked (stays empty)', () => {
      const rows = [
        { email: '', score: '10' },
        { email: 'alice@example.com', score: '20' },
      ];
      const { maskedRows, report } = maskPii(rows, ['email', 'score']);

      expect(maskedRows[0].email).toBe('');
      expect(maskedRows[1].email).toMatch(/@masked\.invalid$/);
      // totalMasked should only count non-empty values
      expect(report.totalMasked).toBe(1);
    });
  });

  describe('Multiple PII columns', () => {
    it('detects and masks both email and phone columns', () => {
      const rows = [
        { email: 'alice@example.com', phone: '555-123-4567', id: '1' },
        { email: 'bob@example.com', phone: '555-987-6543', id: '2' },
      ];
      const { maskedRows, report } = maskPii(rows, ['email', 'phone', 'id']);

      expect(report.maskedColumns).toHaveLength(2);
      const types = report.maskedColumns.map(c => c.type);
      expect(types).toContain('email');
      expect(types).toContain('phone');
      expect(report.totalMasked).toBe(4);

      expect(maskedRows[0].email).toMatch(/@masked\.invalid$/);
      expect(maskedRows[0].phone).toMatch(/^\*{3}-\*{3}-\d{4}$/);
      expect(maskedRows[0].id).toBe('1');
    });
  });
});
