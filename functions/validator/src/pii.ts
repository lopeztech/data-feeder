import crypto from 'crypto';

// Column name patterns that suggest PII
const PII_COLUMN_PATTERNS: { pattern: RegExp; type: PiiType }[] = [
  { pattern: /\b(email|e_mail|email_address|emailaddress)\b/i, type: 'email' },
  { pattern: /\b(phone|phone_number|phonenumber|mobile|cell|telephone|tel)\b/i, type: 'phone' },
  { pattern: /\b(ssn|social_security|social_security_number|tax_id|taxid|tin)\b/i, type: 'ssn' },
  { pattern: /\b(first_name|firstname|last_name|lastname|full_name|fullname|player_name|playername)\b/i, type: 'name' },
  { pattern: /\b(address|street|street_address|home_address|mailing_address)\b/i, type: 'address' },
  { pattern: /\b(date_of_birth|dob|birth_date|birthdate|birthday)\b/i, type: 'dob' },
  { pattern: /\b(credit_card|card_number|cardnumber|cc_number|ccnumber)\b/i, type: 'credit_card' },
  { pattern: /\b(ip_address|ipaddress|ip_addr|client_ip)\b/i, type: 'ip' },
];

// Value-level regex patterns for detection when column names aren't revealing
const PII_VALUE_PATTERNS: { pattern: RegExp; type: PiiType }[] = [
  { pattern: /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/, type: 'email' },
  { pattern: /^\+?1?\s*[-.(]?\d{3}[-.)]\s*\d{3}[-.]?\d{4}$/, type: 'phone' },
  { pattern: /^\d{3}-?\d{2}-?\d{4}$/, type: 'ssn' },
  { pattern: /^\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}$/, type: 'credit_card' },
];

type PiiType = 'email' | 'phone' | 'ssn' | 'name' | 'address' | 'dob' | 'credit_card' | 'ip';

export interface PiiReport {
  maskedColumns: { column: string; type: PiiType }[];
  totalMasked: number;
}

function hashValue(value: string): string {
  return crypto.createHash('sha256').update(value.toLowerCase().trim()).digest('hex').slice(0, 16);
}

function maskValue(value: string, type: PiiType): string {
  if (!value || value.trim() === '') return value;

  switch (type) {
    case 'email': {
      // user@domain.com → sha256hash@masked.invalid
      return `${hashValue(value)}@masked.invalid`;
    }
    case 'phone': {
      // +1-555-123-4567 → ***-***-4567
      const digits = value.replace(/\D/g, '');
      return digits.length >= 4 ? `***-***-${digits.slice(-4)}` : '***-***-****';
    }
    case 'ssn': {
      // 123-45-6789 → ***-**-6789
      const digits = value.replace(/\D/g, '');
      return digits.length >= 4 ? `***-**-${digits.slice(-4)}` : '***-**-****';
    }
    case 'name': {
      // John Smith → J*** S***
      return value.split(/\s+/).map(part =>
        part.length > 0 ? part[0] + '***' : ''
      ).join(' ');
    }
    case 'address': {
      // 123 Main St → *** Main St
      return value.replace(/^\d+\s*/, '*** ');
    }
    case 'dob': {
      // 1990-05-15 → 1990-**-**
      return value.replace(/(\d{4})[/-](\d{2})[/-](\d{2})/, '$1-**-**');
    }
    case 'credit_card': {
      // 4111-1111-1111-1234 → ****-****-****-1234
      const digits = value.replace(/\D/g, '');
      return digits.length >= 4 ? `****-****-****-${digits.slice(-4)}` : '****-****-****-****';
    }
    case 'ip': {
      // 192.168.1.100 → 192.168.xxx.xxx
      const parts = value.split('.');
      if (parts.length === 4) return `${parts[0]}.${parts[1]}.xxx.xxx`;
      return 'xxx.xxx.xxx.xxx';
    }
    default:
      return '***REDACTED***';
  }
}

/**
 * Detect PII columns by checking column names and sampling values.
 */
function detectPiiColumns(
  columns: string[],
  sampleRows: Record<string, string>[],
): Map<string, PiiType> {
  const piiMap = new Map<string, PiiType>();

  for (const col of columns) {
    // Check column name
    for (const { pattern, type } of PII_COLUMN_PATTERNS) {
      if (pattern.test(col)) {
        piiMap.set(col, type);
        break;
      }
    }

    // If not detected by name, check values
    if (!piiMap.has(col)) {
      const values = sampleRows.slice(0, 20).map(r => String(r[col] ?? ''));
      const nonEmpty = values.filter(v => v.trim().length > 0);
      if (nonEmpty.length === 0) continue;

      for (const { pattern, type } of PII_VALUE_PATTERNS) {
        const matchRate = nonEmpty.filter(v => pattern.test(v)).length / nonEmpty.length;
        if (matchRate >= 0.5) {
          piiMap.set(col, type);
          break;
        }
      }
    }
  }

  return piiMap;
}

/**
 * Mask PII in rows. Returns masked rows and a report of what was masked.
 */
export function maskPii(
  rows: Record<string, string>[],
  columns: string[],
): { maskedRows: Record<string, string>[]; report: PiiReport } {
  const piiColumns = detectPiiColumns(columns, rows);

  if (piiColumns.size === 0) {
    return {
      maskedRows: rows,
      report: { maskedColumns: [], totalMasked: 0 },
    };
  }

  let totalMasked = 0;
  const maskedRows = rows.map(row => {
    const masked = { ...row };
    for (const [col, type] of piiColumns) {
      if (masked[col] !== undefined && masked[col] !== '') {
        masked[col] = maskValue(String(masked[col]), type);
        totalMasked++;
      }
    }
    return masked;
  });

  const maskedColumns = Array.from(piiColumns.entries()).map(([column, type]) => ({ column, type }));

  return { maskedRows, report: { maskedColumns, totalMasked } };
}
