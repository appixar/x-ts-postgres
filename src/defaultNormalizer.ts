// ─────────────────────────────────────────────
// x-postgres — Default value normalizer
// ─────────────────────────────────────────────
// Handles all logic for converting YAML default values to SQL,
// and normalizing database defaults for diff comparison.

/**
 * Convert a raw default value from YAML into a safe SQL expression.
 *
 * Rules:
 * - null/empty/"null" → null (no DEFAULT clause)
 * - Numbers / booleans → as-is
 * - SQL functions (ending with ")") or keywords (CURRENT_TIMESTAMP, etc.) → as-is
 * - JSON/JSONB with {} or [] → cast with ::jsonb / ::json
 * - UUID literals → single-quoted lowercase
 * - Already single-quoted → as-is
 * - Double-quoted → convert to single-quoted
 * - Everything else → single-quoted string
 */
export function normalizeDefaultSql(rawDefault: string | null | undefined, typeRealUpper: string): string | null {
    if (rawDefault === null || rawDefault === undefined) return null;

    let raw = String(rawDefault).trim();
    if (raw === '') return null;

    // Explicit "null" → no default
    if (raw.toLowerCase() === 'null') return null;

    // Strip "DEFAULT " prefix if user typed it
    if (raw.toLowerCase().startsWith('default ')) {
        raw = raw.substring(8).trim();
    }

    const upperRaw = raw.toUpperCase();

    // SQL functions (ends with ")") or SQL keywords
    if (/\)\s*$/.test(raw) || ['CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME'].includes(upperRaw)) {
        return raw;
    }

    // Boolean
    if (['true', 'false'].includes(raw.toLowerCase())) {
        return raw.toUpperCase();
    }

    // Numeric
    if (/^-?\d+(\.\d+)?$/.test(raw)) {
        return raw;
    }

    // JSON / JSONB
    if (typeRealUpper.includes('JSONB') || typeRealUpper.includes('JSON')) {
        const first = raw[0];
        if (first === '{' || first === '[') {
            const escaped = raw.replace(/'/g, "''");
            if (typeRealUpper.includes('JSONB')) return `'${escaped}'::jsonb`;
            return `'${escaped}'::json`;
        }
        // Already advanced expression
        return raw;
    }

    // UUID literal
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(raw)) {
        return `'${raw.toLowerCase()}'`;
    }

    // Already single-quoted
    if (/^'(.*)'$/s.test(raw)) {
        return raw;
    }

    // Double-quoted → convert
    const dqMatch = raw.match(/^"(.*)"$/s);
    if (dqMatch) {
        raw = dqMatch[1];
    }

    // Default: wrap as string
    const escaped = raw.replace(/'/g, "''");
    return `'${escaped}'`;
}

/**
 * Build the full DEFAULT clause for SQL, e.g. "DEFAULT 'value'"
 */
export function buildDefaultClause(rawDefault: string | null | undefined, typeRealUpper: string): string {
    const sql = normalizeDefaultSql(rawDefault, typeRealUpper);
    if (sql === null) return '';
    return `DEFAULT ${sql}`;
}

/**
 * Normalize a column_default value from Postgres for safe comparison.
 *
 * Examples:
 * - "'0'::integer" → "0"
 * - "true::boolean" → "true"
 * - "now()" → "now()" (unchanged)
 * - "nextval('tbl_id_seq'::regclass)" → kept as-is
 */
export function normalizeDbDefaultForCompare(dbDefault: string | null | undefined): string {
    let d = String(dbDefault ?? '').trim();
    if (d === '') return '';

    d = d.replace(/\s+/g, ' ');

    // Don't touch nextval (serial/sequence)
    if (d.toLowerCase().includes('nextval(')) return d;

    // Special handling for encode(gen_random_bytes... which often has internal casts like 'hex'::text
    if (d.toLowerCase().startsWith('encode(')) {
        // Remove ::text inside the expression
        d = d.replace(/::text/gi, '');
        // Remove ::unknown inside the expression (sometimes happens)
        d = d.replace(/::unknown/gi, '');
    }

    // Strip trailing ::type casts (handles multiword like "timestamp without time zone")
    let prev = '';
    while (prev !== d) {
        prev = d;
        const m = d.match(/^(.*)::[a-zA-Z][a-zA-Z0-9_ ]*$/);
        if (m) d = m[1].trim();
    }

    // Strip outer parentheses
    const parenMatch = d.match(/^\((.*)\)$/);
    if (parenMatch) d = parenMatch[1].trim();

    // Strip outer single quotes
    const quoteMatch = d.match(/^'(.*)'$/s);
    if (quoteMatch) d = quoteMatch[1];

    // Boolean normalize
    if (['true', 'false'].includes(d.toLowerCase())) {
        d = d.toLowerCase();
    }

    // Unescape double single-quotes
    d = d.replace(/''/g, "'");

    return d;
}
