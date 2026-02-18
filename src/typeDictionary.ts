// ─────────────────────────────────────────────
// x-postgres — Postgres type dictionary
// ─────────────────────────────────────────────
// Maps YAML/config type names to their PostgreSQL
// information_schema.columns data_type equivalents.
// Used by the diff engine for accurate comparison.

export const POSTGRES_TYPE_DICTIONARY: Record<string, string> = {
    SERIAL: 'integer',
    VARCHAR: 'character varying',
    INT: 'integer',
    INTEGER: 'integer',
    TEXT: 'text',
    TIMESTAMP: 'timestamp without time zone',
    DATE: 'date',
    TIME: 'time without time zone',
    BOOLEAN: 'boolean',
    SMALLINT: 'smallint',
    BIGINT: 'bigint',
    REAL: 'real',
    DOUBLE: 'double precision',
    NUMERIC: 'numeric',
    DECIMAL: 'numeric',
    JSON: 'json',
    JSONB: 'jsonb',
    UUID: 'uuid',
};
