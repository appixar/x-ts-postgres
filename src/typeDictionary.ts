// ─────────────────────────────────────────────
// x-postgres — Postgres type dictionary
// ─────────────────────────────────────────────
// Maps YAML/config type names to their PostgreSQL
// information_schema.columns data_type equivalents.
// Used by the diff engine for accurate comparison.

export const POSTGRES_TYPE_DICTIONARY: Record<string, string> = {
    // Serial types
    SERIAL: 'integer',
    SMALLSERIAL: 'smallint',
    BIGSERIAL: 'bigint',
    SERIAL2: 'smallint',
    SERIAL4: 'integer',
    SERIAL8: 'bigint',

    // Character types
    VARCHAR: 'character varying',
    CHAR: 'character',
    TEXT: 'text',

    // Integer types
    INT: 'integer',
    INTEGER: 'integer',
    INT2: 'smallint',
    INT4: 'integer',
    INT8: 'bigint',
    SMALLINT: 'smallint',
    BIGINT: 'bigint',

    // Floating-point types
    REAL: 'real',
    DOUBLE: 'double precision',
    FLOAT: 'double precision',
    FLOAT4: 'real',
    FLOAT8: 'double precision',
    NUMERIC: 'numeric',
    DECIMAL: 'numeric',

    // Date/time types
    TIMESTAMP: 'timestamp without time zone',
    TIMESTAMPTZ: 'timestamp with time zone',
    DATE: 'date',
    TIME: 'time without time zone',
    TIMETZ: 'time with time zone',

    // Boolean
    BOOLEAN: 'boolean',
    BOOL: 'boolean',

    // JSON
    JSON: 'json',
    JSONB: 'jsonb',

    // Other
    UUID: 'uuid',
    VARBIT: 'bit varying',
};
