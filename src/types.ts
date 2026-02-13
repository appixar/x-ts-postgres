// ─────────────────────────────────────────────
// x-postgres — TypeScript types
// ─────────────────────────────────────────────

/** Parsed field definition from YAML DSL */
export interface FieldDefinition {
    field: string;
    type: string;       // e.g. "VARCHAR(64)", "SERIAL", "NUMERIC(16,8)"
    nullable: string;   // "NULL", "NOT NULL", or "" (for SERIAL/id)
    key: string;        // "PRI", "UNI", or ""
    defaultValue: string | null; // raw default from YAML (before SQL normalization)
    extra: string;      // e.g. "" or other extras
}

/** Result of parsing a full table's field list from YAML */
export interface ParsedSchema {
    fields: Record<string, FieldDefinition>;
    individualIndexes: string[];
    compositeIndexes: Record<string, string[]>;
    compositeUniqueIndexes: Record<string, string[]>;
}

/** Custom field type definition (from custom_fields.yml) */
export interface CustomFieldDef {
    Type: string;
    Null?: string;
    Default?: string;
    Key?: string;
    Extra?: string;
}

/** Single node in a database cluster */
export interface DbNodeConfig {
    TYPE?: 'write' | 'read';
    NAME: string;
    HOST: string | string[];
    USER: string;
    PASS: string;
    PORT: number | string;
    PREF?: string;
    PATH?: string | string[];
    TENANT_KEYS?: TenantKeysConfig;
}

/** Tenant key resolution config */
export interface TenantKeysConfig {
    DBKEY?: string;
    TABLE?: string;
    FIELD?: string;
    WHERE?: string;
    CONTROLLER?: string;
    JSON_URL?: string;
}

/** Top-level postgres config shape */
export interface PostgresConfig {
    POSTGRES: {
        DB: Record<string, DbNodeConfig | DbNodeConfig[]>;
        CUSTOM_FIELDS?: Record<string, CustomFieldDef>;
    };
}

/** Row from information_schema.columns */
export interface DbColumnInfo {
    [key: string]: unknown;
    column_name: string;
    data_type: string;
    is_nullable: string;
    character_maximum_length: number | null;
    column_default: string | null;
    numeric_precision: number | null;
    numeric_scale: number | null;
}

/** Valid colors for terminal output */
export type LogColor = 'green' | 'yellow' | 'cyan' | 'gray' | 'red' | 'magenta' | 'blue' | 'white';

/** A queued SQL action */
export interface QueuedQuery {
    sql: string;
    mini: string;
    color: LogColor;
}
