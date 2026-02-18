// ─────────────────────────────────────────────
// x-postgres — Public API exports
// ─────────────────────────────────────────────
// Use these exports when importing x-postgres as
// a library in Next.js or other Node.js projects.

export { Database, Database as PgService, type TransactionClient } from './pgService.js';
export { up, up as migrate, type BuilderOptions, type MigrationResult } from './builder.js';
export { parseSchema } from './schemaParser.js';
export { loadConfig } from './configLoader.js';
export { SchemaEngine, type TargetDb } from './schemaEngine.js';
export * as logger from './logger.js';

// Types
export type {
    FieldDefinition,
    ParsedSchema,
    DbNodeConfig,
    CustomFieldDef,
    PostgresConfig,
    DbColumnInfo,
    QueuedQuery,
    TenantKeysConfig,
    LogColor,
} from './types.js';
