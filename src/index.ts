// ─────────────────────────────────────────────
// x-postgres — Public API exports
// ─────────────────────────────────────────────
// Use these exports when importing x-postgres as
// a library in Next.js or other Node.js projects.

export { Database, Database as PgService, type TransactionClient } from './pgService.js';
export { up, up as migrate, type BuilderOptions, type MigrationResult } from './builder.js';
export { parseSchema } from './schemaParser.js';
export { loadConfig } from './configLoader.js';
export { generateCreateTable, generateDropTable, generateCreateDatabase } from './sqlGenerator.js';
export { generateUpdateTable } from './diffEngine.js';
export { normalizeDefaultSql, buildDefaultClause, normalizeDbDefaultForCompare } from './defaultNormalizer.js';
export { POSTGRES_TYPE_DICTIONARY } from './typeDictionary.js';
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
