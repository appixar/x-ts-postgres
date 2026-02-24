// ─────────────────────────────────────────────
// x-postgres — Public API exports
// ─────────────────────────────────────────────
// Use these exports when importing x-postgres as
// a library in Next.js or other Node.js projects.

export { Database, Database as PgService, type TransactionClient } from './database.js';
export { up, up as migrate, type BuilderOptions, type MigrationResult } from './migrator.js';
export { parseSchema } from './schemaParser.js';
export { loadConfig, type LoadedConfig } from './configLoader.js';
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

// ─────────────────────────────────────────────
// Default export — shorthand API
// ─────────────────────────────────────────────
// import xpg from '@appixar/xpg';
// const db = xpg.connect('main');

import { Database } from './database.js';
import { loadConfig as _loadConfig } from './configLoader.js';
import { up as _up } from './migrator.js';
import type { LoadedConfig } from './configLoader.js';
import type { DatabaseOptions } from './database.js';
import type { BuilderOptions, MigrationResult } from './migrator.js';

let _cachedConfig: LoadedConfig | null = null;

const xpg = {
    /**
     * Connect to a database cluster by name.
     * Automatically loads config on first call.
     *
     * @example
     * ```ts
     * const db = xpg.connect('main');
     * const users = await db.query('SELECT * FROM users');
     * ```
     */
    connect(clusterName: string = 'main', options?: DatabaseOptions): Database {
        if (!_cachedConfig) _cachedConfig = _loadConfig();
        const cluster = _cachedConfig.postgres.DB[clusterName];
        if (!cluster) throw new Error(`[xpg] Cluster "${clusterName}" not found in config`);
        return new Database(cluster, clusterName, options);
    },

    /** Load config manually (cached after first call). */
    loadConfig(configPath?: string): LoadedConfig {
        _cachedConfig = _loadConfig(configPath);
        return _cachedConfig;
    },

    /** Run database migrations. */
    up(options?: BuilderOptions): Promise<MigrationResult> {
        return _up(options);
    },

    /** Close all connection pools. */
    closeAll(): Promise<void> {
        return Database.closeAll();
    },
};

export default xpg;
