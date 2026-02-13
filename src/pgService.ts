// ─────────────────────────────────────────────
// x-postgres — PgService
// ─────────────────────────────────────────────
// PostgreSQL connection & query service.
// Port of PHP PgService with read/write splitting,
// multi-cluster, and connection pooling via pg.Pool.

import pg from 'pg';
import type { DbNodeConfig } from './types.js';

const { Pool } = pg;

interface PgServiceOptions {
    /** Cluster name (key in POSTGRES.DB config) */
    cluster?: string;
    /** Force primary (write) node */
    primary?: boolean;
}

// Pool registry — keyed by connection string to avoid duplicates
// while still being manageable per PgService instance.
const poolRegistry: Map<string, pg.Pool> = new Map();

/**
 * Resolve the read and write node configs from a cluster config entry.
 */
function resolveCluster(
    clusterConf: DbNodeConfig | DbNodeConfig[]
): { writeNode: DbNodeConfig; readNodes: DbNodeConfig[] } {
    // Single-node cluster
    if (!Array.isArray(clusterConf)) {
        return { writeNode: clusterConf, readNodes: [clusterConf] };
    }

    let writeNode: DbNodeConfig | null = null;
    const readNodes: DbNodeConfig[] = [];

    for (const node of clusterConf) {
        if (node.TYPE === 'write') {
            writeNode = node;
        } else if (node.TYPE === 'read') {
            // Support multiple hosts per read node
            const hosts = Array.isArray(node.HOST) ? node.HOST : [node.HOST];
            for (const host of hosts) {
                readNodes.push({ ...node, HOST: host });
            }
        }
    }

    if (!writeNode) throw new Error('No write node found in cluster');
    if (readNodes.length === 0) readNodes.push(writeNode);

    return { writeNode, readNodes };
}

/**
 * Get or create a connection pool for a specific node.
 */
function getPool(node: DbNodeConfig, key: string): pg.Pool {
    if (poolRegistry.has(key)) return poolRegistry.get(key)!;

    const host = Array.isArray(node.HOST) ? node.HOST[0] : node.HOST;

    const pool = new Pool({
        host,
        port: typeof node.PORT === 'string' ? parseInt(node.PORT, 10) : node.PORT,
        database: node.NAME,
        user: node.USER,
        password: node.PASS,
        max: 10,
        idleTimeoutMillis: 30000,
    });

    pool.on('error', (err) => {
        console.error(`[x-postgres] Pool error (${key}):`, err.message);
    });

    poolRegistry.set(key, pool);
    return pool;
}

const READ_COMMANDS = ['SELECT', 'SHOW', 'EXPLAIN', 'WITH'];

function isReadOnly(sql: string): boolean {
    const first = sql.trim().split(/\s+/)[0].toUpperCase();
    return READ_COMMANDS.includes(first);
}

export class PgService {
    private clusterName: string;
    private writeNode: DbNodeConfig;
    private readNodes: DbNodeConfig[];
    private forcePrimary: boolean;
    public error: string | null = null;

    constructor(
        clusterConf: DbNodeConfig | DbNodeConfig[],
        clusterName: string,
        options: PgServiceOptions = {}
    ) {
        this.clusterName = clusterName;
        this.forcePrimary = options.primary ?? false;
        const { writeNode, readNodes } = resolveCluster(clusterConf);
        this.writeNode = writeNode;
        this.readNodes = readNodes;
    }

    private getWritePool(): pg.Pool {
        return getPool(this.writeNode, `${this.clusterName}:write`);
    }

    private getReadPool(): pg.Pool {
        if (this.forcePrimary) return this.getWritePool();
        // Random read replica selection
        const idx = Math.floor(Math.random() * this.readNodes.length);
        return getPool(this.readNodes[idx], `${this.clusterName}:read:${idx}`);
    }

    /**
     * Create a barebone admin connection (without database name).
     * Used for CREATE DATABASE operations.
     */
    getAdminPool(): pg.Pool {
        const node = this.writeNode;
        const key = `${this.clusterName}:admin`;
        if (poolRegistry.has(key)) return poolRegistry.get(key)!;

        const host = Array.isArray(node.HOST) ? node.HOST[0] : node.HOST;
        const pool = new Pool({
            host,
            port: typeof node.PORT === 'string' ? parseInt(node.PORT, 10) : node.PORT,
            database: 'postgres', // connect to default db for admin
            user: node.USER,
            password: node.PASS,
            max: 2,
        });

        pool.on('error', (err) => {
            console.error(`[x-postgres] Admin pool error:`, err.message);
        });

        poolRegistry.set(key, pool);
        return pool;
    }

    /**
     * Execute a SQL query with optional named parameters.
     * Automatically routes to read or write pool.
     */
    async query<T extends Record<string, unknown> = Record<string, unknown>>(
        sql: string,
        params?: Record<string, unknown>
    ): Promise<T[]> {
        const pool = isReadOnly(sql) && !this.forcePrimary
            ? this.getReadPool()
            : this.getWritePool();

        try {
            // Convert named params :key to $N positional params
            let pgSql = sql;
            const values: unknown[] = [];

            if (params && Object.keys(params).length > 0) {
                let paramIndex = 0;
                // Use negative lookbehind to avoid matching ::typecast syntax
                pgSql = sql.replace(/(?<!:):([a-zA-Z_]\w*)/g, (_match, key) => {
                    if (params[key] !== undefined) {
                        paramIndex++;
                        values.push(params[key]);
                        return `$${paramIndex}`;
                    }
                    return _match;
                });
            }

            const result = await pool.query(pgSql, values.length > 0 ? values : undefined);
            return result.rows as T[];
        } catch (err) {
            this.error = (err as Error).message;
            throw err;
        }
    }

    /**
     * Insert a row into a table. Returns the last insert id (if available).
     */
    async insert(table: string, data: Record<string, unknown>): Promise<string | null> {
        const pool = this.getWritePool();
        const keys = Object.keys(data);
        const cols = keys.map(k => `"${k}"`).join(', ');
        const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
        const values = keys.map(k => {
            const v = data[k];
            if (v === 'NULL' || v === 'null' || v === '') return null;
            return v;
        });

        const sql = `INSERT INTO "${table}" (${cols}) VALUES (${placeholders}) RETURNING *`;

        try {
            const result = await pool.query(sql, values);
            return result.rows[0]?.id ?? null;
        } catch (err) {
            this.error = (err as Error).message;
            throw err;
        }
    }

    /**
     * Update rows in a table. Returns the number of affected rows.
     */
    async update(
        table: string,
        data: Record<string, unknown>,
        condition: Record<string, unknown> | string
    ): Promise<number> {
        const pool = this.getWritePool();
        const vals: unknown[] = [];
        let idx = 0;

        // SET clause
        const setClauses = Object.entries(data).map(([k, v]) => {
            if (v === 'NULL' || v === 'null' || v === '') return `"${k}" = NULL`;
            idx++;
            vals.push(v);
            return `"${k}" = $${idx}`;
        });

        // WHERE clause
        let whereClause: string;
        if (typeof condition === 'string') {
            whereClause = condition;
        } else {
            const whereParts = Object.entries(condition).map(([k, v]) => {
                if (v === 'NULL') return `"${k}" IS NULL`;
                if (v === '') return `"${k}" = ''`;
                idx++;
                vals.push(v);
                return `"${k}" = $${idx}`;
            });
            whereClause = whereParts.join(' AND ');
        }

        const sql = `UPDATE "${table}" SET ${setClauses.join(', ')} WHERE ${whereClause}`;

        try {
            const result = await pool.query(sql, vals);
            return result.rowCount ?? 0;
        } catch (err) {
            this.error = (err as Error).message;
            throw err;
        }
    }

    /**
     * Close all connection pools.
     */
    static async closeAll(): Promise<void> {
        const entries = Array.from(poolRegistry.entries());
        for (const [key, pool] of entries) {
            await pool.end();
            poolRegistry.delete(key);
        }
    }
}
