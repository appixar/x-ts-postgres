// ─────────────────────────────────────────────
// x-postgres — SQL generator
// ─────────────────────────────────────────────
// Generates CREATE TABLE, indexes, constraints, DROP TABLE, CREATE DATABASE.

import type { ParsedSchema, QueuedQuery } from './types.js';
import { buildDefaultClause } from './defaultNormalizer.js';
import * as log from './logger.js';

/**
 * Generate CREATE TABLE + all related indexes/constraints for a new table.
 *
 * ⚠ NOTE: Index creation uses CONCURRENTLY which **cannot run inside a transaction**.
 * The builder executes each query individually (no BEGIN/COMMIT wrapping) to avoid this.
 * If you call these queries programmatically, do NOT wrap them in a transaction block.
 */
export function generateCreateTable(
    table: string,
    schema: ParsedSchema,
    mute: boolean
): QueuedQuery[] {
    const queries: QueuedQuery[] = [];
    if (!mute) log.header(`∴ ${table}`, 'blue');

    const { fields, individualIndexes, compositeIndexes, compositeUniqueIndexes } = schema;

    // ─── CREATE TABLE ───
    const colDefs: string[] = [];
    const uniqueFields: string[] = [];

    for (const [k, v] of Object.entries(fields)) {
        const type = v.type;
        const nullable = v.nullable === 'NOT NULL' ? 'NOT NULL' : v.nullable === 'NULL' ? 'NULL' : '';
        const extra = v.extra;

        // SERIAL auto-creates default nextval, don't force DEFAULT
        let defaultClause = '';
        if (!type.includes('SERIAL')) {
            defaultClause = buildDefaultClause(v.defaultValue, type);
        }

        let colSql = `"${k}" ${type}`;
        if (nullable) colSql += ` ${nullable}`;
        if (defaultClause) colSql += ` ${defaultClause}`;
        if (extra) colSql += ` ${extra}`;

        if (v.key === 'PRI') colSql += ' PRIMARY KEY';
        if (v.key === 'UNI') uniqueFields.push(k);

        colDefs.push(colSql);
    }

    const createSql = `CREATE TABLE "${table}" (\n${colDefs.join(',\n')}\n);`;
    queries.push({ sql: createSql, mini: `CREATE TABLE "${table}" ...`, color: 'green' });
    if (!mute) log.say(`→ ${createSql}`, 'green');

    // ─── UNIQUE constraints ───
    for (const field of uniqueFields) {
        const sql = `ALTER TABLE "${table}" ADD CONSTRAINT "${table}_${field}_unique" UNIQUE ("${field}");`;
        const mini = `ADD UNIQUE "${table}_${field}_unique" ...`;
        queries.push({ sql, mini, color: 'cyan' });
        if (!mute) log.say(`→ ${sql}`, 'cyan');
    }

    // ─── Individual indexes ───
    for (const field of individualIndexes) {
        const sql = `CREATE INDEX CONCURRENTLY "${table}_${field}_idx" ON "${table}" ("${field}");`;
        const mini = `ADD INDEX "${table}_${field}_idx" ...`;
        queries.push({ sql, mini, color: 'cyan' });
        if (!mute) log.say(`→ ${sql}`, 'cyan');
    }

    // ─── Composite indexes ───
    for (const [indexName, columns] of Object.entries(compositeIndexes)) {
        const colsStr = columns.map(c => `"${c}"`).join(', ');
        const sql = `CREATE INDEX CONCURRENTLY "${table}_${indexName}_idx" ON "${table}" (${colsStr});`;
        const mini = `ADD INDEX "${table}_${indexName}_idx" ...`;
        queries.push({ sql, mini, color: 'cyan' });
        if (!mute) log.say(`→ ${sql}`, 'cyan');
    }

    // ─── Composite unique indexes ───
    for (const [indexName, columns] of Object.entries(compositeUniqueIndexes)) {
        const colsStr = columns.map(c => `"${c}"`).join(', ');
        const sql = `CREATE UNIQUE INDEX CONCURRENTLY "${table}_${indexName}_unique_idx" ON "${table}" (${colsStr});`;
        const mini = `ADD UNIQUE INDEX "${table}_${indexName}_unique_idx" ...`;
        queries.push({ sql, mini, color: 'cyan' });
        if (!mute) log.say(`→ ${sql}`, 'cyan');
    }

    return queries;
}

/**
 * Generate DROP TABLE CASCADE statement.
 */
export function generateDropTable(table: string, mute: boolean): QueuedQuery {
    if (!mute) log.header(`∴ ${table}`, 'blue');
    const sql = `DROP TABLE IF EXISTS "${table}" CASCADE;`;
    const mini = `DROP TABLE "${table}" ...`;
    if (!mute) log.say(`→ ${sql}`, 'yellow');
    return { sql, mini, color: 'yellow' };
}

/**
 * Generate CREATE DATABASE statement.
 */
export function generateCreateDatabase(name: string): QueuedQuery {
    const sql = `CREATE DATABASE "${name}" ENCODING 'UTF8';`;
    const mini = `CREATE DATABASE "${name}" ...`;
    return { sql, mini, color: 'green' };
}
