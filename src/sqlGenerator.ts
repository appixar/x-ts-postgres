// ─────────────────────────────────────────────
// x-postgres — SQL generator
// ─────────────────────────────────────────────
// Generates CREATE TABLE, indexes, constraints, DROP TABLE, CREATE DATABASE.

import type { ParsedSchema, QueuedQuery } from './types.js';
import { buildDefaultClause } from './defaultNormalizer.js';

/**
 * Generate CREATE TABLE + all related indexes/constraints for a new table.
 *
 * ⚠ NOTE: Index creation uses CONCURRENTLY which **cannot run inside a transaction**.
 * The builder executes each query individually (no BEGIN/COMMIT wrapping) to avoid this.
 * If you call these queries programmatically, do NOT wrap them in a transaction block.
 */
export function generateCreateTable(
    table: string,
    schema: ParsedSchema
): QueuedQuery[] {
    const queries: QueuedQuery[] = [];

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
    queries.push({
        sql: createSql,
        table,
        type: 'CREATE_TABLE',
        description: `Create table ${table}`
    });

    // ─── UNIQUE constraints ───
    for (const field of uniqueFields) {
        const sql = `ALTER TABLE "${table}" ADD CONSTRAINT "${table}_${field}_unique" UNIQUE ("${field}");`;
        queries.push({
            sql,
            table,
            type: 'ADD_INDEX',
            description: `Add unique constraint ${table}_${field}_unique`
        });
    }

    // ─── Individual indexes ───
    for (const field of individualIndexes) {
        const sql = `CREATE INDEX CONCURRENTLY "${table}_${field}_idx" ON "${table}" ("${field}");`;
        queries.push({
            sql,
            table,
            type: 'ADD_INDEX',
            description: `Add index ${table}_${field}_idx`
        });
    }

    // ─── Composite indexes ───
    for (const [indexName, columns] of Object.entries(compositeIndexes)) {
        const colsStr = columns.map(c => `"${c}"`).join(', ');
        const sql = `CREATE INDEX CONCURRENTLY "${table}_${indexName}_idx" ON "${table}" (${colsStr});`;
        queries.push({
            sql,
            table,
            type: 'ADD_INDEX',
            description: `Add composite index ${table}_${indexName}_idx`
        });
    }

    // ─── Composite unique indexes ───
    for (const [indexName, columns] of Object.entries(compositeUniqueIndexes)) {
        const colsStr = columns.map(c => `"${c}"`).join(', ');
        const sql = `CREATE UNIQUE INDEX CONCURRENTLY "${table}_${indexName}_unique_idx" ON "${table}" (${colsStr});`;
        queries.push({
            sql,
            table,
            type: 'ADD_INDEX',
            description: `Add unique composite index ${table}_${indexName}_unique_idx`
        });
    }

    return queries;
}

/**
 * Generate DROP TABLE CASCADE statement.
 */
export function generateDropTable(table: string): QueuedQuery {
    return {
        sql: `DROP TABLE IF EXISTS "${table}" CASCADE;`,
        table,
        type: 'DROP_TABLE',
        description: `Drop table ${table}`
    };
}

/**
 * Generate CREATE DATABASE statement.
 */
export function generateCreateDatabase(name: string): QueuedQuery {
    return {
        sql: `CREATE DATABASE "${name}" ENCODING 'UTF8';`,
        table: '',
        type: 'CREATE_DB',
        description: `Create database ${name}`
    };
}
