// ─────────────────────────────────────────────
// x-postgres — Diff engine
// ─────────────────────────────────────────────
// Compares live DB state vs YAML schema and generates
// minimal ALTER statements. Port of PHP updateTable().

import type { ParsedSchema, DbColumnInfo, QueuedQuery } from './types.js';
import { POSTGRES_TYPE_DICTIONARY } from './typeDictionary.js';
import { buildDefaultClause, normalizeDbDefaultForCompare } from './defaultNormalizer.js';

interface DbIndexRow { indexname: string }
interface DbConstraintRow { conname: string }

export interface DiffContext {
    table: string;
    schema: ParsedSchema;
    currentColumns: Record<string, DbColumnInfo>;
    existingIndexes: DbIndexRow[];
    existingUniques: DbConstraintRow[];
}

/**
 * Generate ALTER statements to bring a live table in sync with YAML schema.
 */
export function generateUpdateTable(ctx: DiffContext): QueuedQuery[] {
    const queries: QueuedQuery[] = [];
    
    queries.push(...diffColumns(ctx));
    queries.push(...diffConstraints(ctx));
    queries.push(...diffIndexes(ctx));

    return queries;
}

function diffColumns(ctx: DiffContext): QueuedQuery[] {
    const { table, schema, currentColumns } = ctx;
    const { fields } = schema;
    const queries: QueuedQuery[] = [];

    // 1. DROP columns no longer in YAML
    for (const column of Object.keys(currentColumns)) {
        if (!fields[column]) {
            const sql = `ALTER TABLE "${table}" DROP COLUMN "${column}";`;
            queries.push({
                sql,
                table,
                type: 'DROP_COLUMN',
                description: `Drop column ${column}`
            });
        }
    }

    // 2. ADD new columns
    for (const [k, v] of Object.entries(fields)) {
        if (!currentColumns[k]) {
            const typeUpper = v.type;
            let defaultClause = '';
            // Skip default for SERIAL as it's handled by type
            if (!typeUpper.includes('SERIAL')) {
                defaultClause = buildDefaultClause(v.defaultValue, typeUpper);
            }
            const sql = `ALTER TABLE "${table}" ADD COLUMN "${k}" ${typeUpper} ${v.nullable} ${defaultClause} ${v.extra};`.replace(/\s+/g, ' ').trim();
            queries.push({
                sql,
                table,
                type: 'ADD_COLUMN',
                description: `Add column ${k} (${typeUpper})`
            });
        }
    }

    // 3. ALTER column TYPE / DEFAULT / NULLABLE
    for (const [k, v] of Object.entries(fields)) {
        if (!currentColumns[k]) continue;

        const dbCol = currentColumns[k];
        
        // Type Changes
        queries.push(...checkTypeMismatch(table, k, v.type, dbCol));

        // Default Changes
        queries.push(...checkDefaultMismatch(table, k, v, dbCol));

        // Nullable Changes
        queries.push(...checkNullableMismatch(table, k, v, dbCol));
    }

    return queries;
}

function checkTypeMismatch(table: string, colName: string, configType: string, dbCol: DbColumnInfo): QueuedQuery[] {
    const queries: QueuedQuery[] = [];
    const typeMatch = configType.match(/^(\w+)(?:\((\d+(?:,\d+)?)\))?$/);
    const configBaseType = typeMatch ? typeMatch[1] : configType.split('(')[0];
    const configLength = typeMatch && typeMatch[2] ? typeMatch[2] : null;

    const mappedConfigType = POSTGRES_TYPE_DICTIONARY[configBaseType] ?? configBaseType.toLowerCase();
    const dbType = dbCol.data_type.toLowerCase();
    const dbLength = dbCol.character_maximum_length;

    if (mappedConfigType !== dbType) {
        const sql = `ALTER TABLE "${table}" ALTER COLUMN "${colName}" TYPE ${configType};`;
        queries.push({
            sql,
            table,
            type: 'ALTER_COLUMN',
            description: `Change type of ${colName} to ${configType}`
        });
    } else if (configLength !== null) {
        // Numeric precision/scale check
        const isNumericType = ['numeric', 'decimal'].includes(dbType) || ['NUMERIC', 'DECIMAL'].includes(configBaseType);
        if (isNumericType) {
            const dbPrecision = (dbCol as any).numeric_precision as number | null;
            const dbScale = (dbCol as any).numeric_scale as number | null;
            const [cfgPrecision, cfgScale] = configLength.split(',');
            
            const precisionMismatch = dbPrecision !== null && String(dbPrecision) !== cfgPrecision;
            const scaleMismatch = cfgScale && dbScale !== null && String(dbScale) !== cfgScale;
            
            if (precisionMismatch || scaleMismatch) {
                 const sql = `ALTER TABLE "${table}" ALTER COLUMN "${colName}" TYPE ${configType};`;
                 queries.push({
                     sql,
                     table,
                     type: 'ALTER_COLUMN',
                     description: `Change precision of ${colName} to ${configType}`
                 });
            }
        } else if (dbLength !== null && String(dbLength) !== configLength.split(',')[0]) {
             const sql = `ALTER TABLE "${table}" ALTER COLUMN "${colName}" TYPE ${configType};`;
             queries.push({
                 sql,
                 table,
                 type: 'ALTER_COLUMN',
                 description: `Change length of ${colName} to ${configType}`
             });
        }
    }
    return queries;
}

function checkDefaultMismatch(table: string, colName: string, fieldDef: any, dbCol: DbColumnInfo): QueuedQuery[] {
    const queries: QueuedQuery[] = [];
    const typeUpper = fieldDef.type;
    const dbDefaultRaw = dbCol.column_default ?? '';

    if (typeUpper.includes('SERIAL')) return [];
    if (fieldDef.key === 'PRI' && dbDefaultRaw.toLowerCase().includes('nextval(')) return [];

    const configDefaultClause = buildDefaultClause(fieldDef.defaultValue, typeUpper);
    let configDefaultSql = '';
    if (configDefaultClause) {
        configDefaultSql = configDefaultClause.substring('DEFAULT '.length).trim();
    }

    const dbDefaultNorm = normalizeDbDefaultForCompare(dbDefaultRaw);
    const cfgDefaultNorm = normalizeDbDefaultForCompare(configDefaultSql);

    if (cfgDefaultNorm === '' && dbDefaultNorm !== '') {
        const sql = `ALTER TABLE "${table}" ALTER COLUMN "${colName}" DROP DEFAULT;`;
        queries.push({
            sql,
            table,
            type: 'ALTER_COLUMN',
            description: `Drop default on ${colName}`
        });
    } else if (cfgDefaultNorm !== '' && dbDefaultNorm !== cfgDefaultNorm) {
        const sql = `ALTER TABLE "${table}" ALTER COLUMN "${colName}" SET DEFAULT ${configDefaultSql};`;
        queries.push({
            sql,
            table,
            type: 'ALTER_COLUMN',
            description: `Set default on ${colName}`
        });
    }

    return queries;
}

function checkNullableMismatch(table: string, colName: string, fieldDef: any, dbCol: DbColumnInfo): QueuedQuery[] {
    const queries: QueuedQuery[] = [];
    if (fieldDef.type.includes('SERIAL')) return [];
    if (fieldDef.nullable === '') return []; // No explicit constraint

    const dbNullable = dbCol.is_nullable; // YES/NO
    const yamlWantsNotNull = fieldDef.nullable === 'NOT NULL';
    const dbIsNotNull = dbNullable === 'NO';

    if (yamlWantsNotNull && !dbIsNotNull) {
        const sql = `ALTER TABLE "${table}" ALTER COLUMN "${colName}" SET NOT NULL;`;
        queries.push({
            sql,
            table,
            type: 'ALTER_COLUMN',
            description: `Set NOT NULL on ${colName}`
        });
    } else if (!yamlWantsNotNull && dbIsNotNull) {
        const sql = `ALTER TABLE "${table}" ALTER COLUMN "${colName}" DROP NOT NULL;`;
        queries.push({
            sql,
            table,
            type: 'ALTER_COLUMN',
            description: `Drop NOT NULL on ${colName}`
        });
    }
    return queries;
}

function diffConstraints(ctx: DiffContext): QueuedQuery[] {
    const { table, schema, existingUniques } = ctx;
    const queries: QueuedQuery[] = [];
    const existingUniqueNames = existingUniques.map(u => u.conname);

    // Identify expected UNIQUE constraints (from individual UNI fields)
    const expectedUniqueNames: string[] = [];
    for (const [k, v] of Object.entries(schema.fields)) {
        if (v.key === 'UNI') {
            expectedUniqueNames.push(`${table}_${k}_unique`);
        }
    }

    // DROP orphaned UNIQUE constraints
    for (const uniqueName of existingUniqueNames) {
        if (!expectedUniqueNames.includes(uniqueName)) {
            const sql = `ALTER TABLE "${table}" DROP CONSTRAINT "${uniqueName}";`;
            queries.push({
                sql,
                table,
                type: 'RAW',
                description: `Drop unique constraint ${uniqueName}`
            });
        }
    }

    // CREATE missing UNIQUE constraints
    for (const [k, v] of Object.entries(schema.fields)) {
        if (v.key === 'UNI') {
            const uniqueName = `${table}_${k}_unique`;
            if (!existingUniqueNames.includes(uniqueName)) {
                const sql = `ALTER TABLE "${table}" ADD CONSTRAINT "${uniqueName}" UNIQUE ("${k}");`;
                queries.push({
                    sql,
                    table,
                    type: 'ADD_INDEX', // Grouping with index/constraint additions
                    description: `Add unique constraint ${uniqueName}`
                });
            }
        }
    }

    return queries;
}

function diffIndexes(ctx: DiffContext): QueuedQuery[] {
    const { table, schema, existingIndexes } = ctx;
    const queries: QueuedQuery[] = [];
    const existingIndexNames = existingIndexes.map(i => i.indexname);
    const { fields, individualIndexes, compositeIndexes, compositeUniqueIndexes } = schema;

    // Build list of all expected indexes
    const expectedIndexes: string[] = [];
    for (const field of individualIndexes) expectedIndexes.push(`${table}_${field}_idx`);
    for (const name of Object.keys(compositeIndexes)) expectedIndexes.push(`${table}_${name}_idx`);
    for (const name of Object.keys(compositeUniqueIndexes)) expectedIndexes.push(`${table}_${name}_unique_idx`);
    for (const [k, v] of Object.entries(fields)) {
        if (v.key === 'UNI') expectedIndexes.push(`${table}_${k}_unique`);
        if (v.key === 'PRI') expectedIndexes.push(`${table}_pkey`);
    }

    // DROP orphaned indexes
    for (const indexName of existingIndexNames) {
        if (!expectedIndexes.includes(indexName)) {
            const sql = `DROP INDEX IF EXISTS "${indexName}";`;
            queries.push({
                sql,
                table,
                type: 'DROP_INDEX',
                description: `Drop index ${indexName}`
            });
        }
    }

    // CREATE missing Individual Indexes
    for (const field of individualIndexes) {
        const indexName = `${table}_${field}_idx`;
        if (!existingIndexNames.includes(indexName)) {
            const sql = `CREATE INDEX CONCURRENTLY "${indexName}" ON "${table}" ("${field}");`;
            queries.push({ sql, table, type: 'ADD_INDEX', description: `Add index ${indexName}` });
        }
    }

    // CREATE missing Composite Indexes
    for (const [indexName, columns] of Object.entries(compositeIndexes)) {
        const fullName = `${table}_${indexName}_idx`;
        if (!existingIndexNames.includes(fullName)) {
            const colsStr = columns.map(c => `"${c}"`).join(', ');
            const sql = `CREATE INDEX CONCURRENTLY "${fullName}" ON "${table}" (${colsStr});`;
            queries.push({ sql, table, type: 'ADD_INDEX', description: `Add composite index ${fullName}` });
        }
    }

    // CREATE missing Composite Unique Indexes
    for (const [indexName, columns] of Object.entries(compositeUniqueIndexes)) {
        const fullName = `${table}_${indexName}_unique_idx`;
        if (!existingIndexNames.includes(fullName)) {
            const colsStr = columns.map(c => `"${c}"`).join(', ');
            const sql = `CREATE UNIQUE INDEX CONCURRENTLY "${fullName}" ON "${table}" (${colsStr});`;
            queries.push({ sql, table, type: 'ADD_INDEX', description: `Add unique composite index ${fullName}` });
        }
    }

    return queries;
}
