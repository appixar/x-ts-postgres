// ─────────────────────────────────────────────
// x-postgres — Diff engine
// ─────────────────────────────────────────────
// Compares live DB state vs YAML schema and generates
// minimal ALTER statements. Port of PHP updateTable().

import type { ParsedSchema, DbColumnInfo, QueuedQuery } from './types.js';
import { POSTGRES_TYPE_DICTIONARY } from './typeDictionary.js';
import { buildDefaultClause, normalizeDbDefaultForCompare } from './defaultNormalizer.js';
import * as log from './logger.js';

interface DbIndexRow { indexname: string }
interface DbConstraintRow { conname: string }

export interface DiffContext {
    table: string;
    schema: ParsedSchema;
    currentColumns: Record<string, DbColumnInfo>;
    existingIndexes: DbIndexRow[];
    existingUniques: DbConstraintRow[];
    mute: boolean;
}

/**
 * Generate ALTER statements to bring a live table in sync with YAML schema.
 */
export function generateUpdateTable(ctx: DiffContext): QueuedQuery[] {
    const { table, schema, currentColumns, existingIndexes, existingUniques, mute } = ctx;
    const queries: QueuedQuery[] = [];
    if (!mute) log.header(`∴ ${table}`, 'blue');

    const { fields, individualIndexes, compositeIndexes, compositeUniqueIndexes } = schema;

    // ─── Existing index/constraint names ───
    const existingIndexNames = existingIndexes.map(i => i.indexname);
    const existingUniqueNames = existingUniques.map(u => u.conname);

    // ─── Expected index/constraint names ───
    const expectedIndexes: string[] = [];
    const expectedUniqueNames: string[] = [];

    for (const field of individualIndexes) {
        expectedIndexes.push(`${table}_${field}_idx`);
    }
    for (const indexName of Object.keys(compositeIndexes)) {
        expectedIndexes.push(`${table}_${indexName}_idx`);
    }
    for (const indexName of Object.keys(compositeUniqueIndexes)) {
        expectedIndexes.push(`${table}_${indexName}_unique_idx`);
    }
    for (const [k, v] of Object.entries(fields)) {
        if (v.key === 'UNI') {
            expectedUniqueNames.push(`${table}_${k}_unique`);
            expectedIndexes.push(`${table}_${k}_unique`);
        }
        if (v.key === 'PRI') {
            expectedIndexes.push(`${table}_pkey`);
        }
    }

    // ─── 1. DROP columns no longer in YAML ───
    for (const column of Object.keys(currentColumns)) {
        if (!fields[column]) {
            const sql = `ALTER TABLE "${table}" DROP COLUMN "${column}";`;
            queries.push({ sql, mini: `DROP COLUMN "${table}"."${column}" ...`, color: 'yellow' });
            if (!mute) log.say(`→ ${sql}`, 'yellow');
        }
    }

    // ─── 2. DROP orphaned UNIQUE constraints ───
    for (const uniqueName of existingUniqueNames) {
        if (!expectedUniqueNames.includes(uniqueName)) {
            const sql = `ALTER TABLE "${table}" DROP CONSTRAINT "${uniqueName}";`;
            queries.push({ sql, mini: `DROP CONSTRAINT "${uniqueName}" ...`, color: 'yellow' });
            if (!mute) log.say(`→ ${sql}`, 'yellow');
        }
    }

    // ─── 3. DROP orphaned indexes ───
    for (const indexName of existingIndexNames) {
        if (!expectedIndexes.includes(indexName)) {
            const sql = `DROP INDEX IF EXISTS "${indexName}";`;
            queries.push({ sql, mini: `DROP INDEX "${indexName}" ...`, color: 'yellow' });
            if (!mute) log.say(`→ ${sql}`, 'yellow');
        }
    }

    // ─── 4. ADD new columns ───
    for (const [k, v] of Object.entries(fields)) {
        if (!currentColumns[k]) {
            const typeUpper = v.type;
            let defaultClause = '';
            if (!typeUpper.includes('SERIAL')) {
                defaultClause = buildDefaultClause(v.defaultValue, typeUpper);
            }
            const sql = `ALTER TABLE "${table}" ADD COLUMN "${k}" ${typeUpper} ${v.nullable} ${defaultClause} ${v.extra};`.replace(/\s+/g, ' ').trim();
            queries.push({ sql, mini: `ADD COLUMN "${table}"."${k}" ...`, color: 'cyan' });
            if (!mute) log.say(`→ ${sql}`, 'cyan');
        }
    }

    // ─── 5. ALTER column TYPE if changed ───
    for (const [k, v] of Object.entries(fields)) {
        if (!currentColumns[k]) continue;

        const typeMatch = v.type.match(/^(\w+)(?:\((\d+(?:,\d+)?)\))?$/);
        const configBaseType = typeMatch ? typeMatch[1] : v.type.split('(')[0];
        const configLength = typeMatch && typeMatch[2] ? typeMatch[2] : null;

        const mappedConfigType = POSTGRES_TYPE_DICTIONARY[configBaseType] ?? configBaseType.toLowerCase();

        const dbType = currentColumns[k].data_type.toLowerCase();
        const dbLength = currentColumns[k].character_maximum_length;

        if (mappedConfigType !== dbType) {
            const sql = `ALTER TABLE "${table}" ALTER COLUMN "${k}" TYPE ${v.type};`;
            queries.push({ sql, mini: `ALTER TYPE "${table}"."${k}" → ${v.type} ...`, color: 'cyan' });
            if (!mute) log.say(`→ ${sql}`, 'cyan');
        } else if (configLength !== null) {
            // For numeric/decimal, compare precision+scale from numeric_precision/numeric_scale
            const isNumericType = ['numeric', 'decimal'].includes(dbType) || ['NUMERIC', 'DECIMAL'].includes(configBaseType);
            if (isNumericType) {
                const dbPrecision = (currentColumns[k] as Record<string, unknown>).numeric_precision as number | null;
                const dbScale = (currentColumns[k] as Record<string, unknown>).numeric_scale as number | null;
                const [cfgPrecision, cfgScale] = configLength.split(',');
                const precisionMismatch = dbPrecision !== null && String(dbPrecision) !== cfgPrecision;
                const scaleMismatch = cfgScale && dbScale !== null && String(dbScale) !== cfgScale;
                if (precisionMismatch || scaleMismatch) {
                    const sql = `ALTER TABLE "${table}" ALTER COLUMN "${k}" TYPE ${v.type};`;
                    queries.push({ sql, mini: `ALTER TYPE "${table}"."${k}" → ${v.type} ...`, color: 'cyan' });
                    if (!mute) log.say(`→ ${sql}`, 'cyan');
                }
            } else if (dbLength !== null && String(dbLength) !== configLength.split(',')[0]) {
                const sql = `ALTER TABLE "${table}" ALTER COLUMN "${k}" TYPE ${v.type};`;
                queries.push({ sql, mini: `ALTER TYPE "${table}"."${k}" → ${v.type} ...`, color: 'cyan' });
                if (!mute) log.say(`→ ${sql}`, 'cyan');
            }
        }
    }

    // ─── 6. SET/DROP DEFAULT ───
    for (const [k, v] of Object.entries(fields)) {
        if (!currentColumns[k]) continue;

        const typeUpper = v.type;
        const dbDefaultRaw = currentColumns[k].column_default ?? '';

        // Skip SERIAL defaults (auto nextval)
        if (typeUpper.includes('SERIAL')) continue;

        // Skip PRI fields that have nextval (legacy serial)
        if (v.key === 'PRI' && dbDefaultRaw.toLowerCase().includes('nextval(')) continue;

        const configDefaultClause = buildDefaultClause(v.defaultValue, typeUpper);
        let configDefaultSql = '';
        if (configDefaultClause) {
            configDefaultSql = configDefaultClause.substring('DEFAULT '.length).trim();
        }

        const dbDefaultNorm = normalizeDbDefaultForCompare(dbDefaultRaw);
        const cfgDefaultNorm = normalizeDbDefaultForCompare(configDefaultSql);

        // YAML has no default but DB does → DROP
        if (cfgDefaultNorm === '' && dbDefaultNorm !== '') {
            const sql = `ALTER TABLE "${table}" ALTER COLUMN "${k}" DROP DEFAULT;`;
            queries.push({ sql, mini: `DROP DEFAULT "${table}"."${k}" ...`, color: 'cyan' });
            if (!mute) log.say(`→ ${sql}`, 'cyan');
            continue;
        }

        // YAML has default but differs from DB → SET
        if (cfgDefaultNorm !== '' && dbDefaultNorm !== cfgDefaultNorm) {
            const sql = `ALTER TABLE "${table}" ALTER COLUMN "${k}" SET DEFAULT ${configDefaultSql};`;
            queries.push({ sql, mini: `SET DEFAULT "${table}"."${k}" ...`, color: 'cyan' });
            if (!mute) log.say(`→ ${sql}`, 'cyan');
        }
    }

    // ─── 6b. ALTER NULL/NOT NULL if changed ───
    for (const [k, v] of Object.entries(fields)) {
        if (!currentColumns[k]) continue;
        // Skip SERIAL (always NOT NULL implicitly)
        if (v.type.includes('SERIAL')) continue;
        if (v.nullable === '') continue; // no constraint specified (e.g. id)

        const dbNullable = currentColumns[k].is_nullable; // 'YES' or 'NO'
        const yamlWantsNotNull = v.nullable === 'NOT NULL';
        const dbIsNotNull = dbNullable === 'NO';

        if (yamlWantsNotNull && !dbIsNotNull) {
            const sql = `ALTER TABLE "${table}" ALTER COLUMN "${k}" SET NOT NULL;`;
            queries.push({ sql, mini: `SET NOT NULL "${table}"."${k}" ...`, color: 'cyan' });
            if (!mute) log.say(`→ ${sql}`, 'cyan');
        } else if (!yamlWantsNotNull && dbIsNotNull) {
            const sql = `ALTER TABLE "${table}" ALTER COLUMN "${k}" DROP NOT NULL;`;
            queries.push({ sql, mini: `DROP NOT NULL "${table}"."${k}" ...`, color: 'cyan' });
            if (!mute) log.say(`→ ${sql}`, 'cyan');
        }
    }

    // ─── 7. CREATE missing individual indexes ───
    for (const field of individualIndexes) {
        const indexName = `${table}_${field}_idx`;
        if (!existingIndexNames.includes(indexName)) {
            const sql = `CREATE INDEX CONCURRENTLY "${indexName}" ON "${table}" ("${field}");`;
            queries.push({ sql, mini: `ADD INDEX "${indexName}" ...`, color: 'cyan' });
            if (!mute) log.say(`→ ${sql}`, 'cyan');
        }
    }

    // ─── 8. CREATE missing composite indexes ───
    for (const [indexName, columns] of Object.entries(compositeIndexes)) {
        const fullName = `${table}_${indexName}_idx`;
        if (!existingIndexNames.includes(fullName)) {
            const colsStr = columns.map(c => `"${c}"`).join(', ');
            const sql = `CREATE INDEX CONCURRENTLY "${fullName}" ON "${table}" (${colsStr});`;
            queries.push({ sql, mini: `ADD INDEX "${fullName}" ...`, color: 'cyan' });
            if (!mute) log.say(`→ ${sql}`, 'cyan');
        }
    }

    // ─── 9. CREATE missing composite unique indexes ───
    for (const [indexName, columns] of Object.entries(compositeUniqueIndexes)) {
        const fullName = `${table}_${indexName}_unique_idx`;
        if (!existingIndexNames.includes(fullName)) {
            const colsStr = columns.map(c => `"${c}"`).join(', ');
            const sql = `CREATE UNIQUE INDEX CONCURRENTLY "${fullName}" ON "${table}" (${colsStr});`;
            queries.push({ sql, mini: `ADD UNIQUE INDEX "${fullName}" ...`, color: 'cyan' });
            if (!mute) log.say(`→ ${sql}`, 'cyan');
        }
    }

    // ─── 10. CREATE missing UNIQUE constraints ───
    for (const [k, v] of Object.entries(fields)) {
        if (v.key === 'UNI') {
            const uniqueName = `${table}_${k}_unique`;
            if (!existingUniqueNames.includes(uniqueName)) {
                const sql = `ALTER TABLE "${table}" ADD CONSTRAINT "${uniqueName}" UNIQUE ("${k}");`;
                queries.push({ sql, mini: `ADD UNIQUE "${uniqueName}" ...`, color: 'cyan' });
                if (!mute) log.say(`→ ${sql}`, 'cyan');
            }
        }
    }

    // ─── Status ───
    if (queries.length === 0 && !mute) {
        log.say('✓ Table is up to date');
    }

    return queries;
}
