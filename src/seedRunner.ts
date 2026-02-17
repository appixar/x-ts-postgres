import { readFileSync, readdirSync, existsSync } from 'node:fs';
import { resolve, join } from 'node:path';
import chalk from 'chalk';
import { confirm } from '@inquirer/prompts';
import YAML from 'yaml';
import { SchemaEngine } from './schemaEngine.js';
import * as log from './logger.js';

export interface SeedOptions {
    filename?: string;
    config?: string;
    /** Skip per-table confirmation prompts */
    yes?: boolean;
    /** Comma-separated list of tables to seed */
    table?: string;
}

interface SeedTable {
    finalName: string;
    rows: Record<string, unknown>[];
    pkColumns: string[];
    sourceFile: string;
}

// ─── Deep value comparison ───
// Handles all PostgreSQL ↔ YAML type mismatches:
//   numeric/decimal → pg returns string "180", YAML has number 180
//   boolean         → pg returns true, YAML may have "true"
//   jsonb           → key order may differ between pg and YAML
//   timestamp       → pg returns Date, YAML has ISO string
//   null            → both sides

function deepEqual(a: unknown, b: unknown): boolean {
    // Identical or both null/undefined
    if (a === b) return true;
    if (a === null || a === undefined) return b === null || b === undefined;
    if (b === null || b === undefined) return false;

    // Date vs string
    if (a instanceof Date && typeof b === 'string') {
        return a.toISOString() === b || a.getTime() === new Date(b).getTime();
    }
    if (b instanceof Date && typeof a === 'string') {
        return b.toISOString() === a || b.getTime() === new Date(a).getTime();
    }
    if (a instanceof Date && b instanceof Date) {
        return a.getTime() === b.getTime();
    }

    // Number vs string (pg returns numeric/decimal as strings)
    if (typeof a === 'number' && typeof b === 'string') return String(a) === b || a === Number(b);
    if (typeof b === 'number' && typeof a === 'string') return String(b) === a || b === Number(a);

    // Boolean vs string
    if (typeof a === 'boolean' && typeof b === 'string') return String(a) === b;
    if (typeof b === 'boolean' && typeof a === 'string') return String(b) === a;

    // Object/Array — deep recursive (handles JSON key order differences)
    if (typeof a === 'object' && typeof b === 'object') {
        const aObj = a as Record<string, unknown>;
        const bObj = b as Record<string, unknown>;

        // Arrays
        if (Array.isArray(a) && Array.isArray(b)) {
            if (a.length !== b.length) return false;
            return a.every((v, i) => deepEqual(v, b[i]));
        }
        if (Array.isArray(a) !== Array.isArray(b)) return false;

        // Objects — compare all keys regardless of order
        const aKeys = Object.keys(aObj);
        const bKeys = Object.keys(bObj);
        if (aKeys.length !== bKeys.length) return false;

        return aKeys.every(k => k in bObj && deepEqual(aObj[k], bObj[k]));
    }

    // Fallback: string coercion
    return String(a) === String(b);
}


export async function runSeed(options: SeedOptions = {}): Promise<void> {
    const engine = new SchemaEngine({ config: options.config });
    const cfg = engine.getConfig();
    const { seedPath } = cfg;

    if (!existsSync(seedPath)) {
        log.warn(`Seed directory not found at: ${seedPath}`);
        log.info('Create a "seeds" directory or set SEED_PATH in xpg.config.yml');
        await engine.close();
        return;
    }

    // Identify files
    let filesToProcess: string[] = [];
    if (options.filename) {
        const fullPath = resolve(seedPath, options.filename);
        if (!existsSync(fullPath)) {
            const withExt = fullPath.endsWith('.yml') || fullPath.endsWith('.yaml')
                ? fullPath
                : fullPath + '.yml';
            if (existsSync(withExt)) {
                filesToProcess = [withExt];
            } else {
                log.fail(`Seed file not found: ${options.filename}`);
                await engine.close();
                return;
            }
        } else {
            filesToProcess = [fullPath];
        }
    } else {
        try {
            const files = readdirSync(seedPath);
            filesToProcess = files
                .filter(f => f.endsWith('.yml') || f.endsWith('.yaml'))
                .sort()
                .map(f => join(seedPath, f));
        } catch (err) {
            log.fail(`Error reading seed directory: ${(err as Error).message}`);
            await engine.close();
            return;
        }
    }

    if (filesToProcess.length === 0) {
        log.warn('No seed files found.');
        await engine.close();
        return;
    }

    try {
        const targets = engine.getTargets();
        const iter = targets.next();
        if (iter.done) {
            log.fail('No valid database target found.');
            return;
        }
        const target = iter.value;
        const { pg, config } = target;

        log.header(`Seeding ${target.id} (${target.config.NAME})`, 'magenta');

        // ────────────────────────────────────
        // PASS 1: Parse files, detect PKs
        // ────────────────────────────────────

        log.spin('Analyzing seed files...');

        const pkCache = new Map<string, string[]>();
        const seedTables: SeedTable[] = [];

        for (const file of filesToProcess) {
            const fileName = file.split('/').pop()!;

            let data: Record<string, unknown[]>;
            try {
                const content = readFileSync(file, 'utf-8');
                data = YAML.parse(content);
            } catch (err) {
                log.stopSpinner();
                log.fail(`Failed to parse ${fileName}: ${(err as Error).message}`);
                continue;
            }

            if (!data || typeof data !== 'object') continue;

            for (const [tableName, rows] of Object.entries(data)) {
                if (!Array.isArray(rows)) continue;

                const finalTableName = (config.PREF && !tableName.startsWith(config.PREF))
                    ? config.PREF + tableName
                    : tableName;

                if (!pkCache.has(finalTableName)) {
                    try {
                        const pkResult = await pg.query<{ column_name: string }>(
                            `SELECT kcu.column_name
                             FROM information_schema.table_constraints tc
                             JOIN information_schema.key_column_usage kcu
                               ON tc.constraint_name = kcu.constraint_name
                              AND tc.table_schema = kcu.table_schema
                             WHERE tc.table_name = :tbl
                               AND tc.constraint_type = 'PRIMARY KEY'
                             ORDER BY kcu.ordinal_position`,
                            { tbl: finalTableName }
                        );
                        pkCache.set(finalTableName, pkResult.map(r => r.column_name));
                    } catch {
                        pkCache.set(finalTableName, []);
                    }
                }

                seedTables.push({
                    finalName: finalTableName,
                    rows: rows.filter(r => typeof r === 'object' && r !== null) as Record<string, unknown>[],
                    pkColumns: pkCache.get(finalTableName)!,
                    sourceFile: fileName,
                });
            }
        }

        log.stopSpinner();

        // Filter by --table if specified
        if (options.table) {
            const requested = options.table.split(',').map(t => t.trim().toLowerCase());
            const before = seedTables.length;
            const filtered = seedTables.filter(st =>
                requested.some(r => st.finalName.toLowerCase() === r || st.finalName.toLowerCase().endsWith(r))
            );
            seedTables.length = 0;
            seedTables.push(...filtered);

            if (seedTables.length === 0) {
                log.warn(`No tables matched: ${options.table}`);
                log.info(`Available: ${Array.from(pkCache.keys()).join(', ')}`);
                return;
            }
        }

        if (seedTables.length === 0) {
            log.warn('No valid tables found in seed files.');
            return;
        }

        // ────────────────────────────────────
        // PASS 2: Per-table analyze → confirm → apply
        // ────────────────────────────────────

        let totalInserted = 0;
        let totalUpdated = 0;
        let totalSkipped = 0;
        let totalUpToDate = 0;
        let totalErrors = 0;

        for (const st of seedTables) {

            // ── Analyze: count inserts vs real updates ──
            log.spin(`Analyzing ${st.finalName}...`);

            let willInsert = 0;
            let willUpdate = 0;
            let willMatch = 0;

            if (st.pkColumns.length > 0) {
                for (const row of st.rows) {
                    const pkWhere = st.pkColumns
                        .map((k, i) => `"${k}" = $${i + 1}`)
                        .join(' AND ');
                    const pkValues = st.pkColumns.map(k => row[k]);

                    try {
                        const existing = await pg.query<Record<string, unknown>>(
                            `SELECT * FROM "${st.finalName}" WHERE ${pkWhere} LIMIT 1`,
                            pkValues
                        );

                        if (existing.length === 0) {
                            willInsert++;
                        } else {
                            // Deep compare non-PK fields
                            const dbRow = existing[0];
                            const nonPkKeys = Object.keys(row).filter(k => !st.pkColumns.includes(k));
                            const hasDiff = nonPkKeys.some(k => !deepEqual(row[k], dbRow[k]));
                            if (hasDiff) {
                                willUpdate++;
                            } else {
                                willMatch++;
                            }
                        }
                    } catch {
                        willInsert++;
                    }
                }
            } else {
                willInsert = st.rows.length;
            }

            log.stopSpinner();

            // ── Preview + Confirm ──
            const isUpToDate = willInsert === 0 && willUpdate === 0;

            if (isUpToDate) {
                console.log(`  ${chalk.green('✔')} ${chalk.white.bold(st.finalName)} ${chalk.dim('→')} ${chalk.dim('up to date')} ${chalk.dim(`(${willMatch} rows)`)}`);
                totalUpToDate++;
                console.log('');
                continue;
            }

            const parts: string[] = [];
            if (willInsert > 0) parts.push(chalk.green(`${willInsert} to insert`));
            if (willUpdate > 0) parts.push(chalk.cyan(`${willUpdate} to update`));
            if (willMatch > 0) parts.push(chalk.dim(`${willMatch} unchanged`));

            console.log(`  ${chalk.blue('◆')} ${chalk.white.bold(st.finalName)} ${chalk.dim('→')} ${parts.join(chalk.dim(', '))}`);

            if (!options.yes) {
                const ok = await confirm({
                    message: `  Apply?`,
                    default: true,
                });
                if (!ok) {
                    console.log(`    ${chalk.dim('○ skipped')}`);
                    totalSkipped += st.rows.length;
                    console.log('');
                    continue;
                }
            }

            // ── Apply ──
            let inserted = 0;
            let updated = 0;
            let errors = 0;

            for (const row of st.rows) {
                const keys = Object.keys(row);
                const values = Object.values(row);
                if (keys.length === 0) continue;

                try {
                    if (st.pkColumns.length > 0) {
                        const cols = keys.map(k => `"${k}"`).join(', ');
                        const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
                        const conflictCols = st.pkColumns.map(k => `"${k}"`).join(', ');
                        const updateCols = keys.filter(k => !st.pkColumns.includes(k));

                        let sql: string;
                        if (updateCols.length > 0) {
                            const setClauses = updateCols.map(k => `"${k}" = EXCLUDED."${k}"`).join(', ');
                            sql = `INSERT INTO "${st.finalName}" (${cols}) VALUES (${placeholders}) ON CONFLICT (${conflictCols}) DO UPDATE SET ${setClauses} RETURNING (xmax = 0) AS is_insert`;
                        } else {
                            sql = `INSERT INTO "${st.finalName}" (${cols}) VALUES (${placeholders}) ON CONFLICT (${conflictCols}) DO NOTHING`;
                        }

                        const result = await pg.query<{ is_insert: boolean }>(sql, values);
                        if (result.length > 0 && result[0].is_insert === false) {
                            updated++;
                        } else {
                            inserted++;
                        }
                    } else {
                        const whereClauses = keys.map((k, idx) => `"${k}" = $${idx + 1}`).join(' AND ');
                        const checkRes = await pg.query(
                            `SELECT 1 FROM "${st.finalName}" WHERE ${whereClauses} LIMIT 1`,
                            values
                        );

                        if (checkRes.length === 0) {
                            const cols = keys.map(k => `"${k}"`).join(', ');
                            const placeholders = keys.map((_, idx) => `$${idx + 1}`).join(', ');
                            await pg.query(
                                `INSERT INTO "${st.finalName}" (${cols}) VALUES (${placeholders})`,
                                values
                            );
                            inserted++;
                        }
                    }
                } catch (err) {
                    errors++;
                    totalErrors++;
                    if (errors === 1) {
                        log.fail(`  ${st.finalName}: ${(err as Error).message}`);
                    }
                }
            }

            totalInserted += inserted;
            totalUpdated += updated;

            // Result line
            const resultParts: string[] = [];
            if (inserted > 0) resultParts.push(chalk.green(`${inserted} inserted`));
            if (updated > 0) resultParts.push(chalk.cyan(`${updated} updated`));
            if (errors > 0) resultParts.push(chalk.red(`${errors} failed`));

            const icon = errors > 0 ? chalk.red('✖') : chalk.green('✔');
            console.log(`    ${icon} ${resultParts.join(chalk.dim(', '))}`);
            console.log('');
        }

        // ────────────────────────────────────
        // Summary
        // ────────────────────────────────────

        const summaryParts: string[] = [];
        if (totalInserted > 0) summaryParts.push(chalk.green(`${totalInserted} inserted`));
        if (totalUpdated > 0) summaryParts.push(chalk.cyan(`${totalUpdated} updated`));
        if (totalUpToDate > 0) summaryParts.push(chalk.dim(`${totalUpToDate} up to date`));
        if (totalSkipped > 0) summaryParts.push(chalk.dim(`${totalSkipped} skipped`));
        if (totalErrors > 0) summaryParts.push(chalk.red(`${totalErrors} failed`));

        if (summaryParts.length > 0) {
            log.succeed(`Done — ${summaryParts.join(chalk.dim(', '))}`);
        } else {
            log.succeed('All data is up to date.');
        }
    } finally {
        await engine.close();
    }
}
