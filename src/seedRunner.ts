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

// ─── Value normalization ───
// Converts pg driver types and YAML parsed types to a common
// representation so identical data always compares as equal.
//   numeric/decimal → pg returns string "180.00", YAML has number 180
//   timestamp       → pg returns Date, YAML has ISO string
//   jsonb           → key order may differ between pg and YAML
//   boolean         → should match directly but coerce for safety

function normalizeObject(obj: Record<string, unknown>): string {
    const sortedKeys = Object.keys(obj).sort();
    const normalized: Record<string, unknown> = {};
    for (const k of sortedKeys) {
        normalized[k] = normalize(obj[k]);
    }
    return JSON.stringify(normalized);
}

function normalize(v: unknown): unknown {
    if (v === null || v === undefined) return null;

    // Date → millisecond timestamp
    if (v instanceof Date) return v.getTime();

    // Object/Array → recursively normalize, then sorted stringify
    if (typeof v === 'object') {
        if (Array.isArray(v)) {
            return JSON.stringify(v.map(normalize));
        }
        return normalizeObject(v as Record<string, unknown>);
    }

    // Boolean → keep as-is
    if (typeof v === 'boolean') return v;

    // Number → keep as number for precision
    if (typeof v === 'number') return v;

    // String → try to interpret as a richer type
    if (typeof v === 'string') {
        // JSON string from text/varchar column storing JSON
        // e.g. '{"color":"#f472b6","toyType":"BEAR"}'
        if ((v.startsWith('{') && v.endsWith('}')) || (v.startsWith('[') && v.endsWith(']'))) {
            try {
                const parsed = JSON.parse(v);
                if (typeof parsed === 'object' && parsed !== null) {
                    if (Array.isArray(parsed)) {
                        return JSON.stringify(parsed.map(normalize));
                    }
                    return normalizeObject(parsed as Record<string, unknown>);
                }
            } catch { /* not valid JSON, treat as string */ }
        }

        // Numeric string from pg driver ("180", "2.50", "-3.14")
        if (v.trim() !== '' && !isNaN(Number(v))) {
            return Number(v);
        }

        // Date string → timestamp
        // Matches both "2026-02-15T01:25:11.068Z" and "2026-02-15 01:25:11.068"
        if (/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}/.test(v)) {
            const d = new Date(v.includes('T') ? v : v.replace(' ', 'T') + 'Z');
            if (!isNaN(d.getTime())) return d.getTime();
        }

        return v;
    }

    return String(v);
}

function valuesMatch(yamlVal: unknown, dbVal: unknown): boolean {
    return normalize(yamlVal) === normalize(dbVal);
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
            console.log('');

            // ── Analyze ──
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
                            const dbRow = existing[0];
                            const nonPkKeys = Object.keys(row).filter(k => !st.pkColumns.includes(k));
                            const hasDiff = nonPkKeys.some(k => !valuesMatch(row[k], dbRow[k]));
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

            // ── Preview ──
            const isUpToDate = willInsert === 0 && willUpdate === 0;

            if (isUpToDate) {
                console.log(`  ${chalk.green('✔')} ${chalk.white.bold(st.finalName)} ${chalk.dim(`— ${willMatch} rows, up to date`)}`);
                totalUpToDate++;
                continue;
            }

            const parts: string[] = [];
            if (willInsert > 0) parts.push(chalk.green(`${willInsert} to insert`));
            if (willUpdate > 0) parts.push(chalk.cyan(`${willUpdate} to update`));
            if (willMatch > 0) parts.push(chalk.dim(`${willMatch} unchanged`));

            console.log(`  ${chalk.blue('◆')} ${chalk.white.bold(st.finalName)} ${chalk.dim('—')} ${parts.join(chalk.dim(', '))}`);

            // ── Confirm ──
            if (!options.yes) {
                const ok = await confirm({
                    message: `Apply to ${st.finalName}?`,
                    default: true,
                });
                if (!ok) {
                    console.log(`  ${chalk.dim('  ○ skipped')}`);
                    totalSkipped += st.rows.length;
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

            // Result
            const resultParts: string[] = [];
            if (inserted > 0) resultParts.push(chalk.green(`${inserted} inserted`));
            if (updated > 0) resultParts.push(chalk.cyan(`${updated} updated`));
            if (errors > 0) resultParts.push(chalk.red(`${errors} failed`));

            const icon = errors > 0 ? chalk.red('✖') : chalk.green('✔');
            console.log(`    ${icon} ${resultParts.join(chalk.dim(', '))}`);
        }

        // ────────────────────────────────────
        // Summary
        // ────────────────────────────────────

        console.log('');
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
