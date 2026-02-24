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
    /** Columns used for matching existing rows (PK if present in data, else UNIQUE) */
    matchColumns: string[];
    sourceFile: string;
}

// ─── Value normalization ───
// Converts pg driver types and YAML parsed types to a common
// representation so identical data always compares as equal.

function sortedStringify(obj: Record<string, unknown>): string {
    const sorted: Record<string, unknown> = {};
    for (const k of Object.keys(obj).sort()) {
        sorted[k] = normalizeValue(obj[k]);
    }
    return JSON.stringify(sorted);
}

// Extract wall-clock datetime string from a Date object.
// Uses LOCAL components (not UTC) because pg creates Date objects
// for "timestamp without time zone" using local time interpretation.
function dateToWallClock(d: Date): string {
    const pad = (n: number, len = 2) => String(n).padStart(len, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} `
        + `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}

// Strip timezone info from a date string → raw datetime
// "2026-02-15T01:25:11.068Z"     → "2026-02-15 01:25:11.068"
// "2026-02-15 01:25:11.068+00"   → "2026-02-15 01:25:11.068"
// "2026-02-15 01:25:11.068"      → "2026-02-15 01:25:11.068"
function stripTimezone(s: string): string {
    return s.replace('T', ' ').replace(/[Z]$/, '').replace(/[+-]\d{2}(:\d{2})?$/, '');
}

function normalizeValue(v: unknown): unknown {
    if (v === null || v === undefined) return null;

    // Date → wall-clock string (timezone-agnostic)
    if (v instanceof Date) return dateToWallClock(v);

    if (typeof v === 'object') {
        if (Array.isArray(v)) return v.map(normalizeValue);
        return sortedStringify(v as Record<string, unknown>);
    }

    if (typeof v === 'boolean') return v;
    if (typeof v === 'number') return v;

    if (typeof v === 'string') {
        // JSON string from text/varchar column → parse and normalize
        const trimmed = v.trim();
        if ((trimmed.startsWith('{') && trimmed.endsWith('}')) ||
            (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
            try {
                const parsed = JSON.parse(trimmed);
                if (typeof parsed === 'object' && parsed !== null) {
                    if (Array.isArray(parsed)) return parsed.map(normalizeValue);
                    return sortedStringify(parsed as Record<string, unknown>);
                }
            } catch { /* not JSON */ }
        }

        // Numeric string → number ("180", "2.50")
        if (trimmed !== '' && !isNaN(Number(trimmed))) {
            return Number(trimmed);
        }

        // Date string → strip timezone, normalize to wall-clock format
        // "2026-02-15T01:25:11.068Z" → "2026-02-15 01:25:11.068"
        if (/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}/.test(trimmed)) {
            return stripTimezone(trimmed);
        }

        return v;
    }

    return String(v);
}

function valuesMatch(a: unknown, b: unknown): boolean {
    const na = normalizeValue(a);
    const nb = normalizeValue(b);

    // Both arrays → element-wise
    if (Array.isArray(na) && Array.isArray(nb)) {
        if (na.length !== nb.length) return false;
        return na.every((v, i) => JSON.stringify(v) === JSON.stringify(nb[i]));
    }

    return na === nb;
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

                const pkCols = pkCache.get(finalTableName)!;
                const sampleRow = rows.find(r => typeof r === 'object' && r !== null) as Record<string, unknown> | undefined;
                const seedKeys = sampleRow ? Object.keys(sampleRow) : [];

                // Determine match columns: PK if in seed data, else UNIQUE columns
                let matchColumns = pkCols.filter(k => seedKeys.includes(k));

                if (matchColumns.length < pkCols.length) {
                    // PK not in seed data — find UNIQUE index columns via pg_indexes
                    try {
                        const idxRows = await pg.query<{ indexdef: string }>(
                            `SELECT indexdef FROM pg_indexes WHERE tablename = $1 AND schemaname = 'public'`,
                            [finalTableName]
                        );

                        for (const row of idxRows) {
                            const def = row.indexdef;
                            if (!def.includes('UNIQUE')) continue;

                            // Extract columns from "... (col1, col2)" 
                            const match = def.match(/\(([^)]+)\)/);
                            if (!match) continue;

                            const cols = match[1].split(',').map(c => c.trim().replace(/"/g, ''));
                            if (cols.every(c => seedKeys.includes(c))) {
                                matchColumns = cols;
                                break;
                            }
                        }
                    } catch (err) {
                        log.warn(`${finalTableName}: ${(err as Error).message}`);
                    }
                }

                if (matchColumns.length === 0) {
                    log.warn(`${finalTableName}: no matchable UNIQUE columns in seed — will insert`);
                }

                seedTables.push({
                    finalName: finalTableName,
                    rows: rows.filter(r => typeof r === 'object' && r !== null) as Record<string, unknown>[],
                    pkColumns: pkCols,
                    matchColumns,
                    sourceFile: fileName,
                });
            }
        }


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

        for (let i = 0; i < seedTables.length; i++) {
            const st = seedTables[i];

            // ── Analyze ──

            let willInsert = 0;
            let willUpdate = 0;
            let willMatch = 0;

            if (st.matchColumns.length > 0) {
                for (const row of st.rows) {
                    const matchWhere = st.matchColumns
                        .map((k, i) => `"${k}" = $${i + 1}`)
                        .join(' AND ');
                    const matchValues = st.matchColumns.map(k => row[k]);

                    try {
                        const existing = await pg.query<Record<string, unknown>>(
                            `SELECT * FROM "${st.finalName}" WHERE ${matchWhere} LIMIT 1`,
                            matchValues
                        );

                        if (existing.length === 0) {
                            willInsert++;
                        } else {
                            const dbRow = existing[0];
                            const nonMatchKeys = Object.keys(row).filter(k => !st.matchColumns.includes(k));
                            const hasDiff = nonMatchKeys.some(k => !valuesMatch(row[k], dbRow[k]));
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
                    theme: { prefix: '   ' },
                });
                if (!ok) {
                    console.log(`  ${chalk.dim('○ skipped')}`);
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
                    if (st.matchColumns.length > 0) {
                        const cols = keys.map(k => `"${k}"`).join(', ');
                        const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
                        const conflictCols = st.matchColumns.map(k => `"${k}"`).join(', ');
                        const updateCols = keys.filter(k => !st.matchColumns.includes(k));

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
            console.log(`  ${icon} ${resultParts.join(chalk.dim(', '))}`);
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
