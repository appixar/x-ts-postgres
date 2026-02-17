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
}

// ─── Types for the two-pass approach ───

interface SeedTable {
    /** Table name as written in YAML */ 
    yamlName: string;
    /** Final table name (with prefix applied) */
    finalName: string;
    /** Rows parsed from YAML */
    rows: Record<string, unknown>[];
    /** Primary key columns */
    pkColumns: string[];
    /** Source file name */
    sourceFile: string;
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

    // Identify files to process
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
        // PASS 1: Parse all files, detect PKs, build preview
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
                log.fail(`Failed to parse ${fileName}: ${(err as Error).message}`);
                continue;
            }

            if (!data || typeof data !== 'object') continue;

            for (const [tableName, rows] of Object.entries(data)) {
                if (!Array.isArray(rows)) continue;

                const finalTableName = (config.PREF && !tableName.startsWith(config.PREF))
                    ? config.PREF + tableName
                    : tableName;

                // Detect PK (cached)
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
                    yamlName: tableName,
                    finalName: finalTableName,
                    rows: rows.filter(r => typeof r === 'object' && r !== null) as Record<string, unknown>[],
                    pkColumns: pkCache.get(finalTableName)!,
                    sourceFile: fileName,
                });
            }
        }

        log.stopSpinner();

        if (seedTables.length === 0) {
            log.warn('No valid tables found in seed files.');
            return;
        }

        // ────────────────────────────────────
        // Preview
        // ────────────────────────────────────

        console.log('');
        for (let i = 0; i < seedTables.length; i++) {
            const st = seedTables[i];
            const isLast = i === seedTables.length - 1;
            const connector = isLast ? '└─' : '├─';
            const rowLabel = st.rows.length === 1 ? 'row' : 'rows';
            const mode = st.pkColumns.length > 0
                ? chalk.cyan('upsert')
                : chalk.yellow('insert');

            console.log(`  ${chalk.dim(connector)} ${chalk.white.bold(st.finalName)} ${chalk.dim('→')} ${chalk.green(`${st.rows.length} ${rowLabel}`)} ${chalk.dim('via')} ${mode} ${chalk.dim(`(${st.sourceFile})`)}`);
        }
        console.log('');

        // ────────────────────────────────────
        // PASS 2: Apply with per-table confirmation
        // ────────────────────────────────────

        let totalInserted = 0;
        let totalUpdated = 0;
        let totalSkipped = 0;
        let totalErrors = 0;

        for (let i = 0; i < seedTables.length; i++) {
            const st = seedTables[i];
            const rowLabel = st.rows.length === 1 ? 'row' : 'rows';

            // Confirm per table (unless --yes)
            if (!options.yes) {
                const ok = await confirm({
                    message: `Seed ${st.finalName} (${st.rows.length} ${rowLabel})?`,
                    default: true,
                });
                if (!ok) {
                    log.say(`  ${chalk.dim('○')} ${chalk.dim(st.finalName)} ${chalk.dim('skipped')}`, 'gray');
                    totalSkipped += st.rows.length;
                    continue;
                }
            }

            log.spin(`Seeding ${st.finalName}...`);

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
                        // No PK → check-then-insert
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
                        log.stopSpinner();
                        log.fail(`${st.finalName}: ${(err as Error).message}`);
                    }
                }
            }

            totalInserted += inserted;
            totalUpdated += updated;

            // Result line
            const parts: string[] = [];
            if (inserted > 0) parts.push(chalk.green(`${inserted} inserted`));
            if (updated > 0) parts.push(chalk.cyan(`${updated} updated`));
            if (errors > 0) parts.push(chalk.red(`${errors} failed`));

            const icon = errors > 0 ? chalk.red('✖') : chalk.green('✔');
            const label = parts.length > 0
                ? `${st.finalName} ${chalk.dim('→')} ${parts.join(chalk.dim(', '))}`
                : `${st.finalName} ${chalk.dim('→')} ${chalk.dim('up to date')}`;

            if (errors === 0) log.stopSpinner();
            log.say(`  ${icon} ${label}`);
        }

        // ────────────────────────────────────
        // Summary
        // ────────────────────────────────────

        console.log('');
        const summaryParts: string[] = [];
        if (totalInserted > 0) summaryParts.push(chalk.green(`${totalInserted} inserted`));
        if (totalUpdated > 0) summaryParts.push(chalk.cyan(`${totalUpdated} updated`));
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
