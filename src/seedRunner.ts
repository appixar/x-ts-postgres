import { readFileSync, readdirSync, existsSync } from 'node:fs';
import { resolve, join } from 'node:path';
import chalk from 'chalk';
import YAML from 'yaml';
import { SchemaEngine } from './schemaEngine.js';
import * as log from './logger.js';

export interface SeedOptions {
    filename?: string;
    config?: string;
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
        log.info(`${filesToProcess.length} seed file(s) to process`);

        // PK cache
        const pkCache = new Map<string, string[]>();
        let totalInserted = 0;
        let totalUpdated = 0;
        let totalErrors = 0;

        for (const file of filesToProcess) {
            const fileName = file.split('/').pop()!;
            log.spin(`Processing ${fileName}...`);
            
            let data: Record<string, any[]>;
            try {
                const content = readFileSync(file, 'utf-8');
                data = YAML.parse(content);
            } catch (err) {
                log.fail(`Failed to parse ${fileName}: ${(err as Error).message}`);
                totalErrors++;
                continue;
            }

            if (!data || typeof data !== 'object') {
                log.warn(`${fileName}: empty or invalid YAML`);
                continue;
            }

            log.stopSpinner();
            console.log(`\n  ${chalk.white.bold(fileName)}`);

            const tableEntries = Object.entries(data);
            for (let t = 0; t < tableEntries.length; t++) {
                const [tableName, rows] = tableEntries[t];
                const isLastTable = t === tableEntries.length - 1;
                const connector = isLastTable ? '└─' : '├─';

                if (!Array.isArray(rows)) {
                    console.log(`    ${chalk.dim(connector)} ${chalk.yellow(`⚠ ${tableName}: not an array, skipped`)}`);
                    continue;
                }

                // Handle table prefix
                const finalTableName = (config.PREF && !tableName.startsWith(config.PREF))
                    ? config.PREF + tableName
                    : tableName;

                // Detect PK
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

                const pkColumns = pkCache.get(finalTableName)!;
                let inserted = 0;
                let updated = 0;
                let errors = 0;

                for (const row of rows) {
                    if (typeof row !== 'object' || row === null) continue;

                    const keys = Object.keys(row);
                    const values = Object.values(row);
                    if (keys.length === 0) continue;

                    try {
                        if (pkColumns.length > 0) {
                            const cols = keys.map(k => `"${k}"`).join(', ');
                            const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
                            const conflictCols = pkColumns.map(k => `"${k}"`).join(', ');
                            const updateCols = keys.filter(k => !pkColumns.includes(k));

                            let sql: string;
                            if (updateCols.length > 0) {
                                const setClauses = updateCols.map(k => `"${k}" = EXCLUDED."${k}"`).join(', ');
                                sql = `INSERT INTO "${finalTableName}" (${cols}) VALUES (${placeholders}) ON CONFLICT (${conflictCols}) DO UPDATE SET ${setClauses} RETURNING (xmax = 0) AS is_insert`;
                            } else {
                                sql = `INSERT INTO "${finalTableName}" (${cols}) VALUES (${placeholders}) ON CONFLICT (${conflictCols}) DO NOTHING`;
                            }

                            const result = await pg.query<{ is_insert: boolean }>(sql, values);
                            if (result.length > 0 && result[0].is_insert === false) {
                                updated++;
                            } else {
                                inserted++;
                            }
                        } else {
                            // No PK → check-then-insert
                            const whereClauses = keys.map((k, i) => `"${k}" = $${i + 1}`).join(' AND ');
                            const checkRes = await pg.query(
                                `SELECT 1 FROM "${finalTableName}" WHERE ${whereClauses} LIMIT 1`,
                                values
                            );

                            if (checkRes.length === 0) {
                                const cols = keys.map(k => `"${k}"`).join(', ');
                                const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
                                await pg.query(
                                    `INSERT INTO "${finalTableName}" (${cols}) VALUES (${placeholders})`,
                                    values
                                );
                                inserted++;
                            }
                        }
                    } catch (err) {
                        errors++;
                        totalErrors++;
                    }
                }

                totalInserted += inserted;
                totalUpdated += updated;

                // Build result line
                const parts: string[] = [];
                if (inserted > 0) parts.push(chalk.green(`${inserted} inserted`));
                if (updated > 0) parts.push(chalk.cyan(`${updated} updated`));
                if (errors > 0) parts.push(chalk.red(`${errors} failed`));
                
                const icon = errors > 0 ? chalk.red('✖') : chalk.green('✔');
                const label = parts.length > 0
                    ? `${finalTableName} ${chalk.dim('→')} ${parts.join(chalk.dim(', '))}`
                    : `${finalTableName} ${chalk.dim('→')} ${chalk.dim('up to date')}`;

                console.log(`    ${chalk.dim(connector)} ${icon} ${label}`);
            }
        }

        // Summary
        console.log('');
        const summaryParts: string[] = [];
        if (totalInserted > 0) summaryParts.push(chalk.green(`${totalInserted} inserted`));
        if (totalUpdated > 0) summaryParts.push(chalk.cyan(`${totalUpdated} updated`));
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
