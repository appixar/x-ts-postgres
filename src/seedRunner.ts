import { readFileSync, readdirSync, existsSync } from 'node:fs';
import { resolve, join } from 'node:path';
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

    // 2. Identify files to process
    let filesToProcess: string[] = [];
    if (options.filename) {
        const fullPath = resolve(seedPath, options.filename);
        if (!existsSync(fullPath)) {
            // Try adding .yml extension if missing
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
        // Read all .yml files in seed directory
        try {
            const files = readdirSync(seedPath);
            filesToProcess = files
                .filter(f => f.endsWith('.yml') || f.endsWith('.yaml'))
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
        // Get the first available target (seeding is usually single-target or main)
        const targets = engine.getTargets();
        const iter = targets.next();
        if (iter.done) {
             log.fail('No valid database target found.');
             return;
        }
        const target = iter.value;
        const { pg, config } = target;

        log.header(`Seeding ${target.id}`, 'magenta');

        for (const file of filesToProcess) {
            log.info(`Processing ${file.split('/').pop()}...`);
            
            let data: Record<string, any[]>;
            try {
                const content = readFileSync(file, 'utf-8');
                data = YAML.parse(content);
            } catch (err) {
                log.error(`Failed to parse YAML: ${(err as Error).message}`);
                continue;
            }

            if (!data || typeof data !== 'object') continue;

            for (const [tableName, rows] of Object.entries(data)) {
                if (!Array.isArray(rows)) {
                    log.warn(`Skipping key "${tableName}" (not an array)`);
                    continue;
                }

                // Handle table prefix if configured
                const finalTableName = (config.PREF && !tableName.startsWith(config.PREF))
                    ? config.PREF + tableName
                    : tableName;

                let insertedCount = 0;
                let skippedCount = 0;

                for (const row of rows) {
                    if (typeof row !== 'object' || row === null) continue;

                    const keys = Object.keys(row);
                    const values = Object.values(row);

                    if (keys.length === 0) continue;

                    // 4. Check existence logic
                    // WHERE key1 = $1 AND key2 = $2 ...
                    const whereClauses = keys.map((k, i) => `"${k}" = $${i + 1}`).join(' AND ');
                    
                    try {
                        const checkRes = await pg.query(
                            `SELECT 1 FROM "${finalTableName}" WHERE ${whereClauses} LIMIT 1`,
                            values
                        );

                        if (checkRes.length > 0) {
                            skippedCount++;
                        } else {
                            // 5. Insert logic
                            const cols = keys.map(k => `"${k}"`).join(', ');
                            const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
                            
                            await pg.query(
                                `INSERT INTO "${finalTableName}" (${cols}) VALUES (${placeholders})`,
                                values
                            );
                            insertedCount++;
                        }
                    } catch (err) {
                         log.fail(`Error on table ${finalTableName}: ${(err as Error).message}`);
                    }
                }

                if (insertedCount > 0 || skippedCount > 0) {
                     log.succeed(`${finalTableName}: ${insertedCount} inserted, ${skippedCount} skipped`);
                }
            }
        }
    } finally {
        await engine.close();
    }
}
