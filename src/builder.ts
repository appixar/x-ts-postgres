// ─────────────────────────────────────────────
// x-postgres — Builder (orchestrator)
// ─────────────────────────────────────────────
// Port of PgBuilder.up() — loads config, scans YAML,
// diffs against live DB, and executes migration queries.

import { readFileSync, readdirSync, existsSync, statSync } from 'node:fs';
import { resolve, join } from 'node:path';
import YAML from 'yaml';
import * as readline from 'node:readline';
import { loadConfig, resolveSchemaPath, type LoadedConfig } from './configLoader.js';
import { parseSchema } from './schemaParser.js';
import { generateCreateTable, generateDropTable, generateCreateDatabase } from './sqlGenerator.js';
import { generateUpdateTable, type DiffContext } from './diffEngine.js';
import { PgService } from './pgService.js';
import * as log from './logger.js';
import type { QueuedQuery, DbColumnInfo, DbNodeConfig } from './types.js';

export interface BuilderOptions {
    mute?: boolean;
    create?: boolean;
    name?: string;
    tenant?: string;
    dry?: boolean;
    config?: string;
    /** If true, DROP tables that exist in DB but not in YAML. Default: false (safe). */
    dropOrphans?: boolean;
}

export interface MigrationResult {
    executed: number;
    failed: { sql: string; error: string }[];
    total: number;
}

/**
 * Prompt the user for yes/no confirmation.
 */
async function promptConfirm(message: string): Promise<boolean> {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    return new Promise(resolve => {
        rl.question(message, answer => {
            rl.close();
            resolve(answer.trim() === '1' || answer.trim().toLowerCase() === 'y');
        });
    });
}

/**
 * Run the full database migration flow.
 */
export async function up(options: BuilderOptions = {}): Promise<MigrationResult> {
    const mute = options.mute ?? false;
    const dryRun = options.dry ?? false;
    const result: MigrationResult = { executed: 0, failed: [], total: 0 };

    // Load config
    let cfg: LoadedConfig;
    try {
        cfg = loadConfig(options.config);
    } catch (err) {
        const msg = (err as Error).message;
        throw new Error(`Config error: ${msg}. Please verify your xpg.config.yml or config/postgres.yml`);
    }

    const { postgres, customFields, configDir } = cfg;

    // Iterate database clusters
    for (const [dbId, dbConf] of Object.entries(postgres.DB)) {
        // Normalize: could be array (multi-node) or single object
        const nodes = Array.isArray(dbConf) ? dbConf : [dbConf];
        const writeNode = nodes.find(n => n.TYPE === 'write') ?? nodes[0];

        // Filter by --tenant
        if (options.tenant && !writeNode.TENANT_KEYS) continue;

        // Filter by --name
        if (options.name) {
            if (options.name !== writeNode.NAME && !writeNode.TENANT_KEYS) continue;
        }

        if (!mute) log.say(`\n► PostgreSQL '${dbId}' ...`, 'cyan');

        // Resolve schema paths
        let databasePaths: string[] = [];
        if (writeNode.PATH) {
            const paths = Array.isArray(writeNode.PATH) ? writeNode.PATH : [writeNode.PATH];
            databasePaths = paths.map(p => resolveSchemaPath(p, configDir));
        } else {
            // Default: database/ dir in configDir
            const defaultPath = resolve(configDir, 'database');
            if (existsSync(defaultPath)) databasePaths = [defaultPath];
        }

        // Connect
        const pg = new PgService(dbConf, dbId);
        const allQueries: QueuedQuery[] = [];
        let createDbCount = 0;

        // --create: check if database exists
        if (options.create) {
            try {
                const adminPool = pg.getAdminPool();
                const res = await adminPool.query('SELECT datname FROM pg_database WHERE datname = $1', [writeNode.NAME]);
                if (res.rows.length === 0) {
                    const q = generateCreateDatabase(writeNode.NAME);
                    allQueries.push(q);
                    if (!mute) log.say(`→ ${q.sql}`, 'green');
                    createDbCount++;
                }
            } catch (err) {
                log.error(`Failed to check database existence: ${(err as Error).message}`);
            }
        }

        // Skip schema processing if we're only creating the database
        if (createDbCount > 0 && allQueries.length > 0) {
            // Execute create database first, then re-run
            if (!dryRun) {
                if (!mute) {
                    console.log('');
                    const ok = await promptConfirm('Create database first? (1: Yes, 0: No): ');
                    if (!ok) { log.warn('Aborting!'); continue; }
                }
                const adminPool = pg.getAdminPool();
                for (const q of allQueries) {
                    await adminPool.query(q.sql);
                }
                if (!mute) log.success(`Database created. Re-running schema migration...`);
                allQueries.length = 0;
            }
        }

        // Get existing tables
        let tablesReal: string[] = [];
        try {
            const t = await pg.query<{ table_name: string }>(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            );
            tablesReal = t.map(r => r.table_name);
        } catch {
            if (!mute) log.warn('Could not fetch existing tables (database may not exist yet)');
        }

        // Process YAML schema files
        const tablesNew: string[] = [];

        for (const schemaDir of databasePaths) {
            if (!existsSync(schemaDir) || !statSync(schemaDir).isDirectory()) continue;

            const files = readdirSync(schemaDir).filter(f => f.endsWith('.yml') || f.endsWith('.yaml'));

            for (const fn of files) {
                const fp = join(schemaDir, fn);
                if (!mute) log.say(`❍ Processing: ${fp}`, 'magenta');

                let data: Record<string, Record<string, string>>;
                try {
                    data = YAML.parse(readFileSync(fp, 'utf-8'));
                    if (!data || typeof data !== 'object') {
                        if (!mute) log.warn('⚠ Invalid file format. Ignored.');
                        continue;
                    }
                } catch {
                    if (!mute) log.warn('⚠ Failed to parse YAML. Ignored.');
                    continue;
                }

                for (let [tableName, tableCols] of Object.entries(data)) {
                    if (!tableCols || typeof tableCols !== 'object') continue;

                    // Tenant prefix: ~tablename → PREF + tablename
                    if (tableName.startsWith('~') && writeNode.PREF) {
                        tableName = writeNode.PREF + tableName.substring(1);
                    }

                    tablesNew.push(tableName);

                    // Check for ~ignore
                    if ((tableCols as Record<string, unknown>)['~ignore']) continue;

                    const schema = parseSchema(tableCols, customFields);
                    if (Object.keys(schema.fields).length === 0) continue;

                    if (tablesReal.includes(tableName)) {
                        // ─── UPDATE existing table ───
                        try {
                            const columns = await pg.query<DbColumnInfo>(
                                `SELECT column_name, data_type, is_nullable, character_maximum_length, column_default, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = :_tbl`,
                                { _tbl: tableName }
                            );

                            if (columns.length > 0) {
                                const currentCols: Record<string, DbColumnInfo> = {};
                                for (const col of columns) {
                                    currentCols[col.column_name] = col;
                                }

                                const indexes = await pg.query<{ indexname: string }>(
                                    `SELECT indexname FROM pg_indexes WHERE tablename = :_tbl`,
                                    { _tbl: tableName }
                                );
                                const uniques = await pg.query<{ conname: string }>(
                                    `SELECT conname FROM pg_constraint WHERE conrelid = :_tbl::regclass AND contype = 'u'`,
                                    { _tbl: tableName }
                                );

                                const diffCtx: DiffContext = {
                                    table: tableName,
                                    schema,
                                    currentColumns: currentCols,
                                    existingIndexes: indexes,
                                    existingUniques: uniques,
                                    mute,
                                };

                                allQueries.push(...generateUpdateTable(diffCtx));
                            } else {
                                if (!mute) {
                                    log.header(`∴ ${tableName}`, 'blue');
                                    log.say('✓ Table is up to date');
                                }
                            }
                        } catch (err) {
                            log.error(`Error reading table ${tableName}: ${(err as Error).message}`);
                        }
                    } else {
                        // ─── CREATE new table ───
                        allQueries.push(...generateCreateTable(tableName, schema, mute));
                    }
                }
            }
        }

        // ─── DROP tables not in YAML (opt-in via --drop-orphans) ───
        if (options.dropOrphans) {
            for (const existingTable of tablesReal) {
                if (!tablesNew.includes(existingTable)) {
                    allQueries.push(generateDropTable(existingTable, mute));
                }
            }
        } else if (!mute) {
            const orphans = tablesReal.filter(t => !tablesNew.includes(t));
            if (orphans.length > 0) {
                log.warn(`⚠ ${orphans.length} table(s) in DB but not in YAML: ${orphans.join(', ')}`);
                log.warn('  Use --drop-orphans to drop them.');
            }
        }

        // ─── Execute or show ───
        if (allQueries.length > 0) {
            if (!mute) {
                log.say(`\n→ ${allQueries.length} requested actions for: ${dbId}`);
                log.say('→ Please verify:');
                for (const q of allQueries) {
                    log.say(`  → ${q.mini}`, q.color);
                }
            }

            if (dryRun) {
                if (!mute) {
                    log.header('🔍 Dry run — no changes applied', 'yellow');
                    for (const q of allQueries) {
                        log.say(q.sql, 'gray');
                    }
                }
            } else if (mute) {
                // Programmatic mode: execute without prompting (no stdin available)
                for (const q of allQueries) {
                    try {
                        await pg.query(q.sql);
                        result.executed++;
                    } catch (err) {
                        const errMsg = (err as Error).message;
                        result.failed.push({ sql: q.sql, error: errMsg });
                    }
                }
            } else {
                console.log('');
                console.log('Are you sure you want to do this? ☝');
                console.log('0: No');
                console.log('1: Yes');

                const ok = await promptConfirm('Choose an option: ');
                if (!ok) {
                    log.warn('Aborting!');
                    continue;
                }

                for (const q of allQueries) {
                    try {
                        await pg.query(q.sql);
                        result.executed++;
                    } catch (err) {
                        const errMsg = (err as Error).message;
                        result.failed.push({ sql: q.sql, error: errMsg });
                        log.error(`Failed: ${q.mini} — ${errMsg}`);
                    }
                }
            }
        }

        result.total += allQueries.length;
        log.header(`❤ Finished ${dbId}. Changes: ${allQueries.length}`);
    }

    await PgService.closeAll();
    return result;
}
