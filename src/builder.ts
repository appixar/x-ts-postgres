// ─────────────────────────────────────────────
// x-postgres — Builder (orchestrator)
// ─────────────────────────────────────────────
// Port of PgBuilder.up() — loads config, scans YAML,
// diffs against live DB, and executes migration queries.

import { readFileSync, readdirSync, existsSync, statSync } from 'node:fs';
import { resolve, join } from 'node:path';
import YAML from 'yaml';
import { confirm } from '@inquirer/prompts';
import Table from 'cli-table3';
import { loadConfig, resolveSchemaPath, type LoadedConfig } from './configLoader.js';
import { parseSchema } from './schemaParser.js';
import { generateCreateTable, generateDropTable, generateCreateDatabase } from './sqlGenerator.js';
import { generateUpdateTable, type DiffContext } from './diffEngine.js';
import { PgService } from './pgService.js';
import * as log from './logger.js';
import type { QueuedQuery, DbColumnInfo } from './types.js';

export interface BuilderOptions {
    mute?: boolean;
    create?: boolean;
    name?: string;
    tenant?: string;
    dry?: boolean;
    config?: string;
    dropOrphans?: boolean;
}

export interface MigrationResult {
    executed: number;
    failed: { sql: string; error: string }[];
    total: number;
}

export async function up(options: BuilderOptions = {}): Promise<MigrationResult> {
    const mute = options.mute ?? false;
    const dryRun = options.dry ?? false;
    const result: MigrationResult = { executed: 0, failed: [], total: 0 };

    // Load config
    let cfg: LoadedConfig;
    if (!mute) log.spin('Loading configuration...');
    try {
        cfg = loadConfig(options.config);
        if (!mute) log.stopSpinner();
    } catch (err) {
        if (!mute) log.fail('Failed to load config');
        const msg = (err as Error).message;
        throw new Error(`Config error: ${msg}`);
    }

    const { postgres, customFields, configDir } = cfg;

    for (const [dbId, dbConf] of Object.entries(postgres.DB)) {
        const nodes = Array.isArray(dbConf) ? dbConf : [dbConf];
        const writeNode = nodes.find(n => n.TYPE === 'write') ?? nodes[0];

        if (options.tenant && !writeNode.TENANT_KEYS) continue;
        if (options.name) {
            if (options.name !== writeNode.NAME && !writeNode.TENANT_KEYS) continue;
        }

        if (!mute) log.header(`${dbId} (${writeNode.NAME})`, 'cyan');

        let databasePaths: string[] = [];
        if (writeNode.PATH) {
            const paths = Array.isArray(writeNode.PATH) ? writeNode.PATH : [writeNode.PATH];
            databasePaths = paths.map(p => resolveSchemaPath(p, configDir));
        } else {
            const defaultPath = resolve(configDir, 'database');
            if (existsSync(defaultPath)) databasePaths = [defaultPath];
        }

        if (!mute) log.spin('Connecting to database...');
        const pg = new PgService(dbConf, dbId);
        const allQueries: QueuedQuery[] = [];
        let createDbCount = 0;

        if (options.create) {
            try {
                const adminPool = pg.getAdminPool();
                const res = await adminPool.query('SELECT datname FROM pg_database WHERE datname = $1', [writeNode.NAME]);
                if (res.rows.length === 0) {
                    const q = generateCreateDatabase(writeNode.NAME);
                    allQueries.push(q);
                    if (!mute) log.info(`Database '${writeNode.NAME}' will be created`);
                    createDbCount++;
                }
            } catch (err) {
                log.warn(`Failed to check database existence: ${(err as Error).message}`);
            }
        }
        if (!mute) log.stopSpinner();

        if (createDbCount > 0 && allQueries.length > 0) {
            if (!dryRun) {
                if (!mute) {
                    const ok = await confirm({ message: 'Create database first?', default: true });
                    if (!ok) { log.warn('Aborting!'); continue; }
                }
                const adminPool = pg.getAdminPool();
                if (!mute) log.spin('Creating database...');
                for (const q of allQueries) {
                    await adminPool.query(q.sql);
                }
                if (!mute) log.succeed(`Database created. Re-running migration...`);
                allQueries.length = 0;
            }
        }

        // Get existing tables
        if (!mute) log.spin('Analyzing database structure...');
        let tablesReal: string[] = [];
        try {
            const t = await pg.query<{ table_name: string }>(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            );
            tablesReal = t.map(r => r.table_name);
        } catch {
            if (!mute) log.warn('Could not fetch existing tables');
        }

        // Process YAML
        const tablesNew: string[] = [];

        for (const schemaDir of databasePaths) {
            if (!existsSync(schemaDir) || !statSync(schemaDir).isDirectory()) continue;
            const files = readdirSync(schemaDir).filter(f => f.endsWith('.yml') || f.endsWith('.yaml'));

            if (!mute) log.spin(`Reading ${files.length} schema files...`);

            for (const fn of files) {
                const fp = join(schemaDir, fn);
                let data: Record<string, Record<string, string>>;
                try {
                    data = YAML.parse(readFileSync(fp, 'utf-8'));
                    if (!data || typeof data !== 'object') continue;
                } catch {
                    continue;
                }

                for (let [tableName, tableCols] of Object.entries(data)) {
                    if (!tableCols || typeof tableCols !== 'object') continue;
                    if (tableName.startsWith('~') && writeNode.PREF) {
                        tableName = writeNode.PREF + tableName.substring(1);
                    }
                    tablesNew.push(tableName);
                    if ((tableCols as Record<string, unknown>)['~ignore']) continue;

                    const schema = parseSchema(tableCols, customFields);
                    if (Object.keys(schema.fields).length === 0) continue;

                    if (tablesReal.includes(tableName)) {
                        // UPDATE
                        try {
                            const columns = await pg.query<DbColumnInfo>(
                                `SELECT column_name, data_type, is_nullable, character_maximum_length, column_default, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = :_tbl`,
                                { _tbl: tableName }
                            );
                            if (columns.length > 0) {
                                const currentCols: Record<string, DbColumnInfo> = {};
                                for (const col of columns) currentCols[col.column_name] = col;

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
                                };
                                allQueries.push(...generateUpdateTable(diffCtx));
                            }
                        } catch (err) {
                            log.error(`Error reading table ${tableName}: ${(err as Error).message}`);
                        }
                    } else {
                        // CREATE
                        allQueries.push(...generateCreateTable(tableName, schema));
                    }
                }
            }
        }
        if (!mute) log.stopSpinner();

        // Drop orphans
        if (options.dropOrphans) {
            for (const existingTable of tablesReal) {
                if (!tablesNew.includes(existingTable)) {
                    allQueries.push(generateDropTable(existingTable));
                }
            }
        } else if (!mute) {
            const orphans = tablesReal.filter(t => !tablesNew.includes(t));
            if (orphans.length > 0) {
                log.warn(`${orphans.length} orphan table(s) found. Use --drop-orphans to clean.`);
            }
        }

        // Execution logic
        if (allQueries.length > 0) {
            if (!mute) {
                const table = new Table({
                    head: ['Table', 'Type', 'Description'],
                    style: { head: ['cyan'] },
                    wordWrap: true
                });

                for (const q of allQueries) {
                    let typeColor = 'white';
                    if (q.type === 'DROP_TABLE' || q.type === 'DROP_COLUMN' || q.type === 'DROP_INDEX') {
                        typeColor = 'yellow'; // User requested yellow for DROP
                    } else if (q.type === 'CREATE_TABLE' || q.type === 'CREATE_DB') {
                        typeColor = 'green';  // User requested green for CREATE
                    } else {
                        typeColor = 'cyan';   // User requested blue (using cyan for visibility) for ADD/ALTER
                    }

                    // @ts-ignore
                    table.push([q.table, { content: q.type, style: { 'padding-left': 1, 'color': typeColor } }, q.description]);
                }

                console.log(table.toString());
                log.say(`\n${allQueries.length} changes to apply.`, 'cyan');
            }

            if (dryRun) {
                if (!mute) {
                    log.header('Dry run — checking SQL', 'yellow');
                    for (const q of allQueries) {
                        console.log(log.colorFns.gray(q.sql));
                    }
                }
            } else {
                if (!mute) {
                    const ok = await confirm({ message: 'Apply changes?', default: true });
                    if (!ok) { log.warn('Cancelled.'); continue; }
                    log.spin('Applying changes...');
                }

                for (const q of allQueries) {
                    try {
                        await pg.query(q.sql);
                        result.executed++;
                    } catch (err) {
                        const errMsg = (err as Error).message;
                        result.failed.push({ sql: q.sql, error: errMsg });
                        if (!mute) log.fail(`Failed: ${q.description} \n ${errMsg}`);
                    }
                }

                if (!mute) {
                    if (result.failed.length > 0) {
                        log.warn(`Finished with errors: ${result.executed} executed, ${result.failed.length} failed.`);
                    } else {
                        log.succeed('All changes applied successfully');
                    }
                }
            }
        } else {
            if (!mute) log.succeed('Database is up to date.');
        }

        result.total += allQueries.length;
    }

    await PgService.closeAll();
    return result;
}
