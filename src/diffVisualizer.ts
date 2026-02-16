// ─────────────────────────────────────────────
// x-postgres — Diff Visualizer
// ─────────────────────────────────────────────
// Shows the schema differences between local YAML and remote DB.
// Read-only operation.

import { readFileSync, readdirSync, existsSync, statSync } from 'node:fs';
import { resolve, join } from 'node:path';
import YAML from 'yaml';
import Table from 'cli-table3';
import { loadConfig, resolveSchemaPath, type LoadedConfig } from './configLoader.js';
import { parseSchema } from './schemaParser.js';
import { generateCreateTable, generateDropTable, generateCreateDatabase } from './sqlGenerator.js';
import { generateUpdateTable, type DiffContext } from './diffEngine.js';
import { PgService } from './pgService.js';
import * as log from './logger.js';
import type { QueuedQuery, DbColumnInfo } from './types.js';

export interface DiffOptions {
    name?: string;
    tenant?: string;
    config?: string;
    dropOrphans?: boolean;
}

export async function visualizeDiff(options: DiffOptions = {}): Promise<void> {
    // Load config
    let cfg: LoadedConfig;
    log.spin('Loading configuration...');
    try {
        cfg = loadConfig(options.config);
        log.stopSpinner();
    } catch (err) {
        log.fail('Failed to load config');
        throw new Error((err as Error).message);
    }

    const { postgres, customFields, configDir } = cfg;

    for (const [dbId, dbConf] of Object.entries(postgres.DB)) {
        const nodes = Array.isArray(dbConf) ? dbConf : [dbConf];
        const writeNode = nodes.find(n => n.TYPE === 'write') ?? nodes[0];

        if (options.tenant && !writeNode.TENANT_KEYS) continue;
        if (options.name) {
            if (options.name !== writeNode.NAME && !writeNode.TENANT_KEYS) continue;
        }

        log.header(`Schema Diff: ${dbId} (${writeNode.NAME})`, 'magenta');

        let databasePaths: string[] = [];
        if (writeNode.PATH) {
            const paths = Array.isArray(writeNode.PATH) ? writeNode.PATH : [writeNode.PATH];
            databasePaths = paths.map(p => resolveSchemaPath(p, configDir));
        } else {
            const defaultPath = resolve(configDir, 'database');
            if (existsSync(defaultPath)) databasePaths = [defaultPath];
        }

        log.spin('Connecting to database...');
        const pg = new PgService(dbConf, dbId);
        const allQueries: QueuedQuery[] = [];

        // Check DB existence
        let dbExists = true;
        try {
            const adminPool = pg.getAdminPool();
            const res = await adminPool.query('SELECT datname FROM pg_database WHERE datname = $1', [writeNode.NAME]);
            if (res.rows.length === 0) {
                dbExists = false;
                allQueries.push(generateCreateDatabase(writeNode.NAME));
            }
        } catch (err) {
            log.warn(`Failed to check database existence: ${(err as Error).message}`);
        }
        log.stopSpinner();

        if (dbExists) {
            log.spin('Analyzing database structure...');
            let tablesReal: string[] = [];
            try {
                const t = await pg.query<{ table_name: string }>(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                );
                tablesReal = t.map(r => r.table_name);
            } catch {
                log.warn('Could not fetch existing tables');
            }

            // Process YAML
            const tablesNew: string[] = [];

            for (const schemaDir of databasePaths) {
                if (!existsSync(schemaDir) || !statSync(schemaDir).isDirectory()) continue;
                const files = readdirSync(schemaDir).filter(f => f.endsWith('.yml') || f.endsWith('.yaml'));
                
                log.spin(`Reading ${files.length} schema files...`);

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
            log.stopSpinner();

            // Drop orphans
            if (options.dropOrphans) {
                for (const existingTable of tablesReal) {
                    if (!tablesNew.includes(existingTable)) {
                        allQueries.push(generateDropTable(existingTable));
                    }
                }
            } else {
                const orphans = tablesReal.filter(t => !tablesNew.includes(t));
                if (orphans.length > 0) {
                    log.warn(`⚠ ${orphans.length} orphan table(s) found (not shown in diff unless --drop-orphans is used)`);
                }
            }
        }

        // Show Diff
        if (allQueries.length > 0) {
            const table = new Table({
                head: ['Table', 'Type', 'Description'],
                style: { head: ['cyan'] },
                wordWrap: true
            });

            for (const q of allQueries) {
                let typeColor = 'white';
                if (q.type === 'DROP_TABLE' || q.type === 'DROP_COLUMN' || q.type === 'DROP_INDEX') {
                    typeColor = 'yellow';
                } else if (q.type === 'CREATE_TABLE' || q.type === 'CREATE_DB') {
                    typeColor = 'green';
                } else {
                    typeColor = 'cyan';
                }

                // @ts-ignore
                table.push([q.table, { content: q.type, style: { 'padding-left': 1, 'color': typeColor } }, q.description]);
            }

            console.log(table.toString());
            log.info(`${allQueries.length} differences found.`);
        } else {
            log.succeed('Schemas are in sync.');
        }
    }

    await PgService.closeAll();
}
