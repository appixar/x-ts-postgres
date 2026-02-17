// ─────────────────────────────────────────────
// x-postgres — Seed Dumper
// ─────────────────────────────────────────────
// Generates YAML seed files from live database data.
// Creates one .yml file per table in the seed directory.

import { writeFileSync, existsSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { confirm } from '@inquirer/prompts';
import YAML from 'yaml';
import { SchemaEngine } from './schemaEngine.js';
import * as log from './logger.js';

export interface SeedDumpOptions {
    /** Comma-separated list of tables to dump */
    tables?: string;
    /** Comma-separated list of tables to exclude */
    exclude?: string;
    /** Dump all tables without prompting for each */
    all?: boolean;
    /** Max rows per table (default: no limit) */
    limit?: number;
    /** Exclude auto-generated columns (SERIAL, DEFAULT now(), etc.) */
    skipAuto?: boolean;
    /** Path to config file */
    config?: string;
}

// Columns that are auto-generated and can be excluded with --skip-auto
const AUTO_DEFAULT_PATTERNS = [
    /^nextval\(/i,
    /^now\(\)/i,
    /^current_timestamp/i,
    /^gen_random_uuid\(\)/i,
    /^uuid_generate/i,
];

function isAutoColumn(columnDefault: string | null, dataType: string): boolean {
    // SERIAL types
    if (dataType === 'integer' && columnDefault?.includes('nextval(')) return true;
    if (dataType === 'bigint' && columnDefault?.includes('nextval(')) return true;

    if (!columnDefault) return false;
    return AUTO_DEFAULT_PATTERNS.some(p => p.test(columnDefault));
}

export async function runSeedDump(options: SeedDumpOptions = {}): Promise<void> {
    const engine = new SchemaEngine({ config: options.config });
    const cfg = engine.getConfig();
    const { seedPath } = cfg;

    // Ensure seed directory exists
    if (!existsSync(seedPath)) {
        mkdirSync(seedPath, { recursive: true });
        log.info(`Created seed directory: ${seedPath}`);
    }

    try {
        // Get first available target
        const targets = engine.getTargets();
        const iter = targets.next();
        if (iter.done) {
            log.fail('No valid database target found.');
            return;
        }
        const { pg, config, id } = iter.value;

        log.header(`Dumping seeds from ${id} (${config.NAME})`, 'magenta');

        // 1. Get all public tables
        const allTables = await pg.query<{ table_name: string }>(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name"
        );

        if (allTables.length === 0) {
            log.warn('No tables found in database.');
            return;
        }

        // 2. Filter tables
        let tablesToDump = allTables.map(t => t.table_name);

        if (options.tables) {
            const requested = options.tables.split(',').map(t => t.trim());
            const missing = requested.filter(t => !tablesToDump.includes(t));
            if (missing.length > 0) {
                log.warn(`Tables not found: ${missing.join(', ')}`);
            }
            tablesToDump = tablesToDump.filter(t => requested.includes(t));
        }

        if (options.exclude) {
            const excluded = options.exclude.split(',').map(t => t.trim());
            tablesToDump = tablesToDump.filter(t => !excluded.includes(t));
        }

        if (tablesToDump.length === 0) {
            log.warn('No tables to dump after filtering.');
            return;
        }

        log.info(`Found ${tablesToDump.length} table(s)`);

        // 3. Get auto-column info if --skip-auto is set
        let autoColumns: Map<string, Set<string>> | null = null;
        if (options.skipAuto) {
            autoColumns = new Map();
            for (const table of tablesToDump) {
                const cols = await pg.query<{ column_name: string; column_default: string | null; data_type: string }>(
                    `SELECT column_name, column_default, data_type FROM information_schema.columns WHERE table_name = :tbl`,
                    { tbl: table }
                );
                const autoSet = new Set<string>();
                for (const col of cols) {
                    if (isAutoColumn(col.column_default, col.data_type)) {
                        autoSet.add(col.column_name);
                    }
                }
                if (autoSet.size > 0) autoColumns.set(table, autoSet);
            }
        }

        // 4. Dump each table
        let dumpedCount = 0;

        for (const table of tablesToDump) {
            // Prompt for confirmation unless --all
            if (!options.all) {
                const ok = await confirm({
                    message: `Dump ${table}?`,
                    default: true,
                });
                if (!ok) {
                    log.say(`  Skipped ${table}`, 'gray');
                    continue;
                }
            }

            // Fetch rows
            let sql = `SELECT * FROM "${table}" ORDER BY 1`;
            if (options.limit) sql += ` LIMIT ${options.limit}`;

            const rows = await pg.query<Record<string, unknown>>(sql);

            if (rows.length === 0) {
                log.say(`  ${table}: 0 rows (empty)`, 'gray');
                continue;
            }

            // Filter out auto columns if requested
            let cleanRows = rows;
            if (autoColumns?.has(table)) {
                const skip = autoColumns.get(table)!;
                cleanRows = rows.map(row => {
                    const clean: Record<string, unknown> = {};
                    for (const [k, v] of Object.entries(row)) {
                        if (!skip.has(k)) clean[k] = v;
                    }
                    return clean;
                });
            }

            // Serialize values for YAML
            const serialized = cleanRows.map(row => {
                const obj: Record<string, unknown> = {};
                for (const [k, v] of Object.entries(row)) {
                    if (v === null) {
                        obj[k] = null;
                    } else if (v instanceof Date) {
                        obj[k] = v.toISOString();
                    } else if (typeof v === 'object') {
                        obj[k] = v; // YAML lib handles objects/arrays natively
                    } else {
                        obj[k] = v;
                    }
                }
                return obj;
            });

            // Build YAML
            const yamlData = { [table]: serialized };
            const yamlStr = YAML.stringify(yamlData, {
                lineWidth: 0,        // no line wrapping
                defaultStringType: 'QUOTE_DOUBLE',
                defaultKeyType: 'PLAIN',
            });

            // Write file
            const filePath = join(seedPath, `${table}.yml`);
            const existed = existsSync(filePath);
            writeFileSync(filePath, yamlStr, 'utf-8');

            const action = existed ? 'Updated' : 'Created';
            log.succeed(`${action} ${table}.yml (${serialized.length} rows)`);
            dumpedCount++;
        }

        // Summary
        if (dumpedCount > 0) {
            log.header(`${dumpedCount} seed file(s) written to ${seedPath}`, 'green');
        } else {
            log.warn('No seed files were generated.');
        }
    } finally {
        await engine.close();
    }
}
