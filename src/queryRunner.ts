// ─────────────────────────────────────────────
// x-postgres — Query Runner
// ─────────────────────────────────────────────
// Executes arbitrary SQL and displays results in a table.

import Table from 'cli-table3';
import { loadConfig, type LoadedConfig } from './configLoader.js';
import { PgService } from './pgService.js';
import * as log from './logger.js';

export interface QueryOptions {
    name?: string;
    tenant?: string;
    config?: string;
}

export async function runQuery(sql: string, options: QueryOptions = {}): Promise<void> {
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

    const { postgres, configDir } = cfg;

    // Find target DB
    // Logic similar to builder.ts but simplified for single execution
    let targetDbId: string | undefined;
    let targetNode: any;

    for (const [dbId, dbConf] of Object.entries(postgres.DB)) {
        const nodes = Array.isArray(dbConf) ? dbConf : [dbConf];
        const writeNode = nodes.find(n => n.TYPE === 'write') ?? nodes[0];

        // Filter by --tenant
        if (options.tenant && !writeNode.TENANT_KEYS) continue;

        // Filter by --name
        if (options.name) {
            if (options.name === writeNode.NAME) {
                targetDbId = dbId;
                targetNode = dbConf; // Pass the whole conf
                break;
            }
        } else {
            // If no specific target, pick first available that matches tenant filter (if any)
            targetDbId = dbId;
            targetNode = dbConf;
            break;
        }
    }

    if (!targetDbId || !targetNode) {
        log.fail('No matching database found.');
        return;
    }

    log.header(`${targetDbId}`, 'cyan');
    log.spin('Executing query...');

    const pg = new PgService(targetNode, targetDbId);

    try {
        const result = await pg.query<any>(sql);
        log.stopSpinner();

        if (Array.isArray(result) && result.length > 0) {
            const head = Object.keys(result[0]);
            const table = new Table({
                head,
                style: { head: ['cyan'] }
            });

            for (const row of result) {
                const values = head.map(k => {
                    const val = row[k];
                    if (val === null) return 'NULL';
                    if (typeof val === 'object') return JSON.stringify(val);
                    return String(val);
                });
                table.push(values);
            }
            console.log(table.toString());
            log.succeed(`${result.length} rows returned.`);
        } else {
            log.succeed('Query executed successfully. No rows returned.');
        }

    } catch (err) {
        log.fail(`Query processing error: ${(err as Error).message}`);
        process.exit(1);
    } finally {
        await PgService.closeAll();
    }
}
