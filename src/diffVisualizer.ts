import Table from 'cli-table3';
import { SchemaEngine } from './schemaEngine.js';
import * as log from './logger.js';
import { generateCreateDatabase } from './sqlGenerator.js';

export interface DiffOptions {
    name?: string;
    tenant?: string;
    config?: string;
    dropOrphans?: boolean;
}

export async function visualizeDiff(options: DiffOptions = {}): Promise<void> {
    const engine = new SchemaEngine({ config: options.config });

    try {
        const targets = engine.getTargets({ name: options.name, tenant: options.tenant });

        for (const target of targets) {
            log.header(`Schema Diff: ${target.id} (${target.config.NAME})`, 'magenta');

            // Check if DB exists
            const createDbQuery = await engine.checkDatabaseExistence(target);
            if (createDbQuery) {
                log.warn(`Database '${target.config.NAME}' does not exist.`);
                log.info('It will be created on `up`.');
            }

            const queries = await engine.generateDiff(target, options.dropOrphans);
            
            if (createDbQuery) {
                queries.unshift(createDbQuery);
            }

            if (queries.length > 0) {
                const table = new Table({
                    head: ['Table', 'Type', 'Description'],
                    style: { head: ['cyan'] },
                    wordWrap: true
                });

                for (const q of queries) {
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
                log.info(`${queries.length} differences found.`);
            } else {
                log.succeed('Schemas are in sync.');
            }
        }
    } finally {
        await engine.close();
    }
}
