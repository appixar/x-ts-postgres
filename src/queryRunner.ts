import Table from 'cli-table3';
import { SchemaEngine } from './schemaEngine.js';
import * as log from './logger.js';

export interface QueryOptions {
    name?: string;
    tenant?: string;
    config?: string;
}

export async function runQuery(sql: string, options: QueryOptions = {}): Promise<void> {
    const engine = new SchemaEngine({ config: options.config });

    try {
        const targets = engine.getTargets({ name: options.name, tenant: options.tenant });
        
        // Take the first target
        const iterator = targets.next();
        if (iterator.done) {
            log.fail('No matching database found.');
            return;
        }
        const target = iterator.value;

        log.header(`${target.id}`, 'cyan');
        log.spin('Executing query...');

        try {
            const result = await target.pg.query<any>(sql);
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
        }
    } finally {
        await engine.close();
    }
}
