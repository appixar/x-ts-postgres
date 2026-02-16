import { SchemaEngine } from './schemaEngine.js';
import { renderQueries, renderSummary, type DisplayMode } from './displayRenderer.js';
import * as log from './logger.js';

export interface DiffOptions {
    name?: string;
    tenant?: string;
    config?: string;
    dropOrphans?: boolean;
    display?: DisplayMode;
}

export async function visualizeDiff(options: DiffOptions = {}): Promise<void> {
    const engine = new SchemaEngine({ config: options.config });
    const displayMode = options.display ?? engine.getConfig().displayMode;

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
                renderQueries(queries, displayMode);
                renderSummary(queries.length, 'differences');
            } else {
                log.succeed('Schemas are in sync.');
            }
        }
    } finally {
        await engine.close();
    }
}
