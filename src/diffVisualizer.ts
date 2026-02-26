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
        const multiCluster = engine.getTargetCount() > 1;

        for (const target of targets) {
            const label = multiCluster
                ? `${target.id} ⬡ ${target.config.NAME}`
                : `⬡ ${target.config.NAME}`;
            log.header(label, 'magenta');

            // Check if DB exists
            const createDbQuery = await engine.checkDatabaseExistence(target);
            if (createDbQuery) {
                log.warn(`Database '${target.config.NAME}' does not exist.`);
                log.info('It will be created on `up`.');
            }

            const allQueries = await engine.generateDiff(target, true);
            
            const orphanQueries = allQueries.filter(q => q.type === 'DROP_TABLE');
            const diffQueries = options.dropOrphans ? allQueries : allQueries.filter(q => q.type !== 'DROP_TABLE');

            if (!options.dropOrphans && orphanQueries.length > 0) {
                const orphanNames = orphanQueries.map(q => q.table).join(', ');
                const prefix = multiCluster ? `[${target.id}] ` : '';
                log.warn(`${prefix}${orphanQueries.length} orphan table(s) found: ${orphanNames}`);
                log.info(`Use --drop-orphans to include them in the diff.`);
            }

            if (createDbQuery) {
                diffQueries.unshift(createDbQuery);
            }

            if (diffQueries.length > 0) {
                renderQueries(diffQueries, displayMode);
                renderSummary(diffQueries.length, 'differences');
            } else {
                if (!options.dropOrphans && orphanQueries.length > 0) {
                    log.succeed('Schemas are in sync (ignoring orphans).');
                } else {
                    log.succeed('Schemas are in sync.');
                }
            }
        }
    } finally {
        await engine.close();
    }
}
