import { confirm } from "@inquirer/prompts";
import { SchemaEngine } from "./schemaEngine.js";
import {
  renderQueries,
  renderSummary,
  type DisplayMode,
} from "./displayRenderer.js";
import * as log from "./logger.js";

export interface BuilderOptions {
  yes?: boolean;
  mute?: boolean;
  create?: boolean;
  name?: string;
  tenant?: string;
  dry?: boolean;
  config?: string;
  dropOrphans?: boolean;
  display?: DisplayMode;
}

export interface MigrationResult {
  executed: number;
  failed: { sql: string; error: string }[];
  total: number;
}

export async function up(
  options: BuilderOptions = {},
): Promise<MigrationResult> {
  const mute = options.mute ?? false;
  const dryRun = options.dry ?? false;
  const result: MigrationResult = { executed: 0, failed: [], total: 0 };

  // Use schema engine
  const engine = new SchemaEngine({ config: options.config, mute });
  const displayMode = options.display ?? engine.getConfig().displayMode;

  try {
    const targets = engine.getTargets({
      name: options.name,
      tenant: options.tenant,
    });

    for (const target of targets) {
      if (!mute) log.header(`${target.id} (${target.config.NAME})`, "cyan");

      // 1. Check/Create Database
      if (options.create) {
        const createDbQuery = await engine.checkDatabaseExistence(target);
        if (createDbQuery) {
          if (!dryRun) {
            if (!mute && !options.yes) {
              const ok = await confirm({
                message: `Database '${target.config.NAME}' does not exist. Create it?`,
                default: true,
              });
              if (!ok) {
                log.warn("Aborting!");
                continue;
              }
            }
            const adminPool = target.pg.getAdminPool();
            if (!mute) log.spin("Creating database...");
            await adminPool.query(createDbQuery.sql);
            if (!mute) log.succeed(`Database created. Re-running migration...`);
          } else {
            if (!mute)
              log.info(
                `[Dry Run] Database '${target.config.NAME}' would be created.`,
              );
          }
        }
      }

      // 2. Diff
      const queries = await engine.generateDiff(target, options.dropOrphans);

      // 3. Execution / Visualization
      if (queries.length > 0) {
        if (!mute) {
          renderQueries(queries, displayMode);
          renderSummary(queries.length, "changes to apply");
        }

        if (dryRun) {
          if (!mute) {
            log.header("Dry run — checking SQL", "yellow");
            for (const q of queries) {
              console.log(log.colorFns.gray(q.sql));
            }
          }
        } else {
          if (!mute && !options.yes) {
            const ok = await confirm({
              message: "Apply changes?",
              default: true,
            });
            if (!ok) {
              log.warn("Cancelled.");
              continue;
            }
          }
          if (!mute) log.spin("Applying changes...");

          for (const q of queries) {
            try {
              await target.pg.query(q.sql);
              result.executed++;
            } catch (err) {
              const errMsg = (err as Error).message;
              result.failed.push({ sql: q.sql, error: errMsg });
              if (!mute) log.fail(`Failed: ${q.description} \n ${errMsg}`);
            }
          }

          if (!mute) {
            if (result.failed.length > 0) {
              log.warn(
                `Finished with errors: ${result.executed} executed, ${result.failed.length} failed.`,
              );
            } else {
              log.succeed("All changes applied successfully");
            }
          }
        }
      } else {
        if (!mute) log.succeed("Database is up to date.");
      }
      result.total += queries.length;
    }
  } finally {
    await engine.close();
  }

  return result;
}
