// ─────────────────────────────────────────────
// x-postgres — Schema Engine (Shared Logic)
// ─────────────────────────────────────────────
// Centralizes the logic for:
// 1. Loading configuration
// 2. Connecting to the target database
// 3. Parsing YAML schemas
// 4. Fetching live DB structure
// 5. Generating Diff (Queries)

import { readFileSync, readdirSync, existsSync, statSync } from "node:fs";
import { resolve, join } from "node:path";
import YAML from "yaml";
import {
  loadConfig,
  resolveSchemaPath,
  type LoadedConfig,
} from "./configLoader.js";
import { parseSchema } from "./schemaParser.js";
import {
  generateCreateTable,
  generateDropTable,
  generateCreateDatabase,
} from "./sqlGenerator.js";
import { generateUpdateTable, type DiffContext } from "./diffEngine.js";
import { Database } from "./database.js";
import * as log from "./logger.js";
import type { QueuedQuery, DbColumnInfo, DbNodeConfig } from "./types.js";

export interface SchemaEngineOptions {
  config?: string;
  mute?: boolean;
}

export interface TargetDb {
  id: string;
  config: DbNodeConfig;
  pg: Database;
}

export class SchemaEngine {
  private config: LoadedConfig;
  private options: SchemaEngineOptions;

  constructor(options: SchemaEngineOptions = {}) {
    this.options = options;
    if (!this.options.mute) log.spin("Loading configuration...");
    try {
      this.config = loadConfig(options.config);
      if (!this.options.mute) log.stopSpinner();
    } catch (err) {
      if (!this.options.mute) log.fail("Failed to load config");
      throw new Error(`Config error: ${(err as Error).message}`);
    }
  }

  public getConfig(): LoadedConfig {
    return this.config;
  }

  /**
   * Iterate over all defined databases, or filter by name/tenant.
   * Yields a connected Database for each matching DB.
   */
  public *getTargets(filter?: {
    name?: string;
    tenant?: string;
  }): Generator<TargetDb> {
    const { postgres } = this.config;

    for (const [dbId, dbConf] of Object.entries(postgres.DB)) {
      const nodes = Array.isArray(dbConf) ? dbConf : [dbConf];
      const writeNode = nodes.find((n) => n.TYPE === "write") ?? nodes[0];

      // Filter logic
      if (filter?.tenant && !writeNode.TENANT_KEYS) continue;
      if (filter?.name) {
        if (filter.name !== writeNode.NAME && !writeNode.TENANT_KEYS) continue;
      }

      const pg = new Database(dbConf, dbId, {
        timeoutMs: this.config.timeoutMs,
      });
      yield { id: dbId, config: writeNode, pg };
    }
  }

  /**
   * Check if database exists, and if not, generate CREATE DATABASE query.
   */
  public async checkDatabaseExistence(
    target: TargetDb,
  ): Promise<QueuedQuery | null> {
    try {
      const adminPool = target.pg.getAdminPool();
      const res = await adminPool.query(
        "SELECT datname FROM pg_database WHERE datname = $1",
        [target.config.NAME],
      );
      if (res.rows.length === 0) {
        return generateCreateDatabase(target.config.NAME);
      }
    } catch (err) {
      log.warn(`Failed to check database existence: ${(err as Error).message}`);
    }
    return null;
  }

  /**
   * Generate the full diff (migrations) for a specific database target.
   */
  public async generateDiff(
    target: TargetDb,
    dropOrphans: boolean = false,
  ): Promise<QueuedQuery[]> {
    const { id, config, pg } = target;
    const { configDir, customFields } = this.config;
    const allQueries: QueuedQuery[] = [];

    // 1. Resolve schema paths
    let databasePaths: string[] = [];
    if (config.PATH) {
      const paths = Array.isArray(config.PATH) ? config.PATH : [config.PATH];
      databasePaths = paths.map((p) => resolveSchemaPath(p, configDir));
    } else {
      const defaultPath = resolve(configDir, "database");
      if (existsSync(defaultPath)) databasePaths = [defaultPath];
    }

    // 2. Fetch existing tables
    if (!this.options.mute) log.spin(`[${id}] Analyzing database structure...`);
    let tablesReal: string[] = [];
    try {
      const t = await pg.query<{ table_name: string }>(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
      );
      tablesReal = t.map((r) => r.table_name);
    } catch {
      if (!this.options.mute)
        log.warn("Could not fetch existing tables (DB might not exist yet)");
      // If DB doesn't exist, tablesReal is empty, so everything will be CREATE TABLE
    }

    // 3. Process YAML files
    const tablesNew: string[] = [];

    for (const schemaDir of databasePaths) {
      if (!existsSync(schemaDir) || !statSync(schemaDir).isDirectory())
        continue;
      const files = readdirSync(schemaDir).filter(
        (f) => f.endsWith(".yml") || f.endsWith(".yaml"),
      );

      if (!this.options.mute)
        log.spin(`[${id}] Reading ${files.length} schema files...`);

      for (const fn of files) {
        const fp = join(schemaDir, fn);
        let data: Record<string, Record<string, string>>;
        try {
          const content = readFileSync(fp, "utf-8");
          data = YAML.parse(content);
          if (!data || typeof data !== "object") continue;
        } catch (err) {
          log.error(`Failed to parse ${fn}: ${(err as Error).message}`);
          continue;
        }

        for (let [tableName, tableCols] of Object.entries(data)) {
          if (!tableCols || typeof tableCols !== "object") continue;

          // Apply prefix: ~tableName → PREF + tableName
          if (tableName.startsWith("~") && config.PREF) {
            tableName = config.PREF + tableName.substring(1);
          }

          tablesNew.push(tableName);
          if ((tableCols as Record<string, unknown>)["~ignore"]) continue;

          const schema = parseSchema(tableCols, customFields);
          if (Object.keys(schema.fields).length === 0) continue;

          if (tablesReal.includes(tableName)) {
            // UPDATE
            try {
              const columns = await pg.query<DbColumnInfo>(
                `SELECT column_name, data_type, is_nullable, character_maximum_length, column_default, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = :_tbl`,
                { _tbl: tableName },
              );

              if (columns.length > 0) {
                const currentCols: Record<string, DbColumnInfo> = {};
                for (const col of columns) currentCols[col.column_name] = col;

                const indexes = await pg.query<{ indexname: string }>(
                  `SELECT indexname FROM pg_indexes WHERE tablename = :_tbl`,
                  { _tbl: tableName },
                );
                const uniques = await pg.query<{ conname: string }>(
                  `SELECT conname FROM pg_constraint WHERE conrelid = :_tbl::regclass AND contype = 'u'`,
                  { _tbl: tableName },
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
              log.error(
                `Error analyzing table ${tableName}: ${(err as Error).message}`,
              );
            }
          } else {
            // CREATE
            allQueries.push(...generateCreateTable(tableName, schema));
          }
        }
      }
    }

    if (!this.options.mute) log.stopSpinner();

    // 4. Drop orphans
    if (dropOrphans) {
      for (const existingTable of tablesReal) {
        if (!tablesNew.includes(existingTable)) {
          allQueries.push(generateDropTable(existingTable));
        }
      }
    } else {
      const orphans = tablesReal.filter((t) => !tablesNew.includes(t));
      if (orphans.length > 0 && !this.options.mute) {
        log.warn(
          `[${id}] ${orphans.length} orphan table(s) found: ${orphans.join(", ")}`,
        );
        log.info(`Use --drop-orphans to clean.`);
      }
    }

    return allQueries;
  }

  public async close(): Promise<void> {
    await Database.closeAll();
  }
}
