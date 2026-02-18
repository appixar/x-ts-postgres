// ─────────────────────────────────────────────
// x-postgres — Config loader
// ─────────────────────────────────────────────
// Loads xpg.config.yml (or separate postgres.yml + custom_fields.yml)
// with environment variable interpolation: <ENV.VAR> → process.env.VAR

import { readFileSync, existsSync, readdirSync, statSync } from "node:fs";
import { resolve, dirname, basename } from "node:path";
import YAML from "yaml";
import type { PostgresConfig, CustomFieldDef } from "./types.js";

/** Accepted config file names (checked in order) */
const CONFIG_FILENAMES = ["xpg.config.yml", "x-postgres.config.yml"];

/** Directories to skip when searching for config files */
const SKIP_DIRS = new Set([
  "node_modules",
  "dist",
  "build",
  "public",
  "tmp",
  "temp",
  "coverage",
  "test",
  "tests",
  "__tests__",
  ".git",
]);

/**
 * Load variables from a .env file into process.env.
 * Does NOT overwrite variables that already exist in process.env.
 */
function loadDotEnv(dir: string): void {
  const envPath = resolve(dir, ".env");
  if (!existsSync(envPath)) return;

  const content = readFileSync(envPath, "utf-8");
  for (const line of content.split("\n")) {
    let trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    // Strip `export ` prefix (common in .env files)
    if (trimmed.startsWith("export ")) trimmed = trimmed.slice(7).trim();
    const eqIdx = trimmed.indexOf("=");
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    let val = trimmed.slice(eqIdx + 1).trim();
    // Strip surrounding quotes
    if (
      (val.startsWith('"') && val.endsWith('"')) ||
      (val.startsWith("'") && val.endsWith("'"))
    ) {
      val = val.slice(1, -1);
    } else {
      // Strip inline comments (only when not quoted)
      const hashIdx = val.indexOf(" #");
      if (hashIdx !== -1) val = val.slice(0, hashIdx).trim();
    }
    // Only set if not already defined (real env takes precedence)
    if (process.env[key] === undefined) {
      process.env[key] = val;
    }
  }
}

/**
 * Search for config file recursively in a directory.
 * Checks both xpg.config.yml and x-postgres.config.yml.
 * Skips directories in SKIP_DIRS and dot-directories.
 * Returns the first match found, or undefined.
 */
function findConfigInDir(
  dir: string,
  maxDepth: number = 3,
  depth: number = 0,
): string | undefined {
  if (depth > maxDepth) return undefined;

  for (const name of CONFIG_FILENAMES) {
    const target = resolve(dir, name);
    if (existsSync(target)) return target;
  }

  if (depth >= maxDepth) return undefined;

  try {
    const entries = readdirSync(dir);
    for (const entry of entries) {
      if (SKIP_DIRS.has(entry) || entry.startsWith(".")) continue;
      const full = resolve(dir, entry);
      try {
        if (statSync(full).isDirectory()) {
          const found = findConfigInDir(full, maxDepth, depth + 1);
          if (found) return found;
        }
      } catch {
        /* permission errors, etc. */
      }
    }
  } catch {
    /* unreadable directory */
  }

  return undefined;
}

/**
 * Find xpg.config.yml with smart discovery.
 *
 * Search order:
 * 1. CWD root (direct check)
 * 2. config/ subdirectory
 * 3. src/ and subdirectories (recursive, max 3 levels)
 * 4. All other directories in CWD (recursive, max 3 levels)
 *
 * Skips: node_modules, dist, build, public, tmp, coverage, test, .dot-dirs
 */
function discoverConfigFile(cwd: string): string | undefined {
  // 1. Root
  for (const name of CONFIG_FILENAMES) {
    const root = resolve(cwd, name);
    if (existsSync(root)) return root;
  }

  // 2. config/
  for (const name of CONFIG_FILENAMES) {
    const configDir = resolve(cwd, "config", name);
    if (existsSync(configDir)) return configDir;
  }

  // 3. src/ (recursive)
  const srcDir = resolve(cwd, "src");
  if (existsSync(srcDir)) {
    const found = findConfigInDir(srcDir);
    if (found) return found;
  }

  // 4. All other directories (recursive)
  try {
    const entries = readdirSync(cwd);
    for (const entry of entries) {
      if (entry === "src" || entry === "config") continue; // already checked
      if (SKIP_DIRS.has(entry) || entry.startsWith(".")) continue;
      const full = resolve(cwd, entry);
      try {
        if (statSync(full).isDirectory()) {
          const found = findConfigInDir(full);
          if (found) return found;
        }
      } catch {
        /* skip */
      }
    }
  } catch {
    /* skip */
  }

  return undefined;
}

/**
 * Interpolate <ENV.VAR_NAME> placeholders with process.env values.
 */
function interpolateEnv(raw: string): string {
  return raw.replace(/<ENV\.([A-Za-z_][A-Za-z0-9_]*)>/g, (_match, varName) => {
    const val = process.env[varName];
    if (val === undefined) {
      console.warn(
        `[x-postgres] ⚠ ENV var "${varName}" is not defined — using empty string`,
      );
    }
    return val ?? "";
  });
}

/**
 * Recursively walk a parsed YAML object and interpolate env vars in all string values.
 */
function deepInterpolate(obj: unknown): unknown {
  if (typeof obj === "string") return interpolateEnv(obj);
  if (Array.isArray(obj)) return obj.map(deepInterpolate);
  if (obj !== null && typeof obj === "object") {
    const result: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj as Record<string, unknown>)) {
      result[k] = deepInterpolate(v);
    }
    return result;
  }
  return obj;
}

export interface LoadedConfig {
  postgres: PostgresConfig["POSTGRES"];
  customFields: Record<string, CustomFieldDef>;
  /** Directory where the config file lives (used to resolve relative paths) */
  configDir: string;
  /** Directory where seed files are located */
  seedPath: string;
  /** Suffix inserted before .yml in seed dump filenames (e.g. ".seed" → table.seed.yml) */
  seedSuffix: string;
  /** Display mode for CLI output: 'table' or 'compact' */
  displayMode: "table" | "compact";
}

/**
 * Load configuration.
 *
 * Resolution order:
 * 1. `--config <path>` CLI argument → single file
 * 2. Smart discovery: root → config/ → src/ → other dirs
 * 3. `config/postgres.yml` + `config/custom_fields.yml` (PHP-compatible layout)
 *
 * Automatically loads .env from CWD (and config dir if different).
 */
export function loadConfig(configPath?: string): LoadedConfig {
  let raw: Record<string, unknown> = {};
  let configDir = process.cwd();

  // Load .env from CWD (most common case for external projects)
  loadDotEnv(process.cwd());

  if (configPath) {
    const abs = resolve(configPath);
    if (!existsSync(abs)) throw new Error(`Config file not found: ${abs}`);
    raw = YAML.parse(readFileSync(abs, "utf-8")) ?? {};
    configDir = dirname(abs);
    // Also load .env from config dir if it differs from CWD
    if (configDir !== process.cwd()) loadDotEnv(configDir);
  } else {
    // Smart discovery: search for xpg.config.yml
    const discovered = discoverConfigFile(process.cwd());

    if (discovered) {
      raw = YAML.parse(readFileSync(discovered, "utf-8")) ?? {};
      configDir = dirname(discovered);
      if (configDir !== process.cwd()) loadDotEnv(configDir);
    } else {
      // Fallback: PHP-compatible layout
      const pgPath = resolve(process.cwd(), "config/postgres.yml");
      const cfPath = resolve(process.cwd(), "config/custom_fields.yml");

      if (existsSync(pgPath)) {
        raw = YAML.parse(readFileSync(pgPath, "utf-8")) ?? {};
      }
      if (existsSync(cfPath)) {
        const cfRaw = YAML.parse(readFileSync(cfPath, "utf-8")) ?? {};
        // Merge custom fields into POSTGRES block
        if (cfRaw?.POSTGRES?.CUSTOM_FIELDS && raw?.POSTGRES) {
          (raw.POSTGRES as Record<string, unknown>).CUSTOM_FIELDS =
            cfRaw.POSTGRES.CUSTOM_FIELDS;
        } else if (cfRaw?.POSTGRES?.CUSTOM_FIELDS) {
          raw.POSTGRES = { CUSTOM_FIELDS: cfRaw.POSTGRES.CUSTOM_FIELDS };
        }
      }
    }
  }

  // Interpolate environment variables
  raw = deepInterpolate(raw) as Record<string, unknown>;

  const postgres = (raw as unknown as PostgresConfig).POSTGRES;
  if (!postgres?.DB) {
    throw new Error(
      "Config error: POSTGRES.DB is missing. Please create xpg.config.yml or config/postgres.yml",
    );
  }

  const customFields: Record<string, CustomFieldDef> =
    postgres.CUSTOM_FIELDS ?? {};

  // Resolve SEED_PATH
  // Defaults to 'seeds' relative to config dir
  const seedPathRaw = (raw.POSTGRES as any)?.SEED_PATH ?? "seeds";
  const seedPath = resolve(configDir, seedPathRaw);

  // Seed suffix (e.g. ".seed" → table.seed.yml)
  const seedSuffix: string = (raw.POSTGRES as any)?.SEED_SUFFIX ?? "";

  // Display mode (table | compact)
  const displayModeRaw = (raw.POSTGRES as any)?.DISPLAY_MODE ?? "compact";
  const displayMode =
    displayModeRaw === "compact" ? "compact" : ("table" as const);

  return {
    postgres,
    customFields,
    configDir,
    seedPath,
    seedSuffix,
    displayMode,
  };
}

/**
 * Resolve database schema paths from config.
 * Tries: absolute → relative to configDir → relative to CWD.
 * This ensures paths work regardless of where the config file was discovered.
 */
export function resolveSchemaPath(
  pathEntry: string,
  configDir: string,
): string {
  if (pathEntry.startsWith("/")) return pathEntry;

  // Try relative to config dir first
  const fromConfig = resolve(configDir, pathEntry);
  if (existsSync(fromConfig)) return fromConfig;

  // Fallback: try relative to CWD (common when config is auto-discovered in a subdir)
  const fromCwd = resolve(process.cwd(), pathEntry);
  if (existsSync(fromCwd)) return fromCwd;

  // Return configDir-relative path (will fail gracefully later with "dir not found")
  return fromConfig;
}
