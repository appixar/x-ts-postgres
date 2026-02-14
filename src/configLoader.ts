// ─────────────────────────────────────────────
// x-postgres — Config loader
// ─────────────────────────────────────────────
// Loads xpg.config.yml (or separate postgres.yml + custom_fields.yml)
// with environment variable interpolation: <ENV.VAR> → process.env.VAR

import { readFileSync, existsSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import YAML from 'yaml';
import type { PostgresConfig, CustomFieldDef } from './types.js';

/**
 * Load variables from a .env file into process.env.
 * Does NOT overwrite variables that already exist in process.env.
 */
function loadDotEnv(dir: string): void {
    const envPath = resolve(dir, '.env');
    if (!existsSync(envPath)) return;

    const content = readFileSync(envPath, 'utf-8');
    for (const line of content.split('\n')) {
        let trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) continue;
        // Strip `export ` prefix (common in .env files)
        if (trimmed.startsWith('export ')) trimmed = trimmed.slice(7).trim();
        const eqIdx = trimmed.indexOf('=');
        if (eqIdx === -1) continue;
        const key = trimmed.slice(0, eqIdx).trim();
        let val = trimmed.slice(eqIdx + 1).trim();
        // Strip surrounding quotes
        if ((val.startsWith('"') && val.endsWith('"')) ||
            (val.startsWith("'") && val.endsWith("'"))) {
            val = val.slice(1, -1);
        } else {
            // Strip inline comments (only when not quoted)
            const hashIdx = val.indexOf(' #');
            if (hashIdx !== -1) val = val.slice(0, hashIdx).trim();
        }
        // Only set if not already defined (real env takes precedence)
        if (process.env[key] === undefined) {
            process.env[key] = val;
        }
    }
}

/**
 * Interpolate <ENV.VAR_NAME> placeholders with process.env values.
 */
function interpolateEnv(raw: string): string {
    return raw.replace(/<ENV\.([A-Za-z_][A-Za-z0-9_]*)>/g, (_match, varName) => {
        const val = process.env[varName];
        if (val === undefined) {
            console.warn(`[x-postgres] ⚠ ENV var "${varName}" is not defined — using empty string`);
        }
        return val ?? '';
    });
}

/**
 * Recursively walk a parsed YAML object and interpolate env vars in all string values.
 */
function deepInterpolate(obj: unknown): unknown {
    if (typeof obj === 'string') return interpolateEnv(obj);
    if (Array.isArray(obj)) return obj.map(deepInterpolate);
    if (obj !== null && typeof obj === 'object') {
        const result: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(obj as Record<string, unknown>)) {
            result[k] = deepInterpolate(v);
        }
        return result;
    }
    return obj;
}

export interface LoadedConfig {
    postgres: PostgresConfig['POSTGRES'];
    customFields: Record<string, CustomFieldDef>;
    /** Directory where the config file lives (used to resolve relative paths) */
    configDir: string;
}

/**
 * Load configuration.
 *
 * Resolution order:
 * 1. `--config <path>` CLI argument → single file
 * 2. `xpg.config.yml` in CWD
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
        raw = YAML.parse(readFileSync(abs, 'utf-8')) ?? {};
        configDir = dirname(abs);
        // Also load .env from config dir if it differs from CWD
        if (configDir !== process.cwd()) loadDotEnv(configDir);
    } else if (existsSync(resolve(process.cwd(), 'xpg.config.yml'))) {
        const abs = resolve(process.cwd(), 'xpg.config.yml');
        raw = YAML.parse(readFileSync(abs, 'utf-8')) ?? {};
        configDir = dirname(abs);
    } else {
        // Try PHP-compatible layout
        const pgPath = resolve(process.cwd(), 'config/postgres.yml');
        const cfPath = resolve(process.cwd(), 'config/custom_fields.yml');

        if (existsSync(pgPath)) {
            raw = YAML.parse(readFileSync(pgPath, 'utf-8')) ?? {};
        }
        if (existsSync(cfPath)) {
            const cfRaw = YAML.parse(readFileSync(cfPath, 'utf-8')) ?? {};
            // Merge custom fields into POSTGRES block
            if (cfRaw?.POSTGRES?.CUSTOM_FIELDS && raw?.POSTGRES) {
                (raw.POSTGRES as Record<string, unknown>).CUSTOM_FIELDS = cfRaw.POSTGRES.CUSTOM_FIELDS;
            } else if (cfRaw?.POSTGRES?.CUSTOM_FIELDS) {
                raw.POSTGRES = { CUSTOM_FIELDS: cfRaw.POSTGRES.CUSTOM_FIELDS };
            }
        }
    }

    // Interpolate environment variables
    raw = deepInterpolate(raw) as Record<string, unknown>;

    const postgres = (raw as unknown as PostgresConfig).POSTGRES;
    if (!postgres?.DB) {
        throw new Error(
            'Config error: POSTGRES.DB is missing. Please create xpg.config.yml or config/postgres.yml'
        );
    }

    const customFields: Record<string, CustomFieldDef> = postgres.CUSTOM_FIELDS ?? {};

    return { postgres, customFields, configDir };
}

/**
 * Resolve database schema paths from config.
 * Handles both absolute paths and paths relative to configDir.
 */
export function resolveSchemaPath(pathEntry: string, configDir: string): string {
    if (pathEntry.startsWith('/')) return pathEntry;
    return resolve(configDir, pathEntry);
}
