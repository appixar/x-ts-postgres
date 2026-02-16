#!/usr/bin/env node
// ─────────────────────────────────────────────
// x-postgres — CLI entry point
// ─────────────────────────────────────────────

import { Command } from 'commander';
import { writeFileSync, existsSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { up } from './builder.js';
import { runQuery } from './queryRunner.js';
import { visualizeDiff } from './diffVisualizer.js';
import { runSeed } from './seedRunner.js';
import * as log from './logger.js';

const program = new Command();

program
  .name('xpg')
  .description('YAML-driven PostgreSQL schema management & migrations')
  .version('1.0.0')
  .option('--no-color', 'Disable colored output')
  .hook('preAction', () => {
    log.welcome();
  });



// ─── diff command ───
program
  .command('diff')
  .description('Show differences between local YAML schemas and remote database')
  .option('--name <db>', 'Target specific database cluster by NAME')
  .option('--tenant <key>', 'Target specific tenant key')
  .option('--drop-orphans', 'Include DROP TABLE statements for orphans')
  .option('--config <path>', 'Path to config file')
  .action(async (opts) => {
    try {
      await visualizeDiff({
        name: opts.name,
        tenant: opts.tenant,
        dropOrphans: opts.dropOrphans,
        config: opts.config,
      });
    } catch (err) {
      log.error((err as Error).message);
      process.exit(1);
    }
  });

// ─── up command ───
program
  .command('up')
  .description('Run database migrations (create/update/drop tables from YAML schemas)')
  .option('--create', 'Create database if it does not exist')
  .option('--name <db>', 'Target specific database cluster by NAME')
  .option('--tenant <key>', 'Target specific tenant key')
  .option('--mute', 'Suppress all output')
  .option('--dry', 'Dry run — show queries without executing')
  .option('--drop-orphans', 'Drop tables that exist in DB but not in YAML')
  .option('--config <path>', 'Path to config file (default: xpg.config.yml)')
  .action(async (opts) => {
    try {
      const result = await up({
        create: opts.create,
        name: opts.name,
        tenant: opts.tenant,
        mute: opts.mute,
        dry: opts.dry,
        dropOrphans: opts.dropOrphans,
        config: opts.config,
      });

      if (result.failed.length > 0) {
        process.exit(1);
      }
    } catch (err) {
      log.error((err as Error).message);
      process.exit(1);
    }
  });

// ─── query command ───
program
  .command('query <sql>')
  .description('Execute a raw SQL query')
  .option('--name <db>', 'Target specific database cluster by NAME')
  .option('--tenant <key>', 'Target specific tenant key')
  .option('--config <path>', 'Path to config file')
  .action(async (sql, opts) => {
    try {
      await runQuery(sql, {
        name: opts.name,
        tenant: opts.tenant,
        config: opts.config,
      });
    } catch (err) {
      log.error((err as Error).message);
      process.exit(1);
    }
  });

// ─── seed command ───
program
  .command('seed [filename]')
  .description('Seed database with data from YAML files')
  .option('--config <path>', 'Path to config file')
  .action(async (filename, opts) => {
    try {
      await runSeed({
        filename,
        config: opts.config,
      });
    } catch (err) {
      log.error((err as Error).message);
      process.exit(1);
    }
  });

// ─── init command ───
program
  .command('init')
  .description('Generate sample configuration files in the current directory')
  .action(() => {
    const configPath = resolve(process.cwd(), 'xpg.config.yml');
    const dbDir = resolve(process.cwd(), 'database');

    if (existsSync(configPath)) {
      log.warn('xpg.config.yml already exists. Skipping.');
    } else {
      writeFileSync(configPath, SAMPLE_CONFIG);
      log.success('✓ Created xpg.config.yml');
    }

    if (!existsSync(dbDir)) {
      mkdirSync(dbDir, { recursive: true });
      writeFileSync(resolve(dbDir, 'example.yml'), SAMPLE_TABLE);
      log.success('✓ Created database/example.yml');
    } else {
      log.warn('database/ already exists. Skipping.');
    }

    log.say('\nEdit xpg.config.yml with your PostgreSQL credentials, then run:');
    log.say('  npx xpg up', 'cyan');
  });

program.parse();

// ─── Sample templates ───

const SAMPLE_CONFIG = `#────────────────────────────────────
# x-postgres configuration
#────────────────────────────────────
POSTGRES:
  DB:
    "main":
      NAME: my_database
      HOST: <ENV.DB_HOST>
      USER: <ENV.DB_USER>
      PASS: <ENV.DB_PASS>
      PORT: <ENV.DB_PORT>
      PREF: app_
      PATH: [database]

  CUSTOM_FIELDS:
    "id":
      Type: serial
      Key: PRI
    "str":
      Type: varchar(64)
    "text":
      Type: text
    "int":
      Type: integer
    "float":
      Type: real
    "date":
      Type: timestamp
    "email":
      Type: varchar(128)
    "phone":
      Type: varchar(11)
    "now":
      Type: timestamp
      Default: now()
    "pid":
      Type: varchar(12)
      Key: UNI
      Default: '"left"(md5((random())::text), 12)'
`;

const SAMPLE_TABLE = `# Example table definition
example_users:
  user_id: id
  user_name: str required
  user_email: email unique index
  user_status: str/32 default/active index
  user_date_insert: now
`;
