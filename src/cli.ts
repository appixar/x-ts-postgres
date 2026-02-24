#!/usr/bin/env node
// ─────────────────────────────────────────────
// x-postgres — CLI entry point
// ─────────────────────────────────────────────

import { Command } from 'commander';
import { writeFileSync, existsSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { up } from './migrator.js';
import { runQuery } from './queryRunner.js';
import { visualizeDiff } from './diffVisualizer.js';
import { runSeed } from './seedRunner.js';
import { runSeedDump } from './seedDumper.js';
import * as log from './logger.js';

const program = new Command();

program
  .name('xpg')
  .description('YAML-driven PostgreSQL schema management & migrations')
  .option('--no-color', 'Disable colored output')
  .option('-v, --version', 'Show version with banner')
  .hook('preAction', (thisCommand) => {
    // Skip welcome for version flag
    if (thisCommand.opts().version) return;
    log.welcome();
  })
  .on('option:version', () => {
    log.banner();
    process.exit(0);
  });



// ─── diff command ───
program
  .command('diff')
  .description('Show differences between local YAML schemas and remote database')
  .option('--name <db>', 'Target specific database cluster by NAME')
  .option('--tenant <key>', 'Target specific tenant key')
  .option('--drop-orphans', 'Include DROP TABLE statements for orphans')
  .option('--display <mode>', 'Display mode: table or compact')
  .option('--config <path>', 'Path to config file')
  .action(async (opts) => {
    try {
      await visualizeDiff({
        name: opts.name,
        tenant: opts.tenant,
        dropOrphans: opts.dropOrphans,
        display: opts.display,
        config: opts.config,
      });
    } catch (err) {
      log.error((err as Error).message);
      process.exit(1);
    }
  });

// ─── status command ───
program
  .command('status')
  .description('Show migration status for each table (without applying)')
  .option('--name <db>', 'Target specific database cluster by NAME')
  .option('--tenant <key>', 'Target specific tenant key')
  .option('--config <path>', 'Path to config file')
  .action(async (opts) => {
    const chalk = (await import('chalk')).default;
    const { SchemaEngine } = await import('./schemaEngine.js');
    try {
      const engine = new SchemaEngine({ config: opts.config, mute: true });
      const targets = engine.getTargets({ name: opts.name, tenant: opts.tenant });

      for (const target of targets) {
        console.log(`\n${chalk.bold(target.id)} ${chalk.dim(`(${target.config.NAME})`)}\n`);

        const queries = await engine.generateDiff(target);

        // Group queries by table
        const byTable = new Map<string, { count: number; types: Set<string> }>();
        for (const q of queries) {
          const tbl = q.table || '(database)';
          if (!byTable.has(tbl)) byTable.set(tbl, { count: 0, types: new Set() });
          const entry = byTable.get(tbl)!;
          entry.count++;
          entry.types.add(q.type);
        }

        // Get existing tables to show up-to-date ones
        const allTables = new Set<string>();
        for (const tbl of byTable.keys()) allTables.add(tbl);
        try {
          const existing = await target.pg.query<{ table_name: string }>(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name"
          );
          for (const row of existing) allTables.add(row.table_name);
        } catch { /* DB might not exist */ }

        const sortedTables = Array.from(allTables).sort();
        let upToDate = 0;
        let pending = 0;

        for (const tbl of sortedTables) {
          const info = byTable.get(tbl);
          if (!info) {
            console.log(`  ${chalk.green('✔')} ${chalk.white(tbl)}`);
            upToDate++;
          } else if (info.types.has('CREATE_TABLE')) {
            console.log(`  ${chalk.green('+')} ${chalk.white.bold(tbl)} ${chalk.dim('— new table')}`);
            pending++;
          } else {
            const desc: string[] = [];
            if (info.types.has('ADD_COLUMN')) desc.push('new columns');
            if (info.types.has('DROP_COLUMN')) desc.push('drop columns');
            if (info.types.has('ALTER_COLUMN')) desc.push('altered');
            if (info.types.has('ADD_INDEX')) desc.push('new indexes');
            if (info.types.has('DROP_INDEX')) desc.push('drop indexes');
            if (desc.length === 0) desc.push(`${info.count} changes`);
            console.log(`  ${chalk.yellow('~')} ${chalk.white.bold(tbl)} ${chalk.dim('—')} ${chalk.yellow(desc.join(', '))}`);
            pending++;
          }
        }

        console.log('');
        if (pending === 0) {
          log.succeed('All tables up to date');
        } else {
          log.info(`${pending} table(s) need migration, ${upToDate} up to date`);
          log.info(`Run ${chalk.cyan('npx xpg up')} to apply changes`);
        }
      }

      await engine.close();
    } catch (err) {
      log.error((err as Error).message);
      process.exit(1);
    }
  });

// ─── up command ───
program
  .command('up')
  .description('Run database migrations (create/update/drop tables from YAML schemas)')
  .option('--yes', 'Skip confirmation prompts')
  .option('--create', 'Create database if it does not exist')
  .option('--name <db>', 'Target specific database cluster by NAME')
  .option('--tenant <key>', 'Target specific tenant key')
  .option('--mute', 'Suppress all output')
  .option('--dry', 'Dry run — show queries without executing')
  .option('--drop-orphans', 'Drop tables that exist in DB but not in YAML')
  .option('--display <mode>', 'Display mode: table or compact')
  .option('--config <path>', 'Path to config file (default: xpg.config.yml)')
  .action(async (opts) => {
    try {
      const result = await up({
        yes: opts.yes,
        create: opts.create,
        name: opts.name,
        tenant: opts.tenant,
        mute: opts.mute,
        dry: opts.dry,
        dropOrphans: opts.dropOrphans,
        display: opts.display,
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
  .option('--yes', 'Skip per-table confirmation prompts')
  .option('--table <list>', 'Comma-separated list of tables to seed')
  .option('--config <path>', 'Path to config file')
  .action(async (filename, opts) => {
    try {
      await runSeed({
        filename,
        yes: opts.yes,
        table: opts.table,
        config: opts.config,
      });
    } catch (err) {
      log.error((err as Error).message);
      process.exit(1);
    }
  });

// ─── seed:dump command ───
program
  .command('seed:dump')
  .description('Generate YAML seed files from live database data')
  .option('--table <list>', 'Comma-separated list of tables to dump')
  .option('--exclude <list>', 'Comma-separated list of tables to exclude')
  .option('--all', 'Dump all tables without prompting')
  .option('--limit <n>', 'Max rows per table', parseInt)
  .option('--skip-auto', 'Exclude auto-generated columns (SERIAL, now(), uuid, etc.)')
  .option('--config <path>', 'Path to config file')
  .action(async (opts) => {
    try {
      await runSeedDump({
        tables: opts.table,
        exclude: opts.exclude,
        all: opts.all,
        limit: opts.limit,
        skipAuto: opts.skipAuto,
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
      POOL_MAX: 10

  # SEED_PATH: seeds         # Path to seed files (default: seeds)
  # SEED_SUFFIX: ".seed"     # Suffix for seed:dump files (e.g. table.seed.yml)
  # DISPLAY_MODE: table      # Display mode: compact (default) | table

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
    "now":
      Type: timestamp
      Default: now()
    "pid":
      Type: varchar(16)
      Key: UNI
      Default: encode(gen_random_bytes(8), 'hex')
    "pid32":
      Type: varchar(32)
      Key: UNI
      Default: encode(gen_random_bytes(16), 'hex')
    "decimal":
      Type: numeric
`;

const SAMPLE_TABLE = `# Example table definition
example_users:
  user_id: id
  user_name: str required
  user_email: email unique index
  user_status: str/32 default/active index
  user_date_insert: now
`;
