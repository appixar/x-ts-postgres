# @appixar/xpg

YAML-driven PostgreSQL schema management, diff-based migrations, and query service for Node.js.

Define your database tables in simple YAML files, and **xpg** automatically creates, updates, and manages your PostgreSQL schema — no manual SQL migrations needed.

## Features

- **YAML Schema DSL** — Define tables with a concise, human-readable syntax
- **Diff-based migrations** — Automatically detects changes and generates `ALTER TABLE` statements
- **Custom field types** — Create reusable type aliases (`id`, `str`, `email`, etc.)
- **Read/write splitting** — Route queries to read replicas automatically
- **Transactions** — Atomic multi-query operations with auto-rollback
- **Multi-cluster & multi-tenant** — Manage multiple databases from a single config
- **Named parameters** — Use `:param` syntax for safe, parameterized queries
- **Data seeding** — Populate tables from YAML seed files
- **Environment variable interpolation** — Use `<ENV.VAR_NAME>` in config files
- **CLI + Programmatic API** — Use from the terminal or import as a library
- **Dry run mode** — Preview SQL without executing
- **Compact display** — Tree-like output for clear, scannable diffs

## Installation

```bash
npm install @appixar/xpg
```

## Quick Start

### 1. Initialize

```bash
npx xpg init
```

This creates:

- `xpg.config.yml` — Database connection and custom field types
- `database/example.yml` — Sample table definition

### 2. Configure

Edit `xpg.config.yml` with your PostgreSQL credentials:

```yaml
POSTGRES:
  DB:
    "main":
      NAME: my_database
      HOST: <ENV.DB_HOST>
      USER: <ENV.DB_USER>
      PASS: <ENV.DB_PASS>
      PORT: <ENV.DB_PORT>
      PREF: app_          # Table prefix (optional)
      PATH: [database]    # Directories containing .yml schema files

  # SEED_PATH: seeds      # Path to seed files (default: seeds)
  # SEED_SUFFIX: ".seed"  # Suffix for seed:dump filenames (e.g. table.seed.yml)
  # DISPLAY_MODE: table   # Display mode: compact (default) | table

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
      Type: varchar(12)
      Key: UNI
      Default: '"left"(md5((random())::text), 12)'
```

### 3. Define Tables

Create YAML files in your `database/` directory:

```yaml
# database/users.yml
users:
  user_id: id
  user_name: str required
  user_email: email required unique index
  user_status: str/32 default/active index
  user_date_insert: now
```

### 4. Run Migrations

```bash
npx xpg up
```

xpg will automatically:

- **Create** tables that don't exist
- **Update** tables that changed (add/alter/drop columns, indexes, constraints)
- Apply a table prefix if configured (e.g. `app_users`)

---

## YAML Schema DSL

Each field is defined as `field_name: type [modifiers...]`

### Types

Use any PostgreSQL type directly or a custom field alias:

| Alias | Resolves to | Notes |
|-------|-------------|-------|
| `id` | `SERIAL PRIMARY KEY` | Auto-increment, not null |
| `str` | `VARCHAR(64)` | Default string |
| `str/128` | `VARCHAR(128)` | Override length with `/N` |
| `text` | `TEXT` | Unlimited text |
| `int` | `INTEGER` | Integer |
| `float` | `REAL` | Float |
| `date` | `TIMESTAMP` | Timestamp |
| `email` | `VARCHAR(128)` | Semantic alias |
| `now` | `TIMESTAMP DEFAULT now()` | Auto-timestamp |
| `pid` | `VARCHAR(12) UNIQUE` | Random public ID |

You can also use raw PostgreSQL types: `varchar(255)`, `boolean`, `jsonb`, `uuid`, `numeric(16,8)`, etc.

### Modifiers

| Modifier | Description | Example |
|----------|-------------|---------|
| `required` | `NOT NULL` constraint | `user_name: str required` |
| `unique` | `UNIQUE` constraint | `user_email: email unique` |
| `unique/group` | Composite unique index | `ticker: str unique/pair` |
| `index` | Individual index | `user_status: str index` |
| `index/group` | Composite index | `created_at: date index/range` |
| `default/value` | Default value | `status: str default/active` |

> **Note:** Fields without `required` default to `NULL`.

### Composite Indexes & Unique Constraints

Group multiple fields into a single index or unique constraint using the `/group_name` syntax. Fields sharing the same group name are combined:

```yaml
stock_prices:
  price_id: id
  ticker: str required unique/ticker_ex index/lookup
  exchange: str required unique/ticker_ex index/lookup
  price: numeric(10,2) required
  date: date required
```

This generates:
- `CREATE UNIQUE INDEX "stock_prices_ticker_ex_unique_idx" ON "stock_prices" ("ticker", "exchange")`
- `CREATE INDEX "stock_prices_lookup_idx" ON "stock_prices" ("ticker", "exchange")`

### Full Example

```yaml
products:
  product_id: id
  product_name: varchar(200) required
  product_price: numeric(10,2) required default/0
  product_active: boolean default/true
  product_metadata: jsonb
  product_category: str index
  product_sku: str/32 required unique
  product_date_insert: now
```

---

## CLI

```bash
npx xpg <command> [options]
```

### Commands

| Command | Description |
|---------|-------------|
| `xpg up` | Run database migrations |
| `xpg diff` | Show schema differences without executing |
| `xpg query <sql>` | Execute a raw SQL query |
| `xpg seed [file]` | Populate database with seed data |
| `xpg seed:dump` | Generate YAML seed files from live database |
| `xpg init` | Generate sample config files |

### Options

| Flag | Commands | Description |
|------|----------|-------------|
| `--create` | `up` | Create the database if it doesn't exist |
| `--dry` | `up` | Preview SQL queries without executing |
| `--mute` | `up` | Suppress all output |
| `--drop-orphans` | `up` `diff` | Include `DROP TABLE` for tables not in YAML |
| `--display <mode>` | `up` `diff` | Output format: `compact` (default) or `table` |
| `--name <db>` | `up` `diff` `query` | Target a specific database cluster |
| `--tenant <key>` | `up` `diff` `query` | Target a specific tenant |
| `--yes` | `seed` | Skip per-table confirmation prompts |
| `--table <list>` | `seed` `seed:dump` | Comma-separated list of tables to seed/dump |
| `--exclude <list>` | `seed:dump` | Comma-separated list of tables to exclude |
| `--all` | `seed:dump` | Dump all tables without prompting |
| `--limit <n>` | `seed:dump` | Max rows per table |
| `--skip-auto` | `seed:dump` | Exclude auto-generated columns (SERIAL, now(), uuid) |
| `--config <path>` | all | Path to a custom config file |
| `--no-color` | all | Disable colored terminal output |

### Examples

```bash
# Preview changes (compact tree view)
npx xpg diff

# Preview changes (table view)
npx xpg diff --display table

# Dry run — show generated SQL without executing
npx xpg up --dry

# Create database if needed, then migrate
npx xpg up --create

# Execute a raw SQL query
npx xpg query "SELECT * FROM app_users LIMIT 10"

# Seed all files in the seed directory
npx xpg seed

# Seed a specific file
npx xpg seed users.yml

# Seed specific tables only
npx xpg seed --table app_users,app_config

# Seed without confirmation prompts
npx xpg seed --yes

# Target a specific cluster
npx xpg up --name main

# Remove tables not defined in YAML
npx xpg up --drop-orphans
```

---

## Data Seeding

Create YAML seed files in your seed directory (default: `seeds/`, configurable via `SEED_PATH`):

```yaml
# seeds/app_users.yml
app_users:
  - user_name: "Admin"
    user_email: "admin@example.com"
    user_status: "active"
  - user_name: "Test User"
    user_email: "test@example.com"
    user_status: "active"
```

```bash
npx xpg seed                           # all seed files
npx xpg seed app_users.yml              # specific file
npx xpg seed --table app_users          # specific table(s)
npx xpg seed --table app_users --yes    # skip confirmations
```

Seeding is **interactive by default** — each table is analyzed and you confirm before applying. Use `--yes` to skip prompts.

Uses **upsert** — rows are inserted if new, or updated if the primary key already exists. Rows already matching the seed data are skipped.

### Dumping Seeds from Database

Generate seed files from live data:

```bash
# Interactive — confirms each table
npx xpg seed:dump

# Specific tables only
npx xpg seed:dump --table app_users,app_products

# All tables, no prompts, limit 500 rows each
npx xpg seed:dump --all --limit 500

# Exclude large/log tables
npx xpg seed:dump --all --exclude app_logs,app_sessions

# Without auto-generated columns (IDs, timestamps, UUIDs)
npx xpg seed:dump --all --skip-auto
```

#### Smart File Updates

When dumping, xpg checks if a seed file already exists for the table (by scanning YAML root keys in all `.yml`/`.yaml` files in the seed directory). If found, the existing file is **updated in-place** — preserving other tables that may share the same file. If no existing file is found, a new one is created.

#### SEED_SUFFIX

By default, dumped files are named `<table>.yml`. You can configure a suffix so files are named `<table><suffix>.yml`:

```yaml
POSTGRES:
  SEED_SUFFIX: ".seed"   # → app_users.seed.yml, app_config.seed.yml
```

The suffix only affects **new** files created by `seed:dump`. If an existing file already contains the table's data, it is updated regardless of its filename. The `seed` command reads all `.yml`/`.yaml` files in `SEED_PATH`, so suffixed files are picked up automatically.

---

## Programmatic API

Import **xpg** as a library in your Node.js / Next.js project:

### Database — Query & Connection

```typescript
import { Database, loadConfig } from '@appixar/xpg';

const config = loadConfig();
const db = new Database(config.postgres.DB['main'], 'main');

// SELECT — auto-routed to read replica
const users = await db.query(
  'SELECT * FROM app_users WHERE user_status = :status',
  { status: 'active' }
);

// INSERT — returns last insert id
const id = await db.insert('app_users', {
  user_name: 'John',
  user_email: 'john@example.com',
});

// UPDATE — returns affected row count
const affected = await db.update('app_users',
  { user_status: 'inactive' },
  { user_id: 1 }
);

// DELETE — returns affected row count
const deleted = await db.delete('app_users', { user_id: 1 });

// FIND ONE — returns single row or null
const user = await db.findOne('app_users', { user_email: 'john@example.com' });

// FIND MANY — with options
const recent = await db.findMany('app_users',
  { user_status: 'active' },
  { orderBy: 'user_date_insert DESC', limit: 10 }
);

// Close all pools when done
await Database.closeAll();
```

### Named Parameters

Use `:paramName` syntax for safe, parameterized queries. Converted to `$1, $2...` internally:

```typescript
const rows = await db.query(
  'SELECT * FROM products WHERE price > :min AND category = :cat',
  { min: 100, cat: 'electronics' }
);
```

### Transactions

All queries inside a transaction share the same connection, ensuring atomicity:

```typescript
const orderId = await db.transaction(async (client) => {
  const [order] = await client.queryWith<{ id: number }>(
    'INSERT INTO orders (user_id) VALUES (:userId) RETURNING id',
    { userId: 42 }
  );

  await client.queryWith(
    'INSERT INTO order_items (order_id, product_id, qty) VALUES (:orderId, :productId, :qty)',
    { orderId: order.id, productId: 7, qty: 3 }
  );

  return order.id;
});
```

- **Auto-ROLLBACK** on error — if any query throws, the entire transaction is rolled back
- **Named params** — `client.queryWith()` supports `:param` syntax
- **Connection safety** — the connection is automatically released back to the pool

### Read/Write Splitting

`Database` automatically routes queries based on the SQL command:

- **SELECT / SHOW / EXPLAIN / WITH** → read replica pool
- **INSERT / UPDATE / DELETE / CREATE / ALTER** → write (primary) pool

Force all queries to primary when you need strong consistency:

```typescript
const db = new Database(cluster, 'main', { primary: true });
```

### Run Migrations Programmatically

```typescript
import { up } from '@appixar/xpg';

const result = await up({
  create: true,
  mute: true,
});

console.log(`Executed: ${result.executed}, Failed: ${result.failed.length}`);
```

---

## Read/Write Cluster Setup

Configure read replicas for automatic query routing:

```yaml
POSTGRES:
  DB:
    "main":
      - TYPE: write
        NAME: my_database
        HOST: primary.db.example.com
        USER: admin
        PASS: <ENV.DB_PASS>
        PORT: 5432
        PREF: app_
        PATH: [database]
      - TYPE: read
        NAME: my_database
        HOST: replica.db.example.com
        USER: reader
        PASS: <ENV.DB_READ_PASS>
        PORT: 5432
```

---

## Environment Variables

Use `<ENV.VAR_NAME>` syntax anywhere in `xpg.config.yml`:

```yaml
HOST: <ENV.DB_HOST>
PASS: <ENV.DB_PASS>
```

xpg automatically loads `.env` from your project root — no need for `dotenv`:

```bash
DB_HOST=localhost
DB_USER=admin
DB_PASS=secret
DB_PORT=5432
```

- System environment variables take precedence over `.env`
- Supports `export` prefix: `export DB_HOST=localhost`
- Supports inline comments: `DB_HOST=localhost # my local db`
- Missing variables print a warning and resolve to empty string

---

## Config Resolution

xpg looks for configuration in this order:

1. `--config <path>` CLI argument
2. `xpg.config.yml` in the current directory (or parent dirs)
3. `config/postgres.yml` + `config/custom_fields.yml` (PHP-compatible layout)

---

## How Migrations Work

When you run `xpg up`:

1. **Load config** — Reads `xpg.config.yml` and interpolates env vars
2. **Parse schemas** — Reads all `.yml` files in configured `PATH` directories
3. **Connect** — Connects to PostgreSQL (optionally creating the database)
4. **Diff** — Compares YAML schemas against the live database:
   - New tables → `CREATE TABLE`
   - Changed columns → `ALTER TABLE` (type, nullable, default)
   - New indexes/constraints → `CREATE INDEX`, `ADD CONSTRAINT`
   - Dropped indexes/constraints → `DROP INDEX`, `DROP CONSTRAINT`
   - Orphan tables (with `--drop-orphans`) → `DROP TABLE`
5. **Execute** — Runs generated SQL queries sequentially
6. **Report** — Summary of executed/failed queries

## Requirements

- Node.js ≥ 18
- PostgreSQL

## License

MIT
