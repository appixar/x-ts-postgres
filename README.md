# @appixar/xpg

YAML-driven PostgreSQL schema management, diff-based migrations, and query service for Node.js.

Define your database tables in simple YAML files, and **xpg** automatically creates, updates, and manages your PostgreSQL schema — no manual SQL migrations needed.

## Features

- **YAML Schema DSL** — Define tables with a concise, human-readable syntax
- **Diff-based migrations** — Automatically detects changes and generates `ALTER TABLE` statements
- **Custom field types** — Create reusable type aliases (`id`, `str`, `email`, etc.)
- **Read/write splitting** — Route queries to read replicas automatically
- **Multi-cluster support** — Manage multiple database clusters from a single config
- **Environment variable interpolation** — Use `<ENV.VAR_NAME>` in config files
- **CLI + Programmatic API** — Use from the terminal or import as a library
- **Dry run mode** — Preview SQL without executing

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
```

### 3. Define Tables

Create YAML files in your `database/` directory:

```yaml
# database/users.yml
users:
  user_id: id
  user_name: str required
  user_email: email unique index
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

You can also use raw PostgreSQL types: `varchar(255)`, `integer`, `boolean`, `jsonb`, `uuid`, `numeric(16,8)`, etc.

### Modifiers

| Modifier | Description |
|----------|-------------|
| `required` | Adds `NOT NULL` constraint |
| `unique` | Adds `UNIQUE` constraint |
| `unique/group_name` | Composite unique constraint |
| `index` | Creates an index on this column |
| `index/group_name` | Composite index |
| `default/value` | Sets default value |

### Examples

```yaml
products:
  product_id: id
  product_name: varchar(200) required
  product_price: numeric(10,2) required default/0
  product_active: boolean default/true
  product_metadata: jsonb default/{}
  product_category: str index
  product_sku: str/32 unique
  product_date_insert: now

  # Composite index on category + active
  product_category: str index/cat_active
  product_active: boolean index/cat_active

  # Composite unique on name + category
  product_name: varchar(200) unique/name_cat
  product_category: str unique/name_cat
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
| `xpg init` | Generate sample config files |

### Options for `xpg up`

| Flag | Description |
|------|-------------|
| `--create` | Create the database if it doesn't exist |
| `--name <db>` | Target a specific database cluster by name |
| `--tenant <key>` | Target a specific tenant |
| `--dry` | Preview SQL queries without executing |
| `--mute` | Suppress all output |
| `--drop-orphans` | Drop tables that exist in DB but not in YAML |
| `--config <path>` | Path to a custom config file |
| `--no-color` | Disable colored terminal output |

### Examples

```bash
# Dry run — preview what would happen
npx xpg up --dry

# Create database if needed, then migrate
npx xpg up --create

# Target a specific cluster
npx xpg up --name main

# Remove tables not defined in YAML
npx xpg up --drop-orphans
```

---

## Programmatic API

Import **xpg** as a library in your Node.js / Next.js project:

### PgService — Query Service

```typescript
import { PgService, loadConfig } from '@appixar/xpg';

const config = loadConfig();
const cluster = config.postgres.DB['main'];

const db = new PgService(cluster, 'main');

// Automatic read/write routing
const users = await db.query<{ user_id: number; user_name: string }>(
  'SELECT * FROM app_users WHERE user_status = :status',
  { status: 'active' }
);

// Insert
const id = await db.insert('app_users', {
  user_name: 'John',
  user_email: 'john@example.com',
});

// Update
const affected = await db.update('app_users', 
  { user_status: 'inactive' },       // data
  { user_id: 1 }                      // condition
);

// Update with string condition
await db.update('app_users',
  { user_status: 'inactive' },
  'user_last_login < NOW() - INTERVAL \'30 days\''
);

// Close pools when done
await db.closeAll();
```

### Named Parameters

Use `:paramName` syntax for safe, parameterized queries:

```typescript
const rows = await db.query(
  'SELECT * FROM products WHERE price > :min AND category = :cat',
  { min: 100, cat: 'electronics' }
);
```

### Read/Write Splitting

`PgService` automatically routes:
- **SELECT / SHOW / EXPLAIN / WITH** → read replica pool
- **INSERT / UPDATE / DELETE / CREATE / ALTER** → write (primary) pool

Force primary for reads when you need consistency:

```typescript
const db = new PgService(cluster, 'main', { primary: true });
```

### Run Migrations Programmatically

```typescript
import { up } from '@appixar/xpg';

const result = await up({
  dry: false,
  create: true,
  mute: true,
});

console.log(`Executed ${result.executed} queries`);
console.log(`Failed: ${result.failed.length}`);
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

If a variable is not defined, xpg will print a warning and use an empty string.

---

## Config Resolution

xpg looks for configuration in this order:

1. `--config <path>` CLI argument
2. `xpg.config.yml` in the current directory
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
5. **Execute** — Runs all generated SQL queries sequentially
6. **Report** — Shows a summary of executed/failed queries

---

## Supported PostgreSQL Types

| YAML Type | PostgreSQL Type |
|-----------|----------------|
| `serial` | `integer` (auto-increment) |
| `varchar(N)` | `character varying(N)` |
| `integer` / `int` | `integer` |
| `text` | `text` |
| `timestamp` | `timestamp without time zone` |
| `date` | `date` |
| `boolean` | `boolean` |
| `smallint` | `smallint` |
| `bigint` | `bigint` |
| `real` / `float` | `real` |
| `double` | `double precision` |
| `numeric(P,S)` | `numeric(P,S)` |
| `json` | `json` |
| `jsonb` | `jsonb` |
| `uuid` | `uuid` |

---

## Requirements

- Node.js ≥ 18
- PostgreSQL

## License

MIT
