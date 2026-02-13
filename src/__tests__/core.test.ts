// ─────────────────────────────────────────────
// x-postgres — Unit tests
// ─────────────────────────────────────────────
// Uses Node.js built-in test runner (node --test)

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { parseSchema } from '../schemaParser.js';
import { normalizeDefaultSql, buildDefaultClause, normalizeDbDefaultForCompare } from '../defaultNormalizer.js';
import { generateCreateTable, generateDropTable } from '../sqlGenerator.js';
import { POSTGRES_TYPE_DICTIONARY } from '../typeDictionary.js';
import type { CustomFieldDef } from '../types.js';

// ──────────────────────────────────────────
// Schema Parser Tests
// ──────────────────────────────────────────

const CUSTOM_FIELDS: Record<string, CustomFieldDef> = {
    id: { Type: 'serial', Key: 'PRI' },
    str: { Type: 'varchar(64)' },
    text: { Type: 'text' },
    int: { Type: 'integer' },
    now: { Type: 'timestamp', Default: 'now()' },
    pid: { Type: 'varchar(12)', Key: 'UNI', Default: '"left"(md5((random())::text), 12)' },
    email: { Type: 'varchar(128)' },
};

describe('schemaParser', () => {
    it('parses a simple id field', () => {
        const result = parseSchema({ user_id: 'id' }, CUSTOM_FIELDS);
        const f = result.fields['user_id'];
        assert.ok(f, 'field should exist');
        assert.equal(f.type, 'SERIAL');
        assert.equal(f.key, 'PRI');
        assert.equal(f.nullable, '');  // SERIAL = implicit NOT NULL
    });

    it('parses str with length override', () => {
        const result = parseSchema({ name: 'str/128' }, CUSTOM_FIELDS);
        const f = result.fields['name'];
        assert.equal(f.type, 'VARCHAR(128)');
    });

    it('parses required modifier → NOT NULL', () => {
        const result = parseSchema({ name: 'str required' }, CUSTOM_FIELDS);
        const f = result.fields['name'];
        assert.equal(f.nullable, 'NOT NULL');
    });

    it('default: NULL when not required', () => {
        const result = parseSchema({ name: 'str' }, CUSTOM_FIELDS);
        const f = result.fields['name'];
        assert.equal(f.nullable, 'NULL');
    });

    it('parses unique modifier', () => {
        const result = parseSchema({ email: 'email unique' }, CUSTOM_FIELDS);
        const f = result.fields['email'];
        assert.equal(f.key, 'UNI');
    });

    it('parses custom field defaults', () => {
        const result = parseSchema({ created: 'now' }, CUSTOM_FIELDS);
        const f = result.fields['created'];
        assert.equal(f.type, 'TIMESTAMP');
        assert.equal(f.defaultValue, 'now()');
    });

    it('parses explicit default/value', () => {
        const result = parseSchema({ status: 'str/32 default/active' }, CUSTOM_FIELDS);
        const f = result.fields['status'];
        assert.equal(f.defaultValue, 'active');
    });

    it('parses individual index', () => {
        const result = parseSchema({ email: 'email index' }, CUSTOM_FIELDS);
        assert.ok(result.individualIndexes.includes('email'));
    });

    it('parses composite index', () => {
        const result = parseSchema({
            user_id: 'int index/user_date',
            log_date: 'now index/user_date',
        }, CUSTOM_FIELDS);
        assert.deepEqual(result.compositeIndexes['user_date'], ['user_id', 'log_date']);
    });

    it('parses composite unique index', () => {
        const result = parseSchema({
            ticker: 'str unique/ticker_ex',
            exchange: 'str unique/ticker_ex',
        }, CUSTOM_FIELDS);
        assert.deepEqual(result.compositeUniqueIndexes['ticker_ex'], ['ticker', 'exchange']);
    });

    it('skips ~ meta keys', () => {
        const result = parseSchema({
            '~ignore': 'true' as string,
            name: 'str',
        }, CUSTOM_FIELDS);
        assert.ok(!result.fields['~ignore']);
        assert.ok(result.fields['name']);
    });

    it('handles null/undefined input', () => {
        const result = parseSchema(null, CUSTOM_FIELDS);
        assert.deepEqual(result.fields, {});
    });
});

// ──────────────────────────────────────────
// Default Normalizer Tests
// ──────────────────────────────────────────

describe('normalizeDefaultSql', () => {
    it('returns null for null/empty/undefined', () => {
        assert.equal(normalizeDefaultSql(null, 'TEXT'), null);
        assert.equal(normalizeDefaultSql(undefined, 'TEXT'), null);
        assert.equal(normalizeDefaultSql('', 'TEXT'), null);
    });

    it('returns null for "null" string', () => {
        assert.equal(normalizeDefaultSql('null', 'TEXT'), null);
        assert.equal(normalizeDefaultSql('NULL', 'TEXT'), null);
    });

    it('keeps SQL functions as-is', () => {
        assert.equal(normalizeDefaultSql('now()', 'TIMESTAMP'), 'now()');
        assert.equal(normalizeDefaultSql('random()', 'REAL'), 'random()');
    });

    it('handles CURRENT_TIMESTAMP keyword', () => {
        assert.equal(normalizeDefaultSql('CURRENT_TIMESTAMP', 'TIMESTAMP'), 'CURRENT_TIMESTAMP');
    });

    it('normalizes booleans', () => {
        assert.equal(normalizeDefaultSql('true', 'BOOLEAN'), 'TRUE');
        assert.equal(normalizeDefaultSql('false', 'BOOLEAN'), 'FALSE');
    });

    it('keeps numeric values as-is', () => {
        assert.equal(normalizeDefaultSql('0', 'INTEGER'), '0');
        assert.equal(normalizeDefaultSql('3.14', 'REAL'), '3.14');
        assert.equal(normalizeDefaultSql('-1', 'INTEGER'), '-1');
    });

    it('casts JSON/JSONB objects', () => {
        assert.equal(normalizeDefaultSql('{}', 'JSONB'), "'{}'::jsonb");
        assert.equal(normalizeDefaultSql('[]', 'JSON'), "'[]'::json");
    });

    it('quotes UUID literals', () => {
        assert.equal(
            normalizeDefaultSql('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'UUID'),
            "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"
        );
    });

    it('wraps plain strings in single quotes', () => {
        assert.equal(normalizeDefaultSql('active', 'VARCHAR(32)'), "'active'");
    });

    it('keeps already single-quoted strings', () => {
        assert.equal(normalizeDefaultSql("'hello'", 'TEXT'), "'hello'");
    });

    it('converts double-quoted to single-quoted', () => {
        assert.equal(normalizeDefaultSql('"world"', 'TEXT'), "'world'");
    });

    it('strips DEFAULT prefix if present', () => {
        assert.equal(normalizeDefaultSql('DEFAULT 42', 'INTEGER'), '42');
    });
});

describe('buildDefaultClause', () => {
    it('returns empty string for null', () => {
        assert.equal(buildDefaultClause(null, 'TEXT'), '');
    });

    it('builds DEFAULT clause', () => {
        assert.equal(buildDefaultClause('0', 'INTEGER'), 'DEFAULT 0');
        assert.equal(buildDefaultClause('now()', 'TIMESTAMP'), 'DEFAULT now()');
        assert.equal(buildDefaultClause('active', 'VARCHAR(32)'), "DEFAULT 'active'");
    });
});

describe('normalizeDbDefaultForCompare', () => {
    it('strips ::type casts', () => {
        assert.equal(normalizeDbDefaultForCompare("'0'::integer"), '0');
        assert.equal(normalizeDbDefaultForCompare("'active'::character varying"), 'active');
    });

    it('strips multiword type casts', () => {
        assert.equal(
            normalizeDbDefaultForCompare("'2024-01-01'::timestamp without time zone"),
            '2024-01-01'
        );
    });

    it('keeps nextval untouched', () => {
        const val = "nextval('users_id_seq'::regclass)";
        assert.equal(normalizeDbDefaultForCompare(val), val);
    });

    it('normalizes booleans', () => {
        assert.equal(normalizeDbDefaultForCompare('TRUE'), 'true');
        assert.equal(normalizeDbDefaultForCompare('false::boolean'), 'false');
    });

    it('handles null/empty', () => {
        assert.equal(normalizeDbDefaultForCompare(null), '');
        assert.equal(normalizeDbDefaultForCompare(''), '');
    });

    it('unescapes double single-quotes', () => {
        assert.equal(normalizeDbDefaultForCompare("'it''s'::text"), "it's");
    });
});

// ──────────────────────────────────────────
// SQL Generator Tests
// ──────────────────────────────────────────

describe('generateCreateTable', () => {
    it('generates CREATE TABLE with correct structure', () => {
        const schema = parseSchema({
            user_id: 'id',
            user_name: 'str required',
            user_email: 'email unique index',
            created: 'now',
        }, CUSTOM_FIELDS);

        const queries = generateCreateTable('users', schema, true);
        assert.ok(queries.length > 0);

        const createQ = queries[0];
        assert.ok(createQ.sql.includes('CREATE TABLE "users"'));
        assert.ok(createQ.sql.includes('"user_id" SERIAL'));
        assert.ok(createQ.sql.includes('PRIMARY KEY'));
        assert.ok(createQ.sql.includes('"user_name" VARCHAR(64) NOT NULL'));
        assert.ok(createQ.sql.includes('DEFAULT now()'));

        // Should have UNIQUE constraint
        const uniqueQ = queries.find(q => q.sql.includes('UNIQUE'));
        assert.ok(uniqueQ, 'should have UNIQUE constraint for email');

        // Should have INDEX
        const indexQ = queries.find(q => q.sql.includes('CREATE INDEX'));
        assert.ok(indexQ, 'should have INDEX for email');
    });
});

describe('generateDropTable', () => {
    it('generates DROP TABLE CASCADE', () => {
        const q = generateDropTable('old_table', true);
        assert.equal(q.sql, 'DROP TABLE IF EXISTS "old_table" CASCADE;');
        assert.equal(q.color, 'yellow');
    });
});

// ──────────────────────────────────────────
// Type Dictionary Tests
// ──────────────────────────────────────────

describe('typeDictionary', () => {
    it('maps SERIAL to integer', () => {
        assert.equal(POSTGRES_TYPE_DICTIONARY['SERIAL'], 'integer');
    });

    it('maps VARCHAR to character varying', () => {
        assert.equal(POSTGRES_TYPE_DICTIONARY['VARCHAR'], 'character varying');
    });

    it('maps TIMESTAMP to timestamp without time zone', () => {
        assert.equal(POSTGRES_TYPE_DICTIONARY['TIMESTAMP'], 'timestamp without time zone');
    });

    it('maps TEXT to text', () => {
        assert.equal(POSTGRES_TYPE_DICTIONARY['TEXT'], 'text');
    });
});
