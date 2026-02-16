// ─────────────────────────────────────────────
// x-postgres — Display Renderer
// ─────────────────────────────────────────────
// Renders query lists in different display modes.
// Supports: 'table' (cli-table3) and 'compact' (tree view).

import Table from 'cli-table3';
import chalk from 'chalk';
import type { QueuedQuery } from './types.js';

export type DisplayMode = 'table' | 'compact';

// ─── Icon & color map ───────────────────────

interface ActionStyle {
    icon: string;
    color: (s: string) => string;
}

const ACTION_STYLES: Record<string, ActionStyle> = {
    CREATE_TABLE: { icon: '✚', color: chalk.green },
    CREATE_DB:    { icon: '⊕', color: chalk.green },
    ADD_COLUMN:   { icon: '✚', color: chalk.green },
    ADD_INDEX:    { icon: '✚', color: chalk.green },
    ALTER_COLUMN: { icon: '✎', color: chalk.cyan },
    DROP_COLUMN:  { icon: '✖', color: chalk.yellow },
    DROP_INDEX:   { icon: '✖', color: chalk.yellow },
    DROP_TABLE:   { icon: '✖', color: chalk.red },
    RAW:          { icon: '•', color: chalk.gray },
};

function getStyle(type: string): ActionStyle {
    return ACTION_STYLES[type] ?? { icon: '•', color: chalk.white };
}

// ─── Public API ─────────────────────────────

/**
 * Render a list of queued queries in the specified display mode.
 */
export function renderQueries(queries: QueuedQuery[], mode: DisplayMode = 'table'): void {
    if (queries.length === 0) return;

    if (mode === 'compact') {
        renderCompact(queries);
    } else {
        renderTable(queries);
    }
}

/**
 * Render summary line after displaying queries.
 */
export function renderSummary(count: number, label: string = 'changes'): void {
    if (count > 0) {
        console.log(chalk.cyan(`\n  ${chalk.bold(String(count))} ${label} found.\n`));
    } else {
        console.log(chalk.green(`\n  ✔ No ${label} — everything is in sync.\n`));
    }
}

// ─── Table mode ─────────────────────────────

function renderTable(queries: QueuedQuery[]): void {
    const table = new Table({
        head: ['Table', 'Type', 'Description'],
        style: { head: ['cyan'] },
        wordWrap: true,
    });

    for (const q of queries) {
        const style = getStyle(q.type);
        const typeColor = style.color === chalk.green ? 'green' : style.color === chalk.yellow ? 'yellow' : style.color === chalk.red ? 'red' : 'cyan';
        // @ts-ignore - cli-table3 style types are incomplete
        table.push([q.table, { content: q.type, style: { 'padding-left': 1, color: typeColor } }, q.description]);
    }

    console.log(table.toString());
}

// ─── Compact mode ───────────────────────────

function renderCompact(queries: QueuedQuery[]): void {
    // Group queries by table name (preserving insertion order)
    const groups = new Map<string, QueuedQuery[]>();
    for (const q of queries) {
        const key = q.table || '(global)';
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key)!.push(q);
    }

    console.log(''); // Leading space

    for (const [tableName, items] of groups) {
        // Table header — bold + colored by predominant action
        const predominant = getPredominantAction(items);
        const headerColor = predominant.color;
        console.log(`  ${headerColor(chalk.bold(tableName))}`);

        // Render each action as a tree branch
        for (let i = 0; i < items.length; i++) {
            const q = items[i];
            const isLast = i === items.length - 1;
            const connector = isLast ? '└─' : '├─';
            const style = getStyle(q.type);

            const line = chalk.dim(connector) + ' ' + style.color(`${style.icon} ${q.description}`);
            console.log(`    ${line}`);
        }

        console.log(''); // Spacing between tables
    }
}

/**
 * Determine the predominant action type to color the table header.
 * Priority: DROP > CREATE > ALTER
 */
function getPredominantAction(queries: QueuedQuery[]): ActionStyle {
    const hasCreate = queries.some(q => q.type === 'CREATE_TABLE');
    const hasDrop = queries.some(q => q.type === 'DROP_TABLE');

    if (hasDrop) return ACTION_STYLES.DROP_TABLE;
    if (hasCreate) return ACTION_STYLES.CREATE_TABLE;
    return ACTION_STYLES.ALTER_COLUMN;
}
