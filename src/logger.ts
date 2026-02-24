// ─────────────────────────────────────────────
// x-postgres — Logger
// ─────────────────────────────────────────────
// Terminal output utilities with color support.
// Respects NO_COLOR env and --no-color flag.

import chalk from 'chalk';
import boxen from 'boxen';
import ora, { type Ora } from 'ora';
import type { LogColor } from './types.js';
import { readFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));

function getVersion(): string {
    try {
        const pkg = JSON.parse(readFileSync(resolve(__dirname, '..', 'package.json'), 'utf-8'));
        return pkg.version ?? '0.0.0';
    } catch {
        return '0.0.0';
    }
}

let activeSpinner: Ora | null = null;

function isColorDisabled(): boolean {
    return 'NO_COLOR' in process.env || process.argv.includes('--no-color');
}

export const colorFns: Record<LogColor, (s: string) => string> = {
    green: (s) => isColorDisabled() ? s : chalk.green(s),
    yellow: (s) => isColorDisabled() ? s : chalk.yellow(s),
    cyan: (s) => isColorDisabled() ? s : chalk.cyan(s),
    gray: (s) => isColorDisabled() ? s : chalk.gray(s),
    red: (s) => isColorDisabled() ? s : chalk.red(s),
    magenta: (s) => isColorDisabled() ? s : chalk.magenta(s),
    blue: (s) => isColorDisabled() ? s : chalk.blue(s),
    white: (s) => isColorDisabled() ? s : chalk.white(s),
};

export function stopSpinner(): void {
    if (activeSpinner) {
        activeSpinner.stop();
        activeSpinner = null;
    }
}

export function spin(text: string): void {
    stopSpinner();
    if (!isColorDisabled()) {
        activeSpinner = ora({
            text,
            color: 'cyan',
            spinner: 'dots'
        }).start();
    } else {
        console.log(`[wait] ${text}`);
    }
}

export function succeed(text: string): void {
    if (activeSpinner) {
        activeSpinner.succeed(chalk.green(text));
        activeSpinner = null;
    } else {
        console.log(chalk.green(`✔ ${text}`));
    }
}

export function fail(text: string): void {
    if (activeSpinner) {
        activeSpinner.fail(chalk.red(text));
        activeSpinner = null;
    } else {
        console.log(chalk.red(`✖ ${text}`));
    }
}

export function welcome(): void {
    if (isColorDisabled()) {
        console.log(`xpg v${getVersion()}`);
        return;
    }
    console.log(chalk.cyan('⚡') + ' ' + chalk.bold('xpg') + ' ' + chalk.dim(`v${getVersion()}`));
}

export function banner(): void {
    if (isColorDisabled()) {
        welcome();
        return;
    }
    const v = getVersion();
    console.log(boxen(chalk.bold.cyan('x-postgres') + ' ' + chalk.dim(`v${v}`) + '\n' + chalk.dim('Schema Management & Migrations'), {
        padding: 1,
        margin: 1,
        borderStyle: 'round',
        borderColor: 'cyan',
        float: 'center'
    }));
}

export function header(text: string, color: LogColor = 'cyan'): void {
    stopSpinner();
    const colorize = colorFns[color] ?? colorFns.cyan;
    console.log('\n' + colorize(chalk.bold(text)) + '\n');
}

export function say(text: string, color?: LogColor): void {
    if (activeSpinner) {
        activeSpinner.stop();
        activeSpinner = null;
    }
    if (color && colorFns[color]) {
        console.log(colorFns[color](text));
    } else {
        console.log(text);
    }
}

export function success(text: string): void {
    succeed(text);
}

export function warn(text: string): void {
    if (activeSpinner) {
        activeSpinner.warn(chalk.yellow(text));
        activeSpinner = null;
    } else {
        console.log(chalk.yellow(`⚠ ${text}`));
    }
}

export function error(text: string): void {
    fail(text);
}

export function step(text: string): void {
    stopSpinner();
    console.log(chalk.blue(`➜ ${text}`));
}

// ─── Semantic Loggers ───

export function added(text: string): void {
    console.log(chalk.green(`+ ${text}`));
}

export function removed(text: string): void {
    console.log(chalk.red(`- ${text}`));
}

export function changed(text: string): void {
    console.log(chalk.yellow(`~ ${text}`));
}

export function info(text: string): void {
    console.log(chalk.blue(`i ${text}`));
}
