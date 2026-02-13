// ─────────────────────────────────────────────
// x-postgres — Logger
// ─────────────────────────────────────────────
// Terminal output utilities with color support.
// Respects NO_COLOR env and --no-color flag.

import chalk from 'chalk';
import type { LogColor } from './types.js';

/**
 * Check if color output is disabled.
 * Respects NO_COLOR standard (https://no-color.org/) and --no-color CLI flag.
 */
function isColorDisabled(): boolean {
    return 'NO_COLOR' in process.env || process.argv.includes('--no-color');
}

const colorFns: Record<LogColor, (s: string) => string> = {
    green: (s) => isColorDisabled() ? s : chalk.green(s),
    yellow: (s) => isColorDisabled() ? s : chalk.yellow(s),
    cyan: (s) => isColorDisabled() ? s : chalk.cyan(s),
    gray: (s) => isColorDisabled() ? s : chalk.gray(s),
    red: (s) => isColorDisabled() ? s : chalk.red(s),
    magenta: (s) => isColorDisabled() ? s : chalk.magenta(s),
    blue: (s) => isColorDisabled() ? s : chalk.blue(s),
    white: (s) => isColorDisabled() ? s : chalk.white(s),
};

/**
 * Print a section header with a separator line.
 */
export function header(text: string, color: LogColor = 'green'): void {
    const line = '━'.repeat(50);
    const colorize = colorFns[color] ?? colorFns.green;
    console.log(colorize(line));
    console.log(colorize(text));
    console.log(colorize(line));
}

/**
 * Print a message, optionally colored.
 */
export function say(text: string, color?: LogColor): void {
    if (color && colorFns[color]) {
        console.log(colorFns[color](text));
    } else {
        console.log(text);
    }
}

/**
 * Print a success message (green).
 */
export function success(text: string): void {
    say(text, 'green');
}

/**
 * Print a warning message (yellow).
 */
export function warn(text: string): void {
    say(text, 'yellow');
}

/**
 * Print an error message (red).
 */
export function error(text: string): void {
    say(text, 'red');
}
