// ─────────────────────────────────────────────
// x-postgres — Logger
// ─────────────────────────────────────────────
// Terminal output utilities with color support.
// Respects NO_COLOR env and --no-color flag.

import chalk from 'chalk';
import boxen from 'boxen';
import ora, { type Ora } from 'ora';
import type { LogColor } from './types.js';

let activeSpinner: Ora | null = null;

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
 * Stop the active spinner if it exists.
 */
export function stopSpinner(): void {
    if (activeSpinner) {
        activeSpinner.stop();
        activeSpinner = null;
    }
}

/**
 * Start a spinner with a message.
 */
export function spin(text: string): void {
    stopSpinner();
    if (!isColorDisabled()) {
        activeSpinner = ora({
            text,
            color: 'cyan',
            spinner: 'dots' // Elegant dots
        }).start();
    } else {
        console.log(`[wait] ${text}`);
    }
}

/**
 * Succeed the current spinner or print a success message.
 */
export function succeed(text: string): void {
    if (activeSpinner) {
        activeSpinner.succeed(chalk.green(text));
        activeSpinner = null;
    } else {
        console.log(chalk.green(`✔ ${text}`));
    }
}

/**
 * Fail the current spinner or print an error message.
 */
export function fail(text: string): void {
    if (activeSpinner) {
        activeSpinner.fail(chalk.red(text));
        activeSpinner = null;
    } else {
        console.log(chalk.red(`✖ ${text}`));
    }
}

/**
 * Print a nice welcome banner.
 */
export function welcome(): void {
    if (isColorDisabled()) return;

    console.log(boxen(chalk.bold.cyan('x-postgres') + '\n' + chalk.dim('Schema Management & Migrations'), {
        padding: 1,
        margin: 1,
        borderStyle: 'round',
        borderColor: 'cyan',
        float: 'center'
    }));
}

/**
 * Print a section header with a separator line.
 */
export function header(text: string, color: LogColor = 'green'): void {
    stopSpinner(); // Stop any overlapping spinner
    const colorize = colorFns[color] ?? colorFns.green;
    console.log('');
    console.log(colorize('● ' + text));
    console.log('');
}

/**
 * Print a message, optionally colored.
 */
export function say(text: string, color?: LogColor): void {
    if (activeSpinner) {
        // If spinner is active, stop it, print, and restart? 
        // Or just stop it for a moment. 
        // Better: stop, print info, leave stopped?
        // Actually ora handles console.log but it's cleaner to stop.
        activeSpinner.stop();
        // We won't restart it here automatically as 'say' is usually a discrete event.
        // If the caller wants the spinner back, they should call spin() again.
        activeSpinner = null;
    }

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
    succeed(text);
}

/**
 * Print a warning message (yellow).
 */
export function warn(text: string): void {
    if (activeSpinner) {
        activeSpinner.warn(chalk.yellow(text));
        activeSpinner = null;
    } else {
        console.log(chalk.yellow(`⚠ ${text}`));
    }
}

/**
 * Print an error message (red).
 */
export function error(text: string): void {
    fail(text);
}

/**
 * Print a step message (blue arrow).
 */
export function step(text: string): void {
    stopSpinner();
    console.log(chalk.blue(`➜ ${text}`));
}
