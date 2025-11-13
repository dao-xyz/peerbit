import pino from "pino";

// Robust Node detection (true only in Node.js)
const isNode = typeof process !== "undefined" && !!process.versions?.node;

type LogLevel =
	| "fatal"
	| "error"
	| "warn"
	| "info"
	| "debug"
	| "trace"
	| "silent";

/**
 * Safely read an env var from:
 * - Node: process.env
 * - Webpack/CRA (browser/worker): globalThis.process.env (if defined by bundler)
 * - Vite (browser/worker): import.meta.env
 */
export const getEnv = (key: string): string | undefined => {
	if (isNode) {
		return process.env?.[key];
	}

	// Some bundlers (Webpack/CRA) inject a process shim
	const g: any = typeof globalThis !== "undefined" ? (globalThis as any) : {};
	if (g.process?.env && typeof g.process.env[key] !== "undefined") {
		return g.process.env[key];
	}

	// Vite exposes import.meta.env
	let metaEnv: any;
	try {
		// eslint-disable-next-line @typescript-eslint/ban-ts-comment
		// @ts-ignore - import.meta may not exist in TS/node type context
		metaEnv = import.meta?.env;
	} catch {
		// ignore if not supported
	}
	if (metaEnv && typeof metaEnv[key] !== "undefined") {
		return metaEnv[key];
	}

	return undefined;
};

export const getLogLevel = (): LogLevel | undefined => {
	const level = getEnv("LOG_LEVEL") || getEnv("REACT_APP_LOG_LEVEL");
	if (!level) return undefined;

	const levels: LogLevel[] = [
		"fatal",
		"error",
		"warn",
		"info",
		"debug",
		"trace",
		"silent",
	];
	if (!levels.includes(level as LogLevel)) {
		throw new Error(
			`Unexpected LOG_LEVEL: ${level}. Expecting one of: ${JSON.stringify(levels)}`,
		);
	}
	return level as LogLevel;
};

const logger = (options?: { module?: string; level?: LogLevel }) => {
	// In browsers/workers, pino's browser build logs to console by default.
	let base = pino.default();

	if (options?.module) {
		base = base.child({ module: options.module });
	}

	const level = options?.level ?? getLogLevel();
	if (level) {
		base.level = level;
	}

	return base;
};

export { logger };
