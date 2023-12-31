import pino from "pino";

const isNode = typeof window === "undefined";

export const getEnv = (key) => {
	if (isNode) {
		// node
		return process.env[key];
	}
	// browser
	return window.process?.env?.[key];
};
export const getLogLevel = () => {
	const level = getEnv("LOG_LEVEL") || getEnv("REACT_APP_LOG_LEVEL");
	if (!level) {
		return undefined;
	}
	const levels = ["fatal", "error", "warn", "info", "debug", "trace"];
	if (levels.indexOf(level) === -1) {
		throw new Error(
			"Unexpected LOG_LEVEL: " +
				level +
				". Expecting one of: " +
				JSON.stringify(levels)
		);
	}
	return level;
};

const logger = (options?: { module?: string; level?: string }) => {
	let logger = pino();
	if (options?.module) {
		logger = logger.child({ module: options.module });
	}
	if (options?.level) {
		logger.level = options.level;
	} else {
		const logLevel = getLogLevel();
		if (logLevel) {
			logger.level = logLevel;
		}
	}
	return logger;
};

export { logger };
