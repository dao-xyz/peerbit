import { logger as logFn } from "@peerbit/logger";

export const logger: ReturnType<typeof logFn> = logFn({
	module: "lazystream",
	level: "warn",
});
