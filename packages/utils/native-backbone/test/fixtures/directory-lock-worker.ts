import { acquireNativeDurabilityNodeLock } from "../../src/durability/node-lease.js";

const [mode, directory] = process.argv.slice(2);

const send = (message: unknown): void => {
	if (!process.send) {
		throw new Error("Directory lock worker requires an IPC channel");
	}
	process.send(message);
};

try {
	if (mode !== "hold" || !directory) {
		throw new Error("Expected: directory-lock-worker hold <directory>");
	}
	const lock = await acquireNativeDurabilityNodeLock(directory);
	send({ event: "held" });
	// The test parent deliberately terminates this process without calling
	// close(), proving the operating-system lock is crash released. Retain the
	// capability strongly so a native database finalizer cannot release it first.
	setInterval(() => void lock, 60_000);
} catch (error) {
	const typed = error as { name?: string; code?: string; message?: string };
	send({
		event: "error",
		name: typed?.name,
		code: typed?.code,
		message: typed?.message ?? String(error),
	});
	process.exitCode = 1;
}
