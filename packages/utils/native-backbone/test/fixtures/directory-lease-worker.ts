import { acquireNativeDurabilityNodeLease } from "../../src/durability/node-lease.js";

const [mode, directory] = process.argv.slice(2);

const send = (message: unknown): void => {
	if (!process.send) {
		throw new Error("Directory lease worker requires an IPC channel");
	}
	process.send(message);
};

try {
	if (mode !== "hold" || !directory) {
		throw new Error("Expected: directory-lease-worker hold <directory>");
	}
	const lease = await acquireNativeDurabilityNodeLease(directory);
	send({
		event: "held",
		fence: {
			epoch: lease.fence.epoch.toString(),
			ownerId: lease.fence.ownerId,
			domainId: lease.fence.domainId,
		},
	});
	// The test parent deliberately terminates this process without calling
	// close(), proving the operating-system lock is crash released.
	setInterval(() => undefined, 60_000);
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
