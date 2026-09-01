// This plain Node worker imports the compiled artifact under test. The parent
// deliberately kills it immediately after the durability barriers resolve.
const { createStore } = await import(
	new URL("../dist/src/store.js", import.meta.url).href
);
const { CrashSafeTwoSlotCheckpoint } = await import(
	new URL("../dist/src/checkpoint.js", import.meta.url).href
);

const [directory, mode = "barrier"] = process.argv.slice(2);
if (!directory) {
	throw new Error("Expected a store directory");
}

const store = createStore(directory);
await store.open();
if (mode === "checkpoint") {
	const checkpoint = await CrashSafeTwoSlotCheckpoint.open({
		store,
		scope: new TextEncoder().encode("hard-kill-checkpoint"),
		maxPayloadBytes: 16,
	});
	const committed = await checkpoint.commit(new Uint8Array([2, 3, 4]));
	process.stdout.write(
		JSON.stringify({
			event: "ack",
			generation: committed.generation.toString(),
		}) + "\n",
	);
} else {
	const sublevel = await store.sublevel("hard-kill-sublevel");
	await sublevel.open();

	await store.del("root-deleted");
	await sublevel.del("sublevel-deleted");
	await store.crashSafeDurability.barrier();
	await sublevel.crashSafeDurability.barrier();

	process.stdout.write('{"event":"ack"}\n');
}
setInterval(() => undefined, 1_000);
