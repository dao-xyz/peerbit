import { serialize } from "@dao-xyz/borsh";
import { Log } from "@peerbit/log";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { join } from "node:path";
import { Peerbit } from "peerbit";

// Support the source and copied dist/test/utils layouts without a TS loader.
let runtime = new URL("../../src/checkpoint-documents.js", import.meta.url);
try {
	await fs.access(runtime);
} catch {
	runtime = new URL("../../dist/src/checkpoint-documents.js", import.meta.url);
}
const { CheckpointDocuments } = await import(runtime.href);
const [mode, directory, argument] = process.argv.slice(2);
if (!directory || !argument || (mode !== "write" && mode !== "read")) {
	throw new Error(
		"Expected write/read, a peer directory and phase/recovery anchors",
	);
}
const local = { local: true, remote: false };
const hex = (bytes) => Buffer.from(bytes).toString("hex");
const send = (message) =>
	new Promise((resolve, reject) => {
		if (!process.send)
			return reject(new Error("Worker requires an IPC channel"));
		process.send(message, (error) => (error ? reject(error) : resolve()));
	});
const snapshot = async (docs) => {
	const rows = await docs.index.search({}, local);
	rows.sort((a, b) => (a.id < b.id ? -1 : a.id === b.id ? 0 : 1));
	return rows.map((row) => ({
		id: row.id,
		value: hex(row.value),
		context: hex(serialize(row.__context)),
		head: row.__context.head,
	}));
};
const assertFrozen = (docs) => {
	assert.equal(docs.status, "frozen");
	assert.throws(() => docs.index, /not ready/);
	assert.throws(
		() => docs.put({ id: "forbidden", name: "while-frozen" }),
		/not ready/,
	);
	// Observation only: no runtime fault hooks or direct lower-layer mutations.
	assert.equal(docs.shared.closed, true);
	assert.equal(docs.projection.closed, true);
};

const copyBlocks = async (source, destination) => {
	for await (const [, bytes] of source.services.blocks.iterator()) {
		await destination.services.blocks.put(bytes);
	}
	await destination.services.blocks.crashSafeDurability.barrier();
};

const writePartialMerge = async (peer, other) => {
	const resource = new CheckpointDocuments({
		owner: peer.identity.publicKey,
		writers: [peer.identity.publicKey, other.identity.publicKey],
	});
	const genesis = await resource.createGenesis(
		peer.services.blocks,
		peer.identity,
	);
	const docs = await peer.open(resource, { args: { checkpoint: genesis } });
	await copyBlocks(peer, other);
	const second = await other.open(docs.address, {
		args: { checkpoint: genesis },
	});
	for (const [view, prefix] of [
		[docs, "owner"],
		[second, "other"],
	]) {
		await view.put({ id: `${prefix}-a`, name: "first incarnation" });
		await view.put({ id: `${prefix}-a`, name: "causal successor" });
		if (prefix === "owner")
			await view.put({ id: `${prefix}-b`, name: "separate key" });
	}
	const expected = [
		...(await snapshot(docs)),
		...(await snapshot(second)),
	].sort((a, b) => (a.id < b.id ? -1 : 1));
	const freezes = [
		await docs.freezeCheckpoint(),
		await second.freezeCheckpoint(),
	];
	assertFrozen(docs);
	assertFrozen(second);
	await copyBlocks(peer, other);
	await copyBlocks(other, peer);
	const logId = hex(docs.shared.log.id);
	const initialCount = docs.facts.size;
	const originalJoin = Log.prototype.join;
	let committedEntries = 0;
	Log.prototype.join = async function (...args) {
		const result = await originalJoin.apply(this, args);
		if (hex(this.id) === logId && ++committedEntries === 1) {
			assert.equal(docs.facts.size, initialCount + 1);
			assert.equal(await this.has(args[0][0].hash), true);
			const committed = docs.facts.get(args[0][0].hash);
			assert.equal(committed.operation.key, "other-a");
			assert.equal(committed.document.decode().name, "first incarnation");
			assert.equal(docs.status, "frozen");
			await send({
				event: "paused",
				phase: "merge-committed",
				address: docs.address,
				committedEntries,
				expected: { owner: expected, other: expected },
				currentCheckpoint: docs.currentCheckpoint,
				connections:
					peer.libp2p.getConnections().length +
					other.libp2p.getConnections().length,
			});
			// The first lower join has crossed the runtime's receive durability
			// barriers; the enclosing preparation is still awaiting this hook.
			await new Promise(() => setInterval(() => undefined, 1_000));
		}
		return result;
	};
	try {
		await docs.prepareCheckpoint(freezes);
		throw new Error("Expected reconciliation to pause after a committed entry");
	} finally {
		Log.prototype.join = originalJoin;
	}
};

const readPartialMerge = async (peer, other, address) => {
	let docs = await peer.open(address);
	let second = await other.open(address);
	assertFrozen(docs);
	assertFrozen(second);
	// Every authority anchor comes from these independently reopened stores.
	// No expected row, entry, freeze CID or proposal was passed by the parent.
	const freezes = [
		await docs.freezeCheckpoint(),
		await second.freezeCheckpoint(),
	];
	const proposal = await docs.prepareCheckpoint(freezes);
	await copyBlocks(peer, other);
	const approvals = [
		await docs.approveCheckpoint(proposal),
		await second.approveCheckpoint(proposal),
	];
	const checkpoint = await docs.publishCheckpoint(proposal, approvals);
	await copyBlocks(peer, other);
	docs = await peer.open(address, { args: { checkpoint } });
	second = await other.open(address, { args: { checkpoint } });
	assert.equal(docs.currentCheckpoint, checkpoint);
	assert.equal(second.currentCheckpoint, checkpoint);
	await send({
		event: "recovered",
		restored: { owner: await snapshot(docs), other: await snapshot(second) },
		recoveredFrozen: true,
		currentCheckpoint: checkpoint,
		connections:
			peer.libp2p.getConnections().length +
			other.libp2p.getConnections().length,
	});
};

const peer = await Peerbit.create({ directory });
const otherDirectory = join(directory, "other-writer");
const multiwriter =
	mode === "write"
		? argument === "merge-committed"
		: await fs.stat(otherDirectory).then(
				(stat) => stat.isDirectory(),
				(error) => {
					if (error.code !== "ENOENT") throw error;
					return false;
				},
			);
let other;
try {
	assert.equal(peer.libp2p.getConnections().length, 0);
	if (multiwriter) {
		other = await Peerbit.create({ directory: otherDirectory });
		assert.equal(other.libp2p.getConnections().length, 0);
		if (mode === "write") {
			await writePartialMerge(peer, other);
		} else {
			const anchors = JSON.parse(argument);
			assert.deepEqual(Object.keys(anchors), ["address"]);
			assert.equal(typeof anchors.address, "string");
			await readPartialMerge(peer, other, anchors.address);
		}
	} else if (mode === "write") {
		const phase = argument;
		assert(
			["put-committed", "prepared", "published", "successor-put"].includes(
				phase,
			),
		);
		const resource = new CheckpointDocuments({
			owner: peer.identity.publicKey,
		});
		const genesis = await resource.createGenesis(
			peer.services.blocks,
			peer.identity,
		);
		let docs = await peer.open(resource, { args: { checkpoint: genesis } });
		const address = docs.address;
		await docs.put({ id: "survivor", name: "committed", nested: { n: 7 } });
		await docs.put({ id: "deleted", name: "must-not-return" });
		await docs.del("deleted");
		let expected = await snapshot(docs);
		let proposal;
		let checkpoint;
		if (phase !== "put-committed") {
			proposal = await docs.prepareCheckpoint();
			assertFrozen(docs);
			if (phase !== "prepared") {
				const approval = await docs.approveCheckpoint(proposal);
				checkpoint = await docs.publishCheckpoint(proposal, [approval]);
				if (phase === "successor-put") {
					docs = await peer.open(address, { args: { checkpoint } });
					await docs.put({
						id: "survivor",
						name: "successor",
						nested: { n: 8 },
					});
					await docs.put({ id: "after-checkpoint", name: "also-durable" });
					expected = await snapshot(docs);
				}
			}
		}
		await send({
			event: "paused",
			phase,
			address,
			proposal,
			checkpoint,
			expected,
			currentCheckpoint: docs.currentCheckpoint,
			connections: peer.libp2p.getConnections().length,
		});
		// Stay at the acknowledged durability boundary until the parent's SIGKILL.
		// There is no graceful peer.stop, test teardown, or successful forced exit.
		await new Promise(() => setInterval(() => undefined, 1_000));
	} else {
		const anchors = JSON.parse(argument);
		assert.equal(typeof anchors.address, "string");
		assert(
			Object.keys(anchors).every((key) =>
				["address", "proposal", "checkpoint"].includes(key),
			),
		);
		let docs = await peer.open(anchors.address, {
			args: anchors.checkpoint ? { checkpoint: anchors.checkpoint } : {},
		});
		let recoveredFrozen = false;
		if (anchors.proposal) {
			assertFrozen(docs);
			recoveredFrozen = true;
			const approval = await docs.approveCheckpoint(anchors.proposal);
			const checkpoint = await docs.publishCheckpoint(anchors.proposal, [
				approval,
			]);
			docs = await peer.open(anchors.address, { args: { checkpoint } });
			assert.equal(docs.currentCheckpoint, checkpoint);
		}
		assert.equal(docs.status, "ready");
		const restored = await snapshot(docs);
		assert.equal(await docs.index.get("deleted", local), undefined);
		await docs.put({ id: "recovery-probe", name: "writes-resumed" });
		assert.equal(
			(await docs.index.get("recovery-probe", local)).decode().name,
			"writes-resumed",
		);
		await docs.del("recovery-probe");
		assert.equal(await docs.index.get("recovery-probe", local), undefined);
		await send({
			event: "recovered",
			restored,
			recoveredFrozen,
			currentCheckpoint: docs.currentCheckpoint,
			connections: peer.libp2p.getConnections().length,
		});
	}
} finally {
	try {
		await other?.stop();
	} finally {
		await peer.stop();
	}
	process.disconnect?.();
}
