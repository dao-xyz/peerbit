import { NativeBackboneNodeCoordinatePersistence } from "@peerbit/native-backbone";
import path from "node:path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";

// Keep package build output out of TypeScript's input graph while retaining a
// plain Node worker that executes the exact compiled artifacts under test.
const distModule = (relativePath) =>
	new URL(`../dist/${relativePath}`, import.meta.url).href;
const { policy, transform } = await import(distModule("src/index.js"));
const { Documents } = await import(distModule("src/program.js"));
const { Document, TestStore } = await import(distModule("test/data.js"));

const [mode, directory, expectedHash, expectedOtherHash] =
	process.argv.slice(2);
if (!mode || !directory) {
	throw new Error("Expected mode and directory");
}

const storeId = new Uint8Array(32);
for (let index = 0; index < storeId.length; index++) {
	storeId[index] = (index * 13 + 7) & 0xff;
}

const client = await Peerbit.create({
	directory,
	...createRustPeerbitOptions(),
});
const store = new TestStore({ docs: new Documents({ id: storeId }) });
store.id = storeId;
await client.open(store, {
	args: {
		mode: "native",
		replicate: false,
		nativeGraph: true,
		nativeBackbone: {
			optional: false,
			documentIndex: true,
			coordinatePersistence: new NativeBackboneNodeCoordinatePersistence(
				path.join(directory, "coordinate-wal"),
				{ flushOnAppend: true },
			),
		},
		canPerform: policy.allowAll(),
		index: { type: Document, transform: transform.identity() },
		...(mode.startsWith("trim-")
			? { log: { trim: { type: "length", to: 1 } } }
			: {}),
	},
});

if (mode === "strict-generic-torn-marker-write") {
	const sharedLog = store.docs.log;
	const entryIndex = sharedLog.log.entryIndex;
	const intentStore = sharedLog._nativeBackboneCoordinatePersistenceStore;
	const originalIntentWrite = intentStore.write.bind(intentStore);
	const originalIndexPut = entryIndex.put.bind(entryIndex);
	let genericHash;
	let genericJoin;
	let genericJoinEnteredResolve;
	const genericJoinEntered = new Promise((resolve) => {
		genericJoinEnteredResolve = resolve;
	});
	entryIndex.put = async (...args) => {
		if (args[0]?.hash === genericHash) genericJoinEnteredResolve();
		return originalIndexPut(...args);
	};

	let sourceClient;
	let tornMarker = false;
	let strictHash;
	intentStore.write = async (name, bytes) => {
		let record;
		try {
			record = JSON.parse(new TextDecoder().decode(bytes));
		} catch {}
		if (
			!tornMarker &&
			record?.format === "peerbit-native-strict-durable-transaction" &&
			record.intent?.lowerMarkerCommitted === true
		) {
			strictHash = record.intent.appendHashes[0];
			const strictEntry = await sharedLog.log.get(strictHash);
			const strictBytes = await sharedLog.log.blocks.get(strictHash);
			if (!strictEntry || !strictBytes) {
				throw new Error(
					"Expected the strict append to be readable at its marker",
				);
			}

			sourceClient = await Peerbit.create(createRustPeerbitOptions());
			const sourceStore = new TestStore({
				docs: new Documents({ id: storeId }),
			});
			sourceStore.id = storeId;
			await sourceClient.open(sourceStore, {
				args: {
					replicate: false,
					canPerform: policy.allowAll(),
					index: { type: Document, transform: transform.identity() },
				},
			});
			await sourceStore.docs.log.log.blocks.putKnown(strictHash, strictBytes);
			await sourceStore.docs.log.log.join([strictEntry]);
			const generic = await sourceStore.docs.put(
				new Document({ id: "strict-generic-y", name: "generic" }),
				{
					replicate: false,
					target: "none",
					meta: { next: [strictEntry] },
				},
			);
			genericHash = generic.entry.hash;
			if (!generic.entry.meta.next.includes(strictHash)) {
				throw new Error(
					"Expected the generic append to extend the strict append",
				);
			}
			const genericBytes =
				await sourceStore.docs.log.log.blocks.get(genericHash);
			if (!genericBytes) throw new Error("Expected generic append bytes");
			await sharedLog.log.blocks.putKnown(genericHash, genericBytes);

			genericJoin = sharedLog.log.join([generic.entry]);
			await genericJoinEntered;
			const joinedWhileStrictLeaseHeld = await Promise.race([
				genericJoin.then(() => true),
				new Promise((resolve) => setTimeout(() => resolve(false), 25)),
			]);
			if (joinedWhileStrictLeaseHeld) {
				throw new Error(
					"Generic receive bypassed the strict native hash mutation lease",
				);
			}

			// Persist only a torn true marker. The following clear is deliberately
			// acknowledged without touching disk, leaving the preceding complete false
			// intent as the sole valid generation for fresh-process recovery.
			await originalIntentWrite(
				name,
				bytes.subarray(0, Math.max(1, Math.floor(bytes.byteLength / 2))),
			);
			tornMarker = true;
			return;
		}
		if (tornMarker && record?.state === "cleared") return;
		return originalIntentWrite(name, bytes);
	};

	const strict = await store.docs.put(
		new Document({ id: "strict-generic-x", name: "strict" }),
		{ replicate: false, target: "none" },
	);
	if (strict.entry.hash !== strictHash || !genericHash || !genericJoin) {
		throw new Error("Strict/generic torn-marker race did not reach its marker");
	}
	await genericJoin;
	entryIndex.put = originalIndexPut;
	await sourceClient?.stop();
	const strictShallow = await entryIndex.getShallow(strictHash);
	const genericShallow = await entryIndex.getShallow(genericHash);
	const heads = await sharedLog.log.getHeads().all();
	process.stdout.write(
		`${JSON.stringify({
			event: "strict-generic-torn-marker",
			strictHash,
			genericHash,
			strictIndexed: await entryIndex.has(strictHash),
			genericIndexed: await entryIndex.has(genericHash),
			strictHead: strictShallow?.value.head,
			genericHead: genericShallow?.value.head,
			headHashes: heads.map((entry) => entry.hash),
		})}\n`,
	);
	setInterval(() => {}, 60_000);
} else if (mode === "strict-generic-torn-marker-read") {
	const sharedLog = store.docs.log;
	const strictShallow = expectedHash
		? await sharedLog.log.entryIndex.getShallow(expectedHash)
		: undefined;
	const genericShallow = expectedOtherHash
		? await sharedLog.log.entryIndex.getShallow(expectedOtherHash)
		: undefined;
	const heads = await sharedLog.log.getHeads().all();
	process.stdout.write(
		`${JSON.stringify({
			event: "strict-generic-torn-marker-read",
			lowerLogLength: sharedLog.log.length,
			strictIndexed: expectedHash
				? await sharedLog.log.entryIndex.has(expectedHash)
				: false,
			genericIndexed: expectedOtherHash
				? await sharedLog.log.entryIndex.has(expectedOtherHash)
				: false,
			strictHead: strictShallow?.value.head,
			genericHead: genericShallow?.value.head,
			headHashes: heads.map((entry) => entry.hash),
		})}\n`,
	);
	await client.stop();
} else if (mode === "intent-marker-write") {
	const sharedLog = store.docs.log;
	const intentStore = sharedLog._nativeBackboneCoordinatePersistenceStore;
	const originalWrite = intentStore.write.bind(intentStore);
	intentStore.write = async (name, bytes) => {
		let record;
		try {
			record = JSON.parse(new TextDecoder().decode(bytes));
		} catch {}
		if (
			record?.format === "peerbit-native-strict-durable-transaction" &&
			record.intent?.lowerMarkerCommitted === true
		) {
			// Simulate a process dying after truncate/partial write. The alternating
			// journal must retain the preceding complete generation, whose exact lower
			// row is sufficient to infer this committed marker.
			await originalWrite(
				name,
				bytes.subarray(0, Math.max(1, Math.floor(bytes.byteLength / 2))),
			);
			process.stdout.write(
				`${JSON.stringify({
					event: "intent-marker-partial",
					hash: record.intent.appendHashes[0],
				})}\n`,
			);
			await new Promise(() => {});
		}
		return originalWrite(name, bytes);
	};
	await store.docs.put(new Document({ id: "hard-kill", name: "hard-kill" }));
} else if (mode === "write") {
	const result = await store.docs.put(
		new Document({ id: "hard-kill", name: "hard-kill" }),
	);
	process.stdout.write(
		`${JSON.stringify({ event: "ack", hash: result.entry.hash })}\n`,
	);
	// The parent deliberately SIGKILLs this process after observing the ack.
	setInterval(() => {}, 60_000);
} else if (mode === "premarker-write") {
	const sharedLog = store.docs.log;
	const lowerIndex = sharedLog.log.entryIndex.properties.index;
	lowerIndex.put = async (value) => {
		process.stdout.write(
			`${JSON.stringify({ event: "premarker", hash: value.hash })}\n`,
		);
		await new Promise(() => {});
	};
	await store.docs.put(
		new Document({ id: "premarker-hard-kill", name: "must-rollback" }),
	);
} else if (mode === "pending-next-premarker-write") {
	const first = await store.docs.put(
		new Document({ id: "pending-next-premarker", name: "old" }),
		{ replicate: false, target: "none" },
	);
	const sharedLog = store.docs.log;
	const entryIndex = sharedLog.log.entryIndex;
	const lowerIndex = entryIndex.properties.index;
	const oldShallow = (await entryIndex.getShallow(first.entry.hash))?.value;
	if (!oldShallow) throw new Error("Expected acknowledged old shallow row");
	if (!lowerIndex.delIds)
		throw new Error("Expected exact lower-index deletion");
	await lowerIndex.delIds([first.entry.hash]);
	// Recreate the normal deferred-publication state: the acknowledged old head is
	// logically visible to EntryIndex but absent from the durable index. The next
	// append consumes and demotes this pending row before its own marker is written.
	entryIndex.pendingIndexWrites.set(first.entry.hash, oldShallow);
	entryIndex.pendingIndexWriteGenerations.set(first.entry.hash, 1_000_000);
	const originalPut = lowerIndex.put.bind(lowerIndex);
	lowerIndex.put = async (value) => {
		await originalPut(value);
		if (value.hash !== first.entry.hash || value.head !== false) return;
		process.stdout.write(
			`${JSON.stringify({
				event: "pending-next-premarker",
				firstHash: first.entry.hash,
			})}\n`,
		);
		await new Promise(() => {});
	};
	await store.docs.put(
		new Document({ id: "pending-next-premarker", name: "replacement" }),
		{ replicate: false, target: "none" },
	);
} else if (mode === "pending-next-premarker-read") {
	const sharedLog = store.docs.log;
	const document = await store.docs.get("pending-next-premarker");
	const oldShallow = expectedHash
		? await sharedLog.log.entryIndex.getShallow(expectedHash)
		: undefined;
	process.stdout.write(
		`${JSON.stringify({
			event: "pending-next-premarker-read",
			documentName: document?.name,
			lowerLogLength: sharedLog.log.length,
			oldIndexed: expectedHash
				? await sharedLog.log.entryIndex.has(expectedHash)
				: false,
			oldHead: oldShallow?.value.head,
		})}\n`,
	);
	await client.stop();
} else if (mode === "premarker-read") {
	const sharedLog = store.docs.log;
	const document = await store.docs.get("premarker-hard-kill");
	process.stdout.write(
		`${JSON.stringify({
			event: "premarker-read",
			documentVisible: !!document,
			lowerLogLength: sharedLog.log.length,
			coordinateVisible: expectedHash
				? sharedLog._residentEntryCoordinatesByHash?.has(expectedHash) === true
				: false,
		})}\n`,
	);
	await client.stop();
} else if (mode === "read") {
	const document = await store.docs.get("hard-kill");
	const block = expectedHash
		? await store.docs.log.log.blocks.get(expectedHash)
		: undefined;
	process.stdout.write(
		`${JSON.stringify({
			event: "read",
			documentName: document?.name,
			entryHash: block ? expectedHash : undefined,
		})}\n`,
	);
	await client.stop();
} else if (mode === "trim-failure-write") {
	const first = await store.docs.put(
		new Document({ id: "trim-hard-kill", name: "acknowledged" }),
		{ unique: true },
	);
	const sharedLog = store.docs.log;
	const durable = sharedLog.remoteBlocks.localStore.durable;
	durable.rmMany = async () => {
		throw new Error("injected durable trim cleanup failure");
	};
	const replacement = await store.docs.put(
		new Document({ id: "trim-hard-kill", name: "replacement" }),
		{ unique: true },
	);
	process.stdout.write(
		`${JSON.stringify({
			event: "trim-ack",
			firstHash: first.entry.hash,
			replacementHash: replacement.entry.hash,
			cleanupDebt:
				sharedLog.remoteBlocks.localStore.pendingNativeDeleteCleanup.size,
		})}\n`,
	);
	setInterval(() => {}, 60_000);
} else if (mode === "trim-marker-write") {
	const first = await store.docs.put(
		new Document({ id: "trim-marker-hard-kill", name: "old" }),
		{ unique: true },
	);
	const sharedLog = store.docs.log;
	const lowerIndex = sharedLog.log.entryIndex.properties.index;
	const originalPut = lowerIndex.put.bind(lowerIndex);
	let replacementHash;
	lowerIndex.put = async (value) => {
		await originalPut(value);
		if (value.hash !== first.entry.hash) replacementHash = value.hash;
	};
	const originalDelIds = lowerIndex.delIds?.bind(lowerIndex);
	if (!originalDelIds) throw new Error("Expected exact lower-index deletion");
	lowerIndex.delIds = async (hashes) => {
		process.stdout.write(
			`${JSON.stringify({
				event: "trim-marker",
				firstHash: first.entry.hash,
				replacementHash,
				trimHashes: hashes,
			})}\n`,
		);
		await new Promise(() => {});
	};
	await store.docs.put(
		new Document({ id: "trim-marker-hard-kill", name: "replacement" }),
		{ unique: true },
	);
} else if (mode === "trim-marker-read") {
	const sharedLog = store.docs.log;
	const document = await store.docs.get("trim-marker-hard-kill");
	const heads = await sharedLog.log.getHeads().all();
	const oldBlock = expectedOtherHash
		? await sharedLog.log.blocks.get(expectedOtherHash)
		: undefined;
	process.stdout.write(
		`${JSON.stringify({
			event: "trim-marker-read",
			documentName: document?.name,
			lowerLogLength: sharedLog.log.length,
			headHashes: heads.map((entry) => entry.hash),
			replacementIndexed: expectedHash
				? await sharedLog.log.entryIndex.has(expectedHash)
				: false,
			oldBlockPresent: !!oldBlock,
		})}\n`,
	);
	await client.stop();
} else if (mode === "next-marker-write") {
	const first = await store.docs.put(
		new Document({ id: "next-marker-hard-kill", name: "old" }),
		{ replicate: false, target: "none" },
	);
	const sharedLog = store.docs.log;
	const lowerIndex = sharedLog.log.entryIndex.properties.index;
	const originalPut = lowerIndex.put.bind(lowerIndex);
	lowerIndex.put = async (value) => {
		await originalPut(value);
		if (value.hash === first.entry.hash) return;
		process.stdout.write(
			`${JSON.stringify({
				event: "next-marker",
				firstHash: first.entry.hash,
				replacementHash: value.hash,
				deleteHashes: [first.entry.hash],
			})}\n`,
		);
		await new Promise(() => {});
	};
	await store.docs.put(
		new Document({ id: "next-marker-hard-kill", name: "replacement" }),
		{ replicate: false, target: "none" },
	);
} else if (mode === "next-marker-read") {
	const sharedLog = store.docs.log;
	const document = await store.docs.get("next-marker-hard-kill");
	const heads = await sharedLog.log.getHeads().all();
	process.stdout.write(
		`${JSON.stringify({
			event: "next-marker-read",
			documentName: document?.name,
			lowerLogLength: sharedLog.log.length,
			headHashes: heads.map((entry) => entry.hash),
			oldCoordinateVisible: expectedHash
				? sharedLog._residentEntryCoordinatesByHash?.has(expectedHash) === true
				: false,
			replacementCoordinateVisible: expectedOtherHash
				? sharedLog._residentEntryCoordinatesByHash?.has(expectedOtherHash) ===
					true
				: false,
		})}\n`,
	);
	await client.stop();
} else if (mode === "trim-failure-read") {
	const block = expectedHash
		? await store.docs.log.log.blocks.get(expectedHash)
		: undefined;
	const document = await store.docs.get("trim-hard-kill");
	const heads = await store.docs.log.log.getHeads().all();
	const oldBlock = expectedOtherHash
		? await store.docs.log.log.blocks.get(expectedOtherHash)
		: undefined;
	process.stdout.write(
		`${JSON.stringify({
			event: "trim-read",
			replacementBlockPresent: !!block,
			documentName: document?.name,
			lowerLogLength: store.docs.log.log.length,
			headHashes: heads.map((entry) => entry.hash),
			oldBlockPresent: !!oldBlock,
		})}\n`,
	);
	await client.stop();
} else if (
	mode === "mirror-failure-write" ||
	mode === "mirror-failure-graceful-write"
) {
	const sharedLog = store.docs.log;
	const durable = sharedLog.remoteBlocks.localStore.durable;
	durable.putKnown = async () => {
		throw new Error("injected durable mirror failure");
	};
	let rejection;
	try {
		await store.docs.put(
			new Document({ id: "mirror-failed", name: "must-not-survive" }),
		);
	} catch (error) {
		rejection = error;
	}
	const failedHash = rejection?.committedCids?.[0];
	process.stdout.write(
		`${JSON.stringify({
			event: "mirror-rejected",
			failedHash,
			errorName: rejection?.name,
			documentVisible: !!(await store.docs.get("mirror-failed")),
			lowerLogLength: sharedLog.log.length,
			coordinateVisible: failedHash
				? sharedLog._residentEntryCoordinatesByHash?.has(failedHash) === true
				: false,
		})}\n`,
	);
	if (mode === "mirror-failure-graceful-write") {
		try {
			await client.stop();
		} catch (error) {
			if (error !== rejection) throw error;
		}
		process.exit(0);
	} else {
		setInterval(() => {}, 60_000);
	}
} else if (mode === "mirror-failure-read") {
	const sharedLog = store.docs.log;
	const document = await store.docs.get("mirror-failed");
	const block = expectedHash
		? await sharedLog.log.blocks.get(expectedHash)
		: undefined;
	process.stdout.write(
		`${JSON.stringify({
			event: "mirror-read",
			documentVisible: !!document,
			blockVisible: !!block,
			lowerLogLength: sharedLog.log.length,
			coordinateVisible: expectedHash
				? sharedLog._residentEntryCoordinatesByHash?.has(expectedHash) === true
				: false,
		})}\n`,
	);
	await client.stop();
} else {
	throw new Error(`Unknown mode: ${mode}`);
}
