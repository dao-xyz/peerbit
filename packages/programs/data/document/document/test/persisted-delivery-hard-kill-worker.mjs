import { toId } from "@peerbit/indexer-interface";
import { Peerbit } from "peerbit";

// Keep package build output out of TypeScript's input graph while retaining a
// plain Node worker that executes the exact compiled artifacts under test.
const distModule = (relativePath) =>
	new URL(`../dist/${relativePath}`, import.meta.url).href;
const { policy, transform } = await import(distModule("src/index.js"));
const { Documents } = await import(distModule("src/program.js"));
const { Document, TestStore } = await import(distModule("test/data.js"));

const [mode, directory, expectedHash] = process.argv.slice(2);
if (!mode || !directory) {
	throw new Error("Expected mode and receiver directory");
}

const storeId = new Uint8Array(32);
for (let index = 0; index < storeId.length; index++) {
	storeId[index] = (index * 17 + 11) & 0xff;
}

const createStore = () => {
	const store = new TestStore({ docs: new Documents({ id: storeId }) });
	store.id = storeId;
	return store;
};

const openArgs = (replicate) => ({
	replicas: { min: 1 },
	replicate,
	timeUntilRoleMaturity: 0,
	canPerform: policy.allowAll(),
	index: {
		type: Document,
		transform: transform.identity(),
	},
});

const waitFor = async (predicate, description, timeoutMs = 15_000) => {
	const deadline = Date.now() + timeoutMs;
	let lastError;
	while (Date.now() < deadline) {
		try {
			if (await predicate()) return;
		} catch (error) {
			lastError = error;
		}
		await new Promise((resolve) => setTimeout(resolve, 25));
	}
	throw new Error(
		`Timed out waiting for ${description}${lastError ? `: ${String(lastError)}` : ""}`,
	);
};

if (mode === "write") {
	const writer = await Peerbit.create();
	const receiver = await Peerbit.create({ directory });
	await writer.dial(receiver);

	const writerStore = createStore();
	const receiverStore = createStore();
	await writer.open(writerStore, { args: openArgs(false) });
	await receiver.open(receiverStore, {
		args: openArgs({ offset: 0, factor: 1 }),
	});

	await writerStore.docs.log.waitForReplicator(receiver.identity.publicKey, {
		roleAge: 0,
		timeout: 15_000,
	});
	const receiverHash = receiver.identity.publicKey.hashcode();
	await waitFor(
		() =>
			((writerStore.docs.log._peerSyncCapabilities.get(receiverHash) ?? 0) &
				(1 << 5)) !==
			0,
		"the receiver's persisted-receipt capability",
	);

	const result = await writerStore.docs.put(
		new Document({
			id: "persisted-delivery-hard-kill",
			name: "durable remote receipt",
		}),
		{
			unique: true,
			delivery: {
				reliability: "persisted",
				minAcks: 1,
				timeout: 15_000,
			},
		},
	);
	process.stdout.write(
		`${JSON.stringify({ event: "receipt", hash: result.entry.hash })}\n`,
	);
	// The parent deliberately SIGKILLs this process immediately after receipt.
	setInterval(() => {}, 60_000);
} else if (mode === "read") {
	if (!expectedHash) {
		throw new Error("Expected the acknowledged entry hash");
	}
	const receiver = await Peerbit.create({ directory });
	const receiverStore = createStore();
	await receiver.open(receiverStore, {
		args: openArgs({ offset: 0, factor: 1 }),
	});
	const sharedLog = receiverStore.docs.log;
	const document = await receiverStore.docs.get(
		"persisted-delivery-hard-kill",
		{ local: true, remote: false },
	);
	const [block, lowerIndexed, coordinate, heads] = await Promise.all([
		sharedLog.log.blocks.get(expectedHash),
		sharedLog.log.entryIndex.has(expectedHash),
		sharedLog.entryCoordinatesIndex.get(toId(expectedHash)),
		sharedLog.log.getHeads().all(),
	]);
	process.stdout.write(
		`${JSON.stringify({
			event: "reopened",
			documentName: document?.name,
			blockPresent: block != null,
			lowerIndexed,
			coordinatePresent: coordinate != null,
			headHashes: heads.map((entry) => entry.hash),
		})}\n`,
	);
	await receiver.stop();
} else {
	throw new Error(`Unknown worker mode: ${mode}`);
}
