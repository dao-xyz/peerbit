import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { EntryType } from "../src/entry-type.js";
import { type Entry } from "../src/entry.js";
import { Log } from "../src/log.js";

type BenchRow = {
	name: string;
	nativeGraph: boolean;
	entries: number;
	iterations: number;
	elapsedMs: number;
	opsPerSecond: number;
};

const entries = Number(process.env.PEERBIT_LOG_NATIVE_GRAPH_ENTRIES ?? 1_000);
const iterations = Number(
	process.env.PEERBIT_LOG_NATIVE_GRAPH_ITERATIONS ?? 1_000,
);
const appendEntries = Number(
	process.env.PEERBIT_LOG_NATIVE_GRAPH_APPEND_ENTRIES ?? 1_000,
);
const joinParents = Number(
	process.env.PEERBIT_LOG_NATIVE_GRAPH_JOIN_PARENTS ?? entries,
);

const key = await Ed25519Keypair.create();

const absoluteReplicaData = (value: number) =>
	new Uint8Array([
		0,
		value & 0xff,
		(value >>> 8) & 0xff,
		(value >>> 16) & 0xff,
		(value >>> 24) & 0xff,
	]);

const decodeAbsoluteReplicaData = (data?: Uint8Array) => {
	if (!data || data.length !== 5 || data[0] !== 0) {
		return undefined;
	}
	return (
		(data[1]! | (data[2]! << 8) | (data[3]! << 16) | (data[4]! << 24)) >>> 0
	);
};

const createHeadsLog = async (nativeGraph: boolean) => {
	const store = new AnyBlockStore();
	await store.start();
	const log = new Log<Uint8Array>();
	await log.open(store, key, {
		appendDurability: "strict",
		indexer: new HashmapIndices(),
		nativeGraph,
	});
	let gid: string | undefined;
	const hashes: string[] = [];
	for (let i = 0; i < entries; i++) {
		const { entry } = await log.append(new Uint8Array([i & 0xff]), {
			meta: { next: [] },
		});
		gid ??= entry.meta.gid;
		hashes.push(entry.hash);
	}
	return { log, store, gid: gid!, hashes };
};

const createReplicaHeadsLog = async (nativeGraph: boolean) => {
	const store = new AnyBlockStore();
	await store.start();
	const log = new Log<Uint8Array>();
	await log.open(store, key, {
		appendDurability: "strict",
		indexer: new HashmapIndices(),
		nativeGraph,
	});
	for (let i = 0; i < entries; i++) {
		await log.append(new Uint8Array([i & 0xff]), {
			meta: { next: [], data: absoluteReplicaData((i % 8) + 1) },
		});
	}
	return { log, store };
};

const createChainLog = async (nativeGraph: boolean) => {
	const store = new AnyBlockStore();
	await store.start();
	const log = new Log<Uint8Array>();
	await log.open(store, key, {
		appendDurability: "strict",
		indexer: new HashmapIndices(),
		nativeGraph,
	});
	const root = (await log.append(new Uint8Array([0]), { meta: { next: [] } }))
		.entry;
	let previous = root;
	for (let i = 1; i < entries; i++) {
		previous = (
			await log.append(new Uint8Array([i & 0xff]), {
				meta: { next: [previous] },
			})
		).entry;
	}
	return { log, rootHash: root.hash, store };
};

const measure = async (
	name: string,
	nativeGraph: boolean,
	fn: () => Promise<void>,
	measureIterations = iterations,
): Promise<BenchRow> => {
	const started = performance.now();
	for (let i = 0; i < measureIterations; i++) {
		await fn();
	}
	const elapsed = performance.now() - started;
	return {
		name,
		nativeGraph,
		entries,
		iterations: measureIterations,
		elapsedMs: Math.round(elapsed),
		opsPerSecond: Math.round((measureIterations / elapsed) * 1000),
	};
};

const rows: BenchRow[] = [];

const hasHead = async (log: Log<Uint8Array>) => {
	const nativeHasHead = await log.entryIndex.hasHead();
	if (nativeHasHead != null) {
		return nativeHasHead;
	}
	const heads = await log.entryIndex
		.getHeads(undefined, { type: "shape", shape: { hash: true } })
		.all();
	return heads.length > 0;
};

const hasAnyHead = async (log: Log<Uint8Array>, gids: string[]) => {
	const nativeHasHead = await log.entryIndex.hasAnyHead(gids);
	if (nativeHasHead != null) {
		return nativeHasHead;
	}
	for (const gid of gids) {
		const heads = await log.entryIndex
			.getHeads(gid, { type: "shape", shape: { hash: true } })
			.all();
		if (heads.length > 0) {
			return true;
		}
	}
	return false;
};

const hasAnyHeadBatch = async (log: Log<Uint8Array>, gidSets: string[][]) => {
	const nativeHasHeads = await log.entryIndex.hasAnyHeadBatch(gidSets);
	if (nativeHasHeads != null) {
		return nativeHasHeads;
	}
	const out: boolean[] = [];
	for (const gids of gidSets) {
		out.push(await hasAnyHead(log, gids));
	}
	return out;
};

const getMaxHeadDataU32 = async (log: Log<Uint8Array>) => {
	const nativeMax = await log.entryIndex.getMaxHeadDataU32();
	if (nativeMax != null) {
		return nativeMax;
	}
	const heads = (await log.entryIndex
		.getHeads(undefined, {
			type: "shape",
			shape: { meta: { data: true } },
		})
		.all()) as { meta: { data?: Uint8Array } }[];
	let max = 0;
	for (const head of heads) {
		const value = decodeAbsoluteReplicaData(head.meta.data);
		if (value != null) {
			max = Math.max(max, value);
		}
	}
	return max;
};

const measureAppend = async (
	name: string,
	nativeGraph: boolean,
	fn: (log: Log<Uint8Array>) => Promise<void>,
): Promise<BenchRow> => {
	const store = new AnyBlockStore();
	await store.start();
	const log = new Log<Uint8Array>();
	await log.open(store, key, {
		appendDurability: "strict",
		indexer: new HashmapIndices(),
		nativeGraph,
	});
	await log.append(new Uint8Array([0]), { meta: { next: [] } });
	const started = performance.now();
	await fn(log);
	const elapsed = performance.now() - started;
	await log.close();
	await store.stop();
	return {
		name,
		nativeGraph,
		entries: appendEntries,
		iterations: appendEntries,
		elapsedMs: Math.round(elapsed),
		opsPerSecond: Math.round((appendEntries / elapsed) * 1000),
	};
};

for (const nativeGraph of [false, true]) {
	rows.push(
		await measureAppend("append loop auto-next", nativeGraph, async (log) => {
			for (let i = 0; i < appendEntries; i++) {
				await log.append(new Uint8Array([i & 0xff]));
			}
		}),
	);
}

for (const nativeGraph of [false, true]) {
	rows.push(
		await measureAppend("appendMany auto-next", nativeGraph, async (log) => {
			await log.appendMany(
				Array.from(
					{ length: appendEntries },
					(_, index) => new Uint8Array([index & 0xff]),
				),
			);
		}),
	);
}

const measureWideJoin = async (nativeGraph: boolean): Promise<BenchRow> => {
	const store = new AnyBlockStore();
	await store.start();
	const source = new Log<Uint8Array>();
	const target = new Log<Uint8Array>();
	await source.open(store, key, {
		appendDurability: "strict",
		indexer: new HashmapIndices(),
		nativeGraph,
	});
	await target.open(store, key, {
		appendDurability: "strict",
		indexer: new HashmapIndices(),
		nativeGraph,
	});

	const parents: Entry<Uint8Array>[] = [];
	for (let i = 0; i < joinParents; i++) {
		parents.push(
			(
				await source.append(new Uint8Array([i & 0xff]), {
					meta: { next: [] },
				})
			).entry,
		);
	}

	await target.join(parents.slice(0, Math.floor(joinParents / 2)));
	const { entry: merge } = await source.append(new Uint8Array([0xff]), {
		meta: { next: parents },
	});

	const started = performance.now();
	await target.join([merge]);
	const elapsed = performance.now() - started;
	await source.close();
	await target.close();
	await store.stop();
	return {
		name: "join wide merge parent planning",
		nativeGraph,
		entries: joinParents,
		iterations: joinParents,
		elapsedMs: Math.round(elapsed),
		opsPerSecond: Math.round((joinParents / elapsed) * 1000),
	};
};

for (const nativeGraph of [false, true]) {
	rows.push(await measureWideJoin(nativeGraph));
}

const measureDuplicateJoinArray = async (
	nativeGraph: boolean,
): Promise<BenchRow> => {
	const store = new AnyBlockStore();
	await store.start();
	const source = new Log<Uint8Array>();
	const target = new Log<Uint8Array>();
	await source.open(store, key, {
		appendDurability: "strict",
		indexer: new HashmapIndices(),
		nativeGraph,
	});
	await target.open(store, key, {
		appendDurability: "strict",
		indexer: new HashmapIndices(),
		nativeGraph,
	});

	const existing: Entry<Uint8Array>[] = [];
	for (let i = 0; i < joinParents; i++) {
		existing.push(
			(
				await source.append(new Uint8Array([i & 0xff]), {
					meta: { next: [] },
				})
			).entry,
		);
	}
	await target.join(existing);

	const started = performance.now();
	await target.join(existing);
	const elapsed = performance.now() - started;
	await source.close();
	await target.close();
	await store.stop();
	return {
		name: "join duplicate array membership",
		nativeGraph,
		entries: joinParents,
		iterations: joinParents,
		elapsedMs: Math.round(elapsed),
		opsPerSecond: Math.round((joinParents / elapsed) * 1000),
	};
};

for (const nativeGraph of [false, true]) {
	rows.push(await measureDuplicateJoinArray(nativeGraph));
}

const measureCutRecursiveDelete = async (
	nativeGraph: boolean,
): Promise<BenchRow> => {
	const store = new AnyBlockStore();
	await store.start();
	const log = new Log<Uint8Array>();
	await log.open(store, key, {
		appendDurability: "strict",
		indexer: new HashmapIndices(),
		nativeGraph,
	});

	let previous = (
		await log.append(new Uint8Array([0]), {
			meta: { next: [] },
		})
	).entry;
	for (let i = 1; i < joinParents; i++) {
		previous = (
			await log.append(new Uint8Array([i & 0xff]), {
				meta: { next: [previous] },
			})
		).entry;
	}

	const started = performance.now();
	await log.append(new Uint8Array([0xff]), {
		meta: { type: EntryType.CUT, next: [previous] },
	});
	const elapsed = performance.now() - started;
	await log.close();
	await store.stop();
	return {
		name: "cut recursive delete plan",
		nativeGraph,
		entries: joinParents,
		iterations: joinParents,
		elapsedMs: Math.round(elapsed),
		opsPerSecond: Math.round((joinParents / elapsed) * 1000),
	};
};

for (const nativeGraph of [false, true]) {
	rows.push(await measureCutRecursiveDelete(nativeGraph));
}

const measureCutCoveredJoin = async (
	nativeGraph: boolean,
): Promise<BenchRow> => {
	const sourceStore = new AnyBlockStore();
	const targetStore = new AnyBlockStore();
	await sourceStore.start();
	await targetStore.start();
	const source = new Log<Uint8Array>();
	const target = new Log<Uint8Array>();
	await source.open(sourceStore, key, {
		appendDurability: "strict",
		indexer: new HashmapIndices(),
		nativeGraph,
	});
	await target.open(targetStore, key, {
		appendDurability: "strict",
		indexer: new HashmapIndices(),
		nativeGraph,
	});
	const { entry: old } = await source.append(new Uint8Array([1]), {
		meta: { next: [] },
	});
	await target.append(new Uint8Array([2]), {
		meta: { type: EntryType.CUT, next: [old] },
	});

	const started = performance.now();
	for (let i = 0; i < iterations; i++) {
		await target.join([old]);
	}
	const elapsed = performance.now() - started;
	await source.close();
	await target.close();
	await sourceStore.stop();
	await targetStore.stop();
	return {
		name: "join cut-covered skip",
		nativeGraph,
		entries: 1,
		iterations,
		elapsedMs: Math.round(elapsed),
		opsPerSecond: Math.round((iterations / elapsed) * 1000),
	};
};

for (const nativeGraph of [false, true]) {
	rows.push(await measureCutCoveredJoin(nativeGraph));
}

for (const nativeGraph of [false, true]) {
	const { log, store } = await createHeadsLog(nativeGraph);
	rows.push(
		await measure("getHeads().all()", nativeGraph, async () => {
			await log.getHeads().all();
		}),
	);
	await log.close();
	await store.stop();
}

for (const nativeGraph of [false, true]) {
	const { log, store } = await createHeadsLog(nativeGraph);
	rows.push(
		await measure("getHeads(hash shape).all()", nativeGraph, async () => {
			await log.entryIndex
				.getHeads(undefined, { type: "shape", shape: { hash: true } })
				.all();
		}),
	);
	await log.close();
	await store.stop();
}

for (const nativeGraph of [false, true]) {
	const { log, store, gid, hashes } = await createHeadsLog(nativeGraph);
	rows.push(
		await measure("hasHead()", nativeGraph, async () => {
			await hasHead(log);
		}),
	);
	rows.push(
		await measure(
			"hasMany(incoming heads)",
			nativeGraph,
			async () => {
				await log.hasMany(hashes);
			},
			Math.min(iterations, 100),
		),
	);
	rows.push(
		await measure("hasAnyHead(refs)", nativeGraph, async () => {
			await hasAnyHead(log, ["missing", gid]);
		}),
	);
	rows.push(
		await measure("hasAnyHeadBatch(refs)", nativeGraph, async () => {
			await hasAnyHeadBatch(log, [["missing"], ["missing", gid], [gid]]);
		}),
	);
	await log.close();
	await store.stop();
}

for (const nativeGraph of [false, true]) {
	const { log, store } = await createReplicaHeadsLog(nativeGraph);
	rows.push(
		await measure("getHeads(data shape).all()", nativeGraph, async () => {
			await log.entryIndex
				.getHeads(undefined, {
					type: "shape",
					shape: { hash: true, meta: { data: true } },
				})
				.all();
		}),
	);
	await log.close();
	await store.stop();
}

for (const nativeGraph of [false, true]) {
	const { log, store } = await createReplicaHeadsLog(nativeGraph);
	rows.push(
		await measure("getMaxHeadDataU32()", nativeGraph, async () => {
			await getMaxHeadDataU32(log);
		}),
	);
	await log.close();
	await store.stop();
}

for (const nativeGraph of [false, true]) {
	const { log, rootHash, store } = await createChainLog(nativeGraph);
	rows.push(
		await measure("countHasNext(root)", nativeGraph, async () => {
			await log.entryIndex.countHasNext(rootHash);
		}),
	);
	await log.close();
	await store.stop();
}

for (const nativeGraph of [false, true]) {
	const { log, store } = await createHeadsLog(nativeGraph);
	rows.push(
		await measure("getMemoryUsage()", nativeGraph, async () => {
			await log.entryIndex.getMemoryUsage();
		}),
	);
	await log.close();
	await store.stop();
}

console.table(rows);
