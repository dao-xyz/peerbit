import { serialize } from "@dao-xyz/borsh";
import { createStore } from "@peerbit/any-store";
import { AnyBlockStore } from "@peerbit/blocks";
import { calculateRawCid } from "@peerbit/blocks-interface";
import { Ed25519Keypair } from "@peerbit/crypto";
import { toId } from "@peerbit/indexer-interface";
import { type SQLiteIndices, create } from "@peerbit/indexer-sqlite3";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import sinon from "sinon";
import { ShallowEntry } from "../src/entry-shallow.js";
import { Entry } from "../src/entry.js";
import { Log } from "../src/log.js";

const sorted = (hashes: Iterable<string>) => [...hashes].sort();
const rowBytes = (row: ShallowEntry) =>
	Buffer.from(serialize(row)).toString("hex");
const outcome = <T>(operation: Promise<T>) =>
	operation.then(
		(value) => ({ value, error: undefined as unknown }),
		(error: unknown) => ({ value: undefined, error }),
	);
const gate = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => (resolve = done));
	return { promise, resolve };
};
const enteredBeforeCompletion = async (
	entered: Promise<void>,
	operation: Promise<unknown>,
) => {
	await Promise.race([
		entered,
		operation.then(() => {
			throw new Error("operation completed before the controlled boundary");
		}),
	]);
};

describe("appendMany persistent metadata failure", () => {
	let directory: string;
	let store: AnyBlockStore | undefined;
	let indexer: SQLiteIndices | undefined;
	let log: Log<Uint8Array> | undefined;
	let identity: Ed25519Keypair;
	let logId: Uint8Array;

	beforeEach(async () => {
		directory = await mkdtemp(join(tmpdir(), "peerbit-append-batch-"));
		identity = await Ed25519Keypair.create();
		logId = new Uint8Array(32).fill(17);
	});

	const close = async () => {
		try {
			await log?.close();
		} finally {
			log = undefined;
			try {
				await indexer?.stop();
			} finally {
				indexer = undefined;
				await store?.stop();
				store = undefined;
			}
		}
	};
	afterEach(async () => {
		sinon.restore();
		try {
			await close();
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});
	const open = async (nativeGraph: boolean) => {
		store = new AnyBlockStore(createStore(join(directory, "blocks")));
		await store.start();
		indexer = await create(join(directory, "index"));
		log = new Log<Uint8Array>({ id: logId });
		await log.open(store, identity, { indexer, nativeGraph });
		expect(log.appendDurability).to.equal("strict");
		expect(await log.entryIndex.properties.index.persisted()).to.equal(true);
		expect(Boolean(log.entryIndex.properties.nativeGraph)).to.equal(
			nativeGraph,
		);
	};

	const snapshot = async (entries: Entry<Uint8Array>[]) => {
		const index = log!.entryIndex.properties.index;
		// Inspect cache before public reads can resolve any entries into it.
		const cache = [...log!.entryIndex["cache"].map.entries()]
			.filter(([, item]) => item.value != null)
			.map(([key, item]) => [String(key), item.value!.hash]);
		const iterator = index.iterate({ query: [] });
		let rows: ShallowEntry[];
		try {
			rows = (await iterator.all()).map((item) => item.value);
		} finally {
			await iterator.close();
		}
		const has: string[] = [];
		const indexed: string[] = [];
		for (const entry of entries) {
			if (await log!.has(entry.hash)) has.push(entry.hash);
			if (await index.get(toId(entry.hash))) indexed.push(entry.hash);
		}
		const heads = log!.getHeads();
		let headHashes: string[];
		try {
			headHashes = (await heads.all()).map((entry) => entry.hash);
		} finally {
			await heads.close();
		}
		return {
			length: log!.length,
			count: await index.getSize(),
			rows: new Map(rows.map((row) => [row.hash, rowBytes(row)])),
			indexedHeads: sorted(
				rows.filter((row) => row.head).map((row) => row.hash),
			),
			heads: sorted(headHashes),
			array: sorted((await log!.toArray()).map((entry) => entry.hash)),
			has: sorted(has),
			indexed: sorted(indexed),
			cache,
			native: log!.entryIndex.properties.nativeGraph
				? sorted(
						entries
							.filter((entry) =>
								log!.entryIndex.properties.nativeGraph!.graph.has(entry.hash),
							)
							.map((entry) => entry.hash),
					)
				: undefined,
		};
	};

	const checkBlocks = async (
		entries: Entry<Uint8Array>[],
		payloads: Uint8Array[],
		predecessor: Entry<Uint8Array>,
	) => {
		const present: string[] = [];
		for (const [i, entry] of entries.entries()) {
			const bytes = await store!.get(entry.hash);
			// Failed metadata admission does not promise physical block rollback.
			if (!bytes) continue;
			expect((await calculateRawCid(bytes)).cid).to.equal(entry.hash);
			const restored = await Entry.fromMultihash<Uint8Array>(
				store!,
				entry.hash,
			);
			restored.init(log!);
			expect(await restored.verifySignatures()).to.equal(true);
			expect(await restored.getPayloadValue()).to.deep.equal(payloads[i]);
			expect(restored.meta.next).to.deep.equal([
				i === 0 ? predecessor.hash : entries[i - 1].hash,
			]);
			present.push(entry.hash);
		}
		return present;
	};

	for (const nativeGraph of [false, true]) {
		for (const mode of [
			"success",
			"second chunk",
			"post-apply head",
		] as const) {
			it(`${mode} preserves coherent metadata with nativeGraph=${nativeGraph}`, async () => {
				await open(nativeGraph);
				const predecessor = (
					await log!.append(new Uint8Array([200]), { meta: { next: [] } })
				).entry;
				const unrelated = (
					await log!.append(new Uint8Array([201]), { meta: { next: [] } })
				).entry;
				const prior = [predecessor, unrelated];
				// Native-created seed entries may still have pending hot-index writes.
				// Establish the authoritative prior state before installing any fault.
				await log!.entryIndex.flushPendingWrites();
				const before = await snapshot(prior);
				const predecessorBefore = (await log!.entryIndex.properties.index.get(
					toId(predecessor.hash),
				))!.value;
				expect(before.length).to.equal(2);
				expect(before.count).to.equal(2);
				expect(before.rows.size).to.equal(2);
				expect(before.heads).to.deep.equal(
					sorted(prior.map((entry) => entry.hash)),
				);

				const failure = new Error(`injected ${mode} rejection`);
				const db = indexer!.properties.db;
				const exec = db.exec.bind(db);
				let chunks = 0;
				let releases = 0;
				let injected = false;
				let chunksAtFailure: number | undefined;
				let releasesAtFailure: number | undefined;
				sinon.stub(db, "exec").callsFake(async (sql) => {
					if (sql.startsWith("SAVEPOINT peerbit_put_batch_")) {
						chunks++;
						if (mode === "second chunk" && chunks === 2) {
							injected = true;
							chunksAtFailure = chunks;
							releasesAtFailure = releases;
							throw failure;
						}
					}
					const result = await exec(sql);
					if (sql.startsWith("RELEASE SAVEPOINT peerbit_put_batch_"))
						releases++;
					return result;
				});
				const index = log!.entryIndex.properties.index;
				if (mode === "post-apply head") {
					const put = index.put.bind(index);
					sinon.stub(index, "put").callsFake(async (row) => {
						await put(row);
						if (!injected && row.hash === predecessor.hash && !row.head) {
							injected = true;
							chunksAtFailure = chunks;
							releasesAtFailure = releases;
							expect(
								(await index.get(toId(predecessor.hash)))!.value.head,
							).to.equal(false);
							throw failure;
						}
					});
				}
				const batch = sinon.spy(log!.entryIndex, "putAppendBatch");
				const payloads = Array.from(
					{ length: 65 },
					(_, i) => new Uint8Array([i]),
				);
				const authorized: number[] = [];
				const result = await outcome(
					log!.appendMany(payloads, {
						meta: { next: [predecessor] },
						canAppend: async (entry) => {
							expect(await entry.verifySignatures()).to.equal(true);
							expect(log!.length).to.equal(2);
							authorized.push((await entry.getPayloadValue())[0]);
							return true;
						},
					}),
				);
				expect(batch.callCount).to.equal(1);
				const entries = batch.firstCall.args[0] as Entry<Uint8Array>[];
				sinon.restore();
				expect(entries.length).to.equal(65);
				expect(authorized).to.deep.equal(
					Array.from({ length: 65 }, (_, i) => i),
				);
				expect(injected).to.equal(mode !== "success");
				expect(result.error).to.equal(mode === "success" ? undefined : failure);
				if (mode !== "success") {
					// The predecessor's scalar put has its own one-row SQLite chunk.
					expect(chunksAtFailure).to.equal(mode === "second chunk" ? 2 : 3);
					expect(releasesAtFailure).to.equal(mode === "second chunk" ? 1 : 3);
				} else {
					expect(chunks).to.equal(3);
				}
				const allEntries = [...prior, ...entries];
				const live = await snapshot(allEntries);
				const liveBlocks = await checkBlocks(entries, payloads, predecessor);
				await close();
				await open(nativeGraph);
				const reopened = await snapshot(allEntries);
				await log!.load();
				const loaded = await snapshot(allEntries);
				expect(await checkBlocks(entries, payloads, predecessor)).to.deep.equal(
					liveBlocks,
				);

				const committed = mode === "success" ? allEntries : prior;
				const hashes = sorted(committed.map((entry) => entry.hash));
				const headHashes = sorted([
					unrelated.hash,
					mode === "success" ? entries.at(-1)!.hash : predecessor.hash,
				]);
				const expectedRows = new Map(before.rows);
				if (mode === "success") {
					// Compare the authoritative prior row, changing only its head bit.
					expectedRows.set(
						predecessor.hash,
						rowBytes(new ShallowEntry({ ...predecessorBefore, head: false })),
					);
					for (const entry of entries) {
						expectedRows.set(
							entry.hash,
							rowBytes(entry.toShallow(headHashes.includes(entry.hash))),
						);
					}
					expect(
						result.value!.entries.map((entry) => entry.hash),
					).to.deep.equal(entries.map((entry) => entry.hash));
					expect(liveBlocks.length).to.equal(65);
				}
				for (const [stage, state] of Object.entries({
					live,
					reopened,
					loaded,
				})) {
					expect(state.length, `${stage} length`).to.equal(committed.length);
					expect(state.count, `${stage} index count`).to.equal(
						committed.length,
					);
					expect(
						sorted(state.rows.keys()),
						`${stage} exact persisted hashes`,
					).to.deep.equal(sorted(expectedRows.keys()));
					for (const [hash, expected] of expectedRows) {
						expect(
							state.rows.get(hash),
							`${stage} exact persisted row ${hash}`,
						).to.equal(expected);
					}
					expect(state.heads, `${stage} public heads`).to.deep.equal(
						headHashes,
					);
					expect(state.indexedHeads, `${stage} persisted heads`).to.deep.equal(
						headHashes,
					);
					expect(state.array, `${stage} array membership`).to.deep.equal(
						hashes,
					);
					expect(state.has, `${stage} public membership`).to.deep.equal(hashes);
					expect(state.indexed, `${stage} exact hash lookups`).to.deep.equal(
						hashes,
					);
					if (nativeGraph)
						expect(state.native, `${stage} native membership`).to.deep.equal(
							hashes,
						);
					for (const [key, cachedHash] of state.cache) {
						expect(key, `${stage} cache key`).to.equal(cachedHash);
						expect(
							hashes,
							`${stage} cache contains only committed entries`,
						).to.include(key);
					}
				}
			});
		}
	}

	for (const mode of ["get", "getMany"] as const) {
		it(`invalidates completed and in-flight ${mode} cache publication on rollback`, async () => {
			await open(false);
			const chunkEntered = gate();
			const failAppend = gate();
			const readEntered = gate();
			const releaseRead = gate();
			const failure = new Error("second chunk failed during a block read");
			const db = indexer!.properties.db;
			const exec = db.exec.bind(db);
			let chunks = 0;
			sinon.stub(db, "exec").callsFake(async (sql) => {
				if (sql.startsWith("SAVEPOINT peerbit_put_batch_") && ++chunks === 2) {
					chunkEntered.resolve();
					await failAppend.promise;
					throw failure;
				}
				return exec(sql);
			});
			const batch = sinon.spy(log!.entryIndex, "putAppendBatch");
			const appended = outcome(
				log!.appendMany(
					Array.from({ length: 65 }, (_, i) => new Uint8Array([i])),
					{
						meta: { next: [] },
						canAppend: async (entry) => {
							expect(await entry.verifySignatures()).to.equal(true);
							return true;
						},
					},
				),
			);
			let pendingRead:
				| ReturnType<typeof outcome<Entry<Uint8Array> | undefined>>
				| undefined;
			try {
				await enteredBeforeCompletion(chunkEntered.promise, appended);
				const [completedEntry, delayedEntry] = batch.firstCall.args[0];
				const cache = log!.entryIndex["cache"];
				expect(cache.has(completedEntry.hash)).to.equal(false);
				expect(cache.has(delayedEntry.hash)).to.equal(false);
				const completed = await log!.entryIndex.get(completedEntry.hash);
				expect(await completed!.verifySignatures()).to.equal(true);
				expect(await completed!.getPayloadValue()).to.deep.equal(
					new Uint8Array([0]),
				);
				expect(cache.get(completedEntry.hash)?.hash).to.equal(
					completedEntry.hash,
				);

				let bytes: Uint8Array | undefined;
				if (mode === "get") {
					const get = store!.get.bind(store);
					sinon.stub(store!, "get").callsFake(async (hash, options) => {
						const result = await get(hash, options);
						if (hash === delayedEntry.hash) {
							bytes = result;
							readEntered.resolve();
							await releaseRead.promise;
						}
						return result;
					});
					pendingRead = outcome(log!.entryIndex.get(delayedEntry.hash));
				} else {
					const getMany = store!.getMany.bind(store);
					sinon.stub(store!, "getMany").callsFake(async (hashes, options) => {
						const results = await getMany(hashes, options);
						const position = hashes.indexOf(delayedEntry.hash);
						if (position !== -1) {
							bytes = results[position];
							readEntered.resolve();
							await releaseRead.promise;
						}
						return results;
					});
					pendingRead = outcome(
						log!.entryIndex
							.getMany([delayedEntry.hash])
							.then((entries) => entries[0]),
					);
				}
				await enteredBeforeCompletion(readEntered.promise, pendingRead);
				expect(bytes).not.to.equal(undefined);
				expect((await calculateRawCid(bytes!)).cid).to.equal(delayedEntry.hash);
				failAppend.resolve();
				expect((await appended).error).to.equal(failure);
				expect(
					cache.has(completedEntry.hash),
					"completed read cache evicted",
				).to.equal(false);
				expect(
					cache.has(delayedEntry.hash),
					"delayed read has not published",
				).to.equal(false);
				expect(log!.length).to.equal(0);
				expect(await log!.entryIndex.properties.index.getSize()).to.equal(0);

				releaseRead.resolve();
				const resolved = await pendingRead;
				expect(resolved.error).to.equal(undefined);
				expect(resolved.value!.hash).to.equal(delayedEntry.hash);
				expect(await resolved.value!.verifySignatures()).to.equal(true);
				expect(await resolved.value!.getPayloadValue()).to.deep.equal(
					new Uint8Array([1]),
				);
				expect(
					cache.has(delayedEntry.hash),
					"stale read must not republish",
				).to.equal(false);
				expect(cache.has(completedEntry.hash)).to.equal(false);
				expect(await log!.has(delayedEntry.hash)).to.equal(false);
				// Known retained blocks may resolve without being log members.
				expect(await store!.get(delayedEntry.hash)).to.deep.equal(bytes);
			} finally {
				failAppend.resolve();
				releaseRead.resolve();
				await appended;
				await pendingRead;
			}
		});
	}
});
