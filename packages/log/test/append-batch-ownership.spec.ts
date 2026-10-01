import { serialize } from "@dao-xyz/borsh";
import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { toId } from "@peerbit/indexer-interface";
import { type SQLiteIndices, create } from "@peerbit/indexer-sqlite3";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import sinon from "sinon";
import { createEntry } from "../src/entry-create.js";
import type { ShallowEntry } from "../src/entry-shallow.js";
import type { Entry } from "../src/entry.js";
import { Log } from "../src/log.js";

const deferred = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => {
		resolve = done;
	});
	return { promise, resolve };
};
const outcome = <T>(promise: Promise<T>) =>
	promise.then(
		(value) => ({ value, error: undefined as unknown }),
		(error: unknown) => ({ value: undefined, error }),
	);
const within = async <T>(promise: Promise<T>, label: string): Promise<T> => {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<never>((_, reject) => {
				timer = setTimeout(
					() => reject(new Error(`Timed out: ${label}`)),
					5000,
				);
			}),
		]);
	} finally {
		clearTimeout(timer);
	}
};
const bytes = (row: ShallowEntry) =>
	Buffer.from(serialize(row)).toString("hex");
const containsError = (actual: unknown, expected: Error): boolean =>
	actual === expected ||
	(actual instanceof AggregateError &&
		actual.errors.some((error) => containsError(error, expected))) ||
	(actual instanceof Error && containsError(actual.cause, expected));

describe("ordinary append batch metadata ownership", () => {
	let directory: string;
	let identity: Ed25519Keypair;
	let store: AnyBlockStore;
	let indexer: SQLiteIndices;
	let log: Log<Uint8Array>;
	const id = new Uint8Array(32).fill(31);
	const open = async () => {
		indexer = await create(directory);
		log = new Log<Uint8Array>({ id });
		await log.open(store, identity, {
			indexer,
			nativeGraph: false,
			appendDurability: "strict",
		});
		expect(Boolean(log.entryIndex.properties.nativeGraph)).to.equal(false);
	};
	beforeEach(async () => {
		directory = await mkdtemp(join(tmpdir(), "peerbit-batch-ownership-"));
		identity = await Ed25519Keypair.create();
		store = new AnyBlockStore();
		await store.start();
		await open();
	});
	afterEach(async () => {
		sinon.restore();
		const failures: unknown[] = [];
		for (const [label, close] of [
			["log close", () => log?.close()],
			["indexer stop", () => indexer?.stop()],
			["block store stop", () => store?.stop()],
		] as const) {
			try {
				await within(Promise.resolve(close()), label);
			} catch (error) {
				failures.push(error);
			}
		}
		await rm(directory, { recursive: true, force: true });
		if (failures.length)
			throw new AggregateError(failures, "Fixture cleanup failed");
	});
	const append = async (value: number, next: Entry<Uint8Array>[] = []) =>
		(
			await log.append(new Uint8Array([value]), {
				meta: { next },
				canAppend: async () => true,
			})
		).entry;
	const batch = (predecessor: Entry<Uint8Array>) =>
		log.appendMany(
			Array.from({ length: 65 }, (_, i) => new Uint8Array([i])),
			{
				meta: { next: [predecessor] },
				canAppend: async () => true,
			},
		);
	const exact = async (
		entries: Entry<Uint8Array>[],
		heads: Entry<Uint8Array>[],
	) => {
		const index = log.entryIndex.properties.index;
		const iterator = index.iterate({ query: [] });
		try {
			const actual = (await iterator.all()).map((item) => [
				item.value.hash,
				bytes(item.value),
			]);
			const expected = entries.map((entry) => [
				entry.hash,
				bytes(entry.toShallow(heads.includes(entry))),
			]);
			expect(actual.sort()).to.deep.equal(expected.sort());
		} finally {
			await iterator.close();
		}
		expect(log.length).to.equal(entries.length);
		const publicHeads = log.getHeads();
		try {
			expect(
				(await publicHeads.all()).map((entry) => entry.hash).sort(),
			).to.deep.equal(heads.map((entry) => entry.hash).sort());
		} finally {
			await publicHeads.close();
		}
	};
	const holdSecondChunk = () => {
		const reached = deferred();
		const release = deferred();
		const failure = new Error("second batch chunk rejected");
		const exec = indexer.properties.db.exec.bind(indexer.properties.db);
		let chunks = 0;
		let releases = 0;
		sinon.stub(indexer.properties.db, "exec").callsFake(async (sql) => {
			if (sql.startsWith("SAVEPOINT peerbit_put_batch_") && ++chunks === 2) {
				expect(releases).to.equal(1);
				reached.resolve();
				await release.promise;
				throw failure;
			}
			const result = await exec(sql);
			if (sql.startsWith("RELEASE SAVEPOINT peerbit_put_batch_")) releases++;
			return result;
		});
		return { reached, release, failure };
	};
	const observeAdmission = (entry: Entry<Uint8Array>, deleting = false) => {
		const reached = deferred();
		const acquire = log.entryIndex.acquireHashMutationLocks.bind(
			log.entryIndex,
		);
		sinon
			.stub(log.entryIndex, "acquireHashMutationLocks")
			.callsFake((hashes) => {
				const keys = [...hashes];
				const pending = acquire(keys);
				if (keys.includes(entry.hash)) {
					expect(keys).to.include.members(entry.meta.next);
					reached.resolve();
				}
				return pending;
			});
		// The original non-native implementation has no hash admission. Observe
		// its actual adapter admission instead, so baseline failures are assertions
		// about surviving metadata, not a missing instrumentation hook timeout.
		const index = log.entryIndex.properties.index;
		const get = index.get.bind(index);
		sinon.stub(index, "get").callsFake((...args) => {
			const pending = get(...args);
			if (args[0].primitive === entry.hash) reached.resolve();
			return pending;
		});
		if (deleting) {
			const remove = index.del.bind(index);
			sinon.stub(index, "del").callsFake((...args) => {
				const pending = remove(...args);
				reached.resolve();
				return pending;
			});
		} else {
			const put = index.put.bind(index);
			sinon.stub(index, "put").callsFake((row) => {
				const pending = put(row);
				if (row.hash === entry.hash) {
					expect(row.meta.next).to.deep.equal(entry.meta.next);
					reached.resolve();
				}
				return pending;
			});
		}
		return reached;
	};

	for (const kind of ["append", "join", "delete"] as const) {
		it(`preserves a queued lower ${kind} mutation sharing the rejected batch's predecessor`, async () => {
			const predecessor = await append(200);
			const removed =
				kind === "delete" ? await append(201, [predecessor]) : undefined;
			const sibling =
				kind !== "delete"
					? await createEntry({
							store,
							identity,
							data: new Uint8Array([202]),
							meta: { next: [predecessor] },
						})
					: undefined;
			const gate = holdSecondChunk();
			const rejected = outcome(batch(predecessor));
			let competitor:
				| ReturnType<typeof outcome<Entry<Uint8Array> | undefined>>
				| undefined;
			try {
				await within(gate.reached.promise, "second committed chunk boundary");
				const entered = observeAdmission(
					removed ?? sibling!,
					kind === "delete",
				);
				// The public APIs pre-read SQLite before reaching lower admission and
				// would block behind the held chunk. Pre-create signed entries and
				// exercise the actual lower append/join/delete ownership boundary.
				competitor = outcome(
					kind === "delete"
						? log.entryIndex
								.delete(removed!.hash, removed!)
								.then(() => undefined)
						: log.entryIndex
								.put(sibling!, {
									unique: kind === "append",
									isHead: true,
									toMultiHash: kind === "join",
									deferIndexWrite: false,
								})
								.then(() => sibling!),
				);
				await within(
					entered.promise,
					"competing hash or adapter admission reached",
				);
				gate.release.resolve();
				expect((await within(rejected, "batch rejection")).error).to.equal(
					gate.failure,
				);
				const result = await within(competitor, "competing mutation settled");
				expect(result.error).to.equal(undefined);
				await exact(
					result.value ? [predecessor, result.value] : [predecessor],
					result.value ? [result.value] : [predecessor],
				);
				if (removed) expect(await log.has(removed.hash)).to.equal(false);
			} finally {
				gate.release.resolve();
				await within(
					Promise.all([rejected, ...(competitor ? [competitor] : [])]),
					"owned operations settled",
				);
			}
		});
	}

	it("promotes a buffered predecessor after deleting its last child without relocking itself", async () => {
		const predecessor = await append(210);
		const child = await append(211, [predecessor]);
		// Exercise the lower deferred-facts state explicitly: ordinary strict child
		// append normally flushes its predecessor before this deletion can occur.
		await log.entryIndex.putNativeCommittedAppendFacts({
			hash: predecessor.hash,
			unique: false,
			externalNextHashes: [],
			shallowEntry: predecessor.toShallow(false),
			isHead: false,
		});
		expect(log.entryIndex["pendingIndexWrites"].has(predecessor.hash)).to.equal(
			true,
		);
		const removed = await within(
			log.delete(child.hash),
			"buffered predecessor promotion",
		);
		expect(removed?.hash).to.equal(child.hash);
		await log.entryIndex.flushPendingWrites();
		await exact([predecessor], [predecessor]);
	});

	it("releases delete's initial hash lease when its row snapshot fails", async () => {
		const entry = await append(215);
		const index = log.entryIndex.properties.index;
		const get = index.get.bind(index);
		const failure = new Error("initial delete snapshot failed");
		const read = sinon.stub(index, "get").callsFake(get);
		read.onFirstCall().rejects(failure);
		// Deliberately omit `from`: deletion must read immutable next pointers
		// while owning its initial hash lease, before acquiring the complete set.
		const result = await within(
			outcome(log.entryIndex.delete(entry.hash)),
			"initial delete snapshot rejection",
		);
		expect(read.callCount).to.equal(1);
		expect(result.error).to.equal(failure);
		read.restore();
		const removed = await within(
			log.entryIndex.delete(entry.hash),
			"same-hash delete after failed snapshot",
		);
		expect(removed?.hash).to.equal(entry.hash);
		await exact([], []);
	});

	it("poisons a queued lower mutation after rollback failure and lets close retry exact compensation", async () => {
		const predecessor = await append(220);
		const sibling = await createEntry({
			store,
			identity,
			data: new Uint8Array([221]),
			meta: { next: [predecessor] },
		});
		const gate = holdSecondChunk();
		const cleanupFailure = new Error("rollback deletion failed");
		const rejected = outcome(batch(predecessor));
		let competitor: ReturnType<typeof outcome<Entry<Uint8Array>>> | undefined;
		let rollbackCalls = 0;
		try {
			await within(gate.reached.promise, "second committed chunk boundary");
			const entered = observeAdmission(sibling);
			const failedDelete = sinon
				.stub(log.entryIndex.properties.index, "del")
				.callsFake(async () => {
					rollbackCalls++;
					throw cleanupFailure;
				});
			competitor = outcome(
				log.entryIndex
					.put(sibling, {
						unique: true,
						isHead: true,
						toMultiHash: false,
						deferIndexWrite: false,
					})
					.then(() => sibling),
			);
			await within(entered.promise, "queued lower mutation reached admission");
			gate.release.resolve();
			const result = await within(rejected, "failed compensation rejects");
			expect(rollbackCalls).to.be.greaterThan(0);
			expect(containsError(result.error, gate.failure)).to.equal(true);
			expect(containsError(result.error, cleanupFailure)).to.equal(true);
			const queued = await within(
				competitor,
				"queued append rejects while poisoned",
			);
			expect(queued.value).to.equal(undefined);
			expect(containsError(queued.error, cleanupFailure)).to.equal(true);
			failedDelete.restore();
			await within(log.close(), "close retries retained rollback");
			await indexer.stop();
			sinon.restore();
			await open();
			await exact([predecessor], [predecessor]);
		} finally {
			gate.release.resolve();
			await within(
				Promise.all([rejected, ...(competitor ? [competitor] : [])]),
				"failed operations settled",
			);
		}
	});

	it("restores a pre-existing batch hash exactly instead of treating it as newly owned", async () => {
		const predecessor = await append(230);
		const existing = await append(231, [predecessor]);
		const index = log.entryIndex.properties.index;
		const prior = bytes((await index.get(toId(existing.hash)))!.value);
		const entries: Entry<Uint8Array>[] = [existing];
		for (let i = 0; i < 64; i++)
			entries.push(
				await createEntry({
					store,
					identity,
					data: new Uint8Array([i]),
					meta: { next: [entries.at(-1)!] },
				}),
			);
		const gate = holdSecondChunk();
		// Lower invocation is intentional: a freshly clocked public appendMany
		// would not normally recreate an already indexed content-addressed hash.
		const rejected = outcome(
			log.entryIndex.putAppendBatch(entries, {
				unique: true,
				externalNextHashes: [predecessor.hash],
			}),
		);
		try {
			await within(
				gate.reached.promise,
				"pre-existing hash batch reached second chunk",
			);
			gate.release.resolve();
			expect(
				(await within(rejected, "duplicate-row batch rejected")).error,
			).to.equal(gate.failure);
			expect(bytes((await index.get(toId(existing.hash)))!.value)).to.equal(
				prior,
			);
			await exact([predecessor, existing], [existing]);
		} finally {
			gate.release.resolve();
			await within(rejected, "duplicate-row operation settled");
		}
	});
});
