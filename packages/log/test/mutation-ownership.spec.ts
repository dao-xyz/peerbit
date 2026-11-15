import { Ed25519Keypair } from "@peerbit/crypto";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { createNativeLogBlockStore } from "@peerbit/log-rust";
import { expect } from "chai";
import sinon from "sinon";
import { NO_ENCODING } from "../src/encoding.js";
import type { EntryIndexHashMutationLockOwner } from "../src/entry-index.js";
import type { ShallowEntry } from "../src/entry-shallow.js";
import { EntryV0 } from "../src/entry-v0.js";
import type { Entry } from "../src/entry.js";
import { Log } from "../src/log.js";

describe("native mutation ownership", () => {
	let log: Log<Uint8Array>;
	let store: Awaited<ReturnType<typeof createNativeLogBlockStore>>;
	let identity: Ed25519Keypair;

	beforeEach(async () => {
		store = await createNativeLogBlockStore();
		await store.start();
		identity = await Ed25519Keypair.create();
		log = new Log();
		await log.open(store, identity, {
			indexer: new HashmapIndices(),
			nativeGraph: true,
		});
	});

	afterEach(async () => {
		await log.close();
		await store.stop();
	});

	it("keeps disjoint hash owners concurrent and admits a queued exclusive before later readers", async () => {
		const index = log.entryIndex;
		const first = await index.acquireHashMutationLocks(["first"]);
		const second = await index.acquireHashMutationLocks(["second"]);
		const order: string[] = [];
		const exclusivePending = index
			.acquireExclusiveMutationLock()
			.then((owner) => {
				order.push("exclusive");
				return owner;
			});
		const latePending = index
			.acquireHashMutationLocks(["late"])
			.then((owner) => {
				order.push("late");
				return owner;
			});
		await Promise.resolve();
		expect(order).to.deep.equal([]);
		index.releaseHashMutationLocks(first);
		await Promise.resolve();
		expect(order).to.deep.equal([]);
		index.releaseHashMutationLocks(second);
		const exclusive = await exclusivePending;
		try {
			expect(order).to.deep.equal(["exclusive"]);
			index.assertHashMutationLocks(exclusive, ["not-known-at-acquisition"]);
		} finally {
			index.releaseHashMutationLocks(exclusive);
		}
		const late = await latePending;
		index.releaseHashMutationLocks(late);
		expect(order).to.deep.equal(["exclusive", "late"]);
		expect(() => index.assertHashMutationLocks(exclusive, [])).to.throw(
			"released",
		);
	});

	it("rejects and releases queued owners when their predecessor poisons the index", async () => {
		const index = log.entryIndex;
		const held = await index.acquireHashMutationLocks(["held"]);
		const exclusive = index
			.acquireExclusiveMutationLock()
			.catch((error) => error);
		const reader = index
			.acquireHashMutationLocks(["later"])
			.catch((error) => error);
		const failure = new Error("unretired durable intent");
		index.poisonNativeDurableTransactionMutations(failure);
		index.releaseHashMutationLocks(held);
		expect(await exclusive).to.have.property("cause", failure);
		expect(await reader).to.have.property("cause", failure);
		index.clearNativeDurableTransactionMutationFailure();
		const recovered = await index.acquireExclusiveMutationLock();
		index.releaseHashMutationLocks(recovered);
	});

	it("limits poison bypass to a releasing recovery scope without clearing poison", async () => {
		const index = log.entryIndex;
		index.poisonNativeDurableTransactionMutations(new Error("recover me"));
		for (const asynchronous of [false, true]) {
			const failure = new Error("recovery failed");
			const result = await Promise.resolve()
				.then(() =>
					index.withExclusiveMutationRecovery((owner) => {
						index.assertHashMutationLocks(owner, ["recovery-row"]);
						if (asynchronous) return Promise.reject(failure);
						throw failure;
					}),
				)
				.catch((error) => error);
			expect(result).to.equal(failure);
		}
		expect(await index.acquireExclusiveMutationLock().catch((error) => error))
			.to.have.property("message")
			.that.includes("poisoned");
		index.clearNativeDurableTransactionMutationFailure();
		const recovered = await index.acquireHashMutationLocks(["recovery-row"]);
		index.releaseHashMutationLocks(recovered);
	});

	it("defers ownerless append-facts transactions behind an exclusive owner", async () => {
		const { entry } = await log.append(Uint8Array.of(1), {
			meta: { next: [] },
		});
		const index = log.entryIndex;
		const held = await index.acquireExclusiveMutationLock();
		const transaction = index.beginNativeCommittedAppendFactsTransaction([
			entry.hash,
		]);
		let staged = false;
		const pending = Promise.resolve(
			index.putNativeCommittedAppendFacts(
				{
					hash: entry.hash,
					unique: true,
					externalNextHashes: [],
					shallowEntry: entry.toShallow(true),
				},
				transaction,
			),
		).then(() => {
			staged = true;
		});
		await Promise.resolve();
		expect(staged).to.equal(false);
		expect(transaction.rows).to.have.length(0);
		index.releaseHashMutationLocks(held);
		await pending;
		expect(transaction.rows).to.have.length(1);
		await index.rollbackNativeCommittedAppendFacts(transaction);
		const after = await index.acquireExclusiveMutationLock();
		index.releaseHashMutationLocks(after);
	});

	it("rejects premature transaction acknowledgement without stranding admission", async () => {
		const index = log.entryIndex;
		const held = await index.acquireExclusiveMutationLock();
		const transaction = index.beginNativeCommittedAppendFactsTransaction([
			"pending",
		]);
		try {
			expect(() =>
				index.acknowledgeNativeCommittedAppendFacts(transaction),
			).to.throw("awaiting mutation admission");
			expect(transaction.state).to.equal("open");
		} finally {
			index.releaseHashMutationLocks(held);
			await index.rollbackNativeCommittedAppendFacts(transaction);
		}
		const after = await index.acquireExclusiveMutationLock();
		index.releaseHashMutationLocks(after);
	});

	it("releases opaque preparation ownership but fails closed after a native throw", async () => {
		const index = log.entryIndex;
		const failure = new Error(
			"native preparation failed without a mutation result",
		);
		const prepare = sinon
			.stub(
				index.properties.nativeGraph!.graph,
				"prepareEntryV0PlainEntryCommit",
			)
			.throws(failure);
		try {
			expect(
				await log
					.append(Uint8Array.of(1), { meta: { next: [] } })
					.catch((error) => error),
			).to.equal(failure);
			expect(prepare.callCount).to.equal(1);
			expect(
				await index.acquireHashMutationLocks(["later"]).catch((error) => error),
			).to.have.property("cause", failure);
			await index.withExclusiveMutationRecovery((owner) =>
				index.assertHashMutationLocks(owner, ["unknown-native-hash"]),
			);
		} finally {
			prepare.restore();
			index.clearNativeDurableTransactionMutationFailure();
		}
	});

	it("releases pure native ownership before generic trim and change callbacks", async () => {
		await log.append(Uint8Array.of(1), { meta: { next: [] } });
		let trimmed = false;
		let changed = false;
		await log.append(Uint8Array.of(2), {
			meta: { next: [] },
			trim: {
				type: "length",
				to: 1,
				filter: {
					canTrim: async () => {
						if (!trimmed) {
							trimmed = true;
							await log.append(Uint8Array.of(3), { meta: { next: [] } });
						}
						return false;
					},
				},
			},
			onChange: async () => {
				changed = true;
				await log.append(Uint8Array.of(4), { meta: { next: [] } });
			},
		});
		expect(trimmed).to.equal(true);
		expect(changed).to.equal(true);
		expect(log.length).to.equal(4);
	});

	for (const route of ["single", "batch", "native trim"] as const) {
		it(`fails closed when ${route} deletion fails before mandatory metadata completion`, async () => {
			const entries: Entry<Uint8Array>[] = [];
			for (const value of route === "single" ? [1] : [1, 2]) {
				entries.push(
					(await log.append(Uint8Array.of(value), { meta: { next: [] } }))
						.entry,
				);
			}
			const index = log.entryIndex;
			await index.flushPendingWrites();
			const complete = sinon.spy();
			index.properties.onDeleteCommitted = complete;
			const failure = new Error("store deletion failed after index deletion");
			const remove = sinon
				.stub(store, route === "single" ? "rm" : "rmMany")
				.rejects(failure);
			try {
				if (route === "native trim") {
					for (const entry of entries)
						index.properties.nativeGraph!.graph.delete(entry.hash);
				}
				const result = await Promise.resolve()
					.then<unknown>(() =>
						route === "single"
							? index.delete(entries[0]!.hash)
							: route === "batch"
								? index.deleteMany(
										entries.map((entry) => entry.toShallow(true)),
									)
								: index.consumeNativeTrimmedEntriesMaybe(
										entries.map((entry) => entry.toShallow(true)),
										{ deleteBlocks: true, skipNextHeadUpdates: true },
									),
					)
					.catch((error) => error);
				expect(result).to.equal(failure);
				expect(complete.callCount).to.equal(0);
				expect(await index.properties.index.getSize()).to.equal(0);
				expect(
					await index
						.acquireHashMutationLocks(["later"])
						.catch((error) => error),
				).to.have.property("cause", failure);
				await index.withExclusiveMutationRecovery((owner) =>
					index.assertHashMutationLocks(
						owner,
						entries.map((entry) => entry.hash),
					),
				);
			} finally {
				remove.restore();
				index.properties.onDeleteCommitted = undefined;
				index.clearNativeDurableTransactionMutationFailure();
			}
		});
	}

	for (const superseded of [false, true]) {
		it(`excludes recovery-retained pending generations from close${superseded ? " without excluding a newer same-CID generation" : " without manufacturing a marker"}`, async () => {
			const entry = await EntryV0.create({
				store,
				identity,
				encoding: NO_ENCODING,
				data: Uint8Array.of(1),
				meta: { next: [] },
			});
			const index = log.entryIndex;
			const transaction = index.beginNativeCommittedAppendFactsTransaction([
				entry.hash,
			]);
			await index.putNativeCommittedAppendFacts(
				{
					hash: entry.hash,
					unique: true,
					externalNextHashes: [],
					shallowEntry: entry.toShallow(true),
					isHead: true,
				},
				transaction,
			);
			index.retainNativeCommittedAppendFactsForRecovery(transaction);
			if (superseded) {
				await index.putNativeCommittedAppendFacts({
					hash: entry.hash,
					unique: false,
					externalNextHashes: [],
					shallowEntry: entry.toShallow(false),
					isHead: false,
				});
			}
			const failure = new Error("retained strict intent");
			index.poisonNativeDurableTransactionMutations(failure);
			const put = sinon.spy(index.properties.index, "put");
			try {
				await log.close();
				expect(put.callCount).to.equal(superseded ? 1 : 0);
				if (superseded) expect(put.firstCall.args[0].head).to.equal(false);
				expect(
					await index.acquireExclusiveMutationLock().catch((error) => error),
				).to.have.property("cause", failure);
			} finally {
				put.restore();
			}
		});
	}

	for (const failFlush of [false, true]) {
		it(`drains committed pending writes on poisoned close${failFlush ? " after a retryable flush failure" : " without clearing the original failure"}`, async () => {
			const entry = await EntryV0.create({
				store,
				identity,
				encoding: NO_ENCODING,
				data: Uint8Array.of(1),
				deferStore: true,
				meta: { next: [] },
			});
			const index = log.entryIndex;
			await index.put(entry, {
				unique: true,
				isHead: true,
				toMultiHash: true,
				deferIndexWrite: true,
			});
			expect(await index.properties.index.getSize()).to.equal(0);
			const failure = new Error("mandatory metadata failed");
			index.poisonNativeDurableTransactionMutations(failure);
			const indexer = (log as any)._indexer;
			const originalStop = indexer.stop.bind(indexer);
			let rowsAtStop: number | undefined;
			const stop = sinon.stub(indexer, "stop").callsFake(async () => {
				rowsAtStop = await index.properties.index.getSize();
				await originalStop();
			});
			try {
				if (failFlush) {
					const flushFailure = new Error("pending write flush failed");
					const put = sinon
						.stub(index.properties.index, "put")
						.rejects(flushFailure);
					try {
						expect(await log.close().catch((error) => error)).to.equal(
							flushFailure,
						);
						expect(stop.called).to.equal(false);
						expect(await index.properties.index.getSize()).to.equal(0);
						expect(await log.has(entry.hash)).to.equal(true);
					} finally {
						put.restore();
					}
				}
				await log.close();
				expect(stop.calledOnce).to.equal(true);
				expect(rowsAtStop).to.equal(1);
				expect(await store.has(entry.hash)).to.equal(true);
				expect(
					await index.acquireExclusiveMutationLock().catch((error) => error),
				).to.have.property("cause", failure);
			} finally {
				stop.restore();
			}
		});
	}

	for (const route of ["independent", "recursive"] as const) {
		it(`fails closed when ${route} receive fails after lower commit but before mandatory completion`, async () => {
			const entries = await Promise.all(
				(route === "independent" ? [1, 2] : [1]).map((value) =>
					EntryV0.create({
						store,
						identity,
						encoding: NO_ENCODING,
						data: Uint8Array.of(value),
						deferStore: true,
						meta: { next: [] },
					}),
				),
			);
			const index = log.entryIndex;
			const complete = sinon.spy();
			const failure = new Error("post-commit receive failure");
			const probes = sinon.createSandbox();
			try {
				if (route === "recursive") {
					const put = index.put.bind(index);
					probes.stub(index, "put").callsFake(async (...args) => {
						await put(...args);
						throw failure;
					});
				}
				const result = await Promise.resolve()
					.then(() =>
						route === "independent"
							? (log as any).tryJoinIndependentAppendBatch(
									entries,
									new Map(entries.map((entry) => [entry.hash, true])),
									{
										__peerbitOnJoinCommitted: complete,
										__peerbitProfile: (event: { name: string }) => {
											if (event.name === "log.joinIndependent.entryIndex")
												throw failure;
										},
									},
									new AbortController().signal,
								)
							: log.join(entries, {
									__peerbitOnJoinCommitted: complete,
								} as any),
					)
					.catch((error) => error);
				expect(result).to.equal(failure);
				expect(complete.callCount).to.equal(0);
				for (const entry of entries)
					expect(await log.has(entry.hash)).to.equal(true);
				expect(
					await index.acquireExclusiveMutationLock().catch((error) => error),
				).to.have.property("cause", failure);
				await index.withExclusiveMutationRecovery((owner) =>
					index.assertHashMutationLocks(
						owner,
						entries.map((entry) => entry.hash),
					),
				);
			} finally {
				probes.restore();
				index.clearNativeDurableTransactionMutationFailure();
			}
		});
	}

	for (const route of [
		"append",
		"appendMany",
		"prepared",
		"commit-only",
		"independent-batch",
	] as const) {
		it(`owns native ${route} preparation before mutating a leased parent`, async () => {
			const { entry: parent } = await log.append(Uint8Array.of(1), {
				meta: { next: [] },
			});
			const index = log.entryIndex;
			const graph = index.properties.nativeGraph!.graph;
			let held: EntryIndexHashMutationLockOwner | undefined =
				await index.acquireHashMutationLocks([parent.hash]);
			let entered!: () => void;
			const entering = new Promise<void>((resolve) => {
				entered = resolve;
			});
			const acquire = index.acquireExclusiveMutationLockMaybe.bind(index);
			const probes = sinon.createSandbox();
			probes.stub(index, "acquireExclusiveMutationLockMaybe").callsFake(() => {
				const pending = acquire();
				entered();
				return pending;
			});
			const nativeCalls = [
				probes.spy(graph, "prepareEntryV0PlainEntryCommit"),
				probes.spy(graph, "prepareEntryV0PlainChainCommit"),
				probes.spy(graph, "prepareEntryV0PlainEntriesCommit"),
			];
			const options = { meta: { next: [parent] } };
			let pending: Promise<unknown> | undefined;
			try {
				pending = Promise.resolve(
					route === "append"
						? log.append(Uint8Array.of(2), options)
						: route === "appendMany"
							? log.appendMany([Uint8Array.of(2), Uint8Array.of(3)], options)
							: route === "prepared"
								? (log as any).appendLocallyPrepared(Uint8Array.of(2), options)
								: route === "commit-only"
									? (log as any).appendLocallyPreparedCommitOnly(
											Uint8Array.of(2),
											options,
										)
									: (log as any).appendLocallyPreparedManyIndependent(
											[Uint8Array.of(2), Uint8Array.of(3)],
											{},
											{ nexts: [[parent], [parent]] },
										),
				);
				await entering;
				expect(
					nativeCalls.reduce((sum, spy) => sum + spy.callCount, 0),
				).to.equal(0);
				expect(graph.heads()).to.deep.equal([parent.hash]);
				expect(log.length).to.equal(1);
				index.releaseHashMutationLocks(held);
				held = undefined;
				expect(await pending).to.exist;
				expect(
					nativeCalls.reduce((sum, spy) => sum + spy.callCount, 0),
				).to.be.greaterThan(0);
				expect(graph.heads()).not.to.include(parent.hash);
				expect(log.length).to.equal(
					route === "appendMany" || route === "independent-batch" ? 3 : 2,
				);
			} finally {
				if (held) index.releaseHashMutationLocks(held);
				await pending?.catch(() => {});
				probes.restore();
			}
		});
	}

	for (const handled of [false, true]) {
		it(`owns prepared replacement and trim completion with ${handled ? "operation" : "fallback"} metadata`, async () => {
			const { entry: parent } = await log.append(Uint8Array.of(1), {
				meta: { next: [] },
			});
			const index = log.entryIndex;
			const fallback = sinon.spy();
			index.properties.onDeleteCommitted = fallback;
			let completed = 0;
			try {
				const result = await (log as any).appendLocallyPrepared(
					Uint8Array.of(2),
					{ meta: { next: [parent] }, trim: { type: "length", to: 1 } },
					{
						resolveTrimmedEntries: false,
						onTrimCommitted: (properties: {
							entry: Entry<Uint8Array>;
							removed: ShallowEntry[];
							hashMutationLockOwner: EntryIndexHashMutationLockOwner;
						}) => {
							index.assertHashMutationLocks(properties.hashMutationLockOwner, [
								properties.entry.hash,
								...properties.entry.meta.next,
								...properties.removed.map((entry) => entry.hash),
							]);
							expect(
								properties.removed.map((entry) => entry.hash),
							).to.deep.equal([parent.hash]);
							completed++;
							return handled;
						},
					},
				);
				expect(completed).to.equal(1);
				expect(fallback.callCount).to.equal(handled ? 0 : 1);
				expect(
					result.removed.map((entry: ShallowEntry) => entry.hash),
				).to.deep.equal([parent.hash]);
				expect(await log.has(parent.hash)).to.equal(false);
				expect(await log.has(result.entry.hash)).to.equal(true);
			} finally {
				index.properties.onDeleteCommitted = undefined;
			}
		});
	}

	it("releases native delete admission when its initial row snapshot rejects", async () => {
		const { entry } = await log.append(Uint8Array.of(1), {
			meta: { next: [] },
		});
		const index = log.entryIndex;
		await index.flushPendingWrites();
		expect(index.properties.nativeGraph).to.exist;
		const failure = new Error("initial delete snapshot failed");
		const read = sinon.stub(index.properties.index, "get").rejects(failure);
		const acquire = sinon.spy(index, "acquireHashMutationLocks");
		const release = sinon.spy(index, "releaseHashMutationLocks");
		let captured: EntryIndexHashMutationLockOwner | undefined;
		try {
			expect(await index.delete(entry.hash).catch((error) => error)).to.equal(
				failure,
			);
			captured = await acquire.firstCall.returnValue;
			expect(read.calledOnce).to.equal(true);
			expect(release.calledWith(captured)).to.equal(true);
			expect(() =>
				index.assertHashMutationLocks(captured!, [entry.hash]),
			).to.throw("released");
			read.restore();
			const exclusive = await index.acquireExclusiveMutationLock();
			index.releaseHashMutationLocks(exclusive);
			expect((await index.delete(entry.hash))?.hash).to.equal(entry.hash);
		} finally {
			if (captured && !release.calledWith(captured))
				index.releaseHashMutationLocks(captured);
			read.restore();
			acquire.restore();
			release.restore();
		}
	});

	it("does not remove a readmitted block after one-by-one trim completion releases ownership", async () => {
		const { entry } = await log.append(Uint8Array.of(1), {
			meta: { next: [] },
		});
		const index = log.entryIndex;
		const remove = index.deleteMany.bind(index);
		let completed = false;
		const deleting = sinon
			.stub(index, "deleteMany")
			.callsFake(async (...args) => {
				const removed = await remove(...args);
				expect(completed).to.equal(true);
				expect(await store.has(entry.hash)).to.equal(false);
				// The lower deletion has released its owner. Re-admit the same CID before
				// its trim adapter resumes, as another legitimate operation can now do.
				await index.put(entry, {
					unique: true,
					isHead: true,
					toMultiHash: true,
				});
				return removed;
			});
		try {
			await (log as any)._trim._log.deleteNode(entry.toShallow(true), {
				resolveDeletedEntry: false,
				onDeleteCommitted: (
					removed: ShallowEntry[],
					owner: EntryIndexHashMutationLockOwner,
				) => {
					index.assertHashMutationLocks(owner, [entry.hash]);
					expect(removed.map((value) => value.hash)).to.deep.equal([
						entry.hash,
					]);
					completed = true;
					return true;
				},
			});
			expect(await store.has(entry.hash)).to.equal(true);
			expect(await log.has(entry.hash)).to.equal(true);
			expect(log.length).to.equal(1);
		} finally {
			deleting.restore();
		}
	});

	it("borrows exclusive ownership when implicit native heads must flush a queued pending row", async () => {
		const index = log.entryIndex;
		index.properties.nativeGraph!.useHeads = false;
		const parent = await EntryV0.create({
			store,
			identity,
			encoding: NO_ENCODING,
			data: Uint8Array.of(1),
			meta: { next: [] },
		});
		let held: EntryIndexHashMutationLockOwner | undefined =
			await index.acquireHashMutationLocks([parent.hash]);
		let entered!: () => void;
		const entering = new Promise<void>((resolve) => {
			entered = resolve;
		});
		const acquire = index.acquireExclusiveMutationLockMaybe.bind(index);
		const flush = index.flushPendingWrites.bind(index);
		const probes = sinon.createSandbox();
		let nativeOwner: EntryIndexHashMutationLockOwner | undefined;
		let checkedPendingFlush = false;
		probes.stub(index, "acquireExclusiveMutationLockMaybe").callsFake(() => {
			const pending = acquire();
			entered();
			return Promise.resolve(pending).then((owner) => {
				nativeOwner = owner;
				return owner;
			});
		});
		probes
			.stub(index, "flushPendingWrites")
			.callsFake(async (hashes, owner) => {
				if (nativeOwner && (index as any).pendingIndexWrites.has(parent.hash)) {
					// Fail deterministically instead of parking teardown behind a reentrant lease.
					expect(owner).to.equal(nativeOwner);
					checkedPendingFlush = true;
				}
				await flush(hashes, owner);
			});
		let pending: Promise<unknown> | undefined;
		try {
			const append = log.append(Uint8Array.of(2));
			pending = append;
			await entering;
			await index.put(parent, {
				unique: true,
				isHead: true,
				toMultiHash: false,
				deferIndexWrite: true,
				hashMutationLockOwner: held,
			});
			(index as any).clearPendingIndexFlushTimer();
			expect(await index.properties.index.getSize()).to.equal(0);
			index.releaseHashMutationLocks(held);
			held = undefined;
			const { entry } = await append;
			expect(checkedPendingFlush).to.equal(true);
			expect(entry.meta.next).to.deep.equal([parent.hash]);
			expect(log.length).to.equal(2);
		} finally {
			if (held) index.releaseHashMutationLocks(held);
			await pending?.catch(() => {});
			probes.restore();
		}
	});
});
