import { AnyBlockStore, type BlockStore } from "@peerbit/blocks";
import { expect } from "chai";
import { EntryType } from "../src/entry-type.js";
import { Entry } from "../src/entry.js";
import { Log } from "../src/log.js";
import { signKey } from "./fixtures/privateKey.js";
import { JSON_ENCODING } from "./utils/encoding.js";

describe("delete", function () {
	let store: BlockStore;
	const deferred = () => {
		let resolve!: () => void;
		const promise = new Promise<void>((done) => {
			resolve = done;
		});
		return { promise, resolve };
	};

	beforeEach(async () => {
		store = new AnyBlockStore();
		await store.start();
	});

	afterEach(async () => {
		await store.stop();
	});

	const blockExists = async (hash: string): Promise<boolean> => {
		try {
			return !!(await store.get(hash, { remote: { timeout: 3000 } }));
		} catch (error) {
			return false;
		}
	};

	describe("in-flight reads", () => {
		it("does not republish a deleted entry after a single store read", async () => {
			const log = new Log<Uint8Array>();
			await log.open(store, signKey);
			const { entry } = await log.append(new Uint8Array([1]));
			(log.entryIndex as any).cache.clear();

			const readStarted = deferred();
			const releaseRead = deferred();
			const originalGet = store.get.bind(store);
			let intercepted = false;
			store.get = async (cid, options) => {
				const value = await originalGet(cid, options);
				if (cid === entry.hash && !intercepted) {
					intercepted = true;
					readStarted.resolve();
					await releaseRead.promise;
				}
				return value;
			};

			let inFlight: Promise<Entry<Uint8Array> | undefined> | undefined;
			try {
				inFlight = log.get(entry.hash);
				await readStarted.promise;
				await log.delete(entry.hash);
				releaseRead.resolve();
				expect((await inFlight)?.hash).to.equal(entry.hash);
			} finally {
				releaseRead.resolve();
				store.get = originalGet;
				await inFlight;
			}

			expect(await log.get(entry.hash)).to.equal(undefined);
		});

		it("does not publish a store read started during deletion", async () => {
			const log = new Log<Uint8Array>();
			await log.open(store, signKey);
			const { entry } = await log.append(new Uint8Array([1]));

			const removeStarted = deferred();
			const releaseRemove = deferred();
			const originalRm = store.rm.bind(store);
			let intercepted = false;
			store.rm = async (cid) => {
				if (cid === entry.hash && !intercepted) {
					intercepted = true;
					removeStarted.resolve();
					await releaseRemove.promise;
				}
				return originalRm(cid);
			};

			let deletion: Promise<unknown> | undefined;
			try {
				deletion = log.delete(entry.hash);
				await removeStarted.promise;
				const resolvedDuringDelete = await log.get(entry.hash);
				expect(resolvedDuringDelete?.hash).to.equal(entry.hash);
				expect((log.entryIndex as any).cache.get(entry.hash)).to.equal(
					undefined,
				);
				releaseRemove.resolve();
				await deletion;
			} finally {
				releaseRemove.resolve();
				store.rm = originalRm;
				await deletion;
			}

			expect(await log.get(entry.hash)).to.equal(undefined);
		});

		it("does not publish batched reads started during bulk deletion", async () => {
			const log = new Log<Uint8Array>();
			await log.open(store, signKey);
			const { entry: firstDeleted } = await log.append(new Uint8Array([1]));
			const { entry: secondDeleted } = await log.append(new Uint8Array([2]));
			const { entry: retained } = await log.append(new Uint8Array([3]));
			const firstShallow = (await log.getShallow(firstDeleted.hash))!;
			const secondShallow = (await log.getShallow(secondDeleted.hash))!;
			(log.entryIndex as any).cache.clear();

			const removeStarted = deferred();
			const releaseRemove = deferred();
			const originalRmMany = store.rmMany.bind(store);
			let intercepted = false;
			store.rmMany = async (cids) => {
				if (cids.includes(firstDeleted.hash) && !intercepted) {
					intercepted = true;
					removeStarted.resolve();
					await releaseRemove.promise;
				}
				return originalRmMany(cids);
			};

			let deletion: Promise<unknown> | undefined;
			try {
				deletion = log.entryIndex.deleteMany([firstShallow, secondShallow], {
					skipNextHeadUpdates: true,
				});
				await removeStarted.promise;
				const resolved = await log.entryIndex.getMany(
					[firstDeleted.hash, secondDeleted.hash, retained.hash],
					{ type: "full", ignoreMissing: true },
				);
				expect(resolved.map((entry) => entry?.hash)).to.deep.equal([
					firstDeleted.hash,
					secondDeleted.hash,
					retained.hash,
				]);
				expect((log.entryIndex as any).cache.get(firstDeleted.hash)).to.equal(
					undefined,
				);
				expect((log.entryIndex as any).cache.get(secondDeleted.hash)).to.equal(
					undefined,
				);
				expect((log.entryIndex as any).cache.get(retained.hash)).to.equal(
					resolved[2],
				);
				releaseRemove.resolve();
				await deletion;
			} finally {
				releaseRemove.resolve();
				store.rmMany = originalRmMany;
				await deletion;
			}

			expect(await log.get(firstDeleted.hash)).to.equal(undefined);
			expect(await log.get(secondDeleted.hash)).to.equal(undefined);
			expect(await log.get(retained.hash)).to.exist;
		});

		it("does not publish a batched read invalidated by native trim", async () => {
			const log = new Log<Uint8Array>();
			await log.open(store, signKey);
			const { entry: trimmedEntry } = await log.append(new Uint8Array([1]));
			const { entry: retainedEntry } = await log.append(new Uint8Array([2]));
			(log.entryIndex as any).cache.clear();

			const readStarted = deferred();
			const releaseRead = deferred();
			const originalGetMany = store.getMany.bind(store);
			let intercepted = false;
			store.getMany = async (cids, options) => {
				const values = await originalGetMany(cids, options);
				if (cids.includes(trimmedEntry.hash) && !intercepted) {
					intercepted = true;
					readStarted.resolve();
					await releaseRead.promise;
				}
				return values;
			};

			let inFlight: Promise<Array<Entry<Uint8Array> | undefined>> | undefined;
			try {
				inFlight = log.entryIndex.getMany(
					[trimmedEntry.hash, retainedEntry.hash],
					{ type: "full", ignoreMissing: true },
				);
				await readStarted.promise;
				const consumed =
					log.entryIndex.consumeNativeTrimmedEntryHashesNoReturnMaybe(
						[trimmedEntry.hash],
						{ skipNextHeadUpdates: true, deleteBlocks: false },
					);
				expect(consumed).not.to.equal(undefined);
				expect(await consumed).to.equal(true);
				releaseRead.resolve();
				const resolved = await inFlight;
				expect(resolved.map((entry) => entry?.hash)).to.deep.equal([
					trimmedEntry.hash,
					retainedEntry.hash,
				]);
				expect((log.entryIndex as any).cache.get(trimmedEntry.hash)).to.equal(
					undefined,
				);
				expect((log.entryIndex as any).cache.get(retainedEntry.hash)).to.equal(
					resolved[1],
				);
			} finally {
				releaseRead.resolve();
				store.getMany = originalGetMany;
				await inFlight;
			}

			expect((log.entryIndex as any).activeCacheInvalidations.size).to.equal(0);
			expect((log.entryIndex as any).inFlightCachePublications.size).to.equal(
				0,
			);
		});

		it("keeps overlapping cache invalidations active until all settle", async () => {
			const log = new Log<Uint8Array>();
			await log.open(store, signKey);
			const { entry } = await log.append(new Uint8Array([1]));
			const shallow = (await log.getShallow(entry.hash))!;
			(log.entryIndex as any).cache.clear();

			const guardStarted = [deferred(), deferred()];
			const releaseGuard = [deferred(), deferred()];
			const withCacheInvalidation = (
				log.entryIndex as any
			).withCacheInvalidation.bind(log.entryIndex);

			let firstGuard: Promise<unknown> | undefined;
			let secondGuard: Promise<unknown> | undefined;
			try {
				firstGuard = withCacheInvalidation([entry.hash], async () => {
					guardStarted[0]!.resolve();
					await releaseGuard[0]!.promise;
				});
				await guardStarted[0]!.promise;
				secondGuard = withCacheInvalidation([entry.hash], async () => {
					guardStarted[1]!.resolve();
					await releaseGuard[1]!.promise;
					await log.entryIndex.delete(entry.hash, shallow);
				});
				await guardStarted[1]!.promise;
				expect(
					(log.entryIndex as any).activeCacheInvalidations.get(entry.hash),
				).to.equal(2);

				releaseGuard[0]!.resolve();
				await firstGuard;
				expect(
					(log.entryIndex as any).activeCacheInvalidations.get(entry.hash),
				).to.equal(1);

				const resolvedDuringDelete = await log.get(entry.hash);
				expect(resolvedDuringDelete?.hash).to.equal(entry.hash);
				expect((log.entryIndex as any).cache.get(entry.hash)).to.equal(
					undefined,
				);

				releaseGuard[1]!.resolve();
				await secondGuard;
			} finally {
				for (const release of releaseGuard) {
					release.resolve();
				}
				await Promise.allSettled([firstGuard, secondGuard]);
			}

			expect((log.entryIndex as any).activeCacheInvalidations.size).to.equal(0);
			expect((log.entryIndex as any).inFlightCachePublications.size).to.equal(
				0,
			);
			expect(await log.get(entry.hash)).to.equal(undefined);
		});

		it("serializes same-hash deletions without publishing reads into the cache", async () => {
			const log = new Log<Uint8Array>();
			logs.add(log);
			await log.open(store, signKey);
			const { entry } = await log.append(new Uint8Array([1]));
			const shallow = (await log.getShallow(entry.hash))!;
			(log.entryIndex as any).cache.clear();

			const removeStarted = deferred();
			const releaseRemove = deferred();
			const secondAdmission = deferred();
			const originalRm = store.rm.bind(store);
			const originalAcquire = log.entryIndex.acquireHashMutationLocks.bind(
				log.entryIndex,
			);
			let admissions = 0;
			let removeCalls = 0;
			let secondAcquired = false;
			log.entryIndex.acquireHashMutationLocks = (hashes) => {
				const pending = originalAcquire(hashes);
				if (++admissions === 2) {
					secondAdmission.resolve();
					return pending.then((owner) => {
						secondAcquired = true;
						return owner;
					});
				}
				return pending;
			};
			store.rm = async (cid) => {
				if (cid === entry.hash && ++removeCalls === 1) {
					removeStarted.resolve();
					await releaseRemove.promise;
				}
				return originalRm(cid);
			};

			let firstDeletion: Promise<unknown> | undefined;
			let secondDeletion: Promise<unknown> | undefined;
			try {
				firstDeletion = log.entryIndex.delete(entry.hash, shallow);
				await removeStarted.promise;
				secondDeletion = log.entryIndex.delete(entry.hash, shallow);
				// A missing lease must fail the assertions, not strand this gate.
				await Promise.race([secondAdmission.promise, secondDeletion]);
				expect(removeCalls).to.equal(1);
				expect((await log.get(entry.hash))?.hash).to.equal(entry.hash);
				expect((log.entryIndex as any).cache.get(entry.hash)).to.equal(
					undefined,
				);
				expect(secondAcquired).to.equal(false);
				expect(removeCalls).to.equal(1);
				releaseRemove.resolve();
				await Promise.all([firstDeletion, secondDeletion]);
				expect(secondAcquired).to.equal(true);
				expect(removeCalls).to.equal(2);
			} finally {
				releaseRemove.resolve();
				await Promise.allSettled([firstDeletion, secondDeletion]);
				store.rm = originalRm;
				log.entryIndex.acquireHashMutationLocks = originalAcquire;
			}

			expect(log.length).to.equal(0);
			expect(await log.has(entry.hash)).to.equal(false);
			expect(await log.get(entry.hash)).to.equal(undefined);
			expect((log.entryIndex as any).cache.get(entry.hash)).to.equal(undefined);
			expect((log.entryIndex as any).activeCacheInvalidations.size).to.equal(0);
			expect((log.entryIndex as any).inFlightCachePublications.size).to.equal(
				0,
			);
		});

		it("only skips deleted entries when publishing a batched store read", async () => {
			const log = new Log<Uint8Array>();
			await log.open(store, signKey);
			const { entry: deletedEntry } = await log.append(new Uint8Array([1]));
			const { entry: retainedEntry } = await log.append(new Uint8Array([2]));
			(log.entryIndex as any).cache.clear();

			const readStarted = deferred();
			const releaseRead = deferred();
			const originalGetMany = store.getMany.bind(store);
			let intercepted = false;
			store.getMany = async (cids, options) => {
				const values = await originalGetMany(cids, options);
				if (cids.includes(deletedEntry.hash) && !intercepted) {
					intercepted = true;
					readStarted.resolve();
					await releaseRead.promise;
				}
				return values;
			};

			let inFlight: Promise<Array<Entry<Uint8Array> | undefined>> | undefined;
			try {
				inFlight = log.entryIndex.getMany(
					[deletedEntry.hash, retainedEntry.hash],
					{ type: "full", ignoreMissing: true },
				);
				await readStarted.promise;
				await log.delete(deletedEntry.hash);
				releaseRead.resolve();
				const resolved = await inFlight;
				expect(resolved.map((entry) => entry?.hash)).to.deep.equal([
					deletedEntry.hash,
					retainedEntry.hash,
				]);
				expect(await log.get(retainedEntry.hash)).to.equal(resolved[1]);
			} finally {
				releaseRead.resolve();
				store.getMany = originalGetMany;
				await inFlight;
			}

			expect(await log.get(deletedEntry.hash)).to.equal(undefined);
		});
	});

	describe("deleteRecursively", () => {
		it("deleted unreferences", async () => {
			const log = new Log();
			await log.open(store, signKey);
			const { entry: e1 } = await log.append(new Uint8Array([1]));
			const { entry: e2 } = await log.append(new Uint8Array([2]));
			const { entry: e3 } = await log.append(new Uint8Array([3]));

			await log.deleteRecursively(e2);
			expect((await log.toArray()).length).equal(1);
			expect(await log.get(e1.hash)).equal(undefined);
			expect(await blockExists(e1.hash)).to.be.false;
			expect(await log.get(e2.hash)).equal(undefined);
			expect(await blockExists(e2.hash)).to.be.false;
			expect(await log.get(e3.hash)).to.exist;
			expect(await blockExists(e3.hash)).to.be.true;

			await log.deleteRecursively(e3);
			expect((await log.toArray()).length).equal(0);
			expect(await log.getHeads().all()).to.be.empty;
		});

		it("processes as long as allowed", async () => {
			const log = new Log();
			await log.open(store, signKey, { encoding: JSON_ENCODING });
			const { entry: e1 } = await log.append(new Uint8Array([1]));
			const { entry: e2 } = await log.append("hello2a");
			const { entry: e2b } = await log.append("hello2b", {
				meta: { next: [e2] },
			});
			const { entry: e3 } = await log.append(new Uint8Array([3]), {
				meta: {
					next: [e2],
					type: EntryType.CUT,
				},
			});
			expect(await log.toArray()).to.have.length(4); // will still have lengrt 4 because e2 references 2eb which is not a CUT
			await log.deleteRecursively(e2b);
			expect((await log.toArray()).map((x) => x.hash)).to.deep.equal([e3.hash]);
			expect(await log.get(e1.hash)).equal(undefined);
			expect(await blockExists(e1.hash)).to.be.false;
			expect(await log.get(e2.hash)).equal(undefined);
			expect(await blockExists(e2.hash)).to.be.false;
			expect(await log.get(e3.hash)).to.exist;
			expect(await blockExists(e3.hash)).to.be.true;

			await log.deleteRecursively(e3);
			expect((await log.toArray()).length).equal(0);
			expect(await log.getHeads().all()).to.be.empty;
		});

		it("keeps references", async () => {
			const log = new Log();
			await log.open(store, signKey, { encoding: JSON_ENCODING });
			const { entry: e1 } = await log.append(new Uint8Array([1]));
			const { entry: e2a } = await log.append("hello2a", {
				meta: { next: [e1] },
			});
			const { entry: e2b } = await log.append("hello2b", {
				meta: { next: [e1] },
			});

			await log.deleteRecursively(e2a);
			expect((await log.toArray()).length).equal(2);
			expect(await log.get(e1.hash)).to.exist;
			expect(await blockExists(e1.hash)).to.be.true;
			expect(await log.get(e2a.hash)).equal(undefined);
			expect(await blockExists(e2a.hash)).to.be.false;
			expect(await log.get(e2b.hash)).to.exist;
			expect(await blockExists(e2b.hash)).to.be.true;
			await log.deleteRecursively(e2b);
			expect((await log.toArray()).length).equal(0);
			expect(await log.getHeads().all()).to.be.empty;
		});
	});

	describe("remove", () => {
		it("can resolve the full entry from deleted", async () => {
			const log = new Log();
			let deleted: number = 0;

			await log.open(store, signKey, {
				encoding: JSON_ENCODING,
				onChange: async (change) => {
					if (change.removed.length > 0) {
						deleted += change.removed.length;
					}
				},
			});
			const { entry: e1 } = await log.append(new Uint8Array([1]));

			await log.remove(e1);
			await log.remove(e1);

			expect(deleted).to.equal(1);
		});

		it("if already removed no change", async () => {
			const log = new Log();
			let deleted: number = 0;

			await log.open(store, signKey, {
				encoding: JSON_ENCODING,
				onChange: async (change) => {
					if (change.removed.length > 0) {
						// try to resolve the full entry
						await Promise.all(
							change.removed.map(async (e) => {
								const entry = await log.get(e.hash);
								expect(entry).to.exist;
								expect(entry!.hash).to.equal(e.hash);
								expect(entry).to.be.instanceOf(Entry);
							}),
						);

						deleted += change.removed.length;
					}
				},
			});
			const { entry: _e1 } = await log.append(new Uint8Array([2]));
			const { entry: e2 } = await log.append(new Uint8Array([3]));

			await log.remove(e2);

			expect(deleted).to.equal(1);
		});

		it("concurrently after join", async () => {
			const log1 = new Log();
			const log2 = new Log();
			await log1.open(store, signKey, { encoding: JSON_ENCODING });
			await log2.open(store, signKey, { encoding: JSON_ENCODING });

			const { entry: e1 } = await log2.append(new Uint8Array([1]), {
				meta: { next: [] },
			});
			const { entry: e2 } = await log2.append(new Uint8Array([2]), {
				meta: { next: [] },
			});

			await log1.join(log2);
			expect((await log1.toArray()).map((x) => x.hash)).to.deep.equal([
				e1.hash,
				e2.hash,
			]);
			expect(log1.length).to.equal(2);
			const p1 = log1.remove(e1, { recursively: true });
			const p2 = log1.remove(e2, { recursively: true });
			await Promise.all([p1, p2]);
			expect((await log1.toArray()).map((x) => x.hash)).to.be.empty;
			expect(log1.length).to.equal(0);
		});

		it("concurrently delete same after join", async () => {
			const log1 = new Log();
			const log2 = new Log();
			await log1.open(store, signKey, { encoding: JSON_ENCODING });
			await log2.open(store, signKey, { encoding: JSON_ENCODING });

			const { entry: e1 } = await log2.append(new Uint8Array([1]), {
				meta: { next: [] },
			});

			await log1.join(log2);
			expect((await log1.toArray()).map((x) => x.hash)).to.deep.equal([
				e1.hash,
			]);
			expect(log1.length).to.equal(1);
			const p1 = log1.remove(e1, { recursively: true });
			const p2 = log1.remove(e1, { recursively: true });
			await Promise.all([p1, p2]);
			expect((await log1.toArray()).map((x) => x.hash)).to.be.empty;
			expect(log1.length).to.equal(0);
		});
	});

	describe("delete", () => {
		it("updates for new heads", async () => {
			const log = new Log();
			await log.open(store, signKey, { encoding: JSON_ENCODING });
			const { entry: e1 } = await log.append(new Uint8Array([2]));
			const { entry: e2 } = await log.append(new Uint8Array([3]));
			const { entry: e2b } = await log.append(new Uint8Array([3]), {
				meta: { next: [e1] },
			});

			// log have 2 heads e2 and e2b, delete e2 and e2b should be the only head

			expect((await log.getHeads().all()).map((x) => x.hash)).to.deep.equal([
				e2.hash,
				e2b.hash,
			]);
			await log.delete(e2.hash);
			const newHeads = await log.getHeads().all();
			expect(newHeads.map((x) => x.hash)).to.deep.equal([e2b.hash]);
		});
	});
});
