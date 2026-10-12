import { createStore } from "@peerbit/any-store";
import { AnyBlockStore } from "@peerbit/blocks";
import { HashmapIndices } from "@peerbit/indexer-simple";
import { create as createSQLite } from "@peerbit/indexer-sqlite3";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import sinon from "sinon";
import type { EntryIndexHashMutationLockOwner } from "../src/entry-index.js";
import { Log } from "../src/log.js";
import { signKey } from "./fixtures/privateKey.js";

const deferred = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => (resolve = done));
	return { promise, resolve };
};

// Exact completion only: callers still own authentication, retained redo
// evidence, projection, admission fences and durability barriers.
describe("delete exact completion", () => {
	for (const nativeGraph of [false, true]) {
		describe(nativeGraph ? "native graph" : "JS graph", () => {
			let store: AnyBlockStore;
			let log: Log<Uint8Array>;
			beforeEach(async () => {
				store = new AnyBlockStore();
				await store.start();
				log = new Log();
				await log.open(store, signKey, {
					indexer: new HashmapIndices(),
					nativeGraph,
				});
			});
			afterEach(async () => {
				sinon.restore();
				await log.close();
				await store.stop();
			});

			it("completes absent-row blocks and coupled metadata without recursive deletion", async () => {
				const parent = (await log.append(Uint8Array.of(1))).entry;
				const victim = (
					await log.append(Uint8Array.of(2), { meta: { next: [parent] } })
				).entry;
				const survivor = (
					await log.append(Uint8Array.of(3), { meta: { next: [victim] } })
				).entry;
				const index = log.entryIndex;
				const targets = [victim.toShallow(false), victim.toShallow(false)];
				const coordinates = new Set([victim.hash, parent.hash, survivor.hash]);
				const complete = sinon.spy(
					async (nodes: readonly { hash: string }[]) => {
						for (const node of nodes) {
							expect(await store.has(node.hash)).equal(false);
							coordinates.delete(node.hash);
						}
					},
				);
				index.properties.onDeleteCommitted = complete;
				const failure = new Error("physical block removal interrupted");
				const remove = sinon.stub(store, "rmMany").rejects(failure);
				const owner = await index.acquireExclusiveMutationLock();
				try {
					expect(
						await index.completeExactDelete(targets, owner).catch((e) => e),
					).equal(failure);
				} finally {
					index.releaseHashMutationLocks(owner);
					remove.restore();
				}
				expect(await index.properties.index.getSize()).equal(2);
				expect(log.length).equal(2);
				expect(await store.has(victim.hash)).equal(true);
				expect(complete.callCount).equal(0);
				expect(
					await index.acquireExclusiveMutationLock().catch((e) => e),
				).to.have.property("cause", failure);
				for (let repeat = 0; repeat < 2; repeat++) {
					await index.withExclusiveMutationRecovery((recoveryOwner) =>
						index.completeExactDelete(targets, recoveryOwner),
					);
					expect(await index.properties.index.getSize()).equal(2);
					expect(log.length).equal(2);
					expect(await store.has(victim.hash)).equal(false);
					expect(await log.has(victim.hash)).equal(false);
					expect(await log.has(parent.hash)).equal(true);
					expect(await log.has(survivor.hash)).equal(true);
					expect((await log.getShallow(parent.hash))?.head).equal(true);
				}
				expect(complete.callCount).equal(2);
				expect(complete.firstCall.args[0]).to.have.length(1);
				expect([...coordinates]).deep.equal([parent.hash, survivor.hash]);
				// Finishing deletion does not independently authorize clearing poison.
				expect(
					await index.acquireExclusiveMutationLock().catch((e) => e),
				).to.have.property("cause", failure);
			});

			it("requires a live exclusive owner before mutating anything", async () => {
				const victim = (await log.append(Uint8Array.of(1))).entry;
				const index = log.entryIndex;
				const targets = [victim.toShallow(true)];
				const shared = await index.acquireHashMutationLocks([victim.hash]);
				try {
					expect(
						await index.completeExactDelete(targets, shared).catch((e) => e),
					)
						.to.have.property("message")
						.that.includes("exclusive");
				} finally {
					index.releaseHashMutationLocks(shared);
				}
				const released = await index.acquireExclusiveMutationLock();
				index.releaseHashMutationLocks(released);
				for (const invalid of [
					released,
					{} as EntryIndexHashMutationLockOwner,
				]) {
					expect(
						await index.completeExactDelete(targets, invalid).catch((e) => e),
					)
						.to.have.property("message")
						.that.includes("Invalid or released");
				}
				expect(await log.has(victim.hash)).equal(true);
				expect(await store.has(victim.hash)).equal(true);
				const valid = await index.acquireExclusiveMutationLock();
				index.releaseHashMutationLocks(valid);
			});

			it("copies targets before awaiting and keeps completion behind physical removal", async () => {
				const victim = (await log.append(Uint8Array.of(1))).entry;
				const other = (
					await log.append(Uint8Array.of(2), { meta: { next: [] } })
				).entry;
				const index = log.entryIndex;
				const target = victim.toShallow(true);
				const started = deferred();
				const release = deferred();
				const original = store.rmMany.bind(store);
				sinon.stub(store, "rmMany").callsFake(async (hashes) => {
					started.resolve();
					await release.promise;
					return original(hashes);
				});
				const complete = sinon.spy();
				index.properties.onDeleteCommitted = complete;
				const owner = await index.acquireExclusiveMutationLock();
				const deletion = index.completeExactDelete([target], owner);
				let competingOwner: EntryIndexHashMutationLockOwner | undefined;
				const waiting = index.acquireExclusiveMutationLock().then((next) => {
					competingOwner = next;
				});
				try {
					target.hash = other.hash;
					target.meta.next.push(other.hash);
					await started.promise;
					expect(complete.callCount).equal(0);
					expect(competingOwner).equal(undefined);
					expect(index.captureMutationGeneration()).equal(undefined);
					release.resolve();
					await deletion;
					expect(complete.firstCall.args[0][0].hash).equal(victim.hash);
					expect(complete.firstCall.args[0][0].meta.next).deep.equal([]);
					expect(await store.has(other.hash)).equal(true);
					expect(competingOwner).equal(undefined);
				} finally {
					release.resolve();
					await deletion;
					index.releaseHashMutationLocks(owner);
					await waiting;
					index.releaseHashMutationLocks(competingOwner!);
				}
			});

			it("finishes orphan cleanup after an ambiguous row deletion and disk reopen", async () => {
				await log.close();
				await store.stop();
				const directory = await mkdtemp(
					join(tmpdir(), "peerbit-exact-delete-"),
				);
				try {
					store = new AnyBlockStore(createStore(join(directory, "blocks")));
					await store.start();
					log = new Log();
					const open = async () =>
						log.open(store, signKey, {
							indexer: await createSQLite(join(directory, "index")),
							nativeGraph,
						});
					await open();
					const victim = (await log.append(Uint8Array.of(1))).entry;
					const target = victim.toShallow(true);
					const index = log.entryIndex;
					const failure = new Error(
						"row removed before storage acknowledgment failed",
					);
					const underlying = index.properties.index;
					const del = underlying.del.bind(underlying);
					const interrupted = sinon
						.stub(underlying, "del")
						.callsFake(async (query) => {
							await del(query);
							throw failure;
						});
					const owner = await index.acquireExclusiveMutationLock();
					try {
						expect(
							await index.completeExactDelete([target], owner).catch((e) => e),
						).equal(failure);
					} finally {
						index.releaseHashMutationLocks(owner);
						interrupted.restore();
					}
					expect(await index.properties.index.getSize()).equal(0);
					expect(await store.has(victim.hash)).equal(true);
					const logId = new Uint8Array(log.id);
					await log.close();
					await store.stop();
					store = new AnyBlockStore(createStore(join(directory, "blocks")));
					await store.start();
					log = new Log({ id: logId });
					await open();
					const complete = sinon.spy();
					log.entryIndex.properties.onDeleteCommitted = complete;
					await log.entryIndex.withExclusiveMutationRecovery((recoveryOwner) =>
						log.entryIndex.completeExactDelete([target], recoveryOwner),
					);
					expect(log.length).equal(0);
					expect(await store.has(victim.hash)).equal(false);
					expect(complete.callCount).equal(1);
					expect(complete.firstCall.args[0][0].hash).equal(victim.hash);
				} finally {
					await log.close();
					await store.stop();
					await rm(directory, { recursive: true, force: true });
				}
			});
		});
	}
});
