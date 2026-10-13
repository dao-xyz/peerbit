import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { Ed25519Keypair } from "@peerbit/crypto";
import { type Index, StringKey, toId } from "@peerbit/indexer-interface";
import { create as createSQLite } from "@peerbit/indexer-sqlite3";
import {
	Entry,
	EntryType,
	EntryV0,
	LamportClock,
	Log,
	Timestamp,
} from "@peerbit/log";
import { SharedLog } from "@peerbit/shared-log";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pDefer from "p-defer";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import type { Operation } from "../src/operation.js";
import { Documents } from "../src/program.js";
import { createProofCommitCoordinator } from "../src/proof-commit.js";
import {
	type ProofDeleteContract,
	deriveProofDeleteScope,
} from "../src/proof-delete-manifest.js";
import { createProofEntryOwner } from "../src/proof-entry-owner.js";
import {
	BORSH_ENCODING_PROOF_OPERATION,
	ProofDeleteOperation,
	type ProofOperation,
	ProofPutOperation,
	deriveProofKeyGid,
} from "../src/proof-operations.js";
import { DocumentIndex, type OpenOptions } from "../src/search.js";
import { Document } from "./data.js";

@variant(0)
class Coordinate {
	@field({ type: "string" })
	hash: string;
	constructor(hash: string) {
		this.hash = hash;
	}
}

// Real disk blocks, lower index and DocumentIndex projection. The fixture owns
// coordinate callbacks explicitly; no public Documents profile is activated.
describe("private proof commit coordinator", () => {
	for (const nativeGraph of [false, true]) {
		describe(nativeGraph ? "native graph" : "JS graph", () => {
			let directory: string;
			let peer: Peerbit;
			let identity: Ed25519Keypair;
			let log: Log<ProofOperation>;
			let store: Parameters<Log<ProofOperation>["open"]>[0];
			let projection: DocumentIndex<Document, Document, any>;
			let coordinates: Index<Coordinate>;
			let contract: ProofDeleteContract;
			let scope: Uint8Array;
			let coordinator: ReturnType<typeof createProofCommitCoordinator>;
			let admissions: number;
			let poison: unknown;
			let generation: object;
			let beforeProject: (() => Promise<void>) | undefined;
			let afterProject: (() => void) | undefined;
			let validatePut: Parameters<
				typeof createProofCommitCoordinator
			>[0]["validatePut"];
			const open = async () => {
				peer = await Peerbit.create({ directory });
				// SharedLog uses this real RemoteBlocks boundary. DirectBlock delegates
				// storage but does not itself expose the local crash-safe capability.
				store = (
					peer.services.blocks as unknown as { remoteBlocks: typeof store }
				).remoteBlocks;
				log = new Log({ id: new Uint8Array(32).fill(96) });
				await log.open(store, identity, {
					encoding: BORSH_ENCODING_PROOF_OPERATION,
					indexer: await createSQLite(join(directory, "proof-lower")),
					nativeGraph,
					appendDurability: "strict",
				});
				coordinates = await (
					await peer.indexer.scope("proof-coordinates")
				).init({
					schema: Coordinate,
					indexBy: ["hash"],
				});
				projection = new DocumentIndex();
				await projection.beforeOpen(peer);
				const wrapper = new SharedLog<ProofOperation>();
				wrapper.log = log;
				const properties: OpenOptions<Document, Document, any> = {
					log: wrapper as unknown as SharedLog<Operation>,
					documentType: Document,
					dbType: Documents,
					documentEvents: new EventTarget() as any,
					replicate: async () => {},
					maybeOpen: async (value) => value,
				};
				await (
					projection as unknown as {
						openWithDeferredQuery(args: typeof properties): Promise<unknown>;
					}
				).openWithDeferredQuery(properties);
				admissions = 0;
				poison = undefined;
				beforeProject = undefined;
				afterProject = undefined;
				validatePut = undefined;
				const captured = (generation = {});
				coordinator = createProofCommitCoordinator({
					log,
					contract,
					assertActive: () => {
						if (captured !== generation || log.closed || poison)
							throw new Error("Inactive proof owner");
					},
					beginAdmission: () => {
						admissions++;
						return {
							poison: (error) => {
								poison = error;
							},
							release: () => {
								admissions--;
							},
						};
					},
					validatePut: (fact) => validatePut?.(fact),
					prepareCoordinates: async (entry) => ({
						complete: async ({ removed }) => {
							await coordinates.put(new Coordinate(entry.hash));
							for (const target of removed)
								await coordinates.del({ query: { hash: target.hash } });
						},
						barrier: async () => {
							await coordinates.crashSafeDurability!.barrier();
							await projection.index.crashSafeDurability!.barrier();
						},
						release: () => {},
					}),
					project: async ({ key, winner }) => {
						await beforeProject?.();
						if (!winner) {
							await projection.del(key);
							return afterProject;
						}
						const entry = deserialize(
							winner.bytes,
							Entry,
						) as Entry<ProofOperation>;
						entry.hash = winner.hash;
						entry.init({ encoding: BORSH_ENCODING_PROOF_OPERATION });
						const value = deserialize(winner.data, Document);
						await projection.put(
							value,
							key,
							entry as unknown as Entry<Operation>,
							undefined,
						);
						return afterProject;
					},
				});
			};
			const close = async () => {
				await projection?.close();
				await log?.close();
				await peer?.stop();
			};
			beforeEach(async () => {
				directory = await mkdtemp(join(tmpdir(), "peerbit-proof-commit-"));
				identity = await Ed25519Keypair.create();
				contract = {
					version: 1,
					nonce: new Uint8Array(32).fill(9),
					owner: identity.publicKey.publicKey,
					keyKind: 0,
				};
				scope = deriveProofDeleteScope(contract);
				await open();
			});
			afterEach(async () => {
				sinon.restore();
				await close();
				await rm(directory, { recursive: true, force: true });
			});
			const make = async (
				operation: ProofPutOperation | ProofDeleteOperation,
				next: Entry<ProofOperation>[],
				wallTime: bigint,
			) => {
				const entry = await (
					EntryV0 as unknown as {
						createWithExplicitGid<T>(
							properties: Parameters<typeof EntryV0.create<T>>[0],
							gid: string,
						): Promise<Entry<T>>;
					}
				).createWithExplicitGid(
					{
						store,
						identity,
						encoding: BORSH_ENCODING_PROOF_OPERATION,
						data: operation,
						deferStore: true,
						meta: {
							type:
								operation instanceof ProofDeleteOperation
									? EntryType.CUT
									: EntryType.APPEND,
							next,
							clock: new LamportClock({
								id: identity.publicKey.bytes,
								timestamp: new Timestamp({ wallTime }),
							}),
						},
					},
					deriveProofKeyGid(scope, operation.key),
				);
				return {
					entry,
					block: {
						cid: entry.hash,
						bytes: Entry.getPreparedStorageBytes(entry)!,
					},
				};
			};
			const put = (
				key: string,
				wallTime: bigint,
				next: Entry<ProofOperation>[] = [],
			) =>
				make(
					new ProofPutOperation({
						scope,
						key: new StringKey(key),
						data: serialize(new Document({ id: key, name: String(wallTime) })),
					}),
					next,
					wallTime,
				);
			const deletion = async (
				target: Awaited<ReturnType<typeof put>>,
				witnesses: Awaited<ReturnType<typeof put>>[],
				targets: Awaited<ReturnType<typeof put>>[],
				wallTime: bigint,
			) => {
				const operation =
					(await target.entry.getPayloadValue()) as ProofPutOperation;
				return make(
					new ProofDeleteOperation({
						scope,
						key: operation.key,
						target: target.entry.hash,
						witnesses: witnesses.map((item) => item.block.bytes),
					}),
					targets
						.map((item) => item.entry)
						.sort((a, b) => (a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0)),
					wallTime,
				);
			};
			const projected = async (key: string) =>
				(await projection.index.get(toId(key)))?.value;
			const installOwner = (traversalLimits?: {
				entries: number;
				bytes: number;
			}) =>
				createProofEntryOwner({
					log,
					contract,
					coordinator,
					traversalLimits,
					assertActive: () => {
						if (log.closed || poison) throw new Error("Inactive proof owner");
					},
				});

			it("validates every ordinary PUT before writes even when it cannot win", async () => {
				const winner = await put("a", 10n);
				await coordinator.put(winner.block);
				const invalid = await make(
					new ProofPutOperation({
						scope,
						key: new StringKey("a"),
						data: serialize(new Document({ id: "other" })),
					}),
					[],
					1n,
				);
				validatePut = (fact) => {
					if (deserialize(fact.data, Document).id !== fact.key.key)
						throw new Error("Decoded id differs from signed key");
				};
				const writes = sinon.spy(store, "put");
				const evidence = sinon.spy();
				await expect(
					coordinator.put(invalid.block, { onLocalCommit: evidence }),
				).rejectedWith("Decoded id differs from signed key");
				expect(writes.called).equal(false);
				expect(evidence.called).equal(false);
				expect(await log.has(invalid.entry.hash)).equal(false);
				expect((await projected("a"))?.__context.head).equal(winner.entry.hash);
				expect(admissions).equal(0);
				expect(poison).equal(undefined);
			});

			it("validates older nonwinning indexed rows during recovery", async () => {
				const winner = await put("a", 10n);
				await coordinator.put(winner.block);
				const invalid = await make(
					new ProofPutOperation({
						scope,
						key: new StringKey("a"),
						data: serialize(new Document({ id: "other" })),
					}),
					[],
					1n,
				);
				// Model an existing row before the caller enabled its required codec check.
				await coordinator.put(invalid.block);
				await close();
				await open();
				validatePut = (fact) => {
					if (deserialize(fact.data, Document).id !== fact.key.key)
						throw new Error("Decoded id differs from signed key");
				};
				const owner = installOwner();
				await expect(owner.recover()).rejectedWith(
					"Decoded id differs from signed key",
				);
				expect(() => owner.assertReady()).throws("has not recovered");
				expect(admissions).equal(0);
			});

			it("binds canonical local preparation, signed metadata and complete-history deletion through Log.append", async () => {
				const owner = installOwner();
				expect(() => owner.assertReady()).throws("has not recovered");
				await owner.recover();
				const first = await owner.preparePut(
					new StringKey("a"),
					serialize(new Document({ id: "a", name: "first" })),
				);
				const a = await log.append(first.operation, {
					meta: { next: first.next, data: Uint8Array.of(7) },
				});
				expect(a.entry.meta.data).deep.equal(Uint8Array.of(7));
				const second = await owner.preparePut(
					new StringKey("a"),
					serialize(new Document({ id: "a", name: "second" })),
				);
				expect(second.next.map((entry) => entry.hash)).deep.equal([
					a.entry.hash,
				]);
				await log.append(second.operation, { meta: { next: second.next } });
				const remove = await owner.prepareDelete(new StringKey("a"));
				expect(remove.next).to.have.length(2);
				await log.append(remove.operation, { meta: { next: remove.next } });
				expect(log.length).equal(1);
				expect(await projected("a")).equal(undefined);
				await expect(
					log.append(first.operation, { durability: "buffered" }),
				).rejectedWith("Unsupported proof owner");
			});

			it("joins 300 ordinary versions beyond the delete-proof bound with a small sliding window", async () => {
				const owner = installOwner({ entries: 8, bytes: 64 * 1024 });
				await owner.recover();
				const history: Awaited<ReturnType<typeof put>>[] = [];
				for (let i = 0; i < 300; i++)
					history.push(
						await put("a", BigInt(i + 1), i ? [history[i - 1]!.entry] : []),
					);
				const callbacks: string[] = [];
				await log.join(
					[
						{
							entry: history[299]!.entry,
							references: history.slice(0, -1).map((item) => item.entry),
						},
					],
					{
						onChange: async (change) => {
							callbacks.push(...change.added.map((item) => item.entry.hash));
							const lease = await log.entryIndex.acquireExclusiveMutationLock();
							log.entryIndex.releaseHashMutationLocks(lease);
						},
					},
				);
				expect(log.length).equal(300);
				expect(new Set(callbacks).size).equal(300);
				expect(callbacks).to.have.length(300);
				expect((await projected("a"))?.__context.head).equal(
					history[299]!.entry.hash,
				);
			});

			it("retains later branches and shared ancestors when the traversal window evicts the root", async () => {
				const owner = installOwner({ entries: 2, bytes: 64 * 1024 });
				await owner.recover();
				const common = await put("a", 1n);
				const left1 = await put("a", 2n, [common.entry]);
				const left2 = await put("a", 3n, [left1.entry]);
				const right = await put("b", 4n, [common.entry]);
				const root = await put("c", 5n, [left2.entry, right.entry]);
				await log.join([
					{
						entry: root.entry,
						references: [common, left1, left2, right].map((item) => item.entry),
					},
				]);
				expect(log.length).equal(5);
				for (const item of [common, left1, left2, right, root])
					expect(await log.has(item.entry.hash)).equal(true);
				expect((await projected("c"))?.__context.head).equal(root.entry.hash);
			});

			it("drains a committed join prefix on cancellation and resumes the same root later", async () => {
				const owner = installOwner({ entries: 2, bytes: 64 * 1024 });
				await owner.recover();
				const first = await put("a", 1n);
				const second = await put("a", 2n, [first.entry]);
				const third = await put("a", 3n, [second.entry]);
				const root = await put("a", 4n, [third.entry]);
				const input = [
					{
						entry: root.entry,
						references: [first.entry, second.entry, third.entry],
					},
				];
				const controller = new AbortController();
				await expect(
					log.join(input, {
						signal: controller.signal,
						onChange: async () => {
							controller.abort();
						},
					}),
				).rejected;
				expect(log.length).equal(1);
				expect(await log.has(root.entry.hash)).equal(false);
				expect((await projected("a"))?.__context.head).equal(first.entry.hash);
				expect(admissions).equal(0);
				expect(poison).equal(undefined);
				await log.join(input);
				expect(log.length).equal(4);
				expect((await projected("a"))?.__context.head).equal(root.entry.hash);
			});

			it("makes progress when encoded bytes, not depth, evict the traversal prefix", async () => {
				const history: Awaited<ReturnType<typeof put>>[] = [];
				for (let i = 0; i < 12; i++)
					history.push(
						await make(
							new ProofPutOperation({
								scope,
								key: new StringKey("a"),
								data: serialize(
									new Document({ id: "a", name: "x".repeat(4096) + i }),
								),
							}),
							i ? [history[i - 1]!.entry] : [],
							BigInt(i + 1),
						),
					);
				const largest = Math.max(
					...history.map((item) => item.block.bytes.byteLength),
				);
				const owner = installOwner({ entries: 16, bytes: 2 * largest + 16 });
				await owner.recover();
				await log.join([
					{
						entry: history[11]!.entry,
						references: history.slice(0, -1).map((item) => item.entry),
					},
				]);
				expect(log.length).equal(12);
				expect((await projected("a"))?.__context.head).equal(
					history[11]!.entry.hash,
				);
			});

			it("counts the captured requested root inside the private byte budget", async () => {
				const item = await put("a", 1n);
				const owner = installOwner({
					entries: 2,
					bytes: item.block.bytes.byteLength * 2 - 1,
				});
				await owner.recover();
				await expect(log.join([item.entry])).rejectedWith(
					"staging byte budget exceeded",
				);
				expect(log.length).equal(0);
				expect(await projected("a")).equal(undefined);
				expect(poison).equal(undefined);
			});

			it("recovers retained CUTs before exposing rebuilt projection and seeds the signed clock", async () => {
				const victim = await put("a", log.hlc.last.wallTime + 1_000_000_000n);
				await coordinator.put(victim.block);
				const cut = await deletion(
					victim,
					[victim],
					[victim],
					victim.entry.meta.clock.timestamp.wallTime + 1n,
				);
				const fail = sinon
					.stub(log.entryIndex, "completeExactDelete")
					.rejects(new Error("unfinished marker"));
				await expect(coordinator.deletion(cut.block)).rejectedWith(
					"unfinished marker",
				);
				fail.restore();
				await close();
				await open();
				const owner = installOwner();
				await projection.index.del({ query: [] });
				await owner.recover();
				owner.assertReady();
				expect(await projected("a")).equal(undefined);
				expect(await log.has(victim.entry.hash)).equal(false);
				expect(log.hlc.last.compare(cut.entry.meta.clock.timestamp)).equal(1);
				const next = await owner.preparePut(
					new StringKey("a"),
					serialize(new Document({ id: "a", name: "new" })),
				);
				const added = await log.append(next.operation, {
					meta: { next: next.next },
				});
				expect(
					added.entry.meta.clock.timestamp.compare(
						cut.entry.meta.clock.timestamp,
					),
				).equal(1);
			});

			it("runs projection events only after coupled barriers and poisons a failed event callback", async () => {
				const item = await put("a", 1n);
				const barrier = sinon.spy(
					projection.index.crashSafeDurability!,
					"barrier",
				);
				const failure = new Error("projection event failed");
				afterProject = () => {
					expect(barrier.called).equal(true);
					expect(admissions).equal(1);
					throw failure;
				};
				await expect(coordinator.put(item.block)).rejectedWith(failure);
				expect(await log.has(item.entry.hash)).equal(true);
				expect(poison).equal(failure);
				expect(admissions).equal(0);
			});

			it("authenticates before effects and leaves rejected or cancelled input unpoisoned", async () => {
				const wallTime = log.hlc.last.wallTime + 1_000_000_000n;
				const item = await put("a", wallTime);
				const before = log.hlc.last.clone();
				const bad = {
					cid: item.block.cid,
					bytes: new Uint8Array(item.block.bytes),
				};
				bad.bytes[bad.bytes.length - 1] ^= 1;
				await expect(coordinator.put(bad)).rejectedWith("Proof CID mismatch");
				const cancelled = new AbortController();
				cancelled.abort();
				await expect(coordinator.put(item.block, { signal: cancelled.signal }))
					.rejected;
				expect(log.length).equal(0);
				expect(await peer.services.blocks.has(item.entry.hash)).equal(false);
				expect(admissions).equal(0);
				expect(poison).equal(undefined);
				expect(log.hlc.last).deep.equal(before);
				await coordinator.put(item.block);
				expect((await projected("a"))?.name).equal(String(wallTime));
				expect(log.hlc.last.compare(item.entry.meta.clock.timestamp)).equal(1);
			});

			it("rejects an authenticated clock overflow before mutation or poison", async () => {
				const item = await put("a", log.hlc.last.wallTime + 1_000_000_000n);
				const before = log.hlc.last.clone();
				log.hlc.wallTimeUpperBound = before.wallTime;
				await expect(coordinator.put(item.block)).rejectedWith(
					"exceeds the max time",
				);
				expect(await store.has(item.entry.hash)).equal(false);
				expect(log.length).equal(0);
				expect(log.hlc.last).deep.equal(before);
				expect(poison).equal(undefined);
				expect(admissions).equal(0);
			});

			it("retires complete same-key history while preserving independent replacements and cross-key boundaries", async () => {
				const earlier = await put("a", 1n);
				const boundary = await put("other", 2n, [earlier.entry]);
				const victim = await put("a", 3n, [boundary.entry]);
				const replacement = await put("a", 4n);
				for (const item of [earlier, boundary, victim, replacement])
					await coordinator.put(item.block);
				const cut = await deletion(victim, [victim, boundary], [victim], 5n);
				await coordinator.deletion(cut.block);
				expect(await log.has(victim.entry.hash)).equal(false);
				expect(await peer.services.blocks.has(victim.entry.hash)).equal(false);
				for (const item of [earlier, boundary, replacement, cut])
					expect(await log.has(item.entry.hash)).equal(true);
				expect((await projected("a"))?.__context.head).equal(
					replacement.entry.hash,
				);
				expect((await projected("other"))?.__context.head).equal(
					boundary.entry.hash,
				);
				const replay = await coordinator.put(victim.block);
				expect(replay.admitted).equal(false);
				expect(await log.has(victim.entry.hash)).equal(false);
				expect(admissions).equal(0);
			});

			it("rejects missing parents but uses retained original evidence without re-admitting it", async () => {
				const parent = await put("a", 1n);
				const child = await put("a", 3n, [parent.entry]);
				await expect(coordinator.put(child.block)).rejectedWith(
					"Missing proof PUT parent",
				);
				expect(poison).equal(undefined);
				const cut = await deletion(parent, [parent], [parent], 2n);
				await coordinator.deletion(cut.block);
				await coordinator.put(child.block);
				expect(await log.has(parent.entry.hash)).equal(false);
				expect(await peer.services.blocks.has(parent.entry.hash)).equal(false);
				expect((await projected("a"))?.__context.head).equal(child.entry.hash);
			});

			it("converges all PUT/delete/replacement arrival orders and prevents later victim replay", async () => {
				const orders = [
					[0, 1, 2],
					[0, 2, 1],
					[1, 0, 2],
					[1, 2, 0],
					[2, 0, 1],
					[2, 1, 0],
				];
				for (const replacementTime of [2n, 3n]) {
					for (let i = 0; i < orders.length; i++) {
						const key = `arrival-${replacementTime}-${i}`;
						const victim = await put(key, 1n);
						const replacement = await put(key, replacementTime);
						const cut = await deletion(victim, [victim], [victim], 3n);
						const actions = [
							() => coordinator.put(victim.block),
							() => coordinator.deletion(cut.block),
							() => coordinator.put(replacement.block),
						];
						for (const index of orders[i]!) await actions[index]!();
						expect(await log.has(victim.entry.hash)).equal(false);
						expect(await log.has(cut.entry.hash)).equal(true);
						expect((await projected(key))?.__context.head).equal(
							replacement.entry.hash,
						);
						await coordinator.deletion(cut.block);
						const removeReplacement = await deletion(
							replacement,
							[replacement],
							[replacement],
							4n,
						);
						await coordinator.deletion(removeReplacement.block);
						expect((await coordinator.put(victim.block)).admitted).equal(false);
						expect(await projected(key)).equal(undefined);
					}
				}
			});

			it("reopens offline and redoes cleanup after the durable CUT marker but before deletion", async () => {
				const old = await put("a", 1n);
				const victim = await put("a", 2n, [old.entry]);
				const survivor = await put("a", 3n);
				for (const item of [old, victim, survivor])
					await coordinator.put(item.block);
				const cut = await deletion(victim, [old, victim], [old, victim], 4n);
				const fault = new Error("stopped after durable marker");
				const fail = sinon
					.stub(log.entryIndex, "completeExactDelete")
					.rejects(fault);
				await expect(coordinator.deletion(cut.block)).rejectedWith(fault);
				expect(poison).equal(fault);
				expect(admissions).equal(0);
				expect(await log.has(cut.entry.hash)).equal(true);
				expect(await log.has(victim.entry.hash)).equal(true);
				fail.restore();
				await expect(coordinator.deletion(cut.block)).rejected;
				await close();
				await open();
				expect(peer.libp2p.getConnections()).to.have.length(0);
				const replay = await coordinator.deletion(cut.block);
				expect(replay.changed).equal(false);
				expect(log.length).equal(2);
				for (const item of [old, victim]) {
					expect(await log.has(item.entry.hash)).equal(false);
					expect(await coordinates.get(toId(item.entry.hash))).equal(undefined);
				}
				expect((await projected("a"))?.__context.head).equal(
					survivor.entry.hash,
				);
				expect(await peer.services.blocks.get(cut.entry.hash)).deep.equal(
					cut.block.bytes,
				);
			});

			it("persists the original block before its marker row, and the marker before target removal", async () => {
				const victim = await put("a", 1n);
				await coordinator.put(victim.block);
				const cut = await deletion(victim, [victim], [victim], 2n);
				const entered = pDefer();
				const release = pDefer();
				const durability = store.crashSafeDurability!;
				const barrier = durability.barrier.bind(durability);
				const pause = sinon.stub(durability, "barrier").callsFake(async () => {
					entered.resolve();
					await release.promise;
					await barrier();
				});
				const remove = sinon.spy(log.entryIndex, "completeExactDelete");
				const commit = coordinator.deletion(cut.block);
				try {
					await entered.promise;
					expect(await log.has(cut.entry.hash)).equal(false);
					expect(await log.has(victim.entry.hash)).equal(true);
					expect(remove.called).equal(false);
					expect(admissions).equal(1);
					release.resolve();
					await commit;
					expect(await log.has(cut.entry.hash)).equal(true);
					expect(await log.has(victim.entry.hash)).equal(false);
					expect(remove.callCount).equal(1);
				} finally {
					release.resolve();
					await commit.catch(() => undefined);
					pause.restore();
				}
			});

			it("fails closed before target removal if the marker index barrier fails", async () => {
				const victim = await put("a", 1n);
				await coordinator.put(victim.block);
				const cut = await deletion(victim, [victim], [victim], 2n);
				const fault = new Error("marker index barrier failed");
				const barrier = sinon
					.stub(log.entryIndex.properties.index.crashSafeDurability!, "barrier")
					.rejects(fault);
				const remove = sinon.spy(log.entryIndex, "completeExactDelete");
				const evidence = sinon.spy();
				await expect(
					coordinator.deletion(cut.block, { onLocalCommit: evidence }),
				).rejectedWith(fault);
				expect(evidence.callCount).equal(1);
				expect(evidence.firstCall.args[0].hash).equal(cut.entry.hash);
				expect(remove.called).equal(false);
				expect(await log.has(victim.entry.hash)).equal(true);
				expect(poison).equal(fault);
				expect(admissions).equal(0);
				barrier.restore();
			});

			it("repairs projection on offline marker replay after exact target removal already finished", async () => {
				const victim = await put("a", 1n);
				await coordinator.put(victim.block);
				const cut = await deletion(victim, [victim], [victim], 2n);
				const fault = new Error("projection stopped after target removal");
				beforeProject = async () => {
					throw fault;
				};
				await expect(coordinator.deletion(cut.block)).rejectedWith(fault);
				expect(await log.has(victim.entry.hash)).equal(false);
				expect((await projected("a"))?.__context.head).equal(victim.entry.hash);
				expect(poison).equal(fault);
				await close();
				await open();
				expect(peer.libp2p.getConnections()).to.have.length(0);
				expect((await coordinator.deletion(cut.block)).changed).equal(false);
				expect(await projected("a")).equal(undefined);
				expect(await coordinates.get(toId(victim.entry.hash))).equal(undefined);
			});

			it("rejects callback-owned close and drop before terminal side effects", async () => {
				const item = await put("a", 1n);
				beforeProject = async () => {
					await expect(log.close()).rejectedWith(
						"mutation callback is running",
					);
					await expect(log.drop()).rejectedWith("mutation callback is running");
					expect(log.closed).equal(false);
				};
				await coordinator.put(item.block);
				expect(log.closed).equal(false);
				expect((await projected("a"))?.__context.head).equal(item.entry.hash);
				expect(poison).equal(undefined);
			});

			it("drains a physically pending commit before drop and ignores cancellation after writes start", async () => {
				const item = await put("a", 1n);
				const entered = pDefer();
				const release = pDefer();
				const original = store.put.bind(store);
				sinon.stub(store, "put").callsFake(async (...args) => {
					entered.resolve();
					await release.promise;
					return original(...args);
				});
				const cancelled = new AbortController();
				const commit = coordinator.put(item.block, {
					signal: cancelled.signal,
				});
				await entered.promise;
				let dropped = false;
				const dropping = log.drop().then(() => {
					dropped = true;
				});
				try {
					cancelled.abort();
					await Promise.resolve();
					expect(dropped).equal(false);
					expect(admissions).equal(1);
					release.resolve();
					expect((await commit).admitted).equal(true);
					await dropping;
					expect(dropped).equal(true);
					expect(admissions).equal(0);
					expect(poison).equal(undefined);
				} finally {
					release.resolve();
					await commit.catch(() => undefined);
					await dropping;
				}
			});
		});
	}
});
