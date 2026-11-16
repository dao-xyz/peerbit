import { serialize } from "@dao-xyz/borsh";
import { Ed25519Keypair } from "@peerbit/crypto";
import { StringKey, toId } from "@peerbit/indexer-interface";
import { create as createSQLite } from "@peerbit/indexer-sqlite3";
import {
	Entry,
	type EntryIndexHashMutationLockOwner,
	EntryType,
	EntryV0,
	LamportClock,
	Log,
	Timestamp,
} from "@peerbit/log";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import {
	type ProofDeleteContract,
	deriveProofDeleteScope,
} from "../src/proof-delete-manifest.js";
import {
	BORSH_ENCODING_PROOF_OPERATION,
	ProofDeleteOperation,
	ProofOperation,
	ProofPutOperation,
	deriveProofKeyGid,
} from "../src/proof-operations.js";
import { createProofRetainedState } from "../src/proof-retained-state.js";

const deferred = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => (resolve = done));
	return { promise, resolve };
};

// Real block/index storage and offline reopen of the private read side. Entries
// are explicitly inserted as fixture facts, not through production Documents
// admission. This does not claim crash-atomic retirement or durable receipts.
describe("proof retained owner state", () => {
	for (const nativeGraph of [false, true]) {
		describe(nativeGraph ? "native graph" : "JS graph", () => {
			let directory: string;
			let peer: Peerbit;
			let identity: Ed25519Keypair;
			let log: Log<ProofOperation>;
			let contract: ProofDeleteContract;
			let scope: Uint8Array;
			let state: ReturnType<typeof createProofRetainedState>;
			let generation: object;
			let serial: number;
			const open = async () => {
				peer = await Peerbit.create({ directory });
				log = new Log<ProofOperation>({ id: new Uint8Array(32).fill(77) });
				await log.open(peer.services.blocks, identity, {
					encoding: BORSH_ENCODING_PROOF_OPERATION,
					indexer: await createSQLite(join(directory, "proof-index")),
					nativeGraph,
					appendDurability: "strict",
				});
				const captured = (generation = {});
				state = createProofRetainedState({
					log,
					contract,
					assertActive: () => {
						if (generation !== captured)
							throw new Error("Inactive owner generation");
					},
				});
			};
			beforeEach(async () => {
				directory = await mkdtemp(join(tmpdir(), "peerbit-proof-retained-"));
				identity = await Ed25519Keypair.create();
				contract = {
					version: 1,
					nonce: new Uint8Array(32).fill(8),
					owner: identity.publicKey.publicKey,
					keyKind: 0,
				};
				scope = deriveProofDeleteScope(contract);
				serial = 0;
				await open();
			});
			afterEach(async () => {
				sinon.restore();
				await log?.close();
				await peer?.stop();
				await rm(directory, { recursive: true, force: true });
			});
			const owned = async <T>(
				operation: (owner: EntryIndexHashMutationLockOwner) => Promise<T>,
			) => {
				const index = log.entryIndex;
				const owner = await index.acquireExclusiveMutationLock();
				try {
					return await operation(owner);
				} finally {
					index.releaseHashMutationLocks(owner);
				}
			};
			const make = async (
				operation: ProofPutOperation | ProofDeleteOperation,
				next: Entry<ProofOperation>[],
				wallTime: bigint,
				logical = 0,
				deferIndexWrite = false,
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
						store: peer.services.blocks,
						identity,
						encoding: BORSH_ENCODING_PROOF_OPERATION,
						data: operation,
						meta: {
							type:
								operation instanceof ProofDeleteOperation
									? EntryType.CUT
									: EntryType.APPEND,
							next,
							clock: new LamportClock({
								id: identity.publicKey.bytes,
								timestamp: new Timestamp({ wallTime, logical }),
							}),
						},
					},
					deriveProofKeyGid(operation.scope, operation.key),
				);
				await log.entryIndex.put(entry, {
					unique: true,
					isHead: true,
					toMultiHash: false,
					deferIndexWrite,
				});
				return entry;
			};
			const put = (
				key = "a",
				wallTime = BigInt(++serial),
				next: Entry<ProofOperation>[] = [],
				logical = 0,
				deferIndexWrite = false,
			) =>
				make(
					new ProofPutOperation({
						scope,
						key: new StringKey(key),
						data: serialize(new StringKey(`value-${++serial}`)),
					}),
					next,
					wallTime,
					logical,
					deferIndexWrite,
				);
			const raw = async (entry: Entry<ProofOperation>) => {
				const value = await peer.services.blocks.get(entry.hash, {
					remote: false,
				});
				if (!value) throw new Error("Missing fixture bytes");
				return value;
			};
			const marker = async (
				target: Entry<ProofOperation>,
				witnesses: Entry<ProofOperation>[],
				wallTime: bigint,
				declaredKey = "a",
			) => {
				const sorted = witnesses
					.slice()
					.sort((a, b) => (a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : 0));
				return make(
					new ProofDeleteOperation({
						scope,
						key: new StringKey(declaredKey),
						target: target.hash,
						witnesses: await Promise.all(witnesses.map(raw)),
					}),
					sorted,
					wallTime,
				);
			};

			it("selects nonhead survivors and resolves retired original bytes after disk reopen", async () => {
				const first = await put("a", 10n);
				const firstBytes = new Uint8Array(await raw(first));
				const survivor = await put("a", 15n);
				const victim = await put("a", 20n, [first]);
				const child = await put("b", 30n, [survivor]);
				const cut = await marker(victim, [first, victim], 40n);
				// Demote the marker without removing its target lookup row.
				await put("c", 50n, [cut]);
				expect((await log.getShallow(survivor.hash))?.head).equal(false);
				expect((await log.getShallow(cut.hash))?.head).equal(false);
				const check = async () =>
					owned(async (owner) => {
						expect(
							(await state.winner({ key: new StringKey("a"), owner }))?.hash,
						).equal(survivor.hash);
						expect(
							(await state.winner({ key: new StringKey("b"), owner }))?.hash,
						).equal(child.hash);
						const original = await state.retiredOriginal({
							hash: first.hash,
							owner,
						});
						expect(original).deep.equal(firstBytes);
						original!.fill(0);
						expect(
							await state.retiredOriginal({
								hash: first.hash,
								key: new StringKey("a"),
								owner,
							}),
						).deep.equal(firstBytes);
						expect(
							await state.retiredOriginal({ hash: child.hash, owner }),
						).equal(undefined);
						await expect(
							state.retiredOriginal({
								hash: first.hash,
								key: new StringKey("b"),
								owner,
							}),
						).rejectedWith("Wrong retained proof target key");
					});
				// Block presence is not admission/liveness: retained proof already retires
				// victim while its original is still physically present.
				await check();
				await owned((owner) =>
					log.entryIndex.completeExactDelete(
						[first.toShallow(false), victim.toShallow(false)],
						owner,
					),
				);
				expect(await peer.services.blocks.has(first.hash)).equal(false);
				await check();
				await log.close();
				await peer.stop();
				await open();
				expect(peer.libp2p.getConnections()).to.have.length(0);
				expect(await log.has(first.hash)).equal(false);
				await check();
			});

			it("does not exhaust the candidate budget for 257 ordinary versions", async () => {
				let latest: Entry<ProofOperation> | undefined;
				for (let i = 1; i <= 257; i++)
					latest = await put("a", BigInt(i), latest ? [latest] : []);
				const get = sinon.spy(peer.services.blocks, "get");
				await owned(async (owner) => {
					expect(
						(await state.winner({ key: new StringKey("a"), owner }))?.hash,
					).equal(latest!.hash);
				});
				expect(get.callCount).equal(1);
			});

			it("uses signed logical time then descending CID for equal-clock candidates", async () => {
				await put("a", 1n, [], 99);
				await put("a", 2n, [], 1);
				const a = await put("a", 2n, [], 2);
				const b = await put("a", 2n, [], 2);
				await owned(async (owner) => {
					expect(
						(await state.winner({ key: new StringKey("a"), owner }))?.hash,
					).equal([a.hash, b.hash].sort().at(-1));
				});
			});

			it("fails closed on an incomplete over-budget retired prefix", async () => {
				for (let i = 1; i <= 257; i++) {
					const candidate = await put("a", BigInt(i));
					await marker(candidate, [candidate], 1000n + BigInt(i));
				}
				await owned(async (owner) => {
					await expect(
						state.winner({ key: new StringKey("a"), owner }),
					).rejectedWith("PUT budget exceeded");
				});
			});

			it("shares retained-proof read budgets across every parent and winner in one lease", async () => {
				const first = await make(
					new ProofPutOperation({
						scope,
						key: new StringKey("a"),
						data: new Uint8Array(4_500_000),
					}),
					[],
					1n,
				);
				const parents = [first];
				for (let i = 2; i <= 8; i++)
					parents.push(await put("a", BigInt(i), [parents.at(-1)!]));
				const cut = await marker(parents.at(-1)!, parents, 10n);
				const markerSize = (await raw(cut)).byteLength;
				const get = sinon.spy(peer.services.blocks, "get");
				await owned(async (owner) => {
					for (const parent of parents.slice(0, 7))
						expect(await state.retiredOriginal({ hash: parent.hash, owner })).to
							.exist;
					await expect(
						state.retiredOriginal({ hash: parents[7]!.hash, owner }),
					).rejectedWith("byte budget exceeded");
					await expect(
						state.winner({ key: new StringKey("a"), owner }),
					).rejectedWith("byte budget exceeded");
					expect(get.callCount).equal(7);
					expect(markerSize * get.callCount).lessThan(32 * 1024 * 1024);
				});
				// A new physical owner gets a fresh bound, not a lifetime-global cap.
				await owned(async (owner) => {
					expect(await state.retiredOriginal({ hash: first.hash, owner })).to
						.exist;
				});
				expect(get.callCount).equal(8);
			});

			it("uses one budget for indexed parents, retained proof and winner without trusting raw-only blocks", async () => {
				const indexed = await make(
					new ProofPutOperation({
						scope,
						key: new StringKey("a"),
						data: new Uint8Array(4_500_000),
					}),
					[],
					1n,
				);
				const victim = await put("a", 2n, [indexed]);
				const cut = await marker(victim, [indexed, victim], 3n);
				const rawOnly = await put("b", 4n);
				const rawOnlyBytes = new Uint8Array(await raw(rawOnly));
				await owned((owner) =>
					log.entryIndex.completeExactDelete(
						[victim.toShallow(false), rawOnly.toShallow(false)],
						owner,
					),
				);
				await peer.services.blocks.put(rawOnlyBytes);
				const get = sinon.spy(peer.services.blocks, "get");
				await owned(async (owner) => {
					expect(
						await state.parentOriginal({ hash: rawOnly.hash, owner }),
					).equal(undefined);
					expect(get.callCount).equal(0);
					for (let i = 0; i < 6; i++)
						expect(await state.parentOriginal({ hash: indexed.hash, owner })).to
							.exist;
					expect(await state.parentOriginal({ hash: victim.hash, owner })).to
						.exist;
					expect(get.lastCall.args[0]).equal(cut.hash);
					await expect(
						state.parentOriginal({ hash: indexed.hash, owner }),
					).rejectedWith("byte budget exceeded");
					await expect(
						state.winner({ key: new StringKey("a"), owner }),
					).rejectedWith("byte budget exceeded");
					expect(get.callCount).equal(7);
				});
			});

			it("fails closed on malformed retained proof and mismatched admitted clock", async () => {
				const candidate = await put("a", 1n);
				await marker(candidate, [candidate], 2n, "wrong");
				await owned(async (owner) => {
					await expect(
						state.winner({ key: new StringKey("a"), owner }),
					).rejectedWith("Wrong proof target key");
					await expect(
						state.retiredOriginal({ hash: candidate.hash, owner }),
					).rejectedWith("Wrong proof target key");
				});
				const other = await put("b", 3n);
				const corrupted = other.toShallow(true);
				corrupted.meta.clock.timestamp.logical++;
				await log.entryIndex.properties.index.put(corrupted);
				await owned(async (owner) => {
					await expect(
						state.winner({ key: new StringKey("b"), owner }),
					).rejectedWith("row disagrees");
				});
			});

			it("requires a live exclusive owner and flushes pending rows with that owner", async () => {
				const candidate = await put("a", 1n, [], 0, true);
				expect(
					await log.entryIndex.properties.index.get(toId(candidate.hash)),
				).equal(undefined);
				const shared = await log.entryIndex.acquireHashMutationLocks([
					candidate.hash,
				]);
				try {
					await expect(
						state.winner({ key: new StringKey("a"), owner: shared }),
					).rejectedWith("exclusive ownership");
				} finally {
					log.entryIndex.releaseHashMutationLocks(shared);
				}
				const released = await log.entryIndex.acquireExclusiveMutationLock();
				log.entryIndex.releaseHashMutationLocks(released);
				await expect(
					state.winner({ key: new StringKey("a"), owner: released }),
				).rejectedWith("Invalid or released");
				const flush = sinon.spy(log.entryIndex, "flushPendingWrites");
				await owned(async (owner) => {
					expect(
						(await state.winner({ key: new StringKey("a"), owner }))?.hash,
					).equal(candidate.hash);
					expect(flush.firstCall.args).deep.equal([undefined, owner]);
				});
			});

			it("does not read unrelated APPEND children while finding a retained marker", async () => {
				const target = await put("a", 1n);
				for (let i = 0; i < 80; i++)
					await put(`child-${i}`, 2n + BigInt(i), [target]);
				await marker(target, [target], 100n);
				const get = sinon.spy(peer.services.blocks, "get");
				await owned(async (owner) => {
					expect(await state.retiredOriginal({ hash: target.hash, owner })).to
						.exist;
				});
				expect(get.callCount).equal(1);
			});

			it("requests one marker candidate and closes its iterator after success or key rejection", async () => {
				const target = await put("a", 1n);
				await marker(target, [target], 2n);
				await marker(target, [target], 3n);
				const iterate = log.entryIndex.iterate.bind(log.entryIndex);
				const cursors: { next: sinon.SinonSpy; close: sinon.SinonSpy }[] = [];
				sinon.stub(log.entryIndex, "iterate").callsFake(((
					...args: Parameters<typeof iterate>
				) => {
					const iterator = iterate(...args);
					cursors.push({
						next: sinon.spy(iterator, "next"),
						close: sinon.spy(iterator, "close"),
					});
					return iterator;
				}) as typeof iterate);
				await owned(async (owner) => {
					expect(
						await state.retiredOriginal({ hash: target.hash, owner }),
					).deep.equal(await raw(target));
					await expect(
						state.retiredOriginal({
							hash: target.hash,
							key: new StringKey("b"),
							owner,
						}),
					).rejectedWith("Wrong retained proof target key");
				});
				expect(cursors).to.have.length(2);
				for (const cursor of cursors) {
					expect(cursor.next.args).deep.equal([[1]]);
					expect(cursor.close.callCount).equal(1);
				}
			});

			it("does not mistake an indexed-but-missing block for an absent document", async () => {
				const candidate = await put();
				await peer.services.blocks.rm(candidate.hash);
				await owned(async (owner) => {
					await expect(
						state.winner({ key: new StringKey("a"), owner }),
					).rejectedWith("Missing indexed proof retained-state block");
				});
			});

			it("permits only captured physical admission to finish reads while close drains", async () => {
				const candidate = await put();
				let admitted = false;
				let closing: Promise<void> | undefined;
				let admittedState:
					| ReturnType<typeof createProofRetainedState>
					| undefined;
				await (
					log as unknown as {
						withNativeCommittedAppendAdmission<T>(
							operation: () => Promise<T>,
						): Promise<T>;
					}
				).withNativeCommittedAppendAdmission(() =>
					owned(async (owner) => {
						admitted = true;
						admittedState = createProofRetainedState({
							log,
							contract,
							assertActive: () => {},
							assertAdmitted: () => {
								if (!admitted) throw new Error("Physical admission settled");
							},
						});
						try {
							closing = log.close();
							expect(log.closed).equal(true);
							await expect(
								state.winner({ key: new StringKey("a"), owner }),
							).rejectedWith("Stale proof retained-state lifecycle");
							expect(
								(await admittedState.winner({ key: new StringKey("a"), owner }))
									?.hash,
							).equal(candidate.hash);
						} finally {
							admitted = false;
						}
					}),
				);
				await closing;
				await expect(
					admittedState!.winner({
						key: new StringKey("a"),
						owner: {} as EntryIndexHashMutationLockOwner,
					}),
				).rejectedWith("Physical admission settled");
			});

			for (const cancellation of [
				"abort",
				"generation",
				"released-owner",
			] as const) {
				it(`rejects ${cancellation} after a held original-block read and closes the iterator`, async () => {
					await put();
					const entered = deferred();
					const release = deferred();
					const controller = new AbortController();
					const get = peer.services.blocks.get.bind(peer.services.blocks);
					sinon.stub(peer.services.blocks, "get").callsFake(async (...args) => {
						const result = await get(...args);
						entered.resolve();
						await release.promise;
						return result;
					});
					const iterate = log.entryIndex.iterate.bind(log.entryIndex);
					const closed = sinon.spy();
					sinon.stub(log.entryIndex, "iterate").callsFake(((
						...args: Parameters<typeof iterate>
					) => {
						const iterator = iterate(...args);
						const close = iterator.close;
						iterator.close = async () => {
							await close();
							closed();
						};
						return iterator;
					}) as typeof iterate);
					const owner = await log.entryIndex.acquireExclusiveMutationLock();
					const pending = state.winner({
						key: new StringKey("a"),
						owner,
						signal: controller.signal,
					});
					const outcome = pending.catch((error) => error);
					try {
						await entered.promise;
						if (cancellation === "abort")
							controller.abort(new Error("Cancelled fixture"));
						else if (cancellation === "generation") generation = {};
						else log.entryIndex.releaseHashMutationLocks(owner);
						release.resolve();
						expect(await outcome).to.be.instanceOf(Error);
						expect(closed.callCount).equal(1);
					} finally {
						release.resolve();
						await outcome;
						if (cancellation !== "released-owner")
							log.entryIndex.releaseHashMutationLocks(owner);
					}
				});
			}
		});
	}
});
