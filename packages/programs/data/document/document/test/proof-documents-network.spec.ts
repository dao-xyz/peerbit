import { deserialize, serialize } from "@dao-xyz/borsh";
import { toId } from "@peerbit/indexer-interface";
import { EntryType } from "@peerbit/log";
import { waitFor } from "@peerbit/time";
import { expect } from "chai";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pDefer from "p-defer";
import { Peerbit } from "peerbit";
import sinon from "sinon";
import { Documents } from "../src/program.js";
import { Document } from "./data.js";

const localRead = { local: true, remote: false } as const;
const delivery = {
	reliability: "persisted" as const,
	minAcks: 1,
	timeout: 15_000,
};

const stored = async (
	docs: Documents<Document>,
	hash: string,
	present: boolean,
) => {
	const blocks = (
		docs.log as unknown as {
			remoteBlocks: { localStore: { has(hash: string): Promise<boolean> } };
		}
	).remoteBlocks.localStore;
	const [block, row, coordinate] = await Promise.all([
		blocks.has(hash),
		docs.log.log.entryIndex.properties.index.get(toId(hash)),
		docs.log.entryCoordinatesIndex.get(toId(hash)),
	]);
	expect(block, `local block ${hash}`).equal(present);
	expect(row != null, `lower row ${hash}`).equal(present);
	expect(coordinate != null, `coordinate ${hash}`).equal(present);
};

const heads = async (docs: Documents<Document>) =>
	(await docs.log.log.getHeads(false).all()).map((entry) => entry.hash);

describe("owner-proofs Documents network durability", function () {
	this.timeout(30_000);
	for (const nativeGraph of [false, true]) {
		describe(nativeGraph ? "native graph" : "JS graph", () => {
			let directory: string;
			let source: Peerbit | undefined;
			let receiver: Peerbit | undefined;
			let writer: Documents<Document>;
			let reader: Documents<Document>;
			let manifest: Uint8Array;
			const args = {
				type: Document,
				nativeGraph,
				replicate: 1,
				timeUntilRoleMaturity: 0,
			};
			beforeEach(async () => {
				directory = await mkdtemp(join(tmpdir(), "peerbit-proof-network-"));
				source = await Peerbit.create({ directory: join(directory, "source") });
				writer = await source.open(
					new Documents<Document>({
						format: {
							version: "owner-proofs-v1",
							owner: source.identity.publicKey,
							keyType: "string",
						},
					}),
					{ args },
				);
				manifest = serialize(writer);
				expect(Boolean(writer.log.log.entryIndex.properties.nativeGraph)).equal(
					nativeGraph,
				);
			});
			afterEach(async () => {
				sinon.restore();
				await source?.stop();
				await receiver?.stop();
				if (directory) await rm(directory, { recursive: true, force: true });
			});

			const openReader = async (
				prepare?: (docs: Documents<Document>) => void,
			) => {
				receiver = await Peerbit.create({
					directory: join(directory, "receiver"),
				});
				const candidate = deserialize(
					manifest,
					Documents,
				) as unknown as Documents<Document>;
				prepare?.(candidate);
				reader = await receiver.open(candidate, { args });
				expect(Boolean(reader.log.log.entryIndex.properties.nativeGraph)).equal(
					nativeGraph,
				);
			};
			const connect = async () => {
				await source!.dial(receiver!);
				await Promise.all([
					writer.log.waitForReplicator(receiver!.identity.publicKey, {
						roleAge: 0,
						timeout: 15_000,
					}),
					reader.log.waitForReplicator(source!.identity.publicKey, {
						roleAge: 0,
						timeout: 15_000,
					}),
				]);
			};
			const reopenOffline = async () => {
				const identity = receiver!.identity.publicKey.hashcode();
				await source!.stop();
				source = undefined;
				await receiver!.stop();
				receiver = undefined;
				await openReader();
				expect(receiver!.identity.publicKey.hashcode()).equal(identity);
				expect(receiver!.libp2p.getConnections()).to.have.length(0);
			};

			it("persists exact put/delete receipts and reopens the custodian offline", async () => {
				await openReader();
				await connect();
				const first = await writer.put(
					new Document({ id: "a", name: "first" }),
					{ delivery },
				);
				await stored(reader, first.entry.hash, true);
				expect((await reader.get("a", localRead))?.name).equal("first");
				const second = await writer.put(
					new Document({ id: "a", name: "second" }),
					{ delivery },
				);
				await stored(reader, second.entry.hash, true);
				expect((await reader.get("a", localRead))?.name).equal("second");
				const survivor = await writer.put(
					new Document({ id: "b", name: "keep" }),
					{ delivery },
				);
				const deleted = await writer.del("a", { delivery });
				expect(deleted.entry.meta.type).equal(EntryType.CUT);
				expect(deleted.entry.meta.next).to.have.members([
					first.entry.hash,
					second.entry.hash,
				]);
				const assertState = async () => {
					await stored(reader, deleted.entry.hash, true);
					await stored(reader, survivor.entry.hash, true);
					await stored(reader, first.entry.hash, false);
					await stored(reader, second.entry.hash, false);
					expect(await reader.get("a", localRead)).undefined;
					expect((await reader.get("b", localRead))?.name).equal("keep");
					expect(await heads(reader)).to.have.members([
						deleted.entry.hash,
						survivor.entry.hash,
					]);
				};
				await assertState();
				await reopenOffline();
				await assertState();
			});

			it("withholds a real delete receipt until the remote projection barrier settles", async () => {
				const entered = pDefer();
				const release = pDefer();
				let armed = false;
				let holding = false;
				let projectionDurable = false;
				await openReader((candidate) => {
					const index = candidate.index as any;
					const prepare = index.openWithDeferredQuery.bind(index);
					sinon
						.stub(index, "openWithDeferredQuery")
						.callsFake(async (...values) => {
							const activate = await prepare(...values);
							const durability = candidate.index.index.crashSafeDurability!;
							// SQLite scopes share one database capability. Give only the
							// projection its forwarding facade BEFORE Documents captures it;
							// lower/coordinate barriers and actual fsync remain unchanged.
							sinon
								.stub(candidate.index.index, "crashSafeDurability")
								.get(() => ({
									...durability,
									barrier: async () => {
										if (armed) {
											holding = true;
											entered.resolve();
											await release.promise;
										}
										await durability.barrier();
										if (armed) {
											projectionDurable = true;
											holding = false;
										}
									},
								}));
							return activate;
						});
				});
				await connect();
				const victim = await writer.put(new Document({ id: "barrier" }), {
					delivery,
				});
				await stored(reader, victim.entry.hash, true);
				const shared = reader.log as any;
				const handle = shared.handleRequestPersistedEntriesV1.bind(shared);
				const negative = pDefer<{ requested: string[]; confirmed: string[] }>();
				let positiveAfterBarrier = false;
				let markerHash: string | undefined;
				sinon
					.stub(shared, "handleRequestPersistedEntriesV1")
					.callsFake(async (request: any, context: any, lane: any) => {
						const response = await handle(request, context, lane);
						if (
							lane.fromHash === source!.identity.publicKey.hashcode() &&
							shared.isPersistedReceiptRequestSessionCurrent(
								request,
								context,
								lane,
							) &&
							response &&
							request.hashes.length === 1 &&
							request.hashes[0] !== victim.entry.hash
						) {
							if (
								projectionDurable &&
								markerHash !== undefined &&
								response.hashes.includes(markerHash)
							)
								positiveAfterBarrier = true;
							else if (holding)
								negative.resolve({
									requested: [...request.hashes],
									confirmed: [...response.hashes],
								});
						}
						return response;
					});
				armed = true;
				let settled = false;
				const writing = writer.del("barrier", { delivery }).then(
					(value) => {
						settled = true;
						return { value, error: undefined };
					},
					(error) => {
						settled = true;
						return { value: undefined, error };
					},
				);
				const beforeDelivery = <T>(event: Promise<T>) =>
					Promise.race([
						event,
						writing.then(({ error }) => {
							throw (
								error ??
								new Error("Persisted delivery settled before the held barrier")
							);
						}),
					]);
				try {
					await beforeDelivery(entered.promise);
					const [hash] = await heads(writer);
					expect(hash).to.be.a("string");
					markerHash = hash;
					await stored(reader, hash, true);
					await stored(reader, victim.entry.hash, false);
					const observed = await beforeDelivery(negative.promise);
					expect(observed.requested).deep.equal([hash]);
					expect(observed.confirmed).deep.equal([]);
					expect(projectionDurable).false;
					expect(settled).false;
					release.resolve();
					const outcome = await writing;
					expect(outcome.error).undefined;
					expect(outcome.value!.entry.hash).equal(hash);
					expect(outcome.value!.entry.meta.type).equal(EntryType.CUT);
					expect(outcome.value!.entry.meta.next).deep.equal([
						victim.entry.hash,
					]);
					expect(projectionDurable).true;
					expect(positiveAfterBarrier).true;
					expect(await reader.get("barrier", localRead)).undefined;
				} finally {
					release.resolve();
					await writing;
				}
			});

			it("cold-joins two retained markers and an independent same-key survivor", async () => {
				const survivor = await writer.put(
					new Document({ id: "a", name: "independent survivor" }),
					{ unique: true },
				);
				const targets: string[] = [];
				const markers: string[] = [];
				for (const name of ["first target", "second target"]) {
					const target = await writer.put(new Document({ id: "a", name }), {
						unique: true,
					});
					const deleted = await writer.del("a");
					expect(deleted.entry.meta.next).deep.equal([target.entry.hash]);
					targets.push(target.entry.hash);
					markers.push(deleted.entry.hash);
				}
				const expectedHeads = [survivor.entry.hash, ...markers];
				expect(await heads(writer)).to.have.members(expectedHeads);
				for (const hash of targets) await stored(writer, hash, false);
				expect((await writer.get("a", localRead))?.name).equal(
					"independent survivor",
				);
				await openReader();
				expect(reader.log.log.length).equal(0);
				await connect();
				await waitFor(
					async () => {
						const observedHeads = new Set(await heads(reader));
						return (
							expectedHeads.every((hash) => observedHeads.has(hash)) &&
							(await reader.get("a", localRead))?.name ===
								"independent survivor"
						);
					},
					{ timeout: 15_000, timeoutMessage: "all retained proof heads" },
				);
				const assertState = async () => {
					expect(await heads(reader)).to.have.members(expectedHeads);
					for (const hash of expectedHeads) await stored(reader, hash, true);
					for (const hash of targets) await stored(reader, hash, false);
					expect((await reader.get("a", localRead))?.name).equal(
						"independent survivor",
					);
					expect(await reader.count()).equal(1);
				};
				await assertState();
				await reopenOffline();
				await assertState();
			});
		});
	}
});
