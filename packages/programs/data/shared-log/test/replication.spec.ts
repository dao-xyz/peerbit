import { deserialize, serialize } from "@dao-xyz/borsh";
import { keys } from "@libp2p/crypto";
import {
	BlockRequest,
	BlockResponse,
	type BlockMessage as InnerBlocksMessage,
} from "@peerbit/blocks";
import { type PublicSignKey, randomBytes, toBase64 } from "@peerbit/crypto";
import { Entry, EntryType } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { AbortError, delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import mapSeries from "p-each-series";
import sinon from "sinon";
import { BlocksMessage } from "../src/blocks.js";
import { ExchangeHeadsMessage, RequestIPrune } from "../src/exchange-heads.js";
import {
	type ReplicationOptions,
	createReplicationDomainHash,
} from "../src/index.js";
import { createNumbers } from "../src/integers.js";
import type { ReplicationRangeIndexable } from "../src/ranges.js";
import {
	AbsoluteReplicas,
	AddedReplicationSegmentMessage,
	decodeReplicas,
	maxReplicas,
} from "../src/replication.js";
import { RatelessIBLTSynchronizer } from "../src/sync/rateless-iblt.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import {
	type TestSetupConfig,
	checkBounded,
	collectMessages,
	collectMessagesFn,
	dbgLogs,
	getReceivedHeads,
	slowDownSend,
	waitForConverged,
} from "./utils.js";
import { EventStore, type Operation } from "./utils/stores/event-store.js";

const DEFAULT_ROLE_MATURITY = 2000;

export const testSetups: TestSetupConfig<any>[] = [
	{
		domain: createReplicationDomainHash("u32"),
		type: "u32",
		syncronizer: SimpleSyncronizer,
		name: "u32-simple",
	},
	/* {
		domain: createReplicationDomainHash("u64"),
		type: "u64",
		syncronizer: SimpleSyncronizer,
		name: "u64-simple",
	}, */
	{
		domain: createReplicationDomainHash("u64"),
		type: "u64",
		syncronizer: RatelessIBLTSynchronizer,
		name: "u64-iblt",
	},
];

testSetups.forEach((setup) => {
	describe(setup.name, () => {
		const numbers = createNumbers(setup.type);
		describe(`replication`, function () {
			let session: TestSession;
			let db1: EventStore<string, any>, db2: EventStore<string, any>;
			let _fetchEvents: number;
			let fetchHashes: Set<string>;
			let fromMultihash: any;
			before(() => {
				fromMultihash = Entry.fromMultihash;
				// TODO monkeypatching might lead to sideeffects in other tests!
				Entry.fromMultihash = (s, h, o) => {
					fetchHashes.add(h);
					_fetchEvents += 1;
					return fromMultihash(s, h, o);
				};
			});
			after(() => {
				Entry.fromMultihash = fromMultihash;
			});

			beforeEach(async () => {
				_fetchEvents = 0;
				fetchHashes = new Set();
				session = await TestSession.connected(2, [
					{
						libp2p: {
							privateKey: keys.privateKeyFromRaw(
								new Uint8Array([
									204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91, 229,
									157, 168, 15, 45, 242, 144, 98, 75, 58, 208, 9, 223, 143, 251,
									52, 252, 159, 64, 83, 52, 197, 24, 246, 24, 234, 141, 183,
									151, 82, 53, 142, 57, 25, 148, 150, 26, 209, 223, 22, 212, 40,
									201, 6, 191, 72, 148, 82, 66, 138, 199, 185,
								]),
							),
							services: {
								relay: null, // https://github.com/libp2p/js-libp2p/issues/2794
							} as any,
						},
					},
					{
						libp2p: {
							privateKey: keys.privateKeyFromRaw(
								new Uint8Array([
									237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214, 122,
									28, 157, 208, 163, 15, 215, 104, 193, 151, 177, 62, 231, 253,
									120, 122, 222, 174, 242, 120, 50, 165, 97, 8, 235, 97, 186,
									148, 251, 100, 168, 49, 10, 119, 71, 246, 246, 174, 163, 198,
									54, 224, 6, 174, 212, 159, 187, 2, 137, 47, 192,
								]),
							),
							services: {
								relay: null, // https://github.com/libp2p/js-libp2p/issues/2794
							} as any,
						},
					},
				]);

				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							factor: 1,
						},
						timeUntilRoleMaturity: DEFAULT_ROLE_MATURITY,
						waitForPruneDelay: DEFAULT_ROLE_MATURITY,
						setup,
					},
				});
			});

			afterEach(async () => {
				if (db1 && db1.closed === false) {
					await db1.drop();
				}
				if (db2 && db2.closed === false) {
					await db2.drop();
				}

				await session.stop();
			});

			it("verifies remote signatures by default", async () => {
				const entry = await db1.add("a", { meta: { next: [] } });
				await (session.peers[0] as any)["libp2p"].hangUp(
					session.peers[1].peerId,
				);
				db2 = await session.peers[1].open(new EventStore<string, any>(), {
					args: {
						setup,
					},
				});

				const clonedEntry = deserialize(serialize(entry.entry), Entry);

				let verified = false;
				const verifyFn = clonedEntry.verifySignatures.bind(clonedEntry);

				clonedEntry.verifySignatures = () => {
					verified = true;
					return verifyFn();
				};
				await db2.log.log.join([clonedEntry]);
				expect(verified).to.be.true;
			});

			it("does not verify owned signatures by default", async () => {
				const entry = await db1.add("a", { meta: { next: [] } });
				await (session.peers[0] as any)["libp2p"].hangUp(
					session.peers[1].peerId,
				);
				db2 = await session.peers[1].open(new EventStore<string, any>(), {
					args: {
						setup,
					},
				});

				const clonedEntry = deserialize(serialize(entry.entry), Entry);

				let verified = false;
				const verifyFn = clonedEntry.verifySignatures.bind(clonedEntry);
				clonedEntry.createdLocally = true;
				clonedEntry.verifySignatures = () => {
					verified = true;
					return verifyFn();
				};
				await db2.log.log.join([clonedEntry]);
				expect(verified).to.be.false;
			});

			it("logs are unique", async () => {
				const entryCount = 33;
				const entryArr: number[] = [];

				const db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						setup,
					},
				});
				const db3 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						setup,
					},
				});

				// Create the entries in the first database
				for (let i = 0; i < entryCount; i++) {
					entryArr.push(i);
				}

				await mapSeries(entryArr, (i) => db1.add("hello" + i));

				// Open the second database
				const db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							setup,
						},
					},
				))!;

				const db4 = (await EventStore.open<EventStore<string, any>>(
					db3.address!,
					session.peers[1],
					{
						args: {
							setup,
						},
					},
				))!;

				await waitForResolved(async () =>
					expect((await db2.iterator({ limit: -1 })).collect()).to.have.length(
						entryCount,
					),
				);

				const result1 = (await db1.iterator({ limit: -1 })).collect();
				const result2 = (await db2.iterator({ limit: -1 })).collect();

				expect(result1.length).equal(result2.length);
				for (let i = 0; i < result1.length; i++) {
					expect(result1[i].equals(result2[i])).to.be.true;
				}

				expect(db3.log.log.length).equal(0);
				expect(db4.log.log.length).equal(0);
			});

			it("can replicate many ranges", async () => {
				const startSize = await db1.log.replicationIndex.getSize();
				const toReplicate = 1600;
				let ranges: { factor: number; offset: number }[] = [];
				for (let i = 0; i < toReplicate; i++) {
					ranges.push({ factor: 0.0001, offset: Math.random() });
				}
				await db1.log.replicate(ranges);
				expect(await db1.log.replicationIndex.getSize()).to.eq(
					toReplicate + startSize,
				);
			});

			describe("references", () => {
				it("joins by references", async () => {
					db1.log.replicas = { min: new AbsoluteReplicas(1) };
					db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicas: {
									min: 1,
								},
								waitForReplicatorTimeout: 2000,
								setup,
							},
						},
					))!;
					await db1.log.replicate({ factor: 0.5 });
					await db2.log.replicate({ factor: 0.5 });

					const getParticipationPerPer = (
						ranges: ReplicationRangeIndexable<any>[],
					) => {
						let map = new Map<string, number>();
						for (const range of ranges) {
							map.set(
								range.hash,
								(map.get(range.hash) || 0) + range.widthNormalized,
							);
						}
						return map;
					};

					await waitForResolved(async () =>
						expect(
							[
								...getParticipationPerPer(
									(await db1.log.replicationIndex.iterate().all()).map(
										(x) => x.value,
									),
								).values(),
							].map((x) => Math.round(x * 200)),
						).to.deep.equal([100, 100]),
					);

					await waitForResolved(async () =>
						expect(
							[
								...getParticipationPerPer(
									(await db2.log.replicationIndex.iterate().all()).map(
										(x) => x.value,
									),
								).values(),
							].map((x) => Math.round(x * 200)),
						).to.deep.equal([100, 100]),
					);

					const { entry: entryA } = await db1.add("a", {
						meta: { next: [], gidSeed: new Uint8Array([1]) },
					});
					const { entry: entryB } = await db1.add("b", {
						meta: { next: [], gidSeed: new Uint8Array([0]) },
					});
					await db1.add("ab", {
						meta: { next: [entryA, entryB] },
					});

					await waitForResolved(() => {
						expect(Math.max(db1.log.log.length, db2.log.log.length)).equal(3); // one is now responsible for everything
						expect(Math.min(db1.log.log.length, db2.log.log.length)).equal(0); // one has to do nothing
					});
				});

				/* TODO feature not implemented yet, when is this expected?
				it("references all gids on exchange", async () => {
					const { entry: entryA } = await db1.add("a", { meta: { next: [] } });
					const { entry: entryB } = await db1.add("b", { meta: { next: [] } });
					const { entry: entryAB } = await db1.add("ab", {
						meta: { next: [entryA, entryB] },
					});
		
					expect(entryA.meta.gid).not.equal(entryB.meta.gid);
					expect(
						entryAB.meta.gid === entryA.meta.gid ||
							entryAB.meta.gid === entryB.meta.gid,
					).to.be.true;
		
					let entryWithNotSameGid =
						entryAB.meta.gid === entryA.meta.gid
							? entryB.meta.gid
							: entryA.meta.gid;
		
					const sendFn = db1.log.rpc.send.bind(db1.log.rpc);
		
					db1.log.rpc.send = async (msg, options) => {
						if (msg instanceof ExchangeHeadsMessage) {
							expect(msg.heads.map((x) => x.entry.hash)).to.deep.equal([
								entryAB.hash,
							]);
							expect(
								msg.heads.map((x) => x.gidRefrences.map((y) => y)).flat(),
							).to.deep.equal([entryWithNotSameGid]);
						}
						return sendFn(msg, options);
					};
		
					let cacheLookups: Entry<any>[][] = [];
					let db1GetShallowFn = db1.log["_gidParentCache"].get.bind(
						db1.log["_gidParentCache"],
					);
					db1.log["_gidParentCache"].get = (k) => {
						const result = db1GetShallowFn(k);
						cacheLookups.push(result!);
						return result;
					}; 
		
					db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									factor: 1,
								},
							},
						},
					))!;
		
					await waitForResolved(() => expect(db2.log.log.length).equal(3));
		
					expect(cacheLookups).to.have.length(1);
					expect(
						cacheLookups.map((x) => x.map((y) => y.meta.gid)).flat(),
					).to.have.members([entryWithNotSameGid, entryAB.meta.gid]);
		
					await db1.close();
					 expect(db1.log["_gidParentCache"].size).equal(0);
				}); 
				*/

				it("fetches next blocks once", async () => {
					db1 = await session.peers[0].open(new EventStore<string, any>(), {
						args: {
							replicas: {
								min: 0,
							},
							replicate: false,
							timeUntilRoleMaturity: 1000,
							setup,
						},
					});

					// followwing entries set minReplicas to 1 which means only db2 or db3 needs to hold it
					const e1 = await db1.add("0", {
						replicas: new AbsoluteReplicas(3),
						meta: { next: [] },
					});
					await db1.add("1", {
						replicas: new AbsoluteReplicas(1), // will be overriden by 'maxReplicas' above
						meta: { next: [e1.entry] },
					});

					db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicas: {
									min: 0,
								},

								replicate: {
									factor: 1,
								},
								timeUntilRoleMaturity: 1000,
								setup,
							},
						},
					))!;

					const onMessageFn1 = db1.log.onMessage.bind(db1.log);

					let receivedMessageDb1: InnerBlocksMessage[] = [];
					db1.log.rpc["_responseHandler"] = async (msg: any, cxt: any) => {
						if (msg instanceof BlocksMessage) {
							receivedMessageDb1.push(msg.message);
						}
						return onMessageFn1(msg, cxt);
					};

					let receivedMessageDb2: InnerBlocksMessage[] = [];
					const onMessageFn2 = db2.log.onMessage.bind(db2.log);
					db2.log.rpc["_responseHandler"] = async (msg: any, cxt: any) => {
						if (msg instanceof BlocksMessage) {
							receivedMessageDb2.push(msg.message);
						}
						return onMessageFn2(msg, cxt);
					};

					await waitForResolved(() => {
						expect(db1.log.log.length).equal(0);
						expect(db2.log.log.length).greaterThanOrEqual(1);
					});

					expect(receivedMessageDb1).to.have.length(1);
					expect(receivedMessageDb1[0]).to.be.instanceOf(BlockRequest);
					expect(receivedMessageDb2).to.have.length(1);
					expect(receivedMessageDb2[0]).to.be.instanceOf(BlockResponse);
				});
			});

			describe("replication", () => {
				describe("one way", () => {
					it("replicates database of 1 entry", async () => {
						const value = "hello";
						await db1.add(value);

						db2 = (await EventStore.open<EventStore<string, any>>(
							db1.address!,
							session.peers[1],
							{
								args: {
									setup,
									timeUntilRoleMaturity: DEFAULT_ROLE_MATURITY,
								},
							},
						))!;

						await db1.waitFor(session.peers[1].peerId);
						await db2.waitFor(session.peers[0].peerId);

						await waitForResolved(() => expect(db2.log.log.length).equal(1));

						expect((await db2.iterator({ limit: -1 })).collect().length).equal(
							1,
						);

						const db1Entries: Entry<Operation<string>>[] = (
							await db1.iterator({ limit: -1 })
						).collect();
						expect(db1Entries.length).equal(1);

						await waitForResolved(async () =>
							expect([
								...(
									await db1.log.findLeadersFromEntry(
										db1Entries[0],
										maxReplicas(db1.log, db1Entries),
									)
								).keys(),
							]).to.have.members([
								session.peers[0].identity.publicKey.hashcode(),
								session.peers[1].identity.publicKey.hashcode(),
							]),
						);

						expect(db1Entries[0].payload.getValue().value).equal(value);
						const db2Entries: Entry<Operation<string>>[] = (
							await db2.iterator({ limit: -1 })
						).collect();
						expect(db2Entries.length).equal(1);

						expect([
							...(
								await db2.log.findLeadersFromEntry(
									db2Entries[0],
									maxReplicas(db2.log, db2Entries),
								)
							).keys(),
						]).include.members([
							session.peers[0].identity.publicKey.hashcode(),
							session.peers[1].identity.publicKey.hashcode(),
						]);
						expect(db2Entries[0].payload.getValue().value).equal(value);
					});

					it("replicates database of 1000 entries", async () => {
						db2 = (await EventStore.open<EventStore<string, any>>(
							db1.address!,
							session.peers[1],
							{
								args: {
									replicate: {
										factor: 1,
									},
									setup,
								},
							},
						))!;

						await db1.waitFor(session.peers[1].peerId);
						await db2.waitFor(session.peers[0].peerId);

						const entryCount = 1e3;
						for (let i = 0; i < entryCount; i++) {
							//	entryArr.push(i);
							await db1.add("hello" + i, { meta: { next: [] } });
						}

						await waitForResolved(() =>
							expect(db2.log.log.length).equal(entryCount),
						);

						const entries = (await db2.iterator({ limit: -1 })).collect();
						expect(entries.length).equal(entryCount);
						for (let i = 0; i < entryCount; i++) {
							try {
								expect(entries[i].payload.getValue().value).equal("hello" + i);
							} catch (error) {
								console.error(
									"Entries out of order: " +
										entries.map((x) => x.payload.getValue().value).join(", "),
								);
								throw error;
							}
						}
					});

					it("distributes after merge", async () => {
						await session.stop();
						session = await TestSession.connected(3);

						await slowDownSend(session.peers[2], session.peers[0]); // we do this to potentially change the sync order
						const db1 = await session.peers[0].open(
							new EventStore<string, any>(),
							{
								args: {
									replicate: {
										factor: 1,
									},
									/* replicas: {
										min: 1,
									}, */
									timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
									setup,
								},
							},
						);

						await db1.add("hello");

						let db2 = (await EventStore.open<EventStore<string, any>>(
							db1.address!,
							session.peers[1],
							{
								args: {
									replicate: {
										factor: 1,
									},
									/* replicas: {
										min: 1,
									}, */
									timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
									setup,
								},
							},
						))!;

						let db3 = (await EventStore.open<EventStore<string, any>>(
							db1.address!,
							session.peers[2],
							{
								args: {
									replicate: {
										factor: 1,
									},
									/* replicas: {
										min: 1,
									}, */
									timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
									setup,
								},
							},
						))!;

						await waitForResolved(() => expect(db1.log.log.length).to.be.eq(1));
						await waitForResolved(() => expect(db2.log.log.length).to.be.eq(1));
						await waitForResolved(() => expect(db3.log.log.length).to.be.eq(1));
					});

					it("replicates database of large entries", async () => {
						let count = 10;
						for (let i = 0; i < count; i++) {
							const value = toBase64(randomBytes(4e6));
							await db1.add(value, { meta: { next: [] } }); // force unique heads
						}
						db2 = (await EventStore.open<EventStore<string, any>>(
							db1.address!,
							session.peers[1],
							{
								args: {
									replicate: {
										factor: 1,
									},
									setup,
								},
							},
						))!;

						await waitForResolved(() =>
							expect(db2.log.log.length).equal(count),
						);
					});

					it("replicates 1 entry with cut next", async () => {
						const first = await db1.add("old");
						const second = await db1.add("new", {
							meta: { type: EntryType.CUT, next: [first.entry] },
						});
						expect(
							(await db1.iterator({ limit: -1 })).collect().map((x) => x.hash),
						).to.deep.equal([second.entry.hash]);
						expect(db1.log.log.length).equal(1);

						db2 = (await EventStore.open<EventStore<string, any>>(
							db1.address!,
							session.peers[1],
							{
								args: {
									setup,
								},
							},
						))!;

						await waitForResolved(async () => {
							expect(
								(await db2.iterator({ limit: -1 }))
									.collect()
									.map((x) => x.hash),
							).to.deep.equal([second.entry.hash]);
						});
					});

					it("it does not fetch missing entries from remotes when exchanging heads to remote", async () => {
						const first = await db1.add("a", { meta: { next: [] } });
						const second = await db1.add("b", { meta: { next: [] } });
						await db1.log.log.entryIndex.delete(second.entry.hash);

						db2 = (await EventStore.open<EventStore<string, any>>(
							db1.address!,
							session.peers[1],
							{
								args: {
									setup,
								},
							},
						))!;

						let remoteFetchOptions: any[] = [];
						const db1LogGet = db1.log.log.get.bind(db1.log.log);

						db1.log.log.get = async (hash, options) => {
							if (hash === second.entry.hash) {
								remoteFetchOptions.push(options?.remote);
								return undefined;
							}
							return db1LogGet(hash, options);
						};

						await waitForResolved(async () => {
							expect(
								(await db2.iterator({ limit: -1 }))
									.collect()
									.map((x) => x.hash),
							).to.deep.equal([first.entry.hash]);
						});
						await waitForResolved(() =>
							expect(remoteFetchOptions).to.have.length(1),
						);
						expect(remoteFetchOptions[0]).to.be.undefined;
					});
				});
				describe("two way", () => {
					beforeEach(async () => {
						await session.stop();
						session = await TestSession.disconnected(2);

						const store = new EventStore<string, any>();
						db1 = await session.peers[0].open(store.clone(), {
							args: {
								replicate: {
									factor: 1,
								},
								timeUntilRoleMaturity: 0,
								setup,
							},
						});

						db2 = await session.peers[1].open(store.clone(), {
							args: {
								replicate: {
									factor: 1,
								},
								timeUntilRoleMaturity: 0,
								setup,
							},
						});

						expect(db1.log.replicas.min.getValue(db1.log)).to.eq(2);
						expect(db2.log.replicas.min.getValue(db1.log)).to.eq(2);
					});

					it("no change", async () => {
						await db1.log.replicate({ factor: 1 });
						await db2.log.replicate({ factor: 1 });

						let count = 1000;
						for (let i = 0; i < count; i++) {
							const { entry: entry1 } = await db1.add("hello " + i, {
								meta: { next: [] },
							});
							await db2.log.join([entry1]);
						}

						// dial for sync
						await db1.node.dial(db2.node.getMultiaddrs());

						await waitForResolved(() =>
							expect(db1.log.log.length).equal(count),
						);
						await waitForResolved(() =>
							expect(db2.log.log.length).equal(count),
						);
					});

					it("partially synced one", async () => {
						const { entry: entry1 } = await db1.add("a", {
							meta: { next: [] },
						});
						const { entry: entry2 } = await db2.add("b", {
							meta: { next: [] },
						});

						await db1.log.join([entry2]);
						await db2.log.join([entry1]);

						// now ad som unsynced entries

						await db1.add("c", { meta: { next: [] } });
						await db2.add("d", { meta: { next: [] } });

						// dial for sync
						await db1.node.dial(db2.node.getMultiaddrs());

						await waitForResolved(() => expect(db1.log.log.length).equal(4));
						await waitForResolved(() => expect(db2.log.log.length).equal(4));
					});

					it("partially synced large", async () => {
						let alreadySyncCount = 1000;
						let unsyncedCount = 1;
						let totalCount = alreadySyncCount + unsyncedCount * 2;
						for (let i = 0; i < alreadySyncCount; i++) {
							const { entry } = await db1.add("hello-ab- " + i, {
								meta: { next: [] },
							});
							await db2.log.join([entry]);
						}

						expect(db1.log.log.length).equal(alreadySyncCount);
						expect(db2.log.log.length).equal(alreadySyncCount);

						expect(await db1.log.entryCoordinatesIndex.getSize()).to.equal(
							alreadySyncCount,
						);
						expect(await db2.log.entryCoordinatesIndex.getSize()).to.equal(
							alreadySyncCount,
						);

						for (let i = 0; i < unsyncedCount; i++) {
							await db1.add("hello-a- " + i, { meta: { next: [] } });
							await db2.add("hello-b- " + i, { meta: { next: [] } });
						}

						await db1.node.dial(db2.node.getMultiaddrs());
						await waitForResolved(() =>
							expect(db1.log.log.length).equal(totalCount),
						);
						await waitForResolved(() =>
							expect(db2.log.log.length).equal(totalCount),
						);
					});
				});
			});
		});

		describe("redundancy", () => {
			let session: TestSession;
			let db1: EventStore<string, any>,
				db2: EventStore<string, any>,
				db3: EventStore<string, any>;

			let fetchEvents: number;
			let fetchHashes: Set<string>;
			let fromMultihash: any;

			before(async () => {
				session = await TestSession.connected(3);
				fromMultihash = Entry.fromMultihash;
				// TODO monkeypatching might lead to sideeffects in other tests!
				Entry.fromMultihash = (s, h, o) => {
					fetchHashes.add(h);
					fetchEvents += 1;
					return fromMultihash(s, h, o);
				};
			});
			after(async () => {
				await session.stop();
			});

			beforeEach(() => {
				fetchEvents = 0;
				fetchHashes = new Set();
			});
			afterEach(async () => {
				if (db1 && db1.closed === false) await db1.drop();
				if (db2 && db2.closed === false) await db2.drop();
				if (db3 && db3.closed === false) await db3.drop();
			});

			it("only sends entries once, 2 peers dynamic", async () => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						setup,
					},
				});
				await db1.log.replicate();
				let count = 100;
				for (let i = 0; i < count; i++) {
					await db1.add("hello " + i, { meta: { next: [] } });
				}
				const message1 = collectMessages(db1.log);

				let db2 = db1.clone();

				// start to collect messages before opening the second db so we don't miss any
				const { messages: message2, fn } = collectMessagesFn(db2.log);
				db2 = await session.peers[1].open(db2, {
					args: {
						replicate: true,
						onMessage: fn,
						setup,
					},
				});

				const check = () => {
					const dataMessages2 = getReceivedHeads(message2);
					expect(dataMessages2).to.have.length(count);

					const dataMessages1 = getReceivedHeads(message1);
					expect(dataMessages1).to.be.empty; // no data is sent back
				};

				await waitForResolved(() => {
					check();
				});
				await delay(3000);
				check();
			});

			it("only sends entries once, 2 peers fixed", async () => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						setup,
					},
				});
				db1.log.replicate({ factor: 1 });
				let count = 1000;
				for (let i = 0; i < count; i++) {
					await db1.add("hello " + i, { meta: { next: [] } });
				}
				const message1 = collectMessages(db1.log);

				db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								factor: 1,
							},
							setup,
						},
					},
				))!;

				const message2 = collectMessages(db2.log);
				await delay(3000);

				const dataMessages2 = getReceivedHeads(message2);
				await waitForResolved(() =>
					expect(dataMessages2).to.have.length(count),
				);

				const dataMessages1 = getReceivedHeads(message1);
				expect(dataMessages1).to.be.empty; // no data is sent back
			});

			it("only sends entries once, 2 peers fixed, write after open", async () => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: { factor: 1 },
						setup,
					},
				});
				let count = 1;
				const message1 = collectMessages(db1.log);

				db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								factor: 1,
							},
							setup,
						},
					},
				))!;

				const message2 = collectMessages(db2.log);

				await waitForResolved(async () =>
					expect((await db1.log.getReplicators())?.size).equal(2),
				);

				await waitForResolved(async () =>
					expect((await db2.log.getReplicators())?.size).equal(2),
				);

				await db1.add("hello", { meta: { next: [] } });

				await waitForResolved(() => expect(db2.log.log.length).equal(1));

				const dataMessages2 = getReceivedHeads(message2);
				await waitForResolved(() =>
					expect(dataMessages2).to.have.length(count),
				);

				const dataMessages1 = getReceivedHeads(message1);
				expect(dataMessages1).to.be.empty; // no data is sent back
			});

			it("only sends entries once, 3 peers", async () => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							factor: 1,
						},
						setup,
					},
				});
				const message1 = collectMessages(db1.log);

				db2 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								factor: 1,
							},
							setup,
						},
					},
				);
				const message2 = collectMessages(db2.log);

				let count = 10; // TODO make higher count work in Github CI

				for (let i = 0; i < count; i++) {
					await db1.add("hello " + i, { meta: { next: [] } });
				}
				await waitForResolved(() => expect(db2.log.log.length).equal(count));

				db3 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[2],
					{
						args: {
							replicate: {
								factor: 1,
							},
							setup,
						},
					},
				);
				const message3 = collectMessages(db3.log);

				await waitForResolved(() => expect(db3.log.log.length).equal(count));

				const heads = getReceivedHeads(message3);
				expect(heads).to.have.length(count);

				expect(getReceivedHeads(message1)).to.be.empty;
				expect(getReceivedHeads(message2)).to.have.length(count);

				await delay(3000); // wait for potential additional messages

				expect(getReceivedHeads(message1)).to.be.empty;
				expect(getReceivedHeads(message2)).to.have.length(count);

				await waitForResolved(() => expect(db3.log.log.length).equal(count));

				// gc check,.
				// TODO dont do this, this way
				/* await waitForResolved(() => {
					expect((db3.log.syncronizer as any)["syncInFlightQueue"].size).equal(
						0,
					);
					expect(
						(db3.log.syncronizer as any)["syncInFlightQueueInverted"].size,
					).equal(0);
				});
*/
				expect(db3.log.syncronizer.pending).to.eq(0);
			});

			it("no fetches needed when replicating live ", async () => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						setup,
					},
				});

				db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							setup,
						},
					},
				))!;

				await db1.waitFor(session.peers[1].peerId);
				await db2.waitFor(session.peers[0].peerId);

				const entryCount = 10; // todo when larger (N) this test usually times out at N - 1 or 2, unless a delay is put beforehand

				// Trigger replication
				let adds: number[] = [];
				for (let i = 0; i < entryCount; i++) {
					adds.push(i);
					await db1.add("hello " + i, { meta: { next: [] } });
					// TODO when nexts is omitted, entrise will dependon each other,
					// When entries arrive in db2 unecessary fetches occur because there is already a sync in progress?
				}

				//await mapSeries(adds, (i) => db1.add("hello " + i));

				// All entries should be in the database
				await waitForResolved(() =>
					expect(db2.log.log.length).equal(entryCount),
				);

				// All entries should be in the database
				expect(db2.log.log.length).equal(entryCount);

				// progress events should increase monotonically
				expect(fetchEvents).equal(fetchHashes.size);
				expect(fetchEvents).equal(0); // becausel all entries were sent
			});
			it("fetches only once after open", async () => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						setup,
					},
				});

				const entryCount = 15;

				// Trigger replication
				const adds: number[] = [];
				for (let i = 0; i < entryCount; i++) {
					adds.push(i);
				}

				const add = async (i: number) => {
					await db1.add("hello " + i);
				};

				await mapSeries(adds, add);

				db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							setup,
						},
					},
				))!;

				// All entries should be in the database
				await waitForResolved(() =>
					expect(db2.log.log.length).equal(entryCount),
				);

				// progress events should (increase monotonically)
				expect((await db2.iterator({ limit: -1 })).collect().length).equal(
					entryCount,
				);
				expect(fetchEvents).equal(fetchHashes.size);
				expect(fetchEvents).equal(entryCount - 1); // - 1 because we also send some references for faster syncing (see exchange-heads.ts)
			});
		});

		describe(`start/stop`, function () {
			let session: TestSession;
			beforeEach(async () => {
				session = await TestSession.connected(3);
			});

			afterEach(async () => {
				await session.stop();
			});

			it("replicate on connect", async () => {
				const entryCount = 10;
				const entryArr: number[] = [];
				const db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							factor: 1,
						},
						setup,
					},
				});

				// Create the entries in the first database
				for (let i = 0; i < entryCount; i++) {
					entryArr.push(i);
				}

				await mapSeries(entryArr, (i) => db1.add("hello" + i));

				// Open the second database
				const db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								factor: 1,
							},
							setup,
						},
					},
				))!;
				try {
					await waitForResolved(async () =>
						expect(db2.log.log.length).equal(entryCount),
					);
				} catch (error) {
					await dbgLogs([db1.log, db2.log]);
					throw error;
				}
				const result1 = (await db1.iterator({ limit: -1 })).collect();
				const result2 = (await db2.iterator({ limit: -1 })).collect();
				expect(result1.length).equal(result2.length);
				for (let i = 0; i < result1.length; i++) {
					expect(result1[i].equals(result2[i])).to.be.true;
				}
			});

			it("can restart replicate", async () => {
				const db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							factor: 1,
						},
						setup,
					},
				});

				await db1.add("hello");

				let db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								factor: 1,
							},
							setup,
						},
					},
				))!;

				await waitForResolved(() => expect(db2.log.log.length).equal(1));

				await db2.close();
				await db1.add("world");
				db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								factor: 1,
							},
							setup,
						},
					},
				))!;
				await waitForResolved(() => expect(db2.log.log.length).equal(2));
			});

			/* 	it("start stop many times", async () => {
					const iterations = 1000;
					for (let i = 0; i < iterations; i++) {
						await session.stop()
						session = await TestSession.connected(3, [
							{
								libp2p: {
									privateKey: await privateKeyFromRaw(
										new Uint8Array([
											237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214, 122,
											28, 157, 208, 163, 15, 215, 104, 193, 151, 177, 62, 231, 253,
											120, 122, 222, 174, 242, 120, 50, 165, 97, 8, 235, 97, 186,
											148, 251, 100, 168, 49, 10, 119, 71, 246, 246, 174, 163, 198,
											54, 224, 6, 174, 212, 159, 187, 2, 137, 47, 192,
										]),
									),
								},
							},
							{
								libp2p: {
									privateKey: privateKeyFromRaw(
										new Uint8Array([
											27, 246, 37, 180, 13, 75, 242, 124, 185, 205, 207, 9, 16, 54,
											162, 197, 247, 25, 211, 196, 127, 198, 82, 19, 68, 143, 197,
											8, 203, 18, 179, 181, 105, 158, 64, 215, 56, 13, 71, 156, 41,
											178, 86, 159, 80, 222, 167, 73, 3, 37, 251, 67, 86, 6, 90,
											212, 16, 251, 206, 54, 49, 141, 91, 171,
										]),
									),
								},
							},
	
							{
								libp2p: {
									privateKey: privateKeyFromRaw(
										new Uint8Array([
											204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91, 229,
											157, 168, 15, 45, 242, 144, 98, 75, 58, 208, 9, 223, 143, 251,
											52, 252, 159, 64, 83, 52, 197, 24, 246, 24, 234, 141, 183,
											151, 82, 53, 142, 57, 25, 148, 150, 26, 209, 223, 22, 212, 40,
											201, 6, 191, 72, 148, 82, 66, 138, 199, 185,
										]),
									),
								},
							},
						]);
	
					
	
	
						let db1 = await session.peers[0].open(new EventStore<string, any>(), {
							args: {
								replicate: {
									factor: 0.333,
									offset: 0.333,
								},
								setup,
							},
						});
	
						let db2 = await EventStore.open<EventStore<string, any>>(
							db1.address!,
							session.peers[1],
							{
								args: {
									replicate: {
										factor: 0.333,
										offset: 0,
									},
									setup,
								},
							},
						);
						let db3 = await EventStore.open<EventStore<string, any>>(
							db1.address!,
							session.peers[2],
							{
								args: {
									replicate: {
										factor: 0.333,
										offset: 0.666,
									},
									setup,
								},
							},
						);
	
	
	
						try {
							await waitForResolved(async () =>
								expect(await db1.log.replicationIndex?.getSize()).equal(3),
							);
							await waitForResolved(async () =>
								expect(await db2.log.replicationIndex?.getSize()).equal(3),
							);
							await waitForResolved(async () =>
								expect(await db3.log.replicationIndex?.getSize()).equal(3),
							);
	
						} catch (error) {
							throw error;
						}
	
						if (db1 && db1.closed === false) await db1.drop();
	
						if (db2 && db2.closed === false) await db2.drop();
	
						if (db3 && db3.closed === false) await db3.drop();
	
	
					}
	
				}) */
		});

		describe("canReplicate", () => {
			let session: TestSession;
			let db1: EventStore<string, any>,
				db2: EventStore<string, any>,
				db3: EventStore<string, any>;

			const init = async (
				canReplicate: (publicKey: PublicSignKey) => Promise<boolean> | boolean,
				replicate: ReplicationOptions = { factor: 1 },
			) => {
				let min = 100;
				let max = undefined;
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicas: {
							min,
							max,
						},
						replicate,
						canReplicate,
						setup,
					},
				});
				db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicas: {
								min,
								max,
							},
							replicate,
							canReplicate,
							setup,
						},
					},
				))!;

				db3 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[2],
					{
						args: {
							replicas: {
								min,
								max,
							},
							replicate,
							canReplicate,
							setup,
						},
					},
				))!;

				await db1.waitFor(session.peers[1].peerId);
				await db2.waitFor(session.peers[0].peerId);
				await db3.waitFor(session.peers[0].peerId);
			};
			beforeEach(async () => {
				session = await TestSession.connected(3);
				db1 = undefined as any;
				db2 = undefined as any;
				db3 = undefined as any;

				// Ensure no leaked static args
				(EventStore as any).staticArgs = undefined;
			});

			afterEach(async () => {
				if (db1) await db1.drop();

				if (db2) await db2.drop();

				if (db3) await db3.drop();

				await session.stop();
			});

			it("can filter unwanted replicators", async () => {
				// allow all replicaotors except node 0
				await init((key) => !key.equals(session.peers[0].identity.publicKey));

				const expectedReplicators = [
					session.peers[1].identity.publicKey.hashcode(),
					session.peers[2].identity.publicKey.hashcode(),
				];

				await Promise.all(
					[db1, db2, db3].map((db) =>
						waitForResolved(async () =>
							expect([...(await db.log.getReplicators())]).to.have.members(
								expectedReplicators,
							),
						),
					),
				);

				const unionFromPeer0 = await db1.log.getCover(
					{ args: undefined },
					{
						roleAge: 0,
					},
				);
				let selfIndex = unionFromPeer0.findIndex(
					(x) => x === db1.node.identity.publicKey.hashcode(),
				);

				// should always include self in the cover set, also include one of the remotes since their replication factor is 1
				expect([
					db2.node.identity.publicKey.hashcode(),
					db3.node.identity.publicKey.hashcode(),
				]).include(unionFromPeer0[selfIndex === 0 ? 1 : 0]);
				expect(unionFromPeer0).to.have.length(2);

				// the other ones should only have to cover themselves
				await Promise.all(
					[db2, db3].map((log) =>
						waitForResolved(async () =>
							expect(
								await log.log.getCover({ args: undefined }, { roleAge: 0 }),
							).to.have.members([log.node.identity.publicKey.hashcode()]),
						),
					),
				);

				await db2.add("hello");

				await waitForResolved(() => {
					expect(db2.log.log.length).equal(1);
					expect(db3.log.log.length).equal(1);
				});
				await delay(1000); // Add some delay so that all replication events most likely have occured
				expect(db1.log.log.length).equal(0); // because not trusted for replication job
			});

			/* TODO feat(?)
			
			it("replicate even if not allowed if factor is 1 ", async () => {
				await init(() => false, { factor: 1 });
		
				const mySegments = await db1.log.getMyReplicationSegments();
				expect(mySegments).to.have.length(1);
				expect(mySegments[0].widthNormalized).to.equal(1);
			}); */

			it("does not replicate if not allowed and dynamic ", async () => {
				await init(() => false, true);

				const mySegments = await db1.log.getMyReplicationSegments();
				expect(mySegments).to.have.length(0);
			});
		});

		describe("replication degree", function () {
			this.timeout(120_000);
			let session: TestSession;
			let db1: EventStore<string, any>,
				db2: EventStore<string, any>,
				db3: EventStore<string, any>;

			const init = async (props: {
				min: number;
				max?: number;
				beforeOther?: () => Promise<any> | void;
				waitForPruneDelay?: number;
			}) => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicas: props,
						replicate: false,
						timeUntilRoleMaturity: 1000,
						setup,
						waitForPruneDelay: props?.waitForPruneDelay || 5e3,
					},
				});

				await props.beforeOther?.();
				db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicas: props,
							replicate: {
								factor: 0.5,
								offset: 0,
							},
							timeUntilRoleMaturity: 1000,
							setup,
							waitForPruneDelay: props?.waitForPruneDelay ?? 5e3,
						},
					},
				))!;

				db3 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[2],
					{
						args: {
							replicas: props,

							replicate: {
								factor: 0.5,
								offset: 0.5,
							},
							timeUntilRoleMaturity: 1000,
							setup,
							waitForPruneDelay: 5e3,
						},
					},
				))!;

				await db1.waitFor(session.peers[1].peerId);
				await db2.waitFor(session.peers[0].peerId);
				await db2.waitFor(session.peers[2].peerId);
				await db3.waitFor(session.peers[0].peerId);
			};

			beforeEach(async () => {
				session = await TestSession.connected(3, [
					{
						libp2p: {
							privateKey: await keys.privateKeyFromRaw(
								new Uint8Array([
									237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214, 122,
									28, 157, 208, 163, 15, 215, 104, 193, 151, 177, 62, 231, 253,
									120, 122, 222, 174, 242, 120, 50, 165, 97, 8, 235, 97, 186,
									148, 251, 100, 168, 49, 10, 119, 71, 246, 246, 174, 163, 198,
									54, 224, 6, 174, 212, 159, 187, 2, 137, 47, 192,
								]),
							),
						},
					},
					{
						libp2p: {
							privateKey: keys.privateKeyFromRaw(
								new Uint8Array([
									27, 246, 37, 180, 13, 75, 242, 124, 185, 205, 207, 9, 16, 54,
									162, 197, 247, 25, 211, 196, 127, 198, 82, 19, 68, 143, 197,
									8, 203, 18, 179, 181, 105, 158, 64, 215, 56, 13, 71, 156, 41,
									178, 86, 159, 80, 222, 167, 73, 3, 37, 251, 67, 86, 6, 90,
									212, 16, 251, 206, 54, 49, 141, 91, 171,
								]),
							),
						},
					},

					{
						libp2p: {
							privateKey: keys.privateKeyFromRaw(
								new Uint8Array([
									204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91, 229,
									157, 168, 15, 45, 242, 144, 98, 75, 58, 208, 9, 223, 143, 251,
									52, 252, 159, 64, 83, 52, 197, 24, 246, 24, 234, 141, 183,
									151, 82, 53, 142, 57, 25, 148, 150, 26, 209, 223, 22, 212, 40,
									201, 6, 191, 72, 148, 82, 66, 138, 199, 185,
								]),
							),
						},
					},
				]);
				db1 = undefined as any;
				db2 = undefined as any;
				db3 = undefined as any;
			});

			afterEach(async () => {
				if (db1 && db1.closed === false) await db1.drop();

				if (db2 && db2.closed === false) await db2.drop();

				if (db3 && db3.closed === false) await db3.drop();

				await session.stop();
			});

			it("will not prune below replication degree", async () => {
				let replicas = 2;
				const db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: false,
						replicas: {
							min: replicas,
						},
						setup,
					},
				});

				let db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								factor: 1,
							},
							replicas: {
								min: replicas,
							},
							setup,
						},
					},
				))!;

				await db1.add("hello");

				await waitForResolved(() => expect(db2.log.log.length).equal(1));
				await delay(3e3);
				expect(db1.log.log.length).equal(1);
			});

			it("prune on insert many", async () => {
				const pruneDelay = 5_000;
				await init({ min: 1, waitForPruneDelay: pruneDelay });
				let count = 100;
				for (let i = 0; i < count; i++) {
					await db1.add("hello", {
						meta: { next: [] },
					});
				}

				try {
					await waitForResolved(() =>
						expect(db2.log.log.length + db3.log.log.length).equal(count),
					);
					await waitForResolved(
						async () => {
							expect((await db1.log.getPrunable()).length).equal(0);
							expect(db1.log.log.length).equal(0);
						},
						{ timeout: pruneDelay + 30_000, delayInterval: 200 },
					);
				} catch (error) {
					await dbgLogs([db1.log, db2.log, db3.log]);
					const prunable = await db1.log.getPrunable();
					if (prunable.length > 0) {
						console.error("Remaining prunable count:", prunable.length);
					}
					throw error;
				}
			});

			it("will prune on before join", async () => {
				const pruneDelay = 5_000;
				await init({
					min: 1,
					waitForPruneDelay: pruneDelay,
					beforeOther: () => db1.add("hello"),
				});
				await waitForResolved(
					async () => {
						expect((await db1.log.getPrunable()).length).equal(0);
						expect(db1.log.log.length).equal(0);
					},
					{ timeout: pruneDelay + 10_000, delayInterval: 200 },
				);
				await waitForResolved(() =>
					expect(db2.log.log.length + db3.log.log.length).equal(1),
				);
			});
			it("will prune on put 300 before join", async () => {
				const pruneDelay = 5_000;
				let count = 100;
				await init({
					min: 1,
					waitForPruneDelay: pruneDelay,
					beforeOther: async () => {
						for (let i = 0; i < count; i++) {
							await db1.add("hello", {
								meta: { next: [] },
							});
						}
					},
				});

				try {
					await waitForResolved(() =>
						expect(db2.log.log.length + db3.log.log.length).equal(count),
					);
					await waitForResolved(
						async () => {
							expect((await db1.log.getPrunable()).length).equal(0);
							expect(db1.log.log.length).equal(0);
						},
						{ timeout: pruneDelay + 90_000, delayInterval: 200 },
					);
				} catch (error) {
					await dbgLogs([db1.log, db2.log, db3.log]);
					const pending = (db1.log as any)?.["_pendingDeletes"];
					if (pending) {
						console.error("pending deletes", pending.size);
					}
					throw error;
				}
			});

			it("will prune on put 300 after join", async () => {
				const pruneDelay = 5_000;
				await init({ min: 1, waitForPruneDelay: pruneDelay });

				let count = 300;
				for (let i = 0; i < count; i++) {
					await db1.add("hello", {
						meta: { next: [] },
					});
				}

				try {
					await waitForResolved(() =>
						expect(db2.log.log.length + db3.log.log.length).equal(count),
					);
					await waitForResolved(
						async () => {
							expect((await db1.log.getPrunable()).length).equal(0);
							expect(db1.log.log.length).equal(0);
						},
						{ timeout: pruneDelay + 90_000, delayInterval: 200 },
					);
				} catch (error) {
					await dbgLogs([db1.log, db2.log, db3.log]);
					const pending = (db1.log as any)?.["_pendingDeletes"];
					if (pending) {
						console.error("pending deletes", pending.size);
					}
					throw error;
				}
			});

			it("will prune when join with partial coverage", async () => {
				const db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: false,
						replicas: {
							min: 1,
						},
						setup,
						timeUntilRoleMaturity: DEFAULT_ROLE_MATURITY,
					},
				});

				const entry = (await db1.add("hello"))!.entry;
				let db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								offset: 0,
								factor: 1,
								normalized: false,
							},

							replicas: {
								min: 1,
							},
							timeUntilRoleMaturity: DEFAULT_ROLE_MATURITY,
							setup,
						},
					},
				))!;

				try {
					const coordinate = (await db1.log.createCoordinates(entry, 1))[0];
					await db2.log.replicate({
						offset: coordinate,
						factor: 1,
						normalized: false,
					});
					await waitForResolved(() => expect(db1.log.log.length).equal(0));
					await waitForResolved(() => expect(db2.log.log.length).equal(1));
				} catch (error) {
					await dbgLogs([db1.log, db2.log]);
					throw error;
				}
			});

			it("will prune when join with complete coverage", async () => {
				const db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: false,
						replicas: {
							min: 1,
						},
						setup,
					},
				});

				await db1.add("hello");
				let db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								offset: 0,
								factor: 1,
								normalized: true,
							},

							replicas: {
								min: 1,
							},
							setup,
						},
					},
				))!;

				await waitForResolved(() => expect(db1.log.log.length).equal(0));
				await waitForResolved(() => expect(db2.log.log.length).equal(1));
			});

			it("will prune when join even if rapidly updating", async () => {
				let timeUntilRoleMaturity = 2e3;

				const db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: false,
						replicas: {
							min: 1,
						},
						setup,
						timeUntilRoleMaturity,
					},
				});

				await db1.add("hello");
				let db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: false,

							replicas: {
								min: 1,
							},
							setup,
							timeUntilRoleMaturity,
						},
					},
				))!;

				let rangeId = randomBytes(32);

				let i = 0;
				let factorStart = numbers.maxValue;
				let interval = setInterval(async () => {
					await db2.log.replicate({
						id: rangeId,
						factor:
							(factorStart as any) -
							((typeof factorStart === "bigint" ? BigInt(i) : i) as any),
						offset: 0,
						normalized: false,
					});
					i++;
				}, 500);

				try {
					await waitForResolved(() => expect(db1.log.log.length).equal(0));
					await waitForResolved(() => expect(db2.log.log.length).equal(1));
				} finally {
					clearInterval(interval);
				}
			});

			it("will prune on insert after join 2 peers", async () => {
				const db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							offset: 0,
							factor: 0,
							normalized: false,
						},
						replicas: {
							min: 1,
						},
						setup,
					},
				});

				let db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								offset: 0,
								factor: 1,
								normalized: false,
							},

							replicas: {
								min: 1,
							},
							setup,
						},
					},
				))!;

				await db1.add("hello");

				try {
					await waitForResolved(() => expect(db1.log.log.length).equal(0));
					await waitForResolved(() => expect(db2.log.log.length).equal(1));
				} catch (error) {
					await dbgLogs([db1.log, db2.log]);
					throw error;
				}
			});

			it("will prune once reaching max replicas", async () => {
				await session.stop();

				session = await TestSession.disconnected(3, [
					{
						libp2p: {
							privateKey: keys.privateKeyFromRaw(
								new Uint8Array([
									48, 245, 17, 66, 32, 106, 72, 98, 203, 253, 86, 138, 133, 155,
									243, 214, 8, 11, 14, 230, 18, 126, 173, 3, 62, 252, 92, 46,
									214, 0, 226, 184, 104, 58, 22, 118, 214, 182, 125, 233, 106,
									94, 13, 16, 6, 164, 236, 215, 159, 135, 117, 8, 240, 168, 169,
									96, 38, 86, 213, 250, 103, 183, 38, 205,
								]),
							),
						},
					},
					{
						libp2p: {
							privateKey: keys.privateKeyFromRaw(
								new Uint8Array([
									113, 203, 231, 235, 7, 120, 3, 194, 138, 113, 131, 40, 251,
									158, 121, 38, 190, 114, 116, 252, 100, 202, 107, 97, 119, 184,
									24, 56, 27, 76, 150, 62, 132, 22, 246, 177, 200, 6, 179, 117,
									218, 216, 120, 235, 147, 249, 48, 157, 232, 161, 145, 3, 63,
									158, 217, 111, 65, 105, 99, 83, 4, 113, 62, 15,
								]),
							),
						},
					},
					{
						libp2p: {
							privateKey: keys.privateKeyFromRaw(
								new Uint8Array([
									27, 246, 37, 180, 13, 75, 242, 124, 185, 205, 207, 9, 16, 54,
									162, 197, 247, 25, 211, 196, 127, 198, 82, 19, 68, 143, 197,
									8, 203, 18, 179, 181, 105, 158, 64, 215, 56, 13, 71, 156, 41,
									178, 86, 159, 80, 222, 167, 73, 3, 37, 251, 67, 86, 6, 90,
									212, 16, 251, 206, 54, 49, 141, 91, 171,
								]),
							),
						},
					},
				]);

				let minReplicas = 2;
				let maxReplicas = 2;

				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicas: {
							min: minReplicas,
							max: maxReplicas,
						},
						replicate: {
							offset: 0,
							factor: 0.333,
						},
						setup,
						timeUntilRoleMaturity: 0,
					},
				});
				db2 = (await session.peers[1].open(db1.clone(), {
					args: {
						replicas: {
							min: minReplicas,
							max: maxReplicas,
						},
						replicate: {
							offset: 0.333,
							factor: 0.666,
						},
						setup,
						timeUntilRoleMaturity: 0,
					},
				}))!;

				db3 = (await session.peers[2].open(db1.clone(), {
					args: {
						replicas: {
							min: minReplicas,
							max: maxReplicas,
						},
						replicate: {
							offset: 0.666,
							factor: 0.333,
						},
						setup,
						timeUntilRoleMaturity: 0,
					},
				}))!;

				const entryCount = 100;
				for (let i = 0; i < entryCount; i++) {
					await db1.add("hello", {
						replicas: new AbsoluteReplicas(3), // will be overriden by 'maxReplicas' above
						meta: { next: [] },
					});
				}

				// TODO why is this needed?
				await waitForResolved(() =>
					session.peers[1].dial(session.peers[0].getMultiaddrs()),
				);

				await waitForResolved(() =>
					expect(db2.log.log.length).equal(entryCount),
				);

				await db2.close();

				session.peers[2].dial(session.peers[0].getMultiaddrs());

				await waitForResolved(() =>
					expect(db3.log.log.length).to.eq(entryCount),
				);

				// reopen db2 again and make sure either db3 or db2 drops the entry (not both need to replicate)
				await delay(2000);

				db2 = await session.peers[1].open(db2.clone(), {
					args: {
						replicas: {
							min: minReplicas,
							max: maxReplicas,
						},
						replicate: {
							offset: 0.333,
							factor: 0.666,
						},
						setup,
					},
				});

				// await db1.log["pruneDebouncedFn"]();
				//await db1.log.waitForPruned()

				await waitForResolved(() => {
					expect(db1.log.log.length).to.be.lessThan(entryCount);
				});
			});

			describe("commit options", () => {
				it("control per commmit put before join", async () => {
					const entryCount = 100;

					await init({
						min: 1,
						beforeOther: async () => {
							const value = "hello";
							for (let i = 0; i < entryCount; i++) {
								await db1.add(value, {
									replicas: new AbsoluteReplicas(3),
									meta: { next: [] },
								});
							}
						},
					});

					const check = async (log: EventStore<string, any>) => {
						let replicated3Times = 0;
						for (const entry of await log.log.log.toArray()) {
							if (decodeReplicas(entry).getValue(db2.log) === 3) {
								replicated3Times += 1;
							}
						}
						expect(replicated3Times).equal(entryCount);
					};

					await waitForResolved(() => check(db2));
					await waitForResolved(() => check(db3));
				});

				it("control per commmit", async () => {
					const entryCount = 100;

					await init({
						min: 1,
					});

					const value = "hello";
					for (let i = 0; i < entryCount; i++) {
						await db1.add(value, {
							replicas: new AbsoluteReplicas(3),
							meta: { next: [] },
						});
					}

					const check = async (log: EventStore<string, any>) => {
						let replicated3Times = 0;
						for (const entry of await log.log.log.toArray()) {
							if (decodeReplicas(entry).getValue(db2.log) === 3) {
								replicated3Times += 1;
							}
						}
						expect(replicated3Times).equal(entryCount);
					};

					await waitForResolved(() => check(db2));
					await waitForResolved(() => check(db3));
				});

				it("mixed control per commmit", async () => {
					await init({ min: 1 });

					const value = "hello";

					const entryCount = 100;
					for (let i = 0; i < entryCount; i++) {
						await db1.add(value, {
							replicas: new AbsoluteReplicas(1),
							meta: { next: [] },
						});
						await db1.add(value, {
							replicas: new AbsoluteReplicas(3),
							meta: { next: [] },
						});
					}

					// expect e1 to be replicated at db1 and/or 1 other peer (when you write you always store locally)
					// expect e2 to be replicated everywhere
					const check = async (log: EventStore<string, any>) => {
						let replicated3Times = 0;
						let other = 0;
						for (const entry of await log.log.log.toArray()) {
							if (decodeReplicas(entry).getValue(db2.log) === 3) {
								replicated3Times += 1;
							} else {
								other += 1;
							}
						}
						expect(replicated3Times).equal(entryCount);
						expect(other).greaterThan(0);
					};
					await waitForResolved(() => check(db2));
					await waitForResolved(() => check(db3));
				});

				it("will index replication underflow degree", async () => {
					db1 = await session.peers[0].open(new EventStore<string, any>(), {
						args: {
							replicas: {
								min: 4,
							},
							replicate: false,
							timeUntilRoleMaturity: 1000,
							setup,
						},
					});

					db2 = await session.peers[1].open<EventStore<string, any>>(
						db1.address,
						{
							args: {
								replicas: {
									min: 4,
								},
								replicate: {
									factor: 1,
								},
								timeUntilRoleMaturity: 1000,
								setup,
							},
						},
					);

					await db1.add("hello", {
						replicas: new AbsoluteReplicas(4),
					});

					try {
						await waitForResolved(() => expect(db1.log.log.length).equal(1));
						await waitForResolved(() => expect(db2.log.log.length).equal(1));
					} catch (error) {
						await dbgLogs([db1.log, db2.log]);
						throw error;
					}

					const indexedDb1 = await db1.log.entryCoordinatesIndex
						.iterate()
						.all();
					const indexedDb2 = await db2.log.entryCoordinatesIndex
						.iterate()
						.all();

					const assignedToRangeBoundaryDb1 = indexedDb1.filter(
						(x) => x.value.assignedToRangeBoundary,
					);
					expect(assignedToRangeBoundaryDb1).to.have.length(1);
					expect(
						assignedToRangeBoundaryDb1[0].value.coordinates,
					).to.have.length(4);
					const assignedToRangeBoundaryDb2 = indexedDb2.filter(
						(x) => x.value.assignedToRangeBoundary,
					);
					expect(assignedToRangeBoundaryDb2).to.have.length(1);
					expect(
						assignedToRangeBoundaryDb2[0].value.coordinates,
					).to.have.length(4);
				});
			});

			it("min replicas with be maximum value for gid", async () => {
				await init({ min: 1 });

				await delay(3e3); // TODO this test fails without this delay, FIX THIS inconsitency. Calling rebalance all on db1 also seem to work

				// followwing entries set minReplicas to 1 which means only db2 or db3 needs to hold it
				const entryCount = 1e2;
				for (let i = 0; i < entryCount / 2; i++) {
					const e1 = await db1.add(String(i), {
						replicas: new AbsoluteReplicas(3),
						meta: { next: [] },
					});
					await db1.add(String(i), {
						replicas: new AbsoluteReplicas(1), // will be overriden by 'maxReplicas' above
						meta: { next: [e1.entry] },
					});
				}

				await waitForResolved(() => {
					expect(db1.log.log.length).equal(0);
					let total = db2.log.log.length + db3.log.log.length;
					expect(total).greaterThanOrEqual(entryCount);
					expect(db2.log.log.length).greaterThan(entryCount * 0.2);
					expect(db3.log.log.length).greaterThan(entryCount * 0.2);
				});
			});

			it("observer will not delete unless replicated", async () => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicas: {
							min: 10,
						},
						replicate: false,
						setup,
					},
				});
				db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicas: {
								min: 10,
							},
							replicate: {
								factor: 1,
							},
							setup,
						},
					},
				))!;

				const e1 = await db1.add("hello");

				await waitForResolved(() => expect(db1.log.log.length).equal(1));
				await waitForResolved(() => expect(db2.log.log.length).equal(1));
				await expect(
					Promise.all(
						db1.log.prune(
							new Map([
								[
									e1.entry.hash,
									{
										entry: e1.entry,
										leaders: new Set([db2.node.identity.publicKey.hashcode()]),
									},
								],
							]),
							{ timeout: 3000 },
						),
					),
				).rejectedWith("Timeout for checked pruning");
				expect(db1.log.log.length).equal(1); // No deletions
			});

			it("replicator will not delete unless replicated", async () => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicas: {
							min: 10,
						},
						replicate: {
							factor: 1,
						},
						setup,
					},
				});
				db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicas: {
								min: 10,
							},
							replicate: {
								factor: 1,
							},
							setup,
						},
					},
				))!;

				const e1 = await db1.add("hello");
				await waitForResolved(() => expect(db1.log.log.length).equal(1));
				await waitForResolved(() => expect(db2.log.log.length).equal(1));
				await expect(
					Promise.all(
						db1.log.prune(
							new Map([
								[
									e1.entry.hash,
									{
										entry: e1.entry,
										leaders: new Set(
											[db1, db2].map((x) =>
												x.node.identity.publicKey.hashcode(),
											),
										),
									},
								],
							]),
							{ timeout: 3000 },
						),
					),
				).rejectedWith("Failed to delete, is leader");
				expect(db1.log.log.length).equal(1); // No deletions
			});

			it("keep degree while updating role", async () => {
				let min = 1;
				let max = 1;

				// peer 1 observer
				// peer 2 observer

				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicas: {
							min,
							max,
						},
						replicate: false,
						setup,
					},
				});

				db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicate: {
								factor: 0,
							},
							replicas: {
								min,
								max,
							},
							setup,
						},
					},
				))!;

				let db2ReorgCounter = 0;
				let db2ReplicationReorganizationFn = db2.log.onReplicationChange.bind(
					db2.log,
				);
				db2.log.onReplicationChange = (args) => {
					db2ReorgCounter += 1;
					return db2ReplicationReorganizationFn(args);
				};
				await db1.add("hello");

				// peer 1 observer
				// peer 2 replicator (will get entry)

				await waitForResolved(() => expect(db1.log.log.length).equal(1));
				expect(db2ReorgCounter).equal(0);
				await db2.log.replicate({
					factor: 1,
				});
				await waitForResolved(() => expect(db2ReorgCounter).equal(1));
				await waitForResolved(() => expect(db2.log.log.length).equal(1));

				// peer 1 removed
				// peer 2 replicator (has entry)
				await db1.drop();

				// peer 1 observer
				// peer 2 replicator (has entry)
				await session.peers[0].open(db1, {
					args: {
						replicas: {
							min,
							max,
						},
						replicate: false,
						setup,
					},
				});

				// peer 1 observer
				// peer 2 observer
				expect(db2.log.log.length).equal(1);
				await delay(2000);
				// expect(db2ReorgCounter).equal(1); TODO limit distributions and test this

				await db2.log.replicate(false);

				// 	expect(db2ReorgCounter).equal(2); TODO limit distributions and test this
				expect(await db2.log.isReplicating()).to.be.false;

				// peer 1 replicator (will get entry)
				// peer 2 observer (will safely delete the entry)
				await db1.log.replicate({
					factor: 1,
				});

				await waitForResolved(() => expect(db1.log.log.length).equal(1));
				await waitForResolved(() => expect(db2.log.log.length).equal(0));
				// expect(db2ReorgCounter).equal(3); TODO
			});
			it("can override min on program level", async () => {
				let minReplicas = 1;
				let maxReplicas = 1;

				await init({ min: minReplicas, max: maxReplicas });

				const entryCount = 100;
				for (let i = 0; i < entryCount; i++) {
					await db1.add("hello", {
						replicas: new AbsoluteReplicas(5), // will be overriden by 'maxReplicas' above
						meta: { next: [] },
					});
				}
				await waitForResolved(
					() => {
						expect(db1.log.log.length).equal(0); // because db1 is not replicating at all, but just pruning once it knows entries are replicated elsewhere
						let total = db2.log.log.length + db3.log.log.length;
						expect(total).greaterThanOrEqual(entryCount);
						expect(total).lessThan(entryCount * 2);
						expect(db2.log.log.length).greaterThan(entryCount * 0.2);
						expect(db3.log.log.length).greaterThan(entryCount * 0.2);
					},
					{ timeout: 3e4 },
				);
			});
			it("time out when pending IHave are never resolved", async () => {
				let min = 1;
				let max = 1;

				// peer 1 observer
				// peer 2 observer

				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicas: {
							min,
							max,
						},
						replicate: false,
						setup,
					},
				});

				let respondToIHaveTimeout = 3000;
				db2 = await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							replicas: {
								min,
								max,
							},
							replicate: {
								factor: 1,
							},
							respondToIHaveTimeout,
							setup,
						},
					},
				);

				// TODO this test is flaky because background prune calls are intefering with assertions
				// Todo make sure no background prunes are done (?)

				const onMessageFn = db2.log.onMessage.bind(db2.log);
				db2.log.rpc["_responseHandler"] = async (msg: any, cxt: any) => {
					if (msg instanceof ExchangeHeadsMessage) {
						return; // prevent replication
					}
					return onMessageFn(msg, cxt);
				};
				const { entry } = await db1.add("hello");
				const expectPromise = expect(
					Promise.all(
						db1.log.prune(
							new Map([
								[
									entry.hash,
									{
										entry: entry,
										leaders: new Set(
											[db2].map((x) => x.node.identity.publicKey.hashcode()),
										),
									},
								],
							]),
							{ timeout: db1.log.timeUntilRoleMaturity },
						),
					),
				).rejectedWith("Timeout");
				await waitForResolved(() =>
					expect(db2.log["_pendingIHave"].size).equal(1),
				);
				await delay(respondToIHaveTimeout + 1000);
				await waitForResolved(() =>
					expect(db2.log["_pendingIHave"].size).equal(0),
				); // shoulld clear up
				await expectPromise;
			});

			it("does not get blocked by slow sends", async () => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							factor: 1,
						},
						setup,
					},
				});

				db2 = await session.peers[1].open<EventStore<any, any>>(db1.address, {
					args: {
						replicate: {
							factor: 1,
						},
						setup,
					},
				});

				await waitForResolved(async () =>
					expect((await db1.log.getReplicators()).size).equal(2),
				);

				let db1Delay = 0;
				const db1Send = db1.log.rpc.send.bind(db1.log.rpc);
				db1.log.rpc.send = async (message, options) => {
					const controller = new AbortController();
					db1.log.rpc.events.addEventListener("close", () => {
						controller.abort(new AbortError());
					});
					db1.log.rpc.events.addEventListener("drop", () => {
						controller.abort(new AbortError());
					});
					try {
						await delay(db1Delay, { signal: controller.signal });
					} catch (error) {
						return;
					}
					return db1Send(message, options);
				};

				db1Delay = 1e4;

				db1.add("hello");

				await delay(1000); // make sure we have gotten "stuck" into the rpc.send unction

				let t0 = +new Date();
				db1Delay = 0;
				db3 = await session.peers[2].open<EventStore<any, any>>(db1.address, {
					args: {
						replicate: {
							factor: 1,
						},
						setup,
					},
				});

				await waitForResolved(() => expect(db3.log.log.length).equal(1));
				let t1 = +new Date();
				expect(t1 - t0).lessThan(2000);
			});

			it("restarting node will receive entries", async () => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							factor: 1,
						},
						setup,
					},
				});

				db2 = await session.peers[1].open<EventStore<any, any>>(db1.address, {
					args: {
						replicate: {
							factor: 1,
						},
						setup,
					},
				});
				await db1.add("hello");
				await waitForResolved(() => expect(db2.log.log.length).equal(1));
				await db2.drop();
				await session.peers[1].stop();
				await session.peers[1].start();
				db2 = await session.peers[1].open<EventStore<any, any>>(db1.address, {
					args: {
						replicate: {
							factor: 1,
						},
						setup,
					},
				});
				await waitForResolved(() => expect(db2.log.log.length).equal(1));
			});

			it("can handle many large messages", async () => {
				db1 = await session.peers[0].open(new EventStore<string, any>(), {
					args: {
						replicate: {
							factor: 1,
						},
						setup,
					},
				});

				// append more than 30 mb
				const count = 5;
				for (let i = 0; i < count; i++) {
					await db1.add(toBase64(randomBytes(6e6)), { meta: { next: [] } });
				}
				db2 = await session.peers[1].open<EventStore<any, any>>(db1.address, {
					args: {
						replicate: {
							factor: 1,
						},
						setup,
					},
				});
				await waitForResolved(() => expect(db2.log.log.length).equal(count));
			});

			describe("update", () => {
				it("shift to 0 factor", async () => {
					const db1 = await session.peers[0].open(
						new EventStore<string, any>(),
						{
							args: {
								replicate: {
									offset: 0,
									factor: 1,
								},
								replicas: {
									min: 1,
								},
								setup,
							},
						},
					);

					let entryCount = 100;
					for (let i = 0; i < entryCount; i++) {
						await db1.add("hello" + i, { meta: { next: [] } });
					}

					// half of he entries entries will end up in a region where there are no replicators
					expect(
						(await db1.log.entryCoordinatesIndex.iterate().all()).filter(
							(x) => x.value.assignedToRangeBoundary,
						).length,
					).to.be.lessThan(100);

					let db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									offset: 0,
									factor: 1,
								},
								replicas: {
									min: 1,
								},
								setup,
							},
						},
					))!;

					await waitForResolved(() =>
						expect(db2.log.log.length).to.be.above(entryCount / 3),
					);

					await db2.log.replicate(
						{ factor: 0, offset: 0, normalized: false },
						{ reset: true },
					);

					await waitForResolved(() =>
						expect(db1.log.log.length).to.eq(entryCount),
					);
					await waitForResolved(() => expect(db2.log.log.length).to.eq(0));
					await waitForResolved(() =>
						expect(db1.log.log.length + db2.log.log.length).to.equal(
							entryCount,
						),
					);
				});

				it("shift half prune", async () => {
					const halfRegion = Number(numbers.maxValue) / 2;
					const db1 = await session.peers[0].open(
						new EventStore<string, any>(),
						{
							args: {
								replicate: {
									offset: 0,
									factor: halfRegion,
									normalized: false,
								},
								replicas: {
									min: 1,
								},
								setup,
							},
						},
					);

					let entryCount = 100;
					for (let i = 0; i < entryCount; i++) {
						await db1.add("hello" + i, { meta: { next: [] } });
					}

					// half of he entries entries will end up in a region where there are no replicators
					expect(
						(await db1.log.entryCoordinatesIndex.iterate().all()).filter(
							(x) => x.value.assignedToRangeBoundary,
						).length,
					).to.be.lessThan(100);

					let db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									offset: 0,
									factor: halfRegion,
									normalized: false,
								},
								replicas: {
									min: 1,
								},
								setup,
							},
						},
					))!;

					await waitForResolved(() =>
						expect(db2.log.log.length).to.be.above(entryCount / 3),
					);

					await db2.log.replicate(
						{ factor: halfRegion, offset: halfRegion, normalized: false },
						{ reset: true },
					);

					try {
						await waitForResolved(() =>
							expect(db1.log.log.length).to.closeTo(entryCount / 2, 20),
						);
						await waitForResolved(() =>
							expect(db2.log.log.length).to.closeTo(entryCount / 2, 20),
						);
						await waitForResolved(() =>
							expect(db1.log.log.length + db2.log.log.length).to.equal(
								entryCount,
							),
						);
					} catch (error) {
						await dbgLogs([db1.log, db2.log]);
						throw error;
					}
				});

				it("to same range", async () => {
					const db1 = await session.peers[0].open(
						new EventStore<string, any>(),
						{
							args: {
								replicate: {
									offset: 0,
									factor: 1,
								},
								replicas: {
									min: 1,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					);

					let db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									offset: 0,
									factor: 1,
								},
								replicas: {
									min: 1,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					))!;
					await db1.add("hello", { meta: { next: [] } });
					await waitForResolved(() => expect(db1.log.log.length).equal(1));
					await waitForResolved(() => expect(db2.log.log.length).equal(1));

					const findLeaders1 = sinon.spy(db1.log, "findLeaders");
					/* const findLeaders2 = sinon.spy(db2.log, "findLeaders");
					const onMessage1 = sinon.spy(db1.log, "onMessage"); */

					const range = (
						await db2.log.getMyReplicationSegments()
					)[0].toReplicationRange();
					await db2.log.replicate(range);

					expect(findLeaders1.callCount).equal(0); // no changes

					// is this really needed?
					/* try {
						await waitForResolved(() => expect(onMessage1.callCount).equal(1)); // one message
					} catch (error) {
						throw new Error("Never received message");
					}
					expect(findLeaders2.callCount).equal(0); // no changes emitted */
				});

				it("to smaller but already replicated", async () => {
					const db1 = await session.peers[0].open(
						new EventStore<string, any>(),
						{
							args: {
								replicate: {
									offset: 0,
									factor: 1,
								},
								replicas: {
									min: 1,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					);

					let db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									offset: 0,
									factor: 1,
								},
								replicas: {
									min: 1,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					))!;

					let entryCount = 100;
					for (let i = 0; i < entryCount; i++) {
						await db1.add("hello" + i, { meta: { next: [] } });
					}
					await waitForResolved(() =>
						expect(db1.log.log.length).equal(entryCount),
					);
					await waitForResolved(() =>
						expect(db2.log.log.length).equal(entryCount),
					);

					const findLeaders1 = sinon.spy(db1.log, "findLeaders");
					const findLeaders2 = sinon.spy(db2.log, "findLeaders");
					const onMessage1 = sinon.spy(db1.log, "onMessage");

					const range = (
						await db2.log.getMyReplicationSegments()
					)[0].toReplicationRange();

					let newFactor = 0.5;
					await db2.log.replicate({
						factor: newFactor,
						offset: 0,
						id: range.id,
					});
					const expectedAmountOfEntriesToPrune = entryCount * newFactor;

					await waitForResolved(async () => {
						expect(db2.log.log.length).to.be.closeTo(
							entryCount - expectedAmountOfEntriesToPrune,
							30,
						);

						// TODO reenable expect(onMessage1.callCount).equal(2); // two messages (the updated range) and request for pruning
						expect(findLeaders1.callCount).to.be.lessThan(entryCount * 3.5); // some upper bound, TODO make more strict
						expect(findLeaders2.callCount).to.be.lessThan(entryCount * 3.5); // some upper bound, TODO make more strict
						/* 
						TODO stricter boundes like below
						expect(findLeaders1.callCount).to.closeTo(prunedEntries * 2, 30); // redistribute + prune about 50% of the entries
						expect(findLeaders2.callCount).to.closeTo(prunedEntries * 2, 30); // redistribute + handle prune requests 
						*/
					});

					// we do below separetly because this will interefere with the callCounts above
					await waitForResolved(async () =>
						expect(await db2.log.getPrunable()).to.length(0),
					);

					// eslint-disable-next-line no-useless-catch
					try {
						expect(onMessage1.getCall(0).args[0]).instanceOf(
							AddedReplicationSegmentMessage,
						);
						expect(onMessage1.getCall(1).args[0]).instanceOf(RequestIPrune);
					} catch (error) {
						// eslint-disable-next-line no-useless-catch
						try {
							expect(onMessage1.getCall(1).args[0]).instanceOf(
								AddedReplicationSegmentMessage,
							);
							expect(onMessage1.getCall(0).args[0]).instanceOf(RequestIPrune);
						} catch (error) {
							throw error;
						}
					}
					/* const entryRefs1 = await db1.log.entryCoordinatesIndex.iterate().all();
					const entryRefs2 = await db2.log.entryCoordinatesIndex.iterate().all();
		
					expect(
						entryRefs1.filter((x) => x.value.replicators === 2),
					).to.have.length(db2.log.log.length);
					expect(
						entryRefs1.filter((x) => x.value.replicators === 1),
					).to.have.length(entryCount - db2.log.log.length);
					expect(
						entryRefs2.filter((x) => x.value.replicators === 2),
					).to.have.length(db2.log.log.length); */
				});

				it("to smaller will need transfer", async () => {
					const db1 = await session.peers[0].open(
						new EventStore<string, any>(),
						{
							args: {
								replicate: {
									offset: 0,
									factor: 0.5,
								},
								replicas: {
									min: 1,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					);

					let db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									offset: 0.5,
									factor: 0.5,
								},
								replicas: {
									min: 1,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					))!;

					let entryCount = 100;
					for (let i = 0; i < entryCount; i++) {
						await db1.add("hello" + i, { meta: { next: [] } });
					}

					await waitForResolved(() =>
						expect(db1.log.log.length).to.be.closeTo(entryCount / 2, 30),
					);
					await waitForResolved(() =>
						expect(db2.log.log.length).to.be.closeTo(entryCount / 2, 30),
					);

					/* 
					// TODO assert findLeaders call count strict
					const findLeaders1 = sinon.spy(db1.log, "findLeaders");
					const findLeaders2 = sinon.spy(db2.log, "findLeaders"); 
					*/

					const prune2 = sinon.spy(db2.log, "prune");

					const range = (
						await db2.log.getMyReplicationSegments()
					)[0].toReplicationRange();

					await db2.log.replicate({
						factor: 0.001,
						offset: 0.99,
						id: range.id,
					});

					/* const entriesThatWillBeChecked = entryCount / 2;
					const entriesThatWillBePruned = entryCount / 4; // the change is that the range [0.5, 0.75] will be owned by db1 and [0.75, 1] will be owned by db2
					await waitForResolved(() =>
						expect(findLeaders2.callCount).to.closeTo(
							entriesThatWillBeChecked + entriesThatWillBePruned,
							30,
						),
					); TODO assert findLeaders call count strictly */

					/* await waitForResolved(() => {  // TODO should be one?
						expect(prune2.callCount).to.be.eq(1);
						expect(
							[...prune2.getCall(0).args[0].values()].length,
						).to.be.closeTo(entryCount / 4, 15); // a quarter of the entries should be pruned becuse the range [0, 0.75] will be owned by db1 and [0.75, 1] will be owned by db2
					}); */
					/* await waitForResolved(() => {  // TODO should be one?
						expect(prune2.callCount).to.be.eq(2);
						expect(
							[...prune2.getCall(0).args[0].values()].length,
						).to.be.closeTo(entryCount / 4, 15); // a quarter of the entries should be pruned becuse the range [0, 0.75] will be owned by db1 and [0.75, 1] will be owned by db2
					}); */

					await waitForResolved(() => {
						// TODO should better be the assert statement above
						const sumPruneLength = prune2
							.getCalls()
							.map((x) => [...x.args[0].values()])
							.reduce((acc, x) => acc + x.length, 0);
						expect(sumPruneLength).to.be.closeTo(entryCount / 4, 15); // a quarter of the entries should be pruned becuse the range [0, 0.75] will be owned by db1 and [0.75, 1] will be owned by db2
					});

					// TODO assert some kind of findLeaders callCount ?
				});

				it("to smaller then to larger", async () => {
					const db1 = await session.peers[0].open(
						new EventStore<string, any>(),
						{
							args: {
								replicate: {
									offset: 0,
									factor: 0.5,
								},
								replicas: {
									min: 1,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					);

					let db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									offset: 0.5,
									factor: 0.5,
								},
								replicas: {
									min: 1,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					))!;

					let entryCount = 100;
					for (let i = 0; i < entryCount; i++) {
						await db1.add("hello" + i, { meta: { next: [] } });
					}
					await waitForResolved(() =>
						expect(db1.log.log.length).to.be.closeTo(entryCount / 2, 30),
					);
					await waitForResolved(() =>
						expect(db2.log.log.length).to.be.closeTo(entryCount / 2, 30),
					);

					const range = (
						await db2.log.getMyReplicationSegments()
					)[0].toReplicationRange();

					await waitForConverged(() => db2.log.log.length);

					let startSize = db2.log.log.length;
					await db2.log.replicate({ factor: 0.25, offset: 0.5, id: range.id });

					await waitForResolved(() =>
						expect(db2.log.log.length).to.be.lessThan(startSize),
					);
					await delay(1000);

					await db2.log.replicate({ factor: 0.5, offset: 0.5, id: range.id });
					await waitForResolved(() =>
						expect(db2.log.log.length).to.eq(startSize),
					);
				});

				it("range replace then restore recovers lengths (small set, 2 clients)", async () => {
					const db1 = await session.peers[0].open(
						new EventStore<string, any>(),
						{
							args: {
								replicate: { offset: 0, factor: 0.5 },
								replicas: { min: 1 },
								timeUntilRoleMaturity: 0,
								setup,
							},
						},
					);

					let db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: { offset: 0.5, factor: 0.5 },
								replicas: { min: 1 },
								timeUntilRoleMaturity: 0,
								setup,
							},
						},
					))!;

					const entryCount = 100;
					for (let i = 0; i < entryCount; i++) {
						await db1.add("hello" + i, { meta: { next: [] } });
					}

					await waitForResolved(() =>
						expect(db1.log.log.length).to.be.closeTo(entryCount / 2, 20),
					);
					await waitForResolved(() =>
						expect(db2.log.log.length).to.be.closeTo(entryCount / 2, 20),
					);

					const db2Length = db2.log.log.length;
					await waitForResolved(() =>
						expect(db2.log.log.length).to.be.greaterThan(0),
					);

					const range2 = (
						await db2.log.getMyReplicationSegments()
					)[0].toReplicationRange();
					await db2.log.replicate({ id: range2.id, offset: 0.1, factor: 0.1 });

					await waitForResolved(() =>
						expect(db1.log.log.length).to.be.closeTo(entryCount / 2, 20),
					);
					await waitForResolved(() =>
						expect(db2.log.log.length).to.be.closeTo(entryCount / 10, 10),
					);

					expect(db2.log.log.length).to.be.lessThan(db2Length);

					// restore original ranges
					await db2.log.replicate({ id: range2.id, offset: 0.5, factor: 0.5 });
					await waitForResolved(() =>
						expect(db2.log.log.length).to.eq(db2Length),
					);
				});

				it("to smaller will initiate prune", async () => {
					const db1 = await session.peers[0].open(
						new EventStore<string, any>(),
						{
							args: {
								replicate: {
									offset: 0,
									factor: 1,
								},
								replicas: {
									min: 1,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					);

					let entryCount = 100;
					for (let i = 0; i < entryCount; i++) {
						await db1.add("hello" + i, { meta: { next: [] } });
					}

					let db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									offset: 0,
									factor: 1,
								},
								replicas: {
									min: 1,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					))!;

					await waitForResolved(() =>
						expect(db2.log.log.length).to.be.eq(entryCount),
					);

					const range1 = (
						await db1.log.getMyReplicationSegments()
					)[0].toReplicationRange();

					await db1.log.replicate({ id: range1.id, offset: 0, factor: 0 });
					await waitForResolved(() => expect(db1.log.log.length).to.eq(0));
				});

				it("replace range with another node write before join with slowed down send", async () => {
					let sendDelay = 500;
					let waitForPruneDelay = sendDelay + 1000;
					slowDownSend(session.peers[2], session.peers[0], sendDelay); // we do this to force a replication pattern where peer[1] needs to send entries to peer[2]
					const db1 = await session.peers[0].open(
						new EventStore<string, any>(),
						{
							args: {
								replicate: {
									offset: 0,
									factor: 0.5,
								},
								replicas: {
									min: 1,
								},
								waitForPruneDelay,
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					);

					let entryCount = 100;

					for (let i = 0; i < entryCount; i++) {
						await db1.add("hello" + i, { meta: { next: [] } });
					}

					let db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									offset: 0.5,
									factor: 0.5,
								},
								replicas: {
									min: 1,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								waitForPruneDelay,
								setup,
							},
						},
					))!;

					let db3 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[2],
						{
							args: {
								replicate: {
									offset: 0.5,
									factor: 0.5,
								},
								replicas: {
									min: 1,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								waitForPruneDelay,
								setup,
							},
						},
					))!;

					await waitForResolved(() =>
						expect(db1.log.log.length).to.be.closeTo(entryCount / 2, 20),
					);
					await waitForResolved(() =>
						expect(db2.log.log.length).to.be.closeTo(entryCount / 2, 20),
					);
					await waitForResolved(() =>
						expect(db3.log.log.length).to.be.closeTo(entryCount / 2, 20),
					);

					const db2Length = db2.log.log.length;
					const db3Length = db3.log.log.length;

					await waitForResolved(() =>
						expect(db2.log.log.length).to.be.greaterThan(0),
					);

					await waitForResolved(() =>
						expect(db3.log.log.length).to.be.greaterThan(0),
					);

					const range2 = (
						await db2.log.getMyReplicationSegments()
					)[0].toReplicationRange();

					await db2.log.replicate({ id: range2.id, offset: 0.1, factor: 0.1 });

					// await delay(5000)

					const range3 = (
						await db3.log.getMyReplicationSegments()
					)[0].toReplicationRange();

					await db3.log.replicate({ id: range3.id, offset: 0.1, factor: 0.1 });

					await waitForResolved(() =>
						expect(db1.log.log.length).to.be.closeTo(entryCount / 2, 20),
					);
					await waitForResolved(() =>
						expect(db2.log.log.length).to.be.closeTo(entryCount / 10, 10),
					);
					await waitForResolved(() =>
						expect(db3.log.log.length).to.be.closeTo(entryCount / 10, 10),
					);

					expect(db2.log.log.length).to.be.lessThan(db2Length);
					expect(db3.log.log.length).to.be.lessThan(db3Length);

					// reset to original

					await db2.log.replicate({ id: range2.id, offset: 0.5, factor: 0.5 });
					await db3.log.replicate({ id: range3.id, offset: 0.5, factor: 0.5 });

					try {
						await waitForResolved(() =>
							expect(db2.log.log.length).to.eq(db2Length),
						);
						await waitForResolved(() =>
							expect(db3.log.log.length).to.eq(db3Length),
						);
					} catch (error) {
						await dbgLogs([db2.log, db3.log]);
						throw error;
					}
				});

				it("replace range with another node write after join", async () => {
					const db1 = await session.peers[0].open(
						new EventStore<string, any>(),
						{
							args: {
								replicate: {
									offset: 0,
									factor: 0.5,
								},
								replicas: {
									min: 1,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					);

					let db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									offset: 0.5,
									factor: 0.5,
								},
								replicas: {
									min: 1,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					))!;

					let db3 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[2],
						{
							args: {
								replicate: {
									offset: 0.5,
									factor: 0.5,
								},
								replicas: {
									min: 1,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					))!;

					try {
						await waitForResolved(async () =>
							expect((await db1.log.getReplicators()).size).to.eq(3),
						);

						let entryCount = 100;
						for (let i = 0; i < entryCount; i++) {
							await db1.add("hello" + i, { meta: { next: [] } });
						}

						await waitForResolved(() =>
							expect(db1.log.log.length).to.be.closeTo(entryCount / 2, 20),
						);
						await waitForResolved(() =>
							expect(db2.log.log.length).to.be.closeTo(entryCount / 2, 20),
						);
						await waitForResolved(() =>
							expect(db3.log.log.length).to.be.closeTo(entryCount / 2, 20),
						);

						const db2Length = db2.log.log.length;
						const db3Length = db3.log.log.length;

						await waitForResolved(() =>
							expect(db2.log.log.length).to.be.greaterThan(0),
						);

						await waitForResolved(() =>
							expect(db3.log.log.length).to.be.greaterThan(0),
						);

						const range2 = (
							await db2.log.getMyReplicationSegments()
						)[0].toReplicationRange();

						await db2.log.replicate({
							id: range2.id,
							offset: 0.1,
							factor: 0.1,
						});

						const range3 = (
							await db3.log.getMyReplicationSegments()
						)[0].toReplicationRange();

						await db3.log.replicate({
							id: range3.id,
							offset: 0.1,
							factor: 0.1,
						});

						await waitForResolved(() =>
							expect(db1.log.log.length).to.be.closeTo(entryCount / 2, 20),
						);
						await waitForResolved(() =>
							expect(db2.log.log.length).to.be.closeTo(entryCount / 10, 10),
						);
						await waitForResolved(() =>
							expect(db3.log.log.length).to.be.closeTo(entryCount / 10, 10),
						);

						// reset to original

						await db2.log.replicate({
							id: range2.id,
							offset: 0.5,
							factor: 0.5,
						});

						await db3.log.replicate({
							id: range3.id,
							offset: 0.5,
							factor: 0.5,
						});

						await waitForResolved(() =>
							expect(db2.log.log.length).to.eq(db2Length),
						);
						await waitForResolved(() =>
							expect(db3.log.log.length).to.eq(db3Length),
						);
					} catch (error) {
						await dbgLogs([db1.log, db2.log, db3.log]);
						throw error;
					}
				});

				it("distribute", async () => {
					const maxDiv3 = Math.round(Number(numbers.maxValue) / 3);
					const db1 = await session.peers[0].open(
						new EventStore<string, any>(),
						{
							args: {
								replicate: {
									offset: 0,
									factor: numbers.maxValue,
									normalized: false,
								},
								replicas: {
									min: 1,
								},
								setup,
							},
						},
					);

					let db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									offset: 0,
									factor: numbers.maxValue,
									normalized: false,
								},
								replicas: {
									min: 1,
								},
								setup,
							},
						},
					))!;

					let db3 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[2],
						{
							args: {
								replicate: {
									offset: 0,
									factor: numbers.maxValue,
									normalized: false,
								},
								replicas: {
									min: 1,
								},
								setup,
							},
						},
					))!;

					let entryCount = 300;

					for (let i = 0; i < entryCount; i++) {
						await db1.add("hello" + i, { meta: { next: [] } });
					}

					await waitForResolved(() =>
						expect(db1.log.log.length).equal(entryCount),
					);
					await waitForResolved(() =>
						expect(db2.log.log.length).equal(entryCount),
					);
					await waitForResolved(() =>
						expect(db3.log.log.length).equal(entryCount),
					);

					db1.log.replicate(
						{ factor: maxDiv3, offset: 0, normalized: false },
						{ reset: true },
					);
					db2.log.replicate(
						{ factor: maxDiv3, offset: maxDiv3, normalized: false },
						{ reset: true },
					);
					db3.log.replicate(
						{ factor: maxDiv3, offset: maxDiv3 * 2, normalized: false },
						{ reset: true },
					);

					await waitForResolved(() =>
						expect(db1.log.log.length).to.closeTo(entryCount / 3, 30),
					);
					await waitForResolved(() =>
						expect(db2.log.log.length).to.closeTo(entryCount / 3, 30),
					);
					await waitForResolved(() =>
						expect(db3.log.log.length).to.closeTo(entryCount / 3, 30),
					);
					await waitForResolved(() =>
						expect(
							db1.log.log.length + db2.log.log.length + db3.log.log.length,
						).to.equal(entryCount),
					);
					for (const db of [db1, db2, db3]) {
						expect(await db.log.getPrunable()).to.have.length(0);
					}
				});

				it("close", async () => {
					db1 = await session.peers[0].open(new EventStore<string, any>(), {
						args: {
							replicate: {
								factor: 0.333,
								offset: 0.333,
							},
							setup,
						},
					});

					db2 = await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									factor: 0.333,
									offset: 0,
								},
								setup,
							},
						},
					);
					db3 = await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[2],
						{
							args: {
								replicate: {
									factor: 0.333,
									offset: 0.666,
								},
								setup,
							},
						},
					);

					const sampleSize = 1e3;
					const entryCount = sampleSize;

					await waitForResolved(async () =>
						expect(await db1.log.replicationIndex?.getSize()).equal(3),
					);
					await waitForResolved(async () =>
						expect(await db2.log.replicationIndex?.getSize()).equal(3),
					);
					await waitForResolved(async () =>
						expect(await db3.log.replicationIndex?.getSize()).equal(3),
					);

					const promises: Promise<any>[] = [];
					for (let i = 0; i < entryCount; i++) {
						promises.push(
							db1.add(toBase64(new Uint8Array([i])), {
								meta: { next: [] },
							}),
						);
					}

					await Promise.all(promises);

					await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);

					const distribute = sinon.spy(db1.log.onReplicationChange);
					db1.log.onReplicationChange = distribute;
					await db3.close();
					await checkBounded(entryCount, 1, 1, db1, db2);
				});

				it("unreplicate", async () => {
					db1 = await session.peers[0].open(new EventStore<string, any>(), {
						args: {
							replicate: {
								factor: 0.333,
								offset: 0.333,
							},
							setup,
						},
					});

					db2 = await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									factor: 0.333,
									offset: 0,
								},
								setup,
							},
						},
					);
					db3 = await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[2],
						{
							args: {
								replicate: {
									factor: 0.333,
									offset: 0.666,
								},
								setup,
							},
						},
					);

					const sampleSize = 1e3;
					const entryCount = sampleSize;

					await waitForResolved(async () =>
						expect(await db1.log.replicationIndex?.getSize()).equal(3),
					);
					await waitForResolved(async () =>
						expect(await db2.log.replicationIndex?.getSize()).equal(3),
					);
					await waitForResolved(async () =>
						expect(await db3.log.replicationIndex?.getSize()).equal(3),
					);

					const promises: Promise<any>[] = [];
					for (let i = 0; i < entryCount; i++) {
						promises.push(
							db1.add(toBase64(new Uint8Array([i])), {
								meta: { next: [] },
							}),
						);
					}

					await Promise.all(promises);

					await checkBounded(entryCount, 0.5, 0.9, db1, db2, db3);

					const distribute = sinon.spy(db1.log.onReplicationChange);
					db1.log.onReplicationChange = distribute;

					const segments = await db3.log.replicationIndex.iterate().all();
					await db3.log.unreplicate(segments.map((x) => x.value));

					await checkBounded(entryCount, 1, 1, db1, db2);
				});

				it("a smaller replicator join leave joins", async () => {
					let minReplicas = 2;
					const db1 = await session.peers[0].open(
						new EventStore<string, any>(),
						{
							args: {
								replicate: {
									factor: 1, // this  replicator will get all entries
								},
								replicas: {
									min: minReplicas, // we set min replicas to 2 to ensure second node should have all entries no matter what
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					);
					let entryCount = 100;
					for (let i = 0; i < entryCount; i++) {
						await db1.add("hello" + i, { meta: { next: [] } });
					}

					expect(
						(await db1.log.entryCoordinatesIndex.iterate().all()).filter(
							(x) => x.value.assignedToRangeBoundary,
						).length,
					).to.eq(entryCount);

					let db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									offset: 0.1,
									factor: 0.1, // some small range
								},
								replicas: {
									min: minReplicas,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					))!;

					await waitForResolved(() =>
						expect(db1.log.log.length).to.be.equal(entryCount),
					);
					await waitForResolved(() =>
						expect(db2.log.log.length).to.be.equal(entryCount),
					);

					await db2.close();
					db2 = (await EventStore.open<EventStore<string, any>>(
						db1.address!,
						session.peers[1],
						{
							args: {
								replicate: {
									factor: 0.2, // some small range
									offset: 0.2, // but on another place
								},
								replicas: {
									min: minReplicas,
								},
								timeUntilRoleMaturity: 0, // prevent additiona replicationChangeEvents to occur when maturing
								setup,
							},
						},
					))!;

					await waitForResolved(() =>
						expect(db1.log.log.length).to.be.equal(entryCount),
					);
					await waitForResolved(() =>
						expect(db2.log.log.length).to.be.equal(entryCount),
					);
				});
			});

			/*  TODO feat
			it("will reject early if leaders does not have entry", async () => {
				await init(1);
		
				const value = "hello";
		
				const e1 = await db1.add(value, { replicas: new AbsoluteReplicas(2) });
		
				// Assume all peers gets it
				await waitForResolved(() => expect(db1.log.log.length).equal(1));
				await waitForResolved(() => expect(db2.log.log.length).equal(1));
				await waitForResolved(() => expect(db3.log.log.length).equal(1));
				await db3.log.log.deleteRecursively(await db3.log.log.getHeads());
				await waitForResolved(() => expect(db3.log.log.length).equal(0));
		
				expect(db2.log.log.length).equal(1);
				const fn = () => db2.log.safelyDelete([e1.entry], { timeout: 3000 })[0];
				await expect(fn).rejectedWith(
					"Insufficient replicators to safely delete: " + e1.entry.hash
				);
				expect(db2.log.log.length).equal(1);
			}); */
		});

		describe("sync", () => {
			let session: TestSession;
			let db1: EventStore<string, any>, db2: EventStore<string, any>;

			before(async () => {
				session = await TestSession.connected(2);
			});
			after(async () => {
				await session.stop();
			});

			afterEach(async () => {
				if (db1) await db1.drop();
				if (db2) await db2.drop();
			});

			it("manually synced entries will not get pruned", async () => {
				db1 = await session.peers[0].open<EventStore<string, any>>(
					new EventStore(),
					{
						args: {
							/* sync: () => true, */
							replicas: {
								min: 1,
							},
							replicate: {
								factor: 1,
							},
							setup,
						},
					},
				)!;

				db2 = (await EventStore.open<EventStore<string, any>>(
					db1.address!,
					session.peers[1],
					{
						args: {
							/* 	sync: () => true, */
							replicas: {
								min: 1,
							},
							replicate: {
								factor: 1,
							},
							setup,
						},
					},
				))!;
				await db1.add("data");
				await waitForResolved(() => expect(db2.log.log.length).equal(1));
				await db2.log.replicate(false);
				await delay(3000);
				await waitForResolved(() => expect(db2.log.log.length).equal(0));
			});
		});
	});
});
