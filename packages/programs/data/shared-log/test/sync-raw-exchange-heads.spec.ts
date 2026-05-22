import { keys } from "@libp2p/crypto";
import { create as createRustIndexer } from "@peerbit/indexer-rust";
import {
	NativeBackboneCoordinatePersistence,
	NativeBackboneMemoryCoordinatePersistenceStore,
} from "@peerbit/native-backbone";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { v4 as uuid } from "uuid";
import {
	EntryWithRefs,
	ExchangeHeadsMessage,
	RawEntryWithRefs,
	RawExchangeHeadsMessage,
	RequestIPrune,
	createRawExchangeHeadsMessages,
} from "../src/exchange-heads.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { groupByGid, tryGroupByGidSync } from "../src/utils.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("raw exchange-head sync", () => {
	it("groups already-materialized entry refs without async meta reads", async () => {
		const session = await TestSession.disconnected(1, {
			indexer: (directory) => createRustIndexer(directory),
		});
		try {
			const store = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate: false,
					setup: {
						domain: createReplicationDomainHash("u32"),
						type: "u32" as const,
						syncronizer: SimpleSyncronizer,
						name: "u32-simple-raw",
					},
				},
			});
			const first = await store.add(uuid(), { meta: { next: [] } });
			const second = await store.add(uuid(), { meta: { next: [] } });
			const firstGetMetaSpy = sinon.spy(first.entry, "getMeta");
			const secondGetMetaSpy = sinon.spy(second.entry, "getMeta");
			try {
				const heads = [
					new EntryWithRefs({
						entry: first.entry,
						gidRefrences: [],
					}),
					new EntryWithRefs({
						entry: second.entry,
						gidRefrences: [],
					}),
				];
				const syncGrouped = tryGroupByGidSync(heads);
				expect(syncGrouped?.size).equal(2);
				const grouped = await groupByGid(heads);
				expect(grouped.size).equal(2);
				expect(firstGetMetaSpy.callCount).equal(0);
				expect(secondGetMetaSpy.callCount).equal(0);
			} finally {
				secondGetMetaSpy.restore();
				firstGetMetaSpy.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("uses raw exchange heads for capable simple-sync peers", async () => {
		const session = await TestSession.disconnected(2, [
			{
				libp2p: {
					privateKey: keys.privateKeyFromRaw(
						new Uint8Array([
							204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91,
							229, 157, 168, 15, 45, 242, 144, 98, 75, 58, 208, 9,
							223, 143, 251, 52, 252, 159, 64, 83, 52, 197, 24, 246,
							24, 234, 141, 183, 151, 82, 53, 142, 57, 25, 148, 150,
							26, 209, 223, 22, 212, 40, 201, 6, 191, 72, 148, 82, 66,
							138, 199, 185,
						]),
					),
				},
				indexer: (directory) => createRustIndexer(directory),
			},
			{
				libp2p: {
					privateKey: keys.privateKeyFromRaw(
						new Uint8Array([
							237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214,
							122, 28, 157, 208, 163, 15, 215, 104, 193, 151, 177, 62,
							231, 253, 120, 122, 222, 174, 242, 120, 50, 165, 97, 8,
							235, 97, 186, 148, 251, 100, 168, 49, 10, 119, 71, 246,
							246, 174, 163, 198, 54, 224, 6, 174, 212, 159, 187, 2,
							137, 47, 192,
						]),
					),
				},
				indexer: (directory) => createRustIndexer(directory),
			},
		]);

		try {
			const setup = {
				domain: createReplicationDomainHash("u32"),
				type: "u32" as const,
				syncronizer: SimpleSyncronizer,
				name: "u32-simple-raw",
			};
			const store = new EventStore<string, any>();
			const profileEvents: any[] = [];
			const openArgs = {
				replicate: { factor: 1 },
				setup,
				nativeGraph: true,
				sync: {
					rawExchangeHeads: true,
					profile: (event: any) => profileEvents.push(event),
				},
			};
			const db1 = await session.peers[0].open(store.clone(), {
				args: openArgs,
			});
			const db2 = await session.peers[1].open(store.clone(), {
				args: openArgs,
			});
			const putKnownSpy = sinon.spy(db2.log.log.blocks as any, "putKnown");
			const putKnownManySpy = sinon.spy(
				db2.log.log.blocks as any,
				"putKnownMany",
			);
			const putKnownManyColumnsSpy =
				"putKnownManyColumns" in db2.log.log.blocks &&
				typeof (db2.log.log.blocks as any).putKnownManyColumns === "function"
					? sinon.spy(db2.log.log.blocks as any, "putKnownManyColumns")
					: undefined;
			const lowerNativeGraph = db2.log.log.entryIndex.properties.nativeGraph!
				.graph as any;
			const planJoinBatchSpy = sinon.spy(lowerNativeGraph, "planJoinBatch");
			const planJoinSpy = sinon.spy(lowerNativeGraph, "planJoin");
			const lowerPutAppendBatchSpy = sinon.spy(
				db2.log.log.entryIndex,
				"putAppendBatch",
			);
			const lowerPutAppendFactsBatchSpy = sinon.spy(
				db2.log.log.entryIndex,
				"putAppendFactsBatch",
			);
			const coordinateIndex = db2.log.entryCoordinatesIndex as any;
			const coordinateBatchSpy = sinon.spy(
				coordinateIndex,
				"putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn",
			);
			const persistBatchSpy = sinon.spy(
				db2.log as any,
				"persistCoordinatesBatch",
			);
			const coordinatePrepareSpy = sinon.spy(
				db2.log as any,
				"createCoordinatePersistenceEntryFromNativePlan",
			);
			const sharedOnChangeSpy = sinon.spy(db2.log as any, "onChange");
			const entryAddedHashSpy = sinon.spy(db2.log as any, "onEntryAddedHash");
			const markKnownSpy = sinon.spy(db2.log as any, "markEntriesKnownByPeer");
			const receivedEntriesSpy = sinon.spy(
				db2.log.syncronizer as any,
				"onReceivedEntries",
			);
			const receivedEntryHashesSpy = sinon.spy(
				db2.log.syncronizer as any,
				"onReceivedEntryHashes",
			);

			let exchangeHeads = 0;
			let rawExchangeHeads = 0;
			for (const db of [db1, db2]) {
				const send = db.log.rpc.send.bind(db.log.rpc);
				db.log.rpc.send = async (message, options) => {
					if (message instanceof RawExchangeHeadsMessage) {
						rawExchangeHeads += 1;
					} else if (message instanceof ExchangeHeadsMessage) {
						exchangeHeads += 1;
					}
					return send(message, options);
				};
			}

			const entryCount = 10;
			const hashes: string[] = [];
			for (let i = 0; i < entryCount; i++) {
				const { entry } = await db1.add(uuid(), { meta: { next: [] } });
				hashes.push(entry.hash);
			}
			expect(db1.log.log.length).to.equal(entryCount);

			await waitForResolved(() =>
				session.peers[0].dial(session.peers[1].getMultiaddrs()),
			);
			await (db2.log.syncronizer as SimpleSyncronizer<any>).queueSync(
				hashes,
				db1.node.identity.publicKey,
				{ skipCheck: true },
			);
			await waitForResolved(
				() => {
					expect(db2.log.log.length).to.equal(entryCount);
				},
				{ timeout: 30_000, delayInterval: 100 },
			);

			expect(rawExchangeHeads).to.be.greaterThan(0);
			expect(exchangeHeads).to.equal(0);
			expect(
				putKnownSpy.callCount +
					putKnownManySpy.callCount +
					(putKnownManyColumnsSpy?.callCount ?? 0),
			).to.be.greaterThan(0);
			expect(
				putKnownManySpy.callCount +
					(putKnownManyColumnsSpy?.callCount ?? 0),
			).to.be.greaterThan(0);
			expect(planJoinBatchSpy.callCount).to.be.greaterThan(0);
			expect(planJoinSpy.callCount).to.equal(0);
			const preparedLowerJoinCall = lowerPutAppendBatchSpy
				.getCalls()
				.find(
					(call) =>
						call.args[0]?.length === entryCount &&
						call.args[1]?.prepared?.shallowEntries?.length === entryCount,
				);
			const preparedLowerJoin = preparedLowerJoinCall?.args[1]?.prepared as
				| {
						nativeEntries?: unknown[];
				  }
				| undefined;
			if (preparedLowerJoin) {
				expect(preparedLowerJoin.nativeEntries).to.have.length(entryCount);
			} else {
				const preparedFactsJoinCall = lowerPutAppendFactsBatchSpy
					.getCalls()
					.find((call) => call.args[0]?.length === entryCount);
				expect(preparedFactsJoinCall).to.exist;
				expect(
					preparedFactsJoinCall!.args[0].every(
						(row: { nativeEntry?: unknown }) => !!row.nativeEntry,
					),
				).to.equal(true);
			}
			expect(persistBatchSpy.callCount).to.be.greaterThan(0);
			expect(coordinateBatchSpy.callCount).to.be.greaterThan(0);
			expect(coordinatePrepareSpy.callCount).to.be.greaterThan(0);
			expect(
				coordinatePrepareSpy.returnValues.some(
					(prepared: any) =>
						prepared &&
						prepared !== false &&
						!prepared.coordinateEntry &&
						prepared.fields?.metaBytes instanceof Uint8Array,
				),
			).equal(true);
			expect(receivedEntriesSpy.callCount).to.equal(0);
			expect(receivedEntryHashesSpy.callCount).to.equal(1);
			expect(receivedEntryHashesSpy.firstCall.args[0].hashes).to.have.length(
				entryCount,
			);
			expect(markKnownSpy.callCount).to.equal(1);
			expect([...markKnownSpy.firstCall.args[0]]).to.have.length(entryCount);
			expect(sharedOnChangeSpy.callCount).to.equal(0);
			expect(entryAddedHashSpy.callCount).to.equal(entryCount);
			expect(
				entryAddedHashSpy
					.getCalls()
					.every((call) => call.args.length === 1),
			).equal(true);
			const profileNames = profileEvents.map((event) => event.name);
			expect(profileNames).to.include("sharedLog.rawReceive.materialize");
			expect(profileNames).to.include("sharedLog.receive.lowerLogJoin");
			expect(profileNames).to.include("sharedLog.receive.coordinatePersist");
			const materializeProfile = profileEvents.find(
				(event) => event.name === "sharedLog.rawReceive.materialize",
			);
			expect(materializeProfile.entries).to.equal(entryCount);
			expect(materializeProfile.bytes).to.be.greaterThan(0);
			const metadataProfile = profileEvents.find(
				(event) => event.name === "sharedLog.canAppendBatch.metadata",
			);
			expect(metadataProfile.details.replicaCacheHits).to.equal(entryCount);
			const receivePlanProfile = profileEvents.find(
				(event) => event.name === "sharedLog.receive.plan",
			);
			expect(receivePlanProfile.details.predecodedReplicaHits).to.equal(
				entryCount,
			);
			const lowerLogJoinProfile = profileEvents.find(
				(event) => event.name === "sharedLog.receive.lowerLogJoin",
			);
			expect(lowerLogJoinProfile.details.hashOnlyEntryAdded).to.equal(true);
			receivedEntryHashesSpy.restore();
			receivedEntriesSpy.restore();
			markKnownSpy.restore();
			entryAddedHashSpy.restore();
			sharedOnChangeSpy.restore();
			coordinatePrepareSpy.restore();
			persistBatchSpy.restore();
			coordinateBatchSpy.restore();
			lowerPutAppendBatchSpy.restore();
			planJoinSpy.restore();
			planJoinBatchSpy.restore();
			putKnownSpy.restore();
			putKnownManySpy.restore();
			putKnownManyColumnsSpy?.restore();
		} finally {
			await session.stop();
		}
	});

	it("uses prepared raw join for a single received raw head", async () => {
		const session = await TestSession.disconnected(2, {
			indexer: (directory) => createRustIndexer(directory),
		});

		try {
			const setup = {
				domain: createReplicationDomainHash("u32"),
				type: "u32" as const,
				syncronizer: SimpleSyncronizer,
				name: "u32-simple-raw",
			};
			const store = new EventStore<string, any>();
			const openArgs = {
				replicate: false,
				setup,
				nativeGraph: true,
				sync: { rawExchangeHeads: true },
				keep: () => true,
				timeUntilRoleMaturity: 0,
			};
			const db1 = await session.peers[0].open(store.clone(), {
				args: openArgs,
			});
			const db2 = await session.peers[1].open(store.clone(), {
				args: openArgs,
			});
			const { entry } = await db1.add(uuid(), { meta: { next: [] } });

			let message:
				| RawExchangeHeadsMessage
				| ExchangeHeadsMessage<any>
				| undefined;
			for await (const generated of createRawExchangeHeadsMessages(
				db1.log.log,
				[entry.hash],
			)) {
				message = generated;
				break;
			}
			expect(message).to.be.instanceOf(RawExchangeHeadsMessage);

			const lowerNativeGraph = db2.log.log.entryIndex.properties.nativeGraph!
				.graph as any;
			const putAppendChainSpy = sinon.spy(lowerNativeGraph, "putAppendChain");
			const putBatchSpy = sinon.spy(lowerNativeGraph, "putBatch");
			const lowerPutAppendBatchSpy = sinon.spy(
				db2.log.log.entryIndex,
				"putAppendBatch",
			);
			const lowerPutAppendFactsBatchSpy = sinon.spy(
				db2.log.log.entryIndex,
				"putAppendFactsBatch",
			);
			try {
				await db2.log.onMessage(message!, {
					from: db1.node.identity.publicKey,
				} as any);

				expect(db2.log.log.length).to.equal(1);
				expect(lowerPutAppendFactsBatchSpy.callCount).to.equal(1);
				expect(lowerPutAppendFactsBatchSpy.firstCall.args[0]).to.have.length(1);
				expect(
					lowerPutAppendFactsBatchSpy.firstCall.args[0][0].nativeEntry,
				).to.exist;
				expect(putAppendChainSpy.callCount).to.equal(1);
				expect(putBatchSpy.callCount).to.equal(0);
				expect(lowerPutAppendBatchSpy.callCount).to.equal(0);
			} finally {
				lowerPutAppendFactsBatchSpy.restore();
				lowerPutAppendBatchSpy.restore();
				putBatchSpy.restore();
				putAppendChainSpy.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("batch plans independent raw heads before joining when not replicating", async () => {
		const session = await TestSession.disconnected(2, {
			indexer: (directory) => createRustIndexer(directory),
		});

		try {
			const setup = {
				domain: createReplicationDomainHash("u32"),
				type: "u32" as const,
				syncronizer: SimpleSyncronizer,
				name: "u32-simple-raw",
			};
			const store = new EventStore<string, any>();
			const openArgs = {
				replicate: false,
				setup,
				nativeGraph: true,
				sync: { rawExchangeHeads: true },
				keep: () => true,
				timeUntilRoleMaturity: 0,
			};
			const db1 = await session.peers[0].open(store.clone(), {
				args: openArgs,
			});
			const db2 = await session.peers[1].open(store.clone(), {
				args: openArgs,
			});
			const lowerHasManySpy = sinon.spy(db2.log.log, "hasMany");

			const entryCount = 6;
			const hashes: string[] = [];
			for (let i = 0; i < entryCount; i++) {
				const { entry } = await db1.add(uuid(), { meta: { next: [] } });
				hashes.push(entry.hash);
			}

			let message:
				| RawExchangeHeadsMessage
				| ExchangeHeadsMessage<any>
				| undefined;
			for await (const generated of createRawExchangeHeadsMessages(
				db1.log.log,
				hashes,
			)) {
				message = generated;
				break;
			}
			expect(message).to.be.instanceOf(RawExchangeHeadsMessage);
			const sharedPlanEntryLeaderBatchSpy = sinon.spy(
				db2.log as any,
				"planEntryLeaderBatch",
			);
			const nativePlanner = (db2.log as any)._nativeRangePlanner;
			expect(nativePlanner).to.exist;
			const batchSpy = sinon.spy(nativePlanner, "planLeadersForGidsBatch");
			const singleSpy = sinon.spy(nativePlanner, "planLeadersForGid");
			const hasAnyHeadBatchSpy = sinon.spy(
				db2.log.log.entryIndex,
				"hasAnyHeadBatch",
			);
			const hasAnyHeadSpy = sinon.spy(db2.log.log.entryIndex, "hasAnyHead");
			try {
				await db2.log.onMessage(message!, {
					from: db1.node.identity.publicKey,
				} as any);

				expect(db2.log.log.length).to.equal(entryCount);
				expect(sharedPlanEntryLeaderBatchSpy.callCount).to.equal(1);
				expect(batchSpy.callCount).to.be.greaterThan(0);
				expect(batchSpy.firstCall.args[0]).to.have.length(entryCount);
				expect(singleSpy.callCount).to.equal(0);
				expect(hasAnyHeadBatchSpy.callCount).to.equal(0);
				expect(hasAnyHeadSpy.callCount).to.equal(0);
				expect(lowerHasManySpy.callCount).to.equal(1);
				expect(lowerHasManySpy.firstCall.args[0]).to.have.length(entryCount);
			} finally {
				lowerHasManySpy.restore();
				hasAnyHeadSpy.restore();
				hasAnyHeadBatchSpy.restore();
				singleSpy.restore();
				batchSpy.restore();
				sharedPlanEntryLeaderBatchSpy.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("commits raw receive blocks and graph through native backbone", async () => {
		const session = await TestSession.disconnected(2, {
			indexer: (directory) => createRustIndexer(directory),
		});

		try {
			const setup = {
				domain: createReplicationDomainHash("u32"),
				type: "u32" as const,
				syncronizer: SimpleSyncronizer,
				name: "u32-simple-raw",
			};
			const store = new EventStore<string, any>();
			const openArgs: any = {
				replicate: false,
				setup,
				nativeGraph: true,
				nativeBackbone: { optional: false },
				sync: { rawExchangeHeads: true },
				keep: () => true,
				timeUntilRoleMaturity: 0,
			};
			const source = await session.peers[0].open(store.clone(), {
				args: openArgs,
			});
			const target = await session.peers[1].open(store.clone(), {
				args: openArgs,
			});

			const hashes: string[] = [];
			for (let i = 0; i < 3; i++) {
				const { entry } = await source.add(uuid(), { meta: { next: [] } });
				hashes.push(entry.hash);
			}

			let message:
				| RawExchangeHeadsMessage
				| ExchangeHeadsMessage<any>
				| undefined;
			for await (const generated of createRawExchangeHeadsMessages(
				source.log.log,
				hashes,
			)) {
				message = generated;
				break;
			}
			expect(message).to.be.instanceOf(RawExchangeHeadsMessage);

			const sharedLog = target.log as any;
			const backbone = sharedLog._nativeBackbone;
			const nativePreparedCommitSpy = sinon.spy(
				backbone.graph,
				"commitPreparedRawReceiveBatch",
			);
			const nativePreparedJoinCommitSpy = sinon.spy(
				backbone.graph,
				"commitPreparedRawReceiveJoinBatch",
			);
			const nativeCommitSpy = sinon.spy(
				backbone.graph,
				"commitBlocksAndGraphBatch",
			);
			const blockPutColumnsSpy = sinon.spy(
				target.log.log.blocks as any,
				"putKnownManyColumns",
			);
			const lowerNativeGraph = target.log.log.entryIndex.properties.nativeGraph!
				.graph as any;
			const graphPutBatchSpy = sinon.spy(lowerNativeGraph, "putBatch");
			const graphPutAppendChainSpy = sinon.spy(
				lowerNativeGraph,
				"putAppendChain",
			);
			const lowerPutAppendFactsBatchSpy = sinon.spy(
				target.log.log.entryIndex,
				"putAppendFactsBatch",
			);
			try {
				await target.log.onMessage(message!, {
					from: source.node.identity.publicKey,
				} as any);

				expect(target.log.log.length).to.equal(hashes.length);
				expect(nativePreparedJoinCommitSpy.callCount).to.equal(1);
				expect(nativePreparedJoinCommitSpy.firstCall.args[0]).to.have.length(
					hashes.length,
				);
				expect(nativePreparedCommitSpy.callCount).to.equal(0);
				expect(nativeCommitSpy.callCount).to.equal(0);
				expect(blockPutColumnsSpy.callCount).to.equal(0);
				expect(graphPutBatchSpy.callCount).to.equal(0);
				expect(graphPutAppendChainSpy.callCount).to.equal(0);
				expect(lowerPutAppendFactsBatchSpy.callCount).to.equal(1);
				expect(
					lowerPutAppendFactsBatchSpy.firstCall.args[1].nativeGraphUpdated,
				).to.equal(true);
			} finally {
				lowerPutAppendFactsBatchSpy.restore();
				graphPutAppendChainSpy.restore();
				graphPutBatchSpy.restore();
				blockPutColumnsSpy.restore();
				nativeCommitSpy.restore();
				nativePreparedJoinCommitSpy.restore();
				nativePreparedCommitSpy.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("persists receive coordinate items through native backbone WAL without generic coordinate index writes", async () => {
		const session = await TestSession.disconnected(2, {
			indexer: (directory) => createRustIndexer(directory),
		});

		try {
			const setup = {
				domain: createReplicationDomainHash("u32"),
				type: "u32" as const,
				syncronizer: SimpleSyncronizer,
				name: "u32-simple-raw",
			};
			const store = new EventStore<string, any>();
			const coordinateStore = new NativeBackboneMemoryCoordinatePersistenceStore();
			const coordinatePersistence = new NativeBackboneCoordinatePersistence(
				coordinateStore,
			);
			const sourceOpenArgs: any = {
				replicate: false,
				setup,
				nativeGraph: true,
				nativeBackbone: { optional: false },
				keep: () => true,
				timeUntilRoleMaturity: 0,
			};
			const targetOpenArgs: any = {
				replicate: false,
				setup,
				nativeGraph: true,
				nativeBackbone: {
					optional: false,
					coordinatePersistence,
				},
				keep: () => true,
				timeUntilRoleMaturity: 0,
			};
			const source = await session.peers[0].open(store.clone(), {
				args: sourceOpenArgs,
			});
			const target = await session.peers[1].open(store.clone(), {
				args: targetOpenArgs,
			});

			const hashes: string[] = [];
			const entries: any[] = [];
			for (let i = 0; i < 4; i++) {
				const { entry } = await source.add(uuid(), { meta: { next: [] } });
				hashes.push(entry.hash);
				entries.push(entry);
			}

			const sharedLog = target.log as any;
			const backbone = sharedLog._nativeBackbone;
			const coordinateIndex = sharedLog.entryCoordinatesIndex as any;
			expect(sharedLog._nativeBackboneCoordinatePersistence).to.exist;
			expect(sharedLog._residentEntryCoordinatesByHash).to.be.instanceOf(Map);
			expect(sharedLog.canUseBackboneOnlyCoordinatePersistence()).to.equal(
				true,
			);
			const backboneCommitSpy = sinon.spy(
				backbone,
				"commitEntryCoordinatesColumnsBatch",
			);
			const backboneOnlyPersistSpy = sinon.spy(
				sharedLog,
				"persistBackboneOnlyReceiveCoordinateBatch",
			);
			const flushOnAppendSpy = sinon.spy(
				sharedLog._nativeBackboneCoordinatePersistence,
				"flushJournalOnAppend",
			);
			const coordinateIndexBatchPutSpy =
				coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn
					? sinon.spy(
							coordinateIndex,
							"putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn",
						)
					: undefined;
			const coordinateIndexPutSpy =
				coordinateIndex.putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn
					? sinon.spy(
							coordinateIndex,
							"putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn",
						)
					: undefined;
			try {
				const persistItems: any[] = [];
				for (const entry of entries) {
					const replicas = 1;
					const plan = await sharedLog.planEntryLeaders(entry, replicas, {
						roleAge: 0,
						persist: false,
					});
					const prepared =
						sharedLog.createCoordinatePersistenceEntryFromLeaderPlan({
							entry,
							plan,
							replicas,
						});
					expect(prepared).to.not.equal(false);
					persistItems.push({
						coordinates: plan.coordinates,
						entry,
						leaders: plan.leaders,
						replicas,
						assignedToRangeBoundary: plan.assignedToRangeBoundary,
						prepared,
					});
				}
				const persisted =
					await sharedLog.persistBackboneOnlyReceiveCoordinateBatch(
						persistItems,
					);

				expect(backboneOnlyPersistSpy.callCount).to.equal(1);
				expect(persisted?.size).to.equal(hashes.length);
				expect(backboneCommitSpy.callCount).to.equal(1);
				expect(backboneCommitSpy.firstCall.args[0].hashes).to.have.length(
					hashes.length,
				);
				expect(coordinateIndexBatchPutSpy?.callCount ?? 0).to.equal(0);
				expect(coordinateIndexPutSpy?.callCount ?? 0).to.equal(0);
				expect(flushOnAppendSpy.callCount).to.equal(1);
				expect(backbone.getEntryCoordinateHashes()).to.have.length(
					hashes.length,
				);
				expect(
					coordinateStore.files.get("coordinates.wal")?.byteLength,
				).to.be.greaterThan(backbone.coordinateJournalHeader().byteLength);
			} finally {
				coordinateIndexPutSpy?.restore();
				coordinateIndexBatchPutSpy?.restore();
				flushOnAppendSpy.restore();
				backboneOnlyPersistSpy.restore();
				backboneCommitSpy.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("commits raw receive blocks graph and coordinates through one native backbone transaction", async () => {
		const session = await TestSession.disconnected(2, {
			indexer: (directory) => createRustIndexer(directory),
		});

		try {
			const setup = {
				domain: createReplicationDomainHash("u32"),
				type: "u32" as const,
				syncronizer: SimpleSyncronizer,
				name: "u32-simple-raw",
			};
			const store = new EventStore<string, any>();
			const coordinateStore = new NativeBackboneMemoryCoordinatePersistenceStore();
			const coordinatePersistence = new NativeBackboneCoordinatePersistence(
				coordinateStore,
			);
			const sourceOpenArgs: any = {
				replicate: false,
				setup,
				nativeGraph: true,
				nativeBackbone: { optional: false },
				sync: { rawExchangeHeads: true },
				keep: () => true,
				timeUntilRoleMaturity: 0,
			};
			const targetOpenArgs: any = {
				...sourceOpenArgs,
				nativeBackbone: {
					optional: false,
					coordinatePersistence,
				},
			};
			const source = await session.peers[0].open(store.clone(), {
				args: sourceOpenArgs,
			});
			const target = await session.peers[1].open(store.clone(), {
				args: targetOpenArgs,
			});

			const hashes: string[] = [];
			for (let i = 0; i < 4; i++) {
				const { entry } = await source.add(uuid(), { meta: { next: [] } });
				hashes.push(entry.hash);
			}

			let message:
				| RawExchangeHeadsMessage
				| ExchangeHeadsMessage<any>
				| undefined;
			for await (const generated of createRawExchangeHeadsMessages(
				source.log.log,
				hashes,
			)) {
				message = generated;
				break;
			}
			expect(message).to.be.instanceOf(RawExchangeHeadsMessage);

			const sharedLog = target.log as any;
			const backbone = sharedLog._nativeBackbone;
			const nativePreparedCommitSpy = sinon.spy(
				backbone.graph,
				"commitPreparedRawReceiveBatch",
			);
			const nativePreparedJoinCommitSpy = sinon.spy(
				backbone.graph,
				"commitPreparedRawReceiveJoinBatch",
			);
			const combinedCommitSpy = sinon.spy(
				backbone.graph,
				"commitBlocksGraphAndCoordinatesBatch",
			);
			const blockGraphCommitSpy = sinon.spy(
				backbone.graph,
				"commitBlocksAndGraphBatch",
			);
			const coordinateCommitSpy = sinon.spy(
				backbone,
				"commitEntryCoordinatesColumnsBatch",
			);
			const backboneOnlyPersistSpy = sinon.spy(
				sharedLog,
				"persistBackboneOnlyReceiveCoordinateBatch",
			);
			const finishSpy = sinon.spy(
				sharedLog,
				"finishBackboneOnlyReceiveCoordinateBatch",
			);
			try {
				await target.log.onMessage(message!, {
					from: source.node.identity.publicKey,
				} as any);

				expect(target.log.log.length).to.equal(hashes.length);
				expect(nativePreparedJoinCommitSpy.callCount).to.equal(1);
				expect(nativePreparedJoinCommitSpy.firstCall.args[0]).to.have.length(
					hashes.length,
				);
				expect(nativePreparedJoinCommitSpy.firstCall.args[2].hashes).to
					.have.length(hashes.length);
				expect(nativePreparedCommitSpy.callCount).to.equal(0);
				expect(combinedCommitSpy.callCount).to.equal(0);
				expect(blockGraphCommitSpy.callCount).to.equal(0);
				expect(coordinateCommitSpy.callCount).to.equal(0);
				expect(backboneOnlyPersistSpy.callCount).to.equal(0);
				expect(finishSpy.callCount).to.equal(1);
				expect(backbone.getEntryCoordinateHashes()).to.have.length(
					hashes.length,
				);
				expect(
					coordinateStore.files.get("coordinates.wal")?.byteLength,
				).to.be.greaterThan(backbone.coordinateJournalHeader().byteLength);
			} finally {
				finishSpy.restore();
				backboneOnlyPersistSpy.restore();
				coordinateCommitSpy.restore();
				blockGraphCommitSpy.restore();
				combinedCommitSpy.restore();
				nativePreparedJoinCommitSpy.restore();
				nativePreparedCommitSpy.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("uses native raw receive group planning for repeated gids", async () => {
		const session = await TestSession.disconnected(2, {
			indexer: (directory) => createRustIndexer(directory),
		});

		try {
			const setup = {
				domain: createReplicationDomainHash("u32"),
				type: "u32" as const,
				syncronizer: SimpleSyncronizer,
				name: "u32-simple-raw",
			};
			const store = new EventStore<string, any>();
			const profileEvents: any[] = [];
			const openArgs: any = {
				replicate: false,
				setup,
				nativeGraph: true,
				nativeBackbone: { optional: false },
				sync: {
					rawExchangeHeads: true,
					profile: (event: any) => profileEvents.push(event),
				},
				keep: () => true,
				timeUntilRoleMaturity: 0,
			};
			const source = await session.peers[0].open(store.clone(), {
				args: openArgs,
			});
			const target = await session.peers[1].open(store.clone(), {
				args: openArgs,
			});

			const hashes: string[] = [];
			for (let i = 0; i < 3; i++) {
				const { entry } = await source.add(uuid(), {
					meta: { next: [], gidSeed: new Uint8Array([9]) },
				});
				hashes.push(entry.hash);
			}

			let message:
				| RawExchangeHeadsMessage
				| ExchangeHeadsMessage<any>
				| undefined;
			for await (const generated of createRawExchangeHeadsMessages(
				source.log.log,
				hashes,
			)) {
				message = generated;
				break;
			}
			expect(message).to.be.instanceOf(RawExchangeHeadsMessage);

			await target.log.onMessage(message!, {
				from: source.node.identity.publicKey,
			} as any);

			expect(target.log.log.length).to.equal(hashes.length);
			const receivePlanProfile = profileEvents.find(
				(event) => event.name === "sharedLog.receive.plan",
			);
			expect(receivePlanProfile.details.nativeRawGroups).to.equal(true);
			expect(receivePlanProfile.details.predecodedReplicaHits).to.equal(
				hashes.length,
			);
			const metadataProfile = profileEvents.find(
				(event) => event.name === "sharedLog.canAppendBatch.metadata",
			);
			expect(metadataProfile.count).to.equal(hashes.length);
			const verifyProfile = profileEvents.find(
				(event) => event.name === "sharedLog.canAppendBatch.verifySignatures",
			);
			expect(verifyProfile.entries).to.equal(hashes.length);
		} finally {
			await session.stop();
		}
	});

	it("batches request-prune bookkeeping while queuing only newly confirmed hashes", async () => {
		const session = await TestSession.disconnected(2, {
			indexer: (directory) => createRustIndexer(directory),
		});

		try {
			const setup = {
				domain: createReplicationDomainHash("u32"),
				type: "u32" as const,
				syncronizer: SimpleSyncronizer,
				name: "u32-simple-raw",
			};
			const store = new EventStore<string, any>();
			const profileEvents: any[] = [];
			const db = await session.peers[1].open(store.clone(), {
				args: {
					replicate: false,
					setup,
					nativeGraph: true,
					timeUntilRoleMaturity: 0,
					sync: {
						profile: (event: any) => profileEvents.push(event),
					},
				},
			});
			const hashes: string[] = [];
			const entries: any[] = [];
			for (let i = 0; i < 4; i++) {
				const { entry } = await db.add(uuid(), { meta: { next: [] } });
				hashes.push(entry.hash);
				entries.push(entry);
			}

			const removeKnownSpy = sinon.spy(
				db.log as any,
				"removeEntriesKnownByPeer",
			);
			const removePruneRequestsSentSpy = sinon.spy(
				db.log as any,
				"removePruneRequestsSent",
			);
			const removeConfirmedReplicatorsSpy = sinon.spy(
				(db.log as any)._checkedPrune,
				"removeConfirmedReplicators",
			);
			const removeGidBatchSpy = sinon.spy(
				db.log as any,
				"removePeerFromGidPeerHistoryBatch",
			);
			const responseAddStub = sinon
				.stub((db.log as any).responseToPruneDebouncedFn, "add")
				.resolves();
			const hasManyStub = sinon
				.stub(db.log.log.blocks as any, "hasMany")
				.resolves(hashes.map(() => true));
			const nativeMetadataStub = sinon
				.stub(db.log as any, "getNativeLogEntryMetadataBatch")
				.returns(
					entries.map((entry) => ({
						hash: entry.hash,
						gid: entry.meta.gid,
						data: entry.meta.data,
					})),
				);
			const selfHash = db.node.identity.publicKey.hashcode();
			const nativePlanner =
				(db.log as any)._nativeBackbone ?? (db.log as any)._nativeRangePlanner;
			expect(nativePlanner).to.exist;
			const nativeBatchPlanStub = sinon
				.stub(nativePlanner, "planLeadersForGidsBatch")
				.callsFake((...args: unknown[]) =>
					[
						...(args[0] as Iterable<{ gid: string; replicas: number }>),
					].map((_item, index) => ({
						coordinates: [index],
						leaders: new Map([[selfHash, { intersecting: true }]]),
					})),
				);
			const waitForGidStub = sinon
				.stub(db.log as any, "_waitForGidReplicators")
				.callsFake(async (_gid, _replicas, _waitFor, options: any) => {
					options?.onLeader?.(selfHash);
					return new Map([[selfHash, { intersecting: true }]]);
				});
			const waitForEntryStub = sinon
				.stub(db.log as any, "_waitForEntryReplicators")
				.callsFake(async (_entry, _replicas, _waitFor, options: any) => {
					options?.onLeader?.(selfHash);
					return new Map([[selfHash, { intersecting: true }]]);
				});
			try {
				await db.log.onMessage(new RequestIPrune({ hashes }), {
					from: session.peers[0].identity.publicKey,
				} as any);

				expect(removeKnownSpy.callCount).to.equal(1);
				expect([...removeKnownSpy.firstCall.args[0]]).to.deep.equal(hashes);
				expect(removeKnownSpy.firstCall.args[1]).to.equal(
					session.peers[0].identity.publicKey.hashcode(),
				);
				expect(removePruneRequestsSentSpy.callCount).to.equal(1);
				expect([...removePruneRequestsSentSpy.firstCall.args[0]]).to.deep.equal(
					hashes,
				);
				expect(removeConfirmedReplicatorsSpy.callCount).to.equal(1);
				expect(
					[...removeConfirmedReplicatorsSpy.firstCall.args[0]],
				).to.deep.equal(hashes);
				const queuedHashes: string[] = [];
				expect(nativeBatchPlanStub.callCount).to.equal(1);
				const plannedItems = [...nativeBatchPlanStub.firstCall.args[0]];
				expect(plannedItems.length).to.be.greaterThan(0);
				expect(removeGidBatchSpy.callCount).to.equal(1);
				expect(removeGidBatchSpy.firstCall.args[0]).to.equal(
					session.peers[0].identity.publicKey.hashcode(),
				);
				expect([...removeGidBatchSpy.firstCall.args[1]]).to.have.length(
					plannedItems.length,
				);
				expect(waitForGidStub.callCount).to.equal(0);
				expect(responseAddStub.callCount).to.equal(
					plannedItems.length > 0 ? 1 : 0,
				);
				if (plannedItems.length > 0) {
					const [queued] = responseAddStub.firstCall.args;
					expect(queued.hashes).to.have.length(plannedItems.length);
					expect(queued.peers).to.deep.equal([
						session.peers[0].identity.publicKey.hashcode(),
					]);
					for (const queuedHash of queued.hashes) {
						expect(hashes).to.include(queuedHash);
						queuedHashes.push(queuedHash);
					}
				}
				expect(new Set(queuedHashes).size).to.equal(queuedHashes.length);
				const profileNames = profileEvents.map((event) => event.name);
				expect(profileNames).to.include.members([
					"sharedLog.receive.requestPrune.coordinatorCleanup",
					"sharedLog.receive.requestPrune.nativeMetadata",
					"sharedLog.receive.requestPrune.blockHasMany",
					"sharedLog.receive.requestPrune.nativeLeaderPlan",
					"sharedLog.receive.requestPrune.gidCleanup",
					"sharedLog.receive.requestPrune.loop",
					"sharedLog.receive.requestPrune.total",
				]);
				const loopProfile = profileEvents.find(
					(event) => event.name === "sharedLog.receive.requestPrune.loop",
				);
				expect(loopProfile.entries).to.equal(hashes.length);
				expect(loopProfile.details.leaderResponses).to.equal(
					plannedItems.length,
				);
				expect(
					loopProfile.details.leaderResponses +
						loopProfile.details.pendingIHaveCreated +
						loopProfile.details.pendingIHaveExtended,
				).to.equal(hashes.length);
				expect(loopProfile.details.leaderResponseBatches).to.equal(
					plannedItems.length > 0 ? 1 : 0,
				);
				expect(loopProfile.details.pendingIHaveCreated).to.equal(
					hashes.length - plannedItems.length,
				);
			} finally {
				waitForEntryStub.restore();
				waitForGidStub.restore();
				nativeBatchPlanStub.restore();
				nativeMetadataStub.restore();
				hasManyStub.restore();
				responseAddStub.restore();
				removeGidBatchSpy.restore();
				removeConfirmedReplicatorsSpy.restore();
				removePruneRequestsSentSpy.restore();
				removeKnownSpy.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("skips request-prune indexed lookups for missing blocks", async () => {
		const session = await TestSession.disconnected(2, {
			indexer: (directory) => createRustIndexer(directory),
		});

		try {
			const setup = {
				domain: createReplicationDomainHash("u32"),
				type: "u32" as const,
				syncronizer: SimpleSyncronizer,
				name: "u32-simple-raw",
			};
			const store = new EventStore<string, any>();
			const profileEvents: any[] = [];
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup, nativeGraph: true },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: {
					replicate: false,
					setup,
					nativeGraph: true,
					timeUntilRoleMaturity: 0,
					sync: {
						profile: (event: any) => profileEvents.push(event),
					},
				},
			});
			const hashes: string[] = [];
			for (let i = 0; i < 4; i++) {
				const { entry } = await source.add(uuid(), { meta: { next: [] } });
				hashes.push(entry.hash);
			}

			const getShallowSpy = sinon.spy(target.log.log.entryIndex, "getShallow");
			const hasManyStub = sinon
				.stub(target.log.log.blocks as any, "hasMany")
				.resolves(hashes.map(() => false));
			try {
				await target.log.onMessage(new RequestIPrune({ hashes }), {
					from: source.node.identity.publicKey,
				} as any);

				expect(hasManyStub.callCount).to.equal(1);
				expect(getShallowSpy.callCount).to.equal(0);
				const pending = (target.log as any)._pendingIHave as Map<
					string,
					{ clear?: () => void }
				>;
				expect(pending.size).to.equal(hashes.length);
				for (const value of pending.values()) {
					value.clear?.();
				}
				pending.clear();

				const loopProfile = profileEvents.find(
					(event) => event.name === "sharedLog.receive.requestPrune.loop",
				);
				expect(loopProfile.details.indexedFallbackLookups).to.equal(0);
				expect(
					loopProfile.details.skippedIndexedLookupsForMissingBlocks,
				).to.equal(hashes.length);
				expect(loopProfile.details.pendingIHaveCreated).to.equal(hashes.length);
			} finally {
				hasManyStub.restore();
				getShallowSpy.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("skips raw entry materialization for already-present heads", async () => {
		const session = await TestSession.disconnected(2, [
			{
				libp2p: {
					privateKey: keys.privateKeyFromRaw(
						new Uint8Array([
							204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91,
							229, 157, 168, 15, 45, 242, 144, 98, 75, 58, 208, 9,
							223, 143, 251, 52, 252, 159, 64, 83, 52, 197, 24, 246,
							24, 234, 141, 183, 151, 82, 53, 142, 57, 25, 148, 150,
							26, 209, 223, 22, 212, 40, 201, 6, 191, 72, 148, 82, 66,
							138, 199, 185,
						]),
					),
				},
			},
			{
				libp2p: {
					privateKey: keys.privateKeyFromRaw(
						new Uint8Array([
							237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214,
							122, 28, 157, 208, 163, 15, 215, 104, 193, 151, 177, 62,
							231, 253, 120, 122, 222, 174, 242, 120, 50, 165, 97, 8,
							235, 97, 186, 148, 251, 100, 168, 49, 10, 119, 71, 246,
							246, 174, 163, 198, 54, 224, 6, 174, 212, 159, 187, 2,
							137, 47, 192,
						]),
					),
				},
			},
		]);

		try {
			const setup = {
				domain: createReplicationDomainHash("u32"),
				type: "u32" as const,
				syncronizer: SimpleSyncronizer,
				name: "u32-simple-raw",
			};
			const store = new EventStore<string, any>();
			const db1 = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup, nativeGraph: true },
			});
			const db2 = await session.peers[1].open(store.clone(), {
				args: { replicate: false, setup, nativeGraph: true },
			});
			const { entry } = await db1.add(uuid(), { meta: { next: [] } });
			await db2.log.join([entry]);
			expect(await db2.log.log.has(entry.hash)).to.equal(true);

			await db2.log.onMessage(
				new RawExchangeHeadsMessage({
					heads: [
						new RawEntryWithRefs({
							hash: entry.hash,
							bytes: new Uint8Array([255]),
							gidRefrences: [],
						}),
					],
				}),
				{ from: db2.node.identity.publicKey } as any,
			);
		} finally {
			await session.stop();
		}
	});
});
