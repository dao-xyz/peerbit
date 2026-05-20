import { keys } from "@libp2p/crypto";
import { create as createRustIndexer } from "@peerbit/indexer-rust";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { v4 as uuid } from "uuid";
import {
	ExchangeHeadsMessage,
	RawEntryWithRefs,
	RawExchangeHeadsMessage,
	createRawExchangeHeadsMessages,
} from "../src/exchange-heads.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("raw exchange-head sync", () => {
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
			const openArgs = {
				replicate: { factor: 1 },
				setup,
				nativeGraph: true,
				sync: { rawExchangeHeads: true },
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
			const coordinateIndex = db2.log.entryCoordinatesIndex as any;
			const coordinateBatchSpy = sinon.spy(
				coordinateIndex,
				"putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn",
			);
			const persistBatchSpy = sinon.spy(
				db2.log as any,
				"persistCoordinatesBatch",
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
			expect(putKnownSpy.callCount + putKnownManySpy.callCount).to.be.greaterThan(
				0,
			);
			expect(putKnownManySpy.callCount).to.be.greaterThan(0);
			expect(persistBatchSpy.callCount).to.be.greaterThan(0);
			expect(coordinateBatchSpy.callCount).to.be.greaterThan(0);
			persistBatchSpy.restore();
			coordinateBatchSpy.restore();
			putKnownSpy.restore();
			putKnownManySpy.restore();
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
				expect(batchSpy.callCount).to.be.greaterThan(0);
				expect(batchSpy.firstCall.args[0]).to.have.length(entryCount);
				expect(singleSpy.callCount).to.equal(0);
				expect(hasAnyHeadBatchSpy.callCount).to.equal(1);
				expect(hasAnyHeadBatchSpy.firstCall.args[0]).to.have.length(entryCount);
				expect(hasAnyHeadSpy.callCount).to.equal(0);
			} finally {
				hasAnyHeadSpy.restore();
				hasAnyHeadBatchSpy.restore();
				singleSpy.restore();
				batchSpy.restore();
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
