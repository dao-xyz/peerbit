import { keys } from "@libp2p/crypto";
import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { createReplicationDomainTime } from "../src/replication-domain-time.js";
import {
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
} from "../src/replication.js";
import { RequestMaybeSync, SimpleSyncronizer } from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("join", () => {
	let session: TestSession;
	let db1: EventStore<string, any>, db2: EventStore<string, any>;

		before(async () => {
			session = await TestSession.disconnected(3, [
			{
				libp2p: {
					privateKey: await keys.privateKeyFromRaw(
						new Uint8Array([
							204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91, 229, 157,
							168, 15, 45, 242, 144, 98, 75, 58, 208, 9, 223, 143, 251, 52, 252,
							159, 64, 83, 52, 197, 24, 246, 24, 234, 141, 183, 151, 82, 53,
							142, 57, 25, 148, 150, 26, 209, 223, 22, 212, 40, 201, 6, 191, 72,
							148, 82, 66, 138, 199, 185,
						]),
					),
				},
			},
			{
				libp2p: {
					privateKey: await keys.privateKeyFromRaw(
						new Uint8Array([
							237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214, 122, 28,
							157, 208, 163, 15, 215, 104, 193, 151, 177, 62, 231, 253, 120,
							122, 222, 174, 242, 120, 50, 165, 97, 8, 235, 97, 186, 148, 251,
							100, 168, 49, 10, 119, 71, 246, 246, 174, 163, 198, 54, 224, 6,
							174, 212, 159, 187, 2, 137, 47, 192,
						]),
					),
				},
			},
			{
				libp2p: {
					privateKey: keys.privateKeyFromRaw(
						new Uint8Array([
							27, 246, 37, 180, 13, 75, 242, 124, 185, 205, 207, 9, 16, 54, 162,
							197, 247, 25, 211, 196, 127, 198, 82, 19, 68, 143, 197, 8, 203,
							18, 179, 181, 105, 158, 64, 215, 56, 13, 71, 156, 41, 178, 86,
							159, 80, 222, 167, 73, 3, 37, 251, 67, 86, 6, 90, 212, 16, 251,
							206, 54, 49, 141, 91, 171,
						]),
					),
				},
			},
		]);
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);
			await session.peers[1].services.blocks.waitFor(session.peers[0].peerId);
			await session.peers[2].services.blocks.waitFor(session.peers[1].peerId);
			// Pubsub/fanout use additional DirectStream protocols; ensure those neighbour
			// streams are established in this sparse topology before opening programs.
			await session.peers[0].services.pubsub.waitFor(session.peers[1].peerId);
			await session.peers[1].services.pubsub.waitFor(session.peers[0].peerId);
			await session.peers[1].services.pubsub.waitFor(session.peers[2].peerId);
			await session.peers[2].services.pubsub.waitFor(session.peers[1].peerId);

			const fanout0: any = (session.peers[0].services as any).fanout;
			const fanout1: any = (session.peers[1].services as any).fanout;
			const fanout2: any = (session.peers[2].services as any).fanout;
			await fanout0.waitFor(session.peers[1].peerId);
			await fanout1.waitFor(session.peers[0].peerId);
			await fanout1.waitFor(session.peers[2].peerId);
			await fanout2.waitFor(session.peers[1].peerId);
		});

	after(async () => {
		await session.stop();
	});

	beforeEach(async () => {});

	afterEach(async () => {
		if (db1?.closed === false) {
			await db1?.drop();
		}
		if (db2?.closed === false) {
			await db2?.drop();
		}
	});

	it("can join replicate", async () => {
		db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		db2 = (await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: { replicate: false },
			},
		))!;

		await db1.waitFor(session.peers[1].peerId);
		await db2.log.waitForReplicator(db1.node.identity.publicKey); // in order to make the join operation to index correctly replicator info we want to wait here

		const e1 = await db1.add("hello");
		expect(await db2.log.getMyReplicationSegments()).to.have.length(0);
		await db2.log.join([e1.entry], { replicate: true });
		expect(
			(await db2.log.getMyReplicationSegments()).map((x) => x.width),
		).to.deep.eq([1n]); // a single pin
		expect(db2.log.log.length).to.equal(1);

		// expect entry to be indexed min replicas times
		expect(await db2.log.entryCoordinatesIndex.count()).to.eq(1);

		const indexedEntry = await db2.log.entryCoordinatesIndex.iterate().all();

		expect(indexedEntry[0].value.assignedToRangeBoundary).to.be.false; // since there should be 2 overlapping segments
	});

	it("can join replicate while dynamically replicating", async () => {
		db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		db2 = (await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: { replicate: true },
			},
		))!;

		await db1.waitFor(session.peers[1].peerId);
		await db2.log.waitForReplicator(db1.node.identity.publicKey); // in order to make the join operation to index correctly replicator info we want to wait here

		const e1 = await db1.add("hello");

		const mySegments = await db2.log.getMyReplicationSegments();
		expect(mySegments).to.have.length(1);
		await db2.log.join([e1.entry], { replicate: true });
		expect(db2.log.log.length).to.equal(1);
		expect(db2.log.isAdaptiveReplicating).to.be.true;
		const mySegmentsAfterJoin = await db2.log.getMyReplicationSegments();
		expect(
			mySegmentsAfterJoin.find((x) => x.idString === mySegments[0].idString),
		).to.not.be.undefined;
		expect(await db2.log.entryCoordinatesIndex.count()).to.eq(1);
	});

	describe("mergeSegments", () => {
		it("can join replicate and merge multiple segments", async () => {
			db1 = await session.peers[0].open(new EventStore<string, any>());

			db2 = (await EventStore.open<EventStore<string, any>>(
				db1.address!,
				session.peers[1],
				{
					args: { replicate: false },
				},
			))!;

			await db1.waitFor(session.peers[1].peerId);

			const e1 = await db1.add("hello", { meta: { next: [] } });
			const e2 = await db1.add("hello again", { meta: { next: [] } });

			expect(await db2.log.getMyReplicationSegments()).to.have.length(0);
			await db2.log.join([e1.entry, e2.entry], {
				replicate: { mergeSegments: true },
			});
			expect(await db2.log.getMyReplicationSegments()).to.have.length(1);
			expect(
				Number((await db2.log.getMyReplicationSegments())[0].width),
			).to.be.greaterThan(1); // a segment covering more than one entry
			expect(db2.log.log.length).to.equal(2);
		});

			it("can join and merge with existing segments (2)", async () => {
			db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					domain: createReplicationDomainTime({
						canMerge: (_a, _b) => true,
					}),
				},
			});

			db2 = (await EventStore.open<EventStore<string, any>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: false,
						domain: createReplicationDomainTime({
							canMerge: (_a, _b) => true,
						}),
					},
				},
				))!;

				await db1.waitFor(session.peers[1].peerId);
				// Ensure the writer is known as a replicator before we expect segment
				// announcements to propagate; otherwise this can flake under load.
				await db2.log.waitForReplicator(db1.node.identity.publicKey);

				const e1 = await db1.add("hello", { meta: { next: [] } });

			await db2.log.join([e1.entry], {
				replicate: { mergeSegments: true },
			});

			const mySegments = await db2.log.getMyReplicationSegments();
			expect(mySegments).to.have.length(1);

			await waitForResolved(async () => {
				// make sure segment update propagate correctly
				const node2SegmentsInNode1 = await db1.log.replicationIndex
					.iterate({ query: { hash: db2.node.identity.publicKey.hashcode() } })
					.all();
				expect(node2SegmentsInNode1.map((x) => x.value.idString)).to.deep.eq([
					mySegments[0].idString,
				]);
			});

			const e2 = await db1.add("hello again", { meta: { next: [] } });

			await db2.log.join([e2.entry], {
				replicate: { mergeSegments: true },
			});

			const mySegmentsAfterSecondJoin =
				await db2.log.getMyReplicationSegments();
			expect(mySegmentsAfterSecondJoin).to.have.length(1);

			expect(
				Number((await db2.log.getMyReplicationSegments())[0].width),
			).to.be.greaterThan(1); // a segment covering more than one entry
			expect(mySegmentsAfterSecondJoin[0].idString).to.not.eq(
				mySegments[0].idString,
			);
			expect(db2.log.log.length).to.equal(2);

			await waitForResolved(async () => {
				// make sure segment update propagate correctly
				const node2SegmentsInNode1 = await db1.log.replicationIndex
					.iterate({ query: { hash: db2.node.identity.publicKey.hashcode() } })
					.all();
				expect(node2SegmentsInNode1.map((x) => x.value.idString)).to.deep.eq([
					mySegmentsAfterSecondJoin[0].idString,
				]);
			});
		});

		it("can join and merge with existing segments (3)", async () => {
			db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate: {
						factor: 1,
					},
					domain: createReplicationDomainTime({
						canMerge: (_a, _b) => true,
					}),
				},
			});

			db2 = (await EventStore.open<EventStore<string, any>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: false,
						domain: createReplicationDomainTime({
							canMerge: (_a, _b) => true,
						}),
					},
				},
			))!;

			await db1.waitFor(session.peers[1].peerId);

			const e1 = await db1.add("hello", { meta: { next: [] } });

			await db2.log.join([e1.entry], {
				replicate: { mergeSegments: false },
			});

			const e2 = await db1.add("hello again", { meta: { next: [] } });

			await db2.log.join([e2.entry], {
				replicate: { mergeSegments: false },
			});

			const mySegments = await db2.log.getMyReplicationSegments();
			expect(mySegments).to.have.length(2);

			await waitForResolved(async () => {
				// make sure segment update propagate correctly
				const node2SegmentsInNode1 = await db1.log.replicationIndex
					.iterate({ query: { hash: db2.node.identity.publicKey.hashcode() } })
					.all();
				expect(node2SegmentsInNode1).to.have.length(2);
			});

			const e3 = await db1.add("hello again again", { meta: { next: [] } });

			await db2.log.join([e3.entry], {
				replicate: { mergeSegments: true },
			});

			const mySegmentsAfterSecondJoin =
				await db2.log.getMyReplicationSegments();
			expect(mySegmentsAfterSecondJoin).to.have.length(1);

			expect(
				Number((await db2.log.getMyReplicationSegments())[0].width),
			).to.be.greaterThan(1); // a segment covering more than one entry
			expect(mySegmentsAfterSecondJoin[0].idString).to.not.eq(
				mySegments[0].idString,
			);
			expect(db2.log.log.length).to.equal(3);

			await waitForResolved(async () => {
				// make sure segment update propagate correctly
				const node2SegmentsInNode1 = await db1.log.replicationIndex
					.iterate({ query: { hash: db2.node.identity.publicKey.hashcode() } })
					.all();
				expect(node2SegmentsInNode1.map((x) => x.value.idString)).to.deep.eq([
					mySegmentsAfterSecondJoin[0].idString,
				]);
			});
		});

		it("can join merge into existing segment", async () => {
			let origin = new Date(+new Date() - 1e3); // some time back so we can replicate ranges below with negative offsets without going negative
			db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					replicate: {
						factor: 1,
					},
					domain: createReplicationDomainTime({
						origin,
						canMerge: (_a, _b) => true,
					}),
				},
			});

			const domain = createReplicationDomainTime({
				origin,
				canMerge: (_a, _b) => true,
			});

			db2 = (await EventStore.open<EventStore<string, any>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: false,
						domain,
					},
				},
			))!;

			const e1 = await db1.add("hello", { meta: { next: [] } });

			await db1.close(); // does not need for this test

			const expectedCoordinate = await domain(db2.log).fromEntry(e1.entry);

			await db2.log.replicate({
				factor: 1e3,
				offset: expectedCoordinate - 10,
				normalized: false,
			});
			await db2.log.join([e1.entry], {
				replicate: { mergeSegments: true },
			});

			const mergedSegments = await db2.log.getMyReplicationSegments();
			expect(mergedSegments).to.have.length(1);
			expect(mergedSegments[0].width).to.eq(1e3);
		});

		it("rejoins same entry with canMerge", async () => {
			db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					domain: createReplicationDomainTime({
						canMerge: (_a, _b) => true,
					}),
				},
			});

			db2 = (await EventStore.open<EventStore<string, any>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: false,
						domain: createReplicationDomainTime({
							canMerge: (_a, _b) => true,
						}),
					},
				},
			))!;

			await db1.waitFor(session.peers[1].peerId);

			const e1 = await db1.add("hello", { meta: { next: [] } });

			await db2.log.join([e1.entry], {
				replicate: { mergeSegments: true },
			});

			const mySegments = await db2.log.getMyReplicationSegments();
			expect(mySegments).to.have.length(1);

			const emittedMessages: any[] = [];

			const sendFn = db2.log.rpc.send.bind(db2.log.rpc);
			db2.log.rpc.send = (message, options) => {
				emittedMessages.push(message);
				return sendFn(message, options);
			};

			await db2.log.join([e1.entry], {
				replicate: { mergeSegments: true },
			});

			const mySegmentsAgain = await db2.log.getMyReplicationSegments();
			expect(mySegmentsAgain.map((x) => x.start1)).to.deep.eq(
				mySegments.map((x) => x.start1),
			);
			expect(mySegmentsAgain.map((x) => x.width)).to.deep.eq(
				mySegments.map((x) => x.width),
			);

			expect(emittedMessages).to.have.length(0);
		});

		it("will persist both segments when can not merge", async () => {
			db1 = await session.peers[0].open(new EventStore<string, any>(), {
				args: {
					domain: createReplicationDomainTime({
						canMerge: (_a, _b) => false,
					}),
				},
			});

			db2 = (await EventStore.open<EventStore<string, any>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						replicate: false,
						domain: createReplicationDomainTime({
							canMerge: (_a, _b) => false,
						}),
					},
				},
			))!;

			await db1.waitFor(session.peers[1].peerId);

			const e1 = await db1.add("hello", { meta: { next: [] } });
			const e2 = await db2.add("hello again", { meta: { next: [] } });

			await db2.log.join([e1.entry], {
				replicate: { mergeSegments: true },
			});

			await db2.log.join([e2.entry], {
				replicate: { mergeSegments: true },
			});

			const mySegments = await db2.log.getMyReplicationSegments();
			expect(mySegments).to.have.length(2);
		});
	});

	it("join replicate, assume synced", async () => {
		db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: { factor: 1 },
				setup: {
					domain: createReplicationDomainHash("u32"),
					name: "u32-hash",
					syncronizer: SimpleSyncronizer, // we set this synchronizer so we can test the assumeSynced option that it does not initate syncing
					type: "u32",
				},
				waitForReplicatorTimeout: 1e3,
			},
		});

		db2 = (await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicate: false,
					setup: {
						domain: createReplicationDomainHash("u32"),
						name: "u32-hash",
						syncronizer: SimpleSyncronizer, // we set this synchronizer so we can test the assumeSynced option that it does not initate syncing
						type: "u32",
					},
					waitForReplicatorTimeout: 1e3,
				},
			},
		))!;

		await db2.log.waitForReplicator(session.peers[0].identity.publicKey);
		const e1 = await db1.add("hello", { meta: { next: [] } });
		let syncMessagesSent = false;
		let sendFn = db2.log.rpc.send.bind(db2.log.rpc);
		db2.log.rpc.send = async (message: any, options: any) => {
			if (message instanceof RequestMaybeSync) {
				syncMessagesSent = true;
			}
			return sendFn(message, options);
		};

		let rebalanced = false;

		const onReplicationChange = db2.log.onReplicationChange.bind(db2.log);

		db2.log.onReplicationChange = async (changes) => {
			const out = await onReplicationChange(changes);
			rebalanced = true;
			return out;
		};

		await db2.log.join([e1.entry], {
			replicate: { mergeSegments: true, assumeSynced: true },
		});

		await delay(3e3);
		expect(syncMessagesSent).to.be.false;
		expect(rebalanced).to.be.false;

		// checkl that no rebalance occurs
	});

	it("join with slow replicate", async () => {
		db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: { factor: 1 },
				waitForReplicatorTimeout: 1e3,
				replicas: {
					min: 1,
				},
			},
		});

		db2 = (await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicate: false,
					waitForReplicatorTimeout: 1e3,
					replicas: {
						min: 1, // make sure that we can prune even if there is one replicator
					},
				},
			},
		))!;

		await db2.log.waitForReplicator(session.peers[0].identity.publicKey);
		const e1 = await db1.add("hello", { meta: { next: [] } });
		const replicateFn = db2.log.replicate.bind(db2.log);

		let deferred = pDefer();
		db2.log.replicate = async (range, options) => {
			await deferred.promise;
			return replicateFn(range, options);
		};

		const joinPromise = db2.log.join([e1.entry], { replicate: true });

		await waitForResolved(() => expect(db2.log.log.length).to.eq(1));

		await db2.log.rebalanceAll();
		await delay(3000);

		// checkl that no rebalance occurs
		expect(db2.log.log.length).to.eq(1);
		deferred.resolve();
		await joinPromise;
	});

	it("will emit one message when replicating multiple entries", async () => {
		db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: false },
		});
		db2 = db1.clone();

		const onSubscriptionChange = db2.log.handleSubscriptionChange.bind(db2.log);

		let subscribed = false;
		db2.log.handleSubscriptionChange = async (a, b, c) => {
			await onSubscriptionChange(a, b, c);
			subscribed = true;
			return;
		};

		db2 = await session.peers[1].open(db2, {
			args: { replicate: false },
		});

		await waitForResolved(() => expect(subscribed).to.be.true); // we do this to assert that this message producing event has happend before we test stuff later

		const e1 = await db1.add("hello", { meta: { next: [] } });
		const e2 = await db1.add("hello again", { meta: { next: [] } });

		let sentMessages: any[] = [];
		const sendFn = db2.log.rpc.send.bind(db2.log.rpc);
		db2.log.rpc.send = async (message: any, options: any) => {
			sentMessages.push(message);
			return sendFn(message, options);
		};

		// now join entries
		await db2.log.join([e1.entry, e2.entry], { replicate: true });
		expect(db2.log.log.length).to.equal(2);

		expect(
			sentMessages.filter((x) => x instanceof AllReplicatingSegmentsMessage),
		).to.have.length(0);
		const replicationMessages = sentMessages.filter(
			(x) => x instanceof AddedReplicationSegmentMessage,
		);

		expect(replicationMessages).to.have.length(1);
		expect(replicationMessages[0].segments).to.have.length(2);
	});

	it("will emit one message when replicating new and already joined entries", async () => {
		db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: false },
		});
		db2 = db1.clone();

		const onSubscriptionChange = db2.log.handleSubscriptionChange.bind(db2.log);

		let subscribed = false;
		db2.log.handleSubscriptionChange = async (a, b, c) => {
			await onSubscriptionChange(a, b, c);
			subscribed = true;
			return;
		};

		db2 = await session.peers[1].open(db2, {
			args: { replicate: false },
		});

		await waitForResolved(() => expect(subscribed).to.be.true); // we do this to assert that this message producing event has happend before we test stuff later

		const e1 = await db1.add("hello", { meta: { next: [] } });
		const e2 = await db1.add("hello again", { meta: { next: [] } });

		// join one entry but dont replicate

		await db2.log.join([e1.entry]);

		let sentMessages: any[] = [];
		const sendFn = db2.log.rpc.send.bind(db2.log.rpc);
		db2.log.rpc.send = async (message: any, options: any) => {
			sentMessages.push(message);
			return sendFn(message, options);
		};

		// now join e1 again and replicate this time
		await db2.log.join([e1.entry, e2.entry], { replicate: true });
		expect(db2.log.log.length).to.equal(2);

		expect(
			sentMessages.filter((x) => x instanceof AllReplicatingSegmentsMessage),
		).to.have.length(0);
		const replicationMessages = sentMessages.filter(
			(x) => x instanceof AddedReplicationSegmentMessage,
		);

		expect(replicationMessages).to.have.length(1);
		expect(replicationMessages[0].segments).to.have.length(2);
	});

	describe("already but not replicated", () => {
		it("entry", async () => {
			db1 = await session.peers[0].open(new EventStore<string, any>());

			db2 = (await EventStore.open<EventStore<string, any>>(
				db1.address!,
				session.peers[1],
				{
					args: { replicate: false },
				},
			))!;

			await db1.waitFor(session.peers[1].peerId);
			const e1 = await db1.add("hello");
			expect(await db2.log.getMyReplicationSegments()).to.have.length(0);
			await db2.log.join([e1.entry]);
			expect(db2.log.log.length).to.equal(1);
			expect(await db2.log.getMyReplicationSegments()).to.have.length(0);
			await db2.log.join([e1.entry], { replicate: true });
			expect(await db2.log.getMyReplicationSegments()).to.have.length(1);
			expect(db2.log.log.length).to.equal(1);
		});

		it("hash", async () => {
			db1 = await session.peers[0].open(new EventStore<string, any>());

			db2 = (await EventStore.open<EventStore<string, any>>(
				db1.address!,
				session.peers[1],
				{
					args: { replicate: false },
				},
			))!;

			await db1.waitFor(session.peers[1].peerId);
			const e1 = await db1.add("hello");
			expect(await db2.log.getMyReplicationSegments()).to.have.length(0);
			await db2.log.join([e1.entry.hash]);
			expect(db2.log.log.length).to.equal(1);
			expect(await db2.log.getMyReplicationSegments()).to.have.length(0);
			await db2.log.join([e1.entry.hash], { replicate: true });
			expect(await db2.log.getMyReplicationSegments()).to.have.length(1);
			expect(db2.log.log.length).to.equal(1);
		});
		it("shallow entry", async () => {
			db1 = await session.peers[0].open(new EventStore<string, any>());

			db2 = (await EventStore.open<EventStore<string, any>>(
				db1.address!,
				session.peers[1],
				{
					args: { replicate: false },
				},
			))!;

			await db1.waitFor(session.peers[1].peerId);
			const e1 = await db1.add("hello");
			expect(await db2.log.getMyReplicationSegments()).to.have.length(0);
			await db2.log.join([e1.entry.toShallow(true)]);
			expect(db2.log.log.length).to.equal(1);
			expect(await db2.log.getMyReplicationSegments()).to.have.length(0);
			await db2.log.join([e1.entry.toShallow(true)], { replicate: true });
			expect(await db2.log.getMyReplicationSegments()).to.have.length(1);
			expect(db2.log.log.length).to.equal(1);
		});
	});
});
