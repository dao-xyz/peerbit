import { privateKeyFromRaw } from "@libp2p/crypto/keys";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
} from "../src/replication.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("join", () => {
	let session: TestSession;
	let db1: EventStore<string>, db2: EventStore<string>;

	before(async () => {
		session = await TestSession.disconnected(3, [
			{
				libp2p: {
					privateKey: await privateKeyFromRaw(
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
					privateKey: await privateKeyFromRaw(
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
					privateKey: privateKeyFromRaw(
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
		db1 = await session.peers[0].open(new EventStore<string>());

		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: { replicate: false },
			},
		))!;

		await db1.waitFor(session.peers[1].peerId);
		const e1 = await db1.add("hello");
		expect(await db2.log.getMyReplicationSegments()).to.have.length(0);
		await db2.log.join([e1.entry], { replicate: true });
		expect(await db2.log.getMyReplicationSegments()).to.have.length(1);
		expect(db2.log.log.length).to.equal(1);
	});

	it("will emit one message when replicating multiple entries", async () => {
		db1 = await session.peers[0].open(new EventStore<string>(), {
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
		db1 = await session.peers[0].open(new EventStore<string>(), {
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
			db1 = await session.peers[0].open(new EventStore<string>());

			db2 = (await EventStore.open<EventStore<string>>(
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
			db1 = await session.peers[0].open(new EventStore<string>());

			db2 = (await EventStore.open<EventStore<string>>(
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
			db1 = await session.peers[0].open(new EventStore<string>());

			db2 = (await EventStore.open<EventStore<string>>(
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
