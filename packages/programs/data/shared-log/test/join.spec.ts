import { deserialize } from "@dao-xyz/borsh";
import { Ed25519Keypair } from "@peerbit/crypto";
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
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 193, 202, 95, 29, 8, 42, 238, 188, 32, 59, 103, 187, 192,
							93, 202, 183, 249, 50, 240, 175, 84, 87, 239, 94, 92, 9, 207, 165,
							88, 38, 234, 216, 0, 183, 243, 219, 11, 211, 12, 61, 235, 154, 68,
							205, 124, 143, 217, 234, 222, 254, 15, 18, 64, 197, 13, 62, 84,
							62, 133, 97, 57, 150, 187, 247, 215,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 235, 231, 83, 185, 72, 206, 24, 154, 182, 109, 204, 158, 45,
							46, 27, 15, 0, 173, 134, 194, 249, 74, 80, 151, 42, 219, 238, 163,
							44, 6, 244, 93, 0, 136, 33, 37, 186, 9, 233, 46, 16, 89, 240, 71,
							145, 18, 244, 158, 62, 37, 199, 0, 28, 223, 185, 206, 109, 168,
							112, 65, 202, 154, 27, 63, 15,
						]),
						Ed25519Keypair,
					).toPeerId(),
				},
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 132, 56, 63, 72, 241, 115, 159, 73, 215, 187, 97, 34, 23,
							12, 215, 160, 74, 43, 159, 235, 35, 84, 2, 7, 71, 15, 5, 210, 231,
							155, 75, 37, 0, 15, 85, 72, 252, 153, 251, 89, 18, 236, 54, 84,
							137, 152, 227, 77, 127, 108, 252, 59, 138, 246, 221, 120, 187,
							239, 56, 174, 184, 34, 141, 45, 242,
						]),
						Ed25519Keypair,
					).toPeerId(),
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
