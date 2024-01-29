import { delay, waitFor, waitForResolved } from "@peerbit/time";
import { EventStore, Operation } from "./utils/stores/event-store";
import { TestSession } from "@peerbit/test-utils";
import { Observer, ReplicationSegment, Replicator } from "../role";
import { deserialize } from "@dao-xyz/borsh";
import { Ed25519Keypair, randomBytes, toBase64 } from "@peerbit/crypto";

describe(`role`, () => {
	let session: TestSession;
	let db1: EventStore<string>, db2: EventStore<string>;

	beforeAll(async () => {
		session = await TestSession.disconnected(3, [
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 193, 202, 95, 29, 8, 42, 238, 188, 32, 59, 103, 187, 192,
							93, 202, 183, 249, 50, 240, 175, 84, 87, 239, 94, 92, 9, 207, 165,
							88, 38, 234, 216, 0, 183, 243, 219, 11, 211, 12, 61, 235, 154, 68,
							205, 124, 143, 217, 234, 222, 254, 15, 18, 64, 197, 13, 62, 84,
							62, 133, 97, 57, 150, 187, 247, 215
						]),
						Ed25519Keypair
					).toPeerId()
				}
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 235, 231, 83, 185, 72, 206, 24, 154, 182, 109, 204, 158, 45,
							46, 27, 15, 0, 173, 134, 194, 249, 74, 80, 151, 42, 219, 238, 163,
							44, 6, 244, 93, 0, 136, 33, 37, 186, 9, 233, 46, 16, 89, 240, 71,
							145, 18, 244, 158, 62, 37, 199, 0, 28, 223, 185, 206, 109, 168,
							112, 65, 202, 154, 27, 63, 15
						]),
						Ed25519Keypair
					).toPeerId()
				}
			},
			{
				libp2p: {
					peerId: await deserialize(
						new Uint8Array([
							0, 0, 132, 56, 63, 72, 241, 115, 159, 73, 215, 187, 97, 34, 23,
							12, 215, 160, 74, 43, 159, 235, 35, 84, 2, 7, 71, 15, 5, 210, 231,
							155, 75, 37, 0, 15, 85, 72, 252, 153, 251, 89, 18, 236, 54, 84,
							137, 152, 227, 77, 127, 108, 252, 59, 138, 246, 221, 120, 187,
							239, 56, 174, 184, 34, 141, 45, 242
						]),
						Ed25519Keypair
					).toPeerId()
				}
			}
		]);
		await session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]]
		]);
		await session.peers[1].services.blocks.waitFor(session.peers[0].peerId);
		await session.peers[2].services.blocks.waitFor(session.peers[1].peerId);
	});

	afterAll(async () => {
		await session.stop();
	});

	beforeEach(async () => {
		db1 = await session.peers[0].open(new EventStore<string>());
	});

	afterEach(async () => {
		await db1?.drop();
		await db2?.drop();
	});

	it("none", async () => {
		db2 = (await EventStore.open<EventStore<string>>(
			db1.address!,
			session.peers[1],
			{
				args: { role: "observer" }
			}
		))!;

		await db1.waitFor(session.peers[1].peerId);

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.log.log.values.length === 2); // db2 can write ...
		expect(
			(await db1.log.log.values.toArray()).map(
				(x) => x.payload.getValue().value
			)
		).toContainAllValues(["hello", "world"]);
		expect(db2.log.log.values.length).toEqual(1); // ... but will not receive entries
	});

	describe("observer", () => {
		it("can update", async () => {
			expect(
				db1.log.node.services.pubsub["subscriptions"].get(db1.log.rpc.topic)
					.counter
			).toEqual(1);
			expect(
				db1.log
					.getReplicatorsSorted()
					?.toArray()
					?.map((x) => x.publicKey.hashcode())
			).toEqual([db1.node.identity.publicKey.hashcode()]);
			expect(db1.log.role).toBeInstanceOf(Replicator);
			await db1.log.updateRole("observer");
			expect(db1.log.role).toBeInstanceOf(Observer);
			expect(
				db1.log.node.services.pubsub["subscriptions"].get(db1.log.rpc.topic)
					.counter
			).toEqual(1);
		});

		it("observer", async () => {
			db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: { role: "observer" }
				}
			))!;

			await db1.waitFor(session.peers[1].peerId);

			await db1.add("hello");
			await db2.add("world");

			await waitFor(() => db1.log.log.values.length === 2); // db2 can write ...
			expect(
				(await db1.log.log.values.toArray()).map(
					(x) => x.payload.getValue().value
				)
			).toContainAllValues(["hello", "world"]);
			expect(db2.log.log.values.length).toEqual(1); // ... but will not receive entries
		});
	});

	describe("replictor", () => {
		it("dynamic by default", async () => {
			db2 = (await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1]
			))!;
			const roles: any[] = [];
			db2.log.events.addEventListener("role", (change) => {
				if (
					change.detail.publicKey.equals(session.peers[1].identity.publicKey)
				) {
					roles.push(change.detail);
				}
			});
			/// expect role to update a few times
			await waitForResolved(() => expect(roles.length).toBeGreaterThan(3));
		});

		it("passing by string evens by default", async () => {
			db2 = await EventStore.open<EventStore<string>>(
				db1.address!,
				session.peers[1],
				{
					args: {
						role: "replicator"
					}
				}
			);

			const roles: any[] = [];
			db2.log.events.addEventListener("role", (change) => {
				if (
					change.detail.publicKey.equals(session.peers[1].identity.publicKey)
				) {
					roles.push(change.detail);
				}
			});
			/// expect role to update a few times
			await waitForResolved(() => expect(roles.length).toBeGreaterThan(3));
		});
	});
});

describe("segment", () => {
	describe("overlap", () => {
		it("non-wrapping", () => {
			const s1 = new ReplicationSegment({ offset: 0, factor: 0.5 });
			const s2 = new ReplicationSegment({ offset: 0.45, factor: 0.5 });
			expect(s1.overlaps(s2)).toBeTrue();
			expect(s2.overlaps(s1)).toBeTrue();
		});
		it("wrapped", () => {
			const s1 = new ReplicationSegment({ offset: 0.7, factor: 0.5 });
			const s2 = new ReplicationSegment({ offset: 0.2, factor: 0.2 });
			expect(s1.overlaps(s2)).toBeTrue();
			expect(s2.overlaps(s1)).toBeTrue();
		});

		it("inside", () => {
			const s1 = new ReplicationSegment({ offset: 0.7, factor: 0.5 });
			const s2 = new ReplicationSegment({ offset: 0.8, factor: 0.1 });
			expect(s1.overlaps(s2)).toBeTrue();
			expect(s2.overlaps(s1)).toBeTrue();
		});
		it("insde-wrapped", () => {
			const s1 = new ReplicationSegment({ offset: 0.7, factor: 0.5 });
			const s2 = new ReplicationSegment({ offset: 0.1, factor: 0.1 });
			expect(s1.overlaps(s2)).toBeTrue();
			expect(s2.overlaps(s1)).toBeTrue();
		});
	});
});

/* it("encrypted clock sync write 1 entry replicate false", async () => {
	await waitForPeers(session.peers[1], [client1.id], db1.address.toString());
	const encryptionKey = await client1.keystore.createEd25519Key({
		id: "encryption key",
		group: topic,
	});
	db2 = await client2.open<EventStore<string>>(
		await EventStore.load<EventStore<string>>(
			client2.libp2p.services.blocks,
			db1.address!
		),
		{ replicate: false }
	);

	await db1.add("hello", {
		receiver: {
			next: encryptionKey.keypair.publicKey,
			meta: encryptionKey.keypair.publicKey,
			payload: encryptionKey.keypair.publicKey,
			signatures: encryptionKey.keypair.publicKey,
		},
	});


	// Now the db2 will request sync clocks even though it does not replicate any content
	await db2.add("world");

	await waitFor(() => db1.store.oplog.values.length === 2);
	expect(
		db1.store.oplog.values.toArray().map((x) => x.payload.getValue().value)
	).toContainAllValues(["hello", "world"]);
	expect(db2.store.oplog.values.length).toEqual(1);
}); */
