// Include test utilities
import { TestSession } from "@peerbit/test-utils";
import { waitFor, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { EventStore } from "./utils/stores/event-store.js";

/**
 * Tests that are relavent for browser environments
 */

describe(`network`, () => {
	let session: TestSession;
	let db1: EventStore<string, any>, db2: EventStore<string, any>;

	after(async () => {});

	beforeEach(async () => {});

	afterEach(async () => {
		if (db1) await db1.drop();
		if (db2) await db2.drop();
		await session.stop();
	});

	it("can replicate entries through relay", async () => {
		session = await TestSession.disconnected(3);

		// peer 3 is relay, and dont connect 1 with 2 directly
		await session.peers[0].dial(session.peers[2].getMultiaddrs()[0]);
		await session.peers[1].dial(session.peers[2].getMultiaddrs()[0]);

		await session.peers[0].services.blocks.waitFor(session.peers[2].peerId);
		await session.peers[1].services.blocks.waitFor(session.peers[2].peerId);

		// Sharded pubsub requires a stable shard-root candidate set across peers.
		// Force all shards to resolve to the relay peer so the overlay is reachable
		// even when peer[0] and peer[1] never connect directly.
		const relayHash = (session.peers[2].services as any).pubsub.publicKeyHash as
			| string
			| undefined;
		expect(relayHash, "relayHash").to.be.a("string");
		for (const peer of session.peers as any[]) {
			const servicesAny: any = peer.services;
			const fanoutPlane = servicesAny?.fanout?.topicRootControlPlane;
			const pubsubPlane = servicesAny?.pubsub?.topicRootControlPlane;
			const planes = [...new Set([fanoutPlane, pubsubPlane].filter(Boolean))];
			for (const plane of planes) {
				try {
					plane.setTopicRootCandidates([relayHash]);
				} catch {
					// ignore
				}
			}
		}
		await (session.peers[2].services as any).pubsub.hostShardRootsNow();

		db1 = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicas: { min: 2 },
				replicate: { offset: 0, factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});

		db2 = await EventStore.open<EventStore<string, any>>(
			db1.address!,
			session.peers[1],
			{
				args: {
					replicas: { min: 2 },
					replicate: { offset: 0, factor: 1 },
					timeUntilRoleMaturity: 0,
				},
			},
		);

		// Ensure both peers see each other as replicators before we append.
		await db1.log.waitForReplicator(session.peers[1].identity.publicKey, {
			timeout: 15e3,
			roleAge: 0,
		});
		await db2.log.waitForReplicator(session.peers[0].identity.publicKey, {
			timeout: 15e3,
			roleAge: 0,
		});

		await db1.add("hello");
		await db2.add("world");

		await waitFor(() => db1.log.log.length === 2);
		expect(
			(await db1.log.log.toArray()).map((x) => x.payload.getValue().value),
		).to.have.members(["hello", "world"]);
		await waitForResolved(() => expect(db2.log.log.length).equal(2));
	});
});
