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

	const forceRelayShardRoots = async (
		currentSession: TestSession,
		relayIndex: number,
	) => {
		const relayHash = (currentSession.peers[relayIndex].services as any).pubsub
			.publicKeyHash as string | undefined;
		expect(relayHash, "relayHash").to.be.a("string");
		for (const peer of currentSession.peers as any[]) {
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
		await (currentSession.peers[relayIndex].services as any).pubsub.hostShardRootsNow();
	};

	after(async () => {});

	beforeEach(async () => {});

	afterEach(async () => {
		if (db1) await db1.drop();
		if (db2) await db2.drop();
		await session.stop();
	});

	it("can replicate entries through relay without forced shard-root candidates", async function () {
		this.timeout(120_000);

		session = await TestSession.disconnected(3);

		// Keep a relay-only topology while still converging sharded pubsub/fanout
		// root candidates for this connected component.
		await session.connect([
			[session.peers[0], session.peers[2]],
			[session.peers[1], session.peers[2]],
		]);

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

	it("prunes departed replicator in relay topology after abrupt stop", async () => {
		session = await TestSession.disconnected(3);

		await session.connect([
			[session.peers[0], session.peers[2]],
			[session.peers[1], session.peers[2]],
		]);

		// This test targets shared-log liveness after abrupt stop, not shard-root
		// convergence. Pin the relay as root so root placement stays out of the way.
		await forceRelayShardRoots(session, 2);

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

		const peerHash = session.peers[1].identity.publicKey.hashcode();
		await db1.log.waitForReplicator(session.peers[1].identity.publicKey, {
			timeout: 20_000,
			roleAge: 0,
		});
		await waitForResolved(
			async () => expect((await db1.log.getReplicators()).size).to.equal(2),
			{ timeout: 20_000, delayInterval: 100 },
		);

		// Simulate abrupt tab/process loss without program-level close/reset messages.
		db2 = undefined as any;
		await (session.peers[1] as any).libp2p.stop();

		await waitForResolved(
			async () => {
				await (db1.log as any).probeReplicatorLiveness(peerHash);
				expect((await db1.log.getReplicators()).size).to.equal(1);
			},
			{ timeout: 30_000, delayInterval: 100 },
		);
	});
});
