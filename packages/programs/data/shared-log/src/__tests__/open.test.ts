// Include test utilities
import { LSession } from "@peerbit/test-utils";
import { EventStore } from "./utils/stores";
import { delay, waitForResolved } from "@peerbit/time";

describe("replicators", () => {
	let session: LSession;

	beforeEach(async () => {
		session = await LSession.connected(2);
	});
	afterEach(async () => {
		await session.stop();
	});

	it("uses existing subsription", async () => {
		const store = new EventStore();
		const db1 = await session.peers[0].open(store);
		await session.peers[1].services.pubsub.requestSubscribers(db1.log.topic);
		await waitForResolved(async () =>
			expect(
				(await session.peers[1].services.pubsub.getSubscribers(
					db1.log.topic
				))!.has(session.peers[0].identity.publicKey.hashcode())
			)
		);

		// Adding a delay is necessary so that old subscription messages are not flowing around
		// so that we are sure the we are "really" using existing subsriptions on start to build replicator set
		await delay(1000);

		const db2 = await session.peers[1].open(store.clone());
		await waitForResolved(() =>
			expect(db1.log.replicators()[0].map((x) => x.hash)).toContainAllValues(
				session.peers.map((x) => x.identity.publicKey.hashcode())
			)
		);
		await waitForResolved(() =>
			expect(db2.log.replicators()[0].map((x) => x.hash)).toContainAllValues(
				session.peers.map((x) => x.identity.publicKey.hashcode())
			)
		);
	});
});
