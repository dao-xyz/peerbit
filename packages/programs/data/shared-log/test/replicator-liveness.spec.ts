import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { EventStore } from "./utils/stores/index.js";

describe("replicator liveness", () => {
	let session: TestSession;

	afterEach(async () => {
		await session?.stop();
	});

	it("evicts replicators that disappear without a clean close (sparse topology)", async () => {
		// Create a line topology: 0 <-> 2 <-> 1.
		// This mimics browser/relay scenarios where a peer can learn about a replicator via
		// broadcast replication announcements without having a direct connection that would
		// immediately emit unsubscribe/disconnect events.
		session = await TestSession.disconnectedMock(3);
		await session.connect([
			[session.peers[0], session.peers[2]],
			[session.peers[1], session.peers[2]],
		]);

		const store = new EventStore<string, any>();
		const db0 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				replicas: { min: 2 },
				timeUntilRoleMaturity: 0,
			},
		});
		await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				replicas: { min: 2 },
				timeUntilRoleMaturity: 0,
			},
		});

		await waitForResolved(
			async () => expect((await db0.log.getReplicators()).size).to.equal(2),
			{ timeout: 20_000, delayInterval: 200 },
		);

		// Simulate abrupt tab-close: stop the peer without calling `Program.close()` / sending
		// replication reset messages.
		await session.peers[1].stop();

		await waitForResolved(
			async () => expect((await db0.log.getReplicators()).size).to.equal(1),
			{ timeout: 20_000, delayInterval: 200 },
		);
	});
});
