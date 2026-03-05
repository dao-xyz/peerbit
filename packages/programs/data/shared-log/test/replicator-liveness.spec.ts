import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	ReplicationPingMessage,
	RequestReplicationInfoMessage,
} from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

describe("replicator liveness", () => {
	let session: TestSession;

	afterEach(async () => {
		await session?.stop();
	});

	it("evicts replicators that disappear without a clean close and emits leave", async () => {
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
		const peerHash = session.peers[1].identity.publicKey.hashcode();
		const leaveEvents: string[] = [];
		db0.log.events.addEventListener("replicator:leave", (event) => {
			leaveEvents.push(event.detail.publicKey.hashcode());
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
		expect(leaveEvents).to.deep.equal([peerHash]);
	});

	it("does not evict a healthy replicator after a single missed ping", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore<string, any>();
		const db0 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});

		const peerHash = session.peers[1].identity.publicKey.hashcode();
		await waitForResolved(
			async () => expect((await db0.log.getReplicators()).size).to.equal(2),
			{ timeout: 20_000, delayInterval: 100 },
		);

		const originalSend = db0.log.rpc.send.bind(db0.log.rpc);
		let pingFailuresLeft = 1;
		let failRecoveryRequests = true;
		db0.log.rpc.send = async (message: any, _options: any) => {
			if (message instanceof ReplicationPingMessage && pingFailuresLeft-- > 0) {
				throw new Error("synthetic ping miss");
			}
			if (
				failRecoveryRequests &&
				message instanceof RequestReplicationInfoMessage
			) {
				throw new Error("synthetic replication-info miss");
			}
			return originalSend(message, _options);
		};

		try {
			await (db0.log as any).probeReplicatorLiveness(peerHash);
			expect((await db0.log.getReplicators()).size).to.equal(2);

			failRecoveryRequests = false;
			await (db0.log as any).probeReplicatorLiveness(peerHash);
			expect((await db0.log.getReplicators()).size).to.equal(2);
		} finally {
			db0.log.rpc.send = originalSend;
		}
	});

	it("can relearn a liveness-evicted replicator from later replication info", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore<string, any>();
		const db0 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});
		const peerHash = session.peers[1].identity.publicKey.hashcode();
		const joinEvents: string[] = [];
		const leaveEvents: string[] = [];
		db0.log.events.addEventListener("replicator:join", (event) => {
			joinEvents.push(event.detail.publicKey.hashcode());
		});
		db0.log.events.addEventListener("replicator:leave", (event) => {
			leaveEvents.push(event.detail.publicKey.hashcode());
		});

		const db1 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity: 0,
			},
		});

		await waitForResolved(
			async () => expect((await db0.log.getReplicators()).size).to.equal(2),
			{ timeout: 20_000, delayInterval: 100 },
		);
		await waitForResolved(
			() =>
				expect(joinEvents.filter((eventHash) => eventHash === peerHash)).to.have.length(
					1,
				),
			{ timeout: 20_000, delayInterval: 100 },
		);

		const originalSend = db0.log.rpc.send.bind(db0.log.rpc);
		let pingFailuresLeft = 2;
		let failRecoveryRequests = true;
		db0.log.rpc.send = async (message: any, _options: any) => {
			if (message instanceof ReplicationPingMessage && pingFailuresLeft-- > 0) {
				throw new Error("synthetic ping miss");
			}
			if (
				failRecoveryRequests &&
				message instanceof RequestReplicationInfoMessage
			) {
				throw new Error("synthetic replication-info miss");
			}
			return originalSend(message, _options);
		};

		try {
			await (db0.log as any).probeReplicatorLiveness(peerHash);
			await (db0.log as any).probeReplicatorLiveness(peerHash);

			await waitForResolved(
				async () => expect((await db0.log.getReplicators()).size).to.equal(1),
				{ timeout: 5_000, delayInterval: 100 },
			);
			expect(leaveEvents).to.deep.equal([peerHash]);

			failRecoveryRequests = false;
			await db1.log.replicate({ factor: 1 }, { reset: true });

			await waitForResolved(
				async () => expect((await db0.log.getReplicators()).size).to.equal(2),
				{ timeout: 20_000, delayInterval: 100 },
			);
			await waitForResolved(
				() =>
					expect(joinEvents.filter((eventHash) => eventHash === peerHash)).to.have
						.length(2),
				{ timeout: 20_000, delayInterval: 100 },
			);
		} finally {
			db0.log.rpc.send = originalSend;
		}
	});
});
