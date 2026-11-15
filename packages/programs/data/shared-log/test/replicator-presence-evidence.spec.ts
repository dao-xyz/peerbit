import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { ReplicationPingMessage } from "../src/replication.js";
import type { ReplicatorLivenessMonitor } from "../src/replicator-liveness.js";
import { EventStore } from "./utils/stores/index.js";

describe("waitForReplicator presence evidence", () => {
	let session: TestSession;
	const sandbox = sinon.createSandbox();

	afterEach(async () => {
		sandbox.restore();
		await session?.stop();
	});

	it("currently lets discovery hints reset ACK misses, then evicts after two negative confirmations", async () => {
		session = await TestSession.connected(2);
		const store = new EventStore<string, any>();
		const args = { replicate: { factor: 1 }, timeUntilRoleMaturity: 0 };
		const db0 = await session.peers[0].open(store, { args });
		const log = db0.log as any;
		const liveness: ReplicatorLivenessMonitor = log._liveness;
		// Stop before any remote target exists; stopping does not drain a probe.
		liveness.stopReplicatorLivenessSweep();
		await session.peers[1].open(store.clone(), { args });
		const remoteKey = session.peers[1].identity.publicKey;
		const peerHash = remoteKey.hashcode();
		await waitForResolved(async () => {
			expect((await db0.log.getReplicators()).has(peerHash)).to.be.true;
			expect(
				await db0.log.replicationIndex.count({ query: { hash: peerHash } }),
			).to.be.greaterThan(0);
		});

		// Drive the probe directly; elapsed sweep time is not evidence in this test.
		const originalSend = db0.log.rpc.send.bind(db0.log.rpc);
		let failedAcks = 0;
		sandbox.stub(db0.log.rpc, "send").callsFake(async (...parameters) => {
			if (parameters[0] instanceof ReplicationPingMessage) {
				failedAcks++;
				throw new Error("controlled ACK failure");
			}
			return originalSend(...parameters);
		});
		// Recovery messages are a separate source of positive activity. Do not let
		// them reintroduce the peer during this controlled negative-evidence phase.
		sandbox.stub(log, "scheduleReplicationInfoRequests");
		// Stop new network/subscription admission, then drain work admitted before
		// these stubs. Merely stopping the sweep would leave queued V2 replies able
		// to reset activity or re-add ranges during the controlled probes below.
		sandbox.stub(db0.log, "onMessage").resolves();
		sandbox.stub(log, "runSubscriptionChangeCallback").returns(undefined);
		await log.drainPeerReceiveHandlers(peerHash);
		await log.drainSubscriptionChangeCallbacks();
		await log.drainReplicationInfoApplyQueues();
		log.cancelReplicationInfoRequests(peerHash);

		let discoveryHint = false;
		const originalTopicSubscribers = log._getTopicSubscribers.bind(log);
		sandbox
			.stub(log, "_getTopicSubscribers")
			.callsFake(async (topic) =>
				topic === db0.log.rpc.topic
					? discoveryHint
						? [remoteKey]
						: []
					: originalTopicSubscribers(topic),
			);
		const originalConfirm =
			liveness.confirmReplicatorSubscriberPresence.bind(liveness);
		let hintedConfirmations = 0;
		let negativeConfirmations = 0;
		sandbox
			.stub(liveness, "confirmReplicatorSubscriberPresence")
			.callsFake(async (hash) => {
				if (discoveryHint) {
					const confirmed = await originalConfirm(hash);
					expect(confirmed).to.be.true;
					hintedConfirmations++;
					return confirmed;
				}
				// Supply a completed negative confirmation at the existing seam,
				// rather than waiting for subscriber-discovery timeouts. This does not
				// claim that stale discovery hints expire on the wire.
				negativeConfirmations++;
				return false;
			});
		const leaves: string[] = [];
		db0.log.events.addEventListener("replicator:leave", (event) => {
			leaves.push(event.detail.publicKey.hashcode());
		});
		const probe = async () => {
			liveness.markReplicatorActivity(peerHash, Date.now() - 60_000);
			await liveness.probeReplicatorLiveness(peerHash);
		};
		const expectPresent = async () => {
			expect((await db0.log.getReplicators()).has(peerHash)).to.be.true;
			expect(
				await db0.log.replicationIndex.count({ query: { hash: peerHash } }),
			).to.be.greaterThan(0);
			expect(leaves).to.deep.equal([]);
		};

		await probe();
		expect(liveness._replicatorLivenessFailures.get(peerHash)).to.equal(1);
		await expectPresent();

		// The same unrefreshed hint suppresses eviction despite every ACK failing,
		// including clearing the real negative streak established above.
		discoveryHint = true;
		for (let i = 0; i < 2; i++) {
			await probe();
			expect(liveness._replicatorLivenessFailures.has(peerHash)).to.be.false;
			await expectPresent();
		}
		expect(hintedConfirmations).to.equal(2);
		expect(failedAcks).to.equal(3);
		expect(negativeConfirmations).to.equal(1);

		discoveryHint = false;
		await probe();
		expect(liveness._replicatorLivenessFailures.get(peerHash)).to.equal(1);
		await expectPresent();
		await probe();
		expect((await db0.log.getReplicators()).has(peerHash)).to.be.false;
		expect(
			await db0.log.replicationIndex.count({ query: { hash: peerHash } }),
		).to.equal(0);
		expect(leaves).to.deep.equal([peerHash]);
		expect(failedAcks).to.equal(5);
		expect(negativeConfirmations).to.equal(3);

		await probe();
		expect(failedAcks).to.equal(5);
		expect(negativeConfirmations).to.equal(3);
		expect(leaves).to.deep.equal([peerHash]);
	});
});
