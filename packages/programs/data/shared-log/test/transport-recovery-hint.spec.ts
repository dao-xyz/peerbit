import { Ed25519PublicKey } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { EventStore } from "./utils/stores/index.js";

const eventName = "peer:stream-ready";
const observeListeners = (pubsub: any) => {
	const listeners: Array<{
		original: (event: Event) => void;
		observed: sinon.SinonSpy;
		signal: AbortSignal;
	}> = [];
	const add = pubsub.addEventListener.bind(pubsub);
	sinon.stub(pubsub, "addEventListener").callsFake((...args: any[]) => {
		if (args[0] === eventName) {
			const observed = sinon.spy(args[1]);
			listeners.push({ original: args[1], observed, signal: args[2].signal });
			args[1] = observed;
		}
		return add(...args);
	});
	return listeners;
};

describe("receive admission transport recovery hint lifecycle", () => {
	let session: TestSession | undefined;
	afterEach(async () => {
		try {
			await session?.stop();
		} finally {
			sinon.restore();
		}
	});

	it("wakes only the current open session and removes its listener across reopen", async () => {
		session = await TestSession.connected(2);
		const [local, remote] = session.peers;
		const pubsub = local.services.pubsub as any;
		const listeners = observeListeners(pubsub);
		const args = { replicate: 1, timeUntilRoleMaturity: 0 };
		const store = await local.open(new EventStore(), { args });
		await remote.open(store.clone(), { args });
		const log = store.log as any;
		const remoteKey = remote.identity.publicKey;
		const peerHash = remoteKey.hashcode();
		const event = () => new CustomEvent(eventName, { detail: remoteKey });
		const awaitReady = async () => {
			await waitForResolved(
				() => {
					expect(log._peerSessions.current(peerHash)?.phase).to.equal("open");
					expect(log._v2Receive._receiveStates.get(peerHash)?.phase).to.equal(
						"active",
					);
					expect(log._v2Send._sendStates.get(peerHash)?.established).to.equal(
						true,
					);
				},
				{ timeout: 5_000 },
			);
			await log._v2Send.drain();
		};
		await awaitReady();
		const peerSession = log._peerSessions.current(peerHash);
		const receiveEpoch = log._peerSessions.receiveEpoch(peerHash);
		await log._v2Send.confirmLatestForPeer(
			{
				peerHash,
				peerSession,
				receiverTransportSession: log._peerSyncCapabilitySessions.get(peerHash),
			},
			{ timeout: 5_000 },
		);
		await log._v2Send.drain();
		const sendState = log._v2Send._sendStates.get(peerHash);
		const receiveState = log._v2Receive._receiveStates.get(peerHash);
		const proof = sendState.appliedRevision;
		expect(proof).not.to.equal(undefined);
		const sender = sinon.spy(log._v2Send, "resumeAfterTransportRecovery");
		const receiver = sinon.spy(log._v2Receive, "resumeAfterTransportRecovery");
		expect(listeners).to.have.length(1);
		pubsub.dispatchEvent(event());
		expect(sender.calledOnceWithExactly(peerHash, peerSession)).to.equal(true);
		expect(
			receiver.calledOnceWithExactly({ peerHash, peerSession, receiveEpoch }),
		).to.equal(true);
		expect(log._peerSessions.current(peerHash)).to.equal(peerSession);
		expect(log._peerSessions.receiveEpoch(peerHash)).to.equal(receiveEpoch);
		expect(log._v2Send._sendStates.get(peerHash)).to.equal(sendState);
		expect(log._v2Receive._receiveStates.get(peerHash)).to.equal(receiveState);
		expect(sendState.appliedRevision).to.equal(proof);

		sender.resetHistory();
		receiver.resetHistory();
		pubsub.dispatchEvent(
			new CustomEvent(eventName, {
				detail: new Ed25519PublicKey({
					publicKey: new Uint8Array(32).fill(77),
				}),
			}),
		);
		log._peerSessions.rotate(peerHash, "departing");
		pubsub.dispatchEvent(event());
		expect(sender.notCalled && receiver.notCalled).to.equal(true);

		const oldListener = listeners[0];
		await store.close();
		expect(oldListener.signal.aborted).to.equal(true);
		oldListener.observed.resetHistory();
		pubsub.dispatchEvent(event());
		expect(
			oldListener.observed.notCalled,
			"listener is removed, not merely gated",
		).to.equal(true);
		expect(sender.notCalled && receiver.notCalled).to.equal(true);

		await local.open(store, { args });
		await awaitReady();
		expect(listeners).to.have.length(2);
		sender.resetHistory();
		receiver.resetHistory();
		oldListener.original(event()); // A previously queued callback stays fenced.
		expect(sender.notCalled && receiver.notCalled).to.equal(true);
		const replacement = log._peerSessions.current(peerHash);
		expect(replacement).not.to.equal(peerSession);
		pubsub.dispatchEvent(event());
		expect(sender.calledOnceWithExactly(peerHash, replacement)).to.equal(true);
		expect(
			receiver.calledOnceWithExactly({
				peerHash,
				peerSession: replacement,
				receiveEpoch: log._peerSessions.receiveEpoch(peerHash),
			}),
		).to.equal(true);
		expect(oldListener.observed.notCalled).to.equal(true);
	});

	it("removes the recovery listener when communication setup fails during open", async () => {
		session = await TestSession.disconnected(1);
		const peer = session.peers[0];
		const pubsub = peer.services.pubsub as any;
		const listeners = observeListeners(pubsub);
		const store = new EventStore();
		const failure = new Error("forced RPC open failure");
		sinon.stub(store.log.rpc, "open").rejects(failure);
		const result = await peer.open(store).catch((error: unknown) => error);
		expect(result).to.equal(failure);
		expect(listeners).to.have.length(1);
		expect(listeners[0].signal.aborted).to.equal(true);
		listeners[0].observed.resetHistory();
		pubsub.dispatchEvent(
			new CustomEvent(eventName, {
				detail: peer.identity.publicKey,
			}),
		);
		expect(listeners[0].observed.notCalled).to.equal(true);
	});
});
