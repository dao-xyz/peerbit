import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import { EventStore } from "./utils/stores/event-store.js";

describe("join provider resolution", () => {
	let session: TestSession;

	afterEach(async () => {
		sinon.restore();
		await session.stop();
	});

	it("returns a live local candidate without awaiting tracker discovery", async () => {
		session = await TestSession.disconnected(2);
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: false },
		});
		const log = store.log as any;
		const liveKey = session.peers[1].identity.publicKey;
		const liveHash = liveKey.hashcode();
		const staleReplicators = Array.from(
			{ length: 8 },
			(_, index) => `stale-replicator-${index}`,
		);
		for (const peerHash of staleReplicators) {
			log.uniqueReplicators.add(peerHash);
		}
		log.uniqueReplicators.add(liveHash);
		const pubsub = session.peers[0].services.pubsub as any;
		const subscribers = sinon
			.stub(pubsub, "getSubscribers")
			.resolves([liveKey]);
		const broadDiscovery = sinon
			.stub(log, "_getTopicSubscribers")
			.returns(new Promise(() => {}));
		const providerDirectory = sinon
			.stub((session.peers[0].services as any).fanout, "queryProviders")
			.returns(new Promise(() => {}));
		let stallTimer: ReturnType<typeof setTimeout> | undefined;

		try {
			const providers = await Promise.race([
				log.remoteBlocks.options.resolveProviders(
					"provider-resolution-saturation",
				),
				new Promise<never>((_, reject) => {
					stallTimer = setTimeout(
						() => reject(new Error("local provider lookup stalled")),
						500,
					);
				}),
			]);
			expect(providers).to.have.length(8);
			expect(providers[0]).to.equal(liveHash);
			expect(providers).to.include(liveHash);
			expect(subscribers.calledOnce).to.be.true;
			expect(broadDiscovery.notCalled).to.be.true;
			expect(providerDirectory.notCalled).to.be.true;
		} finally {
			if (stallTimer) clearTimeout(stallTimer);
			for (const peerHash of staleReplicators) {
				log.uniqueReplicators.delete(peerHash);
			}
			log.uniqueReplicators.delete(liveHash);
		}
	});

	it("coalesces concurrent local reachability snapshots", async () => {
		session = await TestSession.disconnected(2);
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: false },
		});
		const log = store.log as any;
		const liveKey = session.peers[1].identity.publicKey;
		let releaseSubscribers: (keys: (typeof liveKey)[]) => void;
		const subscriberSnapshot = new Promise<(typeof liveKey)[]>((resolve) => {
			releaseSubscribers = resolve;
		});
		const subscribers = sinon
			.stub(session.peers[0].services.pubsub as any, "getSubscribers")
			.returns(subscriberSnapshot);

		const first = log._getLocalReachablePeerHashes(log.topic);
		const second = log._getLocalReachablePeerHashes(log.topic);
		expect(subscribers.calledOnce).to.be.true;
		releaseSubscribers!([liveKey]);
		const [firstHashes, secondHashes] = await Promise.all([first, second]);
		expect(firstHashes).to.deep.equal([liveKey.hashcode()]);
		expect(secondHashes).to.deep.equal(firstHashes);
	});

	it("invalidates a warm reachability snapshot before waking block reads", async () => {
		session = await TestSession.disconnected(2);
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: false },
		});
		const log = store.log as any;
		const remote = session.peers[1].identity.publicKey;
		const remoteHash = remote.hashcode();
		const subscribers = sinon.stub(
			session.peers[0].services.pubsub as any,
			"getSubscribers",
		);
		subscribers.onFirstCall().returns([]);
		subscribers.onSecondCall().returns([remote]);
		expect(await log._getLocalReachablePeerHashes(log.topic)).to.deep.equal([]);

		let snapshotDuringWake: Promise<string[]> | undefined;
		sinon.stub(log.remoteBlocks, "onReachable").callsFake(() => {
			snapshotDuringWake = log._getLocalReachablePeerHashes(log.topic);
		});
		sinon.stub(log, "handleSubscriptionChange").resolves();

		await log._onSubscription({
			detail: { from: remote, topics: [log.topic] },
		} as any);

		expect(await snapshotDuringWake).to.deep.equal([remoteHash]);
		expect(subscribers.calledTwice).to.be.true;
	});

	it("does not let an arbitrary live fallback suppress CID discovery", async () => {
		session = await TestSession.disconnected(2);
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: false },
		});
		const log = store.log as any;
		const liveKey = session.peers[1].identity.publicKey;
		const liveHash = liveKey.hashcode();
		const directoryProviders = Array.from(
			{ length: 8 },
			(_, index) => `directory-provider-${index}`,
		);
		sinon
			.stub(session.peers[0].services.pubsub as any, "getSubscribers")
			.resolves([liveKey]);
		const providerDirectory = sinon
			.stub((session.peers[0].services as any).fanout, "queryProviders")
			.resolves(directoryProviders);

		const providers = await log.remoteBlocks.options.resolveProviders(
			"provider-resolution-initial",
		);
		expect(providers).to.have.length(8);
		expect(providers[0]).to.equal(liveHash);
		expect(providers).to.include(directoryProviders[0]);
		expect(providerDirectory.calledOnce).to.be.true;
	});
});
