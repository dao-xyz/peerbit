import { BlockRequest, BlockResponse } from "@peerbit/blocks";
import { TestSession } from "@peerbit/test-utils";
import { expect } from "chai";
import sinon from "sinon";
import type { SyncProfileEvent } from "../src/index.js";
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
		expect(providerDirectory.callCount).to.equal(2);
		expect(
			providerDirectory.getCalls().map((call) => call.args[0]),
		).to.have.members([
			"cid:provider-resolution-initial",
			`shared-log|${log.topic}`,
		]);
	});

	it("widens a refreshed directory window past eight stale providers", async () => {
		const profileEvents: SyncProfileEvent[] = [];
		session = await TestSession.disconnected(2);
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate: false,
				sync: { profile: (event) => profileEvents.push(event) },
			},
		});
		const log = store.log as any;
		const staleProviders = Array.from(
			{ length: 8 },
			(_, index) => `stale-provider-${index}`,
		);
		const liveProvider = session.peers[1].identity.publicKey.hashcode();
		log.uniqueReplicators.add(liveProvider);
		sinon
			.stub(log, "resolveCandidatePeersForHash")
			.resolves([...staleProviders, liveProvider]);
		sinon.stub(log, "_getLocalReachablePeerHashes").resolves([liveProvider]);
		const freshDirectoryProviders = Array.from(
			{ length: 8 },
			(_, index) => `fresh-directory-provider-${index}`,
		);
		const providerDirectory = sinon
			.stub((session.peers[0].services as any).fanout, "queryProviders")
			.callsFake((...args: unknown[]) => {
				const options = args[1] as { want: number };
				return [...staleProviders, ...freshDirectoryProviders].slice(
					0,
					options.want,
				);
			});

		const providers = await log.remoteBlocks.options.resolveProviders(
			"provider-resolution-refresh",
			{ refresh: true, exclude: staleProviders },
		);

		expect(providerDirectory.callCount).to.equal(2);
		expect(
			providerDirectory.getCalls().map((call) => call.args[1].want),
		).to.deep.equal([16, 16]);
		expect(providers).to.have.length(8);
		expect(providers[0]).to.equal(liveProvider);
		expect(providers).to.include(liveProvider);
		expect(providers).not.to.include(staleProviders[7]);

		const resolution = profileEvents.findLast(
			(event) => event.name === "sharedLog.blocks.resolveProviders",
		)!;
		expect(resolution).to.include({
			component: "shared-log",
			count: 8,
		});
		expect(resolution.peer).to.equal(undefined);
		expect(resolution.details).to.include({
			status: "directory",
			refresh: true,
			excluded: 8,
			lookupPeers: 16,
		});
		for (const name of [
			"sharedLog.open.localState",
			"sharedLog.open.blockStore",
			"sharedLog.open.remoteBlocks",
			"sharedLog.open.lowerLog",
			"sharedLog.open.rpcSubscriptions",
			"sharedLog.open.providerAndOwnership",
			"sharedLog.open.replication",
			"sharedLog.open.synchronizer",
			"sharedLog.open.total",
		]) {
			const event = profileEvents.find((candidate) => candidate.name === name)!;
			expect(event, name).not.to.equal(undefined);
			expect(event.peer, name).to.equal(undefined);
		}

		let replacementProfileCalls = 0;
		log._logProperties.sync.profile = () => {
			replacementProfileCalls += 1;
			throw new Error("diagnostic sink failed");
		};
		const providersWithThrowingProfile =
			await log.remoteBlocks.options.resolveProviders(
				"provider-resolution-profile-replacement",
				{ refresh: true, exclude: staleProviders },
			);
		expect(providersWithThrowingProfile[0]).to.equal(liveProvider);
		expect(replacementProfileCalls).to.equal(1);
	});

	it("widens provider discovery while resolving a missing parent", async function () {
		this.timeout(10_000);
		session = await TestSession.disconnected(2);
		const source = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: false },
		});
		const target = await session.peers[1].open(source.clone(), {
			args: { replicate: false },
		});
		const parent = (
			await source.add("provider-parent", {
				target: "none",
				meta: { next: [] },
			})
		).entry;
		const child = (
			await source.add("provider-child", {
				target: "none",
				meta: { next: [parent] },
			})
		).entry;
		const parentBytes = await (source.log as any).remoteBlocks.localStore.get(
			parent.hash,
		);
		expect(parentBytes).not.to.equal(undefined);

		const sharedLog = target.log as any;
		const remoteBlocks = sharedLog.remoteBlocks;
		const staleProviders = Array.from(
			{ length: 8 },
			(_, index) => `missing-parent-stale-${index}`,
		);
		const liveKey = session.peers[0].identity.publicKey;
		const liveProvider = liveKey.hashcode();
		const candidateLimits: number[] = [];
		sinon
			.stub(sharedLog, "resolveCandidatePeersForHash")
			.callsFake((...args: unknown[]) => {
				candidateLimits.push((args[1] as { maxPeers: number }).maxPeers);
				return staleProviders;
			});
		const queryWants: number[] = [];
		sinon
			.stub((session.peers[1].services as any).fanout, "queryProviders")
			.callsFake((...args: unknown[]) => {
				const options = args[1] as { want: number };
				queryWants.push(options.want);
				return [...staleProviders, liveProvider].slice(0, options.want);
			});
		const requestTargets: string[][] = [];
		let markFirstRequestSeen!: () => void;
		const firstRequestSeen = new Promise<void>(
			(resolve) => (markFirstRequestSeen = resolve),
		);
		let markLiveRequestSeen!: () => void;
		const liveRequestSeen = new Promise<void>(
			(resolve) => (markLiveRequestSeen = resolve),
		);
		sinon
			.stub(remoteBlocks.options, "publish")
			.callsFake(async (message: unknown, options: any) => {
				if (!(message instanceof BlockRequest)) return;
				const targets = [...(options.mode?.to ?? [])] as string[];
				requestTargets.push(targets);
				if (targets.includes(liveProvider)) {
					markLiveRequestSeen();
				} else if (requestTargets.length === 1) {
					markFirstRequestSeen();
				}
			});

		const pendingJoin = target.log.log.join([child], { timeout: 5_000 });
		await Promise.race([
			firstRequestSeen,
			pendingJoin.then(() => {
				throw new Error("join settled before its first provider request");
			}),
		]);
		// Let the first publish retire and install the reachability listener before
		// simulating the newly reachable ninth provider.
		await new Promise((resolve) => setTimeout(resolve, 0));
		remoteBlocks.onReachable(liveKey);
		await Promise.race([
			liveRequestSeen,
			pendingJoin.then(() => {
				throw new Error(
					"join settled before the live ninth provider was requested",
				);
			}),
		]);
		await new Promise((resolve) => setTimeout(resolve, 0));
		await remoteBlocks.onMessage(new BlockResponse(parent.hash, parentBytes), {
			from: liveProvider,
		});
		await pendingJoin;

		expect(await target.log.log.has(parent.hash)).to.equal(true);
		expect(await target.log.log.has(child.hash)).to.equal(true);
		expect(candidateLimits.slice(0, 2)).to.deep.equal([8, 10]);
		expect(queryWants).to.deep.equal([10, 10]);
		expect(requestTargets[0]).to.deep.equal(staleProviders.slice(0, 2));
		expect(
			requestTargets.some((targets) => targets.includes(liveProvider)),
		).to.equal(true);
	});

	it("keeps legacy CID discovery alongside the renewable batch lease", async () => {
		session = await TestSession.disconnected(1);
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: false },
		});
		const log = store.log as any;
		const close = sinon.spy();
		const provide = sinon
			.stub((session.peers[0].services as any).fanout, "provide")
			.returns({ close });
		let announcedNamespaces: string[] = [];
		const announce = sinon
			.stub((session.peers[0].services as any).fanout, "announceProviders")
			.callsFake(async (namespaces: unknown) => {
				announcedNamespaces = [...(namespaces as Iterable<string>)];
			});
		const cids = Array.from({ length: 10 }, (_, index) => `batch-${index}`);

		await log.remoteBlocks.options.onPutMany(cids);

		expect(provide.calledOnce).to.be.true;
		expect(provide.firstCall.args).to.deep.equal([
			`shared-log|${log.topic}`,
			{ ttlMs: 120_000, announceIntervalMs: 60_000 },
		]);
		expect(announce.calledOnce).to.be.true;
		expect(announcedNamespaces).to.deep.equal(cids.map((cid) => `cid:${cid}`));
		expect(announce.firstCall.args[1]).to.deep.equal({
			ttlMs: 120_000,
			bootstrapMaxPeers: 2,
		});
	});

	it("watches both CID and log-wide provider namespaces", async () => {
		session = await TestSession.disconnected(1);
		const store = await session.peers[0].open(new EventStore<string, any>(), {
			args: { replicate: false },
		});
		const log = store.log as any;
		const closes = [sinon.spy(), sinon.spy()];
		const watch = sinon
			.stub((session.peers[0].services as any).fanout, "watchProviders")
			.onFirstCall()
			.returns({ close: closes[0] });
		watch.onSecondCall().returns({ close: closes[1] });

		const handle = log.remoteBlocks.options.watchProviders("watched-cid", {
			onProviders: sinon.spy(),
		});
		expect(watch.callCount).to.equal(2);
		expect(watch.getCalls().map((call) => call.args[0])).to.deep.equal([
			"cid:watched-cid",
			`shared-log|${log.topic}`,
		]);

		handle.close();
		expect(closes[0].calledOnce).to.be.true;
		expect(closes[1].calledOnce).to.be.true;
	});
});
