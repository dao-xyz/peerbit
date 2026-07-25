import { getPublicKeyFromPeerId } from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import {
	TopicRootCandidates,
	TopicRootQueryResponse,
} from "@peerbit/pubsub-interface";
import { waitForNeighbour } from "@peerbit/stream";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	FanoutTree,
	TopicControlPlane,
	TopicRootControlPlane,
} from "../src/index.js";

describe("pubsub (subscribe race regressions)", function () {
	let session:
		| TestSession<{
				pubsub: TopicControlPlane;
				fanout: FanoutTree;
		  }>
		| undefined;

	const createDisconnectedSession = async (
		peerCount: number,
		options?: {
			pubsub?: Partial<ConstructorParameters<typeof TopicControlPlane>[1]>;
		},
	) => {
		const topicRootControlPlane = new TopicRootControlPlane();
		const fanoutByHash = new Map<string, FanoutTree>();
		const getOrCreateFanout = (c: any) => {
			const hash = getPublicKeyFromPeerId(c.peerId).hashcode();
			let fanout = fanoutByHash.get(hash);
			if (!fanout) {
				fanout = new FanoutTree(c, {
					connectionManager: false,
					topicRootControlPlane,
				});
				fanoutByHash.set(hash, fanout);
			}
			return fanout;
		};

		return TestSession.disconnected<{
			pubsub: TopicControlPlane;
			fanout: FanoutTree;
		}>(peerCount, {
			services: {
				fanout: (c: any) => getOrCreateFanout(c),
				pubsub: (c: any) =>
					new TopicControlPlane(c, {
						canRelayMessage: true,
						connectionManager: false,
						topicRootControlPlane,
						fanout: getOrCreateFanout(c),
						shardCount: 16,
						fanoutJoin: {
							timeoutMs: 10_000,
							retryMs: 50,
							bootstrapEnsureIntervalMs: 200,
							trackerQueryIntervalMs: 200,
							joinReqTimeoutMs: 1_000,
							trackerQueryTimeoutMs: 1_000,
						},
						...(options?.pubsub || {}),
					}),
			},
			});
		};

	const createDisconnectedSessionWithPerPeerRoots = async (
		peerCount: number,
		options?: {
			pubsub?: Partial<ConstructorParameters<typeof TopicControlPlane>[1]>;
		},
	) => {
		const perPeer = new Map<
			string,
			{ fanout: FanoutTree; topicRootControlPlane: TopicRootControlPlane }
		>();
		const getOrCreatePerPeer = (c: any) => {
			const hash = getPublicKeyFromPeerId(c.peerId).hashcode();
			let existing = perPeer.get(hash);
			if (!existing) {
				const topicRootControlPlane = new TopicRootControlPlane();
				const fanout = new FanoutTree(c, {
					connectionManager: false,
					topicRootControlPlane,
				});
				existing = { fanout, topicRootControlPlane };
				perPeer.set(hash, existing);
			}
			return existing;
		};

		return TestSession.disconnected<{
			pubsub: TopicControlPlane;
			fanout: FanoutTree;
		}>(peerCount, {
			services: {
				fanout: (c: any) => getOrCreatePerPeer(c).fanout,
				pubsub: (c: any) => {
					const { fanout, topicRootControlPlane } = getOrCreatePerPeer(c);
					return new TopicControlPlane(c, {
						canRelayMessage: true,
						connectionManager: false,
						topicRootControlPlane,
						fanout,
						shardCount: 16,
						fanoutJoin: {
							timeoutMs: 10_000,
							retryMs: 50,
							bootstrapEnsureIntervalMs: 200,
							trackerQueryIntervalMs: 200,
							joinReqTimeoutMs: 1_000,
							trackerQueryTimeoutMs: 1_000,
						},
						...(options?.pubsub || {}),
					});
				},
			},
		});
	};

	const topicHash32 = (topic: string) => {
		let hash = 0x811c9dc5; // FNV-1a
		for (let index = 0; index < topic.length; index++) {
			hash ^= topic.charCodeAt(index);
			hash = (hash * 0x01000193) >>> 0;
		}
		return hash >>> 0;
	};

	const findCandidateTransitionTopic = (
		beforeCandidates: string[],
		afterCandidates: string[],
		expectedBefore?: string,
		expectedAfter?: string,
		maxSearch = 10_000,
	) => {
		const before = new TopicRootControlPlane({
			defaultCandidates: beforeCandidates,
		});
		const after = new TopicRootControlPlane({
			defaultCandidates: afterCandidates,
		});
		for (let index = 0; index < maxSearch; index++) {
			const topic = `/peerbit/pubsub-shard/1/${index}`;
			const beforeRoot = before.resolveDeterministicTopicRoot(topic);
			const afterRoot = after.resolveDeterministicTopicRoot(topic);
			if (
				beforeRoot !== afterRoot &&
				(expectedBefore === undefined || beforeRoot === expectedBefore) &&
				(expectedAfter === undefined || afterRoot === expectedAfter)
			) {
				return { afterRoot: afterRoot!, beforeRoot: beforeRoot!, index, topic };
			}
		}
		throw new Error("Unable to find a topic whose candidate root changes");
	};

	const deferred = () => {
		let resolve!: () => void;
		const promise = new Promise<void>((next) => {
			resolve = next;
		});
		return { promise, resolve };
	};

	afterEach(async () => {
		if (session) {
			await session.stop();
			session = undefined;
		}
	});

	it("discovers peers when subscribe and connect happen concurrently", async () => {
		const TOPIC = "concurrent-subscribe-connect-regression";
		session = await createDisconnectedSession(2);

		const a = session.peers[0]!.services.pubsub;
		const b = session.peers[1]!.services.pubsub;

		await Promise.all([
			a.subscribe(TOPIC),
			b.subscribe(TOPIC),
			session.connect([[session.peers[0], session.peers[1]]]),
		]);
		await waitForNeighbour(a, b);

		await waitForResolved(() => {
			const aTopics = a.topics.get(TOPIC);
			const bTopics = b.topics.get(TOPIC);
			expect(aTopics).to.not.equal(undefined);
			expect(bTopics).to.not.equal(undefined);
			expect(aTopics?.has(b.publicKeyHash)).to.equal(true);
			expect(bTopics?.has(a.publicKeyHash)).to.equal(true);
		});
	});

	it("does not drop reconciliation requested during an in-flight pass", async () => {
		session = await createDisconnectedSession(1);
		const pubsub = session.peers[0]!.services.pubsub as any;
		const firstGate = deferred();
		const firstEntered = deferred();
		let calls = 0;
		pubsub.reconcileShardOverlays = async () => {
			calls += 1;
			if (calls === 1) {
				firstEntered.resolve();
				await firstGate.promise;
			}
		};

		pubsub.scheduleReconcileShardOverlays();
		await firstEntered.promise;
		pubsub.scheduleReconcileShardOverlays();
		firstGate.resolve();

		await waitForResolved(() => expect(calls).to.equal(2), {
			timeout: 1_000,
			delayInterval: 10,
		});
	});

	it("rejects a stale delayed-retry root after auto candidates change", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const staleRoot = pubsub.publicKeyHash;
		const addedCandidate = "root-candidate-added-during-query";
		const { afterRoot: currentRoot, topic } = findCandidateTransitionTopic(
			[staleRoot],
			[staleRoot, addedCandidate],
		);

		const queryGate = deferred();
		const queryEntered = deferred();
		internals.scheduleReconcileShardOverlays = () => {};
		internals.scheduleHostOwnedShardRoots = () => {};
		internals.scheduleAutoTopicRootCandidatesBroadcast = () => {};
		internals.getConnectedTopicRootTrackers = () => [{}];
		let queryCalls = 0;
		internals.resolveTopicRootThroughPeers = async () => {
			queryCalls += 1;
			if (queryCalls === 1) return undefined;
			queryEntered.resolve();
			await queryGate.promise;
			return staleRoot;
		};

		const resolving = internals.resolveTopicRootState(topic);
		await queryEntered.promise;
		expect(
			internals.mergeAutoTopicRootCandidatesFromPeer([addedCandidate]),
		).to.equal(true);
		queryGate.resolve();

		expect(await resolving).to.deep.equal({
			authoritative: false,
			root: currentRoot,
		});
	});

	it("does not reinsert a stale shard root after candidates clear the cache", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const staleRoot = pubsub.publicKeyHash;
		const addedCandidate = "root-candidate-added-before-cache-write";
		const { afterRoot: currentRoot, topic } = findCandidateTransitionTopic(
			[staleRoot],
			[staleRoot, addedCandidate],
		);

		internals.scheduleReconcileShardOverlays = () => {};
		internals.scheduleHostOwnedShardRoots = () => {};
		internals.scheduleAutoTopicRootCandidatesBroadcast = () => {};
		let queryCalls = 0;
		internals.resolveTopicRootThroughPeers = async () => {
			queryCalls += 1;
			return queryCalls === 1 ? staleRoot : currentRoot;
		};

		const originalResolveTopicRootState =
			internals.resolveTopicRootState.bind(pubsub);
		const firstStateGate = deferred();
		const firstStateReady = deferred();
		let stateCalls = 0;
		internals.resolveTopicRootState = async (...args: any[]) => {
			const state = await originalResolveTopicRootState(...args);
			stateCalls += 1;
			if (stateCalls === 1) {
				firstStateReady.resolve();
				await firstStateGate.promise;
			}
			return state;
		};

		const resolving = internals
			.resolveShardRootState(topic)
			.then((state: { root: string }) => state.root);
		await firstStateReady.promise;
		expect(
			internals.mergeAutoTopicRootCandidatesFromPeer([addedCandidate]),
		).to.equal(true);
		firstStateGate.resolve();

		expect(await resolving).to.equal(currentRoot);
		expect(internals.shardRootCache.get(topic)?.root).to.equal(currentRoot);
		expect(stateCalls).to.equal(2);
	});

	it("does not let an old opener block or overwrite the current generation", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const fanout = session.peers[0]!.services.fanout as any;
		const oldRoot = "stale-generation-root";
		const addedCandidate = "new-generation-candidate";

		internals.scheduleReconcileShardOverlays = () => {};
		internals.scheduleHostOwnedShardRoots = () => {};
		internals.scheduleAutoTopicRootCandidatesBroadcast = () => {};
		expect(internals.mergeAutoTopicRootCandidatesFromPeer([oldRoot])).to.equal(
			true,
		);
		const beforeCandidates =
			pubsub.topicRootControlPlane.getTopicRootCandidates();
		const afterCandidates = [...beforeCandidates, addedCandidate];
		const { topic } = findCandidateTransitionTopic(
			beforeCandidates,
			afterCandidates,
			oldRoot,
			pubsub.publicKeyHash,
		);
		const oldGeneration = internals.getTopicRootCandidateGeneration();

		const confirmationGate = deferred();
		const confirmationEntered = deferred();
		internals.peers.set(oldRoot, {
			isReadable: false,
			isWritable: false,
		});
		internals.queryTopicRootFromPeer = async () => {
			confirmationEntered.resolve();
			await confirmationGate.promise;
			return oldRoot;
		};

		const channelEventTypes = new Set([
			"fanout:data",
			"fanout:unicast",
			"fanout:joined",
			"fanout:kicked",
		]);
		const originalAddEventListener = fanout.addEventListener.bind(fanout);
		let channelListenerAdds = 0;
		fanout.addEventListener = (...args: any[]) => {
			if (channelEventTypes.has(args[0])) channelListenerAdds += 1;
			return originalAddEventListener(...args);
		};

		const resolutionGate = deferred();
		const resolutionEntered = deferred();
		let resolutionCalls = 0;

		const oldOpening = internals.ensureFanoutChannel(topic, {
			root: oldRoot,
			rootCandidateGeneration: oldGeneration,
		});
		try {
			await confirmationEntered.promise;
			expect(
				internals.mergeAutoTopicRootCandidatesFromPeer([addedCandidate]),
			).to.equal(true);
			const currentGeneration = internals.getTopicRootCandidateGeneration();
			internals.resolveShardRootState = async () => {
				resolutionCalls += 1;
				resolutionEntered.resolve();
				await resolutionGate.promise;
				return {
					root: pubsub.publicKeyHash,
					candidateGeneration: currentGeneration,
				};
			};

			const currentOpening = internals.ensureFanoutChannel(topic);
			await resolutionEntered.promise;
			resolutionGate.resolve();
			await currentOpening;
			const currentChannel = internals.fanoutChannels.get(topic);
			expect(currentChannel?.root).to.equal(pubsub.publicKeyHash);

			confirmationGate.resolve();
			await oldOpening;
			expect(internals.fanoutChannels.get(topic)).to.equal(currentChannel);
		} finally {
			confirmationGate.resolve();
			resolutionGate.resolve();
			fanout.addEventListener = originalAddEventListener;
			internals.peers.delete(oldRoot);
		}

		expect(resolutionCalls).to.equal(1);
		expect(internals.fanoutChannels.size).to.equal(1);
		expect(internals.fanoutChannels.get(topic)?.root).to.equal(
			pubsub.publicKeyHash,
		);
		expect(channelListenerAdds).to.equal(4);
	});

	it("serializes concurrent opens for the same shard channel", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const fanout = session.peers[0]!.services.fanout as any;
		const shardTopic = "/peerbit/pubsub-shard/1/concurrent-open";
		const channelEventTypes = new Set([
			"fanout:data",
			"fanout:unicast",
			"fanout:joined",
			"fanout:kicked",
		]);
		const originalAddEventListener = fanout.addEventListener.bind(fanout);
		let channelListenerAdds = 0;
		fanout.addEventListener = (...args: any[]) => {
			if (channelEventTypes.has(args[0])) channelListenerAdds += 1;
			return originalAddEventListener(...args);
		};

		try {
			await Promise.all([
				internals.ensureFanoutChannel(shardTopic),
				internals.ensureFanoutChannel(shardTopic),
			]);
		} finally {
			fanout.addEventListener = originalAddEventListener;
		}

		expect(internals.fanoutChannels.size).to.equal(1);
		expect(channelListenerAdds).to.equal(4);
	});

	it("ignores an in-flight opener from an older lifecycle", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const shardTopic = "/peerbit/pubsub-shard/1/old-lifecycle-opener";
		const candidateGeneration = internals.getTopicRootCandidateGeneration();
		const lifecycleRevision = internals.topicControlPlaneLifecycleRevision;
		internals.ensureFanoutChannelInFlight.set(shardTopic, {
			candidateGeneration,
			lifecycleRevision: lifecycleRevision - 1,
			opening: new Promise<void>(() => {}),
		});

		let settled = false;
		await Promise.race([
			internals.ensureFanoutChannel(shardTopic).then(() => {
				settled = true;
			}),
			delay(500),
		]);

		expect(settled).to.equal(true);
		expect(internals.fanoutChannels.get(shardTopic)?.root).to.equal(
			pubsub.publicKeyHash,
		);
	});

	it("keeps the lifecycle revision stable across an idempotent start", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const lifecycleRevision = internals.topicControlPlaneLifecycleRevision;

		await pubsub.start();

		expect(internals.topicControlPlaneLifecycleRevision).to.equal(
			lifecycleRevision,
		);
	});

	it("does not reinterpret a stale supplied root after leaving auto mode", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const shardTopic = "/peerbit/pubsub-shard/1/stale-supplied-root";
		const staleRoot = pubsub.publicKeyHash;
		const staleGeneration = internals.getTopicRootCandidateGeneration();
		const currentRoot = "explicit-root-after-auto-mode";

		internals.scheduleReconcileShardOverlays = () => {};
		internals.scheduleHostOwnedShardRoots = () => {};
		pubsub.setTopicRootCandidates([currentRoot]);
		const currentGeneration = internals.getTopicRootCandidateGeneration();
		let resolutionCalls = 0;
		internals.resolveShardRootState = async () => {
			resolutionCalls += 1;
			return {
				root: currentRoot,
				candidateGeneration: currentGeneration,
			};
		};
		let confirmationCalls = 0;
		internals.confirmDirectShardRoot = async () => {
			confirmationCalls += 1;
			return currentRoot;
		};

		await internals.ensureFanoutChannel(shardTopic, {
			root: staleRoot,
			rootCandidateGeneration: staleGeneration,
			pin: true,
		});

		expect(resolutionCalls).to.equal(0);
		expect(confirmationCalls).to.equal(0);
		expect(internals.fanoutChannels.has(shardTopic)).to.equal(false);
		expect(internals.pinnedShards.has(shardTopic)).to.equal(false);
	});

	it("rejects a mismatched internal shard root while in auto mode", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const autoPeer = "auto-peer-with-stale-shard-root";
		expect(internals.mergeAutoTopicRootCandidatesFromPeer([autoPeer])).to.equal(
			true,
		);
		const candidates = pubsub.topicRootControlPlane.getTopicRootCandidates();
		const { afterRoot: deterministicRoot, topic: shardTopic } =
			findCandidateTransitionTopic(
				[autoPeer],
				candidates,
				autoPeer,
				pubsub.publicKeyHash,
				1_000,
			);
		expect(
			internals.normalizePeerTopicRootState(
				shardTopic,
				autoPeer,
			),
		).to.deep.equal({
			authoritative: false,
			root: deterministicRoot,
		});
	});

	it("keeps a locally configured shard root authoritative in auto mode", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const shardTopic = "/peerbit/pubsub-shard/1/local-explicit-root";
		const configuredRoot = "locally-configured-root";
		pubsub.topicRootControlPlane.setTopicRoot(shardTopic, configuredRoot);

		expect(internals.autoTopicRootCandidates).to.equal(true);
		expect(await internals.resolveTopicRootState(shardTopic)).to.deep.equal({
			authoritative: true,
			root: configuredRoot,
		});
	});

	it("ignores a topic-root response from a peer that was not queried", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const internals = session.peers[0]!.services.pubsub as any;
		const requestId = 42;
		const topic = "/peerbit/pubsub-shard/1/query-origin";
		let resolved = false;
		const timer = setTimeout(() => {}, 10_000);
		timer.unref?.();
		internals.pendingTopicRootQueries.set(requestId, {
			expectedPeerHash: "expected-peer",
			topic,
			resolve: () => {
				resolved = true;
			},
			timer,
		});
		const response = new TopicRootQueryResponse({
			requestId,
			root: "claimed-root",
			topic,
		});

		expect(
			internals.resolvePendingTopicRootQuery(response, "different-peer"),
		).to.equal(false);
		expect(resolved).to.equal(false);
		expect(internals.pendingTopicRootQueries.has(requestId)).to.equal(true);
		expect(
			internals.resolvePendingTopicRootQuery(response, "expected-peer"),
		).to.equal(true);
		expect(resolved).to.equal(true);
		expect(internals.pendingTopicRootQueries.has(requestId)).to.equal(false);
	});

	it("ignores direct root control signed by a different peer", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(3);
		const receiver = session.peers[0]!.services.pubsub;
		const directPeer = session.peers[1]!.services.pubsub as any;
		const foreignSigner = session.peers[2]!.services.pubsub as any;
		const internals = receiver as any;
		const candidatesBefore =
			receiver.topicRootControlPlane.getTopicRootCandidates();

		await internals.processDirectPubSubMessage({
			pubsubMessage: new TopicRootCandidates({
				candidates: ["forged-auto-root-candidate"],
			}),
			message: {
				header: {
					signatures: { publicKeys: [foreignSigner.publicKey] },
				},
			},
			from: directPeer.publicKey,
			stream: { publicKey: directPeer.publicKey },
		});

		expect(receiver.topicRootControlPlane.getTopicRootCandidates()).to.deep.equal(
			candidatesBefore,
		);
	});

	it("does not install a channel after the control plane stops", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const shardTopic = "/peerbit/pubsub-shard/1/stop-during-resolution";
		const candidateGeneration = internals.getTopicRootCandidateGeneration();
		const resolutionGate = deferred();
		const resolutionEntered = deferred();
		internals.resolveShardRootState = async () => {
			resolutionEntered.resolve();
			await resolutionGate.promise;
			return { root: pubsub.publicKeyHash, candidateGeneration };
		};

		const opening = internals.ensureFanoutChannel(shardTopic);
		await resolutionEntered.promise;
		await pubsub.stop();
		resolutionGate.resolve();
		await opening.catch(() => {});
		await delay(0);

		expect(internals.fanoutChannels.size).to.equal(0);
		expect(internals.ensureFanoutChannelInFlight.size).to.equal(0);
	});

	it("does not track a topic on a peer that never subscribed", async () => {
		const TOPIC = "non-subscriber-should-not-track-regression";
		session = await createDisconnectedSession(2);

		const a = session.peers[0]!.services.pubsub;
		const b = session.peers[1]!.services.pubsub;

		await session.connect([[session.peers[0], session.peers[1]]]);
		await waitForNeighbour(a, b);

		await b.subscribe(TOPIC);
		await waitForResolved(() => {
			expect(b.subscriptions.has(TOPIC)).to.equal(true);
			const bSubscribers = b.getSubscribers(TOPIC);
			expect(
				bSubscribers?.some(
					(subscriber) => subscriber.hashcode() === b.publicKeyHash,
				),
			).to.equal(true);
		});

		expect(a.topics.has(TOPIC)).to.equal(false);
		expect(a.topics.get(TOPIC)).to.equal(undefined);
	});

	it("does not advertise cancelled pending subscriptions to peers", async () => {
		const TOPIC = "subscribe-then-unsubscribe-before-debounce-regression";
		const debounceDelayMs = 500;
		session = await createDisconnectedSession(2, {
			pubsub: {
				subscriptionDebounceDelay: debounceDelayMs,
			},
		});

		const a = session.peers[0]!.services.pubsub;
		const b = session.peers[1]!.services.pubsub;

		await session.connect([[session.peers[0], session.peers[1]]]);
		await waitForNeighbour(a, b);

		const pendingSubscribe = a.subscribe(TOPIC);
		const removed = await a.unsubscribe(TOPIC);
		expect(removed).to.equal(false);

		await b.subscribe(TOPIC);

		// Wait for A's debounced subscribe cycle to settle before asserting.
		// This validates that A does not get (stale) advertised at flush time.
		await pendingSubscribe;
		await delay(debounceDelayMs + 100);

		expect(a.topics.has(TOPIC)).to.equal(false);
		const bTopics = b.topics.get(TOPIC);
		expect(bTopics).to.not.equal(undefined);
		expect(bTopics!.has(a.publicKeyHash)).to.equal(false);
	});

	it("converges a retained auto-root overlay after a late peer joins a star", async function () {
		this.timeout(120_000);
		const shardCount = 64;
		session = await createDisconnectedSessionWithPerPeerRoots(3, {
			pubsub: { shardCount },
		});
		const ordered = session.peers
			.map((node) => ({ node, pubsub: node.services.pubsub }))
			.sort((left, right) =>
				left.pubsub.publicKeyHash.localeCompare(right.pubsub.publicKeyHash),
			);
		const retained = ordered[0]!;
		const source = ordered[1]!;
		const late = ordered[2]!;
		const beforeCandidates = [
			retained.pubsub.publicKeyHash,
			source.pubsub.publicKeyHash,
		].sort();
		const afterCandidates = ordered
			.map(({ pubsub }) => pubsub.publicKeyHash)
			.sort();

		await source.node.dial(retained.node.getMultiaddrs()[0]!);
		await waitForNeighbour(source.pubsub, retained.pubsub);
		await waitForResolved(
			() => {
				expect(
					source.pubsub.topicRootControlPlane.getTopicRootCandidates(),
				).to.deep.equal(beforeCandidates);
				expect(
					retained.pubsub.topicRootControlPlane.getTopicRootCandidates(),
				).to.deep.equal(beforeCandidates);
			},
			{ timeout: 20_000, delayInterval: 50 },
		);

		const {
			beforeRoot,
			index: shardIndex,
			topic: shardTopic,
		} = findCandidateTransitionTopic(
			beforeCandidates,
			afterCandidates,
			undefined,
			late.pubsub.publicKeyHash,
			shardCount,
		);
		let topic = "";
		for (let index = 0; index < 10_000; index++) {
			const candidate = `retained-auto-root-star-${index}`;
			if (topicHash32(candidate) % shardCount === shardIndex) {
				topic = candidate;
				break;
			}
		}
		expect(topic).to.not.equal("");
		await Promise.all([
			source.pubsub.subscribe(topic),
			retained.pubsub.subscribe(topic),
		]);
		await waitForResolved(
			() => {
				expect(
					(source.pubsub as any).fanoutChannels.get(shardTopic)?.root,
				).to.equal(beforeRoot);
				expect(
					(retained.pubsub as any).fanoutChannels.get(shardTopic)?.root,
				).to.equal(beforeRoot);
			},
			{ timeout: 20_000, delayInterval: 50 },
		);
		await late.pubsub.subscribe(topic);
		expect((late.pubsub as any).fanoutChannels.get(shardTopic)?.root).to.equal(
			late.pubsub.publicKeyHash,
		);

		await late.node.dial(source.node.getMultiaddrs()[0]!);
		await waitForNeighbour(late.pubsub, source.pubsub);
		await waitForResolved(
			() => {
				for (const { pubsub } of ordered) {
					expect(
						pubsub.topicRootControlPlane.getTopicRootCandidates(),
					).to.deep.equal(afterCandidates);
				}
			},
			{ timeout: 20_000, delayInterval: 50 },
		);
		await waitForResolved(
			() => {
				for (const { pubsub } of ordered) {
					expect((pubsub as any).fanoutChannels.get(shardTopic)?.root).to.equal(
						late.pubsub.publicKeyHash,
					);
					const subscribers = pubsub.getSubscribers(topic);
					expect(
						subscribers?.map((key) => key.hashcode()).sort(),
					).to.deep.equal(afterCandidates);
				}
			},
			{ timeout: 30_000, delayInterval: 100 },
		);

		let delivered = false;
		const onData = (event: any) => {
			if (event.detail?.data?.topics?.includes?.(topic)) {
				delivered = true;
			}
		};
		late.pubsub.addEventListener("data", onData);
		try {
			await source.pubsub.publish(new Uint8Array([1, 2, 3, 4]), {
				topics: [topic],
			});
			await waitForResolved(() => expect(delivered).to.equal(true), {
				timeout: 10_000,
				delayInterval: 25,
			});
		} finally {
			late.pubsub.removeEventListener("data", onData);
		}
	});

	it("converges sparse relay topology without forced shard-root candidates", async function () {
		this.timeout(120_000);

		const TOPIC = "sparse-relay-root-candidate-convergence";
		session = await createDisconnectedSessionWithPerPeerRoots(3);

		const a = session.peers[0]!.services.pubsub;
		const b = session.peers[1]!.services.pubsub;
		const relay = session.peers[2]!.services.pubsub;

		await session.peers[0]!.dial(session.peers[2]!.getMultiaddrs()[0]!);
		await session.peers[1]!.dial(session.peers[2]!.getMultiaddrs()[0]!);

		await waitForNeighbour(a, relay);
		await waitForNeighbour(b, relay);

		const shardTopic = `/peerbit/pubsub-shard/1/${topicHash32(TOPIC) % 16}`;
		await waitForResolved(
			async () => {
				const candidateLists = [a, b, relay].map((peer) =>
					peer.topicRootControlPlane.getTopicRootCandidates().join(","),
				);
				expect(
					new Set(candidateLists).size,
					`candidate lists: ${candidateLists.join(" | ")}`,
				).to.equal(1);

				const roots = await Promise.all(
					[a, b, relay].map((peer) =>
						peer.topicRootControlPlane.resolveTopicRoot(shardTopic),
					),
				);
				expect(new Set(roots).size, `roots: ${roots.join(",")}`).to.equal(1);
			},
			{ timeout: 20_000, delayInterval: 100 },
		);

		const resolvedRoot =
			await a.topicRootControlPlane.resolveTopicRoot(shardTopic);
		expect(resolvedRoot).to.be.a("string");
		const rootPeer = [a, b, relay].find(
			(peer) => peer.publicKeyHash === resolvedRoot,
		);
		expect(rootPeer, `resolved root ${resolvedRoot}`).to.exist;

		await waitForResolved(
			() => {
				const rootChannel = (rootPeer as any).fanoutChannels?.get?.(shardTopic);
				expect(
					rootChannel,
					`expected root ${resolvedRoot} to host ${shardTopic}`,
				).to.exist;
				expect(rootChannel.root).to.equal(resolvedRoot);
			},
			{ timeout: 20_000, delayInterval: 100 },
		);
	});

	it("resolves shard roots through a gateway after auto mode is disabled", async function () {
		this.timeout(120_000);

		const TOPIC = "dial-gateway-root-tracker-discovery";
		session = await createDisconnectedSessionWithPerPeerRoots(3);

		const a = session.peers[0]!.services.pubsub;
		const gateway = session.peers[1]!.services.pubsub;
		const root = session.peers[2]!.services.pubsub;

		await session.peers[0]!.dial(session.peers[1]!.getMultiaddrs()[0]!);
		await session.peers[1]!.dial(session.peers[2]!.getMultiaddrs()[0]!);

		await waitForNeighbour(a, gateway);
		await waitForNeighbour(gateway, root);

		const shardTopic = `/peerbit/pubsub-shard/1/${topicHash32(TOPIC) % 16}`;
		a.setTopicRootCandidates([]);
		gateway.setTopicRootCandidates([root.publicKeyHash]);
		root.setTopicRootCandidates([root.publicKeyHash]);
		expect((a as any).autoTopicRootCandidates).to.equal(false);

		const resolvedRoot = (await (a as any).resolveShardRootState(shardTopic))
			.root;
		expect(resolvedRoot).to.equal(root.publicKeyHash);
	});

	it("trusts a stable explicit gateway root while the leaf remains in auto mode", async function () {
		this.timeout(120_000);

		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const leaf = session.peers[0]!.services.pubsub;
		const gateway = session.peers[1]!.services.pubsub;

		await session.peers[0]!.dial(session.peers[1]!.getMultiaddrs()[0]!);
		await waitForNeighbour(leaf, gateway);
		await waitForResolved(
			() => {
				const leafCandidates =
					leaf.topicRootControlPlane.getTopicRootCandidates();
				const gatewayCandidates =
					gateway.topicRootControlPlane.getTopicRootCandidates();
				expect(leafCandidates).to.deep.equal(gatewayCandidates);
				expect(leafCandidates).to.have.length(2);
			},
			{ timeout: 20_000, delayInterval: 50 },
		);

		let topicIndex = 0;
		let topic = `auto-leaf-explicit-gateway-${topicIndex}`;
		while (
			leaf.topicRootControlPlane.resolveDeterministicTopicRoot(topic) ===
			gateway.publicKeyHash
		) {
			topicIndex += 1;
			topic = `auto-leaf-explicit-gateway-${topicIndex}`;
		}
		gateway.topicRootControlPlane.setTopicRoot(topic, gateway.publicKeyHash);

		expect(await leaf.resolveTopicRoot(topic)).to.equal(gateway.publicKeyHash);
	});

	it("confirms a gateway-resolved shard with the direct root before joining", async function () {
		this.timeout(120_000);

		const TOPIC = "gateway-root-host-confirmation";
		session = await createDisconnectedSessionWithPerPeerRoots(3);

		const peers = session.peers
			.map((node) => ({ node, pubsub: node.services.pubsub }))
			.sort((a, b) =>
				a.pubsub.publicKeyHash < b.pubsub.publicKeyHash
					? -1
					: a.pubsub.publicKeyHash > b.pubsub.publicKeyHash
						? 1
						: 0,
			);
		const gateway = peers[0]!;
		const root = peers[1]!;
		const leaf = peers[2]!;

		await leaf.node.dial(gateway.node.getMultiaddrs()[0]!);
		await leaf.node.dial(root.node.getMultiaddrs()[0]!);
		await waitForNeighbour(leaf.pubsub, gateway.pubsub);
		await waitForNeighbour(leaf.pubsub, root.pubsub);
		await Promise.all(
			peers.map(
				({ pubsub }) =>
					(pubsub as any).hostOwnedShardRootsInFlight ?? Promise.resolve(),
			),
		);

		const shardTopic = `/peerbit/pubsub-shard/1/${topicHash32(TOPIC) % 16}`;
		leaf.pubsub.setTopicRootCandidates([]);
		gateway.pubsub.setTopicRootCandidates([root.pubsub.publicKeyHash]);
		root.pubsub.setTopicRootCandidates([root.pubsub.publicKeyHash]);
		await root.pubsub.hostShardRootsNow();
		await ((root.pubsub as any).hostOwnedShardRootsInFlight ??
			Promise.resolve());
		await (root.pubsub as any).closeFanoutChannel(shardTopic, { force: true });
		await delay(100);
		expect((root.pubsub as any).fanoutChannels.get(shardTopic)).to.equal(
			undefined,
		);
		expect(
			root.node.services.fanout.getChannelStats(
				shardTopic,
				root.pubsub.publicKeyHash,
			),
		).to.equal(undefined);

		const leafInternals = leaf.pubsub as any;
		leafInternals.shardRootCache.clear();
		expect([...leafInternals.peers.keys()]).to.include(
			gateway.pubsub.publicKeyHash,
		);
		expect([...leafInternals.peers.keys()]).to.include(
			root.pubsub.publicKeyHash,
		);
		const originalQueryTopicRootFromPeer = leafInternals.queryTopicRootFromPeer;
		const queriedPeers: string[] = [];
		leafInternals.queryTopicRootFromPeer = async (...args: any[]) => {
			queriedPeers.push(args[0].publicKey.hashcode());
			return originalQueryTopicRootFromPeer.apply(leaf.pubsub, args);
		};

		try {
			await leaf.pubsub.subscribe(TOPIC);
		} finally {
			leafInternals.queryTopicRootFromPeer = originalQueryTopicRootFromPeer;
		}

		expect(queriedPeers.slice(0, 2)).to.deep.equal([
			gateway.pubsub.publicKeyHash,
			root.pubsub.publicKeyHash,
		]);
		expect((root.pubsub as any).fanoutChannels.get(shardTopic)?.root).to.equal(
			root.pubsub.publicKeyHash,
		);
		expect(
			leaf.node.services.fanout.getChannelStats(
				shardTopic,
				root.pubsub.publicKeyHash,
			)?.parent,
		).to.equal(root.pubsub.publicKeyHash);
	});

	it("hosts a shard before answering a direct root query for itself", async function () {
		this.timeout(120_000);

		const TOPIC = "dial-query-opens-missing-shard-root";
		session = await createDisconnectedSessionWithPerPeerRoots(2);

		const leaf = session.peers[0]!.services.pubsub;
		const root = session.peers[1]!.services.pubsub;

		await session.peers[0]!.dial(session.peers[1]!.getMultiaddrs()[0]!);
		await waitForNeighbour(leaf, root);

		const shardTopic = `/peerbit/pubsub-shard/1/${topicHash32(TOPIC) % 16}`;
		leaf.setTopicRootCandidates([]);
		root.setTopicRootCandidates([root.publicKeyHash]);

		await waitForResolved(
			() => {
				expect((root as any).fanoutChannels.get(shardTopic)).to.exist;
			},
			{ timeout: 20_000, delayInterval: 100 },
		);
		await (root as any).closeFanoutChannel(shardTopic, { force: true });
		expect((root as any).fanoutChannels.get(shardTopic)).to.not.exist;

		const resolvedRoot = (await (leaf as any).resolveShardRootState(shardTopic))
			.root;
		expect(resolvedRoot).to.equal(root.publicKeyHash);

		await waitForResolved(
			() => {
				const rootChannel = (root as any).fanoutChannels.get(shardTopic);
				expect(rootChannel, `expected root to host ${shardTopic}`).to.exist;
				expect(rootChannel.root).to.equal(root.publicKeyHash);
			},
			{ timeout: 20_000, delayInterval: 100 },
		);

		await leaf.subscribe(TOPIC);
	});
});
