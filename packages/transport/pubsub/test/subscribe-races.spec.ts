import { field, variant, vec } from "@dao-xyz/borsh";
import { getPublicKeyFromPeerId, sha256Base64Sync } from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import {
	TOPIC_ROOT_CANDIDATES_MAX,
	TopicRootCandidateClaims,
	TopicRootCandidates,
	TopicRootQuery,
	TopicRootQueryResponse,
} from "@peerbit/pubsub-interface";
import { waitForNeighbour } from "@peerbit/stream";
import {
	AnyWhere,
	DataMessage,
	type DeliveryMode,
	MessageHeader,
} from "@peerbit/stream-interface";
import { AbortError, delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { equals as bytesEqual } from "uint8arrays";
import {
	FanoutChannel,
	FanoutTree,
	TopicControlPlane,
	TopicRootControlPlane,
} from "../src/index.js";

abstract class ForeignDeliveryMode {}

@variant(0)
class ForeignSilentDelivery extends ForeignDeliveryMode {
	@field({ type: vec("string") })
	to: string[];

	@field({ type: "u8" })
	redundancy: number;

	constructor(to: string[], redundancy: number) {
		super();
		this.to = to;
		this.redundancy = redundancy;
	}
}

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

	const canonicalCandidate = (label: string) =>
		sha256Base64Sync(new TextEncoder().encode(label));
	const dataMessageBytes = (message: DataMessage) => {
		const bytes = message.bytes();
		return bytes instanceof Uint8Array ? bytes : bytes.subarray();
	};
	type CandidateClaimOptions = {
		data?: Uint8Array;
		expiresAt?: number;
		expiresInMs?: number;
		extraSigners?: any[];
	};
	const createCandidateClaim = (
		origin: TopicControlPlane,
		options?: CandidateClaimOptions,
	): Promise<DataMessage> => {
		const internals = origin as any;
		return internals.createMessage(
			options?.data ?? internals.topicRootCandidateClaimData,
			{
				...(options?.expiresAt != null
					? { expiresAt: options.expiresAt }
					: { expiresInMs: options?.expiresInMs ?? 90_000 }),
				extraSigners: options?.extraSigners,
				mode: new AnyWhere(),
				priority: 1,
				skipRecipientValidation: true,
			},
		);
	};
	const processDirectRootControl = (
		receiver: TopicControlPlane,
		pubsubMessage: TopicRootCandidateClaims | TopicRootCandidates,
		directPeer: TopicControlPlane,
		protocol: string,
	) => {
		const tracked = receiver.peers.get(directPeer.publicKeyHash);
		return (receiver as any).processDirectPubSubMessage({
			pubsubMessage,
			message: {
				header: { signatures: { publicKeys: [directPeer.publicKey] } },
			},
			from: directPeer.publicKey,
			stream:
				tracked?.protocol === protocol
					? tracked
					: { protocol, publicKey: directPeer.publicKey },
		});
	};
	const nextAutoTopicRootCandidateSnapshot = (
		pubsub: TopicControlPlane,
		additions: readonly string[],
	) => {
		const internals = pubsub as any;
		const before =
			internals.pendingAutoTopicRootCandidates ??
			pubsub.topicRootControlPlane.getTopicRootCandidates();
		return [...new Set([...before, ...additions])]
			.sort((left, right) => (left < right ? -1 : left > right ? 1 : 0))
			.slice(0, TOPIC_ROOT_CANDIDATES_MAX);
	};
	const addAutoTopicRootCandidatesForTest = (
		pubsub: TopicControlPlane,
		additions: readonly string[],
	) =>
		(pubsub as any).setAutoTopicRootCandidateSnapshot(
			nextAutoTopicRootCandidateSnapshot(pubsub, additions),
		);
	const preparePendingAutoTopicRootCandidateUpdate = (
		pubsub: TopicControlPlane,
		label: string,
	) => {
		const internals = pubsub as any;
		internals.scheduleReconcileShardOverlays = () => {};
		internals.scheduleHostOwnedShardRoots = () => {};
		expect(
			addAutoTopicRootCandidatesForTest(pubsub, [
				canonicalCandidate(`${label}-leading`),
			]),
		).to.equal(true);
		expect(
			addAutoTopicRootCandidatesForTest(pubsub, [
				canonicalCandidate(`${label}-pending`),
			]),
		).to.equal(true);
		expect(internals.pendingAutoTopicRootCandidates).to.not.equal(undefined);
		return internals;
	};

	const stubFirstFanoutJoinUntilAbort = (
		expectedRoot: string,
		releaseAfterAbort?: Promise<void>,
		nextExpectedRoot = expectedRoot,
	) => {
		const started = deferred();
		const aborted = deferred();
		let abortCount = 0;
		const join = sinon
			.stub(FanoutChannel.prototype, "join")
			.callsFake(function (this: FanoutChannel, _options, joinOptions) {
				expect(this.root).to.equal(
					join.callCount > 1 ? nextExpectedRoot : expectedRoot,
				);
				if (join.callCount > 1) return Promise.resolve();
				started.resolve();
				return new Promise<void>((_resolve, reject) => {
					const onAbort = async () => {
						abortCount += 1;
						aborted.resolve();
						await releaseAfterAbort;
						reject(
							joinOptions?.signal?.reason ??
								new AbortError("stale join aborted"),
						);
					};
					if (joinOptions?.signal?.aborted) {
						onAbort();
					} else {
						joinOptions?.signal?.addEventListener("abort", onAbort, {
							once: true,
						});
					}
				});
			});
		return {
			aborted: aborted.promise,
			get abortCount() {
				return abortCount;
			},
			join,
			started: started.promise,
		};
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
		const addedCandidate = canonicalCandidate("query-race");
		const { afterRoot: currentRoot, topic } = findCandidateTransitionTopic(
			[staleRoot],
			[staleRoot, addedCandidate],
		);

		const queryGate = deferred();
		const queryEntered = deferred();
		internals.scheduleReconcileShardOverlays = () => {};
		internals.scheduleHostOwnedShardRoots = () => {};
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
			addAutoTopicRootCandidatesForTest(pubsub, [addedCandidate]),
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
		const addedCandidate = canonicalCandidate("cache-race");
		const { afterRoot: currentRoot, topic } = findCandidateTransitionTopic(
			[staleRoot],
			[staleRoot, addedCandidate],
		);

		internals.scheduleReconcileShardOverlays = () => {};
		internals.scheduleHostOwnedShardRoots = () => {};
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
			addAutoTopicRootCandidatesForTest(pubsub, [addedCandidate]),
		).to.equal(true);
		firstStateGate.resolve();

		expect(await resolving).to.equal(currentRoot);
		expect(internals.shardRootCache.get(topic)?.root).to.equal(currentRoot);
		expect(stateCalls).to.equal(2);
	});

	it("coalesces auto-candidate generations globally on a fixed cooldown", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			toFake: ["Date", "clearTimeout", "setTimeout"],
		});
		const scheduled = { host: 0, reconcile: 0 };
		internals.scheduleReconcileShardOverlays = () => scheduled.reconcile++;
		internals.scheduleHostOwnedShardRoots = () => scheduled.host++;
		const candidates = Array.from({ length: 4 }, (_, index) =>
			canonicalCandidate(`coalesced-auto-root-${index}`),
		);
		const initial = pubsub.topicRootControlPlane.getTopicRootCandidates();
		const leading = [...initial, candidates[0]!].sort();
		const interim = [...leading, candidates[1]!].sort();
		const trailing = [...interim, candidates[2]!].sort();
		const final = [...trailing, candidates[3]!].sort();

		try {
			expect(internals.setAutoTopicRootCandidateSnapshot(leading)).to.equal(
				true,
			);
			expect(scheduled).to.deep.equal({ host: 1, reconcile: 1 });
			expect(internals.pendingAutoTopicRootCandidates).to.equal(undefined);
			const leadingTimer = internals.autoTopicRootCandidateUpdateTimer;
			expect(leadingTimer).to.not.equal(undefined);

			expect(internals.setAutoTopicRootCandidateSnapshot(interim)).to.equal(
				true,
			);
			expect(internals.setAutoTopicRootCandidateSnapshot(trailing)).to.equal(
				true,
			);
			expect(internals.autoTopicRootCandidateUpdateTimer).to.equal(
				leadingTimer,
			);
			expect(internals.pendingAutoTopicRootCandidates).to.deep.equal(trailing);
			expect(
				internals.maybeDisableAutoTopicRootCandidatesIfExternallyConfigured(),
			).to.equal(false);
			expect([...internals.autoTopicRootCandidateSet]).to.deep.equal(
				pubsub.topicRootControlPlane.getTopicRootCandidates(),
			);
			expect(scheduled).to.deep.equal({ host: 1, reconcile: 1 });

			clock.tick(1_999);
			expect(scheduled).to.deep.equal({ host: 1, reconcile: 1 });
			clock.tick(1);
			expect(
				pubsub.topicRootControlPlane.getTopicRootCandidates(),
			).to.deep.equal(trailing);
			expect(scheduled).to.deep.equal({ host: 2, reconcile: 2 });
			expect(internals.pendingAutoTopicRootCandidates).to.equal(undefined);

			const trailingTimer = internals.autoTopicRootCandidateUpdateTimer;
			expect(trailingTimer).to.not.equal(undefined);
			expect(trailingTimer).to.not.equal(leadingTimer);
			expect(internals.setAutoTopicRootCandidateSnapshot(final)).to.equal(true);
			expect(internals.autoTopicRootCandidateUpdateTimer).to.equal(
				trailingTimer,
			);
			expect(scheduled).to.deep.equal({ host: 2, reconcile: 2 });

			clock.tick(2_000);
			expect(
				pubsub.topicRootControlPlane.getTopicRootCandidates(),
			).to.deep.equal(final);
			expect(scheduled).to.deep.equal({ host: 3, reconcile: 3 });
		} finally {
			internals.clearAutoTopicRootCandidateUpdateSchedule();
			clock.restore();
		}
	});

	it("drops a pending auto-candidate update when explicit candidates are set", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = preparePendingAutoTopicRootCandidateUpdate(
			pubsub,
			"explicit",
		);

		const explicit = ["explicit-root-candidate"];
		pubsub.setTopicRootCandidates(explicit);
		expect(internals.autoTopicRootCandidates).to.equal(false);
		expect(internals.autoTopicRootCandidateUpdateTimer).to.equal(undefined);
		expect(internals.pendingAutoTopicRootCandidates).to.equal(undefined);
		internals.flushPendingAutoTopicRootCandidateUpdate();
		expect(pubsub.topicRootControlPlane.getTopicRootCandidates()).to.deep.equal(
			explicit,
		);
	});

	it("drops a pending auto-candidate update when auto mode is externally disabled", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = preparePendingAutoTopicRootCandidateUpdate(
			pubsub,
			"external",
		);

		const external = ["externally-configured-root-candidate"];
		pubsub.topicRootControlPlane.setTopicRootCandidates(external);
		expect(
			internals.maybeDisableAutoTopicRootCandidatesIfExternallyConfigured(),
		).to.equal(true);
		expect(internals.autoTopicRootCandidates).to.equal(false);
		expect(internals.autoTopicRootCandidateUpdateTimer).to.equal(undefined);
		expect(internals.pendingAutoTopicRootCandidates).to.equal(undefined);
		internals.flushPendingAutoTopicRootCandidateUpdate();
		expect(pubsub.topicRootControlPlane.getTopicRootCandidates()).to.deep.equal(
			external,
		);
	});

	it("drops a pending auto-candidate update on stop", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = preparePendingAutoTopicRootCandidateUpdate(
			pubsub,
			"stop",
		);
		const appliedBeforeStop =
			pubsub.topicRootControlPlane.getTopicRootCandidates();

		await pubsub.stop();
		expect(internals.autoTopicRootCandidateUpdateTimer).to.equal(undefined);
		expect(internals.pendingAutoTopicRootCandidates).to.equal(undefined);
		internals.flushPendingAutoTopicRootCandidateUpdate();
		expect(pubsub.topicRootControlPlane.getTopicRootCandidates()).to.deep.equal(
			appliedBeforeStop,
		);
	});

	it("does not let an old opener block or overwrite the current generation", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const fanout = session.peers[0]!.services.fanout as any;
		const oldRoot = canonicalCandidate("old-generation");
		const addedCandidate = canonicalCandidate("new-generation");

		internals.scheduleReconcileShardOverlays = () => {};
		internals.scheduleHostOwnedShardRoots = () => {};
		expect(addAutoTopicRootCandidatesForTest(pubsub, [oldRoot])).to.equal(true);
		// The first merge only establishes this race's stale generation. Expire its
		// cooldown so the later merge remains the immediate generation change under test.
		internals.clearAutoTopicRootCandidateUpdateSchedule();
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
				addAutoTopicRootCandidatesForTest(pubsub, [addedCandidate]),
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

	it("aborts a stale shard join when auto-root candidates change", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const staleRoot = canonicalCandidate("stale-join");
		const addedCandidate = canonicalCandidate("moved-root");

		internals.scheduleHostOwnedShardRoots = () => {};
		expect(addAutoTopicRootCandidatesForTest(pubsub, [staleRoot])).to.equal(
			true,
		);
		// The first merge only establishes this race's stale generation. Expire its
		// cooldown so the later merge remains the immediate generation change under test.
		internals.clearAutoTopicRootCandidateUpdateSchedule();
		const beforeCandidates =
			pubsub.topicRootControlPlane.getTopicRootCandidates();
		const afterCandidates = [...beforeCandidates, addedCandidate];
		const { topic } = findCandidateTransitionTopic(
			beforeCandidates,
			afterCandidates,
			staleRoot,
			pubsub.publicKeyHash,
		);
		const staleGeneration = internals.getTopicRootCandidateGeneration();
		internals.resolveShardRootState = async () => {
			const candidateGeneration = internals.getTopicRootCandidateGeneration();
			return {
				root:
					candidateGeneration === staleGeneration
						? staleRoot
						: pubsub.publicKeyHash,
				candidateGeneration,
			};
		};
		const staleJoin = stubFirstFanoutJoinUntilAbort(staleRoot);

		try {
			const opening = internals.ensureFanoutChannel(topic);
			await staleJoin.started;

			expect(
				addAutoTopicRootCandidatesForTest(pubsub, [addedCandidate]),
			).to.equal(true);
			await Promise.race([
				staleJoin.aborted,
				delay(1_000).then(() => {
					throw new Error("stale join was not aborted");
				}),
			]);
			await opening;

			expect(internals.fanoutChannels.get(topic)?.root).to.equal(
				pubsub.publicKeyHash,
			);
		} finally {
			staleJoin.join.restore();
		}
	});

	it("applies a queued candidate on the trailing cooldown and reopens a stale join once", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const fanout = session.peers[0]!.services.fanout;
		const internals = pubsub as any;
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			toFake: ["Date", "clearTimeout", "setTimeout"],
		});
		const staleRoot = canonicalCandidate("queued-stale-join");
		const addedCandidate = canonicalCandidate("queued-moved-root");
		internals.scheduleHostOwnedShardRoots = () => {};

		expect(addAutoTopicRootCandidatesForTest(pubsub, [staleRoot])).to.equal(
			true,
		);
		const beforeCandidates =
			pubsub.topicRootControlPlane.getTopicRootCandidates();
		const afterCandidates = nextAutoTopicRootCandidateSnapshot(pubsub, [
			addedCandidate,
		]);
		const { topic } = findCandidateTransitionTopic(
			beforeCandidates,
			afterCandidates,
			staleRoot,
			addedCandidate,
		);
		const staleGeneration = internals.getTopicRootCandidateGeneration();
		internals.resolveShardRootState = async () => {
			const candidateGeneration = internals.getTopicRootCandidateGeneration();
			return {
				root:
					candidateGeneration === staleGeneration ? staleRoot : addedCandidate,
				candidateGeneration,
			};
		};

		const waitFor = sinon.stub(fanout, "waitFor").resolves([]);
		const staleJoin = stubFirstFanoutJoinUntilAbort(
			staleRoot,
			undefined,
			addedCandidate,
		);

		try {
			const opening = internals.ensureFanoutChannel(topic);
			await staleJoin.started;

			expect(
				addAutoTopicRootCandidatesForTest(pubsub, [addedCandidate]),
			).to.equal(true);
			expect(internals.getTopicRootCandidateGeneration()).to.equal(
				staleGeneration,
			);
			expect(internals.pendingAutoTopicRootCandidates).to.deep.equal(
				afterCandidates,
			);
			expect(staleJoin.abortCount).to.equal(0);

			await clock.tickAsync(1_999);
			expect(staleJoin.abortCount).to.equal(0);
			expect(staleJoin.join.callCount).to.equal(1);

			await clock.tickAsync(1);
			await staleJoin.aborted;
			await opening;
			expect(staleJoin.abortCount).to.equal(1);
			expect(staleJoin.join.callCount).to.equal(2);
			expect(
				pubsub.topicRootControlPlane.getTopicRootCandidates(),
			).to.deep.equal(afterCandidates);
			expect(internals.fanoutChannels.get(topic)?.root).to.equal(
				addedCandidate,
			);

			// The trailing application opens a fresh cooldown. With nothing pending,
			// its expiry must not cause another abort or reopen.
			await clock.tickAsync(2_000);
			expect(staleJoin.abortCount).to.equal(1);
			expect(staleJoin.join.callCount).to.equal(2);
		} finally {
			internals.clearAutoTopicRootCandidateUpdateSchedule();
			staleJoin.join.restore();
			waitFor.restore();
			clock.restore();
		}
	});

	it("retries a stale join when candidate generation cycles back", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const fanout = session.peers[0]!.services.fanout;
		const internals = pubsub as any;
		const staleRoot = canonicalCandidate("cycle-root");
		const addedCandidate = canonicalCandidate("transient-root");

		internals.scheduleHostOwnedShardRoots = () => {};
		internals.reconcileShardOverlays = async () => {};
		expect(addAutoTopicRootCandidatesForTest(pubsub, [staleRoot])).to.equal(
			true,
		);
		const originalCandidates =
			pubsub.topicRootControlPlane.getTopicRootCandidates();
		const { topic } = findCandidateTransitionTopic(
			originalCandidates,
			[...originalCandidates, addedCandidate],
			staleRoot,
		);
		const originalGeneration = internals.getTopicRootCandidateGeneration();
		internals.resolveShardRootState = async () => ({
			root: staleRoot,
			candidateGeneration: internals.getTopicRootCandidateGeneration(),
		});

		const waitFor = sinon.stub(fanout, "waitFor").resolves([]);
		const releaseStaleJoin = deferred();
		const staleJoin = stubFirstFanoutJoinUntilAbort(
			staleRoot,
			releaseStaleJoin.promise,
		);

		try {
			const opening = internals.ensureFanoutChannel(topic);
			await staleJoin.started;

			pubsub.topicRootControlPlane.setTopicRootCandidates([
				...originalCandidates,
				addedCandidate,
			]);
			internals.scheduleReconcileShardOverlays();
			await staleJoin.aborted;

			const externalAbort = new AbortController();
			const externalReason = new AbortError("caller stopped waiting");
			const externallyCancelled = internals.ensureFanoutChannel(topic, {
				signal: externalAbort.signal,
			});
			externalAbort.abort(externalReason);
			await expect(externallyCancelled).to.be.rejectedWith(externalReason);

			const concurrentOpening = internals.ensureFanoutChannel(topic);
			pubsub.topicRootControlPlane.setTopicRootCandidates(originalCandidates);
			internals.scheduleReconcileShardOverlays();

			releaseStaleJoin.resolve();
			await opening;
			await concurrentOpening;

			expect(internals.getTopicRootCandidateGeneration()).to.equal(
				originalGeneration,
			);
			expect(staleJoin.join.callCount).to.equal(2);
			expect(internals.fanoutChannels.get(topic)?.root).to.equal(staleRoot);
			expect(internals.ensureFanoutChannelInFlight.size).to.equal(0);
		} finally {
			releaseStaleJoin.resolve();
			staleJoin.join.restore();
			waitFor.restore();
		}
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
			abortController: new AbortController(),
			candidateGeneration,
			lifecycleRevision: lifecycleRevision - 1,
			opening: new Promise<void>(() => {}),
			settled: Promise.resolve(),
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

	it("does not restore unsigned self on an idempotent start", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const pubsub = session.peers[0]!.services.pubsub;
		const signedPeer = session.peers[1]!.services.pubsub;
		const internals = pubsub as any;
		const send = sinon
			.stub(internals, "sendAutoTopicRootCandidates")
			.resolves();
		const refresh = sinon
			.stub(internals, "refreshLocalTopicRootCandidateClaim")
			.rejects(new Error("signing unavailable"));

		try {
			pubsub.addPeer(
				signedPeer.peerId,
				signedPeer.publicKey,
				"/peerbit/topic-control-plane/2.1.0",
				"idempotent-start-signed-peer",
			);
			internals.localSignedTopicRootCandidateClaim = undefined;
			internals.signedTopicRootCandidateClaims.delete(pubsub.publicKeyHash);
			internals.rebuildAutoTopicRootCandidatesFromClaims(BigInt(Date.now()), {
				immediate: true,
			});
			expect(
				pubsub.topicRootControlPlane.getTopicRootCandidates(),
			).to.not.include(pubsub.publicKeyHash);

			await pubsub.start();

			expect(refresh.called).to.equal(false);
			expect(
				pubsub.topicRootControlPlane.getTopicRootCandidates(),
			).to.not.include(pubsub.publicKeyHash);
		} finally {
			refresh.restore();
			send.restore();
		}
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
		const autoPeer = canonicalCandidate("stale-auto-peer");
		expect(addAutoTopicRootCandidatesForTest(pubsub, [autoPeer])).to.equal(
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
			internals.normalizePeerTopicRootState(shardTopic, autoPeer),
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

	it("processes targeted root control from another delivery-mode identity", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const receiver = session.peers[0]!.services.pubsub;
		const sender = session.peers[1]!.services.pubsub;
		const processDirect = sinon
			.stub(receiver as any, "processDirectPubSubMessage")
			.resolves();
		sinon.stub(receiver, "verifyAndProcess").resolves(true);

		const createMessage = (recipient: string) =>
			new DataMessage({
				data: new TopicRootQuery({
					requestId: 1,
					topic: "/peerbit/pubsub-shard/1/foreign-mode",
				}).bytes(),
				header: new MessageHeader({
					mode: new ForeignSilentDelivery(
						[recipient],
						1,
					) as unknown as DeliveryMode,
					session: 1,
				}),
			});
		const stream = { publicKey: sender.publicKey } as any;

		await receiver.onDataMessage(
			sender.publicKey,
			stream,
			createMessage(receiver.publicKeyHash),
			0,
		);
		expect(processDirect.calledOnce).to.equal(true);

		processDirect.resetHistory();
		await receiver.onDataMessage(
			sender.publicKey,
			stream,
			createMessage("different-recipient"),
			0,
		);
		expect(processDirect.called).to.equal(false);
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

		expect(
			receiver.topicRootControlPlane.getTopicRootCandidates(),
		).to.deep.equal(candidatesBefore);
	});

	it("imports a relayed 2.1 candidate only from its nested self-signature", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(3);
		const receiver = session.peers[0]!.services.pubsub;
		const origin = session.peers[1]!.services.pubsub;
		const relay = session.peers[2]!.services.pubsub;
		const originInternals = origin as any;
		const receiverInternals = receiver as any;
		receiver.addPeer(
			relay.peerId,
			relay.publicKey,
			"/peerbit/topic-control-plane/2.1.0",
			"relayed-claim-test",
		);
		await originInternals.refreshLocalTopicRootCandidateClaim();
		const originClaim = originInternals.signedTopicRootCandidateClaims.get(
			origin.publicKeyHash,
		);
		expect(originClaim.expires - originClaim.timestamp).to.equal(90_000n);
		const rawClaim = originClaim.bytes;
		const relayDelta = sinon
			.stub(receiverInternals, "sendSignedTopicRootCandidateClaims")
			.resolves();
		const relaySnapshot = sinon
			.stub(receiverInternals, "sendAutoTopicRootCandidates")
			.resolves();

		await processDirectRootControl(
			receiver,
			new TopicRootCandidateClaims({ claims: [rawClaim] }),
			relay,
			"/peerbit/topic-control-plane/2.1.0",
		);

		expect(receiver.topicRootControlPlane.getTopicRootCandidates()).to.include(
			origin.publicKeyHash,
		);
		expect(
			receiverInternals.signedTopicRootCandidateClaims.get(origin.publicKeyHash)
				?.bytes,
		).to.deep.equal(rawClaim);
		expect(relayDelta.calledOnce).to.equal(true);
		expect(relayDelta.firstCall.args[0]).to.deep.equal([rawClaim]);
		expect(relaySnapshot.called).to.equal(false);
	});

	it("advertises 2.1 first and keeps signed claims outside the native variants codec", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		expect(pubsub.multicodecs).to.deep.equal([
			"/peerbit/topic-control-plane/2.1.0",
			"/peerbit/topic-control-plane/2.0.0",
		]);
		internals.nativeTopicControl = {
			decodePubSubMessage: () => {
				throw new Error("variant 8 reached native variants codec");
			},
		};
		const encoded = internals.encodePubSubMessage(
			new TopicRootCandidateClaims({ claims: [] }),
		);

		expect(encoded[0]).to.equal(8);
		expect(internals.decodePubSubMessage(encoded)).to.be.instanceOf(
			TopicRootCandidateClaims,
		);
	});

	it("sends the signed snapshot on each outbound stream", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const receiver = session.peers[0]!.services.pubsub;
		const sender = session.peers[1]!.services.pubsub;
		const internals = receiver as any;
		const send = sinon
			.stub(internals, "sendAutoTopicRootCandidates")
			.resolves();

		const stream = receiver.addPeer(
			sender.peerId,
			sender.publicKey,
			"/peerbit/topic-control-plane/2.1.0",
			"outbound-ready-retry-test",
		);
		expect(send.called).to.equal(false);
		stream.dispatchEvent(new CustomEvent("stream:outbound"));
		await waitForResolved(() => expect(send.callCount).to.equal(1), {
			timeout: 1_000,
			delayInterval: 10,
		});
		stream.dispatchEvent(new CustomEvent("stream:outbound"));
		await waitForResolved(() => expect(send.callCount).to.equal(2), {
			timeout: 1_000,
			delayInterval: 10,
		});
	});

	it("forwards retained claims when local refresh fails but envelope signing remains available", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const receiver = session.peers[0]!.services.pubsub;
		const origin = session.peers[1]!.services.pubsub;
		const receiverInternals = receiver as any;
		const originInternals = origin as any;
		const rawClaim = originInternals.localSignedTopicRootCandidateClaim.bytes;
		expect(
			await receiverInternals.importSignedTopicRootCandidateClaim(
				rawClaim,
				origin.publicKeyHash,
			),
		).to.equal(true);
		receiverInternals.localSignedTopicRootCandidateClaim = undefined;
		receiverInternals.signedTopicRootCandidateClaims.delete(
			receiver.publicKeyHash,
		);
		const refresh = sinon
			.stub(receiverInternals, "refreshLocalTopicRootCandidateClaim")
			.rejects(new Error("local claim refresh failed"));
		const send = sinon
			.stub(receiverInternals, "sendSignedTopicRootCandidateClaims")
			.resolves();
		const target = {
			protocol: "/peerbit/topic-control-plane/2.1.0",
		} as any;

		try {
			await receiverInternals.sendAutoTopicRootCandidates([target]);
			expect(refresh.calledOnce).to.equal(true);
			expect(send.calledOnce).to.equal(true);
			expect(send.firstCall.args).to.deep.equal([[rawClaim], [target]]);
		} finally {
			refresh.restore();
			send.restore();
		}
	});

	it("rejects tampered, wrong-scope, noncanonical-lifetime, expired, and multisigned 2.1 claims", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(3);
		const receiver = session.peers[0]!.services.pubsub;
		const origin = session.peers[1]!.services.pubsub;
		const extraSigner = session.peers[2]!.services.pubsub;
		const receiverInternals = receiver as any;
		const originInternals = origin as any;
		const makeClaimBytes = async (options?: CandidateClaimOptions) =>
			dataMessageBytes(await createCandidateClaim(origin, options));

		const valid = await makeClaimBytes();
		const tampered = valid.slice();
		tampered[tampered.length - 1] ^= 1;
		const wrongScope = originInternals.topicRootCandidateClaimData.slice();
		wrongScope[wrongScope.length - 1] ^= 1;
		const future = await createCandidateClaim(origin);
		future.header.timestamp = BigInt(Date.now() + 60_000);
		future.header.expires = future.header.timestamp + 90_000n;
		future.header.signatures = undefined;
		await future.sign(origin.sign);
		const unsupportedPrehash = await createCandidateClaim(origin);
		unsupportedPrehash.header.signatures!.signatures[0]!.prehash = 2 as any;
		const cases = [
			tampered,
			await makeClaimBytes({ data: wrongScope }),
			await makeClaimBytes({ expiresAt: Date.now() - 1 }),
			await makeClaimBytes({ expiresInMs: 89_999 }),
			await makeClaimBytes({ expiresInMs: 90_001 }),
			dataMessageBytes(future),
			await makeClaimBytes({ extraSigners: [extraSigner.sign] }),
			dataMessageBytes(unsupportedPrehash),
		];

		for (const claim of cases) {
			expect(
				await receiverInternals.importSignedTopicRootCandidateClaim(
					claim,
					extraSigner.publicKeyHash,
				),
			).to.equal(false);
		}
		expect(
			receiverInternals.signedTopicRootCandidateClaims.has(
				origin.publicKeyHash,
			),
		).to.equal(false);
	});

	it("suppresses a directly departed root without replaying or deleting its signed lease", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(3);
		const receiver = session.peers[0]!.services.pubsub;
		const origin = session.peers[1]!.services.pubsub;
		const relay = session.peers[2]!.services.pubsub;
		const receiverInternals = receiver as any;
		const protocol = "/peerbit/topic-control-plane/2.1.0";

		receiver.addPeer(
			origin.peerId,
			origin.publicKey,
			protocol,
			"departed-root-expiry-test",
		);
		receiverInternals.clearTopicRootCandidateClaimTimer();
		try {
			const claim = await createCandidateClaim(origin);
			const rawClaim = dataMessageBytes(claim);

			await processDirectRootControl(
				receiver,
				new TopicRootCandidateClaims({
					claims: [rawClaim],
				}),
				origin,
				protocol,
			);

			const beforeCandidates = [
				receiver.publicKeyHash,
				origin.publicKeyHash,
			].sort();
			const afterCandidates = [receiver.publicKeyHash];
			const { topic: shardTopic } = findCandidateTransitionTopic(
				beforeCandidates,
				afterCandidates,
				origin.publicKeyHash,
				receiver.publicKeyHash,
			);
			expect(
				receiver.topicRootControlPlane.getTopicRootCandidates(),
			).to.deep.equal(beforeCandidates);
			expect(
				(await receiverInternals.resolveShardRootState(shardTopic)).root,
			).to.equal(origin.publicKeyHash);
			expect(receiverInternals.shardRootCache.get(shardTopic)?.root).to.equal(
				origin.publicKeyHash,
			);

			await receiverInternals._removePeer(origin.publicKey);
			expect(
				receiver.peers.has(origin.publicKeyHash),
				"the departed direct peer should be removed",
			).to.equal(false);
			expect(
				receiverInternals.signedTopicRootCandidateClaims.has(
					origin.publicKeyHash,
				),
				"the verified lease remains until its signed expiry",
			).to.equal(true);
			expect(
				receiverInternals.topicRootCandidateClaimReplayFloors.get(
					origin.publicKeyHash,
				),
			).to.equal(claim.header.timestamp);
			expect(
				receiverInternals.suppressedDepartedTopicRootCandidateClaims.has(
					origin.publicKeyHash,
				),
			).to.equal(true);
			const suppressed =
				receiverInternals.suppressedDepartedTopicRootCandidateClaims.get(
					origin.publicKeyHash,
				);
			expect(suppressed.timestamp).to.equal(claim.header.timestamp);
			expect(bytesEqual(suppressed.bytes, rawClaim)).to.equal(true);
			expect(
				receiver.topicRootControlPlane.getTopicRootCandidates(),
			).to.deep.equal(afterCandidates);
			expect(
				receiverInternals.shardRootCache.get(shardTopic)?.root,
				"source departure must invalidate the cached departed root",
			).not.to.equal(origin.publicKeyHash);
			expect(
				(await receiverInternals.resolveShardRootState(shardTopic)).root,
			).to.equal(receiver.publicKeyHash);
			const sendClaims = sinon
				.stub(receiverInternals, "sendSignedTopicRootCandidateClaims")
				.resolves();
			try {
				await receiverInternals.sendAutoTopicRootCandidates([
					{ protocol } as any,
				]);
				const advertised = sendClaims
					.getCalls()
					.flatMap((call) => call.args[0] as Uint8Array[]);
				expect(
					advertised.some((candidate) => bytesEqual(candidate, rawClaim)),
					"a suppressed lease must not be advertised to a newly connected peer",
				).to.equal(false);
			} finally {
				sendClaims.restore();
			}

			const relayStream = receiver.addPeer(
				relay.peerId,
				relay.publicKey,
				protocol,
				"sourceless-claim-relay-test",
			);
			expect(
				await receiverInternals.importSignedTopicRootCandidateClaim(
					rawClaim,
					relay.publicKeyHash,
					relayStream,
				),
				"the same signed lease must not undo direct-departure suppression",
			).to.equal(false);
			expect(
				receiver.topicRootControlPlane.getTopicRootCandidates(),
			).to.deep.equal(afterCandidates);
			expect(
				receiverInternals.suppressedDepartedTopicRootCandidateClaims.has(
					origin.publicKeyHash,
				),
			).to.equal(true);

			const retained = receiverInternals.signedTopicRootCandidateClaims.get(
				origin.publicKeyHash,
			);
			const replayFloor =
				receiverInternals.topicRootCandidateClaimReplayFloors.get(
					origin.publicKeyHash,
				);
			const originReconnectStream = receiver.addPeer(
				origin.peerId,
				origin.publicKey,
				protocol,
				"departed-root-reconnect-test",
			);
			expect(
				await receiverInternals.importSignedTopicRootCandidateClaim(
					rawClaim,
					origin.publicKeyHash,
					originReconnectStream,
				),
				"the exact claim is reactivated, not imported or re-relayed",
			).to.equal(false);
			expect(
				receiverInternals.suppressedDepartedTopicRootCandidateClaims.has(
					origin.publicKeyHash,
				),
			).to.equal(false);
			expect(
				receiverInternals.signedTopicRootCandidateClaims.get(
					origin.publicKeyHash,
				),
				"direct reactivation must not replace or extend the retained record",
			).to.equal(retained);
			expect(
				receiverInternals.topicRootCandidateClaimReplayFloors.get(
					origin.publicKeyHash,
				),
			).to.equal(replayFloor);
			expect(
				receiver.topicRootControlPlane.getTopicRootCandidates(),
			).to.deep.equal(beforeCandidates);

			await receiverInternals._removePeer(origin.publicKey);
			expect(
				receiverInternals.suppressedDepartedTopicRootCandidateClaims.has(
					origin.publicKeyHash,
				),
			).to.equal(true);

			await delay(5);
			const fresherClaim = await createCandidateClaim(origin);
			expect(fresherClaim.header.timestamp > claim.header.timestamp).to.equal(
				true,
			);
			expect(
				await receiverInternals.importSignedTopicRootCandidateClaim(
					dataMessageBytes(fresherClaim),
					relay.publicKeyHash,
					relayStream,
				),
			).to.equal(true);
			receiverInternals.rebuildAutoTopicRootCandidatesFromClaims(undefined, {
				immediate: true,
			});
			expect(
				receiverInternals.suppressedDepartedTopicRootCandidateClaims.has(
					origin.publicKeyHash,
				),
			).to.equal(false);
			expect(
				receiver.topicRootControlPlane.getTopicRootCandidates(),
			).to.deep.equal(beforeCandidates);
			expect(
				(await receiverInternals.resolveShardRootState(shardTopic)).root,
			).to.equal(origin.publicKeyHash);
		} finally {
			receiverInternals.clearTopicRootCandidateClaimTimer();
			receiverInternals.clearAutoTopicRootCandidateUpdateSchedule();
		}
	});

	it("does not withdraw a transitive claim when only its relay departs", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(3);
		const receiver = session.peers[0]!.services.pubsub;
		const origin = session.peers[1]!.services.pubsub;
		const relay = session.peers[2]!.services.pubsub;
		const receiverInternals = receiver as any;
		const protocol = "/peerbit/topic-control-plane/2.1.0";
		const relayStream = receiver.addPeer(
			relay.peerId,
			relay.publicKey,
			protocol,
			"transitive-claim-relay-test",
		);
		receiverInternals.clearTopicRootCandidateClaimTimer();
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			toFake: ["Date"],
		});

		try {
			const claim = await createCandidateClaim(origin);
			const rawClaim = dataMessageBytes(claim);
			expect(claim.header.expires - claim.header.timestamp).to.equal(90_000n);
			expect(
				await receiverInternals.importSignedTopicRootCandidateClaim(
					rawClaim,
					relay.publicKeyHash,
					relayStream,
				),
			).to.equal(true);
			receiverInternals.rebuildAutoTopicRootCandidatesFromClaims(undefined, {
				immediate: true,
			});
			expect(
				receiver.topicRootControlPlane.getTopicRootCandidates(),
			).to.include(origin.publicKeyHash);

			await receiverInternals._removePeer(relay.publicKey);
			expect(
				receiverInternals.signedTopicRootCandidateClaims.has(
					origin.publicKeyHash,
				),
				"a relay departure must not invent evidence that the signed origin left",
			).to.equal(true);
			expect(
				receiverInternals.suppressedDepartedTopicRootCandidateClaims.has(
					origin.publicKeyHash,
				),
			).to.equal(false);
			expect(
				receiver.topicRootControlPlane.getTopicRootCandidates(),
			).to.include(origin.publicKeyHash);

			clock.tick(90_001);
			receiverInternals.rebuildAutoTopicRootCandidatesFromClaims(undefined, {
				immediate: true,
			});
			expect(
				receiverInternals.signedTopicRootCandidateClaims.has(
					origin.publicKeyHash,
				),
				"the transitive claim still expires under its original 90s lease",
			).to.equal(false);
			expect(
				receiver.topicRootControlPlane.getTopicRootCandidates(),
			).to.not.include(origin.publicKeyHash);
		} finally {
			receiverInternals.clearTopicRootCandidateClaimTimer();
			receiverInternals.clearAutoTopicRootCandidateUpdateSchedule();
			clock.restore();
		}
	});

	it("does not let a legacy peer inject its advertised candidate list", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const receiver = session.peers[0]!.services.pubsub;
		const sender = session.peers[1]!.services.pubsub;
		const injected = Array.from(
			{ length: TOPIC_ROOT_CANDIDATES_MAX },
			(_, index) => canonicalCandidate(`legacy-injected-${index}`),
		);
		receiver.addPeer(
			sender.peerId,
			sender.publicKey,
			"/peerbit/topic-control-plane/2.0.0",
			"legacy-injection-test",
		);
		const admitted = receiver.topicRootControlPlane.getTopicRootCandidates();
		expect(admitted).to.include(sender.publicKeyHash);

		await processDirectRootControl(
			receiver,
			new TopicRootCandidates({ candidates: injected }),
			sender,
			"/peerbit/topic-control-plane/2.0.0",
		);

		const after = receiver.topicRootControlPlane.getTopicRootCandidates();
		expect(after).to.deep.equal(admitted);
		expect(
			after.filter((candidate) => injected.includes(candidate)),
		).to.deep.equal([]);

		await processDirectRootControl(
			receiver,
			new TopicRootCandidates({ candidates: injected }),
			sender,
			"/peerbit/topic-control-plane/2.1.0",
		);
		expect(
			receiver.topicRootControlPlane.getTopicRootCandidates(),
		).to.deep.equal(admitted);
	});

	it("uses unsigned self fallback only without a signed-protocol peer", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const receiver = session.peers[0]!.services.pubsub;
		const sender = session.peers[1]!.services.pubsub;
		const internals = receiver as any;
		internals.clearTopicRootCandidateClaimTimer();
		internals.localSignedTopicRootCandidateClaim = undefined;
		internals.signedTopicRootCandidateClaims.clear();
		internals.rebuildAutoTopicRootCandidatesFromClaims();
		expect(
			receiver.topicRootControlPlane.getTopicRootCandidates(),
			"standalone unsigned fallback",
		).to.deep.equal([receiver.publicKeyHash]);
		internals.scheduleAutoTopicRootCandidateUpdateCooldown();
		const addCooldown = internals.autoTopicRootCandidateUpdateTimer;

		receiver.addPeer(
			sender.peerId,
			sender.publicKey,
			"/peerbit/topic-control-plane/2.1.0",
			"signed-self-fallback-test",
		);
		expect(
			receiver.topicRootControlPlane.getTopicRootCandidates(),
		).to.not.include(receiver.publicKeyHash);
		expect(internals.autoTopicRootCandidateUpdateTimer).to.not.equal(
			addCooldown,
		);

		const removeCooldown = internals.autoTopicRootCandidateUpdateTimer;
		await internals._removePeer(sender.publicKey);
		expect(
			receiver.topicRootControlPlane.getTopicRootCandidates(),
			"unsigned fallback after the signed peer leaves",
		).to.deep.equal([receiver.publicKeyHash]);
		expect(internals.autoTopicRootCandidateUpdateTimer).to.not.equal(
			removeCooldown,
		);
	});

	it("selects the same lowest 64 signed origins without preferring self", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const left = session.peers[0]!.services.pubsub;
		const right = session.peers[1]!.services.pubsub;
		const origins = [
			left.publicKeyHash,
			right.publicKeyHash,
			...Array.from({ length: 63 }, (_, index) =>
				canonicalCandidate(`signed-cap-${index}`),
			),
		];
		const expected = [...origins].sort().slice(0, TOPIC_ROOT_CANDIDATES_MAX);
		const record = (index: number) => ({
			acceptUntil: performance.now() + 60_000,
			bytes: new Uint8Array([index]),
			expires: BigInt(Date.now() + 60_000),
			timestamp: BigInt(Date.now()),
		});

		for (const [peer, ordered] of [
			[left, origins],
			[right, [...origins].reverse()],
		] as const) {
			const internals = peer as any;
			internals.signedTopicRootCandidateClaims.clear();
			for (const [index, origin] of ordered.entries()) {
				internals.retainSignedTopicRootCandidateClaim(origin, record(index));
			}
			expect(internals.signedTopicRootCandidateClaims.size).to.equal(
				TOPIC_ROOT_CANDIDATES_MAX,
			);
			expect(
				[...internals.topicRootCandidateClaimReplayFloors.keys()].sort(),
			).to.deep.equal(expected);
			if (!expected.includes(peer.publicKeyHash)) {
				expect(internals.localSignedTopicRootCandidateClaim).to.equal(
					undefined,
				);
			}
			internals.rebuildAutoTopicRootCandidatesFromClaims();
			expect(peer.topicRootControlPlane.getTopicRootCandidates()).to.deep.equal(
				expected,
			);
		}
	});

	it("does not retain a claim when its authenticated source stream leaves during verification", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const receiver = session.peers[0]!.services.pubsub;
		const origin = session.peers[1]!.services.pubsub;
		const receiverInternals = receiver as any;
		const sourceStream = receiver.addPeer(
			origin.peerId,
			origin.publicKey,
			"/peerbit/topic-control-plane/2.1.0",
			"claim-source-verification-race-test",
		);
		receiverInternals.clearTopicRootCandidateClaimTimer();
		const rawClaim = dataMessageBytes(await createCandidateClaim(origin));
		const verification = deferred();
		const verify = sinon
			.stub(DataMessage.prototype, "verify")
			.callsFake(async () => {
				await verification.promise;
				return true;
			});

		try {
			const importing = receiverInternals.importSignedTopicRootCandidateClaim(
				rawClaim,
				origin.publicKeyHash,
				sourceStream,
			);
			await waitForResolved(() => expect(verify.calledOnce).to.equal(true), {
				timeout: 1_000,
				delayInterval: 10,
			});
			await receiverInternals._removePeer(origin.publicKey);
			verification.resolve();

			expect(await importing).to.equal(false);
			expect(
				receiverInternals.signedTopicRootCandidateClaims.has(
					origin.publicKeyHash,
				),
			).to.equal(false);
			expect(
				receiverInternals.topicRootCandidateClaimReplayFloors.has(
					origin.publicKeyHash,
				),
			).to.equal(false);
		} finally {
			verification.resolve();
			verify.restore();
			receiverInternals.clearTopicRootCandidateClaimTimer();
		}
	});

	it("does not retain a claim verified across stop and restart", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const receiver = session.peers[0]!.services.pubsub;
		const origin = session.peers[1]!.services.pubsub;
		const receiverInternals = receiver as any;
		const originInternals = origin as any;
		await originInternals.refreshLocalTopicRootCandidateClaim();
		const rawClaim = originInternals.localSignedTopicRootCandidateClaim.bytes;
		const verification = deferred();
		const verify = sinon
			.stub(DataMessage.prototype, "verify")
			.callsFake(async () => {
				await verification.promise;
				return true;
			});

		try {
			const importing = receiverInternals.importSignedTopicRootCandidateClaim(
				rawClaim,
				origin.publicKeyHash,
			);
			await waitForResolved(() => expect(verify.calledOnce).to.equal(true), {
				timeout: 1_000,
				delayInterval: 10,
			});
			await receiver.stop();
			expect(receiverInternals.signedTopicRootCandidateClaims.size).to.equal(0);
			expect(receiverInternals.localSignedTopicRootCandidateClaim).to.equal(
				undefined,
			);
			await receiver.start();
			verification.resolve();

			expect(await importing).to.equal(false);
			expect(
				receiverInternals.signedTopicRootCandidateClaims.has(
					origin.publicKeyHash,
				),
			).to.equal(false);
		} finally {
			verification.resolve();
			verify.restore();
		}
	});

	it("revalidates claim time bounds after asynchronous verification", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const receiver = session.peers[0]!.services.pubsub;
		const origin = session.peers[1]!.services.pubsub;
		const receiverInternals = receiver as any;
		const originInternals = origin as any;
		await originInternals.refreshLocalTopicRootCandidateClaim();
		const originClaim = originInternals.localSignedTopicRootCandidateClaim;
		const verification = deferred();
		const verify = sinon
			.stub(DataMessage.prototype, "verify")
			.callsFake(async () => {
				await verification.promise;
				return true;
			});
		let wallNow: sinon.SinonStub | undefined;

		try {
			const importing = receiverInternals.importSignedTopicRootCandidateClaim(
				originClaim.bytes,
				origin.publicKeyHash,
			);
			await waitForResolved(() => expect(verify.calledOnce).to.equal(true), {
				timeout: 1_000,
				delayInterval: 10,
			});
			wallNow = sinon
				.stub(Date, "now")
				.returns(Number(originClaim.timestamp - 31_000n));
			verification.resolve();

			expect(await importing).to.equal(false);
			expect(
				receiverInternals.signedTopicRootCandidateClaims.has(
					origin.publicKeyHash,
				),
			).to.equal(false);
		} finally {
			verification.resolve();
			wallNow?.restore();
			verify.restore();
		}
	});

	it("caps async and retained claims with a monotonic deadline", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const receiver = session.peers[0]!.services.pubsub;
		const origin = session.peers[1]!.services.pubsub;
		const receiverInternals = receiver as any;
		const wallNow = Date.now();
		const claim = await createCandidateClaim(origin);
		claim.header.timestamp = BigInt(wallNow + 30_000);
		claim.header.expires = claim.header.timestamp + 90_000n;
		claim.header.signatures = undefined;
		await claim.sign(origin.sign);
		const rawClaim = dataMessageBytes(claim);
		let monotonicNow = 1_000;
		const monotonic = sinon
			.stub(performance, "now")
			.callsFake(() => monotonicNow);
		const verification = deferred();
		const verify = sinon
			.stub(DataMessage.prototype, "verify")
			.callsFake(async () => {
				await verification.promise;
				return true;
			});

		try {
			const importing = receiverInternals.importSignedTopicRootCandidateClaim(
				rawClaim,
				origin.publicKeyHash,
			);
			await waitForResolved(() => expect(verify.calledOnce).to.equal(true), {
				timeout: 1_000,
				delayInterval: 10,
			});
			monotonicNow = 121_001;
			verification.resolve();
			expect(await importing).to.equal(false);

			verify.resetBehavior();
			verify.resolves(true);
			monotonicNow = 2_000;
			expect(
				await receiverInternals.importSignedTopicRootCandidateClaim(
					rawClaim,
					origin.publicKeyHash,
				),
			).to.equal(true);
			const retained = receiverInternals.signedTopicRootCandidateClaims.get(
				origin.publicKeyHash,
			);
			receiverInternals.pruneExpiredTopicRootCandidateClaims(
				BigInt(Date.now() - 60_000),
				retained.acceptUntil + 1,
			);
			expect(
				receiverInternals.signedTopicRootCandidateClaims.has(
					origin.publicKeyHash,
				),
			).to.equal(false);
		} finally {
			verification.resolve();
			verify.restore();
			monotonic.restore();
		}
	});

	it("does not re-admit a timer-pruned claim while wall time is frozen", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const receiver = session.peers[0]!.services.pubsub;
		const origin = session.peers[1]!.services.pubsub;
		const receiverInternals = receiver as any;
		const older = await createCandidateClaim(origin);
		await delay(5);
		const newer = await createCandidateClaim(origin);
		const olderBytes = dataMessageBytes(older);
		const newerBytes = dataMessageBytes(newer);
		receiverInternals.clearTopicRootCandidateClaimTimer();
		receiverInternals.localSignedTopicRootCandidateClaim = undefined;
		receiverInternals.signedTopicRootCandidateClaims.delete(
			receiver.publicKeyHash,
		);
		const refresh = sinon
			.stub(receiverInternals, "refreshLocalTopicRootCandidateClaim")
			.resolves(false);
		const clock = sinon.useFakeTimers({
			toFake: ["clearTimeout", "setTimeout"],
		});
		const wall = sinon
			.stub(Date, "now")
			.returns(Number(older.header.timestamp));
		let monotonicNow = 1_000;
		const monotonic = sinon
			.stub(performance, "now")
			.callsFake(() => monotonicNow);

		try {
			expect(
				await receiverInternals.importSignedTopicRootCandidateClaim(
					olderBytes,
					origin.publicKeyHash,
				),
			).to.equal(true);
			const retained = receiverInternals.signedTopicRootCandidateClaims.get(
				origin.publicKeyHash,
			);
			receiverInternals.clearTopicRootCandidateClaimTimer();
			receiverInternals.scheduleTopicRootCandidateClaimMaintenance(200_000);
			const expiryDelayMs = retained.acceptUntil - monotonicNow + 1;
			monotonicNow = retained.acceptUntil + 1;
			await clock.tickAsync(expiryDelayMs);

			expect(
				receiverInternals.signedTopicRootCandidateClaims.has(
					origin.publicKeyHash,
				),
			).to.equal(false);
			expect(
				receiverInternals.topicRootCandidateClaimReplayFloors.get(
					origin.publicKeyHash,
				),
			).to.equal(older.header.timestamp);
			expect(
				await receiverInternals.importSignedTopicRootCandidateClaim(
					olderBytes,
					origin.publicKeyHash,
				),
			).to.equal(false);
			expect(
				await receiverInternals.importSignedTopicRootCandidateClaim(
					newerBytes,
					origin.publicKeyHash,
				),
			).to.equal(true);
			expect(
				receiverInternals.topicRootCandidateClaimReplayFloors.get(
					origin.publicKeyHash,
				),
			).to.equal(newer.header.timestamp);
		} finally {
			receiverInternals.clearTopicRootCandidateClaimTimer();
			receiverInternals.clearAutoTopicRootCandidateUpdateSchedule();
			monotonic.restore();
			wall.restore();
			clock.restore();
			refresh.restore();
		}
	});

	it("rejects delayed expired and noncanonical local claims", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const originalCreate = internals.createMessage.bind(pubsub);
		const claim = await originalCreate(internals.topicRootCandidateClaimData, {
			expiresInMs: 90_000,
			mode: new AnyWhere(),
			priority: 1,
			skipRecipientValidation: true,
		});
		internals.clearSignedTopicRootCandidateState();
		let resolveClaim!: (claim: DataMessage) => void;
		const pendingClaim = new Promise<DataMessage>((resolve) => {
			resolveClaim = resolve;
		});
		const create = sinon.stub(internals, "createMessage").returns(pendingClaim);
		let wallNow: sinon.SinonStub | undefined;

		try {
			const refreshing = internals.refreshLocalTopicRootCandidateClaim();
			wallNow = sinon
				.stub(Date, "now")
				.returns(Number(claim.header.expires + 1n));
			resolveClaim(claim);
			expect(await refreshing).to.equal(false);
			expect(internals.localSignedTopicRootCandidateClaim).to.equal(undefined);

			wallNow.restore();
			wallNow = undefined;
			claim.header.expires = claim.header.timestamp + 89_999n;
			create.resetBehavior();
			create.resolves(claim);
			expect(await internals.refreshLocalTopicRootCandidateClaim()).to.equal(
				false,
			);
			expect(internals.localSignedTopicRootCandidateClaim).to.equal(undefined);
		} finally {
			resolveClaim(claim);
			wallNow?.restore();
			create.restore();
		}
	});

	it("resets replay floors on restart without extending an older lease", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const receiver = session.peers[0]!.services.pubsub;
		const origin = session.peers[1]!.services.pubsub;
		const receiverInternals = receiver as any;

		const older = await createCandidateClaim(origin);
		await delay(5);
		const newer = await createCandidateClaim(origin);
		expect(newer.header.timestamp > older.header.timestamp).to.equal(true);
		expect(newer.header.expires > older.header.expires).to.equal(true);

		expect(
			await receiverInternals.importSignedTopicRootCandidateClaim(
				dataMessageBytes(older),
				origin.publicKeyHash,
			),
		).to.equal(true);
		expect(
			await receiverInternals.importSignedTopicRootCandidateClaim(
				dataMessageBytes(newer),
				origin.publicKeyHash,
			),
		).to.equal(true);
		expect(
			await receiverInternals.importSignedTopicRootCandidateClaim(
				dataMessageBytes(older),
				origin.publicKeyHash,
			),
		).to.equal(false);
		expect(
			receiverInternals.signedTopicRootCandidateClaims.get(origin.publicKeyHash)
				.expires,
		).to.equal(newer.header.expires);

		await receiver.stop();
		expect(
			receiverInternals.signedTopicRootCandidateClaims.has(
				origin.publicKeyHash,
			),
		).to.equal(false);
		await receiver.start();
		const now = sinon
			.stub(Date, "now")
			.returns(Number(older.header.expires - 1_000n));
		try {
			expect(BigInt(Date.now()) < older.header.expires).to.equal(true);
			expect(
				await receiverInternals.importSignedTopicRootCandidateClaim(
					dataMessageBytes(older),
					origin.publicKeyHash,
				),
			).to.equal(true);
			expect(
				receiverInternals.topicRootCandidateClaimReplayFloors.get(
					origin.publicKeyHash,
				),
			).to.equal(older.header.timestamp);

			now.returns(Number(older.header.expires));
			expect(BigInt(Date.now()) < newer.header.expires).to.equal(true);
			receiverInternals.rebuildAutoTopicRootCandidatesFromClaims();
			expect(
				receiverInternals.signedTopicRootCandidateClaims.has(
					origin.publicKeyHash,
				),
			).to.equal(false);
		} finally {
			now.restore();
		}
	});

	it("keeps the newest local claim when signing finishes out of order", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const originalCreate = internals.createMessage.bind(pubsub);
		const makeClaim = (lifetimeMs: number) =>
			originalCreate(internals.topicRootCandidateClaimData, {
				expiresInMs: lifetimeMs,
				mode: new AnyWhere(),
				priority: 1,
				skipRecipientValidation: true,
			});
		const older = await makeClaim(90_000);
		await delay(5);
		const newer = await makeClaim(90_000);
		expect(newer.header.expires > older.header.expires).to.equal(true);
		let resolveOlder!: (claim: DataMessage) => void;
		let resolveNewer!: (claim: DataMessage) => void;
		const olderPending = new Promise<DataMessage>((resolve) => {
			resolveOlder = resolve;
		});
		const newerPending = new Promise<DataMessage>((resolve) => {
			resolveNewer = resolve;
		});
		const create = sinon.stub(internals, "createMessage");
		create.onFirstCall().returns(olderPending);
		create.onSecondCall().returns(newerPending);

		try {
			const first = internals.refreshLocalTopicRootCandidateClaim();
			const second = internals.refreshLocalTopicRootCandidateClaim();
			resolveNewer(newer);
			expect(await second).to.equal(true);
			resolveOlder(older);
			expect(await first).to.equal(false);
			expect(internals.localSignedTopicRootCandidateClaim.timestamp).to.equal(
				newer.header.timestamp,
			);
			expect(internals.localSignedTopicRootCandidateClaim.expires).to.equal(
				newer.header.expires,
			);
		} finally {
			resolveNewer(newer);
			resolveOlder(older);
			create.restore();
		}
	});

	it("backs off when refresh leaves no retained local claim", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		internals.clearTopicRootCandidateClaimTimer();
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			toFake: ["Date", "clearTimeout", "setTimeout"],
		});
		let monotonicNow = 0;
		const monotonic = sinon
			.stub(performance, "now")
			.callsFake(() => monotonicNow);
		internals.localSignedTopicRootCandidateClaim = {
			acceptUntil: performance.now() + 60_000,
			bytes: new Uint8Array([1]),
			timestamp: BigInt(Date.now() - 40_000),
			expires: BigInt(Date.now() + 60_000),
		};
		const refresh = sinon
			.stub(internals, "refreshLocalTopicRootCandidateClaim")
			.callsFake(async () => {
				internals.localSignedTopicRootCandidateClaim = undefined;
				return true;
			});

		try {
			internals.scheduleTopicRootCandidateClaimMaintenance();
			await clock.tickAsync(0);
			expect(refresh.callCount).to.equal(1);
			monotonicNow = 999;
			await clock.tickAsync(999);
			expect(refresh.callCount).to.equal(1);
			monotonicNow = 1_000;
			await clock.tickAsync(1);
			expect(refresh.callCount).to.equal(2);
		} finally {
			internals.clearTopicRootCandidateClaimTimer();
			monotonic.restore();
			clock.restore();
			refresh.restore();
		}
	});

	it("keeps refresh backoff across staggered remote expiries", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		internals.clearTopicRootCandidateClaimTimer();
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			toFake: ["Date", "clearTimeout", "setTimeout"],
		});
		let monotonicNow = 0;
		const monotonic = sinon
			.stub(performance, "now")
			.callsFake(() => monotonicNow);
		internals.localSignedTopicRootCandidateClaim = {
			acceptUntil: 60_000,
			bytes: new Uint8Array([1]),
			timestamp: BigInt(Date.now() - 40_000),
			expires: BigInt(Date.now() + 60_000),
		};
		const remoteOrigins = [100, 200].map((expiresInMs) => {
			const origin = canonicalCandidate(`staggered-expiry-${expiresInMs}`);
			internals.signedTopicRootCandidateClaims.set(origin, {
				acceptUntil: 60_000,
				bytes: new Uint8Array([expiresInMs]),
				timestamp: BigInt(Date.now()),
				expires: BigInt(Date.now() + expiresInMs),
			});
			return origin;
		});
		const refresh = sinon
			.stub(internals, "refreshLocalTopicRootCandidateClaim")
			.resolves(false);

		try {
			internals.scheduleTopicRootCandidateClaimMaintenance();
			await clock.tickAsync(0);
			expect(refresh.callCount).to.equal(1);

			monotonicNow = 100;
			await clock.tickAsync(100);
			expect(
				internals.signedTopicRootCandidateClaims.has(remoteOrigins[0]),
			).to.equal(false);
			expect(refresh.callCount).to.equal(1);

			monotonicNow = 200;
			await clock.tickAsync(100);
			expect(
				internals.signedTopicRootCandidateClaims.has(remoteOrigins[1]),
			).to.equal(false);
			expect(refresh.callCount).to.equal(1);

			monotonicNow = 999;
			await clock.tickAsync(799);
			expect(refresh.callCount).to.equal(1);
			monotonicNow = 1_000;
			await clock.tickAsync(1);
			expect(refresh.callCount).to.equal(2);
		} finally {
			internals.clearTopicRootCandidateClaimTimer();
			internals.clearAutoTopicRootCandidateUpdateSchedule();
			refresh.restore();
			monotonic.restore();
			clock.restore();
		}
	});

	it("prunes remote expiry while local refresh is unresolved", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		internals.clearTopicRootCandidateClaimTimer();
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			toFake: ["Date", "clearTimeout", "setTimeout"],
		});
		let monotonicNow = 0;
		const monotonic = sinon
			.stub(performance, "now")
			.callsFake(() => monotonicNow);
		internals.localSignedTopicRootCandidateClaim = {
			acceptUntil: 60_000,
			bytes: new Uint8Array([1]),
			timestamp: BigInt(Date.now() - 40_000),
			expires: BigInt(Date.now() + 60_000),
		};
		const remoteOrigin = canonicalCandidate("pending-refresh-expiry");
		internals.signedTopicRootCandidateClaims.set(remoteOrigin, {
			acceptUntil: 60_000,
			bytes: new Uint8Array([2]),
			timestamp: BigInt(Date.now()),
			expires: BigInt(Date.now() + 100),
		});
		let resolveRefresh!: (refreshed: boolean) => void;
		const pendingRefresh = new Promise<boolean>((resolve) => {
			resolveRefresh = resolve;
		});
		const refresh = sinon
			.stub(internals, "refreshLocalTopicRootCandidateClaim")
			.returns(pendingRefresh);

		try {
			internals.scheduleTopicRootCandidateClaimMaintenance();
			await clock.tickAsync(0);
			expect(refresh.callCount).to.equal(1);
			const task = internals.topicRootCandidateClaimMaintenanceRefreshInFlight;
			expect(task).to.not.equal(undefined);

			monotonicNow = 100;
			await clock.tickAsync(100);
			expect(
				internals.signedTopicRootCandidateClaims.has(remoteOrigin),
			).to.equal(false);
			expect(refresh.callCount).to.equal(1);

			monotonicNow = 1_100;
			await clock.tickAsync(1_000);
			expect(refresh.callCount).to.equal(1);
			resolveRefresh(false);
			await task;
		} finally {
			resolveRefresh(false);
			internals.clearTopicRootCandidateClaimTimer();
			internals.clearAutoTopicRootCandidateUpdateSchedule();
			refresh.restore();
			monotonic.restore();
			clock.restore();
		}
	});

	it("does not let a stale refresh task clobber restart scheduling", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		internals.clearTopicRootCandidateClaimTimer();
		internals.localSignedTopicRootCandidateClaim = {
			acceptUntil: performance.now() + 60_000,
			bytes: new Uint8Array([1]),
			timestamp: BigInt(Date.now() - 40_000),
			expires: BigInt(Date.now() + 60_000),
		};
		let resolveRefresh!: (refreshed: boolean) => void;
		const pendingRefresh = new Promise<boolean>((resolve) => {
			resolveRefresh = resolve;
		});
		const refresh = sinon
			.stub(internals, "refreshLocalTopicRootCandidateClaim")
			.returns(pendingRefresh);
		let refreshRestored = false;

		try {
			internals.startTopicRootCandidateClaimMaintenanceRefresh();
			const staleTask =
				internals.topicRootCandidateClaimMaintenanceRefreshInFlight;
			expect(staleTask).to.not.equal(undefined);
			await pubsub.stop();
			refresh.restore();
			refreshRestored = true;
			await pubsub.start();
			const restartTimer = internals.topicRootCandidateClaimMaintenanceTimer;
			expect(restartTimer).to.not.equal(undefined);

			resolveRefresh(false);
			await staleTask;
			expect(internals.topicRootCandidateClaimMaintenanceTimer).to.equal(
				restartTimer,
			);
			expect(internals.topicRootCandidateClaimRefreshNotBefore).to.equal(
				undefined,
			);
		} finally {
			resolveRefresh(false);
			if (!refreshRestored) refresh.restore();
		}
	});

	it("times out stalled advertisement envelope creation and allows renewal", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const pubsub = session.peers[0]!.services.pubsub;
		const target = session.peers[1]!.services.pubsub;
		const internals = pubsub as any;
		pubsub.addPeer(
			target.peerId,
			target.publicKey,
			"/peerbit/topic-control-plane/2.1.0",
			"stalled-advertisement-envelope-test",
		);
		internals.clearTopicRootCandidateClaimTimer();
		const originalCreate = internals.createMessage.bind(pubsub);
		const stalledEmbedded = await originalCreate(new Uint8Array([9]), {
			mode: new AnyWhere(),
			priority: 1,
			skipRecipientValidation: true,
		});
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			toFake: ["Date", "clearTimeout", "setTimeout"],
		});
		let monotonicNow = 0;
		const monotonic = sinon
			.stub(performance, "now")
			.callsFake(() => monotonicNow);
		const timeout = sinon
			.stub(AbortSignal, "timeout")
			.callsFake((timeoutMs) => {
				const controller = new AbortController();
				setTimeout(
					() => controller.abort(new AbortError("advertisement timed out")),
					timeoutMs,
				);
				return controller.signal;
			});
		internals.localSignedTopicRootCandidateClaim = {
			acceptUntil: 60_000,
			bytes: new Uint8Array([1]),
			timestamp: BigInt(Date.now() - 40_000),
			expires: BigInt(Date.now() + 60_000),
		};
		const refresh = sinon
			.stub(internals, "refreshLocalTopicRootCandidateClaim")
			.callsFake(async () => {
				const refreshed = {
					acceptUntil: monotonicNow + 100,
					bytes: new Uint8Array([refresh.callCount + 1]),
					timestamp: BigInt(Date.now()),
					expires: BigInt(Date.now() + 100),
				};
				internals.localSignedTopicRootCandidateClaim = refreshed;
				internals.signedTopicRootCandidateClaims.set(
					pubsub.publicKeyHash,
					refreshed,
				);
				return true;
			});
		let resolveCreation!: (message: DataMessage) => void;
		const pendingCreation = new Promise<DataMessage>((resolve) => {
			resolveCreation = resolve;
		});
		const create = sinon.stub(internals, "createMessage");
		create.onFirstCall().returns(pendingCreation);
		create
			.onSecondCall()
			.callsFake((...args: any[]) => originalCreate(...args));
		const publish = sinon.stub(internals, "publishMessageMaybe").resolves(true);

		try {
			internals.scheduleTopicRootCandidateClaimMaintenance();
			await clock.tickAsync(0);
			expect(refresh.callCount).to.equal(1);
			expect(create.callCount).to.equal(1);
			expect(publish.callCount).to.equal(0);
			expect(timeout.calledWith(10_000)).to.equal(true);
			const task = internals.topicRootCandidateClaimMaintenanceRefreshInFlight;
			expect(task).to.not.equal(undefined);

			monotonicNow = 101;
			await clock.tickAsync(101);
			expect(internals.localSignedTopicRootCandidateClaim).to.equal(undefined);
			expect(
				internals.signedTopicRootCandidateClaims.has(pubsub.publicKeyHash),
			).to.equal(false);
			expect(create.callCount).to.equal(1);
			expect(publish.callCount).to.equal(0);

			monotonicNow = 10_000;
			await clock.tickAsync(9_899);
			await task;
			expect(
				internals.topicRootCandidateClaimMaintenanceRefreshInFlight,
			).to.not.equal(task);
			expect(publish.callCount).to.equal(0);
			resolveCreation(stalledEmbedded);
			await Promise.resolve();
			expect(publish.callCount).to.equal(0);

			monotonicNow = 10_001;
			await clock.tickAsync(1);
			expect(refresh.callCount).to.equal(2);
			expect(create.callCount).to.equal(2);
			const renewalTask =
				internals.topicRootCandidateClaimMaintenanceRefreshInFlight;
			if (renewalTask) await renewalTask;
			expect(publish.callCount).to.equal(1);
			expect(
				internals.topicRootCandidateClaimMaintenanceRefreshInFlight,
			).to.equal(undefined);
		} finally {
			resolveCreation(stalledEmbedded);
			internals.clearTopicRootCandidateClaimTimer();
			internals.clearAutoTopicRootCandidateUpdateSchedule();
			publish.restore();
			create.restore();
			refresh.restore();
			timeout.restore();
			monotonic.restore();
			clock.restore();
		}
	});

	it("does not hot-retry signing when self is outside the replay-floor bound", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		internals.clearTopicRootCandidateClaimTimer();
		internals.localSignedTopicRootCandidateClaim = undefined;
		internals.signedTopicRootCandidateClaims.clear();
		internals.topicRootCandidateClaimReplayFloors.clear();
		const floorOrigins = Array.from(
			{ length: TOPIC_ROOT_CANDIDATES_MAX },
			(_, index) => `\0floor-${index}`,
		);
		for (const origin of floorOrigins) {
			internals.topicRootCandidateClaimReplayFloors.set(origin, 0n);
		}
		expect(
			internals.canRetainTopicRootCandidateClaimOrigin(pubsub.publicKeyHash),
		).to.equal(false);
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			toFake: ["Date", "clearTimeout", "setTimeout"],
		});
		const remoteOrigin = floorOrigins[0]!;
		internals.signedTopicRootCandidateClaims.set(remoteOrigin, {
			acceptUntil: performance.now() + 10_000,
			bytes: new Uint8Array([1]),
			timestamp: BigInt(Date.now()),
			expires: BigInt(Date.now() + 100),
		});
		const create = sinon
			.stub(internals, "createMessage")
			.rejects(new Error("must not sign"));

		try {
			expect(await internals.refreshLocalTopicRootCandidateClaim()).to.equal(
				false,
			);
			internals.scheduleTopicRootCandidateClaimMaintenance();
			await clock.tickAsync(99);
			expect(
				internals.signedTopicRootCandidateClaims.has(remoteOrigin),
			).to.equal(true);
			await clock.tickAsync(1);
			expect(
				internals.signedTopicRootCandidateClaims.has(remoteOrigin),
			).to.equal(false);
			await clock.tickAsync(1_000);
			expect(create.called).to.equal(false);
		} finally {
			internals.clearTopicRootCandidateClaimTimer();
			internals.clearAutoTopicRootCandidateUpdateSchedule();
			create.restore();
			clock.restore();
		}
	});

	it("does not let refresh backoff delay a remote expiry", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		internals.clearTopicRootCandidateClaimTimer();
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			toFake: ["Date", "clearTimeout", "setTimeout"],
		});
		const remoteOrigin = canonicalCandidate("refresh-backoff-remote-expiry");
		internals.localSignedTopicRootCandidateClaim = {
			acceptUntil: performance.now() + 60_000,
			bytes: new Uint8Array([1]),
			timestamp: BigInt(Date.now() - 40_000),
			expires: BigInt(Date.now() + 60_000),
		};
		internals.signedTopicRootCandidateClaims.set(remoteOrigin, {
			acceptUntil: performance.now() + 100,
			bytes: new Uint8Array([2]),
			timestamp: BigInt(Date.now() - 1_000),
			expires: BigInt(Date.now() + 100),
		});
		internals.rebuildAutoTopicRootCandidatesFromClaims();
		const refresh = sinon
			.stub(internals, "refreshLocalTopicRootCandidateClaim")
			.resolves(false);

		try {
			internals.scheduleTopicRootCandidateClaimMaintenance();
			await clock.tickAsync(0);
			expect(refresh.callCount).to.equal(1);
			await clock.tickAsync(100);
			expect(
				internals.signedTopicRootCandidateClaims.has(remoteOrigin),
			).to.equal(false);
		} finally {
			internals.clearTopicRootCandidateClaimTimer();
			internals.clearAutoTopicRootCandidateUpdateSchedule();
			clock.restore();
			refresh.restore();
		}
	});

	it("bounds claim verification globally and across reconnects", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const receiver = session.peers[0]!.services.pubsub;
		const sender = session.peers[1]!.services.pubsub;
		const internals = receiver as any;
		receiver.addPeer(
			sender.peerId,
			sender.publicKey,
			"/peerbit/topic-control-plane/2.1.0",
			"verification-budget-test",
		);
		for (let index = 0; index < 256; index++) {
			expect(
				internals.consumeTopicRootCandidateClaimVerifyBudget(
					sender.publicKeyHash,
				),
			).to.equal(true);
		}
		expect(
			internals.consumeTopicRootCandidateClaimVerifyBudget(
				sender.publicKeyHash,
			),
		).to.equal(false);
		await internals._removePeer(sender.publicKey);
		receiver.addPeer(
			sender.peerId,
			sender.publicKey,
			"/peerbit/topic-control-plane/2.1.0",
			"verification-budget-reconnect-test",
		);
		expect(
			internals.consumeTopicRootCandidateClaimVerifyBudget(
				sender.publicKeyHash,
			),
		).to.equal(false);

		internals.topicRootCandidateClaimVerifyBudgets.clear();
		internals.topicRootCandidateClaimGlobalVerifyBudget = {
			remaining: 512,
			refilledAt: Date.now(),
		};
		for (let index = 0; index < 512; index++) {
			expect(
				internals.consumeTopicRootCandidateClaimVerifyBudget(
					canonicalCandidate(`global-budget-${index}`),
				),
			).to.equal(true);
		}
		expect(
			internals.consumeTopicRootCandidateClaimVerifyBudget(
				canonicalCandidate("global-budget-exhausted"),
			),
		).to.equal(false);
	});

	it("clears a pending peer root query immediately when cancelled", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const send = sinon.stub(internals, "sendDirectControlMessage").resolves();
		const abortController = new AbortController();
		const reason = new AbortError("root query cancelled");
		const querying = internals.queryTopicRootFromPeer(
			{ publicKey: pubsub.publicKey },
			"/peerbit/pubsub-shard/1/cancelled-query",
			10_000,
			abortController.signal,
		);

		await waitForResolved(
			() => expect(internals.pendingTopicRootQueries.size).to.equal(1),
			{ timeout: 1_000, delayInterval: 10 },
		);
		abortController.abort(reason);
		await expect(querying).to.be.rejectedWith(reason);
		expect(internals.pendingTopicRootQueries.size).to.equal(0);
		expect(send.calledOnce).to.equal(true);
	});

	it("uses an earlier live root responder without waiting for a silent peer", async function () {
		this.timeout(3_000);
		session = await createDisconnectedSessionWithPerPeerRoots(3);
		const pubsub = session.peers[0]!.services.pubsub;
		const silent = session.peers[1]!.services.pubsub;
		const live = session.peers[2]!.services.pubsub;
		const internals = pubsub as any;
		const topic = "concurrent-live-topic-root";
		const resolvedRoot = live.publicKeyHash;
		const trackers = [
			{ publicKey: live.publicKey },
			{ publicKey: silent.publicKey },
		];
		sinon.stub(internals, "getConnectedTopicRootTrackers").returns(trackers);
		const send = sinon
			.stub(internals, "sendDirectControlMessage")
			.callsFake(async (...args: unknown[]) => {
				const peer = args[0] as { publicKey: { hashcode(): string } };
				const query = args[1] as TopicRootQuery;
				if (peer.publicKey.hashcode() !== live.publicKeyHash) return;
				expect(
					internals.resolvePendingTopicRootQuery(
						new TopicRootQueryResponse({
							requestId: query.requestId,
							root: resolvedRoot,
							topic,
						}),
						live.publicKeyHash,
					),
				).to.equal(true);
			});

		const started = performance.now();
		expect(await internals.resolveTopicRootState(topic)).to.deep.equal({
			authoritative: true,
			root: resolvedRoot,
		});
		expect(performance.now() - started).to.be.lessThan(500);
		expect(
			send.getCalls().map((call) => call.args[0].publicKey.hashcode()),
		).to.deep.equal([live.publicKeyHash, silent.publicKeyHash]);
		expect(internals.pendingTopicRootQueries.size).to.equal(0);
	});

	it("preserves sorted-peer precedence when a later conflicting reply is faster", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(3);
		const pubsub = session.peers[0]!.services.pubsub;
		const ordered = session.peers
			.slice(1)
			.map((node) => node.services.pubsub)
			.sort((left, right) =>
				left.publicKeyHash < right.publicKeyHash
					? -1
					: left.publicKeyHash > right.publicKeyHash
						? 1
						: 0,
			);
		const earlier = ordered[0]!;
		const later = ordered[1]!;
		const internals = pubsub as any;
		const topic = "ordered-conflicting-topic-root";
		let earlierQuery: TopicRootQuery | undefined;
		sinon
			.stub(internals, "getConnectedTopicRootTrackers")
			.returns([
				{ publicKey: earlier.publicKey },
				{ publicKey: later.publicKey },
			]);
		sinon
			.stub(internals, "sendDirectControlMessage")
			.callsFake(async (...args: unknown[]) => {
				const peer = args[0] as { publicKey: { hashcode(): string } };
				const query = args[1] as TopicRootQuery;
				if (peer.publicKey.hashcode() === earlier.publicKeyHash) {
					earlierQuery = query;
					return;
				}
				expect(
					internals.resolvePendingTopicRootQuery(
						new TopicRootQueryResponse({
							requestId: query.requestId,
							root: later.publicKeyHash,
							topic,
						}),
						later.publicKeyHash,
					),
				).to.equal(true);
			});
		let settled = false;
		const resolving = internals.resolveTopicRootState(topic).then(
			(state: unknown) => {
				settled = true;
				return state;
			},
			(error: unknown) => {
				settled = true;
				throw error;
			},
		);

		await waitForResolved(
			() => {
				expect(earlierQuery).to.not.equal(undefined);
				expect(internals.pendingTopicRootQueries.size).to.equal(1);
			},
			{ timeout: 1_000, delayInterval: 10 },
		);
		await delay(0);
		expect(settled).to.equal(false);
		expect(
			internals.resolvePendingTopicRootQuery(
				new TopicRootQueryResponse({
					requestId: earlierQuery!.requestId,
					root: earlier.publicKeyHash,
					topic,
				}),
				earlier.publicKeyHash,
			),
		).to.equal(true);

		expect(await resolving).to.deep.equal({
			authoritative: true,
			root: earlier.publicKeyHash,
		});
		expect(internals.pendingTopicRootQueries.size).to.equal(0);
	});

	it("bounds a stalled earlier send before using a later live reply", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(3);
		const pubsub = session.peers[0]!.services.pubsub;
		const ordered = session.peers
			.slice(1)
			.map((node) => node.services.pubsub)
			.sort((left, right) =>
				left.publicKeyHash < right.publicKeyHash
					? -1
					: left.publicKeyHash > right.publicKeyHash
						? 1
						: 0,
			);
		const earlier = ordered[0]!;
		const later = ordered[1]!;
		const internals = pubsub as any;
		internals.clearTopicRootCandidateClaimTimer();
		internals.clearAutoTopicRootCandidateUpdateSchedule();
		const topic = "stalled-send-ordered-topic-root";
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			toFake: ["Date", "clearTimeout", "setTimeout"],
		});
		const startedAt = Date.now();
		let stalledSendSignal: AbortSignal | undefined;
		sinon
			.stub(internals, "getConnectedTopicRootTrackers")
			.returns([
				{ publicKey: earlier.publicKey },
				{ publicKey: later.publicKey },
			]);
		const send = sinon
			.stub(internals, "sendDirectControlMessage")
			.callsFake(async (...args: unknown[]) => {
				const peer = args[0] as { publicKey: { hashcode(): string } };
				const query = args[1] as TopicRootQuery;
				if (peer.publicKey.hashcode() === earlier.publicKeyHash) {
					stalledSendSignal = args[2] as AbortSignal;
					return new Promise<void>(() => {});
				}
				expect(
					internals.resolvePendingTopicRootQuery(
						new TopicRootQueryResponse({
							requestId: query.requestId,
							root: later.publicKeyHash,
							topic,
						}),
						later.publicKeyHash,
					),
				).to.equal(true);
			});

		try {
			let settledAt: number | undefined;
			const resolving = internals
				.resolveTopicRootState(topic)
				.then((state: unknown) => {
					settledAt = Date.now();
					return state;
				});
			await clock.tickAsync(3_001);

			expect(await resolving).to.deep.equal({
				authoritative: true,
				root: later.publicKeyHash,
			});
			expect(settledAt! - startedAt).to.be.at.most(3_001);
			expect(send.callCount).to.equal(2);
			expect(stalledSendSignal?.aborted).to.equal(true);
			expect(internals.pendingTopicRootQueries.size).to.equal(0);
		} finally {
			clock.restore();
		}
	});

	it("bounds all-silent root retries with one peer-query deadline", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(3);
		const pubsub = session.peers[0]!.services.pubsub;
		const firstSilent = session.peers[1]!.services.pubsub;
		const secondSilent = session.peers[2]!.services.pubsub;
		const internals = pubsub as any;
		internals.clearTopicRootCandidateClaimTimer();
		internals.clearAutoTopicRootCandidateUpdateSchedule();
		sinon
			.stub(internals, "getConnectedTopicRootTrackers")
			.returns([
				{ publicKey: firstSilent.publicKey },
				{ publicKey: secondSilent.publicKey },
			]);
		const send = sinon.stub(internals, "sendDirectControlMessage").resolves();
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			toFake: ["Date", "clearTimeout", "setTimeout"],
		});

		try {
			const startedAt = Date.now();
			let settledAt: number | undefined;
			const resolving = internals
				.resolveTopicRootState("all-silent-topic-root")
				.then((state: unknown) => {
					settledAt = Date.now();
					return state;
				});
			await clock.tickAsync(12_001);

			expect(await resolving).to.deep.equal({
				authoritative: false,
				root: pubsub.publicKeyHash,
			});
			expect(settledAt! - startedAt).to.be.at.most(12_001);
			expect(send.callCount).to.equal(8);
			expect(internals.pendingTopicRootQueries.size).to.equal(0);
		} finally {
			clock.restore();
		}
	});

	it("keeps retrying when a peer becomes ready after 750ms", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(2);
		const pubsub = session.peers[0]!.services.pubsub;
		const warming = session.peers[1]!.services.pubsub;
		const internals = pubsub as any;
		internals.clearTopicRootCandidateClaimTimer();
		internals.clearAutoTopicRootCandidateUpdateSchedule();
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			toFake: ["Date", "clearTimeout", "setTimeout"],
		});
		const startedAt = Date.now();
		const trackers = sinon
			.stub(internals, "getConnectedTopicRootTrackers")
			.returns([{ publicKey: warming.publicKey }]);
		const query = sinon
			.stub(internals, "queryTopicRootFromPeer")
			.callsFake(async () =>
				Date.now() - startedAt > 750 ? warming.publicKeyHash : undefined,
			);

		try {
			const resolving = internals.resolveTopicRootState(
				"late-ready-topic-root",
			);
			await clock.tickAsync(1_051);
			expect(await resolving).to.deep.equal({
				authoritative: true,
				root: warming.publicKeyHash,
			});
			expect(query.callCount).to.equal(5);
			expect(trackers.callCount).to.be.greaterThan(5);
		} finally {
			clock.restore();
		}
	});

	it("preserves caller cancellation across a concurrent root-query round", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(3);
		const pubsub = session.peers[0]!.services.pubsub;
		const firstSilent = session.peers[1]!.services.pubsub;
		const secondSilent = session.peers[2]!.services.pubsub;
		const internals = pubsub as any;
		sinon
			.stub(internals, "getConnectedTopicRootTrackers")
			.returns([
				{ publicKey: firstSilent.publicKey },
				{ publicKey: secondSilent.publicKey },
			]);
		const send = sinon.stub(internals, "sendDirectControlMessage").resolves();
		const abortController = new AbortController();
		const reason = new Error("cancel concurrent root lookup");
		const outcome = pubsub
			.resolveTopicRoot("caller-cancelled-peer-round", {
				signal: abortController.signal,
			})
			.then(
				(value): { value: string | undefined; error?: never } => ({ value }),
				(error: unknown): { value?: never; error: unknown } => ({ error }),
			);

		await waitForResolved(
			() => expect(internals.pendingTopicRootQueries.size).to.equal(2),
			{ timeout: 1_000, delayInterval: 10 },
		);
		abortController.abort(reason);
		const result = await outcome;
		expect(result.value).to.equal(undefined);
		expect(result.error).to.equal(reason);
		expect(send.callCount).to.equal(2);
		expect(internals.pendingTopicRootQueries.size).to.equal(0);
	});

	it("aborts a hanging resolver when the control plane stops", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const resolutionEntered = deferred();
		let resolverSignal: AbortSignal | undefined;
		pubsub.topicRootControlPlane.setTopicRootResolver((_topic, options) => {
			resolverSignal = options?.signal;
			resolutionEntered.resolve();
			return new Promise<string | undefined>(() => {});
		});
		const opening = internals
			.ensureFanoutChannel("/peerbit/pubsub-shard/1/hanging-resolver")
			.then(
				(): undefined => undefined,
				(error: unknown) => error,
			);

		await resolutionEntered.promise;
		expect(resolverSignal?.aborted).to.equal(false);
		await Promise.race([
			pubsub.stop(),
			delay(1_000).then(() => {
				throw new Error("control-plane stop remained blocked");
			}),
		]);
		expect(await opening).to.be.instanceOf(Error);
		expect(resolverSignal?.aborted).to.equal(true);
		expect(internals.ensureFanoutChannelInFlight.size).to.equal(0);
	});

	it("aborts stale root resolution when candidates change", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1);
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const firstResolutionEntered = deferred();
		let firstSignal: AbortSignal | undefined;
		let resolutionCalls = 0;
		pubsub.topicRootControlPlane.setTopicRootResolver((_topic, options) => {
			resolutionCalls += 1;
			if (resolutionCalls === 1) {
				firstSignal = options?.signal;
				firstResolutionEntered.resolve();
				return new Promise<string | undefined>(() => {});
			}
			return pubsub.publicKeyHash;
		});
		internals.hostShardRootsNow = async () => {};
		internals.reconcileShardOverlays = async () => {};
		const shardTopic = "/peerbit/pubsub-shard/1/candidate-cancel";
		const opening = internals.ensureFanoutChannel(shardTopic);

		await firstResolutionEntered.promise;
		const currentCandidates =
			pubsub.topicRootControlPlane.getTopicRootCandidates();
		pubsub.topicRootControlPlane.setTopicRootCandidates([
			...currentCandidates,
			canonicalCandidate("cancel-stale-resolution"),
		]);
		internals.scheduleReconcileShardOverlays();

		await Promise.race([
			opening,
			delay(2_000).then(() => {
				throw new Error("candidate change did not restart root resolution");
			}),
		]);
		expect(firstSignal?.aborted).to.equal(true);
		expect(resolutionCalls).to.equal(2);
		expect(internals.ensureFanoutChannelInFlight.size).to.equal(0);
	});

	it("preserves caller abort reasons and local lookup after stop", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1, {
			pubsub: { shardCount: 1 },
		});
		const pubsub = session.peers[0]!.services.pubsub;
		const entered = deferred();
		let resolverSignal: AbortSignal | undefined;
		pubsub.topicRootControlPlane.setTopicRootResolver((_topic, options) => {
			resolverSignal = options?.signal;
			entered.resolve();
			return new Promise<string | undefined>(() => {});
		});
		const abortController = new AbortController();
		const reason = new Error("caller cancelled exact root lookup");
		const resolving = pubsub.resolveTopicRoot("caller-cancelled", {
			signal: abortController.signal,
		});

		await entered.promise;
		abortController.abort(reason);
		await expect(resolving).to.be.rejectedWith(reason);
		expect(resolverSignal?.reason).to.equal(reason);

		const lifecycleEntered = deferred();
		let lifecycleSignal: AbortSignal | undefined;
		pubsub.topicRootControlPlane.setTopicRootResolver((_topic, options) => {
			lifecycleSignal = options?.signal;
			lifecycleEntered.resolve();
			return new Promise<string | undefined>(() => {});
		});
		const lifecycleResolving = pubsub.resolveTopicRoot("stop-cancelled");
		await lifecycleEntered.promise;
		await pubsub.stop();
		await expect(lifecycleResolving).to.be.rejectedWith(
			"topic control plane stopped",
		);
		expect(lifecycleSignal?.aborted).to.equal(true);

		pubsub.topicRootControlPlane.setTopicRootResolver(undefined);
		expect(await pubsub.resolveTopicRoot("stopped-local-lookup")).to.equal(
			pubsub.publicKeyHash,
		);
	});

	it("handles a pre-aborted retry delay without an unhandled rejection", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1, {
			pubsub: { shardCount: 1 },
		});
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const abortController = new AbortController();
		const reason = new Error("abort immediately before root retry delay");
		const unhandled: unknown[] = [];
		const onUnhandled = (error: unknown) => unhandled.push(error);
		sinon
			.stub(internals, "getConnectedTopicRootTrackers")
			.returns([{ publicKey: pubsub.publicKey }]);
		sinon
			.stub(internals, "resolveTopicRootThroughPeers")
			.callsFake(async () => {
				abortController.abort(reason);
				return undefined;
			});
		process.on("unhandledRejection", onUnhandled);

		try {
			await expect(
				pubsub.resolveTopicRoot("pre-aborted-retry-delay", {
					signal: abortController.signal,
				}),
			).to.be.rejectedWith(reason);
			await delay(0);
			expect(unhandled).to.deep.equal([]);
		} finally {
			process.removeListener("unhandledRejection", onUnhandled);
		}
	});

	it("restarts a queryable resolver when candidates change", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1, {
			pubsub: { shardCount: 1 },
		});
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const entered = deferred();
		const nextRoot = canonicalCandidate("queryable-resolution-next-root");
		let firstSignal: AbortSignal | undefined;
		let calls = 0;
		pubsub.topicRootControlPlane.setTopicRootResolver((_topic, options) => {
			calls += 1;
			if (calls === 1) {
				firstSignal = options?.signal;
				entered.resolve();
				return new Promise<string | undefined>(() => {});
			}
			return nextRoot;
		});
		internals.scheduleHostOwnedShardRoots = () => {};
		internals.reconcileShardOverlays = async () => {};
		const resolving = internals.resolveQueryableTopicRoot(
			"/peerbit/pubsub-shard/1/queryable-candidate-change",
		);

		await entered.promise;
		pubsub.setTopicRootCandidates([
			pubsub.publicKeyHash,
			canonicalCandidate("queryable-candidate-change"),
		]);

		expect(await resolving).to.equal(nextRoot);
		expect(firstSignal?.aborted).to.equal(true);
		expect(calls).to.equal(2);
	});

	it("restarts host resolution when candidates change", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1, {
			pubsub: { shardCount: 1 },
		});
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const entered = deferred();
		let firstSignal: AbortSignal | undefined;
		let calls = 0;
		pubsub.topicRootControlPlane.setTopicRootResolver((_topic, options) => {
			calls += 1;
			if (calls === 1) {
				firstSignal = options?.signal;
				entered.resolve();
				return new Promise<string | undefined>(() => {});
			}
			return pubsub.publicKeyHash;
		});
		internals.shardRootCache.clear();
		internals.scheduleHostOwnedShardRoots = () => {};
		internals.reconcileShardOverlays = async () => {};
		const ensure = sinon.stub(internals, "ensureFanoutChannel").resolves();
		const hosting = pubsub.hostShardRootsNow();

		await entered.promise;
		pubsub.setTopicRootCandidates([
			pubsub.publicKeyHash,
			canonicalCandidate("host-candidate-change"),
		]);

		await hosting;
		expect(firstSignal?.aborted).to.equal(true);
		expect(calls).to.equal(2);
		expect(ensure.calledOnce).to.equal(true);
	});

	it("does not multiply proactive host scans by silent connected peers", async function () {
		this.timeout(5_000);
		const shardCount = 4;
		session = await createDisconnectedSessionWithPerPeerRoots(3, {
			pubsub: { shardCount },
		});
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		internals.clearTopicRootCandidateClaimTimer();
		internals.clearAutoTopicRootCandidateUpdateSchedule();
		const orderedTrackers = session.peers
			.slice(1)
			.map((peer) => ({ publicKey: peer.services.pubsub.publicKey }))
			.sort((left, right) =>
				left.publicKey.hashcode() < right.publicKey.hashcode()
					? -1
					: left.publicKey.hashcode() > right.publicKey.hashcode()
						? 1
						: 0,
			);
		const liveLaterHash = orderedTrackers[1]!.publicKey.hashcode();
		internals.shardRootCache.set("/peerbit/pubsub-shard/1/0", {
			root: liveLaterHash,
			authoritative: true,
		});
		const connectedTrackers = sinon
			.stub(internals, "getConnectedTopicRootTrackers")
			.returns(orderedTrackers);
		const send = sinon
			.stub(internals, "sendDirectControlMessage")
			.callsFake(async (...args: unknown[]) => {
				const peer = args[0] as { publicKey: { hashcode(): string } };
				if (peer.publicKey.hashcode() !== liveLaterHash) return;
				const query = args[1] as TopicRootQuery;
				internals.resolvePendingTopicRootQuery(
					new TopicRootQueryResponse({
						requestId: query.requestId,
						root: pubsub.publicKeyHash,
						topic: query.topic,
					}),
					liveLaterHash,
				);
			});
		const ensure = sinon.stub(internals, "ensureFanoutChannel").resolves();
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			toFake: ["Date", "clearTimeout", "setTimeout"],
		});

		try {
			let settled = false;
			let rejection: unknown;
			const hosting = pubsub.hostShardRootsNow().then(
				() => {
					settled = true;
				},
				(error) => {
					settled = true;
					rejection = error;
				},
			);
			await clock.tickAsync(1);
			const settledWithoutPeerRound = settled;
			if (!settled) {
				// Let the pre-fix serial 3s-per-shard behavior drain before asserting.
				await clock.tickAsync(20_000);
			}
			await hosting;
			if (rejection) throw rejection;

			expect(settledWithoutPeerRound).to.equal(true);
			expect(connectedTrackers.callCount).to.equal(0);
			expect(send.callCount).to.equal(0);
			expect(ensure.callCount).to.equal(shardCount);
			expect(internals.pendingTopicRootQueries.size).to.equal(0);
		} finally {
			clock.restore();
		}
	});

	it("shares one deadline across concurrent proactive resolver scans", async function () {
		this.timeout(5_000);
		const shardCount = 16;
		session = await createDisconnectedSessionWithPerPeerRoots(1, {
			pubsub: { shardCount },
		});
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		internals.clearTopicRootCandidateClaimTimer();
		internals.clearAutoTopicRootCandidateUpdateSchedule();
		const resolverSignals: AbortSignal[] = [];
		pubsub.topicRootControlPlane.setTopicRootResolver((_topic, options) => {
			if (options?.signal) resolverSignals.push(options.signal);
			return new Promise<string | undefined>(() => {});
		});
		const ensure = sinon.stub(internals, "ensureFanoutChannel").resolves();
		const clock = sinon.useFakeTimers({
			now: Date.now(),
			toFake: ["Date", "clearTimeout", "setTimeout"],
		});

		try {
			const startedAt = Date.now();
			let settledAt: number | undefined;
			const outcome = pubsub.hostShardRootsNow().then(
				(): { error: unknown } => ({ error: undefined }),
				(error: unknown) => {
					settledAt = Date.now();
					return { error };
				},
			);
			await clock.tickAsync(12_001);
			const { error } = await outcome;

			expect(error).to.be.instanceOf(AbortError);
			expect((error as Error).message).to.equal(
				"topic root host scan timed out after 12000ms",
			);
			expect(settledAt! - startedAt).to.be.at.most(12_001);
			expect(resolverSignals.length).to.be.greaterThan(1);
			expect(resolverSignals.length).to.be.lessThan(shardCount);
			expect(resolverSignals.every((signal) => signal.aborted)).to.equal(true);
			expect(ensure.callCount).to.equal(0);
		} finally {
			clock.restore();
		}
	});

	it("cancels sibling host scans and drains started joins on failure", async () => {
		session = await createDisconnectedSessionWithPerPeerRoots(1, {
			pubsub: { shardCount: 3 },
		});
		const pubsub = session.peers[0]!.services.pubsub;
		const internals = pubsub as any;
		const candidateGeneration = internals.getTopicRootCandidateGeneration();
		const scanError = new Error("later shard resolution failed");
		const joinError = new Error("earlier shard join failed");
		const siblingEntered = deferred();
		const siblingCancelled = deferred();
		const joinStarted = deferred();
		let siblingAbortReason: unknown;
		let rejectJoin!: (error: unknown) => void;
		const join = new Promise<void>((_resolve, reject) => {
			rejectJoin = reject;
		});
		sinon
			.stub(internals, "resolveShardRootState")
			.callsFake(async (...args: unknown[]) => {
				const shardTopic = args[0] as string;
				const options = args[1] as { signal?: AbortSignal } | undefined;
				if (shardTopic.endsWith("/0")) {
					return { root: pubsub.publicKeyHash, candidateGeneration };
				}
				if (shardTopic.endsWith("/1")) {
					const signal = options?.signal;
					siblingEntered.resolve();
					return new Promise((_resolve, reject) => {
						const onAbort = () => {
							siblingAbortReason = signal?.reason;
							siblingCancelled.resolve();
							reject(signal?.reason);
						};
						if (signal?.aborted) onAbort();
						else signal?.addEventListener("abort", onAbort, { once: true });
					});
				}
				await joinStarted.promise;
				throw scanError;
			});
		sinon.stub(internals, "ensureFanoutChannel").callsFake(() => {
			joinStarted.resolve();
			return join;
		});

		let settled = false;
		const hosting = pubsub.hostShardRootsNow().finally(() => {
			settled = true;
		});
		await siblingEntered.promise;
		await siblingCancelled.promise;
		expect(siblingAbortReason).to.equal(scanError);
		await delay(0);
		expect(settled).to.equal(false);
		rejectJoin(joinError);
		await expect(hosting).to.be.rejectedWith(scanError);
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

	it("exposes the signed transport generation on subscribe events", async () => {
		const TOPIC = "subscribe-event-transport-session";
		session = await createDisconnectedSession(2);

		const a = session.peers[0]!.services.pubsub;
		const b = session.peers[1]!.services.pubsub;
		await session.connect([[session.peers[0], session.peers[1]]]);
		await waitForNeighbour(a, b);
		await b.subscribe(TOPIC);

		let observedSession: bigint | undefined;
		b.addEventListener("subscribe", (event) => {
			if (event.detail.from.hashcode() === a.publicKeyHash) {
				observedSession = event.detail.session;
			}
		});
		await a.subscribe(TOPIC);

		await waitForResolved(() => {
			const recordedSession = b.topics
				.get(TOPIC)
				?.get(a.publicKeyHash)?.session;
			expect(observedSession).to.equal(recordedSession);
			expect(observedSession).to.not.equal(undefined);
		});
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

	it("opens through a live donor after its directly connected root departs", async function () {
		this.timeout(20_000);
		const shardCount = 64;
		session = await createDisconnectedSessionWithPerPeerRoots(3, {
			pubsub: { shardCount },
		});
		const ordered = session.peers
			.map((node) => ({ node, pubsub: node.services.pubsub }))
			.sort((left, right) =>
				left.pubsub.publicKeyHash < right.pubsub.publicKeyHash
					? -1
					: left.pubsub.publicKeyHash > right.pubsub.publicKeyHash
						? 1
						: 0,
			);
		// With modulo candidate selection, the middle hash takes the same slot as
		// the departed low hash before removal and as itself after the high hash joins.
		const departed = ordered[0]!;
		const donor = ordered[1]!;
		const fresh = ordered[2]!;
		const beforeCandidates = [
			donor.pubsub.publicKeyHash,
			departed.pubsub.publicKeyHash,
		].sort();
		const afterCandidates = [
			donor.pubsub.publicKeyHash,
			fresh.pubsub.publicKeyHash,
		].sort();
		const { index: shardIndex, topic: shardTopic } =
			findCandidateTransitionTopic(
				beforeCandidates,
				afterCandidates,
				departed.pubsub.publicKeyHash,
				donor.pubsub.publicKeyHash,
				shardCount,
			);
		let topic = "";
		for (let index = 0; index < 10_000; index++) {
			const candidate = `departed-direct-root-fresh-join-${index}`;
			if (topicHash32(candidate) % shardCount === shardIndex) {
				topic = candidate;
				break;
			}
		}
		expect(topic).to.not.equal("");

		await departed.node.dial(donor.node.getMultiaddrs()[0]!);
		await waitForNeighbour(departed.pubsub, donor.pubsub);
		await waitForResolved(
			() => {
				expect(
					donor.pubsub.topicRootControlPlane.getTopicRootCandidates(),
				).to.deep.equal(beforeCandidates);
				expect(
					departed.pubsub.topicRootControlPlane.getTopicRootCandidates(),
				).to.deep.equal(beforeCandidates);
			},
			{ timeout: 5_000, delayInterval: 25 },
		);
		await Promise.all([
			donor.pubsub.subscribe(topic),
			departed.pubsub.subscribe(topic),
		]);
		const donorInternals = donor.pubsub as any;
		await waitForResolved(
			() => {
				expect(donorInternals.fanoutChannels.get(shardTopic)?.root).to.equal(
					departed.pubsub.publicKeyHash,
				);
			},
			{ timeout: 5_000, delayInterval: 25 },
		);
		expect(
			(await donorInternals.resolveShardRootState(shardTopic)).root,
		).to.equal(departed.pubsub.publicKeyHash);
		const staleGeneration = donorInternals.getTopicRootCandidateGeneration();
		const replayFloor = donorInternals.topicRootCandidateClaimReplayFloors.get(
			departed.pubsub.publicKeyHash,
		);

		const departureStarted = performance.now();
		await departed.node.stop();
		await waitForResolved(
			() => {
				expect(
					donor.pubsub.topicRootControlPlane.getTopicRootCandidates(),
				).to.deep.equal([donor.pubsub.publicKeyHash]);
			},
			{ timeout: 5_000, delayInterval: 10 },
		);
		expect(performance.now() - departureStarted).to.be.lessThan(5_000);
		expect(donorInternals.getTopicRootCandidateGeneration()).to.not.equal(
			staleGeneration,
		);
		expect(donorInternals.shardRootCache.get(shardTopic)?.root).to.not.equal(
			departed.pubsub.publicKeyHash,
		);
		expect(
			(await donorInternals.resolveShardRootState(shardTopic)).root,
		).to.equal(donor.pubsub.publicKeyHash);
		expect(
			donorInternals.topicRootCandidateClaimReplayFloors.get(
				departed.pubsub.publicKeyHash,
			),
		).to.equal(replayFloor);
		expect(
			donorInternals.signedTopicRootCandidateClaims.has(
				departed.pubsub.publicKeyHash,
			),
		).to.equal(true);

		await fresh.node.dial(donor.node.getMultiaddrs()[0]!);
		await waitForNeighbour(fresh.pubsub, donor.pubsub);
		await waitForResolved(
			() => {
				for (const peer of [donor.pubsub, fresh.pubsub]) {
					expect(
						peer.topicRootControlPlane.getTopicRootCandidates(),
					).to.deep.equal(afterCandidates);
					expect(
						peer.topicRootControlPlane.getTopicRootCandidates(),
					).to.not.include(departed.pubsub.publicKeyHash);
				}
			},
			{ timeout: 5_000, delayInterval: 25 },
		);
		const openStarted = performance.now();
		await fresh.pubsub.subscribe(topic);
		expect(performance.now() - openStarted).to.be.lessThan(5_000);
		expect((fresh.pubsub as any).fanoutChannels.get(shardTopic)?.root).to.equal(
			donor.pubsub.publicKeyHash,
		);
		expect(
			fresh.node.services.fanout.getChannelStats(
				shardTopic,
				donor.pubsub.publicKeyHash,
			)?.parent,
		).to.equal(donor.pubsub.publicKeyHash);
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
