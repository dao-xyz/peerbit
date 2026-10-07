import { getPublicKeyFromPeerId } from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import type {
	SubscriptionEvent,
	UnsubcriptionEvent,
	UnsubscriptionReason,
} from "@peerbit/pubsub-interface";
import {
	PeerUnavailable,
	Subscribe,
	SubscriptionData,
	Unsubscribe,
} from "@peerbit/pubsub-interface";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import {
	FanoutTree,
	TopicControlPlane,
	TopicRootControlPlane,
} from "../src/index.js";

describe("pubsub (unsubscribe reason)", function () {
	const createControlEnvelope = (properties: {
		publicKey: ReturnType<typeof getPublicKeyFromPeerId>;
		session: bigint;
		timestamp: bigint;
	}) =>
		({
			header: {
				session: properties.session,
				timestamp: properties.timestamp,
				signatures: {
					signatures: [{ publicKey: properties.publicKey }],
				},
			},
		}) as any;

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

	const setupTrackedSubscribers = async (
		topic: string,
		session: Awaited<ReturnType<typeof createDisconnectedSession>>,
	) => {
		const a = session.peers[0]!.services.pubsub;
		const b = session.peers[1]!.services.pubsub;

		await session.connect([[session.peers[0], session.peers[1]]]);
		await Promise.all([a.subscribe(topic), b.subscribe(topic)]);

		await waitForResolved(() => {
			const aTopics = a.topics.get(topic);
			const bTopics = b.topics.get(topic);
			expect(aTopics?.has(b.publicKeyHash)).to.equal(true);
			expect(bTopics?.has(a.publicKeyHash)).to.equal(true);
		});

		return { a, b };
	};

	const setupTrackedSubscribersViaRelay = async (
		topic: string,
		session: Awaited<ReturnType<typeof createDisconnectedSession>>,
	) => {
		const a = session.peers[0]!.services.pubsub;
		const b = session.peers[1]!.services.pubsub;
		const relay = session.peers[2]!.services.pubsub;

		await session.connect([
			[session.peers[0], session.peers[2]],
			[session.peers[1], session.peers[2]],
		]);

		const relayHash = relay.publicKeyHash;
		for (const peer of session.peers) {
			peer!.services.pubsub.setTopicRootCandidates([relayHash]);
		}
		await relay.hostShardRootsNow();

		await Promise.all([a.subscribe(topic), b.subscribe(topic)]);

		await waitForResolved(() => {
			const aTopics = a.topics.get(topic);
			const bTopics = b.topics.get(topic);
			expect(aTopics?.has(b.publicKeyHash)).to.equal(true);
			expect(bTopics?.has(a.publicKeyHash)).to.equal(true);
		});

		return { a, b };
	};

	const expectUnsubscribeEvent = async (properties: {
		events: UnsubcriptionEvent[];
		fromHash: string;
		topic: string;
		reason: UnsubscriptionReason;
	}) => {
		await waitForResolved(() => {
			const match = properties.events.find(
				(e) =>
					e.from.hashcode() === properties.fromHash &&
					e.topics.includes(properties.topic),
			);
			expect(match).to.not.equal(undefined);
			expect(match!.reason).to.equal(properties.reason);
		});
	};

	it("emits reason=remote-unsubscribe on explicit unsubscribe", async () => {
		const topic = "unsubscribe-reason-remote";
		const session = await createDisconnectedSession(2);
		try {
			const { a, b } = await setupTrackedSubscribers(topic, session);
			const events: UnsubcriptionEvent[] = [];
			const onUnsubscribe = (ev: CustomEvent<UnsubcriptionEvent>) =>
				events.push(ev.detail);

			a.addEventListener("unsubscribe", onUnsubscribe as any);
			try {
				await b.unsubscribe(topic);
				await expectUnsubscribeEvent({
					events,
					fromHash: b.publicKeyHash,
					topic,
					reason: "remote-unsubscribe",
				});
			} finally {
				a.removeEventListener("unsubscribe", onUnsubscribe as any);
			}
		} finally {
			await session.stop();
		}
	});

	it("emits reason=peer-unreachable when a tracked peer becomes unreachable", async () => {
		const topic = "unsubscribe-reason-unreachable";
		const session = await createDisconnectedSession(2);
		try {
			const { a, b } = await setupTrackedSubscribers(topic, session);
			const events: UnsubcriptionEvent[] = [];
			const onUnsubscribe = (ev: CustomEvent<UnsubcriptionEvent>) =>
				events.push(ev.detail);

			a.addEventListener("unsubscribe", onUnsubscribe as any);
			try {
				a.onPeerUnreachable(b.publicKeyHash);
				await expectUnsubscribeEvent({
					events,
					fromHash: b.publicKeyHash,
					topic,
					reason: "peer-unreachable",
				});
			} finally {
				a.removeEventListener("unsubscribe", onUnsubscribe as any);
			}
		} finally {
			await session.stop();
		}
	});

	it("emits reason=peer-unreachable for tracked relay-only subscribers", async () => {
		const topic = "unsubscribe-reason-unreachable-relay";
		const session = await createDisconnectedSession(3);
		try {
			const { a, b } = await setupTrackedSubscribersViaRelay(topic, session);
			const events: UnsubcriptionEvent[] = [];
			const onUnsubscribe = (ev: CustomEvent<UnsubcriptionEvent>) =>
				events.push(ev.detail);

			a.addEventListener("unsubscribe", onUnsubscribe as any);
			try {
				a.onPeerUnreachable(b.publicKeyHash);
				await expectUnsubscribeEvent({
					events,
					fromHash: b.publicKeyHash,
					topic,
					reason: "peer-unreachable",
				});
			} finally {
				a.removeEventListener("unsubscribe", onUnsubscribe as any);
			}
		} finally {
			await session.stop();
		}
	});

	for (const fanoutFirst of [true, false]) {
		it(`removes subscriptions when shared routes are invalidated ${fanoutFirst ? "fanout-first" : "pubsub-first"}`, async () => {
			const topics = [
				"unsubscribe-shared-routes-0",
				"unsubscribe-shared-routes-1",
			];
			const session = await createDisconnectedSession(2);
			try {
				const { a, b } = await setupTrackedSubscribers(topics[0]!, session);
				expect(a["getShardTopicForUserTopic"](topics[0]!)).not.to.equal(
					a["getShardTopicForUserTopic"](topics[1]!),
				);
				await Promise.all([a.subscribe(topics[1]!), b.subscribe(topics[1]!)]);
				await waitForResolved(() => {
					for (const [subject, remote] of [[a, b], [b, a]] as const) {
						expect([
							...(subject.peerToTopic.get(remote.publicKeyHash) ?? []),
						]).to.have.members(topics);
					}
				});
				const fanout = session.peers[0]!.services.fanout;
				expect(fanout.routes).to.equal(a.routes);
				expect(a.routes.isReachable(a.publicKeyHash, b.publicKeyHash)).to.equal(
					true,
				);
				for (const topic of topics) {
					expect(a.getSubscribers(topic)?.map((key) => key.hashcode())).to.include(
						b.publicKeyHash,
					);
				}
				const events: UnsubcriptionEvent[] = [];
				const onUnsubscribe = ({ detail }: CustomEvent<UnsubcriptionEvent>) => {
					if (detail.from.equals(b.publicKey)) events.push(detail);
				};
				a.addEventListener("unsubscribe", onUnsubscribe);
				try {
					// Drive route invalidation synchronously, before live transport traffic
					// can restore a route. Do not bypass shared routing with onPeerUnreachable.
					const services = fanoutFirst ? [fanout, a] : [a, fanout];
					for (const service of services)
						service.removePeerFromRoutes(b.publicKeyHash, true);
					expect(
						a.routes.isReachable(a.publicKeyHash, b.publicKeyHash),
					).to.equal(false);
					for (const topic of topics) {
						expect(a.topics.get(topic)?.has(b.publicKeyHash)).to.equal(false);
						expect(
							a.getSubscribers(topic)?.map((key) => key.hashcode()) ?? [],
						).not.to.include(b.publicKeyHash);
					}
					expect(a.peerToTopic.has(b.publicKeyHash)).to.equal(false);
					expect(events).to.have.length(1);
					expect(events[0]!.reason).to.equal("peer-unreachable");
					expect(events[0]!.topics).to.have.members(topics);
					for (const service of services)
						service.removePeerFromRoutes(b.publicKeyHash, true);
					expect(events).to.have.length(1);
				} finally {
					a.removeEventListener("unsubscribe", onUnsubscribe);
				}
			} finally {
				await session.stop();
			}
		});
	}

	it("removes every shard's subscriptions after a gated transport hang-up", async () => {
		const topics = [
			"unsubscribe-shared-routes-0",
			"unsubscribe-shared-routes-1",
		];
		const session = await createDisconnectedSession(2);
		let partitioned = false;
		const listeners: Array<() => void> = [];
		try {
			for (const peer of session.peers) {
				// TestSession does not forward connectionGater options. Gate its real
				// libp2p components so background discovery cannot undo the partition.
				Object.assign((peer as any).components.connectionGater, {
					denyDialPeer: () => partitioned,
					denyInboundConnection: () => partitioned,
				});
			}
			const { a, b } = await setupTrackedSubscribers(topics[0]!, session);
			expect(a["getShardTopicForUserTopic"](topics[0]!)).not.to.equal(
				a["getShardTopicForUserTopic"](topics[1]!),
			);
			await Promise.all([a.subscribe(topics[1]!), b.subscribe(topics[1]!)]);
			const directions = [[a, b], [b, a]] as const;
			const events: UnsubcriptionEvent[][] = [[], []];
			for (const [index, [subject, remote]] of directions.entries()) {
				const onUnsubscribe = ({ detail }: CustomEvent<UnsubcriptionEvent>) => {
					if (detail.from.equals(remote.publicKey)) events[index]!.push(detail);
				};
				subject.addEventListener("unsubscribe", onUnsubscribe);
				listeners.push(() =>
					subject.removeEventListener("unsubscribe", onUnsubscribe),
				);
			}
			await waitForResolved(() => {
				for (const [index, [subject, remote]] of directions.entries()) {
					expect([
						...(subject.peerToTopic.get(remote.publicKeyHash) ?? []),
					]).to.have.members(topics);
					expect(session.peers[index]!.getDialQueue()).to.have.length(0);
				}
			});
			partitioned = true;
			await Promise.all([
				session.peers[0]!.hangUp(session.peers[1]!.peerId),
				session.peers[1]!.hangUp(session.peers[0]!.peerId),
			]);
			await waitForResolved(
				() => {
					for (const [index, [subject, remote]] of directions.entries()) {
						expect(session.peers[index]!.getConnections()).to.have.length(0);
						expect(
							subject.routes.isReachable(subject.publicKeyHash, remote.publicKeyHash),
						).to.equal(false);
						expect(subject.peerToTopic.has(remote.publicKeyHash)).to.equal(false);
						for (const topic of topics) {
							expect(subject.topics.get(topic)?.has(remote.publicKeyHash)).to.equal(
								false,
							);
						}
						expect(events[index]).to.have.length(1);
						expect(events[index]![0]!.reason).to.equal("peer-unreachable");
						expect(events[index]![0]!.topics).to.have.members(topics);
					}
				},
				{ timeout: 5_000 },
			);
		} finally {
			for (const remove of listeners) remove();
			await session.stop();
		}
	});

	it("retains subscriptions while shared routes have a surviving alternative", async () => {
		const topic = "unsubscribe-shared-routes-alternative";
		const session = await createDisconnectedSession(3);
		try {
			const { a, b } = await setupTrackedSubscribersViaRelay(topic, session);
			await session.connect([[session.peers[0], session.peers[1]]]);
			const fanout = session.peers[0]!.services.fanout;
			const relay = session.peers[2]!.services.pubsub;
			expect(fanout.routes).to.equal(a.routes);
			// Preserve real subscribed peers while arranging both routes through the
			// public route-admission seam. The target key is known to both services.
			for (const service of [fanout, a]) {
				service.updateSession(b.publicKey, b.session);
				for (const nextHop of [relay.publicKeyHash, b.publicKeyHash]) {
					service.addRouteConnection(
						a.publicKeyHash,
						nextHop,
						b.publicKey,
						1,
						b.session,
						b.session,
					);
				}
			}
			expect(
				a.getRouteHints(b.publicKeyHash).map((route) => route.nextHop),
			).to.have.members([relay.publicKeyHash, b.publicKeyHash]);
			const reasons: (UnsubscriptionReason | undefined)[] = [];
			const onUnsubscribe = ({ detail }: CustomEvent<UnsubcriptionEvent>) => {
				if (detail.from.equals(b.publicKey) && detail.topics.includes(topic)) {
					reasons.push(detail.reason);
				}
			};
			a.addEventListener("unsubscribe", onUnsubscribe);
			try {
				for (const service of [fanout, a])
					service.removePeerFromRoutes(relay.publicKeyHash, true);
				expect(a.routes.isReachable(a.publicKeyHash, b.publicKeyHash)).to.equal(
					true,
				);
				expect(
					a.getSubscribers(topic)?.map((key) => key.hashcode()),
				).to.include(b.publicKeyHash);
				expect(reasons).to.deep.equal([]);
				for (const service of [fanout, a])
					service.removePeerFromRoutes(b.publicKeyHash, true);
				expect(a.routes.isReachable(a.publicKeyHash, b.publicKeyHash)).to.equal(
					false,
				);
				expect(
					a.getSubscribers(topic)?.map((key) => key.hashcode()) ?? [],
				).not.to.include(b.publicKeyHash);
				expect(reasons).to.deep.equal(["peer-unreachable"]);
			} finally {
				a.removeEventListener("unsubscribe", onUnsubscribe);
			}
		} finally {
			await session.stop();
		}
	});

	it("propagates relay-observed abrupt child loss to tracked relay-only subscribers", async () => {
		const topic = "unsubscribe-reason-unreachable-relay-propagated";
		const session = await createDisconnectedSession(3);
		try {
			const { a, b } = await setupTrackedSubscribersViaRelay(topic, session);
			const events: UnsubcriptionEvent[] = [];
			const onUnsubscribe = (ev: CustomEvent<UnsubcriptionEvent>) =>
				events.push(ev.detail);

			a.addEventListener("unsubscribe", onUnsubscribe as any);
			try {
				await session.peers[1]!.stop();
				await expectUnsubscribeEvent({
					events,
					fromHash: b.publicKeyHash,
					topic,
					reason: "peer-unreachable",
				});
			} finally {
				a.removeEventListener("unsubscribe", onUnsubscribe as any);
			}
		} finally {
			await session.stop();
		}
	});

	it("keeps a direct subscriber when a signed relay loss arrives over the shard", async () => {
		const topic = "unsubscribe-reason-relay-path-loss";
		const session = await createDisconnectedSession(3);
		try {
			const { a, b } = await setupTrackedSubscribersViaRelay(topic, session);
			const relay = session.peers[2]!.services.pubsub;
			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitForResolved(() => {
				const direct = a.peers.get(b.publicKeyHash);
				expect(direct?.isReadable && direct.isWritable).to.equal(true);
			});
			const events: UnsubcriptionEvent[] = [];
			const onUnsubscribe = (event: CustomEvent<UnsubcriptionEvent>) =>
				events.push(event.detail);
			const internals = a as any;
			const process = internals.processShardPubSubMessage.bind(a);
			let processedHint = false;
			const receive = sinon
				.stub(internals, "processShardPubSubMessage")
				.callsFake(async (input: any) => {
					await process(input);
					if (
						input.pubsubMessage instanceof PeerUnavailable &&
						input.pubsubMessage.publicKeyHash === b.publicKeyHash &&
						input.from.equals(relay.publicKey)
					) {
						processedHint = true;
					}
				});
			a.addEventListener("unsubscribe", onUnsubscribe as any);
			try {
				await (relay as any).announcePeerUnavailableOnShard(
					b.publicKeyHash,
					internals.getShardTopicForUserTopic(topic),
				);
				await waitForResolved(() => expect(processedHint).to.equal(true));
				expect(a.topics.get(topic)?.has(b.publicKeyHash)).to.equal(true);
				expect(events.filter((event) => event.from.equals(b.publicKey))).to.be
					.empty;
			} finally {
				receive.restore();
				a.removeEventListener("unsubscribe", onUnsubscribe as any);
			}
		} finally {
			await session.stop();
		}
	});

	it("emits reason=peer-session-reset on peer session updates", async () => {
		const topic = "unsubscribe-reason-session";
		const session = await createDisconnectedSession(2);
		try {
			const { a, b } = await setupTrackedSubscribers(topic, session);
			const events: UnsubcriptionEvent[] = [];
			const onUnsubscribe = (ev: CustomEvent<UnsubcriptionEvent>) =>
				events.push(ev.detail);
			const bPublicKey = getPublicKeyFromPeerId(session.peers[1]!.peerId);
			const currentSession =
				a.topics.get(topic)?.get(b.publicKeyHash)?.session ?? 0n;

			a.addEventListener("unsubscribe", onUnsubscribe as any);
			try {
				a.onPeerSession(bPublicKey, Number(currentSession + 1n));
				await expectUnsubscribeEvent({
					events,
					fromHash: b.publicKeyHash,
					topic,
					reason: "peer-session-reset",
				});
			} finally {
				a.removeEventListener("unsubscribe", onUnsubscribe as any);
			}
		} finally {
			await session.stop();
		}
	});

	it("ignores duplicate peer-session-reset for the current subscription session", async () => {
		const topic = "unsubscribe-reason-session-duplicate";
		const session = await createDisconnectedSession(2);
		try {
			const { a, b } = await setupTrackedSubscribers(topic, session);
			const events: UnsubcriptionEvent[] = [];
			const onUnsubscribe = (ev: CustomEvent<UnsubcriptionEvent>) =>
				events.push(ev.detail);
			const bPublicKey = getPublicKeyFromPeerId(session.peers[1]!.peerId);
			const currentSession =
				a.topics.get(topic)?.get(b.publicKeyHash)?.session ?? 0n;

			a.addEventListener("unsubscribe", onUnsubscribe as any);
			try {
				a.onPeerSession(bPublicKey, Number(currentSession));
				expect(a.topics.get(topic)?.has(b.publicKeyHash)).to.equal(true);
				expect(events).to.deep.equal([]);
			} finally {
				a.removeEventListener("unsubscribe", onUnsubscribe as any);
			}
		} finally {
			await session.stop();
		}
	});

	it("ignores stale old-session unsubscribe messages", async () => {
		const topic = "unsubscribe-reason-ignore-stale-old-session";
		const session = await createDisconnectedSession(2);
		try {
			const { a, b } = await setupTrackedSubscribers(topic, session);
			const bPublicKey = getPublicKeyFromPeerId(session.peers[1]!.peerId);

			a.topics.get(topic)!.set(
				b.publicKeyHash,
				new SubscriptionData({
					publicKey: bPublicKey,
					session: 2n,
					timestamp: 20n,
				}),
			);

			await (a as any).processShardPubSubMessage({
				pubsubMessage: new Unsubscribe({ topics: [topic] }),
				message: createControlEnvelope({
					publicKey: bPublicKey,
					session: 1n,
					timestamp: 30n,
				}),
				from: bPublicKey,
				shardTopic: (a as any).getShardTopicForUserTopic(topic),
			});

			expect(a.topics.get(topic)?.has(b.publicKeyHash)).to.equal(true);
		} finally {
			await session.stop();
		}
	});

	it("accepts newer-session subscribe after older-session control timestamp", async () => {
		const topic = "unsubscribe-reason-newer-subscribe-beats-older-session";
		const session = await createDisconnectedSession(2);
		try {
			const { a, b } = await setupTrackedSubscribers(topic, session);
			const bPublicKey = getPublicKeyFromPeerId(session.peers[1]!.peerId);

			a.topics.get(topic)!.delete(b.publicKeyHash);
			a.peerToTopic.delete(b.publicKeyHash);
			a.lastSubscriptionMessages.set(
				b.publicKeyHash,
				new Map([[topic, { session: 1n, timestamp: 30n }]]),
			);

			await (a as any).processShardPubSubMessage({
				pubsubMessage: new Subscribe({
					topics: [topic],
					requestSubscribers: false,
				}),
				message: createControlEnvelope({
					publicKey: bPublicKey,
					session: 2n,
					timestamp: 20n,
				}),
				from: bPublicKey,
				shardTopic: (a as any).getShardTopicForUserTopic(topic),
			});

			expect(a.topics.get(topic)?.has(b.publicKeyHash)).to.equal(true);
		} finally {
			await session.stop();
		}
	});

	it("relearns a direct peer subscriber snapshot via requestSubscribers(to)", async () => {
		const topic = "direct-request-subscribers-targeted-peer";
		const session = await createDisconnectedSession(2);
		try {
			const { a, b } = await setupTrackedSubscribers(topic, session);
			const bPublicKey = getPublicKeyFromPeerId(session.peers[1]!.peerId);
			let observedSession: bigint | undefined;
			a.addEventListener(
				"subscribe",
				(event: CustomEvent<SubscriptionEvent>) => {
					if (event.detail.from.hashcode() === b.publicKeyHash) {
						observedSession = event.detail.session;
					}
				},
			);

			a.topics.get(topic)?.delete(b.publicKeyHash);
			a.peerToTopic.delete(b.publicKeyHash);
			a.lastSubscriptionMessages.get(b.publicKeyHash)?.delete(topic);

			await a.requestSubscribers(topic, bPublicKey);

			await waitForResolved(() => {
				const recordedSession = a.topics
					.get(topic)
					?.get(b.publicKeyHash)?.session;
				expect(observedSession).to.equal(recordedSession);
				expect(observedSession).to.not.equal(undefined);
			});
		} finally {
			await session.stop();
		}
	});
});
