import { getPublicKeyFromPeerId } from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import {
	Subscribe,
	TopicRootCandidateClaims,
	Unsubscribe,
} from "@peerbit/pubsub-interface";
import { waitForNeighbour } from "@peerbit/stream";
import { AnyWhere, DataMessage } from "@peerbit/stream-interface";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { Uint8ArrayList } from "uint8arraylist";
import {
	FanoutTree,
	TopicControlPlane,
	TopicRootControlPlane,
} from "../src/index.js";

type Services = { pubsub: TopicControlPlane; fanout: FanoutTree };
type RecoveryEvent = "fanout:joined" | "fanout:child-joined";

const deferred = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => {
		resolve = done;
	});
	return { promise, resolve };
};

describe("pubsub (subscription recovery)", function () {
	this.timeout(20_000);
	let session: TestSession<Services> | undefined;

	const createSession = async (
		count: number,
		shardCount = 1,
		fixedRoots = true,
	) => {
		const perPeer = new Map<
			string,
			{ fanout: FanoutTree; topicRootControlPlane: TopicRootControlPlane }
		>();
		const services = (components: any) => {
			const hash = getPublicKeyFromPeerId(components.peerId).hashcode();
			let value = perPeer.get(hash);
			if (!value) {
				const topicRootControlPlane = new TopicRootControlPlane();
				value = {
					topicRootControlPlane,
					fanout: new FanoutTree(components, {
						connectionManager: false,
						topicRootControlPlane,
					}),
				};
				perPeer.set(hash, value);
			}
			return value;
		};
		session = await TestSession.disconnected<Services>(count, {
			services: {
				fanout: (components: any) => services(components).fanout,
				pubsub: (components: any) =>
					new TopicControlPlane(components, {
						...services(components),
						connectionManager: false,
						canRelayMessage: true,
						subscriptionDebounceDelay: 0,
						shardCount,
						fanoutJoin: {
							timeoutMs: 5_000,
							retryMs: 25,
							joinReqTimeoutMs: 500,
						},
					}),
			},
		});
		const pubsubs = session.peers.map((peer) => peer.services.pubsub);
		if (!fixedRoots) return pubsubs;
		const root = pubsubs[0]!;
		for (const pubsub of pubsubs) {
			pubsub.setTopicRootCandidates([root.publicKeyHash]);
		}
		await root.hostShardRootsNow();
		return pubsubs;
	};

	const channelState = (pubsub: TopicControlPlane, topic: string) => {
		const internals = pubsub as any;
		const shardTopic = internals.getShardTopicForUserTopic(topic);
		return {
			internals,
			shardTopic,
			state: internals.fanoutChannels.get(shardTopic),
		};
	};

	const recover = (
		pubsub: TopicControlPlane,
		shardTopic: string,
		root: string,
		type: RecoveryEvent,
	) => {
		pubsub.fanout.dispatchEvent(
			new CustomEvent(type, {
				detail: {
					topic: shardTopic,
					root,
					...(type === "fanout:joined"
						? { parent: root }
						: { child: "recovered-child" }),
				},
			}) as any,
		);
	};

	const subscribers = (pubsub: TopicControlPlane, topic: string) =>
		(pubsub.getSubscribers(topic) ?? []).map((key) => key.hashcode());

	const decodePublish = (pubsub: TopicControlPlane, payload: Uint8Array) => {
		const message = DataMessage.from(new Uint8ArrayList(payload));
		return (pubsub as any).decodePubSubMessage(message.data);
	};

	afterEach(async () => {
		sinon.restore();
		await session?.stop();
		session = undefined;
	});

	for (const event of [
		"fanout:joined",
		"fanout:child-joined",
		"overlay reattachment",
	] as const) {
		it(`recovers a dropped initial Subscribe after ${event} without application discovery`, async () => {
			const topic = "lost-subscription-announcement";
			const [root, donor, newcomer] = await createSession(3);
			await session!.connect([
				[session!.peers[0], session!.peers[1]],
				[session!.peers[0], session!.peers[2]],
			]);
			await Promise.all([
				waitForNeighbour(root!, donor!),
				waitForNeighbour(root!, newcomer!),
			]);
			await root!.subscribe(topic);
			await donor!.subscribe(topic);
			await waitForResolved(() => {
				expect(subscribers(root!, topic)).to.include(donor!.publicKeyHash);
				expect(subscribers(donor!, topic)).to.include(root!.publicKeyHash);
			});

			const { internals, shardTopic } = channelState(newcomer!, topic);
			// Finish initial overlay attachment before C tracks the topic. This
			// reproduces donors' announcements arriving before C can retain them.
			await internals.ensureFanoutChannel(shardTopic);
			await waitForResolved(() => {
				for (const pubsub of [root!, donor!, newcomer!]) {
					const state = channelState(pubsub, topic).state;
					expect(state?.announceTask).to.equal(undefined);
				}
			});
			const state = channelState(newcomer!, topic).state;
			const originalPublish = state.channel.publish.bind(state.channel);
			let dropped = 0;
			sinon.stub(state.channel, "publish").callsFake(async (payload: any) => {
				const message = decodePublish(newcomer!, payload);
				if (
					dropped === 0 &&
					message instanceof Subscribe &&
					message.requestSubscribers &&
					message.topics.includes(topic)
				) {
					dropped++;
					return;
				}
				return originalPublish(payload);
			});
			const requests = [root!, donor!, newcomer!].map((pubsub) =>
				sinon.spy(pubsub, "requestSubscribers"),
			);
			await newcomer!.subscribe(topic);
			expect(dropped).to.equal(1);
			// B is behind A, so direct-neighbour subscription exchange cannot
			// conceal a lost fanout announcement to B.
			expect(internals.peers.has(donor!.publicKeyHash)).to.equal(false);
			expect(subscribers(newcomer!, topic)).not.to.include(
				donor!.publicKeyHash,
			);

			if (event === "overlay reattachment") {
				// Reattach C through the real join protocol to exercise both the
				// parent notification and the recovered child's notification.
				await state.channel.leave();
				await state.channel.join(
					internals.fanoutNodeChannelOptions,
					internals.fanoutJoinOptions,
				);
			} else {
				// Isolate each notification from repaired overlay membership;
				// publication, verification and subscriber replies use real peers.
				recover(newcomer!, shardTopic, state.root, event);
			}
			await waitForResolved(
				() => {
					expect(subscribers(newcomer!, topic)).to.include.members([
						root!.publicKeyHash,
						donor!.publicKeyHash,
					]);
				},
				{ timeout: 5_000, delayInterval: 10 },
			);
			expect(requests.every((request) => request.notCalled)).to.equal(true);
		});
	}

	it("notifies the root when an overlay child attaches and reattaches", async () => {
		const [root, leaf] = await createSession(2);
		await session!.connect([[session!.peers[0], session!.peers[1]]]);
		await waitForNeighbour(root!, leaf!);
		const topic = "root-child-attachment-notification";
		const { internals, shardTopic } = channelState(leaf!, topic);
		const events: { topic: string; root: string; child: string }[] = [];
		root!.fanout.addEventListener(
			"fanout:child-joined" as any,
			(event: any) => {
				events.push(event.detail);
			},
		);
		await internals.ensureFanoutChannel(shardTopic);
		expect(events).to.deep.equal([
			{
				topic: shardTopic,
				root: root!.publicKeyHash,
				child: leaf!.publicKeyHash,
			},
		]);
		const state = channelState(leaf!, topic).state;
		await state.channel.leave();
		await state.channel.join(
			internals.fanoutNodeChannelOptions,
			internals.fanoutJoinOptions,
		);
		expect(events).to.deep.equal([
			{
				topic: shardTopic,
				root: root!.publicKeyHash,
				child: leaf!.publicKeyHash,
			},
			{
				topic: shardTopic,
				root: root!.publicKeyHash,
				child: leaf!.publicKeyHash,
			},
		]);
	});

	it("coalesces event bursts and announces only current local topics in that shard", async () => {
		const [pubsub] = await createSession(1, 4);
		const topic = "coalesced-subscription-recovery";
		const { internals, shardTopic } = channelState(pubsub!, topic);
		const sameShard: string[] = [];
		let other = "";
		for (let i = 0; sameShard.length < 3 || !other; i++) {
			const candidate = `recovery-topic-${i}`;
			if (internals.getShardTopicForUserTopic(candidate) === shardTopic) {
				sameShard.push(candidate);
			} else {
				other ||= candidate;
			}
		}
		const [second, tracked, pending] = sameShard;
		await Promise.all([
			pubsub!.subscribe(topic),
			pubsub!.subscribe(second),
			pubsub!.subscribe(other),
		]);
		internals.initializeTopic(tracked);
		internals.initializeTopic(pending);
		internals.pendingSubscriptions.add(pending);
		const state = channelState(pubsub!, topic).state;
		const messages: Subscribe[] = [];
		const published = sinon
			.stub(state.channel, "publish")
			.callsFake(async (payload: any) => {
				messages.push(decodePublish(pubsub!, payload));
			});
		const entered = deferred();
		const gate = deferred();
		const originalCreate = internals.createMessage.bind(pubsub);
		sinon.stub(internals, "createMessage").callsFake(async (...args: any[]) => {
			entered.resolve();
			await gate.promise;
			return originalCreate(...args);
		});
		try {
			for (let i = 0; i < 100; i++) {
				recover(pubsub!, shardTopic, state.root, "fanout:child-joined");
			}
			await entered.promise;
			for (let i = 0; i < 100; i++) {
				recover(pubsub!, shardTopic, state.root, "fanout:joined");
			}
			const pending = state.announceTask;
			expect(pending).to.be.instanceOf(Promise);
			gate.resolve();
			await pending;
			expect(published.callCount).to.equal(2);
			for (const message of messages) {
				expect(message).to.be.instanceOf(Subscribe);
				expect(message.requestSubscribers).to.equal(true);
				expect(message.topics).to.have.members([topic, second]);
			}
		} finally {
			gate.resolve();
		}
	});

	it("ignores recovery for channels without local subscriptions and for a different root", async () => {
		const [pubsub] = await createSession(1);
		const topic = "unsubscribed-recovery";
		const { internals, shardTopic, state } = channelState(pubsub!, topic);
		internals.initializeTopic(topic);
		const publish = sinon.spy(state.channel, "publish");
		recover(pubsub!, shardTopic, state.root, "fanout:child-joined");
		await state.announceTask;
		expect(publish.notCalled).to.equal(true);
		await pubsub!.subscribe(topic);
		publish.resetHistory();
		recover(pubsub!, shardTopic, "stale-root", "fanout:joined");
		await delay(0);
		expect(publish.notCalled).to.equal(true);
	});

	it("retains an attachment event between its last publish and task finalization", async () => {
		const [pubsub] = await createSession(1);
		const topic = "recovery-finalization-window";
		await pubsub!.subscribe(topic);
		const { internals, shardTopic, state } = channelState(pubsub!, topic);
		const publish = sinon.stub(state.channel, "publish").resolves();
		const originalAnnounce = internals.announceShardSubscriptions.bind(pubsub);
		let delivered = false;
		sinon
			.stub(internals, "announceShardSubscriptions")
			.callsFake(async (...args: any[]) => {
				await originalAnnounce(...args);
				if (delivered) return;
				delivered = true;
				// The first microtask runs before the loop continuation. The second
				// delivers the event after that loop exits but before finally clears
				// its task, so a queued attachment must start another drain.
				queueMicrotask(() =>
					queueMicrotask(() => {
						recover(pubsub!, shardTopic, state.root, "fanout:child-joined");
					}),
				);
			});
		recover(pubsub!, shardTopic, state.root, "fanout:joined");
		await state.announceTask;
		await state.announceTask;
		expect(publish.callCount).to.equal(2);
		expect(state.announceDirty).to.equal(false);
	});

	for (const { changed, reply } of [
		{ changed: "stops", reply: false },
		{ changed: "replaces the peer", reply: false },
		{ changed: "unsubscribes", reply: false },
		{ changed: "stops", reply: true },
		{ changed: "replaces the peer", reply: true },
	] as const) {
		it(`fences a direct ${reply ? "reply" : "Subscribe"} while signing when the caller ${changed}`, async () => {
			const [pubsub, remote] = await createSession(2);
			const topic = "direct-subscribe-signing-owner";
			await pubsub!.subscribe(topic);
			const internals = pubsub as any;
			const peer = pubsub!.addPeer(
				remote!.peerId,
				remote!.publicKey,
				"/peerbit/topic-control-plane/2.1.0",
				"direct-signing-original",
			);
			sinon.stub(peer, "isWritable").get(() => true);
			const publish = sinon.stub(internals, "publishMessage").resolves();
			const entered = deferred();
			const gate = deferred();
			const originalCreate = internals.createMessage.bind(pubsub);
			sinon
				.stub(internals, "createMessage")
				.callsFake(async (...args: any[]) => {
					const message = await originalCreate(...args);
					if (internals.decodePubSubMessage(args[0]) instanceof Subscribe) {
						entered.resolve();
						await gate.promise;
					}
					return message;
				});
			const pending = reply
				? internals.sendDirectControlMessage(
						peer,
						new Subscribe({ topics: [topic], requestSubscribers: false }),
					)
				: internals.announceDirectSubscriptions([topic], [peer]);
			try {
				await entered.promise;
				if (changed === "stops") {
					await pubsub!.stop();
				} else if (changed === "replaces the peer") {
					await internals._removePeer(remote!.publicKey);
					const replacement = pubsub!.addPeer(
						remote!.peerId,
						remote!.publicKey,
						"/peerbit/topic-control-plane/2.1.0",
						"direct-signing-replacement",
					);
					expect(replacement).not.to.equal(peer);
				} else {
					await pubsub!.unsubscribe(topic);
				}
				gate.resolve();
				await pending;
				const staleSubscribes = publish
					.getCalls()
					.filter(
						(call) =>
							internals.decodePubSubMessage(
								(call.args[1] as DataMessage).data,
							) instanceof Subscribe,
					);
				expect(staleSubscribes).to.have.length(0);
			} finally {
				gate.resolve();
			}
		});
	}

	it("announces a successful shard when another shard join rejects", async () => {
		const [pubsub] = await createSession(1, 4);
		const topic = "successful-independent-shard";
		const { internals, shardTopic, state } = channelState(pubsub!, topic);
		let failedTopic = "";
		for (let i = 0; !failedTopic; i++) {
			const candidate = `failed-shard-${i}`;
			if (internals.getShardTopicForUserTopic(candidate) !== shardTopic)
				failedTopic = candidate;
		}
		const failed = channelState(pubsub!, failedTopic);
		const expected = new Error("deterministic shard join failure");
		sinon
			.stub(internals, "ensureFanoutChannel")
			.callsFake(async (shard: any) => {
				if (shard === failed.shardTopic) throw expected;
			});
		const reconcile = sinon.stub(internals, "scheduleReconcileShardOverlays");
		const successfulPublish = sinon.stub(state.channel, "publish").resolves();
		const failedPublish = sinon
			.stub(failed.state.channel, "publish")
			.resolves();
		const error = await internals
			._subscribe([
				{ key: failedTopic, counter: 1 },
				{ key: topic, counter: 1 },
			])
			.then(
				(): undefined => undefined,
				(error: unknown) => error,
			);
		expect(error).to.equal(expected);
		expect(successfulPublish.calledOnce).to.equal(true);
		const message = decodePublish(
			pubsub!,
			successfulPublish.firstCall.args[0] as Uint8Array,
		);
		expect(message).to.be.instanceOf(Subscribe);
		expect(message.topics).to.deep.equal([topic]);
		expect(failedPublish.notCalled).to.equal(true);
		expect(reconcile.calledOnce).to.equal(true);
		expect(pubsub!.subscriptions.has(failedTopic)).to.equal(true);
	});

	it("rejects a validly signed direct Unsubscribe from a different transport identity", async () => {
		const [receiver, transport, signer] = await createSession(3);
		const topic = "direct-unsubscribe-signer-binding";
		await receiver!.subscribe(topic);
		const stream = receiver!.addPeer(
			transport!.peerId,
			transport!.publicKey,
			"/peerbit/topic-control-plane/2.1.0",
			"direct-signer-binding",
		);
		const deliver = async (
			origin: TopicControlPlane,
			control: Subscribe | Unsubscribe,
		) => {
			const message = await (origin as any).createMessage(control.bytes(), {
				mode: new AnyWhere(),
				skipRecipientValidation: true,
			});
			expect(await message.verify(true)).to.equal(true);
			await receiver!.onDataMessage(transport!.publicKey, stream, message, 0);
		};
		// A transport peer can forward a valid envelope signed by a different
		// identity. It must not introduce mismatched membership/watermark keys.
		await deliver(signer!, new Unsubscribe({ topics: [topic] }));
		expect(
			receiver!.lastSubscriptionMessages.has(signer!.publicKeyHash),
		).to.equal(false);
		expect(
			receiver!.lastSubscriptionMessages.has(transport!.publicKeyHash),
		).to.equal(false);
		expect(subscribers(receiver!, topic)).not.to.include(
			transport!.publicKeyHash,
		);
		await deliver(
			transport!,
			new Subscribe({ topics: [topic], requestSubscribers: false }),
		);
		expect(subscribers(receiver!, topic)).to.include(transport!.publicKeyHash);
		await deliver(transport!, new Unsubscribe({ topics: [topic] }));
		expect(subscribers(receiver!, topic)).not.to.include(
			transport!.publicKeyHash,
		);
		for (const messages of receiver!.lastSubscriptionMessages.values()) {
			expect(messages).to.be.instanceOf(Map);
		}
		await receiver!.unsubscribe(topic);
	});

	it("ignores a direct Subscribe verified after its stream is replaced and accepts the replacement", async () => {
		const [receiver, remote] = await createSession(2);
		const topic = "direct-subscribe-inbound-owner";
		await receiver!.subscribe(topic);
		const addPeer = (connection: string) =>
			receiver!.addPeer(
				remote!.peerId,
				remote!.publicKey,
				"/peerbit/topic-control-plane/2.1.0",
				connection,
			);
		const old = addPeer("inbound-old");
		const createSubscribe = () =>
			(remote as any).createMessage(
				new Subscribe({ topics: [topic], requestSubscribers: false }).bytes(),
				{ mode: new AnyWhere(), skipRecipientValidation: true },
			);
		const stale = await createSubscribe();
		const entered = deferred();
		const gate = deferred();
		const verify = stale.verify.bind(stale);
		sinon.stub(stale, "verify").callsFake(async () => {
			const valid = await verify(true);
			expect(valid).to.equal(true);
			entered.resolve();
			await gate.promise;
			return valid;
		});
		const pending = receiver!.onDataMessage(remote!.publicKey, old, stale, 0);
		try {
			await entered.promise;
			await (receiver as any)._removePeer(remote!.publicKey);
			const replacement = addPeer("inbound-replacement");
			expect(replacement).not.to.equal(old);
			gate.resolve();
			await pending;
			expect(subscribers(receiver!, topic)).not.to.include(
				remote!.publicKeyHash,
			);
			expect(
				receiver!.lastSubscriptionMessages.has(remote!.publicKeyHash),
			).to.equal(false);
			await receiver!.onDataMessage(
				remote!.publicKey,
				replacement,
				await createSubscribe(),
				0,
			);
			expect(subscribers(receiver!, topic)).to.include(remote!.publicKeyHash);
		} finally {
			gate.resolve();
		}
	});

	for (const phase of ["queued", "signing"] as const) {
		it(`does not reannounce after its channel closes while ${phase}`, async () => {
			const [pubsub] = await createSession(1);
			const topic = "closed-subscription-recovery";
			await pubsub!.subscribe(topic);
			const { internals, shardTopic, state } = channelState(pubsub!, topic);
			const publish = sinon.spy(state.channel, "publish");
			const entered = deferred();
			const gate = deferred();
			const originalCreate = internals.createMessage.bind(pubsub);
			if (phase === "signing") {
				sinon
					.stub(internals, "createMessage")
					.callsFake(async (...args: any[]) => {
						entered.resolve();
						await gate.promise;
						return originalCreate(...args);
					});
			}
			try {
				recover(pubsub!, shardTopic, state.root, "fanout:child-joined");
				const pending = state.announceTask;
				if (phase === "signing") await entered.promise;
				await internals.closeFanoutChannel(shardTopic, { force: true });
				gate.resolve();
				await pending;
				recover(pubsub!, shardTopic, state.root, "fanout:joined");
				await delay(0);
				expect(publish.notCalled).to.equal(true);
			} finally {
				gate.resolve();
			}
		});
	}

	it("rebuilds root candidates once after retaining a complete signed claim batch", async () => {
		const [receiver, relay, first, second] = await createSession(4, 1, false);
		const internals = receiver as any;
		const claims = await Promise.all(
			[first!, second!].map(async (origin) => {
				const message = await (origin as any).createMessage(
					(origin as any).topicRootCandidateClaimData,
					{
						mode: new AnyWhere(),
						expiresInMs: 90_000,
						skipRecipientValidation: true,
					},
				);
				const bytes = message.bytes();
				return bytes instanceof Uint8Array ? bytes : bytes.subarray();
			}),
		);
		// Keep the fixture focused on the authenticated import boundary; root
		// hosting and claim relay have their own transport/lifecycle tests.
		sinon.stub(internals, "scheduleHostOwnedShardRoots");
		sinon.stub(internals, "scheduleReconcileShardOverlays");
		sinon
			.stub(internals, "refreshLocalTopicRootCandidateClaim")
			.resolves(false);
		sinon.stub(internals, "sendSignedTopicRootCandidateClaims").resolves();
		const stream = receiver!.addPeer(
			relay!.peerId,
			relay!.publicKey,
			"/peerbit/topic-control-plane/2.1.0",
			"atomic-claim-import",
		);
		internals.clearAutoTopicRootCandidateUpdateSchedule();
		const snapshots: string[][] = [];
		const originalRebuild =
			internals.rebuildAutoTopicRootCandidatesFromClaims.bind(receiver);
		sinon
			.stub(internals, "rebuildAutoTopicRootCandidatesFromClaims")
			.callsFake((...args: any[]) => {
				snapshots.push([...internals.signedTopicRootCandidateClaims.keys()]);
				return originalRebuild(...args);
			});
		await internals.processDirectPubSubMessage({
			pubsubMessage: new TopicRootCandidateClaims({ claims }),
			message: { header: { signatures: { publicKeys: [relay!.publicKey] } } },
			from: relay!.publicKey,
			stream,
		});
		expect(snapshots).to.have.length(1);
		expect(snapshots[0]).to.include.members([
			first!.publicKeyHash,
			second!.publicKeyHash,
		]);
		expect(
			receiver!.topicRootControlPlane.getTopicRootCandidates(),
		).to.include.members([first!.publicKeyHash, second!.publicKeyHash]);
	});
});
