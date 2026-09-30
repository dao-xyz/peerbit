import { TestSession } from "@peerbit/libp2p-test-utils";
import { AnyWhere } from "@peerbit/stream-interface";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { FanoutTree } from "../src/index.js";

const createSession = (count: number) =>
	TestSession.disconnected<{ fanout: FanoutTree }>(count, {
		services: {
			fanout: (components) =>
				new FanoutTree(components, { connectionManager: false }),
		},
	});

const channelOptions = {
	msgRate: 10,
	msgSize: 64,
	uploadLimitBps: 1_000_000,
	maxChildren: 1,
	repair: false,
};

const joinOptions = {
	timeoutMs: 10_000,
	retryMs: 25,
	joinReqTimeoutMs: 100,
	parentProbeTimeoutMs: 100,
	parentUpgradeIntervalMs: 0,
	staleAfterMs: 0,
};

const openPair = async (topic: string) => {
	const session = await createSession(2);
	try {
		const [rootNode, leafNode] = session.peers;
		await session.connect([[rootNode, leafNode]]);
		const root = rootNode.services.fanout;
		const leaf = leafNode.services.fanout;
		const rootId = root.publicKeyHash;
		root.openChannel(topic, rootId, { ...channelOptions, role: "root" });
		await leaf.joinChannel(
			topic,
			rootId,
			{ ...channelOptions, uploadLimitBps: 0, maxChildren: 0 },
			joinOptions,
		);
		return { session, root, leaf, rootId };
	} catch (error) {
		await session.stop();
		throw error;
	}
};

const deferred = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => {
		resolve = done;
	});
	return { promise, resolve };
};

describe("fanout parent liveness", () => {
	it("accepts KICK only from the current parent on its current stream", async () => {
		const session = await createSession(3);
		const [leaf, parent, former] = session.peers.map((peer) => peer.services.fanout);
		const currentPeer = leaf.addPeer(
			parent.peerId,
			parent.publicKey,
			parent.multicodecs[0]!,
			"current-parent",
		);
		const formerPeer = leaf.addPeer(
			former.peerId,
			former.publicKey,
			former.multicodecs[0]!,
			"former-parent",
		);
		try {
			const topic = "parent-kick-ownership";
			const rootId = parent.publicKeyHash;
			const id = leaf.openChannel(topic, rootId, {
				...channelOptions,
				role: "node",
			});
			const internals = leaf as any;
			const channel = internals.channelsBySuffixKey.get(id.suffixKey);
			const route = [rootId, leaf.publicKeyHash];
			// Establish replacement attachment state directly so the handler can be
			// checked independently of join-loop scheduling and transport delivery.
			channel.parent = rootId;
			channel.level = 1;
			channel.routeFromRoot = route;
			const deliverKick = async (
				from: FanoutTree,
				stream: typeof currentPeer,
			) => {
				const message = await from.createMessage(
					internals.codec.encodeKick(id.key),
					{ mode: new AnyWhere() },
				);
				await leaf.onDataMessage(from.publicKey, stream, message, 0);
			};
			await deliverKick(former, formerPeer);
			expect(channel.parent).to.equal(rootId);
			expect(channel.routeFromRoot).to.equal(route);
			expect(leaf.getChannelMetrics(topic, rootId).reparentKicked).to.equal(0);

			internals.startParentUpgradeGrace(channel, {
				previousParent: former.publicKeyHash,
				candidateParent: rootId,
				previousLevel: 2,
				previousRouteFromRoot: [rootId, former.publicKeyHash, leaf.publicKeyHash],
				previousLastParentDataAt: 1,
				previousLastParentUpgradeActivityAt: 1,
				previousReceivedAnyParentData: true,
				candidateFirstMessages: 0,
				previousFirstMessages: 0,
				minCandidateFirstMessages: 1,
				minCandidateAdvantageMessages: 1,
				maxPreviousFirstMessages: 0,
				timeoutMs: 10_000,
			});
			const grace = channel.parentUpgradeGrace;
			const formerReplacement: typeof formerPeer = Object.create(formerPeer);
			leaf.peers.set(former.publicKeyHash, formerReplacement);
			await deliverKick(former, formerPeer);
			expect(channel.parentUpgradeGrace).to.equal(grace);
			await deliverKick(former, formerReplacement);
			expect(channel.parentUpgradeGrace).to.equal(undefined);
			internals.rollbackParentUpgradeGrace(channel);
			expect(channel.parent).to.equal(rootId);
			expect(channel.routeFromRoot).to.equal(route);

			// A kicked shadow cannot retain observations that permit later promotion.
			const shadow = { hash: former.publicKeyHash };
			channel.parentShadow = shadow;
			internals.parentUpgradeShadowInFlightSuffixKey = id.suffixKey;
			await deliverKick(former, formerPeer);
			expect(channel.parentShadow).to.equal(shadow);
			expect(internals.parentUpgradeShadowInFlightSuffixKey).to.equal(id.suffixKey);
			await deliverKick(former, formerReplacement);
			expect(channel.parentShadow).to.equal(undefined);
			expect(internals.parentUpgradeShadowInFlightSuffixKey).to.equal(undefined);
			expect(leaf.getChannelMetrics(topic, rootId).parentShadowReset).to.equal(1);
			expect(leaf.getChannelMetrics(topic, rootId).reparentKicked).to.equal(0);
			expect(channel.parent).to.equal(rootId);

			const replacementPeer: typeof currentPeer = Object.create(currentPeer);
			leaf.peers.set(rootId, replacementPeer);
			await deliverKick(parent, currentPeer);
			expect(channel.parent).to.equal(rootId);
			expect(channel.routeFromRoot).to.equal(route);
			expect(leaf.getChannelMetrics(topic, rootId).reparentKicked).to.equal(0);

			await deliverKick(parent, replacementPeer);
			expect(channel.parent).to.equal(undefined);
			expect(channel.routeFromRoot).to.equal(undefined);
			expect(leaf.getChannelMetrics(topic, rootId).reparentKicked).to.equal(1);
		} finally {
			leaf.peers.set(parent.publicKeyHash, currentPeer);
			leaf.peers.set(former.publicKeyHash, formerPeer);
			await session.stop();
		}
	});

	it("recovers an idle channel from a blackholed parent without a transport disconnect", async function () {
		this.timeout(30_000);
		const session = await createSession(3);

		try {
			const [rootNode, relayNode, leafNode] = session.peers;
			await session.connect([
				[rootNode, relayNode],
				[rootNode, leafNode],
			]);
			const root = rootNode.services.fanout;
			const relay = relayNode.services.fanout;
			const leaf = leafNode.services.fanout;
			const rootId = root.publicKeyHash;
			const topic = "idle-parent-blackhole";
			root.openChannel(topic, rootId, {
				...channelOptions,
				role: "root",
			});
			await relay.joinChannel(topic, rootId, channelOptions, {
				timeoutMs: 10_000,
			});
			await leaf.joinChannel(
				topic,
				rootId,
				{ ...channelOptions, uploadLimitBps: 0, maxChildren: 0 },
				joinOptions,
			);
			expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(
				relay.publicKeyHash,
			);

			const leafToRelay = leaf.peers.get(relay.publicKeyHash)!;
			const relayToLeaf = relay.peers.get(leaf.publicKeyHash)!;
			expect(leafToRelay).to.exist;
			expect(relayToLeaf).to.exist;
			const originalLeafWrite = leafToRelay.write;
			const originalRelayWrite = relayToLeaf.write;
			let droppedWrites = 0;
			const blackholeWrite = () => {
				droppedWrites += 1;
			};
			leafToRelay.write = blackholeWrite;
			relayToLeaf.write = blackholeWrite;

			try {
				// Release the root slot without a KICK, service stop, or connection close.
				// The leaf's parent stream remains writable, but delivers no messages.
				await relay.closeChannel(topic, rootId, {
					notifyParent: true,
					kickChildren: false,
				});
				await waitForResolved(() =>
					expect(root.getChannelStats(topic, rootId)?.children).to.equal(0),
				);
				expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(
					relay.publicKeyHash,
				);
				expect(leafToRelay.isReadable).to.equal(true);
				expect(leafToRelay.isWritable).to.equal(true);
				expect(
					leafNode
						.getConnections(relayNode.peerId)
						.some((c) => c.status === "open"),
				).to.equal(true);
				expect(
					leaf.getChannelMetrics(topic, rootId).reparentDisconnect,
				).to.equal(0);

				// No data, missing sequences, or parent-upgrade requests can trigger this
				// repair: an idle attached parent must receive a bounded health check.
				await waitForResolved(
					() =>
						expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId),
					{ timeout: 10_000, delayInterval: 25 },
				);
				expect(droppedWrites).to.be.greaterThan(0);

				let received: Uint8Array | undefined;
				leaf.addEventListener("fanout:data", (event) => {
					if (event.detail.topic === topic && event.detail.root === rootId) {
						received = event.detail.payload;
					}
				});
				const payload = new Uint8Array([7, 7, 7]);
				await root.publishData(topic, rootId, payload);
				await waitForResolved(() => expect(received).to.deep.equal(payload));
			} finally {
				leafToRelay.write = originalLeafWrite;
				relayToLeaf.write = originalRelayWrite;
			}
		} finally {
			await session.stop();
		}
	});

	it("keeps a healthy idle parent with no spare child capacity", async function () {
		this.timeout(15_000);
		const topic = "idle-full-parent";
		const { session, root, leaf, rootId } = await openPair(topic);
		try {
			expect(root.getChannelStats(topic, rootId)?.children).to.equal(1);
			expect(root.getChannelStats(topic, rootId)?.effectiveMaxChildren).to.equal(
				1,
			);
			await waitForResolved(
				() =>
					expect(
						leaf.getChannelMetrics(topic, rootId).parentProbeReplyReceived,
					).to.be.at.least(1),
				{ timeout: 10_000, delayInterval: 25 },
			);
			expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId);
			const metrics = leaf.getChannelMetrics(topic, rootId);
			expect(metrics.reparentStale).to.equal(0);
			expect(metrics.reparentDisconnect).to.equal(0);
			expect(metrics.joinAcceptReceived).to.equal(1);
			expect(
				root.getChannelMetrics(topic, rootId).parentUpgradeRootReservationCreated,
			).to.equal(0);
		} finally {
			await session.stop();
		}
	});

	for (const change of ["channel reopens", "parent stream is replaced"] as const) {
		it(`ignores an obsolete second probe failure after its ${change}`, async function () {
			this.timeout(25_000);
			const topic = `idle-parent-late-probe-${change}`;
			const { session, leaf, rootId } = await openPair(topic);
			const internals = leaf as any;
			const id = leaf.getChannelId(topic, rootId);
			const oldChannel = internals.channelsBySuffixKey.get(id.suffixKey);
			const oldJoinLoop = oldChannel.joinLoop as Promise<void>;
			const oldPeer = leaf.peers.get(rootId)!;
			let replacementPeer: typeof oldPeer | undefined;
			const originalProbe = internals.probeParentCandidate;
			const releaseProbe = deferred();
			let calls = 0;
			let completedCalls = 0;
			let parked = false;
			try {
				internals.probeParentCandidate = async (...args: any[]) => {
					const call = ++calls;
					const result = await originalProbe.apply(leaf, args);
					if (call === 2) {
						parked = true;
						await releaseProbe.promise;
					}
					completedCalls++;
					// Simulate two lost replies, but retain the real request/reply path.
					// The second failure must not apply to a different attachment owner.
					return call <= 2 ? undefined : result;
				};
				await waitForResolved(() => expect(parked).to.equal(true), {
					timeout: 10_000,
					delayInterval: 25,
				});
				expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId);
				expect(leaf.getChannelMetrics(topic, rootId).reparentStale).to.equal(0);

				if (change === "channel reopens") {
					await leaf.closeChannel(topic, rootId);
					await leaf.joinChannel(
						topic,
						rootId,
						{ ...channelOptions, uploadLimitBps: 0, maxChildren: 0 },
						joinOptions,
					);
					expect(internals.channelsBySuffixKey.get(id.suffixKey)).not.to.equal(
						oldChannel,
					);
				} else {
					// Keep the real live connection while replacing only its ownership
					// identity, as occurs when a protocol stream is superseded.
					replacementPeer = Object.create(oldPeer);
					leaf.peers.set(rootId, replacementPeer!);
				}
				releaseProbe.resolve();
				if (change === "channel reopens") {
					await oldJoinLoop.catch(() => {});
				} else {
					await waitForResolved(
						() => expect(completedCalls).to.be.at.least(3),
						{ timeout: 10_000, delayInterval: 25 },
					);
					expect(leaf.peers.get(rootId)).to.equal(replacementPeer);
				}
				expect(leaf.getChannelStats(topic, rootId)?.parent).to.equal(rootId);
				expect(leaf.getChannelMetrics(topic, rootId).reparentStale).to.equal(0);
				expect(leaf.getChannelMetrics(topic, rootId).reparentDisconnect).to.equal(0);
				expect(oldPeer.isWritable).to.equal(true);
			} finally {
				releaseProbe.resolve();
				internals.probeParentCandidate = originalProbe;
				if (replacementPeer && leaf.peers.get(rootId) === replacementPeer) {
					leaf.peers.set(rootId, oldPeer);
				}
				await session.stop();
			}
		});
	}
});
