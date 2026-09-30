import { getPublicKeyFromPeerId } from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import type { DataEvent } from "@peerbit/pubsub-interface";
import {
	RUST_CORE_GLOBAL_KEY,
	type RustCoreStream,
	waitForNeighbour,
} from "@peerbit/stream";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	FanoutTree,
	TopicControlPlane,
	TopicRootControlPlane,
} from "../src/index.js";

type Services = { fanout: FanoutTree; pubsub: TopicControlPlane };

const topicHash32 = (topic: string) => {
	let hash = 0x811c9dc5;
	for (let index = 0; index < topic.length; index++) {
		hash ^= topic.charCodeAt(index);
		hash = (hash * 0x01000193) >>> 0;
	}
	return hash >>> 0;
};

describe("pubsub stale relay-only root cold join", () => {
	it("discovers a live non-neighbor donor and receives its fresh payload before stale leases expire", async function () {
		this.timeout(40_000);
		const perPeer = new Map<
			string,
			{ fanout: FanoutTree; roots: TopicRootControlPlane }
		>();
		const servicesFor = (
			components: ConstructorParameters<typeof FanoutTree>[0],
		) => {
			const hash = getPublicKeyFromPeerId(components.peerId).hashcode();
			let services = perPeer.get(hash);
			if (!services) {
				const roots = new TopicRootControlPlane();
				services = {
					roots,
					fanout: new FanoutTree(components, {
						connectionManager: false,
						topicRootControlPlane: roots,
					}),
				};
				perPeer.set(hash, services);
			}
			return services;
		};
		const session = await TestSession.disconnected<Services>(5, {
			services: {
				fanout: (components) => servicesFor(components).fanout,
				pubsub: (components) => {
					const { fanout, roots } = servicesFor(components);
					return new TopicControlPlane(components, {
						fanout,
						topicRootControlPlane: roots,
						canRelayMessage: true,
						connectionManager: false,
					});
				},
			},
		});
		let recovery: Promise<void> | undefined;
		let deadline: ReturnType<typeof setTimeout> | undefined;
		const controller = new AbortController();
		try {
			const injected = (globalThis as Record<string, unknown>)[
				RUST_CORE_GLOBAL_KEY
			] as RustCoreStream | undefined;
			const expectedCore = process.env.PEERBIT_STREAM_RUST_CORE
				? injected
				: undefined;
			if (process.env.PEERBIT_STREAM_RUST_CORE) {
				expect(
					expectedCore,
					"the native bridge must inject an actual Rust core",
				).to.exist;
			}
			for (const { services } of session.peers) {
				expect((services.pubsub as any).rustCore).to.equal(expectedCore);
				expect((services.fanout as any).rustCore).to.equal(expectedCore);
				expect((services.pubsub as any).nativeTopicControl).to.equal(
					expectedCore?.topicControl,
				);
				expect((services.fanout as any).nativeFanout).to.equal(
					expectedCore?.fanout,
				);
			}
			// Sorting fixes the candidate-index transitions without fixed peer keys.
			const ordered = [...session.peers].sort((a, b) =>
				a.services.pubsub.publicKeyHash < b.services.pubsub.publicKeyHash
					? -1
					: 1,
			);
			const [staleANode, staleBNode, donorNode, leafNode, relayNode] = ordered;
			const relay = relayNode.services.pubsub;
			const staleA = staleANode.services.pubsub;
			const staleB = staleBNode.services.pubsub;
			const leaf = leafNode.services.pubsub;
			const donor = donorNode.services.pubsub;
			const allCandidates = ordered.map(
				(node) => node.services.pubsub.publicKeyHash,
			);
			const liveCandidates = [
				relay.publicKeyHash,
				leaf.publicKeyHash,
				donor.publicKeyHash,
			].sort();
			const before = new TopicRootControlPlane({
				defaultCandidates: allCandidates,
			});
			const afterOne = new TopicRootControlPlane({
				defaultCandidates: allCandidates.filter(
					(hash) => hash !== staleA.publicKeyHash,
				),
			});
			const afterBoth = new TopicRootControlPlane({
				defaultCandidates: liveCandidates,
			});
			const donorBeforeLeaf = new TopicRootControlPlane({
				defaultCandidates: [relay.publicKeyHash, donor.publicKeyHash],
			});
			let shardIndex = -1;
			for (let index = 0; index < 256; index++) {
				const shard = `/peerbit/pubsub-shard/1/${index}`;
				if (
					before.resolveDeterministicTopicRoot(shard) ===
						staleA.publicKeyHash &&
					afterOne.resolveDeterministicTopicRoot(shard) ===
						staleB.publicKeyHash &&
					afterBoth.resolveDeterministicTopicRoot(shard) ===
						donor.publicKeyHash &&
					donorBeforeLeaf.resolveDeterministicTopicRoot(shard) ===
						donor.publicKeyHash
				) {
					shardIndex = index;
					break;
				}
			}
			expect(
				shardIndex,
				"two stale roots must precede the live donor",
			).to.be.at.least(0);
			const shardTopic = `/peerbit/pubsub-shard/1/${shardIndex}`;
			let topic = "";
			for (let index = 0; index < 10_000; index++) {
				const candidate = `stale-root-cold-donor-${index}`;
				if (topicHash32(candidate) % 256 === shardIndex) {
					topic = candidate;
					break;
				}
			}
			expect(topic).not.to.equal("");

			// Ordinary bootstrap discovery can dial the donor later. The relay never
			// subscribes or manually joins this shard on the application's behalf.
			donorNode.services.fanout.setBootstraps(relayNode.getMultiaddrs());
			leafNode.services.fanout.setBootstraps(relayNode.getMultiaddrs());
			await session.connect([
				[staleANode, donorNode],
				[staleBNode, donorNode],
				[donorNode, relayNode],
			]);
			await Promise.all([
				waitForNeighbour(staleA, donor),
				waitForNeighbour(staleB, donor),
				waitForNeighbour(donor, relay),
			]);
			const initialCandidates = allCandidates.filter(
				(hash) => hash !== leaf.publicKeyHash,
			);
			await waitForResolved(
				() => {
					expect(
						relay.topicRootControlPlane.getTopicRootCandidates(),
					).to.deep.equal(initialCandidates);
					expect(
						donor.topicRootControlPlane.getTopicRootCandidates(),
					).to.deep.equal(initialCandidates);
				},
				{ timeout: 5_000 },
			);

			await Promise.all([staleANode.stop(), staleBNode.stop()]);
			await waitForResolved(
				() => {
					expect(
						donor.topicRootControlPlane.getTopicRootCandidates(),
					).to.deep.equal([relay.publicKeyHash, donor.publicKeyHash].sort());
				},
				{ timeout: 5_000 },
			);
			// Both valid leases reached the relay through the donor, never directly.
			for (const stale of [staleA, staleB]) {
				expect(relay.peers.has(stale.publicKeyHash)).to.equal(false);
				expect(relay.topicRootControlPlane.getTopicRootCandidates()).to.include(
					stale.publicKeyHash,
				);
			}
			await session.connect([[leafNode, relayNode]]);
			await waitForNeighbour(leaf, relay);
			await waitForResolved(
				() => {
					expect(
						leaf.topicRootControlPlane.getTopicRootCandidates(),
					).to.deep.equal(allCandidates);
					expect(
						donor.topicRootControlPlane.getTopicRootCandidates(),
					).to.deep.equal(liveCandidates);
				},
				{ timeout: 5_000 },
			);
			// The leaf must receive the stale snapshot before the donor's ordinary
			// root query gives the relay an opportunity to recover its own mapping.
			await donor.subscribe(topic);
			expect(
				donorNode.services.fanout.getChannelStats(
					shardTopic,
					donor.publicKeyHash,
				)?.level,
			).to.equal(0);
			expect(
				leaf.topicRootControlPlane.resolveDeterministicTopicRoot(shardTopic),
			).to.equal(staleA.publicKeyHash);
			expect(
				leaf.peers.has(donor.publicKeyHash),
				"the cold leaf initially knows the donor only through its relay",
			).to.equal(false);
			expect(leaf.getSubscribers(topic)).to.equal(undefined);
			expect((relay as any).subscriptions.size).to.equal(0);
			for (const stale of [staleA, staleB]) {
				expect(leaf.peers.has(stale.publicKeyHash)).to.equal(false);
				const claim = (leaf as any).signedTopicRootCandidateClaims.get(
					stale.publicKeyHash,
				);
				expect(claim, "the cold leaf retained the actual relayed signed lease")
					.to.exist;
				expect(claim.expires > BigInt(Date.now() + 20_000)).to.equal(true);
			}

			const received: DataEvent[] = [];
			leaf.addEventListener("data", (event) => {
				if (event.detail.data.topics.includes(topic))
					received.push(event.detail);
			});
			const payload = new Uint8Array([9, 3, 0, 2, 0, 2, 6]);
			const startedAt = performance.now();
			let recoveryStage = "subscribe";
			recovery = (async () => {
				await leaf.subscribe(topic);
				controller.signal.throwIfAborted();
				recoveryStage = "subscriber discovery";
				await waitForResolved(
					() => {
						expect(
							leaf
								.getSubscribers(topic)
								?.some((key) => key.equals(donor.publicKey)),
						).to.equal(true);
						expect(
							donor
								.getSubscribers(topic)
								?.some((key) => key.equals(leaf.publicKey)),
						).to.equal(true);
					},
					{ timeout: 20_000, signal: controller.signal },
				);
				recoveryStage = "fresh payload delivery";
				await donor.publish(payload, {
					topics: [topic],
					signal: controller.signal,
				});
				await waitForResolved(() => expect(received).to.have.length(1), {
					timeout: 20_000,
					signal: controller.signal,
				});
			})();
			await Promise.race([
				recovery,
				new Promise<never>((_resolve, reject) => {
					deadline = setTimeout(
						() =>
							reject(
								new Error(
									`cold join did not discover and receive from the live donor within 20s (${recoveryStage})`,
								),
							),
						20_000,
					);
				}),
			]);
			expect(received[0]!.data.data).to.deep.equal(payload);
			expect(
				received[0]!.message.header.signatures!.publicKeys[0]!.equals(
					donor.publicKey,
				),
			).to.equal(true);
			expect((relay as any).subscriptions.size).to.equal(0);
			expect(
				leafNode.services.fanout.getChannelStats(
					shardTopic,
					donor.publicKeyHash,
				)?.level,
				"the cold subscriber must join the donor's actual shard overlay",
			).to.be.greaterThan(0);
			console.info("stale-root-cold-join", {
				rustCore: expectedCore !== undefined,
				recoveryMs: Math.round(performance.now() - startedAt),
				donorBecameDirectNeighbor: leaf.peers.has(donor.publicKeyHash),
				parentIsRelay:
					leafNode.services.fanout.getChannelStats(
						shardTopic,
						donor.publicKeyHash,
					)?.parent === relay.publicKeyHash,
			});
		} finally {
			controller.abort();
			clearTimeout(deadline);
			await session.stop();
			await recovery?.catch(() => {});
		}
	});
});
