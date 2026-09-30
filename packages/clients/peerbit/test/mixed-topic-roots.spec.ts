import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { Peerbit } from "../src/peer.js";

const isNode = typeof process !== "undefined" && !!process.versions?.node;

describe("mixed bootstrap and dial topic roots", function () {
	this.timeout(30_000);
	let peers: Peerbit[] = [];

	const createPeer = async () => {
		const peer = await Peerbit.create();
		peers.push(peer);
		return peer;
	};

	afterEach(async () => {
		await Promise.all(peers.map((peer) => peer.stop()));
		peers = [];
	});

	const createBootstrappedPeer = async () => {
		const root = await createPeer();
		const rootHash = root.services.pubsub.publicKeyHash;
		// A local bootstrap hosts one stable explicit root, so this never depends
		// on public bootstrap availability or automatic root convergence.
		root.services.pubsub.setTopicRootCandidates([rootHash]);
		await root.services.pubsub.hostShardRootsNow();
		const bootstrapped = await createPeer();
		await bootstrapped.bootstrap(root.getMultiaddrs());
		return { rootHash, bootstrapped };
	};

	for (const subscribeBeforeDial of [false, true]) {
		(isNode ? it : it.skip)(
			`discovers subscriptions when the dial-only peer subscribes ${subscribeBeforeDial ? "before" : "after"} dialing`,
			async () => {
				const { rootHash, bootstrapped } = await createBootstrappedPeer();
				const dialOnly = await createPeer();
				const topic = `mixed-root-mode-${subscribeBeforeDial}`;
				await bootstrapped.services.pubsub.subscribe(topic);
				if (subscribeBeforeDial) {
					await dialOnly.services.pubsub.subscribe(topic);
				}
				await dialOnly.dial(bootstrapped.getMultiaddrs()[0]!);
				if (!subscribeBeforeDial) {
					await dialOnly.services.pubsub.subscribe(topic);
				}

				// Discovery must work without an application requestSubscribers call,
				// even though explicit bootstrap policy and automatic policy differ.
				await waitForResolved(
					() => {
						for (const [local, remote] of [
							[bootstrapped, dialOnly],
							[dialOnly, bootstrapped],
						]) {
							expect(
								local.services.pubsub
									.getSubscribers(topic)
									?.map((key) => key.hashcode()),
							).to.include(remote.services.pubsub.publicKeyHash);
						}
					},
					{ timeout: 10_000 },
				);

				expect(
					bootstrapped.services.pubsub.topicRootControlPlane.getTopicRootCandidates(),
				).to.deep.equal([rootHash]);
				expect(
					dialOnly.services.pubsub.topicRootControlPlane.getTopicRootCandidates(),
				).to.deep.equal([dialOnly.services.pubsub.publicKeyHash]);
			},
		);
	}

	(isNode ? it : it.skip)(
		"removes and restores direct subscriptions in both root modes",
		async () => {
			const { rootHash, bootstrapped } = await createBootstrappedPeer();
			const dialOnly = await createPeer();
			const topic = "mixed-root-subscription-lifecycle";
			await bootstrapped.services.pubsub.subscribe(topic);
			await dialOnly.dial(bootstrapped.getMultiaddrs()[0]!);
			await dialOnly.services.pubsub.subscribe(topic);
			const subscriberHashes = (peer: Peerbit) =>
				peer.services.pubsub
					.getSubscribers(topic)
					?.map((key) => key.hashcode()) ?? [];
			const expectBothSubscribers = () => {
				const expected = [
					bootstrapped.services.pubsub.publicKeyHash,
					dialOnly.services.pubsub.publicKeyHash,
				];
				expect(subscriberHashes(bootstrapped)).to.have.members(expected);
				expect(subscriberHashes(dialOnly)).to.have.members(expected);
			};
			await waitForResolved(expectBothSubscribers, { timeout: 10_000 });

			// Each side stays connected while it leaves and re-enters the topic.
			// A shard-only unsubscribe cannot reach the other root mode.
			for (const [leaving, remaining] of [
				[dialOnly, bootstrapped],
				[bootstrapped, dialOnly],
			]) {
				await leaving.services.pubsub.unsubscribe(topic);
				await waitForResolved(
					() => {
						expect(
							subscriberHashes(remaining),
							`${leaving === dialOnly ? "dial-only" : "bootstrapped"} peer unsubscribed`,
						).to.deep.equal([remaining.services.pubsub.publicKeyHash]);
					},
					{ timeout: 10_000 },
				);
				await leaving.services.pubsub.subscribe(topic);
				await waitForResolved(expectBothSubscribers, { timeout: 10_000 });
			}

			expect(
				bootstrapped.services.pubsub.topicRootControlPlane.getTopicRootCandidates(),
			).to.deep.equal([rootHash]);
			expect(
				dialOnly.services.pubsub.topicRootControlPlane.getTopicRootCandidates(),
			).to.deep.equal([dialOnly.services.pubsub.publicKeyHash]);
		},
	);

	(isNode ? it : it.skip)(
		"resolves a gateway's shard root after explicitly selecting leaf mode",
		async () => {
			const { rootHash, bootstrapped } = await createBootstrappedPeer();
			const leaf = await createPeer();
			// This existing opt-in delegates root resolution to connected peers; it
			// does not copy or automatically trust a neighbor's candidate list.
			leaf.services.pubsub.setTopicRootCandidates([]);
			await leaf.dial(bootstrapped.getMultiaddrs()[0]!);
			const topic = "explicit-leaf-root-resolution";
			const shardTopic = (
				leaf.services.pubsub as any
			).getShardTopicForUserTopic(topic);
			expect(await leaf.services.pubsub.resolveTopicRoot(shardTopic)).to.equal(
				rootHash,
			);
			await bootstrapped.services.pubsub.subscribe(topic);
			await leaf.services.pubsub.subscribe(topic);
			await waitForResolved(
				() => {
					expect(
						leaf.services.pubsub
							.getSubscribers(topic)
							?.map((key) => key.hashcode()),
					).to.include(bootstrapped.services.pubsub.publicKeyHash);
				},
				{ timeout: 10_000 },
			);
			expect(
				leaf.services.pubsub.topicRootControlPlane.getTopicRootCandidates(),
			).to.deep.equal([]);
		},
	);
});
