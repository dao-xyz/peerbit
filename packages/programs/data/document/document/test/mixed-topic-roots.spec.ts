import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { Peerbit } from "peerbit";
import { Documents } from "../src/program.js";
import { Document, TestStore } from "./data.js";

const isNode = typeof process !== "undefined" && !!process.versions?.node;

describe("Documents with mixed bootstrap and dial topic roots", function () {
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

	(isNode ? it : it.skip)(
		"replicates from a bootstrapped writer to a peer that only dials it",
		async () => {
			const root = await createPeer();
			const rootHash = root.services.pubsub.publicKeyHash;
			root.services.pubsub.setTopicRootCandidates([rootHash]);
			await root.services.pubsub.hostShardRootsNow();
			const writer = await createPeer();
			await writer.bootstrap(root.getMultiaddrs());
			const source = await writer.open(
				new TestStore({ docs: new Documents<Document>() }),
				{ args: { replicate: { factor: 1 } } },
			);
			const document = new Document({
				id: "x",
				name: "mixed-root-replication",
			});
			await source.docs.put(document);

			const reader = await createPeer();
			await reader.dial(writer.getMultiaddrs()[0]!);
			const replica = await reader.open<TestStore>(source.address, {
				args: { replicate: { factor: 1 } },
			});

			// Do not issue remote reads or application subscription requests: the
			// document must arrive through ordinary replication after open by address.
			await waitForResolved(
				async () => {
					expect(await replica.docs.index.getSize()).to.equal(1);
				},
				{ timeout: 10_000 },
			);
			expect(
				(await replica.docs.index.get(document.id, { remote: false }))?.name,
			).to.equal(document.name);
			expect(
				writer.services.pubsub.topicRootControlPlane.getTopicRootCandidates(),
			).to.deep.equal([rootHash]);
			expect(
				reader.services.pubsub.topicRootControlPlane.getTopicRootCandidates(),
			).to.deep.equal([reader.services.pubsub.publicKeyHash]);
		},
	);
});
