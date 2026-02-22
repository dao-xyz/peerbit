import { expect } from "chai";
import {
	InMemoryNetwork,
	publicKeyHash,
} from "@peerbit/libp2p-test-utils/inmemory-libp2p.js";

describe("pubsub in-memory libp2p shim", () => {
	it("createPeer produces unique public keys beyond 256 nodes", () => {
		const network = new InMemoryNetwork();
		const n = 600;
		const hashes = new Set<string>();
		for (let i = 0; i < n; i++) {
			const port = 45_000 + i;
			const { runtime } = InMemoryNetwork.createPeer({ index: i, port, network });
			hashes.add(publicKeyHash(runtime.peerId));
		}
		expect(hashes.size).to.equal(n);
	});

	it("peerStore.get returns multiaddrs for known peers", async () => {
		const network = new InMemoryNetwork();
		const { runtime: a } = InMemoryNetwork.createPeer({
			index: 0,
			port: 46_000,
			network,
		});
		const { runtime: b } = InMemoryNetwork.createPeer({
			index: 1,
			port: 46_001,
			network,
		});
		network.registerPeer(a, 46_000);
		network.registerPeer(b, 46_001);

		const peer = await a.peerStore.get(b.peerId);
		expect(peer.addresses?.length).to.be.greaterThan(0);
		const ma = peer.addresses[0]!.multiaddr;
		expect(ma.toString()).to.include("/tcp/46001");
	});
});
