import { expect } from "chai";
import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import { identify } from "@libp2p/identify";
import { webSockets } from "@libp2p/websockets";
import type { Libp2p } from "@libp2p/interface";
import { createLibp2p } from "libp2p";
import { Peerbit } from "../src/peer.js";

const isNode = typeof process !== "undefined" && !!process.versions?.node;

describe("bootstrap", () => {
	let peer: Peerbit;
	let bootstrapPeer: Peerbit;

	beforeEach(async () => {
		bootstrapPeer = await Peerbit.create();
		peer = await Peerbit.create();
	});

	afterEach(async () => {
		await peer?.stop();
		await bootstrapPeer?.stop();
	});

	(isNode ? it : it.skip)("local", async function () {
		// Root `pnpm -r test` runs many packages concurrently and can delay libp2p dials
		// beyond the default 60s mocha timeout. Bootstrap should still succeed.
		this.timeout(180_000);
		await peer.bootstrap(bootstrapPeer.getMultiaddrs());
		// Bootstrap readiness is transport-level; DirectStream neighbours may establish
		// asynchronously after the dial succeeds.
		expect(peer.libp2p.getConnections(bootstrapPeer.peerId).length).greaterThan(0);
	});

	(isNode ? it : it.skip)("remote", async function () {
		this.timeout(180_000);
		if (process.env.PEERBIT_RUN_REMOTE_BOOTSTRAP_TEST !== "1") {
			this.skip();
		}
		await peer.bootstrap();
		expect(peer.libp2p.getConnections().length).greaterThan(0);
	});

	(isNode ? it : it.skip)(
		"local: does not require Peerbit services readiness",
		async function () {
			// Regression test: bootstrap must not require pubsub/blocks/fanout neighbour
			// readiness on the bootstrap target. A bootstrap peer can be a plain libp2p
			// rendezvous/relay node.
			this.timeout(180_000);

			let bare: Libp2p | undefined;
			try {
				bare = await createLibp2p({
					addresses: { listen: ["/ip4/127.0.0.1/tcp/0/ws"] },
					transports: [webSockets()],
					connectionEncrypters: [noise()],
					streamMuxers: [yamux()],
					services: { identify: identify() },
					connectionMonitor: { enabled: false },
				});
				await bare.start();

				await peer.bootstrap(bare.getMultiaddrs());
				expect(peer.libp2p.getConnections(bare.peerId).length).greaterThan(0);

				const conn = peer.libp2p.getConnections(bare.peerId)[0];
				expect(Boolean(conn?.limits)).to.equal(false);
			} finally {
				await bare?.stop();
			}
		},
	);
});
