import { expect } from "chai";
import sinon from "sinon";
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

	it("reports partial failures when one bootstrap peer is reachable and another is not", async function () {
		this.timeout(180_000);
		const unreachable = `/ip4/127.0.0.1/tcp/1/ws/p2p/12D3KooWUnreachablePeer111111111111111111111111111111`;
		const result = await peer.bootstrap([
			...bootstrapPeer.getMultiaddrs().map((addr) => addr.toString()),
			unreachable,
		]);

		expect(result.connectedPeerIds).to.include(bootstrapPeer.peerId.toString());
		expect(result.failures).to.have.length(1);
		expect(result.failures[0]?.peerId).to.equal(
			"12D3KooWUnreachablePeer111111111111111111111111111111",
		);
	});

	it("does not report a failure when a fallback address for the same bootstrap peer succeeds", async function () {
		this.timeout(180_000);
		const valid = bootstrapPeer.getMultiaddrs()[0]!.toString();
		const invalidSamePeer = `/ip4/127.0.0.1/tcp/1/ws/p2p/${bootstrapPeer.peerId.toString()}`;
		const result = await peer.bootstrap([invalidSamePeer, valid]);

		expect(result.connectedPeerIds).to.include(bootstrapPeer.peerId.toString());
		expect(result.failures).to.deep.equal([]);
	});

	it("reports unknown-address failures without a bootstrap peer id", async function () {
		this.timeout(180_000);
		const unknown = "/dns4/node-a.peerchecker.com/tcp/1/ws";
		const originalDial = peer.dial.bind(peer);
		const dialStub = sinon
			.stub(peer, "dial")
			.callsFake(async (address, ...args: any[]) => {
				const value =
					typeof address === "string" ? address : (address as any).toString();
				if (value === unknown) {
					throw "forced-string-reason";
				}
				return await (originalDial as any)(address, ...args);
			});

		try {
			const result = await peer.bootstrap([
				...bootstrapPeer.getMultiaddrs().map((addr) => addr.toString()),
				unknown,
			]);

			expect(result.connectedPeerIds).to.include(bootstrapPeer.peerId.toString());
			expect(result.failures).to.deep.equal([
				{
					peerId: undefined,
					reason: "forced-string-reason",
				},
			]);
		} finally {
			dialStub.restore();
		}
	});
});
