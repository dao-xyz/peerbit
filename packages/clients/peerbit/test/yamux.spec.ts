import { yamux } from "@chainsafe/libp2p-yamux";
import { sha256Sync } from "@peerbit/crypto";
import { expect } from "chai";
import {
	PEERBIT_YAMUX_PROTOCOL,
	Peerbit,
	createPeerbitStreamMuxers,
} from "../src/index.js";

const STANDARD_YAMUX_PROTOCOL = "/yamux/1.0.0";
const isNode = typeof process !== "undefined" && !!process.versions?.node;

const deterministicBlock = (size: number): Uint8Array => {
	const bytes = new Uint8Array(size);
	for (let i = 0; i < bytes.length; i++) {
		bytes[i] = (i * 31 + (i >>> 8) * 17 + 23) & 0xff;
	}
	return bytes;
};

const connectionMuxer = (
	client: Peerbit,
	remote: Peerbit,
): string | undefined =>
	client.libp2p
		.getConnections()
		.find((connection) => connection.remotePeer.equals(remote.peerId))
		?.multiplexer;

const expectBlockTransfer = async (provider: Peerbit, reader: Peerbit) => {
	const source = deterministicBlock(5 * 1024 * 1024 + 17);
	const cid = await provider.services.blocks.put(source);
	const received = await reader.services.blocks.get(cid, {
		remote: {
			from: [provider.services.blocks.publicKeyHash],
			timeout: 60_000,
		},
	});

	expect(received).to.exist;
	expect(received!.byteLength).to.equal(source.byteLength);
	expect([...sha256Sync(received!)]).to.deep.equal([...sha256Sync(source)]);
};

describe("Peerbit Yamux profile", function () {
	it("offers the Peerbit profile before standard Yamux", () => {
		const factories = createPeerbitStreamMuxers();
		expect(factories.map((factory) => factory().protocol)).to.deep.equal([
			PEERBIT_YAMUX_PROTOCOL,
			STANDARD_YAMUX_PROTOCOL,
		]);
	});

	if (!isNode) return;

	it("negotiates the larger-window profile between updated peers", async function () {
		this.timeout(90_000);
		const peers = [
			await Peerbit.create({ relay: false }),
			await Peerbit.create({ relay: false }),
		] as const;

		try {
			await peers[0].dial(peers[1].getMultiaddrs()[0]!);
			expect(connectionMuxer(peers[0], peers[1])).to.equal(
				PEERBIT_YAMUX_PROTOCOL,
			);
			expect(connectionMuxer(peers[1], peers[0])).to.equal(
				PEERBIT_YAMUX_PROTOCOL,
			);
			await expectBlockTransfer(peers[0], peers[1]);
		} finally {
			await Promise.all(peers.map((peer) => peer.stop()));
		}
	});

	for (const legacyDialer of [false, true]) {
		it(`falls back safely when the ${legacyDialer ? "legacy" : "updated"} peer dials`, async function () {
			this.timeout(120_000);
			const updated = await Peerbit.create({ relay: false });
			const legacy = await Peerbit.create({
				relay: false,
				libp2p: { streamMuxers: [yamux()] },
			});

			try {
				const dialer = legacyDialer ? legacy : updated;
				const listener = legacyDialer ? updated : legacy;
				await dialer.dial(listener.getMultiaddrs()[0]!);
				expect(connectionMuxer(updated, legacy)).to.equal(
					STANDARD_YAMUX_PROTOCOL,
				);
				expect(connectionMuxer(legacy, updated)).to.equal(
					STANDARD_YAMUX_PROTOCOL,
				);

				// Exercise the standard 256 KiB flow-control path in both data
				// directions with responses larger than either initial window.
				await expectBlockTransfer(updated, legacy);
				await expectBlockTransfer(legacy, updated);
			} finally {
				await Promise.all([updated.stop(), legacy.stop()]);
			}
		});
	}
});
