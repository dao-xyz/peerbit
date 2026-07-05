import { Ed25519Keypair } from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import {
	BackpressureError,
	DirectStream,
	type DirectStreamComponents,
	type DirectStreamOptions,
	Routes,
	type RustCoreStream,
	waitForNeighbour,
} from "@peerbit/stream";
import {
	AcknowledgeDelivery,
	type DataMessage,
	SilentDelivery,
} from "@peerbit/stream-interface";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { createRustCoreStream } from "../src/index.js";

class TestMixedStream extends DirectStream {
	constructor(
		components: DirectStreamComponents,
		options?: DirectStreamOptions,
	) {
		super(components, ["/peerbit-mixed-test/0.0.0"], options);
	}
}

type Session = TestSession<{ directstream: TestMixedStream }>;

/**
 * Minimal libp2p Stream stand-in that reports backpressure on every frame
 * (send → false) until an explicit "drain" event, mirroring the
 * BlockingOutboundStream helper in the @peerbit/stream suite.
 */
class BlockingOutboundStream extends EventTarget {
	id: string;
	protocol = "/peerbit-mixed-test/0.0.0";
	sentPayloads: number[] = [];
	private expectedLength?: number;
	private currentPayloadLength = 0;

	constructor(id: string) {
		super();
		this.id = id;
	}

	send(bytes: Uint8Array): boolean {
		let completedFrame = false;
		for (const byte of bytes) {
			if (this.expectedLength == null) {
				this.expectedLength = byte;
				this.currentPayloadLength = 0;
				continue;
			}
			this.currentPayloadLength += 1;
			if (this.currentPayloadLength === this.expectedLength) {
				this.sentPayloads.push(byte);
				this.expectedLength = undefined;
				this.currentPayloadLength = 0;
				completedFrame = true;
			}
		}
		return completedFrame ? false : true;
	}

	abort() {}

	async close() {}
}

describe("directstream rust-core mixed topology", () => {
	let core: RustCoreStream;
	let session: Session;

	before(async () => {
		core = await createRustCoreStream();
	});

	afterEach(async () => {
		await session?.stop();
	});

	const streamOf = (index: number) =>
		session.peers[index].services.directstream;

	const connectMixedLine = async () => {
		// rust-core peer — default relay — default peer
		session = await TestSession.disconnected(3, [
			{
				services: {
					directstream: (components: DirectStreamComponents) =>
						new TestMixedStream(components, {
							connectionManager: false,
							rustCore: core,
						}),
				},
			},
			{
				services: {
					directstream: (components: DirectStreamComponents) =>
						new TestMixedStream(components, {
							connectionManager: false,
							rustCore: false,
						}),
				},
			},
			{
				services: {
					directstream: (components: DirectStreamComponents) =>
						new TestMixedStream(components, {
							connectionManager: false,
							rustCore: false,
						}),
				},
			},
		]);
		await session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]],
		]);
		await waitForNeighbour(streamOf(0), streamOf(1));
		await waitForNeighbour(streamOf(1), streamOf(2));

		// prove the modes actually differ: the rust-core peer runs the native
		// routing table, the default peers the TS Routes class
		expect(streamOf(0).routes).to.not.be.instanceOf(Routes);
		expect(streamOf(1).routes).to.be.instanceOf(Routes);
		expect(streamOf(2).routes).to.be.instanceOf(Routes);
	};

	it("relays, acknowledges and learns routes across implementations", async () => {
		await connectMixedLine();
		const rustPeer = streamOf(0);
		const relay = streamOf(1);
		const jsPeer = streamOf(2);

		const receivedAtJs: DataMessage[] = [];
		jsPeer.addEventListener("data", (message) => {
			receivedAtJs.push(message.detail);
		});
		const receivedAtRust: DataMessage[] = [];
		rustPeer.addEventListener("data", (message) => {
			receivedAtRust.push(message.detail);
		});

		// rust-core → default via the default relay; resolves only when the
		// ACK has been routed back along the trace (relay → rust peer)
		await rustPeer.publish(new Uint8Array([1, 2, 3]), {
			mode: new AcknowledgeDelivery({
				to: [jsPeer.publicKeyHash],
				redundancy: 1,
			}),
		});
		await waitForResolved(() => expect(receivedAtJs).to.have.length(1));
		expect([...receivedAtJs[0].data!]).to.deep.equal([1, 2, 3]);

		// the ACK taught the rust-core peer a multi-hop route through the relay
		await waitForResolved(() => {
			expect(
				rustPeer.routes.isReachable(
					rustPeer.publicKeyHash,
					jsPeer.publicKeyHash,
				),
			).to.be.true;
		});
		const learned = rustPeer.routes.findNeighbor(
			rustPeer.publicKeyHash,
			jsPeer.publicKeyHash,
		);
		expect(learned?.list.map((relayInfo) => relayInfo.hash)).to.include(
			relay.publicKeyHash,
		);

		// default → rust-core direction over the same relay
		await jsPeer.publish(new Uint8Array([4, 5]), {
			mode: new AcknowledgeDelivery({
				to: [rustPeer.publicKeyHash],
				redundancy: 1,
			}),
		});
		await waitForResolved(() => expect(receivedAtRust).to.have.length(1));
		expect([...receivedAtRust[0].data!]).to.deep.equal([4, 5]);
		await waitForResolved(() => {
			expect(
				jsPeer.routes.isReachable(jsPeer.publicKeyHash, rustPeer.publicKeyHash),
			).to.be.true;
		});

		// with routes in place, silent delivery follows the learned fanout
		await rustPeer.publish(new Uint8Array([9]), {
			mode: new SilentDelivery({ to: [jsPeer.publicKeyHash], redundancy: 1 }),
		});
		await waitForResolved(() => expect(receivedAtJs).to.have.length(2));
		expect([...receivedAtJs[1].data!]).to.deep.equal([9]);
	});

	it("delivers acknowledge-anywhere floods exactly once", async () => {
		await connectMixedLine();
		const rustPeer = streamOf(0);
		const jsPeer = streamOf(2);

		const receivedAtRust: DataMessage[] = [];
		rustPeer.addEventListener("data", (message) => {
			receivedAtRust.push(message.detail);
		});

		// default publish = AcknowledgeAnyWhere discovery flood; the rust-core
		// peer acknowledges (trace routed back through the default relay) and
		// dispatches the payload exactly once via the native seen-cache
		await jsPeer.publish(new Uint8Array([7]));
		await waitForResolved(() => expect(receivedAtRust).to.have.length(1));
		await delay(300);
		expect(receivedAtRust).to.have.length(1);
	});

	it("applies native lane backpressure with BackpressureError and drain", async () => {
		session = await TestSession.disconnected(1, {
			services: {
				directstream: (components: DirectStreamComponents) =>
					new TestMixedStream(components, {
						connectionManager: false,
						rustCore: core,
					}),
			},
		});
		const writer = streamOf(0);
		// the constructor clamps maxBufferedBytes to MAX_DATA_LENGTH_OUT, so
		// (like the @peerbit/stream suite) shrink the budget post-construction
		(writer as any).outboundQueueOptions = {
			maxBufferedBytes: 4,
			reservedPriorityBytes: 1,
			maxTotalBufferedBytes: 4,
			reservedTotalPriorityBytes: 1,
		};
		const remoteKey = await Ed25519Keypair.create();
		const peer = writer.addPeer(
			{ toString: () => "peer-blocked" } as any,
			remoteKey.publicKey,
			"/peerbit-mixed-test/0.0.0",
			"conn-blocked",
		);
		const outbound = new BlockingOutboundStream("peer-blocked");
		await peer.attachOutboundStream(outbound as any);

		// first frame goes straight to the (blocking) socket
		peer.write(new Uint8Array([1]), 0);
		await waitForResolved(() =>
			expect(outbound.sentPayloads).to.deep.equal([1]),
		);

		// background lane (priority 0) admission limit is 4 - 1 reserved = 3
		peer.write(new Uint8Array([11]), 0);
		peer.write(new Uint8Array([12]), 0);
		peer.write(new Uint8Array([13]), 0);
		expect(peer.getOutboundQueuedBytes()).to.equal(3);
		expect(() => peer.write(new Uint8Array([14]), 0)).to.throw(
			BackpressureError,
		);

		// priority traffic may still use the reserved byte
		peer.write(new Uint8Array([99]), 3);
		expect(peer.getOutboundQueuedBytes()).to.equal(4);
		expect(() => peer.write(new Uint8Array([100]), 3)).to.throw();

		// blocked low-priority write resolves once the queue drains below the
		// admission threshold (onBufferedBelow on the native lane sizes)
		let blockedResolved = false;
		const blocked = peer
			.waitForWrite(new Uint8Array([15]), 0)
			.then(() => (blockedResolved = true));
		await delay(50);
		expect(blockedResolved).to.equal(false);

		// the priority frame drains first (ACK/control lane beats background)
		outbound.dispatchEvent(new Event("drain"));
		await waitForResolved(() =>
			expect(outbound.sentPayloads).to.deep.equal([1, 99]),
		);

		outbound.dispatchEvent(new Event("drain"));
		await waitForResolved(() =>
			expect(outbound.sentPayloads).to.deep.equal([1, 99, 11]),
		);
		await waitForResolved(() => expect(blockedResolved).to.equal(true));

		outbound.dispatchEvent(new Event("drain"));
		await waitForResolved(() =>
			expect(outbound.sentPayloads).to.deep.equal([1, 99, 11, 12]),
		);
		outbound.dispatchEvent(new Event("drain"));
		await waitForResolved(() =>
			expect(outbound.sentPayloads).to.deep.equal([1, 99, 11, 12, 13]),
		);
		outbound.dispatchEvent(new Event("drain"));
		await waitForResolved(() =>
			expect(outbound.sentPayloads).to.deep.equal([1, 99, 11, 12, 13, 15]),
		);
		await blocked;
	});
});
