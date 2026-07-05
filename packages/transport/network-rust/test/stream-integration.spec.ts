import { TestSession } from "@peerbit/libp2p-test-utils";
import {
	AcknowledgeDelivery,
	type DataMessage,
	SilentDelivery,
} from "@peerbit/stream-interface";
import {
	DirectStream,
	type DirectStreamComponents,
	type DirectStreamOptions,
	waitForNeighbour,
} from "@peerbit/stream";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { type NativeWireModule, createNativeWire } from "../src/index.js";

class TestWireStream extends DirectStream {
	constructor(
		components: DirectStreamComponents,
		options?: DirectStreamOptions,
	) {
		super(components, ["/peerbit-wire-test/0.0.0"], options);
	}
}

type Session = TestSession<{ directstream: TestWireStream }>;

describe("directstream nativeWire integration", () => {
	let wire: NativeWireModule;
	let session: Session;
	let nativeBatchCalls: number;
	let nativeFramesSeen: number;

	before(async () => {
		wire = await createNativeWire();
	});

	beforeEach(() => {
		nativeBatchCalls = 0;
		nativeFramesSeen = 0;
	});

	afterEach(async () => {
		await session?.stop();
	});

	const connect = async () => {
		// count native invocations to prove the wasm path (not the TS
		// fallback) is what handles the inbound frames
		const countingWire = {
			decodeAndVerifyBatch: (frames: Uint8Array[], nowMs: number) => {
				nativeBatchCalls += 1;
				nativeFramesSeen += frames.length;
				return wire.decodeAndVerifyBatch(frames, nowMs);
			},
		};
		session = await TestSession.connected(2, {
			services: {
				directstream: (components: DirectStreamComponents) =>
					new TestWireStream(components, {
						nativeWire: countingWire,
						connectionManager: false,
					}),
			},
		});
		const [a, b] = session.peers.map((peer) => peer.services.directstream);
		await waitForNeighbour(a, b);
		return [a, b] as const;
	};

	it("delivers and verifies data with the native wire path", async () => {
		const [a, b] = await connect();
		const received: DataMessage[] = [];
		b.addEventListener("data", (event) => {
			received.push(event.detail);
		});
		const payload = new Uint8Array([1, 2, 3, 4]);
		const framesBefore = nativeFramesSeen;
		await a.publish(payload, {
			mode: new SilentDelivery({ to: [b.publicKeyHash], redundancy: 1 }),
		});
		await waitForResolved(() => expect(received).to.have.length(1));
		expect([...received[0].data!]).to.deep.equal([...payload]);
		// the native batch verifier seeded the memoized verification result
		expect(received[0]._verified).to.equal(true);
		expect(nativeBatchCalls).to.be.greaterThan(0);
		expect(nativeFramesSeen).to.be.greaterThan(framesBefore);
	});

	it("acknowledges messages received through the native wire path", async () => {
		const [a, b] = await connect();
		// AcknowledgeDelivery resolves only after the ACK round-trip; both
		// the DataMessage (at b) and the ACK (at a) travel the native path.
		await a.publish(new Uint8Array([9]), {
			mode: new AcknowledgeDelivery({
				to: [b.publicKeyHash],
				redundancy: 1,
			}),
		});
	});

	it("drops frames with tampered signatures", async () => {
		const [a, b] = await connect();
		const received: DataMessage[] = [];
		b.addEventListener("data", (event) => {
			received.push(event.detail);
		});

		// Publish through the normal signing pipeline, but corrupt the
		// signature bytes on the wire by intercepting the outbound frame.
		const peerStreams = a.peers.get(b.publicKeyHash)!;
		const originalWrite = peerStreams.write.bind(peerStreams);
		let corrupted = 0;
		peerStreams.write = (bytes, priority) => {
			const array =
				bytes instanceof Uint8Array ? bytes.slice() : bytes.subarray().slice();
			// flip a byte inside the signed payload region at the very end of
			// the frame; the signature no longer matches
			array[array.length - 1] ^= 0xff;
			corrupted += 1;
			return originalWrite(array, priority);
		};

		await a.publish(new Uint8Array([1, 2, 3]), {
			mode: new SilentDelivery({ to: [b.publicKeyHash], redundancy: 1 }),
		});
		expect(corrupted).to.equal(1);

		// give the frame time to arrive and be rejected
		await new Promise((resolve) => setTimeout(resolve, 500));
		expect(received).to.have.length(0);
	});
});
