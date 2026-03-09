import { type PeerId } from "@libp2p/interface";
import { Ed25519Keypair } from "@peerbit/crypto";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { PeerStreams } from "../src/index.js";

class TestOutboundStream extends EventTarget {
	id = "test-outbound";
	sentPayloads: number[] = [];
	private expectedLength?: number;
	private currentPayloadLength = 0;

	send(bytes: Uint8Array): boolean {
		let completedFrame = false;

		for (const byte of bytes) {
			if (this.expectedLength == null) {
				// 1-byte varint length prefix (our test payloads are <= 127 bytes)
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

		// Block at framed-message boundaries so the test can deterministically interleave writes.
		return completedFrame ? false : true;
	}
}

describe("priority lanes", () => {
	it("preempts queued low-priority traffic with higher priority", async () => {
		const keypair = await Ed25519Keypair.create();
		const streams = new PeerStreams({
			peerId: { toString: () => "test-peer" } as unknown as PeerId,
			publicKey: keypair.publicKey,
			protocol: "/test",
			connId: "test-conn",
		});

		const outbound = new TestOutboundStream();
		await streams.attachOutboundStream(outbound as any);

		// Queue a backlog of low-priority messages first.
		for (let i = 0; i < 5; i++) {
			streams.write(new Uint8Array([i + 1]), 0);
		}

		await waitForResolved(() =>
			expect(outbound.sentPayloads.length).to.equal(1),
		);
		expect(outbound.sentPayloads[0]).to.equal(1);

		// While the stream is blocked on drain, enqueue a high-priority message.
		streams.write(new Uint8Array([200]), 3);

		// Allow the next frame to be flushed.
		outbound.dispatchEvent(new Event("drain"));
		await waitForResolved(() =>
			expect(outbound.sentPayloads.length).to.equal(2),
		);

		// The high-priority message should jump ahead of the remaining low-priority backlog.
		expect(outbound.sentPayloads[1]).to.equal(200);

		// Drain the rest (sanity).
		for (let expected = 3; expected <= 6; expected++) {
			outbound.dispatchEvent(new Event("drain"));
			await waitForResolved(() =>
				expect(outbound.sentPayloads.length).to.equal(expected),
			);
		}

		await streams.close();
	});

	it("applies backpressure to bulk writes while reserving capacity for higher priority traffic", async () => {
		const keypair = await Ed25519Keypair.create();
		const streams = new PeerStreams({
			peerId: { toString: () => "test-peer" } as unknown as PeerId,
			publicKey: keypair.publicKey,
			protocol: "/test",
			connId: "test-conn",
			outboundQueue: {
				maxBufferedBytes: 3,
				reservedPriorityBytes: 1,
			},
		});

		const outbound = new TestOutboundStream();
		await streams.attachOutboundStream(outbound as any);

		await streams.waitForWrite(new Uint8Array([1]), 0);
		await streams.waitForWrite(new Uint8Array([2]), 0);
		await streams.waitForWrite(new Uint8Array([3]), 0);

		await waitForResolved(() =>
			expect(outbound.sentPayloads).to.deep.equal([1]),
		);
		expect(streams.getOutboundQueuedBytes()).to.equal(2);

		let lowResolved = false;
		const blockedLow = streams.waitForWrite(new Uint8Array([4]), 0).then(() => {
			lowResolved = true;
		});

		await delay(50);
		expect(lowResolved).to.equal(false);
		expect(streams.getOutboundQueuedBytes()).to.equal(2);

		await streams.waitForWrite(new Uint8Array([200]), 1);
		expect(streams.getOutboundQueuedBytes()).to.equal(3);

		outbound.dispatchEvent(new Event("drain"));
		await waitForResolved(() =>
			expect(outbound.sentPayloads).to.deep.equal([1, 200]),
		);

		outbound.dispatchEvent(new Event("drain"));
		await waitForResolved(() =>
			expect(outbound.sentPayloads).to.deep.equal([1, 200, 2]),
		);
		await waitForResolved(() => expect(lowResolved).to.equal(true));

		outbound.dispatchEvent(new Event("drain"));
		await waitForResolved(() =>
			expect(outbound.sentPayloads).to.deep.equal([1, 200, 2, 3]),
		);

		outbound.dispatchEvent(new Event("drain"));
		await waitForResolved(() =>
			expect(outbound.sentPayloads).to.deep.equal([1, 200, 2, 3, 4]),
		);

		await blockedLow;
		await streams.close();
	});
});
