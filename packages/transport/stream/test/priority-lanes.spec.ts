import { type PeerId } from "@libp2p/interface";
import { Ed25519Keypair } from "@peerbit/crypto";
import { AbortError, delay, waitForResolved } from "@peerbit/time";
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

	it("does not permanently stall when a stream misses drain", async () => {
		const keypair = await Ed25519Keypair.create();
		const streams = new PeerStreams({
			peerId: { toString: () => "test-peer" } as unknown as PeerId,
			publicKey: keypair.publicKey,
			protocol: "/test",
			connId: "test-conn",
		});

		const outbound = new TestOutboundStream();
		await streams.attachOutboundStream(outbound as any);

		streams.write(new Uint8Array([1]), 0);
		await waitForResolved(() =>
			expect(outbound.sentPayloads).to.deep.equal([1]),
		);

		streams.write(new Uint8Array([200]), 3);
		await waitForResolved(() =>
			expect(outbound.sentPayloads).to.deep.equal([1, 200]),
		);

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

	it("rejects a capacity-blocked write when the peer stream closes", async () => {
		const keypair = await Ed25519Keypair.create();
		const streams = new PeerStreams({
			peerId: { toString: () => "test-peer" } as unknown as PeerId,
			publicKey: keypair.publicKey,
			protocol: "/test",
			connId: "test-conn",
			outboundQueue: {
				maxBufferedBytes: 1,
				reservedPriorityBytes: 0,
			},
		});

		const outbound = new TestOutboundStream();
		await streams.attachOutboundStream(outbound as any);

		// Keep the raw stream blocked after its first frame, then fill the one-byte
		// outbound queue behind it.
		await streams.waitForWrite(new Uint8Array([1]), 0);
		await waitForResolved(() =>
			expect(outbound.sentPayloads).to.deep.equal([1]),
		);
		await streams.waitForWrite(new Uint8Array([2]), 0);
		expect(streams.getOutboundQueuedBytes()).to.equal(1);

		// Observe entry into the real capacity waiter so closing cannot race with
		// the setup of this regression.
		const queue = streams._getActiveOutboundPushable()!;
		const originalOnBufferedBelow = queue.onBufferedBelow.bind(queue);
		let capacityWaitStartedResolve!: () => void;
		const capacityWaitStarted = new Promise<void>((resolve) => {
			capacityWaitStartedResolve = resolve;
		});
		queue.onBufferedBelow = (limitBytes, options) => {
			capacityWaitStartedResolve();
			return originalOnBufferedBelow(limitBytes, options);
		};

		const blockedResult: Promise<unknown | undefined> = streams
			.waitForWrite(new Uint8Array([3]), 0)
			.then(
				(): undefined => undefined,
				(error: unknown): unknown => error,
			);
		await capacityWaitStarted;

		await streams.close();
		expect(await blockedResult).to.be.instanceOf(AbortError);
	});

	it("rechecks closure after outbound readiness wakes a write", async () => {
		const keypair = await Ed25519Keypair.create();
		const streams = new PeerStreams({
			peerId: { toString: () => "test-peer" } as unknown as PeerId,
			publicKey: keypair.publicKey,
			protocol: "/test",
			connId: "test-conn",
		});

		const writeResult: Promise<unknown | undefined> = streams
			.waitForWrite(new Uint8Array([1]), 0)
			.then(
				(): undefined => undefined,
				(error: unknown): unknown => error,
			);

		// waitForWrite registers its outbound listener synchronously. Wake that
		// listener, then close before its promise continuation can run. The ended
		// pushable must not make the pending write look successful.
		const outbound = new TestOutboundStream();
		const attaching = streams.attachOutboundStream(outbound as any);
		const closing = streams.close();

		await attaching;
		await closing;
		expect(await writeResult).to.be.instanceOf(AbortError);
		expect(outbound.sentPayloads).to.deep.equal([]);
	});

	it("rejects a direct write while the peer stream is closing", async () => {
		const keypair = await Ed25519Keypair.create();
		const streams = new PeerStreams({
			peerId: { toString: () => "test-peer" } as unknown as PeerId,
			publicKey: keypair.publicKey,
			protocol: "/test",
			connId: "test-conn",
		});

		const outbound = new TestOutboundStream();
		await streams.attachOutboundStream(outbound as any);

		// close() marks the PeerStreams closed and ends its pushable before its
		// first await. A direct write in that interval must not count the ended
		// queue's no-op push as a successful delivery.
		const closing = streams.close();
		expect(() => streams.write(new Uint8Array([1]), 0)).to.throw(AbortError);

		await closing;
		expect(outbound.sentPayloads).to.deep.equal([]);
	});
});
