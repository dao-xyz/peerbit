import { expect } from "chai";
import { InMemorySession } from "../src/inmemory-libp2p.js";

const concat = (...parts: Uint8Array[]) => {
	const bytes = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
	let offset = 0;
	for (const part of parts) {
		bytes.set(part, offset);
		offset += part.length;
	}
	return bytes;
};

const frame = (body: Uint8Array) => {
	let remaining = body.length;
	const prefix: number[] = [];
	do {
		const byte = remaining % 128;
		remaining = Math.floor(remaining / 128);
		prefix.push(byte | (remaining > 0 ? 128 : 0));
	} while (remaining > 0);
	return concat(Uint8Array.from(prefix), body);
};

// The loss filter identifies DATA by the stream/message variants, FOUT id,
// and low priority. Keep the payload separate, as DataMessage.bytes() does.
const dataHeader = () => {
	const header = new Uint8Array(70);
	header.set([0x46, 0x4f, 0x55, 0x54], 2);
	return header;
};

describe("in-memory frame loss", () => {
	let session: InMemorySession;

	afterEach(async () => {
		await session?.stop();
	});

	const open = async (streamHighWaterMarkBytes?: number) => {
		session = await InMemorySession.disconnected(2, {
			networkOpts: { dropDataFrameRate: 1, streamHighWaterMarkBytes },
		});
		const [a, b] = session.peers;
		const protocol = "/inmemory-framing/1.0.0";
		let inbound: any;
		await b.components.registrar.handle(protocol, async (stream) => {
			inbound = stream;
		});
		const connection = await a.dial(b.getMultiaddrs());
		const outbound = await connection.newStream(protocol, {
			negotiateFully: true,
		});
		return { outbound, inbound };
	};

	const readClosed = async (outbound: any, inbound: any) => {
		await outbound.close();
		const chunks: Uint8Array[] = [];
		for await (const chunk of inbound) chunks.push(chunk);
		return chunks;
	};

	it("drops the entire segmented DATA frame without corrupting following control", async () => {
		const { outbound, inbound } = await open();
		const header = dataHeader();
		const payload = new Uint8Array([6, 7, 8]);
		const control = frame(new Uint8Array([9, 10]));
		outbound.send(new Uint8Array([header.length + payload.length]));
		outbound.send(header);
		outbound.send(payload);
		outbound.send(control);

		expect(await readClosed(outbound, inbound)).to.deep.equal([control]);
		expect(session.network.metrics.framesSent).to.equal(2);
		expect(session.network.metrics.framesDropped).to.equal(1);
		expect(session.network.metrics.bytesDropped).to.equal(74);
		expect(session.network.metrics.bytesSent).to.equal(74 + control.length);
	});

	it("reassembles arbitrary prefix and body fragments before counting or delivering", async () => {
		const { outbound, inbound } = await open();
		const body = new Uint8Array(300).fill(9);
		const encoded = frame(body);
		for (const byte of encoded.subarray(0, encoded.length - 1)) {
			expect(outbound.send(new Uint8Array([byte]))).to.equal(true);
			expect(session.network.metrics.framesSent).to.equal(0);
		}
		outbound.send(encoded.subarray(-1));
		expect(await readClosed(outbound, inbound)).to.deep.equal([encoded]);
		expect(session.network.metrics.framesSent).to.equal(1);
		expect(session.network.metrics.bytesSent).to.equal(encoded.length);
	});

	it("handles coalesced delivered, dropped, and empty frames individually", async () => {
		const { outbound, inbound } = await open();
		const first = frame(new Uint8Array([9, 10]));
		const dropped = frame(concat(dataHeader(), new Uint8Array([6, 7, 8])));
		const empty = frame(new Uint8Array());
		const last = frame(new Uint8Array([11, 12]));
		outbound.send(concat(first, dropped, empty, last));
		expect(await readClosed(outbound, inbound)).to.deep.equal([first, empty, last]);
		expect(session.network.metrics.framesSent).to.equal(4);
		expect(session.network.metrics.framesDropped).to.equal(1);
		expect(session.network.metrics.bytesDropped).to.equal(dropped.length);
	});

	it("preserves backpressure across coalesced frames and a trailing dropped frame", async () => {
		const { outbound, inbound } = await open(4);
		const first = frame(new Uint8Array([9, 10]));
		const last = frame(new Uint8Array([11, 12]));
		const dropped = frame(dataHeader());
		let drains = 0;
		outbound.addEventListener("drain", () => drains++);
		expect(outbound.send(concat(first, last, dropped))).to.equal(false);
		const reader = inbound[Symbol.asyncIterator]();
		expect((await reader.next()).value).to.deep.equal(first);
		expect(drains).to.equal(0);
		expect((await reader.next()).value).to.deep.equal(last);
		expect(drains).to.equal(1);
		expect(outbound.send(dropped)).to.equal(true);
		await outbound.close();
		expect((await reader.next()).done).to.equal(true);
	});

	it("discards unfinished frames on close without delivery or retained chunks", async () => {
		const { outbound, inbound } = await open();
		outbound.send(new Uint8Array([100]));
		outbound.send(new Uint8Array([9, 10]));
		expect(await readClosed(outbound, inbound)).to.deep.equal([]);
		expect(session.network.metrics.framesSent).to.equal(0);
		expect((outbound as any).pendingFrameChunks).to.deep.equal([]);
		expect((outbound as any).pendingLengthPrefix).to.deep.equal([]);
		expect(() => outbound.send(new Uint8Array([0]))).to.throw("closed stream");
	});

	it("rejects overflowing length prefixes without allocating their advertised size", async () => {
		const { outbound, inbound } = await open();
		expect(() => outbound.send(new Uint8Array(8).fill(0xff))).to.throw(
			"varint overflow",
		);
		expect(session.network.metrics.framesSent).to.equal(0);
		expect(await readClosed(outbound, inbound)).to.deep.equal([]);
	});
});
