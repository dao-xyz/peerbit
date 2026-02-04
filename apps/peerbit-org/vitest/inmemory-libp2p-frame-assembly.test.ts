import { AnyWhere, DataMessage, MessageHeader } from "@peerbit/stream-interface";
import { describe, expect, it } from "vitest";

import { InMemoryNetwork } from "../src/sim/inmemory-libp2p.js";

const BENCH_ID_PREFIX = Uint8Array.from([0x50, 0x53, 0x49, 0x4d]); // "PSIM"

const encodeUVarint = (value: number): Uint8Array => {
	let x = value >>> 0;
	const out: number[] = [];
	while (x >= 0x80) {
		out.push((x & 0x7f) | 0x80);
		x >>>= 7;
	}
	out.push(x);
	return Uint8Array.from(out);
};

describe("peerbit-org in-memory libp2p shim", () => {
	it("reassembles length-prefixed frames split across chunks", () => {
		const frames: any[] = [];
		const network = new InMemoryNetwork({
			onFrameSent: (ev) => frames.push(ev),
		});

		const { runtime: a } = InMemoryNetwork.createPeer({
			index: 0,
			port: 50_000,
			network,
		});
		const { runtime: b } = InMemoryNetwork.createPeer({
			index: 1,
			port: 50_001,
			network,
		});

		const id = new Uint8Array(32);
		id.set(BENCH_ID_PREFIX, 0);
		id[4] = 0;
		id[5] = 0;
		id[6] = 0;
		id[7] = 1; // seq=1

		const msg = new DataMessage({
			data: new Uint8Array(256),
			header: new MessageHeader({
				id,
				mode: new AnyWhere(),
				session: 1,
			}),
		});

		const bytes = msg.bytes();
		const payload =
			bytes instanceof Uint8Array ? bytes : (bytes as { subarray: () => Uint8Array }).subarray();
		expect(payload.length).toBeGreaterThan(127); // ensure multi-byte varint length

		const prefix = encodeUVarint(payload.length);
		expect(prefix.length).toBeGreaterThan(1);

		// Simulate how `it-length-prefixed` yields separate chunks (length, then payload),
		// and how the transport may split further.
		const chunks: Uint8Array[] = [
			prefix.subarray(0, 1),
			prefix.subarray(1),
			payload.subarray(0, 11),
			payload.subarray(11, 123),
			payload.subarray(123),
		];

		for (const chunk of chunks) {
			network.recordSend({
				from: a.peerId,
				to: b.peerId,
				protocol: "/bench/1.0.0",
				streamId: "stream-0",
				chunk,
			});
		}

		expect(frames).toHaveLength(1);
		const ev = frames[0]!;
		expect(ev.type).toBe("data");
		expect(ev.payloadOffset).toBe(0);
		expect(ev.payloadLength).toBe(payload.length);
		expect(ev.bytes).toBe(prefix.length + payload.length);

		const frame = ev.encodedFrame as Uint8Array;
		expect(frame[0]).toBe(0); // DATA_VARIANT
		expect(frame[1]).toBe(0); // MessageHeader variant
		expect(Array.from(frame.subarray(2, 6))).toEqual(Array.from(BENCH_ID_PREFIX));
	});
});
