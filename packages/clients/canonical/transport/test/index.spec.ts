import { deserialize, serialize } from "@dao-xyz/borsh";
import { expect } from "chai";
import {
	CanonicalChannelClose,
	CanonicalChannelMessage,
	CanonicalConnection,
	CanonicalControlRequest,
	CanonicalControlResponse,
	CanonicalFrame,
	type CanonicalTransport,
} from "../src/index.js";

const createTransportPair = (): {
	a: CanonicalTransport;
	b: CanonicalTransport;
} => {
	let handlerA: ((data: Uint8Array) => void) | undefined;
	let handlerB: ((data: Uint8Array) => void) | undefined;

	const a: CanonicalTransport = {
		send: (data) => {
			handlerB?.(data);
		},
		onMessage: (handler) => {
			handlerA = handler;
			return () => {
				if (handlerA === handler) handlerA = undefined;
			};
		},
	};

	const b: CanonicalTransport = {
		send: (data) => {
			handlerA?.(data);
		},
		onMessage: (handler) => {
			handlerB = handler;
			return () => {
				if (handlerB === handler) handlerB = undefined;
			};
		},
	};

	return { a, b };
};

describe("@peerbit/canonical-transport", () => {
	it("encodes and decodes control frames", () => {
		const req = new CanonicalControlRequest({
			id: 1,
			op: "peerId",
		});
		const bytes = serialize(req);
		const decoded = deserialize(bytes, CanonicalFrame);
		expect(decoded).to.be.instanceOf(CanonicalControlRequest);

		const resp = new CanonicalControlResponse({
			id: 1,
			ok: true,
			peerId: "peer-id",
		});
		const respBytes = serialize(resp);
		const decodedResp = deserialize(respBytes, CanonicalFrame);
		expect(decodedResp).to.be.instanceOf(CanonicalControlResponse);
	});

	it("routes channel messages over a mux connection", () => {
		const { a, b } = createTransportPair();
		const connA = new CanonicalConnection(a);
		const connB = new CanonicalConnection(b);

		const channelA = connA.createChannel(7);
		const channelB = connB.createChannel(7);

		let seen: Uint8Array | undefined;
		channelB.onMessage((data) => {
			seen = data;
		});

		channelA.send(new Uint8Array([1, 2, 3]));
		expect(seen).to.deep.equal(new Uint8Array([1, 2, 3]));

		const frame = new CanonicalChannelMessage({
			channelId: 7,
			payload: new Uint8Array([9]),
		});
		expect(frame.channelId).to.equal(7);
	});

	it("emits channel close frames when a connection closes", () => {
		let lastFrame: CanonicalFrame | undefined;
		const { a, b } = createTransportPair();
		const sendA = a.send.bind(a);
		a.send = (data) => {
			lastFrame = deserialize(data, CanonicalFrame) as CanonicalFrame;
			sendA(data);
		};
		const connA = new CanonicalConnection(a);
		const connB = new CanonicalConnection(b);

		const channelA = connA.createChannel(7);
		const channelB = connB.createChannel(7);

		let closed = false;
		channelB.onClose?.(() => {
			closed = true;
		});

		channelA.send(new Uint8Array([1]));
		expect(closed).to.equal(false);

		connA.close();
		expect(lastFrame).to.be.instanceOf(CanonicalChannelClose);
		expect((lastFrame as CanonicalChannelClose).channelId).to.equal(7);
		expect(closed).to.equal(true);
	});
});
