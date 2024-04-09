import { Node } from "../src/connection.js";
import { waitForResolved } from "@peerbit/time";
import { TypedEventEmitter } from "@libp2p/interface";
import { EventEmitterNode } from "./utils.js";
import { expect } from 'chai'

const testNodes = async (a: Node, b: Node, c: Node) => {
	a.start();
	b.start();
	c.start();

	expect(a.out.size).equal(0);
	expect(b.out.size).equal(0);
	expect(c.out.size).equal(0);

	a.connect({ to: { id: b.id } });

	expect(a.out.size).equal(1);
	expect(b.out.size).equal(1);
	expect(c.out.size).equal(0);

	let receivedMessage: Uint8Array | undefined = undefined;
	b.subscribe("data", (msg) => {
		receivedMessage = msg.data;
	});

	let receivedMessageC: Uint8Array | undefined = undefined;
	c.subscribe("data", (msg) => {
		receivedMessageC = msg.data;
	});

	a.send(new Uint8Array([1, 2, 3]));
	await waitForResolved(() =>
		expect(receivedMessage).to.deep.equal(new Uint8Array([1, 2, 3]))
	);

	a.send(new Uint8Array([3, 2, 1]), b.id);
	await waitForResolved(() =>
		expect(receivedMessage).to.deep.equal(new Uint8Array([3, 2, 1]))
	);

	expect(receivedMessageC).equal(undefined);
};

describe("index", () => {
	it("event-emitter-node", async () => {
		const events = new TypedEventEmitter();
		const a = new EventEmitterNode(events);
		const b = new EventEmitterNode(events);
		const c = new EventEmitterNode(events);

		await testNodes(a, b, c);
	});

	// TOOD others
});
