import { Node } from "../connection.js";
import { waitForResolved } from "@peerbit/time";
import { EventEmitter } from "@libp2p/interfaces/events";
import { EventEmitterNode } from "./utils.js";

const testNodes = async (a: Node, b: Node, c: Node) => {
	a.start();
	b.start();
	c.start();

	expect(a.out.size).toEqual(0);
	expect(b.out.size).toEqual(0);
	expect(c.out.size).toEqual(0);

	a.connect({ to: { id: b.id } });

	expect(a.out.size).toEqual(1);
	expect(b.out.size).toEqual(1);
	expect(c.out.size).toEqual(0);

	let recievedMessage: Uint8Array | undefined = undefined;
	b.subscribe("data", (msg) => {
		recievedMessage = msg.data;
	});

	let recievedMessageC: Uint8Array | undefined = undefined;
	c.subscribe("data", (msg) => {
		recievedMessageC = msg.data;
	});

	a.send(new Uint8Array([1, 2, 3]));
	await waitForResolved(() =>
		expect(recievedMessage).toEqual(new Uint8Array([1, 2, 3]))
	);

	a.send(new Uint8Array([3, 2, 1]), b.id);
	await waitForResolved(() =>
		expect(recievedMessage).toEqual(new Uint8Array([3, 2, 1]))
	);

	expect(recievedMessageC).toBeUndefined();
};

describe("index", () => {
	it("event-emitter-node", async () => {
		const events = new EventEmitter();
		const a = new EventEmitterNode(events);
		const b = new EventEmitterNode(events);
		const c = new EventEmitterNode(events);

		await testNodes(a, b, c);
	});

	// TOOD others
});
