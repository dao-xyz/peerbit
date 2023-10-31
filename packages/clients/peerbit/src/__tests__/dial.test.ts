import { Peerbit } from "../index.js";
import { waitFor } from "@peerbit/time";
describe(`dial`, function () {
	let clients: [Peerbit, Peerbit];

	beforeEach(async () => {
		clients = [await Peerbit.create(), await Peerbit.create()];
	});

	afterEach(async () => {
		await Promise.all(clients.map((c) => c.stop()));
	});

	it("waits for blocks", async () => {
		const cid = await clients[0].services.blocks.put(new Uint8Array([1]));
		await clients[0].dial(clients[1].getMultiaddrs()[0]);
		expect(
			new Uint8Array((await clients[0].services.blocks.get(cid))!)
		).toEqual(new Uint8Array([1]));
	});

	it("waits for pubsub", async () => {
		let topic = "topic";
		await clients[1].services.pubsub.subscribe(topic);
		let data: Uint8Array | undefined = undefined;
		clients[1].services.pubsub.addEventListener("data", (d) => {
			data = d.detail.data.data;
		});
		await clients[0].dial(clients[1].getMultiaddrs()[0]);
		await clients[0].services.pubsub.publish(new Uint8Array([1]), {
			topics: [topic]
		});
		await waitFor(() => !!data);
		expect(data && new Uint8Array(data)).toEqual(new Uint8Array([1]));
	});

	it("autodials by default", async () => {
		expect(
			clients[0].services.pubsub.connectionManagerOptions.dialer
		).toBeDefined();
		expect(
			clients[1].services.blocks.connectionManagerOptions.dialer
		).toBeDefined();
	});

	it("autoprunes by default", async () => {
		expect(
			clients[0].services.pubsub.connectionManagerOptions.pruner
		).toBeDefined();
		expect(
			clients[1].services.blocks.connectionManagerOptions.pruner
		).toBeDefined();
	});
});
