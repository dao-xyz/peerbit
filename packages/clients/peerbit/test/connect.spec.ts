import { SeekDelivery } from "@peerbit/stream-interface";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { Peerbit } from "../src/index.js";

const isNode = typeof process !== "undefined" && !!process.versions?.node;

describe(`dial`, function () {
	let clients: [Peerbit, Peerbit];

	beforeEach(async () => {
		clients = [await Peerbit.create(), await Peerbit.create()];
	});

	afterEach(async () => {
		await Promise.all(clients.map((c) => c.stop()));
	});

	if (isNode) {
		it("waits for blocks", async () => {
			const cid = await clients[0].services.blocks.put(new Uint8Array([1]));
			await clients[0].dial(clients[1].getMultiaddrs()[0]);
			expect(
				new Uint8Array((await clients[0].services.blocks.get(cid))!),
			).to.deep.equal(new Uint8Array([1]));
		});

		it("waits for pubsub", async () => {
			let topic = "topic";
			await clients[0].services.pubsub.subscribe(topic);
			await clients[1].services.pubsub.subscribe(topic);

			let data: Uint8Array | undefined = undefined;
			clients[1].services.pubsub.addEventListener("data", (d) => {
				data = d.detail.data.data;
			});
			await clients[0].dial(clients[1].getMultiaddrs()[0]);

			await waitForResolved(() =>
				expect(clients[0].services.pubsub.getSubscribers(topic)).to.have.length(
					2,
				),
			);

			await clients[0].services.pubsub.publish(new Uint8Array([1]), {
				topics: [topic],
				mode: new SeekDelivery({ redundancy: 1 }),
			});
			await waitForResolved(() => expect(data).to.exist);
			expect(data && new Uint8Array(data)).to.deep.equal(new Uint8Array([1]));
		});
	}
	it("dialer settings", async () => {
		expect(clients[0].services.pubsub.connectionManagerOptions.dialer).to.exist;
		expect(clients[1].services.blocks.connectionManagerOptions.dialer).equal(
			undefined,
		);
	});

	it("prune settings", async () => {
		expect(clients[0].services.pubsub.connectionManagerOptions.pruner).to.exist;
		expect(clients[1].services.blocks.connectionManagerOptions.pruner).equal(
			undefined,
		);
	});
});

describe(`hangup`, function () {
	let clients: [Peerbit, Peerbit];

	beforeEach(async () => {
		clients = [
			await Peerbit.create({
				relay: false, // https://github.com/libp2p/js-libp2p/issues/2794
			}),
			await Peerbit.create({
				relay: false, // https://github.com/libp2p/js-libp2p/issues/2794
			}),
		];
	});

	afterEach(async () => {
		await Promise.all(clients.map((c) => c.stop()));
	});

	it("pubsub subscribers clears up", async () => {
		let topic = "topic";
		await clients[0].services.pubsub.subscribe(topic);

		if (isNode) {
			await clients[1].services.pubsub.subscribe(topic);
			await clients[0].dial(clients[1].getMultiaddrs()[0]);
			await waitForResolved(() =>
				expect(clients[0].services.pubsub.peers.size).to.eq(1),
			);
			await clients[0].hangUp(clients[1].peerId);
			await waitForResolved(() =>
				expect(clients[0].services.pubsub.peers.size).to.eq(0),
			);
		}
	});
});
