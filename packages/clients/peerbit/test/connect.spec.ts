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
			});
			await waitForResolved(() => expect(data).to.exist);
			expect(data && new Uint8Array(data)).to.deep.equal(new Uint8Array([1]));
		});

		it("exposes fanout helpers separately from pubsub", async () => {
			const topic = "fanout-topic";
			const root = clients[0].services.fanout.publicKeyHash;
			const rootChannel = clients[0].fanoutChannel(topic);
			const leafChannel = clients[1].fanoutChannel(topic, root);

			await clients[0].dial(clients[1].getMultiaddrs()[0]);

			rootChannel.openAsRoot({
				msgRate: 10,
				msgSize: 64,
				uploadLimitBps: 1_000_000,
				maxChildren: 8,
				repair: true,
			});
			await leafChannel.join(
				{
					msgRate: 10,
					msgSize: 64,
					uploadLimitBps: 0,
					maxChildren: 0,
					repair: true,
				},
				{ timeoutMs: 10_000 },
			);

			let received: Uint8Array | undefined;
			leafChannel.addEventListener("data", (ev: any) => {
				received = ev.detail.payload;
			});

			await rootChannel.publish(new Uint8Array([7, 8, 9]));
			await waitForResolved(() => expect(received).to.exist);
			expect([...received!]).to.deep.equal([7, 8, 9]);
		});

		it("waits for fanout by default when fanout service is present", async () => {
			const originalWaitFor = clients[0].services.fanout.waitFor.bind(
				clients[0].services.fanout,
			);
			(clients[0].services.fanout as any).waitFor = async () => {
				throw new Error("fanout-not-ready");
			};

			try {
				await expect(
					clients[0].dial(clients[1].getMultiaddrs()[0], {
						dialTimeoutMs: 5_000,
					}),
				).to.be.rejectedWith("Fanout");
			} finally {
				(clients[0].services.fanout as any).waitFor = originalWaitFor;
			}
		});

		it("allows skipping fanout readiness checks", async () => {
			const originalWaitFor = clients[0].services.fanout.waitFor.bind(
				clients[0].services.fanout,
			);
			(clients[0].services.fanout as any).waitFor = async () => {
				throw new Error("fanout-not-ready");
			};

			try {
				const ok = await clients[0].dial(clients[1].getMultiaddrs()[0], {
					readiness: "services",
					dialTimeoutMs: 5_000,
				});
				expect(ok).to.be.true;
			} finally {
				(clients[0].services.fanout as any).waitFor = originalWaitFor;
			}
		});

		it("supports connection-only dial readiness", async () => {
			const originalPubsubWaitFor = clients[0].services.pubsub.waitFor.bind(
				clients[0].services.pubsub,
			);
			(clients[0].services.pubsub as any).waitFor = async () => {
				throw new Error("pubsub-not-ready");
			};

			try {
				const ok = await clients[0].dial(clients[1].getMultiaddrs()[0], {
					readiness: "connection",
					dialTimeoutMs: 5_000,
				});
				expect(ok).to.be.true;
			} finally {
				(clients[0].services.pubsub as any).waitFor = originalPubsubWaitFor;
			}
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
