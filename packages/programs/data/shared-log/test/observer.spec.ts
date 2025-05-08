import { deserialize, serialize } from "@dao-xyz/borsh";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { EventStore } from "./utils/stores/index.js";

describe("observer", () => {
	let session: TestSession;

	before(async () => {});

	afterEach(async () => {
		await session.stop();
	});

	it("observers will not receive heads by default", async () => {
		session = await TestSession.disconnected(3);
		session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]],
		]);

		let stores: EventStore<string, any>[] = [];
		const s = new EventStore<string, any>();
		const createStore = () => deserialize(serialize(s), EventStore);
		let replicatorEndIndex = 1;

		for (const [i, peer] of session.peers.entries()) {
			const store = await peer.open(createStore(), {
				args: {
					replicate: i <= replicatorEndIndex ? { factor: 1 } : false,
				},
			});
			stores.push(store);
		}

		for (const [i, store] of stores.entries()) {
			for (const [j, peer] of session.peers.entries()) {
				if (i === j) {
					continue;
				}
				await store.waitFor(peer.peerId, { timeout: 10 * 1000 });

				if (j <= replicatorEndIndex) {
					await store.log.waitForReplicator(peer.identity.publicKey);
				}
			}
		}

		const hashes: string[] = [];
		for (let i = 0; i < 100; i++) {
			hashes.push((await stores[0].add(String(i))).entry.hash);
		}

		for (let i = 0; i < hashes.length; i++) {
			for (let j = 1; j < stores.length; j++) {
				if (await stores[j].log.isReplicating()) {
					await waitForResolved(
						async () =>
							expect(await stores[j].log.log.has(hashes[j])).to.be.true,
					);
				} else {
					expect(await stores[j].log.log.has(hashes[j])).to.be.false;
				}
			}
		}
	});

	it("target all will make heads reach observers", async () => {
		session = await TestSession.connected(2);
		await session.connect([[session.peers[0], session.peers[1]]]);

		const s = new EventStore<string, any>();
		const createStore = () => deserialize(serialize(s), EventStore);

		const replicator = await session.peers[0].open(createStore(), {
			args: {
				replicate: {
					factor: 1,
				},
			},
		});

		const observer = await session.peers[1].open(createStore(), {
			args: {
				replicate: false,
				keep: () => true,
			},
		});
		await waitForResolved(async () =>
			expect(await replicator.log.replicationIndex?.getSize()).equal(1),
		);
		await waitForResolved(async () =>
			expect(await observer.log.replicationIndex?.getSize()).equal(1),
		);

		await replicator.add("a", { target: "all" });
		/* await replicator.add("b", { target: 'replicators' }) */
		/* 
				await waitForResolved(() => expect(replicator.log.log.length).equal(2)); */
		await waitForResolved(() => expect(observer.log.log.length).equal(1));
	});

	it("can wait for replicator", async () => {
		session = await TestSession.disconnected(3);
		await session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]],
		]);

		const s = new EventStore<string, any>();
		const createStore = () => deserialize(serialize(s), EventStore);
		const replicator = await session.peers[0].open(createStore(), {
			args: {
				replicate: { factor: 1 },
			},
		});

		const observer = await session.peers[2].open(createStore(), {
			args: {
				replicate: false,
			},
		});

		await observer.log.waitForReplicator(replicator.node.identity.publicKey);
		expect(await observer.log.replicationIndex?.getSize()).equal(1);
		expect(await observer.log.isReplicating()).to.be.false;
	});
});
