import { TestSession } from "@peerbit/test-utils";
import { EventStore } from "./utils/stores";
import { Observer, Replicator } from "../role";
import { waitForResolved } from "@peerbit/time";
import { deserialize, serialize } from "@dao-xyz/borsh";

describe("observer", () => {
	let session: TestSession;

	beforeAll(async () => {});

	afterAll(async () => {
		await session.stop();
	});

	it("observers will not receive heads by default", async () => {
		session = await TestSession.disconnected(3);
		session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]]
		]);

		let stores: EventStore<string>[] = [];
		const s = new EventStore<string>();
		const createStore = () => deserialize(serialize(s), EventStore);
		let replicatorEndIndex = 1;

		for (const [i, peer] of session.peers.entries()) {
			const store = await peer.open(createStore(), {
				args: {
					role:
						i <= replicatorEndIndex
							? { type: "replicator", factor: 1 }
							: "observer"
				}
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
				if (stores[j].log.role instanceof Replicator) {
					await waitForResolved(() =>
						expect(stores[j].log.log.has(hashes[j])).toBeTrue()
					);
				} else {
					expect(stores[j].log.log.has(hashes[j])).toBeFalse();
				}
			}
		}
	});

	it("target all will make heads reach observers", async () => {
		session = await TestSession.connected(2);
		await session.connect([[session.peers[0], session.peers[1]]]);

		const s = new EventStore<string>();
		const createStore = () => deserialize(serialize(s), EventStore);

		const replicator = await session.peers[0].open(createStore(), {
			args: {
				role: {
					type: "replicator",
					factor: 1
				}
			}
		});

		const observer = await session.peers[1].open(createStore(), {
			args: {
				role: "observer",
				sync: () => true
			}
		});
		await waitForResolved(() =>
			expect(replicator.log.getReplicatorsSorted()?.length).toEqual(1)
		);
		await waitForResolved(() =>
			expect(observer.log.getReplicatorsSorted()?.length).toEqual(1)
		);

		await replicator.add("a", { target: "all" });
		/* await replicator.add("b", { target: 'replicators' }) */
		/* 
				await waitForResolved(() => expect(replicator.log.log.length).toEqual(2)); */
		await waitForResolved(() => expect(observer.log.log.length).toEqual(1));
		const l = 123;
	});

	it("can wait for replicator", async () => {
		session = await TestSession.disconnected(3);
		await session.connect([
			[session.peers[0], session.peers[1]],
			[session.peers[1], session.peers[2]]
		]);

		const s = new EventStore<string>();
		const createStore = () => deserialize(serialize(s), EventStore);
		const replicator = await session.peers[0].open(createStore(), {
			args: {
				role: { type: "replicator", factor: 1 }
			}
		});

		const observer = await session.peers[2].open(createStore(), {
			args: {
				role: "observer"
			}
		});

		await observer.log.waitForReplicator(replicator.node.identity.publicKey);
		expect(observer.log.getReplicatorsSorted()?.length).toEqual(1);
		expect(observer.log.role).toBeInstanceOf(Observer);
	});
});
