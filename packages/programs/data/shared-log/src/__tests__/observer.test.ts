import { TestSession } from "@peerbit/test-utils";
import { EventStore } from "./utils/stores";
import { Observer, Replicator } from "../role";
import { waitForResolved } from "@peerbit/time";
import { deserialize, serialize } from "@dao-xyz/borsh";

describe("observer", () => {
	let session: TestSession;

	beforeAll(async () => {
		session = await TestSession.connected(3);
	});

	afterAll(async () => {
		await session.stop();
	});

	it("observers will not receive heads", async () => {
		let stores: EventStore<string>[] = [];
		const s = new EventStore<string>();
		const createStore = () => deserialize(serialize(s), EventStore);
		for (const [i, peer] of session.peers.entries()) {
			const store = await peer.open(createStore(), {
				args: {
					role: i <= 1 ? { type: "replicator", factor: 1 } : "observer"
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
			}
		}

		const hashes: string[] = [];
		for (let i = 0; i < 100; i++) {
			hashes.push((await stores[0].add(String("i"))).entry.hash);
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
});
