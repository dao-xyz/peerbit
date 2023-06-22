import { LSession } from "@peerbit/test-utils";
import { EventStore } from "./utils/stores";
import { Observer, Replicator } from "../role";
import { waitFor } from "@peerbit/time";
import { deserialize, serialize } from "@dao-xyz/borsh";

describe("exchange", () => {
	let session: LSession;
	beforeAll(async () => {
		session = await LSession.connected(3);
	});

	afterAll(async () => {
		await session.stop();
	});

	it("all subscribers will recieve heads", async () => {
		let stores: EventStore<string>[] = [];
		const s = new EventStore<string>();
		const createStore = () => deserialize(serialize(s), EventStore);
		for (const [i, peer] of session.peers.entries()) {
			const store = await createStore().open(peer, {
				setup: (p) =>
					p.setup({
						role: i === 0 ? new Replicator() : new Observer(),
						sync: () => true,
					}),
			});
			stores.push(store);
		}

		for (const [i, store] of stores.entries()) {
			for (const [j, peer] of session.peers.entries()) {
				if (i === j) {
					continue;
				}
				await store.waitFor(peer.peerId);
			}
		}

		const hashes: string[] = [];
		for (let i = 0; i < 100; i++) {
			hashes.push((await stores[0].add(String("i"))).entry.hash);
		}

		for (let i = 0; i < hashes.length; i++) {
			for (let j = 1; j < stores.length; j++) {
				await waitFor(() => stores[j].log.log.has(hashes[j]));
			}
		}
	});
});
