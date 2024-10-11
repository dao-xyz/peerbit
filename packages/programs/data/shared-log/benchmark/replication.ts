import { privateKeyFromRaw } from "@libp2p/crypto/keys";
import { TestSession } from "@peerbit/test-utils";
import {
	AbortError,
	/* waitFor */
} from "@peerbit/time";
import B from "benchmark";
import { v4 as uuid } from "uuid";
import { EventStore } from "../test/utils/stores/event-store.js";

// Run with "node --loader ts-node/esm ./benchmark/replication.ts"

let session: TestSession = await TestSession.connected(2, [
	{
		libp2p: {
			privateKey: privateKeyFromRaw(
				new Uint8Array([
					204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91, 229, 157,
					168, 15, 45, 242, 144, 98, 75, 58, 208, 9, 223, 143, 251, 52, 252,
					159, 64, 83, 52, 197, 24, 246, 24, 234, 141, 183, 151, 82, 53, 142,
					57, 25, 148, 150, 26, 209, 223, 22, 212, 40, 201, 6, 191, 72, 148, 82,
					66, 138, 199, 185,
				]),
			),
		},
	},
	{
		libp2p: {
			privateKey: privateKeyFromRaw(
				new Uint8Array([
					237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214, 122, 28,
					157, 208, 163, 15, 215, 104, 193, 151, 177, 62, 231, 253, 120, 122,
					222, 174, 242, 120, 50, 165, 97, 8, 235, 97, 186, 148, 251, 100, 168,
					49, 10, 119, 71, 246, 246, 174, 163, 198, 54, 224, 6, 174, 212, 159,
					187, 2, 137, 47, 192,
				]),
			),
		},
	},
]);

let db1: EventStore<string>, db2: EventStore<string>;

let abortController = new AbortController();

let resolvers: Map<string, { resolve: () => void }> = new Map();

db1 = await session.peers[0].open(new EventStore<string>(), {
	args: {
		replicate: {
			factor: 1,
		},
	},
});

db2 = (await EventStore.open<EventStore<string>>(
	db1.address!,
	session.peers[1],
	{
		args: {
			replicate: {
				factor: 1,
			},
			onChange: async (change) => {
				for (const added of change.added) {
					try {
						resolvers
							.get(
								added.entry.hash,
							)! /* || await waitFor(() => resolvers.get(entry.hash), { signal: abortController.signal }))? */
							.resolve();
						resolvers.delete(added.entry.hash);
					} catch (error) {
						if (error instanceof AbortError) {
							return;
						}
						return;
						/* throw error; */
					}
				}
			},
		},
	},
))!;

await db1.waitFor(session.peers[1].peerId);
await db2.waitFor(session.peers[0].peerId);

const suite = new B.Suite();
suite
	.add("replication", {
		fn: async (deferred: any) => {
			const { entry } = await db1.add(uuid(), { meta: { next: [] } });
			resolvers.set(entry.hash, deferred);
		},
		defer: true,
	})
	.on("cycle", (event: any) => {
		console.log(String(event.target));
	})
	.on("error", (err: any) => {
		throw err;
	})
	.on("complete", async function (this: any) {
		await abortController.abort();
		await session.stop();
	})
	.run();
