import { deserialize } from "@dao-xyz/borsh";
import { Ed25519Keypair } from "@peerbit/crypto";
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
			peerId: await deserialize(
				new Uint8Array([
					0, 0, 235, 231, 83, 185, 72, 206, 24, 154, 182, 109, 204, 158, 45, 46,
					27, 15, 0, 173, 134, 194, 249, 74, 80, 151, 42, 219, 238, 163, 44, 6,
					244, 93, 0, 136, 33, 37, 186, 9, 233, 46, 16, 89, 240, 71, 145, 18,
					244, 158, 62, 37, 199, 0, 28, 223, 185, 206, 109, 168, 112, 65, 202,
					154, 27, 63, 15,
				]),
				Ed25519Keypair,
			).toPeerId(),
		},
	},
	{
		libp2p: {
			peerId: await deserialize(
				new Uint8Array([
					0, 0, 132, 56, 63, 72, 241, 115, 159, 73, 215, 187, 97, 34, 23, 12,
					215, 160, 74, 43, 159, 235, 35, 84, 2, 7, 71, 15, 5, 210, 231, 155,
					75, 37, 0, 15, 85, 72, 252, 153, 251, 89, 18, 236, 54, 84, 137, 152,
					227, 77, 127, 108, 252, 59, 138, 246, 221, 120, 187, 239, 56, 174,
					184, 34, 141, 45, 242,
				]),
				Ed25519Keypair,
			).toPeerId(),
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
