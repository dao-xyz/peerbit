import { privateKeyFromRaw } from "@libp2p/crypto/keys";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";

/* import { AbsoluteReplicas } from "../src/replication.js"; */
import { EventStore } from "../test/utils/stores/event-store.js";

// Run with "node --loader ts-node/esm ./benchmark/replication-prune.ts"

let session: TestSession = await TestSession.connected(3, [
	{
		libp2p: {
			privateKey: await privateKeyFromRaw(
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
			privateKey: await privateKeyFromRaw(
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

	{
		libp2p: {
			privateKey: await privateKeyFromRaw(
				new Uint8Array([
					27, 246, 37, 180, 13, 75, 242, 124, 185, 205, 207, 9, 16, 54, 162,
					197, 247, 25, 211, 196, 127, 198, 82, 19, 68, 143, 197, 8, 203, 18,
					179, 181, 105, 158, 64, 215, 56, 13, 71, 156, 41, 178, 86, 159, 80,
					222, 167, 73, 3, 37, 251, 67, 86, 6, 90, 212, 16, 251, 206, 54, 49,
					141, 91, 171,
				]),
			),
		},
	},
]);
let db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>;

const init = async (min: number, max?: number) => {
	db1 = await session.peers[0].open(new EventStore<string>(), {
		args: {
			replicas: {
				min,
				max,
			},
			replicate: false,
		},
	});
	db2 = (await EventStore.open<EventStore<string>>(
		db1.address!,
		session.peers[1],
		{
			args: {
				replicas: {
					min,
					max,
				},
			},
		},
	))!;

	db3 = (await EventStore.open<EventStore<string>>(
		db1.address!,
		session.peers[2],
		{
			args: {
				replicas: {
					min,
					max,
				},
			},
		},
	))!;

	await db1.waitFor(session.peers[1].peerId);
	await db2.waitFor(session.peers[0].peerId);
	await db2.waitFor(session.peers[2].peerId);
	await db3.waitFor(session.peers[0].peerId);
};

let minReplicas = 1;
let maxReplicas = 1;

await init(minReplicas, maxReplicas);
const t1 = +new Date();
const entryCount = 1e3;
for (let i = 0; i < entryCount; i++) {
	await db1!.add("hello", {
		/* replicas: new AbsoluteReplicas(15), */ // will be overriden by 'maxReplicas' above
		meta: { next: [] },
	});
}
try {
	await waitForResolved(
		() => {
			expect(db1.log.log.length).equal(0); // because db1 is not replicating at all, but just pruning once it knows entries are replicated elsewhere
			let total = db2.log.log.length + db3.log.log.length;
			expect(total).greaterThanOrEqual(entryCount);
			expect(total).lessThan(entryCount * 2);
			expect(db2.log.log.length).greaterThan(entryCount * 0.2);
			expect(db3.log.log.length).greaterThan(entryCount * 0.2);
		},
		{ timeout: 2e4 },
	);
} catch (error) {
	console.log("Failed to assert replication done");
	console.log([db1!, db2!, db3!].map((x) => x.log.log.length));
} finally {
	const t2 = +new Date();
	console.log("Done: " + (t2 - t1));
	await session.stop();
}
