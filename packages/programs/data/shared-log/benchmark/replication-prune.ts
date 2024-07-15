
import { waitForResolved } from "@peerbit/time";
import { TestSession } from "@peerbit/test-utils";
import {
	Ed25519Keypair
} from "@peerbit/crypto";
import { AbsoluteReplicas } from "../src/replication.js";
import { deserialize } from "@dao-xyz/borsh";
import { expect } from "chai";
import { EventStore } from "../test/utils/stores/event-store.js";

// Run with "node --loader ts-node/esm ./benchmark/replication-prune.ts"

let session: TestSession = await TestSession.connected(3, [
	{
		libp2p: {
			peerId: await deserialize(
				new Uint8Array([
					0, 0, 235, 231, 83, 185, 72, 206, 24, 154, 182, 109, 204, 158, 45,
					46, 27, 15, 0, 173, 134, 194, 249, 74, 80, 151, 42, 219, 238, 163,
					44, 6, 244, 93, 0, 136, 33, 37, 186, 9, 233, 46, 16, 89, 240, 71,
					145, 18, 244, 158, 62, 37, 199, 0, 28, 223, 185, 206, 109, 168,
					112, 65, 202, 154, 27, 63, 15
				]),
				Ed25519Keypair
			).toPeerId()
		}
	},
	{
		libp2p: {
			peerId: await deserialize(
				new Uint8Array([
					0, 0, 132, 56, 63, 72, 241, 115, 159, 73, 215, 187, 97, 34, 23,
					12, 215, 160, 74, 43, 159, 235, 35, 84, 2, 7, 71, 15, 5, 210, 231,
					155, 75, 37, 0, 15, 85, 72, 252, 153, 251, 89, 18, 236, 54, 84,
					137, 152, 227, 77, 127, 108, 252, 59, 138, 246, 221, 120, 187,
					239, 56, 174, 184, 34, 141, 45, 242
				]),
				Ed25519Keypair
			).toPeerId()
		}
	},

	{
		libp2p: {
			peerId: await deserialize(
				new Uint8Array([
					0, 0, 193, 202, 95, 29, 8, 42, 238, 188, 32, 59, 103, 187, 192,
					93, 202, 183, 249, 50, 240, 175, 84, 87, 239, 94, 92, 9, 207, 165,
					88, 38, 234, 216, 0, 183, 243, 219, 11, 211, 12, 61, 235, 154, 68,
					205, 124, 143, 217, 234, 222, 254, 15, 18, 64, 197, 13, 62, 84,
					62, 133, 97, 57, 150, 187, 247, 215
				]),
				Ed25519Keypair
			).toPeerId()
		}
	}
]);
let db1: EventStore<string>, db2: EventStore<string>, db3: EventStore<string>;

const init = async (min: number, max?: number) => {
	db1 = await session.peers[0].open(new EventStore<string>(), {
		args: {
			replicas: {
				min,
				max
			},
			replicate: false
		}
	});
	db2 = (await EventStore.open<EventStore<string>>(
		db1.address!,
		session.peers[1],
		{
			args: {
				replicas: {
					min,
					max
				}
			}
		}
	))!;

	db3 = (await EventStore.open<EventStore<string>>(
		db1.address!,
		session.peers[2],
		{
			args: {
				replicas: {
					min,
					max
				}
			}
		}
	))!;

	await db1.waitFor(session.peers[1].peerId);
	await db2.waitFor(session.peers[0].peerId);
	await db2.waitFor(session.peers[2].peerId);
	await db3.waitFor(session.peers[0].peerId);
};

let minReplicas = 1;
let maxReplicas = 1;

await init(minReplicas, maxReplicas);

const entryCount = 1e3;
for (let i = 0; i < entryCount; i++) {
	await db1!.add("hello", {
		replicas: new AbsoluteReplicas(100), // will be overriden by 'maxReplicas' above
		meta: { next: [] }
	});
}
try {
	await waitForResolved(() => {
		expect(db1.log.log.length).equal(0); // because db1 is not replicating at all, but just pruning once it knows entries are replicated elsewhere
		let total = db2.log.log.length + db3.log.log.length;
		expect(total).greaterThanOrEqual(entryCount);
		expect(total).lessThan(entryCount * 2);
		expect(db2.log.log.length).greaterThan(entryCount * 0.2);
		expect(db3.log.log.length).greaterThan(entryCount * 0.2);
	}, { timeout: 2e4 });
} catch (error) {
	console.log("Failed to assert replication done");
	console.log([db1!, db2!, db3!].map(x => x.log.log.length));
} finally {
	await session.stop()

}

