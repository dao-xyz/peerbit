// this benchmark test the time it takes for two nodes with almost the same data to sync up
import { keys } from "@libp2p/crypto";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { v4 as uuid } from "uuid";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { RatelessIBLTSynchronizer } from "../src/sync/rateless-iblt.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import type { TestSetupConfig } from "../test/utils.js";
import { EventStore } from "../test/utils/stores/event-store.js";

// Run with "node --loader ts-node/esm ./benchmark/partial-sync.ts"
let db1: EventStore<string, any> = undefined as any;
let db2: EventStore<string, any> = undefined as any;

const store = new EventStore<string, any>();

let syncedCount = 20e3;
let unsyncedCount = 1;
let totalCount = syncedCount + unsyncedCount * 2;

const reset = async (session: TestSession, setup: TestSetupConfig<any>) => {
	db1 = await session.peers[0].open(store.clone(), {
		args: {
			replicate: {
				factor: 1,
			},
			setup,
		},
	});

	db2 = await session.peers[1].open(store.clone(), {
		args: {
			replicate: {
				factor: 1,
			},
			setup,
		},
	});

	for (let i = 0; i < syncedCount; i++) {
		const entry = await db1.add(uuid(), { meta: { next: [] } });
		await db2.log.join([entry.entry]);
	}

	expect(db1.log.log.length).to.equal(syncedCount);
	expect(db2.log.log.length).to.equal(syncedCount);

	for (let i = 0; i < unsyncedCount; i++) {
		await db1.add(uuid(), { meta: { next: [] } });
		await db2.add(uuid(), { meta: { next: [] } });
	}

	expect(db1.log.log.length).to.equal(syncedCount + unsyncedCount);
	expect(db2.log.log.length).to.equal(syncedCount + unsyncedCount);
};

export const testSetups: TestSetupConfig<any>[] = [
	{
		domain: createReplicationDomainHash("u32"),
		type: "u32",
		syncronizer: SimpleSyncronizer,
		name: "u32-simple",
	} /*
		{
			domain: createReplicationDomainHash("u64"),
			type: "u64",
			syncronizer: SimpleSyncronizer,
			name: "u64-simple",
		}, */,
	{
		domain: createReplicationDomainHash("u64"),
		type: "u64",
		syncronizer: RatelessIBLTSynchronizer,
		name: "u64-iblt",
	},
];

for (const setup of testSetups) {
	let session: TestSession = await TestSession.disconnected(2, [
		{
			// TODO dialing fails with this?
			libp2p: {
				privateKey: keys.privateKeyFromRaw(
					new Uint8Array([
						204, 234, 187, 172, 226, 232, 70, 175, 62, 211, 147, 91, 229, 157,
						168, 15, 45, 242, 144, 98, 75, 58, 208, 9, 223, 143, 251, 52, 252,
						159, 64, 83, 52, 197, 24, 246, 24, 234, 141, 183, 151, 82, 53, 142,
						57, 25, 148, 150, 26, 209, 223, 22, 212, 40, 201, 6, 191, 72, 148,
						82, 66, 138, 199, 185,
					]),
				),
			},
		},
		{
			libp2p: {
				privateKey: keys.privateKeyFromRaw(
					new Uint8Array([
						237, 55, 205, 86, 40, 44, 73, 169, 196, 118, 36, 69, 214, 122, 28,
						157, 208, 163, 15, 215, 104, 193, 151, 177, 62, 231, 253, 120, 122,
						222, 174, 242, 120, 50, 165, 97, 8, 235, 97, 186, 148, 251, 100,
						168, 49, 10, 119, 71, 246, 246, 174, 163, 198, 54, 224, 6, 174, 212,
						159, 187, 2, 137, 47, 192,
					]),
				),
			},
		},
	]);

	console.log("Resetting...");
	await reset(session, setup);
	console.log("Reset");

	if (!db1 || !db2) {
		throw new Error("db1 or db2 is undefined");
	}

	console.log("Starting sync...");
	const timeLabel =
		setup.name +
		": " +
		"Entries " +
		totalCount +
		" of which " +
		unsyncedCount * 2 +
		" are unsynced";

	console.log("Dialing...");
	await waitForResolved(() => db1.node.dial(db2.node.getMultiaddrs()));
	console.time(timeLabel);
	console.log("Waiting for sync...");

	await waitForResolved(
		() => {
			expect(db1.log.log.length).to.equal(totalCount);
			expect(db2.log.log.length).to.equal(totalCount);
		},
		{
			timeout: 3e4,
		},
	);

	console.timeEnd(timeLabel);

	await session.stop();
}
