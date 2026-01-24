// Benchmarks full catch-up sync time: one peer has N entries, the other starts empty.
//
// Run with:
//   cd packages/programs/data/shared-log
//   CATCHUP_COUNT=5000 CATCHUP_TIMEOUT=60000 node --loader ts-node/esm ./benchmark/sync-catchup.ts
//
// Notes:
// - This is an integration benchmark (network + sync + indexing). It is more variable than pure
//   algorithmic benches; prefer running it a few times and comparing medians.
// - For CI regression tracking, consider smaller `CATCHUP_COUNT` values and low run counts.
import { keys } from "@libp2p/crypto";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { performance } from "node:perf_hooks";
import { v4 as uuid } from "uuid";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { RatelessIBLTSynchronizer } from "../src/sync/rateless-iblt.js";
import { SimpleSyncronizer } from "../src/sync/simple.js";
import type { TestSetupConfig } from "../test/utils.js";
import { EventStore } from "../test/utils/stores/event-store.js";

const entryCount = Number.parseInt(process.env.CATCHUP_COUNT || "5000", 10);
const timeoutMs = Number.parseInt(process.env.CATCHUP_TIMEOUT || "60000", 10);
const runs = Number.parseInt(process.env.CATCHUP_RUNS || "1", 10);

export const testSetups: TestSetupConfig<any>[] = [
	{
		domain: createReplicationDomainHash("u32"),
		type: "u32",
		syncronizer: SimpleSyncronizer,
		name: "u32-simple",
	},
	{
		domain: createReplicationDomainHash("u64"),
		type: "u64",
		syncronizer: RatelessIBLTSynchronizer,
		name: "u64-iblt",
	},
];

const fixedKeys = [
	{
		libp2p: {
			privateKey: keys.privateKeyFromRaw(
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
			privateKey: keys.privateKeyFromRaw(
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
];

const runOnce = async (setup: TestSetupConfig<any>) => {
	const session = await TestSession.disconnected(2, fixedKeys);
	const store = new EventStore<string, any>();

	const db1 = await session.peers[0].open(store.clone(), {
		args: { replicate: { factor: 1 }, setup },
	});
	const db2 = await session.peers[1].open(store.clone(), {
		args: { replicate: { factor: 1 }, setup },
	});

	for (let i = 0; i < entryCount; i++) {
		await db1.add(uuid(), { meta: { next: [] } });
	}

	expect(db1.log.log.length).to.equal(entryCount);

	await waitForResolved(() =>
		session.peers[0].dial(session.peers[1].getMultiaddrs()),
	);

	const t0 = performance.now();
	await waitForResolved(
		() => {
			expect(db2.log.log.length).to.equal(entryCount);
		},
		{ timeout: timeoutMs, delayInterval: 250 },
	);
	const dt = performance.now() - t0;

	await session.stop();

	return dt;
};

const tasks: Array<{
	name: string;
	mean_ms: number;
	hz: number;
	rme: null;
	samples: number;
}> = [];

for (const setup of testSetups) {
	const samples: number[] = [];
	for (let i = 0; i < runs; i++) {
		samples.push(await runOnce(setup));
	}
	const mean_ms = samples.reduce((acc, x) => acc + x, 0) / samples.length;
	tasks.push({
		name: `${setup.name} catchup (n=${entryCount})`,
		mean_ms,
		hz: mean_ms > 0 ? 1000 / mean_ms : 0,
		rme: null,
		samples: samples.length,
	});
}

if (process.env.BENCH_JSON === "1") {
	process.stdout.write(
		JSON.stringify(
			{
				name: "shared-log-sync-catchup",
				tasks,
				meta: { entryCount, timeoutMs, runs },
			},
			null,
			2,
		),
	);
} else {
	console.table(tasks);
}
