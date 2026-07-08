// Cross-backend LIFECYCLE CONFORMANCE: a client opened on an on-disk `directory`
// must be able to REOPEN a program after a clean stop -> restart WITHOUT any
// peer, with its entry heads, log length and documents all restored from disk —
// and the DEFAULT (JS) and NATIVE (rust) backends must behave IDENTICALLY.
//
// Why this exists: the native backend has peerless from-disk restart gates
// (durable-native-restart.spec.ts, coordinate-persistence-restart.spec.ts), but
// the default backend had NO equivalent from-disk-no-peers restart baseline, so
// there was nothing to say "native restart matches default restart" against.
// This parametrizes the SAME sequence and the SAME core assertions over both
// backends: it both establishes the default baseline and asserts conformance.
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import { EventStore } from "./utils/stores/event-store.js";

const BACKENDS = ["default", "native"] as const;
type Backend = (typeof BACKENDS)[number];

for (const backend of BACKENDS) {
	describe(`durable restart conformance (${backend})`, function () {
		this.timeout(120_000);

		let client: Peerbit | undefined;
		let directory: string | undefined;

		const createClient = (dir: string) =>
			backend === "native"
				? Peerbit.create({ directory: dir, ...createRustPeerbitOptions() })
				: Peerbit.create({ directory: dir });

		afterEach(async () => {
			await client?.stop();
			client = undefined;
			if (directory) {
				await fs.rm(directory, { recursive: true, force: true });
				directory = undefined;
			}
		});

		it("reopens a program from disk with heads, length and documents restored (no peers)", async () => {
			directory = await fs.mkdtemp(
				path.join(os.tmpdir(), `peerbit-durable-restart-${backend}-`),
			);

			const storeId = new Uint8Array(32);
			for (let index = 0; index < storeId.length; index++) {
				storeId[index] = (index * 11 + 5) & 0xff;
			}
			const entryCount = 8;

			// --- Session 1: on-disk client, append N docs, no peer ever dialed ---
			client = await createClient(directory);
			const store1 = await client.open(
				new EventStore<string, any>({ id: storeId }),
				{ args: { replicate: { factor: 1 } } },
			);
			for (let index = 0; index < entryCount; index++) {
				await store1.add(`entry-${index}`, { meta: { next: [] } });
			}

			const preRestartValues = (await store1.iterator({ limit: entryCount }))
				.collect()
				.map((entry: any) => entry.payload.getValue().value)
				.sort();
			expect(preRestartValues.length, "session 1 document count").to.equal(
				entryCount,
			);
			const preRestartLength = store1.log.log.length;
			const preRestartHeads = (await store1.log.log.getHeads(true).all())
				.map((entry: any) => entry.hash)
				.sort();

			const identity = client.identity;

			// Clean stop flushes durable storage (both backends).
			await client.stop();
			client = undefined;

			// --- Session 2: FRESH client, SAME directory, NO peers ---------------
			client = await createClient(directory);
			expect(
				client.identity.publicKey.equals(identity.publicKey),
				"same identity restored from disk",
			).to.equal(true);

			// The core conformance invariant, IDENTICAL for both backends: reopening
			// resolves the heads (walks the DAG through the block store) without
			// throwing, and heads/length/documents all match pre-restart purely
			// from disk.
			const store2 = await client.open(
				new EventStore<string, any>({ id: storeId }),
				{ args: { replicate: { factor: 1 } } },
			);

			const restoredHeads = (await store2.log.log.getHeads(true).all())
				.map((entry: any) => entry.hash)
				.sort();
			expect(restoredHeads, "restored heads match pre-restart").to.deep.equal(
				preRestartHeads,
			);
			expect(store2.log.log.length, "restored log length").to.equal(
				preRestartLength,
			);

			const restoredValues = (await store2.iterator({ limit: entryCount }))
				.collect()
				.map((entry: any) => entry.payload.getValue().value)
				.sort();
			expect(
				restoredValues,
				"restored documents match pre-restart",
			).to.deep.equal(preRestartValues);
		});
	});
}
