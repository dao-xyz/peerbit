// Regression proof for the rust-indexer close -> reopen lifecycle bug.
//
// The node-level indexer `Indices` scope is cached per node and outlives a
// program close. When a native (rust) shared-log program closes it stops its
// own rust indices (state -> "closed") but leaves the cached scope alive. On
// reopen the shared-log re-inits through the cached scope and reads
// synchronously on open (`replicationIndex.count(...)` for
// `hasIndexedReplicationInfo`). Before the fix, `RustIndices.init` returned the
// stopped cached index without restarting it, so that first read threw
// `NotStartedError`. This test opens a genuine native peer, writes, closes, and
// reopens the SAME address on the SAME still-running peer and asserts the
// reopen does not throw and the native backbone is re-attached.
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import { EventStore } from "./utils/stores/event-store.js";

describe("reopen native lifecycle", function () {
	this.timeout(120_000);

	let peer: Peerbit;
	let directory: string;

	beforeEach(async () => {
		// A genuine native peer: the rust indexer + native backbone preset. An
		// on-disk directory keeps the block store across a program close so the
		// reopen can resolve its heads — the peer itself is NOT stopped, so the
		// node-cached indexer scope survives (the bug's precondition).
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-reopen-native-"),
		);
		peer = await Peerbit.create({ directory, ...createRustPeerbitOptions() });
	});

	afterEach(async () => {
		await peer?.stop();
		if (directory) {
			await fs.rm(directory, { recursive: true, force: true });
		}
	});

	it("reopens a closed store on the same running peer without NotStartedError", async () => {
		// --- Open #1: plain open, write a few entries -----------------------
		const db1 = await peer.open(new EventStore<string, any>(), {
			args: { replicate: { factor: 1 } },
		});

		// Native backbone is attached on the first open.
		expect((db1.log as any)._nativeBackbone, "open #1 native backbone").to
			.exist;

		const address = db1.address!;
		for (let i = 0; i < 5; i++) {
			await db1.add(`entry-${i}`, { meta: { next: [] } });
		}
		await waitForResolved(async () =>
			expect(db1.log.log.length).to.equal(5),
		);

		// --- Close: stops the program's rust indices; the node-cached indexer
		// scope stays alive. ---------------------------------------------------
		await db1.close();

		// --- Open #2: reopen the SAME address on the SAME running peer. The
		// reopen re-inits through the cached rust indexer scope and reads
		// synchronously on open. Before the fix this threw `NotStartedError`. ---
		const db2 = (await EventStore.open(address, peer, {
			args: { replicate: { factor: 1 } },
		})) as EventStore<string, any>;

		// The reopen resolved without throwing and re-attached the native path.
		expect((db2.log as any)._nativeBackbone, "reopen native backbone").to
			.exist;

		// Sanity: the reopened log is usable (a read on open succeeded above).
		expect(await db2.log.replicationIndex.count()).to.be.a("number");

		await db2.close();
	});
});
