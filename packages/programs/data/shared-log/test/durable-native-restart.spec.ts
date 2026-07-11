// Full end-to-end gate for the "durable native block store" slice: a native
// `peerbit/rust` client on an on-disk `directory` must be able to REOPEN a
// program after a clean stop -> restart WITHOUT any peer, with its entry blocks,
// heads and coordinates all restored from disk.
//
// Background: when the native backbone is active the log's entry blocks live in
// the backbone's in-wasm-memory block store (`NativeBackboneBlockStore`,
// `persisted() === false`). Before this slice, on restart that map was empty, so
// even though the durable heads index still listed the head hashes, the entry
// blocks backing them were gone and `log.getHeads(true).all()` failed with
// "Failed to load entry from head" (see entry-index.ts `resolveMany`). The
// coordinate-persistence slice already restores replication coordinates; this
// slice makes the entry blocks durable via a write-through wrapper over the
// native wasm store and a durable per-program `blocks` sublevel. The wasm map
// remains cold on open; reads fall through to the durable sublevel and lazily
// repopulate the hot map as the log walks the DAG.
//
// This test proves the real durable native restart:
//   (i)  the program reopens WITHOUT "Failed to load entry from head",
//   (ii) all N documents are queryable locally (blocks survived),
//   (iii) the log length / heads match pre-restart,
//   (iv) coordinates are restored (kept green here; the isolated coordinate
//        proof lives in coordinate-persistence-restart.spec.ts).
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import { EventStore } from "./utils/stores/event-store.js";

describe("durable native block store restart", function () {
	this.timeout(120_000);

	let client: Peerbit | undefined;
	let directory: string | undefined;

	afterEach(async () => {
		await client?.stop();
		client = undefined;
		if (directory) {
			await fs.rm(directory, { recursive: true, force: true });
			directory = undefined;
		}
	});

	it("reopens a native program from disk with blocks, heads and coordinates restored", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-restart-"),
		);

		// Deterministic program id keeps the on-disk layout predictable.
		const storeId = new Uint8Array(32);
		for (let index = 0; index < storeId.length; index++) {
			storeId[index] = (index * 11 + 5) & 0xff;
		}

		const entryCount = 8;

		// --- Session 1: on-disk native client, append + commit N docs ---------
		// Network is enabled so the native backbone activates; no peer is ever
		// dialed, so the only source of blocks/heads/coordinates on the next open
		// is disk.
		client = await Peerbit.create({
			directory,
			...createRustPeerbitOptions(),
		});

		const store1 = await client.open(
			new EventStore<string, any>({ id: storeId }),
			{ args: { replicate: { factor: 1 } } },
		);

		const log1 = store1.log as any;
		expect(log1._nativeBackbone, "session 1 native backbone").to.exist;
		expect(
			log1._nativeBackboneCoordinatePersistence,
			"session 1 auto-derived coordinate persistence",
		).to.exist;
		const durableBlockOptions = log1.remoteBlocks.localStore?.durable?._store
			?.options as
			| {
					compactOnClose?: boolean;
					compactOnCloseMinJournalBytes?: number;
			  }
			| undefined;
		expect(
			durableBlockOptions?.compactOnClose,
			"native durable block sublevel skips redundant close snapshots",
		).to.equal(false);
		expect(
			durableBlockOptions?.compactOnCloseMinJournalBytes,
			"native durable block WAL retains an explicit close-time compaction threshold",
		).to.equal(512 * 1024 * 1024);

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
		const preRestartCoordinateHashes = [
			...(log1._nativeBackbone.getEntryCoordinateHashes() as string[]),
		].sort();
		expect(preRestartCoordinateHashes.length).to.equal(entryCount);

		const identity = client.identity;

		// A clean stop flushes the durable block sublevel + coordinate WAL.
		await client.stop();
		client = undefined;

		// The node storage lives in a single LevelDB at `<directory>/cache`; the
		// per-program `blocks` sublevel is a key-prefix inside it (LevelDB
		// sublevels are logical, not separate directories). After a clean stop
		// that LevelDB directory must exist and hold data files, i.e. the blocks
		// written through the wrapper were flushed to disk.
		const cacheDir = path.join(directory, "cache");
		const cacheExists = await fs
			.stat(cacheDir)
			.then(() => true)
			.catch(() => false);
		expect(cacheExists, `durable node storage ${cacheDir}`).to.equal(true);
		const cacheEntries = await fs.readdir(cacheDir);
		expect(
			cacheEntries.length,
			`durable node storage ${cacheDir} is non-empty`,
		).to.be.greaterThan(0);

		// --- Session 2: FRESH client on the SAME directory, NO peers ----------
		client = await Peerbit.create({
			directory,
			...createRustPeerbitOptions(),
		});
		expect(client.identity.publicKey.equals(identity.publicKey)).to.equal(
			true,
			"same identity restored from disk",
		);

		// (i) Reopen must NOT throw "Failed to load entry from head".
		const store2 = await client.open(
			new EventStore<string, any>({ id: storeId }),
			{ args: { replicate: { factor: 1 } } },
		);
		const log2 = store2.log as any;
		expect(log2._nativeBackbone, "session 2 native backbone").to.exist;

		// This is the exact call that previously failed: resolving the heads
		// walks the DAG through the block store, which was empty after restart.
		const restoredHeads = (await store2.log.log.getHeads(true).all())
			.map((entry: any) => entry.hash)
			.sort();

		// (iii) heads + length match pre-restart.
		expect(restoredHeads, "restored heads match pre-restart").to.deep.equal(
			preRestartHeads,
		);
		expect(store2.log.log.length, "restored log length").to.equal(
			preRestartLength,
		);

		// (ii) all N documents queryable locally (blocks survived).
		const restoredValues = (await store2.iterator({ limit: entryCount }))
			.collect()
			.map((entry: any) => entry.payload.getValue().value)
			.sort();
		expect(restoredValues, "restored documents match pre-restart").to.deep.equal(
			preRestartValues,
		);

		// (iv) coordinates restored (coordinate slice; kept green here).
		const restoredCoordinateHashes = [
			...(log2._nativeBackbone.getEntryCoordinateHashes() as string[]),
		].sort();
		expect(
			restoredCoordinateHashes,
			"restored coordinates match pre-restart",
		).to.deep.equal(preRestartCoordinateHashes);
	});
});
