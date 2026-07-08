// Gate for the "durable persistence — first slice": a native node started with
// an on-disk storage `directory` AUTO-PERSISTS its shared-log replication
// coordinates per-program, rooted at `<directory>/coordinates/<hex(log.id)>`,
// so the coordinates survive a clean stop and can be re-hydrated from disk
// WITHOUT any peer to re-derive them from.
//
// Before this slice, the coordinate-persistence machinery
// (NativeBackboneCoordinatePersistence + the Node coordinate store +
// hydrate/flush) was only activated when a caller passed an explicit
// `options.coordinatePersistence`. Nothing derived it from the client
// directory. This test exercises the auto-wire: a `peerbit/rust` client with a
// `directory` derives the store itself.
//
// SCOPE / KNOWN BLOCKER (verified, see the PR notes): a *full* program reopen
// on the same directory does NOT yet resurrect the log, because when the native
// backbone is active the log's entry blocks are held in the backbone's
// wasm-memory block store rather than the durable program block cache
// (`shared-log/src/index.ts`: `localBlocks = this._nativeBackbone.blocks : new
// AnyBlockStore(await storage.sublevel("blocks"))`). On restart the heads index
// still lists the head hashes but the entry blocks backing them are gone, so
// `log.getHeads(true).all()` fails with "Failed to load entry from head".
// Making native-backbone entry blocks durable is a separate, upstream piece of
// work (flush ordering across the block store) and is intentionally out of
// scope for this slice. This test therefore proves the coordinate layer that
// this slice adds — WAL written under the auto-derived namespace on clean stop,
// and those coordinates restored from that same directory into a fresh
// backbone — which is exactly the gap this slice closes.
import { toHexString } from "@peerbit/crypto";
import {
	NativeBackboneCoordinatePersistence,
	NativeBackboneNodeCoordinatePersistenceStore,
	createNativePeerbitBackbone,
} from "@peerbit/native-backbone";
import { expect } from "chai";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import { EventStore } from "./utils/stores/event-store.js";

const fileExists = async (target: string): Promise<boolean> => {
	try {
		await fs.stat(target);
		return true;
	} catch {
		return false;
	}
};

describe("coordinate persistence auto-wire", function () {
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

	it("auto-persists coordinates to disk and restores them from that directory with no peer", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-coordinate-persist-"),
		);

		// A deterministic program id keeps the log.id (coordinate namespace)
		// stable and makes the on-disk directory predictable.
		const storeId = new Uint8Array(32);
		for (let index = 0; index < storeId.length; index++) {
			storeId[index] = (index * 7 + 3) & 0xff;
		}
		const coordinateNamespace = toHexString(storeId);
		const coordinateDir = path.join(
			directory,
			"coordinates",
			coordinateNamespace,
		);

		const entryCount = 8;

		// --- Session 1: on-disk client, append + commit, capture coordinates ---
		// Network is enabled so the native backbone activates; no peer is ever
		// dialed, so the only source of coordinates on the next open is disk.
		client = await Peerbit.create({
			directory,
			...createRustPeerbitOptions(),
		});

		const store1 = await client.open(
			new EventStore<string, any>({ id: storeId }),
			{
				args: { replicate: { factor: 1 } },
			},
		);

		const log1 = store1.log as any;
		// The auto-wire only makes sense with the native backbone present, and
		// must have derived a coordinate persistence adapter (no explicit config
		// was passed) because the node has an on-disk directory.
		expect(log1._nativeBackbone, "session 1 native backbone").to.exist;
		expect(
			log1._nativeBackboneCoordinatePersistence,
			"session 1 auto-derived coordinate persistence",
		).to.exist;

		// Independent heads so each entry produces its own coordinate.
		for (let index = 0; index < entryCount; index++) {
			await store1.add(`entry-${index}`, { meta: { next: [] } });
		}

		const preRestartHashes = [
			...(log1._nativeBackbone.getEntryCoordinateHashes() as string[]),
		].sort();
		expect(
			preRestartHashes.length,
			"session 1 should have coordinates for every entry",
		).to.equal(entryCount);

		const identity = client.identity;

		// A clean close flushes the coordinate WAL and closes the store.
		await client.stop();
		client = undefined;

		// The per-program coordinate directory and its WAL/snapshot must be on
		// disk under the auto-derived `<dir>/coordinates/<hex(id)>` namespace.
		expect(
			await fileExists(coordinateDir),
			`coordinate directory ${coordinateDir}`,
		).to.equal(true);
		const walOnDisk = await fileExists(
			path.join(coordinateDir, "coordinates.wal"),
		);
		const snapshotOnDisk = await fileExists(
			path.join(coordinateDir, "coordinates.bin"),
		);
		expect(
			walOnDisk || snapshotOnDisk,
			`coordinates.wal or coordinates.bin under ${coordinateDir}`,
		).to.equal(true);

		// --- Restore: hydrate a fresh backbone straight from that directory ---
		// This mirrors what shared-log's auto-wire does on reopen (construct the
		// same Node store rooted at the per-program directory and hydrate), but
		// isolated from the separate native block-store durability gap noted at
		// the top of this file. No peer exists; the coordinates come from disk.
		const store = new NativeBackboneNodeCoordinatePersistenceStore(
			coordinateDir,
		);
		const persistence = new NativeBackboneCoordinatePersistence(store);
		const restored = await createNativePeerbitBackbone({
			clockId: identity.publicKey.bytes,
			privateKey: identity.privateKey.privateKey,
			publicKey: identity.publicKey.publicKey,
		});
		await persistence.hydrate(restored);

		const restoredHashes = [
			...(restored.getEntryCoordinateHashes() as string[]),
		].sort();

		expect(
			restoredHashes,
			"restored coordinates match the pre-restart coordinates",
		).to.deep.equal(preRestartHashes);
		expect(restoredHashes.length).to.equal(entryCount);
	});
});
