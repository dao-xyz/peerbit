import { expect } from "chai";
import * as fs from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	NativeBackboneBufferedCoordinatePersistenceStore,
	NativeBackboneNodeCoordinatePersistenceStore,
} from "../src/index.js";

describe("Node coordinate persistence sync access", () => {
	let directory: string;
	let store: NativeBackboneNodeCoordinatePersistenceStore;
	let regularFileSyncError: Error | undefined;
	let opened: { path: string; flags: string; syncs: number; closed: boolean }[];
	let directorySyncs: number;

	beforeEach(async () => {
		directory = await fs.mkdtemp(join(tmpdir(), "peerbit-coordinate-sync-"));
		regularFileSyncError = undefined;
		opened = [];
		directorySyncs = 0;
		store = new NativeBackboneNodeCoordinatePersistenceStore(directory, {
			...fs,
			open: async (path, flags) => {
				if (path === directory) {
					expect(flags).to.equal("r");
					// Keep this fixture about regular-file access. Directory syncing has
					// a separate, unchanged platform policy in the production store.
					return {
						write: async () => {
							throw new Error("unexpected directory write");
						},
						sync: async () => {
							directorySyncs++;
						},
						close: async () => {},
					};
				}
				const handle = await fs.open(path, flags);
				const record = { path, flags, syncs: 0, closed: false };
				opened.push(record);
				return {
					write: (bytes) => handle.write(bytes),
					sync: async () => {
						// Windows FlushFileBuffers refuses read-only regular-file handles.
						if (flags === "r") {
							throw Object.assign(new Error("read-only file fsync"), {
								code: "EPERM",
							});
						}
						if (regularFileSyncError) throw regularFileSyncError;
						await handle.sync();
						record.syncs++;
					},
					close: async () => {
						await handle.close();
						record.closed = true;
					},
				};
			},
		});
	});

	afterEach(async () => {
		await store.close();
		await fs.rm(directory, { recursive: true, force: true });
	});

	it("syncs a reopened coordinate WAL without truncating its bytes", async () => {
		const bytes = new Uint8Array([1, 2, 3, 4]);
		const path = join(directory, "coordinates.wal");
		await fs.writeFile(path, bytes);
		const buffered = new NativeBackboneBufferedCoordinatePersistenceStore(
			store,
		);

		// A fresh store has no cached append handle. Its hydration read must first
		// flush the named backing file using a handle that can actually sync it.
		expect(await buffered.read("coordinates.wal")).to.deep.equal(bytes);
		expect(new Uint8Array(await fs.readFile(path))).to.deep.equal(bytes);
		expect(opened).to.deep.equal([
			{ path, flags: "r+", syncs: 1, closed: true },
		]);
		expect(directorySyncs).to.equal(1);
	});

	it("syncs an atomic coordinate replacement before publishing its bytes", async () => {
		const path = join(directory, "coordinates.snapshot");
		await fs.writeFile(path, new Uint8Array([1]));
		const replacement = new Uint8Array([2, 3]);

		await store.atomicReplace!("coordinates.snapshot", replacement);
		expect(new Uint8Array(await fs.readFile(path))).to.deep.equal(replacement);
		expect(opened).to.have.length(1);
		expect(opened[0]).to.include({ flags: "r+", syncs: 1, closed: true });
		expect(opened[0]!.path).not.to.equal(path);
		expect(directorySyncs).to.equal(1);
	});

	it("keeps regular-file sync failures fatal and closes every handle", async () => {
		const path = join(directory, "coordinates.wal");
		const bytes = new Uint8Array([1, 2, 3]);
		await fs.writeFile(path, bytes);
		for (const code of ["EPERM", "EIO"]) {
			regularFileSyncError = Object.assign(new Error(`injected ${code}`), {
				code,
			});
			expect(
				await store.durableBarrier!("coordinates.wal").catch((error) => error),
			).to.equal(regularFileSyncError);
			expect(
				await store.flush("coordinates.wal").catch((error) => error),
			).to.equal(regularFileSyncError);
			expect(
				await store.atomicReplace!(
					"coordinates.wal",
					new Uint8Array([9]),
				).catch((error) => error),
			).to.equal(regularFileSyncError);
			expect(new Uint8Array(await fs.readFile(path))).to.deep.equal(bytes);
		}
		expect(
			opened.every((handle) => handle.closed && handle.syncs === 0),
		).to.equal(true);
		expect(directorySyncs).to.equal(0);
	});

	it("does not create an absent coordinate file while syncing it", async () => {
		const error = await store.durableBarrier!("missing.wal").catch(
			(error) => error,
		);
		expect(error).to.have.property("code", "ENOENT");
		await store.flush("missing.wal");
		expect(await fs.readdir(directory)).to.deep.equal([]);
		expect(opened).to.deep.equal([]);
		expect(directorySyncs).to.equal(0);
	});
});
