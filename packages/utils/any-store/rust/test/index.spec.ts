import { type AnyStore } from "@peerbit/any-store-interface";
import { expect } from "chai";
import { mkdtemp, readFile, rm, stat, writeFile } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import sinon from "sinon";
import { createStore } from "../src/index.js";
import {
	NodePersistenceBackend,
	OpfsPersistenceBackend,
} from "../src/persistence.js";

const tempDirectory = async () =>
	mkdtemp(join(tmpdir(), "peerbit-any-store-rust-"));

type StoreInternals = {
	persistence: {
		appendJournal(
			record: Uint8Array,
			durability: "normal" | "strict",
		): Promise<void>;
	};
	journalQueue: Promise<unknown>;
};

const internalsOf = (store: unknown): StoreInternals => store as StoreInternals;

class ShortWriteOpfsDirectory {
	readonly files = new Map<string, { bytes: Uint8Array }>();
	maxWriteBytes = Number.POSITIVE_INFINITY;
	stallWrites = false;
	stallAfterWrites: number | undefined;
	truncateFailure: Error | undefined;

	async getFileHandle(
		name: string,
		options?: { create?: boolean },
	): Promise<FileSystemFileHandle> {
		let file = this.files.get(name);
		if (!file && options?.create) {
			file = { bytes: new Uint8Array() };
			this.files.set(name, file);
		}
		if (!file) {
			const error = new Error(`Missing OPFS test file: ${name}`);
			error.name = "NotFoundError";
			throw error;
		}
		const directory = this;
		return {
			async createSyncAccessHandle() {
				let closed = false;
				return {
					getSize: () => file!.bytes.byteLength,
					write(source: Uint8Array, { at = 0 }: { at?: number } = {}) {
						if (closed) {
							throw new Error("OPFS test handle is closed");
						}
						if (directory.stallWrites || directory.stallAfterWrites === 0) {
							return 0;
						}
						if (directory.stallAfterWrites != null) {
							directory.stallAfterWrites--;
						}
						const count = Math.min(source.byteLength, directory.maxWriteBytes);
						const required = at + count;
						if (required > file!.bytes.byteLength) {
							const grown = new Uint8Array(required);
							grown.set(file!.bytes);
							file!.bytes = grown;
						}
						file!.bytes.set(source.subarray(0, count), at);
						return count;
					},
					read(target: Uint8Array, { at = 0 }: { at?: number } = {}) {
						const count = Math.min(
							target.byteLength,
							Math.max(0, file!.bytes.byteLength - at),
						);
						target.set(file!.bytes.subarray(at, at + count));
						return count;
					},
					truncate(size: number) {
						if (directory.truncateFailure) {
							throw directory.truncateFailure;
						}
						const resized = new Uint8Array(size);
						resized.set(file!.bytes.subarray(0, size));
						file!.bytes = resized;
					},
					flush() {},
					close() {
						closed = true;
					},
				} as unknown as FileSystemSyncAccessHandle;
			},
		} as FileSystemFileHandle;
	}

	async removeEntry(name: string): Promise<void> {
		if (!this.files.delete(name)) {
			const error = new Error(`Missing OPFS test file: ${name}`);
			error.name = "NotFoundError";
			throw error;
		}
	}
}

const collectKeys = async (store: AnyStore): Promise<string[]> => {
	const keys: string[] = [];
	for await (const [key] of store.iterator()) {
		keys.push(key);
	}
	return keys.sort();
};

describe("@peerbit/any-store-rust", () => {
	const cleanup: string[] = [];

	afterEach(async () => {
		await Promise.all(
			cleanup.splice(0).map((dir) => rm(dir, { recursive: true, force: true })),
		);
	});

	it("completes short Node WAL writes before acknowledging and reopens exactly", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);
		const first = new Uint8Array([1]);
		const second = new Uint8Array([2, 3, 4, 5, 6]);
		const backend = new NodePersistenceBackend(directory, []);
		await backend.appendJournal(first, "strict");
		const handle = (
			backend as unknown as {
				journalHandle: {
					write: (...args: any[]) => Promise<{ bytesWritten: number }>;
				};
			}
		).journalHandle;
		const write = handle.write.bind(handle);
		handle.write = (buffer, offset, length, position) =>
			write(buffer, offset, Math.min(length, 2), position);

		await backend.appendJournal(second, "strict");
		await backend.close();

		const reopened = new NodePersistenceBackend(directory, []);
		expect(await reopened.readJournal()).to.deep.equal(
			new Uint8Array([...first, ...second]),
		);
		await reopened.close();
	});

	it("rejects a Node WAL write that makes no progress", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);
		const backend = new NodePersistenceBackend(directory, []);
		await backend.appendJournal(new Uint8Array([1]), "strict");
		const handle = (
			backend as unknown as {
				journalHandle: {
					write: (...args: any[]) => Promise<{ bytesWritten: number }>;
				};
			}
		).journalHandle;
		handle.write = async () => ({ bytesWritten: 0 });

		let thrown: unknown;
		try {
			await backend.appendJournal(new Uint8Array([2]), "strict");
		} catch (error) {
			thrown = error;
		}
		expect(thrown).to.be.instanceOf(Error);
		expect((thrown as Error).message).to.contain("invalid progress");
		await backend.close();
	});

	it("rolls back a partial Node WAL write before a later mutation and reopen", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);
		const backend = new NodePersistenceBackend(directory, []);
		const first = new Uint8Array([1, 2, 3]);
		const rejected = new Uint8Array([4, 5, 6, 7]);
		const later = new Uint8Array([8, 9]);
		await backend.appendJournal(first, "strict");
		const handle = (
			backend as unknown as {
				journalHandle: {
					write: (...args: any[]) => Promise<{ bytesWritten: number }>;
				};
			}
		).journalHandle;
		const write = handle.write.bind(handle);
		let call = 0;
		handle.write = (buffer, offset, length, position) => {
			call++;
			return call === 1
				? write(buffer, offset, Math.min(length, 2), position)
				: Promise.resolve({ bytesWritten: 0 });
		};

		let failure: unknown;
		try {
			await backend.appendJournal(rejected, "strict");
		} catch (error) {
			failure = error;
		}
		expect(failure).to.be.instanceOf(Error);
		handle.write = write;
		await backend.appendJournal(later, "strict");
		await backend.close();

		const reopened = new NodePersistenceBackend(directory, []);
		expect(await reopened.readJournal()).to.deep.equal(
			new Uint8Array([...first, ...later]),
		);
		await reopened.close();
	});

	it("poisons the Node WAL backend when rollback cannot be made durable", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);
		const backend = new NodePersistenceBackend(directory, []);
		await backend.appendJournal(new Uint8Array([1, 2, 3]), "strict");
		const handle = (
			backend as unknown as {
				journalHandle: {
					write: (...args: any[]) => Promise<{ bytesWritten: number }>;
					truncate: (length?: number) => Promise<void>;
				};
			}
		).journalHandle;
		const write = handle.write.bind(handle);
		const truncate = handle.truncate.bind(handle);
		let call = 0;
		handle.write = (buffer, offset, length, position) => {
			call++;
			return call === 1
				? write(buffer, offset, Math.min(length, 2), position)
				: Promise.resolve({ bytesWritten: 0 });
		};
		const rollbackFailure = new Error("injected Node truncate failure");
		handle.truncate = async () => {
			throw rollbackFailure;
		};

		let poison: unknown;
		try {
			await backend.appendJournal(new Uint8Array([4, 5, 6]), "strict");
		} catch (error) {
			poison = error;
		}
		expect(poison).to.be.instanceOf(AggregateError);
		expect((poison as Error).message).to.contain("reopen is required");

		handle.write = write;
		handle.truncate = truncate;
		let repeated: unknown;
		try {
			await backend.appendJournal(new Uint8Array([7]), "strict");
		} catch (error) {
			repeated = error;
		}
		expect(repeated).to.equal(poison);
		await backend.close();
	});

	it("completes short OPFS WAL writes before acknowledging and reopens exactly", async () => {
		const directory = new ShortWriteOpfsDirectory();
		directory.maxWriteBytes = 2;
		const first = new Uint8Array([1, 2, 3]);
		const second = new Uint8Array([4, 5, 6, 7, 8]);
		const backend = new OpfsPersistenceBackend(
			directory as unknown as FileSystemDirectoryHandle,
		);
		await backend.appendJournal(first, "strict");
		await backend.appendJournal(second, "strict");
		await backend.close();

		const reopened = new OpfsPersistenceBackend(
			directory as unknown as FileSystemDirectoryHandle,
		);
		expect(await reopened.readJournal()).to.deep.equal(
			new Uint8Array([...first, ...second]),
		);
		await reopened.close();
	});

	it("rejects an OPFS WAL write that makes no progress", async () => {
		const directory = new ShortWriteOpfsDirectory();
		directory.stallWrites = true;
		const backend = new OpfsPersistenceBackend(
			directory as unknown as FileSystemDirectoryHandle,
		);

		let thrown: unknown;
		try {
			await backend.appendJournal(new Uint8Array([1]), "strict");
		} catch (error) {
			thrown = error;
		}
		expect(thrown).to.be.instanceOf(Error);
		expect((thrown as Error).message).to.contain("invalid progress");
		await backend.close();
	});

	it("rolls back a partial OPFS WAL write before a later mutation and reopen", async () => {
		const directory = new ShortWriteOpfsDirectory();
		const backend = new OpfsPersistenceBackend(
			directory as unknown as FileSystemDirectoryHandle,
		);
		const first = new Uint8Array([1, 2, 3]);
		const rejected = new Uint8Array([4, 5, 6, 7]);
		const later = new Uint8Array([8, 9]);
		await backend.appendJournal(first, "strict");
		directory.maxWriteBytes = 2;
		directory.stallAfterWrites = 1;

		let failure: unknown;
		try {
			await backend.appendJournal(rejected, "strict");
		} catch (error) {
			failure = error;
		}
		expect(failure).to.be.instanceOf(Error);
		directory.stallAfterWrites = undefined;
		directory.maxWriteBytes = Number.POSITIVE_INFINITY;
		await backend.appendJournal(later, "strict");
		await backend.close();

		const reopened = new OpfsPersistenceBackend(
			directory as unknown as FileSystemDirectoryHandle,
		);
		expect(await reopened.readJournal()).to.deep.equal(
			new Uint8Array([...first, ...later]),
		);
		await reopened.close();
	});

	it("poisons the OPFS WAL backend when rollback cannot be made durable", async () => {
		const directory = new ShortWriteOpfsDirectory();
		const backend = new OpfsPersistenceBackend(
			directory as unknown as FileSystemDirectoryHandle,
		);
		await backend.appendJournal(new Uint8Array([1, 2, 3]), "strict");
		directory.maxWriteBytes = 2;
		directory.stallAfterWrites = 1;
		directory.truncateFailure = new Error("injected OPFS truncate failure");

		let poison: unknown;
		try {
			await backend.appendJournal(new Uint8Array([4, 5, 6]), "strict");
		} catch (error) {
			poison = error;
		}
		expect(poison).to.be.instanceOf(AggregateError);
		expect((poison as Error).message).to.contain("reopen is required");

		directory.stallAfterWrites = undefined;
		directory.truncateFailure = undefined;
		directory.maxWriteBytes = Number.POSITIVE_INFINITY;
		let repeated: unknown;
		try {
			await backend.appendJournal(new Uint8Array([7]), "strict");
		} catch (error) {
			repeated = error;
		}
		expect(repeated).to.equal(poison);
		await backend.close();
	});

	it("completes short OPFS checkpoint writes before publishing the manifest", async () => {
		const directory = new ShortWriteOpfsDirectory();
		directory.maxWriteBytes = 2;
		const snapshot = new Uint8Array([1, 2, 3, 4, 5, 6, 7]);
		const backend = new OpfsPersistenceBackend(
			directory as unknown as FileSystemDirectoryHandle,
		);
		await backend.writeSnapshot(snapshot);
		await backend.close();

		const reopened = new OpfsPersistenceBackend(
			directory as unknown as FileSystemDirectoryHandle,
		);
		expect(await reopened.readSnapshot()).to.deep.equal(snapshot);
		expect(await reopened.readJournal()).to.deep.equal(new Uint8Array());
		await reopened.close();
	});

	it("rejects an OPFS checkpoint write that makes no progress", async () => {
		const directory = new ShortWriteOpfsDirectory();
		directory.stallWrites = true;
		const backend = new OpfsPersistenceBackend(
			directory as unknown as FileSystemDirectoryHandle,
		);

		const failure = await backend.writeSnapshot(new Uint8Array([1])).then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(failure).to.be.instanceOf(Error);
		expect((failure as Error).message).to.contain("invalid progress");
		await backend.close();
	});

	it("stores transient values", async () => {
		const store = createStore();
		await store.open();
		expect(store.put("a", new Uint8Array([1, 2, 3]))).to.equal(undefined);
		await store.put("b", new Uint8Array([4]));

		expect(await store.get("a")).to.deep.equal(new Uint8Array([1, 2, 3]));
		expect(await store.size()).to.equal(4);
		expect(await collectKeys(store)).to.deep.equal(["a", "b"]);

		await store.del("a");
		expect(await store.get("a")).to.equal(undefined);
		expect(await store.size()).to.equal(1);
		await store.close();
	});

	it("applies batched mutations", async () => {
		const store = createStore();
		await store.open();
		expect(
			store.putMany([
				["a", new Uint8Array([1])],
				["b", new Uint8Array([2, 3])],
			]),
		).to.equal(undefined);

		expect(await store.getMany(["a", "b", "c"])).to.deep.equal([
			new Uint8Array([1]),
			new Uint8Array([2, 3]),
			undefined,
		]);
		expect(await store.hasMany(["c", "a", "b"])).to.deep.equal([
			false,
			true,
			true,
		]);
		expect(await store.size()).to.equal(3);
		expect(await store.delMany(["a", "missing"])).to.equal(1);
		expect(await collectKeys(store)).to.deep.equal(["b"]);
		await store.close();
	});

	it("stores immutable transient values without forcing an async boundary", async () => {
		const store = createStore();
		await store.open();
		expect(store.putImmutable("a", new Uint8Array([1, 2, 3]))).to.equal(
			undefined,
		);
		expect(
			store.putManyImmutable([
				["b", new Uint8Array([4])],
				["c", new Uint8Array([5, 6])],
			]),
		).to.equal(undefined);

		expect(await store.getMany(["a", "b", "c"])).to.deep.equal([
			new Uint8Array([1, 2, 3]),
			new Uint8Array([4]),
			new Uint8Array([5, 6]),
		]);
		await store.close();
	});

	it("persists values across reopen", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		let store = createStore(directory);
		await store.open();
		await store.put("a", new Uint8Array([1, 2, 3]));
		await store.close();

		store = createStore(directory);
		await store.open();
		expect(await store.persisted()).to.equal(true);
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1, 2, 3]));
		expect(await store.size()).to.equal(3);
		await store.close();
	});

	it("persists immutable values across reopen", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		let store = createStore(directory);
		await store.open();
		expect(store.putImmutable("a", new Uint8Array([1, 2, 3]))).to.equal(
			undefined,
		);
		expect(
			store.putManyImmutable([
				["b", new Uint8Array([4])],
				["c", new Uint8Array([5, 6])],
			]),
		).to.equal(undefined);
		await store.close();

		store = createStore(directory);
		await store.open();
		expect(await store.getMany(["a", "b", "c"])).to.deep.equal([
			new Uint8Array([1, 2, 3]),
			new Uint8Array([4]),
			new Uint8Array([5, 6]),
		]);
		await store.close();
	});

	it("keeps strict immutable persistence on an async durability boundary", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		const store = createStore(directory, { durability: "strict" });
		await store.open();
		expect(store.putImmutable("a", new Uint8Array([1]))).to.be.instanceOf(
			Promise,
		);
		expect(
			store.putManyImmutable([["b", new Uint8Array([2])]]),
		).to.be.instanceOf(Promise);
		await store.close();
	});

	it("persists journaled deletes before compaction", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		let store = createStore(directory, { compactOnClose: false });
		await store.open();
		await store.put("a", new Uint8Array([1]));
		await store.put("b", new Uint8Array([2]));
		await store.del("a");
		await store.close();

		store = createStore(directory);
		await store.open();
		expect(await store.get("a")).to.equal(undefined);
		expect(await store.get("b")).to.deep.equal(new Uint8Array([2]));
		await store.close();
	});

	it("validates immutable options when returning a cached sublevel", async () => {
		const store = createStore();
		await store.open();
		const configured = await store.sublevel("blocks", {
			compactOnClose: false,
			compactOnCloseMinJournalBytes: 1024,
		});
		expect(await store.sublevel("blocks")).to.equal(configured);
		expect(
			await store.sublevel("blocks", {
				compactOnClose: false,
				compactOnCloseMinJournalBytes: 1024,
			}),
		).to.equal(configured);
		expect(
			await store.sublevel("blocks", {
				compactOnClose: undefined,
				compactOnCloseMinJournalBytes: undefined,
			}),
		).to.equal(configured);

		let conflict: unknown;
		try {
			await store.sublevel("blocks", { compactOnClose: true });
		} catch (error) {
			conflict = error;
		}
		expect(conflict).to.be.instanceOf(Error);
		expect((conflict as Error).message).to.contain(
			'sublevel "blocks" already exists with compactOnClose=false; requested true',
		);

		conflict = undefined;
		try {
			await store.sublevel("blocks", {
				compactOnCloseMinJournalBytes: 2048,
			});
		} catch (error) {
			conflict = error;
		}
		expect(conflict).to.be.instanceOf(Error);
		expect((conflict as Error).message).to.contain(
			"compactOnCloseMinJournalBytes=1024; requested 2048",
		);

		const defaultConfigured = await store.sublevel("default-blocks");
		expect(
			await store.sublevel("default-blocks", {
				compactOnClose: true,
				durability: "normal",
			}),
		).to.equal(defaultConfigured);
		conflict = undefined;
		try {
			await store.sublevel("default-blocks", { compactOnClose: false });
		} catch (error) {
			conflict = error;
		}
		expect(conflict).to.be.instanceOf(Error);
		expect((conflict as Error).message).to.contain(
			'sublevel "default-blocks" already exists with compactOnClose=true; requested false',
		);
		await store.close();
	});

	it("defers sublevel close compaction below its journal threshold and recovers a torn tail", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);
		const sublevelDirectory = join(directory, "sublevels", "blocks");
		const snapshotPath = join(sublevelDirectory, "store.bin");
		const journalPath = join(sublevelDirectory, "store.wal");
		const sublevelOptions = {
			compactOnClose: false,
			compactOnCloseMinJournalBytes: 1024,
		};

		let root = createStore(directory);
		await root.open();
		let blocks = await root.sublevel("blocks", sublevelOptions);
		await blocks.put("a", new Uint8Array([1]));
		await blocks.put("b", new Uint8Array([2, 3]));
		await root.close();

		// The generic root still uses its unchanged compact-on-close default, while
		// the explicitly configured append-heavy child remains journal-backed below
		// its close-time compaction threshold.
		expect(await stat(join(directory, "store.bin"))).to.exist;
		const snapshotExists = await stat(snapshotPath)
			.then(() => true)
			.catch(() => false);
		expect(snapshotExists).to.equal(false);
		const journal = await readFile(journalPath);
		expect(journal.byteLength).to.be.greaterThan(0);
		expect(journal.byteLength).to.be.lessThan(
			sublevelOptions.compactOnCloseMinJournalBytes,
		);

		// Tear the second record. Reopen must retain the complete first record and
		// durably truncate the incomplete tail before accepting new writes.
		await writeFile(journalPath, journal.subarray(0, journal.byteLength - 3));
		root = createStore(directory);
		await root.open();
		blocks = await root.sublevel("blocks", sublevelOptions);
		expect(await blocks.get("a")).to.deep.equal(new Uint8Array([1]));
		expect(await blocks.get("b")).to.equal(undefined);
		await blocks.put("c", new Uint8Array([4]));
		await root.close();

		root = createStore(directory);
		await root.open();
		blocks = await root.sublevel("blocks", sublevelOptions);
		expect(await blocks.get("a")).to.deep.equal(new Uint8Array([1]));
		expect(await blocks.get("b")).to.equal(undefined);
		expect(await blocks.get("c")).to.deep.equal(new Uint8Array([4]));
		await root.close();
	});

	it("compacts an opted-out sublevel when its close-time journal threshold is reached", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		const root = createStore(directory);
		await root.open();
		const blocks = await root.sublevel("blocks", {
			compactOnClose: false,
			compactOnCloseMinJournalBytes: 1,
		});
		await blocks.put("a", new Uint8Array([1]));
		await root.close();

		const sublevelDirectory = join(directory, "sublevels", "blocks");
		expect(await stat(join(sublevelDirectory, "store.bin"))).to.exist;
		expect(
			(await readFile(join(sublevelDirectory, "store.wal"))).byteLength,
		).to.equal(0);
	});

	it("never checkpoints a strict store even when close compaction is forced", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		const store = createStore(directory, {
			durability: "strict",
			compactOnClose: true,
			compactOnCloseMinJournalBytes: 1,
		});
		await store.open();
		await store.put("a", new Uint8Array([1]));
		await store.close();

		const snapshotExists = await stat(join(directory, "store.bin"))
			.then(() => true)
			.catch(() => false);
		expect(snapshotExists).to.equal(false);
		expect(
			(await readFile(join(directory, "store.wal"))).byteLength,
		).to.be.greaterThan(0);
	});

	it("survives two torn-tail crashes without checkpointing a strict store", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);
		const journalPath = join(directory, "store.wal");
		const options = {
			durability: "strict" as const,
			compactOnClose: true,
			compactOnCloseMinJournalBytes: 1,
		};

		let store = createStore(directory, options);
		await store.open();
		await store.put("a", new Uint8Array([1]));
		await store.put("b", new Uint8Array([2]));
		await store.close();

		let journal = await readFile(journalPath);
		await writeFile(journalPath, journal.subarray(0, journal.byteLength - 3));
		store = createStore(directory, options);
		await store.open();
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1]));
		expect(await store.get("b")).to.equal(undefined);
		await store.put("c", new Uint8Array([3]));
		await store.close();

		expect(
			await stat(join(directory, "store.bin"))
				.then(() => true)
				.catch(() => false),
		).to.equal(false);
		journal = await readFile(journalPath);
		const tornAgain = new Uint8Array(journal.byteLength + 3);
		tornAgain.set(journal);
		// A recoverable EOF tail must be a structural prefix of the next frame,
		// not arbitrary corruption that happens to be shorter than the magic.
		tornAgain.set(new TextEncoder().encode("PBA"), journal.byteLength);
		await writeFile(journalPath, tornAgain);

		store = createStore(directory, options);
		await store.open();
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1]));
		expect(await store.get("b")).to.equal(undefined);
		expect(await store.get("c")).to.deep.equal(new Uint8Array([3]));
		await store.close();
		expect((await readFile(journalPath)).byteLength).to.equal(
			journal.byteLength,
		);
	});

	it("rejects a complete checksum-bad strict WAL without rewriting it", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);
		const journalPath = join(directory, "store.wal");
		const snapshotPath = join(directory, "store.bin");
		const options = {
			durability: "strict" as const,
			compactOnClose: false,
		};

		const writer = createStore(directory, options);
		await writer.open();
		await writer.put("a", new Uint8Array([1]));
		await writer.put("b", new Uint8Array([2, 3]));
		await writer.close();

		const validJournal = new Uint8Array(await readFile(journalPath));
		const corruptJournal = new Uint8Array(validJournal);
		corruptJournal[corruptJournal.byteLength - 1] ^= 0xff;
		await writeFile(journalPath, corruptJournal);

		const store = createStore(directory, options);
		const failure = await store.open().then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(failure)).to.contain("journal checksum mismatch");
		expect(store.status()).to.equal("closed");
		expect(new Uint8Array(await readFile(journalPath))).to.deep.equal(
			corruptJournal,
		);
		expect(
			await stat(snapshotPath)
				.then(() => true)
				.catch(() => false),
		).to.equal(false);

		await writeFile(journalPath, validJournal);
		await store.open();
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1]));
		expect(await store.get("b")).to.deep.equal(new Uint8Array([2, 3]));
		await store.close();
	});

	it("closes a failed replay backend, aggregates close failure, and retries", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);
		const journalPath = join(directory, "store.wal");
		const options = {
			durability: "strict" as const,
			compactOnClose: false,
		};

		const writer = createStore(directory, options);
		await writer.open();
		await writer.put("kept", new Uint8Array([1]));
		await writer.put("torn", new Uint8Array([2]));
		await writer.close();
		const journal = await readFile(journalPath);
		await writeFile(journalPath, journal.subarray(0, journal.byteLength - 3));

		const truncateFailure = new Error("injected replay truncate failure");
		const closeFailure = new Error("injected replay backend close failure");
		const originalTruncate = NodePersistenceBackend.prototype.truncateJournal;
		const originalClose = NodePersistenceBackend.prototype.close;
		const truncate = sinon
			.stub(NodePersistenceBackend.prototype, "truncateJournal")
			.callsFake(async function (this: NodePersistenceBackend) {
				// Open the WAL handle without changing its torn bytes so cleanup has a
				// real resource to release and the retry must still repair the tail.
				await this.appendJournal(new Uint8Array(), "strict");
				throw truncateFailure;
			});
		const close = sinon
			.stub(NodePersistenceBackend.prototype, "close")
			.callsFake(async function (this: NodePersistenceBackend) {
				await originalClose.call(this);
				throw closeFailure;
			});
		const store = createStore(directory, options);
		try {
			const failure = await store.open().then(
				() => undefined,
				(error: unknown) => error,
			);
			expect(failure).to.be.instanceOf(AggregateError);
			expect((failure as AggregateError).errors).to.deep.equal([
				truncateFailure,
				closeFailure,
			]);
			expect(truncate.calledOnce).to.equal(true);
			expect(close.calledOnce).to.equal(true);
			expect(store.status()).to.equal("closed");
		} finally {
			truncate.restore();
			close.restore();
		}

		// Reopening the same object proves the failed generation did not retain
		// the poisoned backend or its file handle.
		expect(NodePersistenceBackend.prototype.truncateJournal).to.equal(
			originalTruncate,
		);
		await store.open();
		expect(await store.get("kept")).to.deep.equal(new Uint8Array([1]));
		expect(await store.get("torn")).to.equal(undefined);
		await store.close();
	});

	it("recovers from a torn journal tail and keeps later writes", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		let store = createStore(directory, { compactOnClose: false });
		await store.open();
		await store.put("a", new Uint8Array([1]));
		await store.put("b", new Uint8Array([2]));
		await store.close();

		// Simulate a crash mid-append: the last journal record is torn.
		const journalPath = join(directory, "store.wal");
		const journal = await readFile(journalPath);
		await writeFile(journalPath, journal.subarray(0, journal.byteLength - 3));

		store = createStore(directory, { compactOnClose: false });
		await store.open();
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1]));
		expect(await store.get("b")).to.equal(undefined);
		await store.put("c", new Uint8Array([3]));
		await store.close();

		store = createStore(directory);
		await store.open();
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1]));
		expect(await store.get("b")).to.equal(undefined);
		expect(await store.get("c")).to.deep.equal(new Uint8Array([3]));
		await store.close();
	});

	it("keeps sublevels isolated and clears them from the parent", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		let store = createStore(directory);
		await store.open();
		const sublevel = await store.sublevel("sub/level");
		await store.put("a", new Uint8Array([1]));
		await sublevel.put("a", new Uint8Array([2]));
		await store.close();

		store = createStore(directory);
		await store.open();
		const reopenedSublevel = await store.sublevel("sub/level");
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1]));
		expect(await reopenedSublevel.get("a")).to.deep.equal(new Uint8Array([2]));

		await store.clear();
		expect(await store.get("a")).to.equal(undefined);
		expect(await reopenedSublevel.get("a")).to.equal(undefined);
		await store.close();

		store = createStore(directory);
		await store.open();
		const clearedSublevel = await store.sublevel("sub/level");
		expect(await store.get("a")).to.equal(undefined);
		expect(await clearedSublevel.get("a")).to.equal(undefined);
		await store.close();
	});

	it("keeps a failed journal append poisoned until close and reopen", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		const store = createStore(directory);
		await store.open();
		await store.put("a", new Uint8Array([1]));
		const internals = internalsOf(store);
		await internals.journalQueue;

		const failure = new Error("journal append failed");
		const persistence = internals.persistence;
		const originalAppend = persistence.appendJournal.bind(persistence);
		let failNext = true;
		persistence.appendJournal = (record, durability) => {
			if (failNext) {
				failNext = false;
				return Promise.reject(failure);
			}
			return originalAppend(record, durability);
		};

		// normal durability: the put resolves while the append fails behind it
		await store.put("b", new Uint8Array([2]));
		await internals.journalQueue;

		let reported: unknown;
		try {
			await store.put("c", new Uint8Array([3]));
		} catch (error) {
			reported = error;
		}
		expect(reported).to.equal(failure);

		let repeated: unknown;
		try {
			await store.put("c", new Uint8Array([3]));
		} catch (error) {
			repeated = error;
		}
		expect(repeated).to.equal(failure);
		let closeFailure: unknown;
		try {
			await store.close();
		} catch (error) {
			closeFailure = error;
		}
		expect(closeFailure).to.equal(failure);
		expect(store.status()).to.equal("closed");

		await store.open();
		await store.put("c", new Uint8Array([3]));
		expect(await store.get("c")).to.deep.equal(new Uint8Array([3]));
		await store.close();
	});

	it("rejects strict mutations queued behind the sticky first journal error", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		const store = createStore(directory, {
			durability: "strict",
			compactOnClose: false,
		});
		await store.open();
		await store.put("kept", new Uint8Array([1]));
		const internals = internalsOf(store);
		await internals.journalQueue;

		const failure = new Error("strict first journal append failed");
		let markStarted!: () => void;
		const started = new Promise<void>((resolve) => {
			markStarted = resolve;
		});
		let releaseFailure!: () => void;
		const failureGate = new Promise<void>((resolve) => {
			releaseFailure = resolve;
		});
		let appendCalls = 0;
		internals.persistence.appendJournal = async () => {
			appendCalls++;
			markStarted();
			await failureGate;
			throw failure;
		};

		const first = Promise.resolve(store.put("failed", new Uint8Array([2])));
		await started;
		const queuedBatch = Promise.resolve(
			store.putMany([
				["queued-a", new Uint8Array([3])],
				["queued-b", new Uint8Array([4])],
			]),
		);
		const queuedDelete = Promise.resolve(store.del("kept"));
		releaseFailure();

		const results = await Promise.allSettled([
			first,
			queuedBatch,
			queuedDelete,
		]);
		for (const result of results) {
			expect(result.status).to.equal("rejected");
			expect((result as PromiseRejectedResult).reason).to.equal(failure);
		}
		expect(appendCalls).to.equal(1);
		expect(await store.get("failed")).to.equal(undefined);
		expect(await store.get("queued-a")).to.equal(undefined);
		expect(await store.get("queued-b")).to.equal(undefined);
		expect(await store.get("kept")).to.deep.equal(new Uint8Array([1]));

		const closeFailure = await store.close().then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(closeFailure).to.equal(failure);
		await store.open();
		expect(await store.get("failed")).to.equal(undefined);
		expect(await store.get("queued-a")).to.equal(undefined);
		expect(await store.get("queued-b")).to.equal(undefined);
		expect(await store.get("kept")).to.deep.equal(new Uint8Array([1]));
		await store.close();
	});

	it("reaches closed status when the journal flush fails during close", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		const store = createStore(directory, { compactOnClose: false });
		await store.open();
		await store.put("a", new Uint8Array([1]));
		const internals = internalsOf(store);
		await internals.journalQueue;

		const failure = new Error("journal append failed");
		internals.persistence.appendJournal = () => Promise.reject(failure);
		await store.put("b", new Uint8Array([2]));

		let closeError: unknown;
		try {
			await store.close();
		} catch (error) {
			closeError = error;
		}
		expect(closeError).to.equal(failure);
		expect(store.status()).to.equal("closed");

		// fully released: the store can be reopened and read again
		await store.open();
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1]));
		await store.close();
		expect(store.status()).to.equal("closed");
	});

	it("keeps the store closed when close() races open()", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		const store = createStore(directory);
		const open = store.open();
		const close = store.close();
		await Promise.all([open, close]);
		expect(store.status()).to.equal("closed");

		// the interleaved open must not resurrect the store afterwards
		await new Promise((resolve) => setTimeout(resolve, 20));
		expect(store.status()).to.equal("closed");

		await store.open();
		expect(store.status()).to.equal("open");
		await store.put("a", new Uint8Array([1]));
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1]));
		await store.close();
		expect(store.status()).to.equal("closed");
	});

	it("rejects mutations after close until reopened", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);

		const store = createStore(directory);
		await store.open();
		await store.put("a", new Uint8Array([1]));
		const close = store.close();
		let duringClose: unknown;
		try {
			await store.put("b", new Uint8Array([2]));
		} catch (error) {
			duringClose = error;
		}
		await close;
		expect(duringClose).to.be.instanceOf(Error);
		expect((duringClose as Error).message).to.equal("RustAnyStore is closed");
		expect(store.status()).to.equal("closed");

		let afterClose: unknown;
		try {
			await store.put("c", new Uint8Array([3]));
		} catch (error) {
			afterClose = error;
		}
		expect(afterClose).to.be.instanceOf(Error);
		expect(store.status()).to.equal("closed");

		// explicit open() restores service, matching the level backend
		await store.open();
		expect(await store.get("a")).to.deep.equal(new Uint8Array([1]));
		await store.put("d", new Uint8Array([4]));
		expect(await store.get("d")).to.deep.equal(new Uint8Array([4]));
		await store.close();

		const transient = createStore();
		await transient.open();
		const transientClose = transient.close();
		let transientThrown: unknown;
		try {
			await transient.put("a", new Uint8Array([1]));
		} catch (error) {
			transientThrown = error;
		}
		await transientClose;
		expect(transientThrown).to.be.instanceOf(Error);
		expect(transient.status()).to.equal("closed");
	});

	it("handles special-character keys and repeated deletes", async () => {
		const store = createStore();
		await store.open();
		const key = "* _ /";
		await store.put(key, new Uint8Array([123]));
		store.del(key);
		store.del(key);
		await store.del(key);
		expect(await store.get(key)).to.equal(undefined);
		expect(await store.size()).to.equal(0);
		await store.close();
	});
});
