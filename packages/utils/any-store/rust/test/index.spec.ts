import { type AnyStore } from "@peerbit/any-store-interface";
import { expect } from "chai";
import { spawn } from "child_process";
import * as nodeFs from "fs/promises";
import { mkdtemp, readFile, readdir, rm, stat, writeFile } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import sinon from "sinon";
import { createStore } from "../src/index.js";
import {
	NodePersistenceBackend,
	OpfsPersistenceBackend,
	supportsCrashSafeNodeJournalReplacement,
} from "../src/persistence.js";

const tempDirectory = async () =>
	mkdtemp(join(tmpdir(), "peerbit-any-store-rust-"));

const itCrashSafeNode = process.platform === "win32" ? it.skip : it;

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

	it("advertises strict checkpoint replacement only with a durable Node directory barrier", () => {
		const backend = new NodePersistenceBackend("unused", []);
		const expected = process.platform !== "win32";
		expect(backend.crashSafeJournalReplacement).to.equal(expected);
		expect(createStore("unused").supportsCrashSafeJournalCheckpoint).to.equal(
			expected,
		);
		expect(createStore().supportsCrashSafeJournalCheckpoint).to.equal(false);
	});

	it("fails checkpoint capability closed outside a definite POSIX Node runtime", () => {
		const descriptor = Object.getOwnPropertyDescriptor(globalThis, "process")!;
		try {
			for (const processLike of [
				{ versions: { node: process.versions.node } },
				{ versions: { node: process.versions.node }, platform: "win32" },
				{ versions: {}, platform: "darwin" },
			]) {
				Object.defineProperty(globalThis, "process", {
					configurable: descriptor.configurable,
					enumerable: descriptor.enumerable,
					writable: true,
					value: processLike,
				});
				expect(supportsCrashSafeNodeJournalReplacement()).to.equal(false);
			}
		} finally {
			Object.defineProperty(globalThis, "process", descriptor);
		}
	});

	it("does no checkpoint sidecar I/O for default Node persistence", async () => {
		const directory = await tempDirectory();
		cleanup.push(directory);
		const injected = new Proxy(nodeFs, {
			get(target, property, receiver) {
				if (property === "readFile") {
					return async (targetPath: string, ...args: unknown[]) => {
						if (targetPath.includes("checkpoint")) {
							throw new Error("unexpected default checkpoint read");
						}
						return (nodeFs.readFile as (...args: unknown[]) => unknown)(
							targetPath,
							...args,
						);
					};
				}
				if (property === "rm") {
					return async (targetPath: string, ...args: unknown[]) => {
						if (
							targetPath.includes("checkpoint") ||
							targetPath.includes("replacement")
						) {
							throw new Error("unexpected default checkpoint cleanup");
						}
						return (nodeFs.rm as (...args: unknown[]) => unknown)(
							targetPath,
							...args,
						);
					};
				}
				return Reflect.get(target, property, receiver);
			},
		}) as typeof nodeFs;
		const backend = new NodePersistenceBackend(directory, [], injected);
		expect(await backend.readJournal()).to.equal(undefined);
		await backend.writeSnapshot(new Uint8Array([1]));
		await backend.close();
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

	itCrashSafeNode(
		"atomically replaces a Node WAL with a verified checkpoint prefix",
		async () => {
			const directory = await tempDirectory();
			cleanup.push(directory);
			const backend = new NodePersistenceBackend(directory, []);
			await backend.appendJournal(new Uint8Array([1, 2, 3]), "strict");
			const records = [new Uint8Array([9, 8]), new Uint8Array([7, 6, 5])];
			const byteLength = await backend.replaceJournalWithCheckpoint(records);
			expect(byteLength).to.equal(5);
			await backend.close();

			const reopened = new NodePersistenceBackend(directory, []);
			const journal = (await reopened.readJournal())!;
			expect(journal).to.deep.equal(new Uint8Array([9, 8, 7, 6, 5]));
			expect(
				await reopened.readJournalCheckpointBase(journal.byteLength),
			).to.equal(5);
			expect(await readdir(directory)).to.not.include(
				"store.wal.replacement.tmp",
			);
			expect(await readdir(directory)).to.not.include(
				"store.wal.checkpoint.tmp",
			);
			await reopened.close();
		},
	);

	itCrashSafeNode(
		"keeps the old WAL and hint when checkpoint construction fails",
		async () => {
			const directory = await tempDirectory();
			cleanup.push(directory);
			const checkpoint = new Uint8Array([1, 2, 3]);
			const suffix = new Uint8Array([4]);
			const oldJournal = new Uint8Array([...checkpoint, ...suffix]);
			const setup = new NodePersistenceBackend(directory, []);
			await setup.replaceJournalWithCheckpoint([checkpoint]);
			await setup.appendJournal(suffix, "strict");
			await setup.close();

			const constructionFailure = new Error(
				"injected checkpoint temporary sync failure",
			);
			const injected = new Proxy(nodeFs, {
				get(target, property, receiver) {
					if (property === "open") {
						return async (targetPath: string, flags: string) => {
							const handle = await nodeFs.open(targetPath, flags);
							if (!targetPath.endsWith("store.wal.replacement.tmp")) {
								return handle;
							}
							return new Proxy(handle, {
								get(handleTarget, handleProperty, handleReceiver) {
									if (handleProperty === "sync") {
										return async () => {
											throw constructionFailure;
										};
									}
									const value = Reflect.get(
										handleTarget,
										handleProperty,
										handleReceiver,
									);
									return typeof value === "function"
										? value.bind(handleTarget)
										: value;
								},
							});
						};
					}
					return Reflect.get(target, property, receiver);
				},
			}) as typeof nodeFs;
			const backend = new NodePersistenceBackend(directory, [], injected);
			const failure = await backend
				.replaceJournalWithCheckpoint([new Uint8Array([5, 6])])
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			expect(failure).to.equal(constructionFailure);
			await backend.close();

			const reopened = new NodePersistenceBackend(directory, []);
			const journal = (await reopened.readJournal())!;
			expect(journal).to.deep.equal(oldJournal);
			expect(
				await reopened.readJournalCheckpointBase(journal.byteLength),
			).to.equal(checkpoint.byteLength);
			expect(await readdir(directory)).to.not.include(
				"store.wal.replacement.tmp",
			);
			await reopened.close();
		},
	);

	itCrashSafeNode(
		"invalidates the prior checkpoint hint before a failed Node WAL rename",
		async () => {
			const directory = await tempDirectory();
			cleanup.push(directory);
			const checkpoint = new Uint8Array([1, 2, 3]);
			const suffix = new Uint8Array([4]);
			const oldJournal = new Uint8Array([...checkpoint, ...suffix]);
			const setup = new NodePersistenceBackend(directory, []);
			await setup.replaceJournalWithCheckpoint([checkpoint]);
			await setup.appendJournal(suffix, "strict");
			await setup.close();

			const renameFailure = new Error("injected checkpoint rename failure");
			const injected = new Proxy(nodeFs, {
				get(target, property, receiver) {
					if (property === "rename") {
						return async (from: string, to: string) => {
							if (to.endsWith("store.wal")) throw renameFailure;
							return nodeFs.rename(from, to);
						};
					}
					return Reflect.get(target, property, receiver);
				},
			}) as typeof nodeFs;
			const backend = new NodePersistenceBackend(directory, [], injected);
			const failure = await backend
				.replaceJournalWithCheckpoint([new Uint8Array([5, 6])])
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			expect(failure).to.equal(renameFailure);
			await backend.close();

			const reopened = new NodePersistenceBackend(directory, []);
			const journal = (await reopened.readJournal())!;
			expect(journal).to.deep.equal(oldJournal);
			// The old journal remains replayable, but the prior base must already be
			// gone. A retry therefore counts the complete old WAL conservatively.
			expect(
				await reopened.readJournalCheckpointBase(journal.byteLength),
			).to.equal(0);
			expect(await readdir(directory)).to.not.include(
				"store.wal.replacement.tmp",
			);
			await reopened.close();
		},
	);

	itCrashSafeNode(
		"reopens the new complete Node WAL when the post-rename directory barrier fails",
		async () => {
			const directory = await tempDirectory();
			cleanup.push(directory);
			const setup = new NodePersistenceBackend(directory, []);
			await setup.replaceJournalWithCheckpoint([new Uint8Array([1, 2, 3])]);
			await setup.appendJournal(new Uint8Array([4]), "strict");
			await setup.close();

			const barrierFailure = new Error(
				"injected post-rename directory sync failure",
			);
			let replacementRenamed = false;
			const injected = new Proxy(nodeFs, {
				get(target, property, receiver) {
					if (property === "rename") {
						return async (from: string, to: string) => {
							await nodeFs.rename(from, to);
							if (to.endsWith("store.wal")) replacementRenamed = true;
						};
					}
					if (property === "open") {
						return async (path: string, flags: string) => {
							const handle = await nodeFs.open(path, flags);
							if (!replacementRenamed || path !== directory) return handle;
							return new Proxy(handle, {
								get(handleTarget, handleProperty, handleReceiver) {
									if (handleProperty === "sync") {
										return async () => {
											throw barrierFailure;
										};
									}
									const value = Reflect.get(
										handleTarget,
										handleProperty,
										handleReceiver,
									);
									return typeof value === "function"
										? value.bind(handleTarget)
										: value;
								},
							});
						};
					}
					return Reflect.get(target, property, receiver);
				},
			}) as typeof nodeFs;
			const backend = new NodePersistenceBackend(directory, [], injected);
			const replacement = new Uint8Array([4, 5, 6, 7]);
			const failure = await backend
				.replaceJournalWithCheckpoint([replacement])
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			expect(failure).to.equal(barrierFailure);
			await backend.close();

			// The checkpoint was not acknowledged, but either side of the rename is a
			// complete equivalent generation. This injected side observed the rename.
			const reopened = new NodePersistenceBackend(directory, []);
			const journal = (await reopened.readJournal())!;
			expect(journal).to.deep.equal(replacement);
			// Even though this fault observed the new WAL, it cannot observe the old
			// scheduling hint: invalidation was durably ordered before WAL rename.
			expect(
				await reopened.readJournalCheckpointBase(journal.byteLength),
			).to.equal(0);
			await reopened.close();
		},
	);

	itCrashSafeNode(
		"keeps the durable Node WAL when checkpoint-hint publication fails",
		async () => {
			const directory = await tempDirectory();
			cleanup.push(directory);
			const injected = new Proxy(nodeFs, {
				get(target, property, receiver) {
					if (property === "rename") {
						return async (from: string, to: string) => {
							if (to.endsWith("store.wal.checkpoint")) {
								throw new Error("injected checkpoint hint rename failure");
							}
							return nodeFs.rename(from, to);
						};
					}
					return Reflect.get(target, property, receiver);
				},
			}) as typeof nodeFs;
			const replacement = new Uint8Array([7, 8, 9]);
			const backend = new NodePersistenceBackend(directory, [], injected);
			expect(
				await backend.replaceJournalWithCheckpoint([replacement]),
			).to.equal(replacement.byteLength);
			await backend.close();

			const reopened = new NodePersistenceBackend(directory, []);
			const journal = (await reopened.readJournal())!;
			expect(journal).to.deep.equal(replacement);
			expect(
				await reopened.readJournalCheckpointBase(journal.byteLength),
			).to.equal(0);
			expect(await readdir(directory)).to.not.include(
				"store.wal.checkpoint.tmp",
			);
			await reopened.close();
		},
	);

	itCrashSafeNode(
		"keeps a legacy snapshot harmless when post-WAL cleanup fails",
		async () => {
			const directory = await tempDirectory();
			cleanup.push(directory);
			const checkpointDirectory = await tempDirectory();
			cleanup.push(checkpointDirectory);

			// Produce a real legacy-compatible CLEAR + live-PUT journal.
			const checkpointSource = createStore(checkpointDirectory, {
				durability: "strict",
				compactOnClose: false,
				compactMaxJournalBytes: 1,
			});
			await checkpointSource.open();
			await checkpointSource.put("kept", new Uint8Array([9]));
			await checkpointSource.close();
			const replacement = new Uint8Array(
				await readFile(join(checkpointDirectory, "store.wal")),
			);

			// The legacy snapshot contains a key that the replacement intentionally
			// omits. CLEAR must prevent that stale key from being resurrected.
			const snapshotSource = createStore(directory);
			await snapshotSource.open();
			await snapshotSource.put("victim", new Uint8Array([1]));
			await snapshotSource.put("kept", new Uint8Array([2]));
			await snapshotSource.close();
			const injected = new Proxy(nodeFs, {
				get(target, property, receiver) {
					if (property === "rm") {
						return async (
							targetPath: string,
							options?: Parameters<typeof nodeFs.rm>[1],
						) => {
							if (targetPath.endsWith("store.bin")) {
								throw new Error("injected legacy snapshot cleanup failure");
							}
							return nodeFs.rm(targetPath, options);
						};
					}
					return Reflect.get(target, property, receiver);
				},
			}) as typeof nodeFs;
			const backend = new NodePersistenceBackend(directory, [], injected);
			expect(
				await backend.replaceJournalWithCheckpoint([replacement]),
			).to.equal(replacement.byteLength);
			await backend.close();

			expect(await stat(join(directory, "store.bin"))).to.exist;
			const reopened = createStore(directory, {
				durability: "strict",
				compactOnClose: false,
			});
			await reopened.open();
			expect(await reopened.get("victim")).to.equal(undefined);
			expect(await reopened.get("kept")).to.deep.equal(new Uint8Array([9]));
			await reopened.close();
		},
	);

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

	it("rejects invalid strict journal checkpoint options", () => {
		expect(() =>
			createStore("persistent", {
				durability: "strict",
				compactMaxJournalBytes: 0,
			}),
		).to.throw("positive safe integer");
		expect(() =>
			createStore("persistent", { compactMaxJournalBytes: 1 }),
		).to.throw("requires strict durability");
		expect(() =>
			createStore(undefined, {
				durability: "strict",
				compactMaxJournalBytes: 1,
			}),
		).to.throw("requires a persistent directory");
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

	itCrashSafeNode(
		"bounds strict Node WAL history by checkpointing only the live map",
		async () => {
			const directory = await tempDirectory();
			cleanup.push(directory);

			let store = createStore(directory, {
				durability: "strict",
				compactOnClose: false,
			});
			await store.open();
			for (let index = 0; index < 200; index++) {
				await store.put(`history-${index}`, new Uint8Array([index & 0xff]));
			}
			for (let index = 0; index < 200; index++) {
				await store.del(`history-${index}`);
			}
			await store.put("kept", new Uint8Array([7, 8, 9]));
			await store.close();
			const historicalBytes = (await readFile(join(directory, "store.wal")))
				.byteLength;

			store = createStore(directory, {
				durability: "strict",
				compactOnClose: false,
				compactMaxJournalBytes: 1,
			});
			await store.open();
			await store.put("trigger", new Uint8Array([4]));
			const checkpointBytes = (await readFile(join(directory, "store.wal")))
				.byteLength;
			expect(checkpointBytes).to.be.lessThan(historicalBytes / 20);
			expect(await stat(join(directory, "store.wal.checkpoint"))).to.exist;
			expect(
				await stat(join(directory, "store.bin"))
					.then(() => true)
					.catch(() => false),
			).to.equal(false);
			await store.close();

			// Reopen without the new scheduling option: the replacement is ordinary
			// legacy journal framing, not a new storage generation or codec.
			store = createStore(directory, {
				durability: "strict",
				compactOnClose: false,
			});
			await store.open();
			expect(await store.get("history-0")).to.equal(undefined);
			expect(await store.get("history-199")).to.equal(undefined);
			expect(await store.get("kept")).to.deep.equal(new Uint8Array([7, 8, 9]));
			expect(await store.get("trigger")).to.deep.equal(new Uint8Array([4]));
			await store.close();
		},
	);

	itCrashSafeNode(
		"does not resurrect a snapshot key after strict WAL checkpointing",
		async () => {
			const directory = await tempDirectory();
			cleanup.push(directory);

			let store = createStore(directory);
			await store.open();
			await store.put("victim", new Uint8Array([1]));
			await store.put("kept", new Uint8Array([2]));
			await store.close();
			expect(await stat(join(directory, "store.bin"))).to.exist;

			store = createStore(directory, {
				durability: "strict",
				compactOnClose: false,
				compactMaxJournalBytes: 1,
			});
			await store.open();
			await store.del("victim");
			await store.close();

			store = createStore(directory, {
				durability: "strict",
				compactOnClose: false,
				compactMaxJournalBytes: 1,
			});
			await store.open();
			expect(await store.get("victim")).to.equal(undefined);
			expect(await store.get("kept")).to.deep.equal(new Uint8Array([2]));
			await store.close();
		},
	);

	itCrashSafeNode(
		"removes a stale checkpoint hint after normal snapshot compaction",
		async () => {
			const directory = await tempDirectory();
			cleanup.push(directory);

			let store = createStore(directory, {
				durability: "strict",
				compactOnClose: false,
				compactMaxJournalBytes: 1,
			});
			await store.open();
			await store.put("kept", new Uint8Array([3]));
			await store.close();
			const staleHint = await readFile(join(directory, "store.wal.checkpoint"));

			// Normal compaction deliberately does no extra sidecar I/O. The next
			// checkpoint-enabled open sees the empty WAL, rejects this stale length,
			// and removes the scheduling hint without involving it in replay.
			store = createStore(directory);
			await store.open();
			await store.close();
			expect(
				await readFile(join(directory, "store.wal.checkpoint")),
			).to.deep.equal(staleHint);

			store = createStore(directory, {
				durability: "strict",
				compactOnClose: false,
				compactMaxJournalBytes: 1,
			});
			await store.open();
			expect(await store.get("kept")).to.deep.equal(new Uint8Array([3]));
			expect(
				await stat(join(directory, "store.wal.checkpoint"))
					.then(() => true)
					.catch(() => false),
			).to.equal(false);
			await store.close();
		},
	);

	itCrashSafeNode(
		"poisons close after a checkpoint failure and replays the durable mutation",
		async () => {
			const directory = await tempDirectory();
			cleanup.push(directory);
			const store = createStore(directory, {
				durability: "strict",
				compactOnClose: false,
				compactMaxJournalBytes: 1,
			});
			await store.open();
			const internals = internalsOf(store) as StoreInternals & {
				persistence: StoreInternals["persistence"] & {
					replaceJournalWithCheckpoint(
						records: Iterable<Uint8Array>,
					): Promise<number>;
				};
			};
			const failure = new Error("injected checkpoint construction failure");
			internals.persistence.replaceJournalWithCheckpoint = async () => {
				throw failure;
			};
			expect(
				await Promise.resolve(
					store.put("ack-ambiguous", new Uint8Array([5])),
				).then(
					() => undefined,
					(error: unknown) => error,
				),
			).to.equal(failure);
			expect(
				await store.close().then(
					() => undefined,
					(error: unknown) => error,
				),
			).to.equal(failure);
			expect(store.status()).to.equal("closed");

			const reopened = createStore(directory, {
				durability: "strict",
				compactOnClose: false,
			});
			await reopened.open();
			expect(await reopened.get("ack-ambiguous")).to.deep.equal(
				new Uint8Array([5]),
			);
			await reopened.close();
		},
	);

	itCrashSafeNode(
		"grows the checkpoint allowance geometrically with an expanding live map",
		async () => {
			const directory = await tempDirectory();
			cleanup.push(directory);
			const store = createStore(directory, {
				durability: "strict",
				compactOnClose: false,
				compactMaxJournalBytes: 256,
			});
			await store.open();
			const internals = internalsOf(store) as StoreInternals & {
				persistence: StoreInternals["persistence"] & {
					replaceJournalWithCheckpoint(
						records: Iterable<Uint8Array>,
					): Promise<number>;
				};
			};
			const original = internals.persistence.replaceJournalWithCheckpoint.bind(
				internals.persistence,
			);
			let checkpointCalls = 0;
			let rewrittenBytes = 0;
			internals.persistence.replaceJournalWithCheckpoint = async (records) => {
				checkpointCalls++;
				const bytes = await original(records);
				rewrittenBytes += bytes;
				return bytes;
			};

			for (let index = 0; index < 128; index++) {
				await store.put(
					`live-${index}`,
					new Uint8Array(512).fill(index & 0xff),
				);
			}
			await store.close();

			const finalJournalBytes = (await stat(join(directory, "store.wal"))).size;
			// A fixed 256-byte history interval would rewrite the whole growing map
			// almost every mutation. The adaptive allowance instead produces a small
			// geometric series whose aggregate stays linear in final live state.
			expect(checkpointCalls).to.be.lessThan(16);
			expect(rewrittenBytes).to.be.lessThan(finalJournalBytes * 4);
		},
	);

	itCrashSafeNode(
		"serializes a checkpoint with concurrent strict mutations",
		async () => {
			const directory = await tempDirectory();
			cleanup.push(directory);
			const store = createStore(directory, {
				durability: "strict",
				compactOnClose: false,
				compactMaxJournalBytes: 1,
			});
			await store.open();
			const internals = internalsOf(store) as StoreInternals & {
				persistence: StoreInternals["persistence"] & {
					replaceJournalWithCheckpoint(
						records: Iterable<Uint8Array>,
					): Promise<number>;
				};
			};
			const original = internals.persistence.replaceJournalWithCheckpoint.bind(
				internals.persistence,
			);
			let startedResolve!: () => void;
			const started = new Promise<void>((resolve) => {
				startedResolve = resolve;
			});
			let release!: () => void;
			const gate = new Promise<void>((resolve) => {
				release = resolve;
			});
			let checkpointCalls = 0;
			internals.persistence.replaceJournalWithCheckpoint = async (records) => {
				checkpointCalls++;
				if (checkpointCalls === 1) {
					startedResolve();
					await gate;
				}
				return original(records);
			};

			const first = Promise.resolve(store.put("first", new Uint8Array([1])));
			await started;
			const second = Promise.resolve(store.put("second", new Uint8Array([2])));
			await Promise.resolve();
			expect(await store.get("first")).to.deep.equal(new Uint8Array([1]));
			expect(await store.get("second")).to.equal(undefined);
			release();
			await Promise.all([first, second]);
			expect(checkpointCalls).to.be.greaterThan(0);
			await store.close();

			const reopened = createStore(directory, {
				durability: "strict",
				compactOnClose: false,
				compactMaxJournalBytes: 1,
			});
			await reopened.open();
			expect(await reopened.get("first")).to.deep.equal(new Uint8Array([1]));
			expect(await reopened.get("second")).to.deep.equal(new Uint8Array([2]));
			await reopened.close();
		},
	);

	itCrashSafeNode(
		"reopens an acknowledged strict checkpoint after SIGKILL",
		async function () {
			this.timeout(30_000);
			const directory = await tempDirectory();
			cleanup.push(directory);
			const workerPath = join(
				process.cwd(),
				"test",
				"checkpoint-hard-kill-worker.mjs",
			);
			const child = spawn(process.execPath, [workerPath, directory], {
				stdio: ["ignore", "pipe", "pipe"],
			});
			let output = "";
			let errors = "";
			child.stdout.setEncoding("utf8");
			child.stderr.setEncoding("utf8");
			child.stdout.on("data", (chunk) => {
				output += chunk;
			});
			child.stderr.on("data", (chunk) => {
				errors += chunk;
			});
			const ack = await new Promise<void>((resolve, reject) => {
				const deadline = setTimeout(
					() => reject(new Error(`checkpoint worker timeout: ${errors}`)),
					20_000,
				);
				const inspect = () => {
					if (!output.includes("CHECKPOINT_ACK")) return;
					clearTimeout(deadline);
					resolve();
				};
				child.stdout.on("data", inspect);
				child.once("exit", (code, signal) => {
					if (output.includes("CHECKPOINT_ACK")) return;
					clearTimeout(deadline);
					reject(
						new Error(
							`checkpoint worker exited before ACK (${code}/${signal}): ${errors}`,
						),
					);
				});
			});
			await ack;
			child.kill("SIGKILL");
			await new Promise<void>((resolve) => child.once("exit", () => resolve()));
			const checkpoint = JSON.parse(
				await readFile(join(directory, "store.wal.checkpoint"), "utf8"),
			) as { byteLength: number };
			expect(checkpoint.byteLength).to.equal(
				(await stat(join(directory, "store.wal"))).size,
			);

			const reopened = createStore(directory, {
				durability: "strict",
				compactOnClose: false,
				compactMaxJournalBytes: 1,
			});
			await reopened.open();
			expect(await reopened.get("deleted")).to.equal(undefined);
			expect(await reopened.get("survivor")).to.deep.equal(
				new Uint8Array([7, 8, 9]),
			);
			await reopened.close();
		},
	);

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
