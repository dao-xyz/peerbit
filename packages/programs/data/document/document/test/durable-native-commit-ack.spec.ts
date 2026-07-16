import { deserialize, serialize } from "@dao-xyz/borsh";
import { ShallowEntry } from "@peerbit/log";
import {
	NativeBackboneNodeCoordinatePersistence,
	NativeBackboneNodeCoordinatePersistenceStore,
} from "@peerbit/native-backbone";
import {
	NativeDurableCommitError,
	StashBackedRawExchangeHeadsMessage,
} from "@peerbit/shared-log";
import { expect } from "chai";
import { spawn } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Peerbit } from "peerbit";
import { createRustPeerbitOptions } from "peerbit/rust";
import sinon from "sinon";
import { policy, transform } from "../src/index.js";
import { Documents } from "../src/program.js";
import { Document, TestStore } from "./data.js";

describe("durable native commit acknowledgement", function () {
	this.timeout(120_000);

	let client: Peerbit | undefined;
	let directory: string | undefined;

	const within = async <T>(
		promise: Promise<T>,
		step: string,
		timeoutMs = 5_000,
	): Promise<T> => {
		let timer: ReturnType<typeof setTimeout> | undefined;
		try {
			return await Promise.race([
				promise,
				new Promise<never>((_, reject) => {
					timer = setTimeout(
						() => reject(new Error(`Timed out waiting for ${step}`)),
						timeoutMs,
					);
				}),
			]);
		} finally {
			if (timer) {
				clearTimeout(timer);
			}
		}
	};

	const waitForWorkerMessage = <T>(
		child: ReturnType<typeof spawn>,
		step: string,
	): Promise<T> => {
		let stdout = "";
		let stderr = "";
		child.stderr?.on("data", (chunk) => {
			stderr += chunk.toString();
		});
		return within(
			new Promise<T>((resolve, reject) => {
				child.stdout?.on("data", (chunk) => {
					stdout += chunk.toString();
					const lines = stdout.split("\n");
					stdout = lines.pop() ?? "";
					for (const line of lines) {
						if (!line.startsWith("{")) {
							continue;
						}
						try {
							resolve(JSON.parse(line) as T);
							return;
						} catch {
							// Ignore non-protocol output and keep reading.
						}
					}
				});
				child.once("exit", (code, signal) => {
					reject(
						new Error(
							`Worker exited before ${step} (code=${String(code)}, signal=${String(signal)}): ${stderr}`,
						),
					);
				});
			}),
			step,
			30_000,
		);
	};

	afterEach(async () => {
		try {
			await client?.stop();
		} catch (error) {
			if (!(error instanceof NativeDurableCommitError)) {
				throw error;
			}
		}
		client = undefined;
		if (directory) {
			await fs.rm(directory, { recursive: true, force: true });
			directory = undefined;
		}
	});

	const createOpenArgs = (
		storeDirectory: string,
		trim = false,
		replicate: boolean = false,
		coordinatePersistence: any = new NativeBackboneNodeCoordinatePersistence(
			path.join(storeDirectory, "coordinate-wal"),
			{ flushOnAppend: true },
		),
	) => ({
		mode: "native" as const,
		replicate,
		nativeGraph: true,
		nativeBackbone: {
			optional: false,
			documentIndex: true,
			coordinatePersistence,
		},
		canPerform: policy.allowAll<Document>(),
		index: {
			type: Document,
			transform: transform.identity<Document>(),
		},
		...(trim ? { log: { trim: { type: "length" as const, to: 1 } } } : {}),
	});

	const createStore = async (
		storeDirectory: string,
		trim = false,
		replicate = false,
		coordinatePersistence?: any,
	) => {
		client = await Peerbit.create({
			directory: storeDirectory,
			...createRustPeerbitOptions(),
		});
		const storeId = new Uint8Array(32);
		for (let index = 0; index < storeId.length; index++) {
			storeId[index] = (index * 13 + 7) & 0xff;
		}
		const store = new TestStore<Document>({
			docs: new Documents<Document>({ id: storeId }),
		});
		store.id = storeId;
		await client.open(store, {
			args: createOpenArgs(
				storeDirectory,
				trim,
				replicate,
				coordinatePersistence,
			),
		});
		const sharedLog = store.docs.log as any;
		const wrapper = sharedLog.remoteBlocks.localStore as any;
		expect(wrapper).to.not.equal(sharedLog._nativeBackbone.blocks);
		return {
			store,
			sharedLog,
			wrapper,
			durable: wrapper.durable as any,
			backbone: sharedLog._nativeBackbone as any,
		};
	};

	const openStore = async (trim = false, replicate = false) => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-commit-"),
		);
		return createStore(directory, trim, replicate);
	};

	const nativePersistenceFiles = [
		"coordinates.bin",
		"coordinates.wal",
		"document-values.bin",
		"document-values.wal",
		"document-signers.bin",
		"document-signers.wal",
		"strict-durable-transaction-intent.json",
		"strict-durable-transaction-intent.backup.json",
	] as const;

	const expectFileAbsent = async (file: string) => {
		try {
			await fs.stat(file);
			expect.fail(`Expected ${file} to be absent`);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
				throw error;
			}
		}
	};

	const expectNativePersistenceErased = async (storeDirectory: string) => {
		const coordinateDirectory = path.join(storeDirectory, "coordinate-wal");
		for (const name of nativePersistenceFiles) {
			let bytes: Uint8Array | undefined;
			try {
				bytes = await fs.readFile(path.join(coordinateDirectory, name));
			} catch (error) {
				if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
					throw error;
				}
			}
			expect(bytes?.byteLength ?? 0, `${name} was not erased`).equal(0);
		}
		await expectFileAbsent(
			path.join(coordinateDirectory, "native-backbone-drop.tombstone"),
		);
	};

	const wrapCoordinatePersistence = (
		persistence: NativeBackboneNodeCoordinatePersistence,
		withIntentStore: boolean,
		withDropLifecycle = false,
	) => ({
		...(withIntentStore ? { intentStore: persistence.intentStore } : {}),
		...(withDropLifecycle
			? {
					drop: persistence.drop.bind(persistence),
					resumeDrop: persistence.resumeDrop.bind(persistence),
					supportsDrop: persistence.supportsDrop,
					dropIsTerminal: persistence.dropIsTerminal,
				}
			: {}),
		durableBarrier: persistence.durableBarrier,
		flushOnAppend: persistence.flushOnAppend,
		flushMaxPendingBytes: persistence.flushMaxPendingBytes,
		flushIntervalMs: persistence.flushIntervalMs,
		compactMaxJournalBytes: persistence.compactMaxJournalBytes,
		compactMaxJournalRecords: persistence.compactMaxJournalRecords,
		crashSafeCompaction: persistence.crashSafeCompaction,
		hydrate: persistence.hydrate.bind(persistence),
		flushJournal: persistence.flushJournal.bind(persistence),
		flushJournalOnAppend: persistence.flushJournalOnAppend.bind(persistence),
		compact: persistence.compact.bind(persistence),
		close: persistence.close.bind(persistence),
	});

	it("reopens an acknowledged native append after SIGKILL", async function () {
		if (process.platform === "win32") {
			this.skip();
		}
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-hard-kill-"),
		);
		const workerPath = path.join(
			process.cwd(),
			"test/durable-native-hard-kill-worker.mjs",
		);
		const writer = spawn(process.execPath, [workerPath, "write", directory], {
			stdio: ["ignore", "pipe", "pipe"],
		});
		const writerExit = once(writer, "exit");
		let acknowledged: { event: "ack"; hash: string } | undefined;
		try {
			acknowledged = await waitForWorkerMessage<{
				event: "ack";
				hash: string;
			}>(writer, "hard-kill write acknowledgement");
			expect(writer.kill("SIGKILL")).equal(true);
			await within(
				writerExit.then(() => undefined),
				"hard-kill writer exit",
			);
		} finally {
			if (writer.exitCode === null && writer.signalCode === null) {
				writer.kill("SIGKILL");
			}
		}
		if (!acknowledged) {
			throw new Error("Writer exited without an acknowledgement");
		}

		const reader = spawn(
			process.execPath,
			[workerPath, "read", directory, acknowledged.hash],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const readerExit = once(reader, "exit") as Promise<
			[number | null, NodeJS.Signals | null]
		>;
		try {
			const reopened = await waitForWorkerMessage<{
				event: "read";
				documentName?: string;
				entryHash?: string;
			}>(reader, "hard-kill reopen");
			expect(reopened).deep.equal({
				event: "read",
				documentName: "hard-kill",
				entryHash: acknowledged.hash,
			});
			const [exitCode] = await within(
				readerExit,
				"hard-kill reader exit",
				30_000,
			);
			expect(exitCode).equal(0);
		} finally {
			if (reader.exitCode === null && reader.signalCode === null) {
				reader.kill("SIGKILL");
			}
		}
	});

	it("fails closed when the only native intent slot is corrupt", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-corrupt-intent-"),
		);
		const journalDirectory = path.join(directory, "coordinate-wal");
		await fs.mkdir(journalDirectory, { recursive: true });
		await fs.writeFile(
			path.join(journalDirectory, "strict-durable-transaction-intent.json"),
			'{"version":1,"lowerMarkerCommitted":',
		);

		let openError: unknown;
		try {
			await createStore(directory);
		} catch (error) {
			openError = error;
		}
		expect(openError).to.be.instanceOf(Error);
		expect(String(openError)).to.contain(
			"No valid native durable transaction journal generation remains",
		);
	});

	it("rejects an opaque coordinate adapter on durable strict-native open", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-opaque-adapter-"),
		);
		const persistence = new NativeBackboneNodeCoordinatePersistence(
			path.join(directory, "coordinate-wal"),
			{ flushOnAppend: true },
		);
		let openError: unknown;
		try {
			await createStore(
				directory,
				false,
				false,
				wrapCoordinatePersistence(persistence, false),
			);
		} catch (error) {
			openError = error;
		}
		expect(openError).to.be.instanceOf(Error);
		expect(String(openError)).to.contain("must expose intentStore");
	});

	it("rejects durable coordinate persistence without a physical barrier", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-no-barrier-"),
		);
		const persistence = new NativeBackboneNodeCoordinatePersistence(
			path.join(directory, "coordinate-wal"),
			{ flushOnAppend: true },
		);
		const adapter = wrapCoordinatePersistence(persistence, true);
		adapter.durableBarrier = false;
		let openError: unknown;
		await createStore(directory, false, false, adapter).catch((error) => {
			openError = error;
		});
		expect(String(openError)).to.contain("physical durability barrier");
	});

	it("rejects a buffered custom store without a durability capability", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-buffered-no-barrier-"),
		);
		const files = new Map<string, Uint8Array>();
		const store = {
			read: async (name: string) => files.get(name),
			write: async (name: string, bytes: Uint8Array) => {
				files.set(name, bytes.slice());
			},
			append: async (name: string, bytes: Uint8Array) => {
				const previous = files.get(name) ?? new Uint8Array();
				const next = new Uint8Array(previous.byteLength + bytes.byteLength);
				next.set(previous);
				next.set(bytes, previous.byteLength);
				files.set(name, next);
			},
			remove: async (name: string) => {
				files.delete(name);
			},
		};
		let openError: unknown;
		await createStore(directory, false, false, {
			store,
			buffered: true,
			flushOnAppend: true,
		}).catch((error) => {
			openError = error;
		});
		expect(String(openError)).to.contain("physical durability barrier");
	});

	it("rejects durable custom compaction without a crash-safe capability", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-unsafe-compaction-"),
		);
		const persistence = new NativeBackboneNodeCoordinatePersistence(
			path.join(directory, "coordinate-wal"),
			{ flushOnAppend: true },
		);
		const adapter = {
			...wrapCoordinatePersistence(persistence, true),
			compactMaxJournalRecords: 1,
			crashSafeCompaction: false,
		};
		let openError: unknown;
		await createStore(directory, false, false, adapter).catch((error) => {
			openError = error;
		});
		expect(String(openError)).to.contain(
			"compaction thresholds require crashSafeCompaction",
		);
	});

	it("accepts a capable custom adapter and reopens its acknowledged append", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-capable-adapter-"),
		);
		const coordinateDirectory = path.join(directory, "coordinate-wal");
		const firstPersistence = new NativeBackboneNodeCoordinatePersistence(
			coordinateDirectory,
			{ flushOnAppend: true },
		);
		const first = await createStore(
			directory,
			false,
			false,
			wrapCoordinatePersistence(firstPersistence, true),
		);
		const acknowledged = await first.store.docs.put(
			new Document({ id: "capable-adapter", name: "acknowledged" }),
			{ unique: true, replicate: false, target: "none" },
		);
		await client!.stop();
		client = undefined;

		const reopenedPersistence = new NativeBackboneNodeCoordinatePersistence(
			coordinateDirectory,
			{ flushOnAppend: true },
		);
		const reopened = await createStore(
			directory,
			false,
			false,
			wrapCoordinatePersistence(reopenedPersistence, true),
		);
		expect((await reopened.store.docs.get("capable-adapter"))?.name).equal(
			"acknowledged",
		);
		expect(
			await reopened.sharedLog.log.entryIndex.has(acknowledged.entry.hash),
		).equal(true);
	});

	it("keeps opaque coordinate adapters compatible for memory-only nodes", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-memory-native-opaque-adapter-"),
		);
		client = await Peerbit.create(createRustPeerbitOptions());
		const storeId = new Uint8Array(32).fill(41);
		const store = new TestStore<Document>({
			docs: new Documents<Document>({ id: storeId }),
		});
		store.id = storeId;
		const persistence = new NativeBackboneNodeCoordinatePersistence(
			path.join(directory, "coordinate-wal"),
			{ flushOnAppend: true },
		);
		await client.open(store, {
			args: createOpenArgs(
				directory,
				false,
				false,
				wrapCoordinatePersistence(persistence, false),
			),
		});
		await store.docs.put(
			new Document({ id: "memory-opaque", name: "compatible" }),
			{ unique: true, replicate: false, target: "none" },
		);
		expect((await store.docs.get("memory-opaque"))?.name).equal("compatible");
		let dropError: unknown;
		try {
			await store.docs.log.drop();
		} catch (error) {
			dropError = error;
		}
		expect(String(dropError)).to.contain("terminal underlying drop capability");
		expect((await store.docs.get("memory-opaque"))?.name).equal("compatible");
	});

	it("rejects unsafe custom compaction thresholds on a memory-only node", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-memory-native-unsafe-compaction-"),
		);
		client = await Peerbit.create(createRustPeerbitOptions());
		const storeId = new Uint8Array(32).fill(43);
		const store = new TestStore<Document>({
			docs: new Documents<Document>({ id: storeId }),
		});
		store.id = storeId;
		const persistence = new NativeBackboneNodeCoordinatePersistence(
			path.join(directory, "coordinate-wal"),
			{ flushOnAppend: true },
		);
		const adapter = {
			...wrapCoordinatePersistence(persistence, false),
			compactMaxJournalRecords: 1,
			crashSafeCompaction: false,
		};
		let openError: unknown;
		await client
			.open(store, {
				args: createOpenArgs(directory, false, false, adapter),
			})
			.catch((error) => {
				openError = error;
			});
		expect(String(openError)).to.contain(
			"compaction thresholds require crashSafeCompaction",
		);
	});

	it("drops a persistent custom adapter on a memory-only node", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-memory-native-drop-adapter-"),
		);
		const coordinateDirectory = path.join(directory, "coordinate-wal");
		const storeId = new Uint8Array(32).fill(42);
		client = await Peerbit.create(createRustPeerbitOptions());
		const firstStore = new TestStore<Document>({
			docs: new Documents<Document>({ id: storeId }),
		});
		firstStore.id = storeId;
		const firstPersistence = new NativeBackboneNodeCoordinatePersistence(
			coordinateDirectory,
			{ flushOnAppend: true },
		);
		await client.open(firstStore, {
			args: createOpenArgs(
				directory,
				false,
				false,
				wrapCoordinatePersistence(firstPersistence, false, true),
			),
		});
		await firstStore.docs.put(
			new Document({ id: "memory-drop", name: "erase-me" }),
			{ unique: true, replicate: false, target: "none" },
		);

		await firstStore.drop();
		await client.stop();
		client = undefined;
		await expectNativePersistenceErased(directory);

		client = await Peerbit.create(createRustPeerbitOptions());
		const reopenedStore = new TestStore<Document>({
			docs: new Documents<Document>({ id: storeId }),
		});
		reopenedStore.id = storeId;
		const reopenedPersistence = new NativeBackboneNodeCoordinatePersistence(
			coordinateDirectory,
			{ flushOnAppend: true },
		);
		await client.open(reopenedStore, {
			args: createOpenArgs(
				directory,
				false,
				false,
				wrapCoordinatePersistence(reopenedPersistence, false, true),
			),
		});
		expect(await reopenedStore.docs.get("memory-drop")).equal(undefined);
		expect((reopenedStore.docs.log as any).log.length).equal(0);
		await expectNativePersistenceErased(directory);
	});

	it("durably drops native rows, heads, documents, blocks, and all persistence files", async () => {
		const first = await openStore();
		const acknowledged = await first.store.docs.put(
			new Document({ id: "durable-drop", name: "erase-me" }),
			{ unique: true, replicate: false, target: "none" },
		);
		expect(await first.store.docs.get("durable-drop")).to.not.equal(undefined);
		expect(await first.durable.has(acknowledged.entry.hash)).equal(true);

		await first.store.drop();
		await client!.stop();
		client = undefined;
		await expectNativePersistenceErased(directory!);

		const reopened = await createStore(directory!);
		expect(await reopened.store.docs.get("durable-drop")).equal(undefined);
		expect(reopened.sharedLog.log.length).equal(0);
		expect(
			await reopened.sharedLog.log.entryIndex.has(acknowledged.entry.hash),
		).equal(false);
		expect(await reopened.sharedLog.log.getHeads().all()).deep.equal([]);
		expect(reopened.backbone.blocks.get(acknowledged.entry.hash)).equal(
			undefined,
		);
		expect(await reopened.durable.has(acknowledged.entry.hash)).equal(false);
		await expectNativePersistenceErased(directory!);
	});

	for (const lowerMarkerCommitted of [true, false]) {
		it(`drops and does not replay a retained ${
			lowerMarkerCommitted ? "committed" : "uncommitted"
		} native intent`, async () => {
			const first = await openStore();
			const id = lowerMarkerCommitted
				? "retained-committed-drop"
				: "retained-uncommitted-drop";
			const acknowledged = await first.store.docs.put(
				new Document({ id, name: "erase-retained-intent" }),
				{ unique: true, replicate: false, target: "none" },
			);
			await first.sharedLog.writeNativeStrictDurableTransactionIntent({
				version: 1,
				lowerMarkerCommitted,
				appendHashes: [acknowledged.entry.hash],
				trimHashes: [],
				coordinateDeleteHashes: [],
				lowerIndexRows: [],
				coordinates: [],
				documents: [
					{
						key: id,
						byteElementIndexLimit: 0,
					},
				],
			});
			expect(
				first.sharedLog._nativeStrictDurableTransactionJournalState.intent
					.lowerMarkerCommitted,
			).equal(lowerMarkerCommitted);

			await first.store.drop();
			await client!.stop();
			client = undefined;
			await expectNativePersistenceErased(directory!);

			const reopened = await createStore(directory!);
			expect(await reopened.store.docs.get(id)).equal(undefined);
			expect(reopened.sharedLog.log.length).equal(0);
			expect(
				await reopened.sharedLog.log.entryIndex.has(acknowledged.entry.hash),
			).equal(false);
			expect(
				reopened.sharedLog._nativeStrictDurableTransactionJournalState.intent,
			).equal(undefined);
		});
	}

	it("resumes a tombstoned native drop after an injected removal failure", async () => {
		const first = await openStore();
		const acknowledged = await first.store.docs.put(
			new Document({ id: "interrupted-drop", name: "erase-on-resume" }),
			{ unique: true, replicate: false, target: "none" },
		);
		const acknowledgedHash = acknowledged.entry.hash;
		const persistenceStore = first.sharedLog
			._nativeBackboneCoordinatePersistenceStore as {
			remove: (name: string) => Promise<void>;
		};
		const originalRemove = persistenceStore.remove.bind(persistenceStore);
		const removalFailure = new Error(
			"injected native namespace removal failure",
		);
		let injected = false;
		persistenceStore.remove = async (name) => {
			if (name === "document-values.wal" && !injected) {
				injected = true;
				throw removalFailure;
			}
			await originalRemove(name);
		};
		let dropError: unknown;
		try {
			await first.store.drop();
		} catch (error) {
			dropError = error;
		} finally {
			persistenceStore.remove = originalRemove;
		}
		expect(injected).equal(true);
		expect(dropError).to.be.instanceOf(AggregateError);
		await fs.stat(
			path.join(directory!, "coordinate-wal", "native-backbone-drop.tombstone"),
		);

		await client!.stop();
		client = undefined;
		const reopened = await createStore(directory!);
		expect(await reopened.store.docs.get("interrupted-drop")).equal(undefined);
		expect(reopened.sharedLog.log.length).equal(0);
		expect(await reopened.sharedLog.log.entryIndex.has(acknowledgedHash)).equal(
			false,
		);
		expect(reopened.backbone.blocks.get(acknowledgedHash)).equal(undefined);
		expect(await reopened.durable.has(acknowledgedHash)).equal(false);
		expect(reopened.backbone.documentValueBytes("interrupted-drop")).equal(
			undefined,
		);
		await expectNativePersistenceErased(directory!);
	});

	it("restarts native drop when failure precedes the durable tombstone", async () => {
		const first = await openStore();
		const acknowledged = await first.store.docs.put(
			new Document({ id: "pre-tombstone-drop", name: "erase-on-retry" }),
			{ unique: true, replicate: false, target: "none" },
		);
		const acknowledgedHash = acknowledged.entry.hash;
		const persistenceStore = first.sharedLog
			._nativeBackboneCoordinatePersistenceStore as {
			write: (name: string, bytes: Uint8Array) => Promise<void>;
		};
		const originalWrite = persistenceStore.write.bind(persistenceStore);
		const tombstoneFailure = new Error("injected drop tombstone write failure");
		let injected = false;
		persistenceStore.write = async (name, bytes) => {
			if (name === "native-backbone-drop.tombstone" && !injected) {
				injected = true;
				throw tombstoneFailure;
			}
			await originalWrite(name, bytes);
		};
		const dropError = await first.store.drop().then(
			() => undefined,
			(error: unknown) => error,
		);
		persistenceStore.write = originalWrite;
		expect(injected).equal(true);
		expect(dropError).equal(tombstoneFailure);
		await expectFileAbsent(
			path.join(directory!, "coordinate-wal", "native-backbone-drop.tombstone"),
		);

		await client!.stop();
		client = undefined;
		const reopened = await createStore(directory!);
		expect(await reopened.store.docs.get("pre-tombstone-drop")).equal(
			undefined,
		);
		expect(reopened.sharedLog.log.length).equal(0);
		expect(await reopened.sharedLog.log.entryIndex.has(acknowledgedHash)).equal(
			false,
		);
		expect(reopened.backbone.blocks.get(acknowledgedHash)).equal(undefined);
		expect(await reopened.durable.has(acknowledgedHash)).equal(false);
		expect(reopened.backbone.documentValueBytes("pre-tombstone-drop")).equal(
			undefined,
		);
		await expectNativePersistenceErased(directory!);
	});

	it("overwrites a partial native drop tombstone on exact retry", async () => {
		const first = await openStore();
		const acknowledged = await first.store.docs.put(
			new Document({ id: "partial-tombstone-drop", name: "erase-on-retry" }),
			{ unique: true, replicate: false, target: "none" },
		);
		const acknowledgedHash = acknowledged.entry.hash;
		const persistenceStore = first.sharedLog
			._nativeBackboneCoordinatePersistenceStore as {
			write: (name: string, bytes: Uint8Array) => Promise<void>;
		};
		const originalWrite = persistenceStore.write.bind(persistenceStore);
		const partialWriteFailure = new Error(
			"injected partial drop tombstone write failure",
		);
		let injected = false;
		persistenceStore.write = async (name, bytes) => {
			if (name === "native-backbone-drop.tombstone" && !injected) {
				injected = true;
				await originalWrite(name, bytes.subarray(0, 1));
				throw partialWriteFailure;
			}
			await originalWrite(name, bytes);
		};
		const dropError = await first.store.drop().then(
			() => undefined,
			(error: unknown) => error,
		);
		persistenceStore.write = originalWrite;
		expect(injected).equal(true);
		expect(dropError).equal(partialWriteFailure);
		await fs.stat(
			path.join(directory!, "coordinate-wal", "native-backbone-drop.tombstone"),
		);

		await client!.stop();
		client = undefined;
		const reopened = await createStore(directory!);
		expect(await reopened.store.docs.get("partial-tombstone-drop")).equal(
			undefined,
		);
		expect(reopened.sharedLog.log.length).equal(0);
		expect(await reopened.sharedLog.log.entryIndex.has(acknowledgedHash)).equal(
			false,
		);
		expect(reopened.backbone.blocks.get(acknowledgedHash)).equal(undefined);
		expect(await reopened.durable.has(acknowledgedHash)).equal(false);
		expect(
			reopened.backbone.documentValueBytes("partial-tombstone-drop"),
		).equal(undefined);
		await expectNativePersistenceErased(directory!);
	});

	for (const target of [
		"entry coordinates index",
		"replication range index",
		"log",
		"post-drop close",
	] as const) {
		it(`retains failed ${target} cleanup for the exact terminal retry`, async () => {
			const first = await openStore();
			const id = `lower-cleanup-${target.replaceAll(" ", "-")}`;
			const acknowledged = await first.store.docs.put(
				new Document({ id, name: "erase-on-retry" }),
				{ unique: true, replicate: false, target: "none" },
			);
			const acknowledgedHash = acknowledged.entry.hash;
			const resource: {
				drop?: () => Promise<void>;
				stop?: () => Promise<void>;
			} =
				target === "entry coordinates index"
					? first.sharedLog._entryCoordinatesIndex
					: target === "replication range index"
						? first.sharedLog._replicationRangeIndex
						: target === "log"
							? first.sharedLog.log
							: first.sharedLog.remoteBlocks;
			const method = target === "post-drop close" ? "stop" : "drop";
			const original = resource[method]!.bind(resource);
			const cleanupFailure = new Error(`injected ${target} cleanup failure`);
			let attempts = 0;
			resource[method] = async () => {
				attempts++;
				if (attempts === 1) {
					throw cleanupFailure;
				}
				await original();
			};

			const dropError = await first.store.drop().then(
				() => undefined,
				(error: unknown) => error,
			);
			expect(dropError).equal(cleanupFailure);
			if (target === "entry coordinates index") {
				expect(first.sharedLog._entryCoordinatesIndex).equal(resource);
			}
			if (target === "replication range index") {
				expect(first.sharedLog._replicationRangeIndex).equal(resource);
			}

			await client!.stop();
			client = undefined;
			expect(attempts).equal(2);

			const reopened = await createStore(directory!);
			expect(await reopened.store.docs.get(id)).equal(undefined);
			expect(reopened.sharedLog.log.length).equal(0);
			expect(
				await reopened.sharedLog.log.entryIndex.has(acknowledgedHash),
			).equal(false);
			expect(reopened.backbone.blocks.get(acknowledgedHash)).equal(undefined);
			expect(await reopened.durable.has(acknowledgedHash)).equal(false);
			await expectNativePersistenceErased(directory!);
		});
	}

	it("rejects a durable custom adapter without drop before lower mutation", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-no-drop-adapter-"),
		);
		const persistence = new NativeBackboneNodeCoordinatePersistence(
			path.join(directory, "coordinate-wal"),
			{ flushOnAppend: true },
		);
		const first = await createStore(
			directory,
			false,
			false,
			wrapCoordinatePersistence(persistence, true, false),
		);
		const acknowledged = await first.store.docs.put(
			new Document({ id: "no-drop-capability", name: "must-remain" }),
			{ unique: true, replicate: false, target: "none" },
		);
		let dropError: unknown;
		try {
			await first.sharedLog.drop();
		} catch (error) {
			dropError = error;
		}
		expect(String(dropError)).to.contain("terminal underlying drop capability");
		expect(first.sharedLog.closed).equal(false);
		expect(first.sharedLog.log.length).equal(1);
		expect(
			await first.sharedLog.log.entryIndex.has(acknowledged.entry.hash),
		).equal(true);
		expect((await first.store.docs.get("no-drop-capability"))?.name).equal(
			"must-remain",
		);
	});

	it("rejects a wrapper whose underlying store can not remove before lower mutation", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-store-no-remove-"),
		);
		const inner = new NativeBackboneNodeCoordinatePersistenceStore(
			path.join(directory, "coordinate-wal"),
		);
		const storeWithoutRemove = {
			read: inner.read.bind(inner),
			write: inner.write.bind(inner),
			append: inner.append.bind(inner),
			durableBarrier: inner.durableBarrier!.bind(inner),
			close: inner.close.bind(inner),
		};
		const first = await createStore(directory, false, false, {
			store: storeWithoutRemove,
			buffered: true,
			flushOnAppend: true,
		});
		const acknowledged = await first.store.docs.put(
			new Document({ id: "no-store-remove", name: "must-remain" }),
			{ unique: true, replicate: false, target: "none" },
		);
		const dropError = await first.sharedLog.drop().then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(String(dropError)).to.contain("terminal underlying drop capability");
		expect(first.sharedLog.closed).equal(false);
		expect(first.sharedLog.log.length).equal(1);
		expect(
			await first.sharedLog.log.entryIndex.has(acknowledged.entry.hash),
		).equal(true);
		expect((await first.store.docs.get("no-store-remove"))?.name).equal(
			"must-remain",
		);
	});

	it("does not call an ordinary custom close after terminal drop", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-terminal-drop-"),
		);
		const coordinateDirectory = path.join(directory, "coordinate-wal");
		const persistence = new NativeBackboneNodeCoordinatePersistence(
			coordinateDirectory,
			{ flushOnAppend: true },
		);
		let closeCalls = 0;
		const adapter = {
			...wrapCoordinatePersistence(persistence, true, true),
			close: async () => {
				closeCalls++;
				await fs.mkdir(coordinateDirectory, { recursive: true });
				await fs.writeFile(
					path.join(coordinateDirectory, "coordinates.wal"),
					new Uint8Array([1, 2, 3]),
				);
			},
		};
		const first = await createStore(directory, false, false, adapter);
		await first.store.docs.put(
			new Document({ id: "terminal-drop", name: "erase-me" }),
			{ unique: true, replicate: false, target: "none" },
		);

		await first.store.drop();
		expect(closeCalls).equal(0);
		await expectNativePersistenceErased(directory);
	});

	it("fully compensates a direct native commit when the operation intent write fails", async () => {
		const { store, sharedLog, durable, backbone } = await openStore(true);
		const forceDirectFallback = sinon
			.stub(sharedLog, "canUseNativeBackboneResidentCoordinateState")
			.returns(false);
		const intentStore = sharedLog._nativeBackboneCoordinatePersistenceStore as {
			write: (name: string, bytes: Uint8Array) => Promise<void>;
		};
		const originalWrite = intentStore.write.bind(intentStore);
		let intentWrites = 0;
		const intentFailure = new Error("injected operation-intent write failure");
		intentStore.write = async (name, bytes) => {
			let record: any;
			try {
				record = JSON.parse(new TextDecoder().decode(bytes));
			} catch {}
			if (record?.state === "intent" && ++intentWrites === 2) {
				throw intentFailure;
			}
			await originalWrite(name, bytes);
		};

		const originalPrepare = backbone.graph.prepareEntryV0PlainEntryCommit.bind(
			backbone.graph,
		);
		let failedHash: string | undefined;
		const prepareStub = sinon
			.stub(backbone.graph, "prepareEntryV0PlainEntryCommit")
			.callsFake((...args: any[]) => {
				const prepared = originalPrepare(...args);
				failedHash = prepared?.cid ?? prepared?.hash;
				return prepared;
			});
		let appendError: unknown;
		try {
			await store.docs.put(
				new Document({ id: "intent-failure", name: "must-rollback" }),
				{ unique: true, replicate: false, target: "none" },
			);
		} catch (error) {
			appendError = error;
		} finally {
			intentStore.write = originalWrite;
			prepareStub.restore();
			forceDirectFallback.restore();
		}
		expect(appendError).equal(intentFailure);
		expect(intentWrites).equal(2);
		expect(failedHash).to.be.a("string").and.not.empty;
		expect(await store.docs.get("intent-failure")).equal(undefined);
		expect(sharedLog.log.length).equal(0);
		expect(await sharedLog.log.entryIndex.has(failedHash!)).equal(false);
		expect(backbone.graph.hasMany([failedHash!]).has(failedHash!)).equal(false);
		expect(backbone.blocks.get(failedHash!)).equal(undefined);
		expect(await durable.has(failedHash!)).equal(false);
		expect(
			sharedLog._residentEntryCoordinatesByHash?.has(failedHash!) ?? false,
		).equal(false);
		expect(sharedLog._nativeStrictDurableTransactionJournalState.intent).equal(
			undefined,
		);

		const later = await store.docs.put(
			new Document({ id: "after-intent-failure", name: "survives" }),
			{ unique: true, replicate: false, target: "none" },
		);
		await client!.stop();
		client = undefined;
		const reopened = await createStore(directory!, true);
		expect(await reopened.store.docs.get("intent-failure")).equal(undefined);
		expect((await reopened.store.docs.get("after-intent-failure"))?.name).equal(
			"survives",
		);
		expect(await reopened.sharedLog.log.entryIndex.has(later.entry.hash)).equal(
			true,
		);
	});

	it("recovers the last valid intent generation after SIGKILL during a marker write", async function () {
		if (process.platform === "win32") this.skip();
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-intent-torn-kill-"),
		);
		const workerPath = path.join(
			process.cwd(),
			"test/durable-native-hard-kill-worker.mjs",
		);
		const writer = spawn(
			process.execPath,
			[workerPath, "intent-marker-write", directory],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const writerExit = once(writer, "exit");
		let interrupted:
			| { event: "intent-marker-partial"; hash: string }
			| undefined;
		try {
			interrupted = await waitForWorkerMessage(
				writer,
				"partial durable intent marker",
			);
			expect(writer.kill("SIGKILL")).equal(true);
			await within(
				writerExit.then(() => undefined),
				"partial intent writer exit",
			);
		} finally {
			if (writer.exitCode === null && writer.signalCode === null) {
				writer.kill("SIGKILL");
			}
		}
		if (!interrupted) throw new Error("Writer did not tear the marker intent");

		const reader = spawn(
			process.execPath,
			[workerPath, "read", directory, interrupted.hash],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const readerExit = once(reader, "exit") as Promise<
			[number | null, NodeJS.Signals | null]
		>;
		try {
			const reopened = await waitForWorkerMessage<{
				event: "read";
				documentName?: string;
				entryHash?: string;
			}>(reader, "partial intent recovery");
			expect(reopened).deep.equal({
				event: "read",
				documentName: "hard-kill",
				entryHash: interrupted.hash,
			});
			expect((await within(readerExit, "partial intent reader exit"))[0]).equal(
				0,
			);
		} finally {
			if (reader.exitCode === null && reader.signalCode === null) {
				reader.kill("SIGKILL");
			}
		}
	});

	it("preserves a later generic receive when recovering a torn strict marker after SIGKILL", async function () {
		if (process.platform === "win32") this.skip();
		directory = await fs.mkdtemp(
			path.join(
				os.tmpdir(),
				"peerbit-durable-native-strict-generic-torn-kill-",
			),
		);
		const workerPath = path.join(
			process.cwd(),
			"test/durable-native-hard-kill-worker.mjs",
		);
		const writer = spawn(
			process.execPath,
			[workerPath, "strict-generic-torn-marker-write", directory],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const writerExit = once(writer, "exit");
		let raced:
			| {
					event: "strict-generic-torn-marker";
					strictHash: string;
					genericHash: string;
					strictIndexed: boolean;
					genericIndexed: boolean;
					strictHead: boolean;
					genericHead: boolean;
					headHashes: string[];
			  }
			| undefined;
		try {
			const writerResult = await waitForWorkerMessage<
				NonNullable<typeof raced>
			>(writer, "strict/generic torn-marker race");
			raced = writerResult;
			expect(writerResult.strictIndexed).equal(true);
			expect(writerResult.genericIndexed).equal(true);
			expect(writerResult.strictHead).equal(false);
			expect(writerResult.genericHead).equal(true);
			expect(writerResult.headHashes).deep.equal([writerResult.genericHash]);
			expect(writer.kill("SIGKILL")).equal(true);
			await within(
				writerExit.then(() => undefined),
				"strict/generic torn-marker writer exit",
			);
		} finally {
			if (writer.exitCode === null && writer.signalCode === null) {
				writer.kill("SIGKILL");
			}
		}
		if (!raced) throw new Error("Writer did not complete the receive race");

		const reader = spawn(
			process.execPath,
			[
				workerPath,
				"strict-generic-torn-marker-read",
				directory,
				raced.strictHash,
				raced.genericHash,
			],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const readerExit = once(reader, "exit") as Promise<
			[number | null, NodeJS.Signals | null]
		>;
		try {
			const reopened = await waitForWorkerMessage<{
				event: "strict-generic-torn-marker-read";
				lowerLogLength: number;
				strictIndexed: boolean;
				genericIndexed: boolean;
				strictHead: boolean;
				genericHead: boolean;
				headHashes: string[];
			}>(reader, "strict/generic torn-marker recovery");
			expect(reopened).deep.equal({
				event: "strict-generic-torn-marker-read",
				lowerLogLength: 2,
				strictIndexed: true,
				genericIndexed: true,
				strictHead: false,
				genericHead: true,
				headHashes: [raced.genericHash],
			});
			expect(
				(await within(readerExit, "strict/generic torn-marker reader exit"))[0],
			).equal(0);
		} finally {
			if (reader.exitCode === null && reader.signalCode === null) {
				reader.kill("SIGKILL");
			}
		}
	});

	it("does not infer a torn marker from pre-existing append presence", async () => {
		const first = await openStore();
		const existing = await first.store.docs.put(
			new Document({ id: "existing-marker-row", name: "existing" }),
			{ replicate: false, target: "none" },
		);
		const indexed = await first.sharedLog.log.entryIndex.getShallow(
			existing.entry.hash,
		);
		expect(indexed).to.not.equal(undefined);
		const before = [...serialize(indexed!.value)];
		const afterRow = deserialize(Uint8Array.from(before), ShallowEntry);
		afterRow.head = !afterRow.head;
		const journal = first.sharedLog
			._nativeStrictDurableTransactionJournalState as {
			intent?: unknown;
		};
		journal.intent = {
			version: 1,
			lowerMarkerCommitted: false,
			appendHashes: [existing.entry.hash],
			trimHashes: [],
			coordinateDeleteHashes: [],
			lowerIndexRows: [
				{
					hash: existing.entry.hash,
					before,
					after: [...serialize(afterRow)],
				},
			],
			coordinates: [],
			documents: [
				{
					key: "existing-marker-rollback-probe",
					byteElementIndexLimit: 0,
				},
			],
		};
		const restoreDocument = sinon.spy(
			first.sharedLog,
			"restoreNativeBackboneDocument",
		);
		try {
			await first.sharedLog.recoverNativeStrictDurableTransactionIntent();
		} finally {
			restoreDocument.restore();
		}
		expect(restoreDocument.calledOnce).equal(true);
		expect(restoreDocument.firstCall.args[0].key).equal(
			"existing-marker-rollback-probe",
		);
		expect(await first.sharedLog.log.entryIndex.has(existing.entry.hash)).equal(
			true,
		);
		expect(
			first.sharedLog._nativeStrictDurableTransactionJournalState.intent,
		).equal(undefined);

		const legacyJournal = first.sharedLog
			._nativeStrictDurableTransactionJournalState as {
			intent?: unknown;
		};
		legacyJournal.intent = {
			version: 1,
			lowerMarkerCommitted: false,
			appendHashes: [existing.entry.hash],
			trimHashes: [],
			coordinateDeleteHashes: [],
			lowerIndexRows: [],
			coordinates: [],
			documents: [
				{
					key: "legacy-existing-marker-rollback-probe",
					byteElementIndexLimit: 0,
				},
			],
		};
		const restoreLegacyDocument = sinon.spy(
			first.sharedLog,
			"restoreNativeBackboneDocument",
		);
		try {
			await first.sharedLog.recoverNativeStrictDurableTransactionIntent();
		} finally {
			restoreLegacyDocument.restore();
		}
		expect(restoreLegacyDocument.calledOnce).equal(true);
		expect(restoreLegacyDocument.firstCall.args[0].key).equal(
			"legacy-existing-marker-rollback-probe",
		);
		expect(await first.sharedLog.log.entryIndex.has(existing.entry.hash)).equal(
			true,
		);
	});

	it("serializes a generic receive batch behind an in-process strict rollback", async () => {
		const first = await openStore();
		const entryIndex = first.sharedLog.log.entryIndex;
		const lowerIndex = entryIndex.properties.index;
		const originalPut = lowerIndex.put.bind(lowerIndex);
		const indexFailure = new Error("injected strict lower-index failure");
		let sourceClient: Peerbit | undefined;
		let genericMutation: Promise<unknown> | undefined;
		let strictHash: string | undefined;
		let genericHash: string | undefined;
		let strictBytes: Uint8Array | undefined;
		let genericBytes: Uint8Array | undefined;
		let injected = false;
		lowerIndex.put = async (value: ShallowEntry) => {
			await originalPut(value);
			if (injected) return;
			injected = true;
			lowerIndex.put = originalPut;
			strictHash = value.hash;
			const strictEntry = await first.sharedLog.log.get(strictHash);
			strictBytes = await first.sharedLog.log.blocks.get(strictHash);
			if (!strictEntry || !strictBytes) {
				throw new Error("Expected strict entry facts before lower rollback");
			}

			sourceClient = await Peerbit.create(createRustPeerbitOptions());
			const storeId = new Uint8Array(32);
			for (let index = 0; index < storeId.length; index++) {
				storeId[index] = (index * 13 + 7) & 0xff;
			}
			const sourceStore = new TestStore<Document>({
				docs: new Documents<Document>({ id: storeId }),
			});
			sourceStore.id = storeId;
			await sourceClient.open(sourceStore, {
				args: {
					replicate: false,
					canPerform: policy.allowAll<Document>(),
					index: {
						type: Document,
						transform: transform.identity<Document>(),
					},
				},
			});
			await sourceStore.docs.log.log.blocks.putKnown!(strictHash, strictBytes);
			await sourceStore.docs.log.log.join([strictEntry]);
			const generic = await sourceStore.docs.put(
				new Document({ id: "rollback-generic-y", name: "generic" }),
				{
					replicate: false,
					target: "none",
					meta: { next: [strictEntry] },
				},
			);
			genericHash = generic.entry.hash;
			genericBytes = await sourceStore.docs.log.log.blocks.get(genericHash);
			if (!genericBytes) throw new Error("Expected generic receive bytes");

			const queuedGenericMutation = entryIndex
				.putAppendBatch([strictEntry, generic.entry], {
					unique: false,
					heads: [false, true],
				})
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			genericMutation = queuedGenericMutation;
			let genericSettled = false;
			void queuedGenericMutation.then(() => {
				genericSettled = true;
			});
			await new Promise((resolve) => setTimeout(resolve, 25));
			expect(genericSettled).equal(false);
			throw indexFailure;
		};

		try {
			const strictError = await first.store.docs
				.put(new Document({ id: "rollback-strict-x", name: "must-rollback" }), {
					replicate: false,
					target: "none",
				})
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			expect(strictError).to.be.instanceOf(Error);
			expect(injected).equal(true);
			expect(genericMutation).to.not.equal(undefined);
			expect(await genericMutation!).equal(undefined);
			if (!strictHash || !genericHash || !strictBytes || !genericBytes) {
				throw new Error("Missing strict/generic rollback race facts");
			}
			await first.sharedLog.log.blocks.putKnown(strictHash, strictBytes);
			await first.sharedLog.log.blocks.putKnown(genericHash, genericBytes);
			const strictRow = await entryIndex.getShallow(strictHash);
			const genericRow = await entryIndex.getShallow(genericHash);
			const heads = await first.sharedLog.log.getHeads().all();
			expect(await entryIndex.has(strictHash)).equal(true);
			expect(await entryIndex.has(genericHash)).equal(true);
			expect(strictRow?.value.head).equal(false);
			expect(genericRow?.value.head).equal(true);
			expect(heads.map((entry: { hash: string }) => entry.hash)).deep.equal([
				genericHash,
			]);
			expect(first.sharedLog.log.length).equal(2);
			expect(
				first.sharedLog._nativeStrictDurableTransactionJournalState.intent,
			).equal(undefined);
		} finally {
			lowerIndex.put = originalPut;
			await sourceClient?.stop();
		}
	});

	it("keeps an acknowledged marker authoritative and poisons mutations when intent retirement fails", async () => {
		const first = await openStore();
		const intentStore = first.sharedLog
			._nativeBackboneCoordinatePersistenceStore as {
			write: (name: string, bytes: Uint8Array) => Promise<void>;
		};
		const originalWrite = intentStore.write.bind(intentStore);
		let clearRejected = false;
		let sawTrueMarker = false;
		let queuedDirectLowerMutation: Promise<unknown> | undefined;
		intentStore.write = async (name, bytes) => {
			let record: any;
			try {
				record = JSON.parse(new TextDecoder().decode(bytes));
			} catch {}
			if (record?.intent?.lowerMarkerCommitted === true) {
				sawTrueMarker = true;
				const hash = record.intent.appendHashes[0] as string;
				queuedDirectLowerMutation ??= first.sharedLog.log.entryIndex
					.delete(hash)
					.then(
						() => undefined,
						(error: unknown) => error,
					);
			}
			if (!clearRejected && sawTrueMarker && record?.state === "cleared") {
				clearRejected = true;
				throw new Error("injected intent retirement failure");
			}
			await originalWrite(name, bytes);
		};

		const acknowledged = await first.store.docs.put(
			new Document({ id: "intent-retirement", name: "acknowledged" }),
		);
		expect(clearRejected).equal(true);
		expect(queuedDirectLowerMutation).to.not.equal(undefined);
		const directLowerMutationError = await queuedDirectLowerMutation!;
		expect(directLowerMutationError).to.be.instanceOf(Error);
		expect((directLowerMutationError as Error).message).to.contain(
			"retained native durable transaction intent",
		);
		expect(
			await first.sharedLog.log.entryIndex.has(acknowledged.entry.hash),
		).equal(true);
		const directLogMutationError = await first.sharedLog.log
			.join([acknowledged.entry], { reset: true })
			.then(
				() => undefined,
				(error: unknown) => error,
			);
		expect(directLogMutationError).to.be.instanceOf(Error);
		expect((directLogMutationError as Error).message).to.contain(
			"retained native durable transaction intent",
		);
		let laterMutationError: unknown;
		try {
			await first.store.docs.put(
				new Document({ id: "must-not-run", name: "must-not-run" }),
			);
		} catch (error) {
			laterMutationError = error;
		}
		expect(laterMutationError).to.be.instanceOf(Error);
		expect((laterMutationError as Error).message).to.contain(
			"recovery is required",
		);

		// Reproduce the exact adverse row evolution from the review. Even if an
		// internal actor bypassed the poison and demoted the row, the persisted true
		// marker is monotonic and recovery must never restore the pre-append absence.
		const indexed = await first.sharedLog.log.entryIndex.getShallow(
			acknowledged.entry.hash,
		);
		expect(indexed).to.not.equal(undefined);
		indexed!.value.head = false;
		await first.sharedLog.log.entryIndex.properties.index.put(indexed!.value);
		intentStore.write = originalWrite;
		await client!.stop();
		client = undefined;

		const reopened = await createStore(directory!);
		expect(
			await reopened.sharedLog.log.entryIndex.has(acknowledged.entry.hash),
		).equal(true);
		expect(reopened.sharedLog.log.length).equal(1);
		expect((await reopened.store.docs.get("intent-retirement"))?.name).equal(
			"acknowledged",
		);
	});

	it("keeps a true marker authoritative when a legacy intent has no row snapshots", async () => {
		const first = await openStore();
		const intentStore = first.sharedLog
			._nativeBackboneCoordinatePersistenceStore as {
			write: (name: string, bytes: Uint8Array) => Promise<void>;
		};
		const originalWrite = intentStore.write.bind(intentStore);
		let clearRejected = false;
		let sawTrueMarker = false;
		intentStore.write = async (name, bytes) => {
			let record: any;
			try {
				record = JSON.parse(new TextDecoder().decode(bytes));
			} catch {}
			if (record?.intent?.lowerMarkerCommitted === true) {
				sawTrueMarker = true;
			}
			if (!clearRejected && sawTrueMarker && record?.state === "cleared") {
				clearRejected = true;
				throw new Error("injected legacy intent retirement failure");
			}
			await originalWrite(name, bytes);
		};

		await first.store.docs.put(
			new Document({ id: "empty-row-marker", name: "committed" }),
		);
		const journal = first.sharedLog
			._nativeStrictDurableTransactionJournalState as {
			intent?: { lowerMarkerCommitted: boolean; lowerIndexRows: unknown[] };
		};
		expect(journal.intent?.lowerMarkerCommitted).equal(true);
		journal.intent!.lowerIndexRows = [];
		const hasManyStub = sinon
			.stub(first.sharedLog.log.entryIndex, "hasMany")
			.resolves(new Set());
		try {
			await first.sharedLog.recoverNativeStrictDurableTransactionIntent();
		} finally {
			hasManyStub.restore();
			intentStore.write = originalWrite;
		}
		expect((await first.store.docs.get("empty-row-marker"))?.name).equal(
			"committed",
		);
	});

	it("retains a true marker for recovery when rollback demotion cannot be persisted", async () => {
		const first = await openStore(true);
		const old = await first.store.docs.put(
			new Document({ id: "rollback-marker-old", name: "old" }),
			{ unique: true },
		);
		const intentStore = first.sharedLog
			._nativeBackboneCoordinatePersistenceStore as {
			write: (name: string, bytes: Uint8Array) => Promise<void>;
		};
		const originalWrite = intentStore.write.bind(intentStore);
		let sawTrueMarker = false;
		let rejectedRollbackMarker = false;
		let replacementHash: string | undefined;
		intentStore.write = async (name, bytes) => {
			let record: any;
			try {
				record = JSON.parse(new TextDecoder().decode(bytes));
			} catch {}
			if (record?.intent?.lowerMarkerCommitted === true) {
				sawTrueMarker = true;
				replacementHash = record.intent.appendHashes[0];
			} else if (
				sawTrueMarker &&
				record?.intent?.lowerMarkerCommitted === false
			) {
				rejectedRollbackMarker = true;
				throw new Error("injected rollback-marker write failure");
			}
			await originalWrite(name, bytes);
		};
		const trimFailure = new Error("injected post-marker trim failure");
		const trimStub = sinon
			.stub(first.sharedLog.log.entryIndex, "flushNativeCommittedTrimFacts")
			.rejects(trimFailure);

		let appendError: unknown;
		try {
			await first.store.docs.put(
				new Document({ id: "rollback-marker-new", name: "new" }),
				{ unique: true },
			);
		} catch (error) {
			appendError = error;
		}
		expect(appendError).to.be.instanceOf(AggregateError);
		expect(sawTrueMarker).equal(true);
		expect(rejectedRollbackMarker).equal(true);
		expect(replacementHash).to.be.a("string");

		let laterMutationError: unknown;
		try {
			await first.store.docs.put(
				new Document({ id: "rollback-marker-must-not-run", name: "blocked" }),
			);
		} catch (error) {
			laterMutationError = error;
		}
		expect(laterMutationError).to.be.instanceOf(Error);
		expect((laterMutationError as Error).message).to.contain(
			"recovery is required",
		);

		intentStore.write = originalWrite;
		trimStub.restore();
		await client!.stop();
		client = undefined;
		const reopened = await createStore(directory!, true);
		expect(reopened.sharedLog.log.length).equal(1);
		expect(await reopened.sharedLog.log.entryIndex.has(replacementHash!)).equal(
			true,
		);
		expect(await reopened.sharedLog.log.entryIndex.has(old.entry.hash)).equal(
			false,
		);
		expect((await reopened.store.docs.get("rollback-marker-new"))?.name).equal(
			"new",
		);
		expect(await reopened.store.docs.get("rollback-marker-old")).equal(
			undefined,
		);
	});

	it("settles a post-marker trim failure before concurrent close tears down recovery state", async () => {
		const first = await openStore(true);
		const old = await first.store.docs.put(
			new Document({ id: "close-race", name: "old" }),
			{ unique: true },
		);
		let markTrimStarted!: () => void;
		const trimStarted = new Promise<void>((resolve) => {
			markTrimStarted = resolve;
		});
		let releaseTrim!: () => void;
		const trimGate = new Promise<void>((resolve) => {
			releaseTrim = resolve;
		});
		const trimFailure = new Error("injected close-race trim failure");
		const trimStub = sinon
			.stub(first.sharedLog.log.entryIndex, "flushNativeCommittedTrimFacts")
			.callsFake(async () => {
				markTrimStarted();
				await trimGate;
				throw trimFailure;
			});

		try {
			const append = first.store.docs
				.put(new Document({ id: "close-race", name: "replacement" }), {
					unique: true,
				})
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			await within(trimStarted, "post-marker trim flush");
			expect(
				first.sharedLog._nativeStrictDurableTransactionJournalState.intent
					.lowerMarkerCommitted,
			).equal(true);

			let closeSettled = false;
			const close = first.sharedLog.close().then(
				(value: boolean) => value,
				(error: unknown) => error,
			);
			void close.then(() => {
				closeSettled = true;
			});
			await new Promise((resolve) => setTimeout(resolve, 25));
			expect(closeSettled).equal(false);

			releaseTrim();
			expect(await append).equal(trimFailure);
			expect(await within(close, "concurrent shared-log close")).equal(true);
		} finally {
			releaseTrim();
			trimStub.restore();
		}

		await client!.stop();
		client = undefined;
		const reopened = await createStore(directory!, true);
		expect(reopened.sharedLog.log.length).equal(1);
		expect(await reopened.sharedLog.log.entryIndex.has(old.entry.hash)).equal(
			true,
		);
		expect(
			(await reopened.sharedLog.log.getHeads().all()).map(
				(entry: { hash: string }) => entry.hash,
			),
		).deep.equal([old.entry.hash]);
		expect((await reopened.store.docs.get("close-race"))?.name).equal("old");
		expect(
			reopened.sharedLog._nativeStrictDurableTransactionJournalState.intent,
		).equal(undefined);
		const afterRecovery = await reopened.store.docs.put(
			new Document({ id: "close-race-after", name: "after" }),
			{ unique: true },
		);
		expect(afterRecovery.entry.hash).to.be.a("string").and.not.empty;
	});

	it("retains the lower finalizer before concurrent close when rollback-marker persistence fails", async () => {
		const first = await openStore(true);
		const old = await first.store.docs.put(
			new Document({ id: "close-marker-race", name: "old" }),
			{ unique: true },
		);
		const intentStore = first.sharedLog
			._nativeBackboneCoordinatePersistenceStore as {
			write: (name: string, bytes: Uint8Array) => Promise<void>;
		};
		const originalWrite = intentStore.write.bind(intentStore);
		let replacementHash: string | undefined;
		let rejectedRollbackMarker = false;
		intentStore.write = async (name, bytes) => {
			let record: any;
			try {
				record = JSON.parse(new TextDecoder().decode(bytes));
			} catch {}
			if (record?.intent?.lowerMarkerCommitted === true) {
				replacementHash = record.intent.appendHashes[0];
			} else if (
				replacementHash &&
				record?.intent?.lowerMarkerCommitted === false
			) {
				rejectedRollbackMarker = true;
				throw new Error("injected close-race rollback-marker failure");
			}
			await originalWrite(name, bytes);
		};
		let markTrimStarted!: () => void;
		const trimStarted = new Promise<void>((resolve) => {
			markTrimStarted = resolve;
		});
		let releaseTrim!: () => void;
		const trimGate = new Promise<void>((resolve) => {
			releaseTrim = resolve;
		});
		const trimStub = sinon
			.stub(first.sharedLog.log.entryIndex, "flushNativeCommittedTrimFacts")
			.callsFake(async () => {
				markTrimStarted();
				await trimGate;
				throw new Error("injected close-race post-marker trim failure");
			});

		try {
			const append = first.store.docs
				.put(new Document({ id: "close-marker-race", name: "replacement" }), {
					unique: true,
				})
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			await within(trimStarted, "rollback-marker close race trim flush");
			expect(replacementHash).to.be.a("string").and.not.empty;

			let closeSettled = false;
			const close = first.sharedLog.close().then(
				(value: boolean) => value,
				(error: unknown) => error,
			);
			void close.then(() => {
				closeSettled = true;
			});
			await new Promise((resolve) => setTimeout(resolve, 25));
			expect(closeSettled).equal(false);

			releaseTrim();
			expect(await append).to.be.instanceOf(AggregateError);
			expect(rejectedRollbackMarker).equal(true);
			expect(await within(close, "rollback-marker concurrent close")).equal(
				true,
			);
		} finally {
			releaseTrim();
			trimStub.restore();
			intentStore.write = originalWrite;
		}

		await client!.stop();
		client = undefined;
		const reopened = await createStore(directory!, true);
		expect(reopened.sharedLog.log.length).equal(1);
		expect(await reopened.sharedLog.log.entryIndex.has(replacementHash!)).equal(
			true,
		);
		expect(await reopened.sharedLog.log.entryIndex.has(old.entry.hash)).equal(
			false,
		);
		expect((await reopened.store.docs.get("close-marker-race"))?.name).equal(
			"replacement",
		);
		expect(
			reopened.sharedLog._nativeStrictDurableTransactionJournalState.intent,
		).equal(undefined);
	});

	it("keeps acknowledged replacement state recoverable after cleanup rejection and SIGKILL", async function () {
		if (process.platform === "win32") this.skip();
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-trim-hard-kill-"),
		);
		const workerPath = path.join(
			process.cwd(),
			"test/durable-native-hard-kill-worker.mjs",
		);
		const writer = spawn(
			process.execPath,
			[workerPath, "trim-failure-write", directory],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const writerExit = once(writer, "exit");
		let acknowledged:
			| {
					event: "trim-ack";
					firstHash: string;
					replacementHash: string;
					cleanupDebt: number;
			  }
			| undefined;
		try {
			acknowledged = await waitForWorkerMessage<{
				event: "trim-ack";
				firstHash: string;
				replacementHash: string;
				cleanupDebt: number;
			}>(writer, "hard-kill trim acknowledgement");
			expect(acknowledged.cleanupDebt).to.be.greaterThan(0);
			expect(writer.kill("SIGKILL")).equal(true);
			await within(
				writerExit.then(() => undefined),
				"hard-kill trim writer exit",
			);
		} finally {
			if (writer.exitCode === null && writer.signalCode === null) {
				writer.kill("SIGKILL");
			}
		}
		if (!acknowledged) {
			throw new Error("Writer exited without trim acknowledgement");
		}

		const reader = spawn(
			process.execPath,
			[
				workerPath,
				"trim-failure-read",
				directory,
				acknowledged.replacementHash,
				acknowledged.firstHash,
			],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const readerExit = once(reader, "exit") as Promise<
			[number | null, NodeJS.Signals | null]
		>;
		try {
			const reopened = await waitForWorkerMessage<{
				event: "trim-read";
				replacementBlockPresent: boolean;
				documentName?: string;
				lowerLogLength: number;
				headHashes: string[];
				oldBlockPresent: boolean;
			}>(reader, "hard-kill trim reopen");
			expect(reopened.event).equal("trim-read");
			expect(reopened.replacementBlockPresent).equal(true);
			expect(reopened.documentName).equal("replacement");
			expect(reopened.lowerLogLength).equal(1);
			expect(reopened.headHashes).deep.equal([acknowledged.replacementHash]);
			expect(reopened.oldBlockPresent).equal(false);
			const [exitCode] = await within(
				readerExit,
				"hard-kill trim reader exit",
				30_000,
			);
			expect(exitCode).equal(0);
		} finally {
			if (reader.exitCode === null && reader.signalCode === null) {
				reader.kill("SIGKILL");
			}
		}
	});

	it("rolls back durable document and coordinate prepublication after SIGKILL before the lower marker", async function () {
		if (process.platform === "win32") this.skip();
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-premark-kill-"),
		);
		const workerPath = path.join(
			process.cwd(),
			"test/durable-native-hard-kill-worker.mjs",
		);
		const writer = spawn(
			process.execPath,
			[workerPath, "premarker-write", directory],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const writerExit = once(writer, "exit");
		let phase: { event: "premarker"; hash: string } | undefined;
		try {
			phase = await waitForWorkerMessage(writer, "pre-marker phase");
			expect(writer.kill("SIGKILL")).equal(true);
			await within(
				writerExit.then(() => undefined),
				"pre-marker writer exit",
			);
		} finally {
			if (writer.exitCode === null && writer.signalCode === null) {
				writer.kill("SIGKILL");
			}
		}
		if (!phase) throw new Error("Writer did not reach the pre-marker phase");

		const reader = spawn(
			process.execPath,
			[workerPath, "premarker-read", directory, phase.hash],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const readerExit = once(reader, "exit") as Promise<
			[number | null, NodeJS.Signals | null]
		>;
		try {
			const reopened = await waitForWorkerMessage<{
				event: "premarker-read";
				documentVisible: boolean;
				lowerLogLength: number;
				coordinateVisible: boolean;
			}>(reader, "pre-marker recovery");
			expect(reopened).deep.equal({
				event: "premarker-read",
				documentVisible: false,
				lowerLogLength: 0,
				coordinateVisible: false,
			});
			expect((await within(readerExit, "pre-marker reader exit"))[0]).equal(0);
		} finally {
			if (reader.exitCode === null && reader.signalCode === null) {
				reader.kill("SIGKILL");
			}
		}
	});

	it("restores a pending external-next head after SIGKILL before the lower marker", async function () {
		if (process.platform === "win32") this.skip();
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-pending-next-kill-"),
		);
		const workerPath = path.join(
			process.cwd(),
			"test/durable-native-hard-kill-worker.mjs",
		);
		const writer = spawn(
			process.execPath,
			[workerPath, "pending-next-premarker-write", directory],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const writerExit = once(writer, "exit");
		let phase:
			| { event: "pending-next-premarker"; firstHash: string }
			| undefined;
		try {
			phase = await waitForWorkerMessage(
				writer,
				"pending external-next pre-marker phase",
			);
			expect(writer.kill("SIGKILL")).equal(true);
			await within(
				writerExit.then(() => undefined),
				"pending external-next writer exit",
			);
		} finally {
			if (writer.exitCode === null && writer.signalCode === null) {
				writer.kill("SIGKILL");
			}
		}
		if (!phase) {
			throw new Error("Writer did not demote the pending external-next row");
		}

		const reader = spawn(
			process.execPath,
			[workerPath, "pending-next-premarker-read", directory, phase.firstHash],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const readerExit = once(reader, "exit") as Promise<
			[number | null, NodeJS.Signals | null]
		>;
		try {
			const reopened = await waitForWorkerMessage<{
				event: "pending-next-premarker-read";
				documentName?: string;
				lowerLogLength: number;
				oldIndexed: boolean;
				oldHead?: boolean;
			}>(reader, "pending external-next recovery");
			expect(reopened).deep.equal({
				event: "pending-next-premarker-read",
				documentName: "old",
				lowerLogLength: 1,
				oldIndexed: true,
				oldHead: true,
			});
			expect(
				(await within(readerExit, "pending external-next reader exit"))[0],
			).equal(0);
		} finally {
			if (reader.exitCode === null && reader.signalCode === null) {
				reader.kill("SIGKILL");
			}
		}
	});

	it("finishes post-marker trim GC after SIGKILL between marker and delete", async function () {
		if (process.platform === "win32") this.skip();
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-marker-trim-kill-"),
		);
		const workerPath = path.join(
			process.cwd(),
			"test/durable-native-hard-kill-worker.mjs",
		);
		const writer = spawn(
			process.execPath,
			[workerPath, "trim-marker-write", directory],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const writerExit = once(writer, "exit");
		let phase:
			| {
					event: "trim-marker";
					firstHash: string;
					replacementHash: string;
			  }
			| undefined;
		try {
			phase = await waitForWorkerMessage(writer, "post-marker trim phase");
			expect(phase!.replacementHash).to.be.a("string").and.not.empty;
			expect(writer.kill("SIGKILL")).equal(true);
			await within(
				writerExit.then(() => undefined),
				"post-marker writer exit",
			);
		} finally {
			if (writer.exitCode === null && writer.signalCode === null) {
				writer.kill("SIGKILL");
			}
		}
		if (!phase) throw new Error("Writer did not reach post-marker trim");

		const reader = spawn(
			process.execPath,
			[
				workerPath,
				"trim-marker-read",
				directory,
				phase.replacementHash,
				phase.firstHash,
			],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const readerExit = once(reader, "exit") as Promise<
			[number | null, NodeJS.Signals | null]
		>;
		try {
			const reopened = await waitForWorkerMessage<{
				event: "trim-marker-read";
				documentName?: string;
				lowerLogLength: number;
				headHashes: string[];
				replacementIndexed: boolean;
				oldBlockPresent: boolean;
			}>(reader, "post-marker trim recovery");
			expect(reopened).deep.equal({
				event: "trim-marker-read",
				documentName: "replacement",
				lowerLogLength: 1,
				headHashes: [phase.replacementHash],
				replacementIndexed: true,
				oldBlockPresent: false,
			});
			expect((await within(readerExit, "post-marker reader exit"))[0]).equal(0);
		} finally {
			if (reader.exitCode === null && reader.signalCode === null) {
				reader.kill("SIGKILL");
			}
		}
	});

	it("finishes post-marker next-coordinate cleanup after SIGKILL", async function () {
		if (process.platform === "win32") this.skip();
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-durable-native-marker-next-kill-"),
		);
		const workerPath = path.join(
			process.cwd(),
			"test/durable-native-hard-kill-worker.mjs",
		);
		const writer = spawn(
			process.execPath,
			[workerPath, "next-marker-write", directory],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const writerExit = once(writer, "exit");
		let phase:
			| {
					event: "next-marker";
					firstHash: string;
					replacementHash: string;
					deleteHashes: string[];
			  }
			| undefined;
		try {
			phase = await waitForWorkerMessage(writer, "post-marker next phase");
			expect(phase!.replacementHash).to.be.a("string").and.not.empty;
			expect(phase!.deleteHashes).to.include(phase!.firstHash);
			expect(writer.kill("SIGKILL")).equal(true);
			await within(
				writerExit.then(() => undefined),
				"post-marker next writer exit",
			);
		} finally {
			if (writer.exitCode === null && writer.signalCode === null) {
				writer.kill("SIGKILL");
			}
		}
		if (!phase)
			throw new Error("Writer did not reach post-marker next cleanup");

		const reader = spawn(
			process.execPath,
			[
				workerPath,
				"next-marker-read",
				directory,
				phase.firstHash,
				phase.replacementHash,
			],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		const readerExit = once(reader, "exit") as Promise<
			[number | null, NodeJS.Signals | null]
		>;
		try {
			const reopened = await waitForWorkerMessage<{
				event: "next-marker-read";
				documentName?: string;
				lowerLogLength: number;
				headHashes: string[];
				oldCoordinateVisible: boolean;
				replacementCoordinateVisible: boolean;
			}>(reader, "post-marker next recovery");
			expect(reopened).deep.equal({
				event: "next-marker-read",
				documentName: "replacement",
				lowerLogLength: 2,
				headHashes: [phase.replacementHash],
				oldCoordinateVisible: false,
				replacementCoordinateVisible: true,
			});
			expect(
				(await within(readerExit, "post-marker next reader exit"))[0],
			).equal(0);
		} finally {
			if (reader.exitCode === null && reader.signalCode === null) {
				reader.kill("SIGKILL");
			}
		}
	});

	for (const coordinateCleanupCase of [
		{
			label: "single commit-only trim",
			trim: true,
			forceCommitOnly: true,
			batch: false,
			externalNext: false,
		},
		{
			label: "direct storage trim",
			trim: true,
			forceCommitOnly: false,
			batch: false,
			externalNext: false,
		},
		{
			label: "native batch trim",
			trim: true,
			forceCommitOnly: false,
			batch: true,
			externalNext: false,
		},
		{
			label: "external-next replacement",
			trim: false,
			forceCommitOnly: false,
			batch: false,
			externalNext: true,
		},
	] as const) {
		it(`retains and recovers committed coordinate cleanup debt for ${coordinateCleanupCase.label}`, async () => {
			const first = await openStore(coordinateCleanupCase.trim);
			const documentId = coordinateCleanupCase.externalNext
				? "coordinate-debt-external-next"
				: `coordinate-debt-old-${coordinateCleanupCase.label}`;
			const old = await first.store.docs.put(
				new Document({ id: documentId, name: "old" }),
				{ unique: true, replicate: false, target: "none" },
			);
			expect(
				first.sharedLog._residentEntryCoordinatesByHash.has(old.entry.hash),
			).equal(true);

			const forceCommitOnly = coordinateCleanupCase.forceCommitOnly
				? sinon
						.stub(
							first.sharedLog,
							"canUseNativeBackboneResidentCoordinateState",
						)
						.returns(false)
				: undefined;
			const storageSpy = sinon.spy(
				first.sharedLog,
				"appendLocallyPreparedPayloadNativeBackboneStorageTransaction",
			);
			const batchSpy = sinon.spy(
				first.sharedLog,
				"appendLocallyPreparedPayloadsManyNativeBackboneDocumentIndexBatch",
			);
			const originalDelete = first.sharedLog.deleteCoordinatesForHashes.bind(
				first.sharedLog,
			);
			let injected = false;
			const cleanupFailure = new Error(
				`injected ${coordinateCleanupCase.label} coordinate cleanup failure`,
			);
			const deleteStub = sinon
				.stub(first.sharedLog, "deleteCoordinatesForHashes")
				.callsFake((...args: unknown[]) => {
					const hashes = args[0] as Iterable<string>;
					const values = [...hashes];
					if (!injected && values.includes(old.entry.hash)) {
						injected = true;
						throw cleanupFailure;
					}
					return originalDelete(values);
				});

			let acknowledgedHash: string | undefined;
			try {
				if (coordinateCleanupCase.batch) {
					const acknowledged = await first.store.docs.putMany(
						[
							new Document({ id: "coordinate-debt-batch-1", name: "one" }),
							new Document({ id: "coordinate-debt-batch-2", name: "two" }),
							new Document({ id: "coordinate-debt-batch-3", name: "three" }),
						],
						{ unique: true, replicate: false, target: "none" },
					);
					acknowledgedHash = acknowledged.entries.at(-1)?.hash;
				} else {
					const acknowledged = await first.store.docs.put(
						new Document({
							id: coordinateCleanupCase.externalNext
								? documentId
								: `coordinate-debt-new-${coordinateCleanupCase.label}`,
							name: "replacement",
						}),
						coordinateCleanupCase.externalNext
							? { replicate: false, target: "none" }
							: { unique: true, replicate: false, target: "none" },
					);
					acknowledgedHash = acknowledged.entry.hash;
				}
				expect(acknowledgedHash).to.be.a("string").and.not.empty;
				expect(injected).equal(true);
				if (coordinateCleanupCase.batch) {
					expect(batchSpy.calledOnce).equal(true);
				} else if (!coordinateCleanupCase.forceCommitOnly) {
					expect(storageSpy.calledOnce).equal(true);
				}
				const retainedIntent =
					first.sharedLog._nativeStrictDurableTransactionJournalState.intent;
				expect(retainedIntent?.lowerMarkerCommitted).equal(true);
				expect(retainedIntent?.coordinateDeleteHashes).to.include(
					old.entry.hash,
				);

				const laterMutation = await first.store.docs
					.put(
						new Document({ id: "coordinate-debt-blocked", name: "blocked" }),
						{ unique: true, replicate: false, target: "none" },
					)
					.then(
						() => undefined,
						(error: unknown) => error,
					);
				expect(laterMutation).to.be.instanceOf(Error);
				expect((laterMutation as Error).message).to.contain(
					"recovery is required",
				);
			} finally {
				deleteStub.restore();
				batchSpy.restore();
				storageSpy.restore();
				forceCommitOnly?.restore();
			}

			if (!acknowledgedHash) {
				throw new Error(
					"Committed coordinate cleanup test was not acknowledged",
				);
			}
			await client!.stop();
			client = undefined;
			const reopened = await createStore(
				directory!,
				coordinateCleanupCase.trim,
			);
			expect(
				reopened.sharedLog._residentEntryCoordinatesByHash.has(old.entry.hash),
			).equal(false);
			if (!coordinateCleanupCase.forceCommitOnly) {
				expect(
					reopened.sharedLog._residentEntryCoordinatesByHash.has(
						acknowledgedHash,
					),
				).equal(true);
			}
			expect(
				reopened.sharedLog._nativeStrictDurableTransactionJournalState.intent,
			).equal(undefined);
			const afterRecovery = await reopened.store.docs.put(
				new Document({
					id: `coordinate-debt-after-${coordinateCleanupCase.label}`,
					name: "after",
				}),
				{ unique: true, replicate: false, target: "none" },
			);
			expect(afterRecovery.entry.hash).to.be.a("string").and.not.empty;
		});
	}

	for (const recovery of [
		{
			label: "graceful close",
			mode: "mirror-failure-graceful-write",
			kill: false,
		},
		{ label: "SIGKILL", mode: "mirror-failure-write", kill: true },
	] as const) {
		it(`discards a failed native document transaction across ${recovery.label} and fresh-process reopen`, async function () {
			if (recovery.kill && process.platform === "win32") this.skip();
			directory = await fs.mkdtemp(
				path.join(os.tmpdir(), "peerbit-durable-native-failed-recovery-"),
			);
			const workerPath = path.join(
				process.cwd(),
				"test/durable-native-hard-kill-worker.mjs",
			);
			const writer = spawn(
				process.execPath,
				[workerPath, recovery.mode, directory],
				{ stdio: ["ignore", "pipe", "pipe"] },
			);
			const writerExit = once(writer, "exit") as Promise<
				[number | null, NodeJS.Signals | null]
			>;
			let rejected:
				| {
						event: "mirror-rejected";
						failedHash?: string;
						errorName?: string;
						documentVisible: boolean;
						lowerLogLength: number;
						coordinateVisible: boolean;
				  }
				| undefined;
			try {
				rejected = await waitForWorkerMessage(
					writer,
					"failed mirror rejection",
				);
				if (!rejected) throw new Error("Missing failed mirror rejection");
				expect(rejected.errorName).equal("NativeDurableCommitError");
				expect(rejected.failedHash).to.be.a("string");
				expect(rejected.documentVisible).equal(false);
				expect(rejected.lowerLogLength).equal(0);
				expect(rejected.coordinateVisible).equal(false);
				if (recovery.kill) {
					expect(writer.kill("SIGKILL")).equal(true);
				}
				const [exitCode] = await within(
					writerExit,
					"failed mirror writer exit",
					30_000,
				);
				if (!recovery.kill) expect(exitCode).equal(0);
			} finally {
				if (writer.exitCode === null && writer.signalCode === null) {
					writer.kill("SIGKILL");
				}
			}
			if (!rejected?.failedHash) throw new Error("Missing failed native CID");

			const reader = spawn(
				process.execPath,
				[workerPath, "mirror-failure-read", directory, rejected.failedHash],
				{ stdio: ["ignore", "pipe", "pipe"] },
			);
			const readerExit = once(reader, "exit") as Promise<
				[number | null, NodeJS.Signals | null]
			>;
			try {
				const reopened = await waitForWorkerMessage<{
					event: "mirror-read";
					documentVisible: boolean;
					blockVisible: boolean;
					lowerLogLength: number;
					coordinateVisible: boolean;
				}>(reader, "failed mirror fresh-process reopen");
				expect(reopened).deep.equal({
					event: "mirror-read",
					documentVisible: false,
					blockVisible: false,
					lowerLogLength: 0,
					coordinateVisible: false,
				});
				const [exitCode] = await within(
					readerExit,
					"failed mirror reader exit",
				);
				expect(exitCode).equal(0);
			} finally {
				if (reader.exitCode === null && reader.signalCode === null) {
					reader.kill("SIGKILL");
				}
			}
		});
	}

	it("reports durable bytes after a cold native reopen", async () => {
		const { store, wrapper, durable } = await openStore();
		const put = await store.docs.put(
			new Document({ id: "durable-size", name: "durable-size" }),
		);
		const beforeClose = await wrapper.size();
		expect(beforeClose).equal(await durable.size());
		expect(beforeClose).greaterThan(0);

		const storeDirectory = directory!;
		await client!.stop();
		client = undefined;
		const reopened = await createStore(storeDirectory);
		expect(reopened.backbone.blocks.has(put.entry.hash)).equal(false);
		expect(reopened.backbone.blocks.size()).equal(0);
		const reopenedSize = await reopened.wrapper.size();
		expect(reopenedSize).equal(await reopened.durable.size());
		expect(reopenedSize).equal(beforeClose);
	});

	it("does not acknowledge a native append before durable storage", async () => {
		const { store, wrapper, durable } = await openStore();
		const originalPutKnown = durable.putKnown.bind(durable);
		let releaseWrite!: () => void;
		const writeGate = new Promise<void>((resolve) => {
			releaseWrite = resolve;
		});
		let markWriteStarted!: () => void;
		const writeStarted = new Promise<void>((resolve) => {
			markWriteStarted = resolve;
		});
		const durablePutStub = sinon
			.stub(durable, "putKnown")
			.callsFake(async (...args: unknown[]) => {
				const [cid, bytes] = args as [string, Uint8Array];
				markWriteStarted();
				await writeGate;
				return originalPutKnown(cid, bytes);
			});
		const mirrorSpy = sinon.spy(wrapper, "mirrorToDurable");

		try {
			let settled = false;
			const pendingPut = store.docs.put(
				new Document({ id: "ordered", name: "ordered" }),
			);
			void pendingPut.then(
				() => {
					settled = true;
				},
				() => {
					settled = true;
				},
			);
			await within(writeStarted, "durable block write");
			await Promise.resolve();
			expect(settled).equal(false);

			releaseWrite();
			const put = await pendingPut;
			expect(mirrorSpy.callCount).equal(1);
			expect(await durable.has(put.entry.hash)).equal(true);
		} finally {
			releaseWrite();
			mirrorSpy.restore();
			durablePutStub.restore();
		}
	});

	it("does not acknowledge when batching defers the native WAL durability barrier", async () => {
		directory = await fs.mkdtemp(
			path.join(os.tmpdir(), "peerbit-native-wal-barrier-"),
		);
		const persistence = new NativeBackboneNodeCoordinatePersistence(
			path.join(directory, "coordinate-wal"),
			{
				flushOnAppend: false,
				flushMaxPendingBytes: Number.MAX_SAFE_INTEGER,
				writeBufferMaxBytes: Number.MAX_SAFE_INTEGER,
			},
		);
		const originalFlush = persistence.intentStore.flush!.bind(
			persistence.intentStore,
		);
		let releaseWalFlush!: () => void;
		const flushGate = new Promise<void>((resolve) => {
			releaseWalFlush = resolve;
		});
		let markWalFlushStarted!: () => void;
		const walFlushStarted = new Promise<void>((resolve) => {
			markWalFlushStarted = resolve;
		});
		let gateArmed = false;
		const flushStub = sinon
			.stub(persistence.intentStore, "flush")
			.callsFake(async (name?: string) => {
				if (gateArmed && name?.endsWith(".wal")) {
					gateArmed = false;
					markWalFlushStarted();
					await flushGate;
				}
				await originalFlush(name);
			});

		try {
			const { store } = await createStore(directory, false, false, persistence);
			gateArmed = true;
			let settled = false;
			const pendingPut = store.docs.put(
				new Document({ id: "wal-barrier", name: "wal-barrier" }),
			);
			void pendingPut.then(
				() => {
					settled = true;
				},
				() => {
					settled = true;
				},
			);
			await within(walFlushStarted, "native WAL durability barrier");
			await Promise.resolve();
			expect(settled).equal(false);

			releaseWalFlush();
			await pendingPut;
			expect(settled).equal(true);
		} finally {
			releaseWalFlush();
			flushStub.restore();
		}
	});

	it("returns a typed unsafe-retry error and poisons later mutations", async () => {
		const { store, sharedLog, wrapper, durable, backbone } = await openStore();
		// Force the graph commit-only variant; the normal test above covers the
		// resident-coordinate storage transaction variant.
		const residentStateStub = sinon
			.stub(sharedLog, "canUseNativeBackboneResidentCoordinateState")
			.returns(false);
		const durablePutStub = sinon
			.stub(durable, "putKnown")
			.rejects(new Error("durable mirror failed"));
		const mirrorSpy = sinon.spy(wrapper, "mirrorToDurable");

		try {
			const failure = await store.docs
				.put(new Document({ id: "failure", name: "failure" }))
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			expect(failure).to.be.instanceOf(NativeDurableCommitError);
			const typed = failure as NativeDurableCommitError;
			expect(typed.nativeCommitApplied).equal(true);
			expect(typed.retrySafe).equal(false);
			expect(store.docs.log.log.length).equal(0);
			expect(mirrorSpy.callCount).equal(1);
			const committedHash = mirrorSpy.firstCall.args[0] as string;
			expect(typed.committedCids).deep.equal([committedHash]);
			expect(typed.failedCids).deep.equal([committedHash]);
			// The prepare happened before exclusive hot-map ownership could be proven.
			// Its unreachable native bytes are retained, while lower-log/durable liveness
			// remains absent and poisoned reads cannot expose the orphan.
			expect(backbone.blocks.has(committedHash)).equal(true);
			expect(await durable.has(committedHash)).equal(false);
			expect(await store.docs.log.log.entryIndex.has(committedHash)).equal(
				false,
			);
			expect(await wrapper.get(committedHash)).equal(undefined);

			const retryFailure = await store.docs
				.put(new Document({ id: "retry", name: "retry" }))
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			expect(retryFailure).equal(failure);
			expect(mirrorSpy.callCount).equal(1);
		} finally {
			mirrorSpy.restore();
			durablePutStub.restore();
			residentStateStub.restore();
		}
	});

	it("rejects concurrent receive writes and onMessage with one poison", async () => {
		const { store, sharedLog, wrapper, durable } = await openStore();
		const baseline = await store.docs.put(
			new Document({ id: "receive-poison", name: "receive-poison" }),
		);
		const cid = baseline.entry.hash;
		const bytes = await durable.get(cid);
		if (!bytes) {
			throw new Error("Expected baseline durable bytes");
		}
		const originalPutKnown = durable.putKnown.bind(durable);
		let releaseFirst!: () => void;
		const firstGate = new Promise<void>((resolve) => {
			releaseFirst = resolve;
		});
		let releaseSecond!: () => void;
		const secondGate = new Promise<void>((resolve) => {
			releaseSecond = resolve;
		});
		let markFirstStarted!: () => void;
		const firstStarted = new Promise<void>((resolve) => {
			markFirstStarted = resolve;
		});
		let markSecondStarted!: () => void;
		const secondStarted = new Promise<void>((resolve) => {
			markSecondStarted = resolve;
		});
		let callIndex = 0;
		const durablePutStub = sinon
			.stub(durable, "putKnown")
			.callsFake(async (...args: unknown[]) => {
				const [putCid, putBytes] = args as [string, Uint8Array];
				const current = callIndex++;
				if (current === 0) {
					markFirstStarted();
					await firstGate;
					throw new Error("receive durable mirror failed");
				}
				markSecondStarted();
				await secondGate;
				return originalPutKnown(putCid, putBytes);
			});

		try {
			const firstWrite = wrapper.putKnown(cid, bytes).then(
				() => undefined,
				(error: unknown) => error,
			);
			const concurrentWrite = wrapper.putKnown(cid, bytes).then(
				() => undefined,
				(error: unknown) => error,
			);
			await within(firstStarted, "first receive durable write");
			await within(secondStarted, "concurrent receive durable write");

			releaseFirst();
			const failure = await firstWrite;
			expect(failure).to.be.instanceOf(NativeDurableCommitError);
			releaseSecond();
			expect(await concurrentWrite).equal(failure);

			let synchronousFailure: unknown;
			try {
				wrapper.putKnownManyColumns([cid], [bytes]);
			} catch (error) {
				synchronousFailure = error;
			}
			expect(synchronousFailure).equal(failure);
			const releaseStash = sinon.stub().returns(true);
			const stashMessage = new StashBackedRawExchangeHeadsMessage({
				messageId: new Uint8Array([1]),
				hashes: [],
				gidRefrences: [],
				byteLengths: new Uint32Array(),
				reserved: new Uint8Array(4),
				stash: {
					stashedBlocks: () => undefined,
					release: releaseStash,
				},
			});
			const messageFailure = await sharedLog
				.onMessage(stashMessage, {} as any)
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			expect(messageFailure).equal(failure);
			expect(releaseStash.calledOnce).equal(true);
			expect(stashMessage.release()).equal(false);
			expect(releaseStash.calledOnce).equal(true);
		} finally {
			releaseFirst();
			releaseSecond();
			durablePutStub.restore();
		}
	});

	for (const forceGenericFallback of [false, true]) {
		it(`holds a two-entry stash receive behind its durable barrier (${forceGenericFallback ? "generic fallback" : "prepared facts"})`, async () => {
			const { store, sharedLog, durable } = await openStore(false, true);
			const sourceDirectory = await fs.mkdtemp(
				path.join(os.tmpdir(), "peerbit-durable-native-receive-source-"),
			);
			const sourceClient = await Peerbit.create({
				directory: sourceDirectory,
				...createRustPeerbitOptions(),
			});
			const storeId = store.id;
			const source = new TestStore<Document>({
				docs: new Documents<Document>({ id: storeId }),
			});
			source.id = storeId;
			await sourceClient.open(source, {
				args: createOpenArgs(sourceDirectory),
			});
			const writes = await Promise.all([
				source.docs.put(new Document({ id: "receive-a", name: "receive-a" })),
				source.docs.put(new Document({ id: "receive-b", name: "receive-b" })),
			]);
			const hashes = writes.map((write) => write.entry.hash);
			expect(await sharedLog.log.hasMany(hashes)).deep.equal(new Set());
			const blocks = await Promise.all(
				hashes.map(async (hash) => {
					const bytes = await source.docs.log.log.blocks.get(hash);
					if (!bytes) throw new Error(`Missing source block ${hash}`);
					return bytes;
				}),
			);

			let releaseWrite!: () => void;
			const writeGate = new Promise<void>((resolve) => {
				releaseWrite = resolve;
			});
			let markWriteStarted!: () => void;
			const writeStarted = new Promise<void>((resolve) => {
				markWriteStarted = resolve;
			});
			const durableFailure = new Error("delayed stash durable rejection");
			const durablePutStub = sinon
				.stub(durable, "putKnownMany")
				.callsFake(async () => {
					markWriteStarted();
					await writeGate;
					throw durableFailure;
				});
			const canAppendValidationStub = sinon
				.stub(sharedLog, "canSkipLowerLogCanAppendForNetworkJoin")
				.resolves(true);
			const preparedJoinProbe = forceGenericFallback
				? sinon
						.stub(sharedLog.log, "joinPreparedAppendFactsBatch")
						.resolves(false)
				: sinon.spy(sharedLog.log, "joinPreparedAppendFactsBatch");
			const genericJoinSpy = sinon.spy(sharedLog.log, "join");
			const releaseStash = sinon.stub().returns(true);
			const stashMessage = new StashBackedRawExchangeHeadsMessage({
				messageId: new Uint8Array([7, forceGenericFallback ? 1 : 0]),
				hashes,
				gidRefrences: hashes.map(() => []),
				byteLengths: Uint32Array.from(blocks.map((bytes) => bytes.byteLength)),
				reserved: new Uint8Array(4),
				stash: {
					stashedBlocks: (_id, indexes) =>
						indexes ? Array.from(indexes, (index) => blocks[index]!) : blocks,
					release: releaseStash,
				},
			});

			try {
				let settled = false;
				const handler = sharedLog
					.onMessage(stashMessage, {
						from: source.node.identity.publicKey,
					} as any)
					.then(
						() => undefined,
						(error: unknown) => error,
					);
				void handler.then(() => {
					settled = true;
				});
				const firstOutcome = await within(
					Promise.race([
						writeStarted.then(() => ({ kind: "write" as const })),
						handler.then((result: unknown) => ({
							kind: "handler" as const,
							result,
						})),
					]),
					"stash durable batch write",
				);
				if (firstOutcome.kind === "handler") {
					throw (
						firstOutcome.result ??
						new Error("Stash handler returned before its durable batch write")
					);
				}
				await Promise.resolve();
				expect(settled).equal(false);
				expect(sharedLog.log.length).equal(0);
				expect(await store.docs.index.getSize()).equal(0);

				releaseWrite();
				const failure = await handler;
				expect(failure).to.be.instanceOf(NativeDurableCommitError);
				expect(failure).equal(sharedLog._nativeDurableCommitFailure);
				expect(releaseStash.calledOnce).equal(true);
				expect(stashMessage.release()).equal(false);
				expect(releaseStash.calledOnce).equal(true);
				expect(sharedLog.log.length).equal(0);
				expect(await store.docs.index.getSize()).equal(0);
				expect(preparedJoinProbe.calledOnce).equal(true);
				expect(genericJoinSpy.callCount).equal(forceGenericFallback ? 1 : 0);
				for (const hash of hashes) {
					expect(
						sharedLog._residentEntryCoordinatesByHash?.has(hash) ?? false,
					).equal(false);
				}
			} finally {
				releaseWrite();
				genericJoinSpy.restore();
				preparedJoinProbe.restore();
				canAppendValidationStub.restore();
				durablePutStub.restore();
				await sourceClient.stop();
				await fs.rm(sourceDirectory, { recursive: true, force: true });
			}
		});
	}

	it("uses one durable barrier for a native batch and reports its failure", async () => {
		const { store, wrapper, durable, backbone } = await openStore();
		const docs = [
			new Document({ id: "batch-1", name: "batch-1" }),
			new Document({ id: "batch-2", name: "batch-2" }),
			new Document({ id: "batch-3", name: "batch-3" }),
		];
		let releaseWrite!: () => void;
		const writeGate = new Promise<void>((resolve) => {
			releaseWrite = resolve;
		});
		let markWriteStarted!: () => void;
		const writeStarted = new Promise<void>((resolve) => {
			markWriteStarted = resolve;
		});
		const durablePutStub = sinon
			.stub(durable, "putKnownMany")
			.callsFake(async () => {
				markWriteStarted();
				await writeGate;
				throw new Error("batch durable mirror failed");
			});
		const singlePutSpy = sinon.spy(durable, "putKnown");
		const mirrorSpy = sinon.spy(wrapper, "mirrorManyToDurable");
		const nativeBatchSpy = sinon.spy(
			backbone,
			"preparePlainCommittedNoNextStorageAppendDocumentIndexCompactBatchTransaction",
		);

		try {
			let settled = false;
			const pending = store.docs.putMany(docs, {
				unique: true,
				target: "none",
			});
			void pending.then(
				() => {
					settled = true;
				},
				() => {
					settled = true;
				},
			);
			await within(writeStarted, "durable batch mirror");
			expect(mirrorSpy.callCount).equal(1);
			expect(durablePutStub.callCount).equal(1);
			expect(singlePutSpy.callCount).equal(0);
			expect(settled).equal(false);

			releaseWrite();
			const failure = await pending.then(
				() => undefined,
				(error: unknown) => error,
			);
			expect(failure).to.be.instanceOf(NativeDurableCommitError);
			const prepared = nativeBatchSpy.firstCall.returnValue as Array<{
				entry: { hash: string };
			}>;
			expect((failure as NativeDurableCommitError).committedCids).deep.equal(
				prepared.map(({ entry }) => entry.hash),
			);
			expect((failure as NativeDurableCommitError).failedCids).deep.equal(
				prepared.map(({ entry }) => entry.hash),
			);
			expect(store.docs.log.log.length).equal(0);
		} finally {
			releaseWrite();
			nativeBatchSpy.restore();
			mirrorSpy.restore();
			singlePutSpy.restore();
			durablePutStub.restore();
		}
	});

	it("does not delete the published head when its replacement mirror fails", async () => {
		const { store, wrapper, durable } = await openStore(true);
		const first = await store.docs.put(
			new Document({ id: "old-head", name: "old-head" }),
			{ unique: true },
		);
		const firstHash = first.entry.hash;
		const durablePutStub = sinon
			.stub(durable, "putKnown")
			.rejects(new Error("replacement mirror failed"));

		try {
			const failure = await store.docs
				.put(new Document({ id: "replacement", name: "replacement" }), {
					unique: true,
				})
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			expect(failure).to.be.instanceOf(NativeDurableCommitError);
			expect(wrapper.stagedNativeDeleteCleanups.size).equal(0);
			expect(store.docs.log.log.length).equal(1);
			expect(await durable.has(firstHash)).equal(true);
			expect(await wrapper.get(firstHash)).to.be.instanceOf(Uint8Array);
		} finally {
			durablePutStub.restore();
		}
	});

	it("preserves acknowledged and shared same-CID bytes during compensation", async () => {
		const { store, wrapper, durable, backbone } = await openStore();
		const appended = await store.docs.put(
			new Document({ id: "same-cid", name: "same-cid" }),
		);
		const cid = appended.entry.hash;
		const bytes = await durable.get(cid);
		if (!bytes) throw new Error("Expected acknowledged durable bytes");

		// No opaque ownership evidence means compensation must retain an orphan.
		await wrapper.rollbackFailedNativeCommits([cid]);
		expect(await durable.has(cid)).equal(true);
		expect(backbone.blocks.has(cid)).equal(true);
		expect(await wrapper.get(cid)).deep.equal(bytes);

		// Two explicit mirrors of the same CID are shared generations. Rolling one
		// back while restoring the CID must not remove either acknowledged copy.
		const firstOwnership = await wrapper.mirrorToDurable(cid, bytes);
		const secondOwnership = await wrapper.mirrorToDurable(cid, bytes);
		await wrapper.rollbackFailedNativeCommits([cid], [cid], secondOwnership);
		wrapper.acknowledgeNativeCommitOwnership(firstOwnership);
		expect(await durable.has(cid)).equal(true);
		expect(backbone.blocks.has(cid)).equal(true);
		expect(await store.docs.log.log.entryIndex.has(cid)).equal(true);
		expect((await store.docs.log.log.getHeads().all())[0]?.hash).equal(cid);
	});

	it("generation-fences unmirrored rollback from a same-CID generic write", async () => {
		const { wrapper, durable, backbone } = await openStore();
		const bytes = new Uint8Array([91, 17, 203, 4, 55, 8]);
		// Simulate the strict native prepare: it writes the hot map directly and
		// therefore does not advance the wrapper's generic-write generation.
		const cid = await backbone.blocks.put(bytes);
		expect(await durable.has(cid)).equal(false);

		const originalHasMany = durable.hasMany.bind(durable);
		let markPresenceReadStarted!: () => void;
		const presenceReadStarted = new Promise<void>((resolve) => {
			markPresenceReadStarted = resolve;
		});
		let releaseStalePresence!: () => void;
		const stalePresenceGate = new Promise<void>((resolve) => {
			releaseStalePresence = resolve;
		});
		const hasManyStub = sinon
			.stub(durable, "hasMany")
			.callsFake(async (...args: unknown[]) => {
				const [cids] = args as [string[]];
				markPresenceReadStarted();
				await stalePresenceGate;
				return cids.map(() => false);
			});
		try {
			const rollback = wrapper.rollbackUnmirroredNativeCommits([cid]);
			await within(presenceReadStarted, "unmirrored rollback presence read");
			const genericWrite = wrapper.putKnown(cid, bytes);
			await genericWrite;
			releaseStalePresence();
			await rollback;
			expect(backbone.blocks.has(cid)).equal(true);
			expect(await durable.has(cid)).equal(true);
			expect(await wrapper.get(cid)).deep.equal(bytes);
		} finally {
			releaseStalePresence();
			hasManyStub.restore();
			// Verify the original adapter still answers after the gated stub is gone.
			expect(await originalHasMany([cid])).deep.equal([true]);
		}
	});

	it("rolls back a trimmed single append when its lower index flush fails", async () => {
		const { store, sharedLog } = await openStore(true);
		const old = await store.docs.put(
			new Document({ id: "index-single-old", name: "index-single-old" }),
			{ unique: true },
		);
		const oldHash = old.entry.hash;
		const changes: unknown[] = [];
		store.docs.events.addEventListener("change", (event) =>
			changes.push(event.detail),
		);
		const indexFailure = new Error("single lower index flush failed");
		const lowerIndex = sharedLog.log.entryIndex.properties.index;
		const originalPut = lowerIndex.put.bind(lowerIndex);
		const putStub = sinon
			.stub(lowerIndex, "put")
			.callsFake(async (...args: Parameters<typeof lowerIndex.put>) => {
				await originalPut(...args);
				throw indexFailure;
			});

		try {
			const failure = await store.docs
				.put(
					new Document({
						id: "index-single-new",
						name: "index-single-new",
					}),
					{ unique: true },
				)
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			expect(failure).equal(indexFailure);
			const failedHash = (putStub.firstCall.args[0] as { hash: string }).hash;
			expect(sharedLog.log.length).equal(1);
			expect(
				(await sharedLog.log.entryIndex.getShallow(oldHash))?.value.hash,
				"old lower index row",
			).equal(oldHash);
			expect(await sharedLog.log.entryIndex.has(oldHash)).equal(true);
			expect(await sharedLog.log.entryIndex.has(failedHash)).equal(false);
			expect(await sharedLog.log.entryIndex.getShallow(failedHash)).equal(
				undefined,
			);
			expect(
				(await sharedLog.log.getHeads().all()).map(
					(entry: { hash: string }) => entry.hash,
				),
			).deep.equal([oldHash]);
			expect((await store.docs.get("index-single-old"))?.name).equal(
				"index-single-old",
			);
			expect(await store.docs.get("index-single-new")).equal(undefined);
			expect(changes).deep.equal([]);

			await new Promise((resolve) => setTimeout(resolve, 400));
			expect(sharedLog.log.length).equal(1);
			expect(await sharedLog.log.entryIndex.has(oldHash)).equal(true);
			expect(changes).deep.equal([]);
			await store.close();
			await client!.open(store, {
				args: createOpenArgs(directory!, true),
			});
			expect(store.docs.log.log.length).equal(1);
			expect(
				(await store.docs.log.log.getHeads().all()).map((entry) => entry.hash),
			).deep.equal([oldHash]);
			expect(await store.docs.log.log.entryIndex.has(failedHash)).equal(false);
			expect((await store.docs.get("index-single-old"))?.name).equal(
				"index-single-old",
			);
			expect(await store.docs.get("index-single-new")).equal(undefined);
		} finally {
			putStub.restore();
		}
	});

	it("rolls back a trimmed batch when its lower index flush fails", async () => {
		const { store, sharedLog } = await openStore(true);
		const old = await store.docs.put(
			new Document({ id: "index-batch-old", name: "index-batch-old" }),
			{ unique: true },
		);
		const oldHash = old.entry.hash;
		const changes: unknown[] = [];
		store.docs.events.addEventListener("change", (event) =>
			changes.push(event.detail),
		);
		const indexFailure = new Error("batch lower index flush failed");
		const lowerIndex = sharedLog.log.entryIndex.properties.index;
		const originalPutBatch = lowerIndex.putBatch!.bind(lowerIndex);
		const putBatchStub = sinon
			.stub(lowerIndex, "putBatch")
			.callsFake(async (...args: Parameters<typeof originalPutBatch>) => {
				await originalPutBatch(...args);
				throw indexFailure;
			});

		try {
			const failure = await store.docs
				.putMany(
					[
						new Document({ id: "index-batch-new-1", name: "new-1" }),
						new Document({ id: "index-batch-new-2", name: "new-2" }),
						new Document({ id: "index-batch-new-3", name: "new-3" }),
					],
					{ unique: true, target: "none" },
				)
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			expect(failure).equal(indexFailure);
			const failedHashes = (
				putBatchStub.firstCall.args[0] as Array<{ hash: string }>
			).map((entry) => entry.hash);
			expect(sharedLog.log.length).equal(1);
			expect(await sharedLog.log.entryIndex.has(oldHash)).equal(true);
			for (const hash of failedHashes) {
				expect(await sharedLog.log.entryIndex.has(hash)).equal(false);
				expect(await sharedLog.log.entryIndex.getShallow(hash)).equal(
					undefined,
				);
			}
			expect(
				(await sharedLog.log.getHeads().all()).map(
					(entry: { hash: string }) => entry.hash,
				),
			).deep.equal([oldHash]);
			expect((await store.docs.get("index-batch-old"))?.name).equal(
				"index-batch-old",
			);
			for (const id of [
				"index-batch-new-1",
				"index-batch-new-2",
				"index-batch-new-3",
			]) {
				expect(await store.docs.get(id)).equal(undefined);
			}
			expect(changes).deep.equal([]);

			await new Promise((resolve) => setTimeout(resolve, 400));
			expect(sharedLog.log.length).equal(1);
			expect(await sharedLog.log.entryIndex.has(oldHash)).equal(true);
			expect(changes).deep.equal([]);
			await store.close();
			await client!.open(store, {
				args: createOpenArgs(directory!, true),
			});
			expect(store.docs.log.log.length).equal(1);
			expect(
				(await store.docs.log.log.getHeads().all()).map((entry) => entry.hash),
			).deep.equal([oldHash]);
			for (const hash of failedHashes) {
				expect(await store.docs.log.log.entryIndex.has(hash)).equal(false);
			}
			expect((await store.docs.get("index-batch-old"))?.name).equal(
				"index-batch-old",
			);
			for (const id of [
				"index-batch-new-1",
				"index-batch-new-2",
				"index-batch-new-3",
			]) {
				expect(await store.docs.get(id)).equal(undefined);
			}
		} finally {
			putBatchStub.restore();
		}
	});

	it("commits replacement facts and retains retry debt when trim cleanup fails", async () => {
		const { store, sharedLog, wrapper, durable } = await openStore(true);
		const first = await store.docs.put(
			new Document({ id: "trim-old", name: "trim-old" }),
			{ unique: true },
		);
		const cleanupFailure = new Error("durable trim cleanup failed");
		const changes: Array<{
			added: Array<{ id: string }>;
			removed: Array<{ id: string }>;
		}> = [];
		store.docs.events.addEventListener("change", (event) =>
			changes.push(event.detail as (typeof changes)[number]),
		);
		const originalRmMany = durable.rmMany.bind(durable);
		let cleanupFailuresRemaining = 2;
		const durableRmManyStub = sinon
			.stub(durable, "rmMany")
			.callsFake(async (...args: unknown[]) => {
				const [cids] = args as [string[]];
				if (cleanupFailuresRemaining > 0) {
					cleanupFailuresRemaining--;
					throw cleanupFailure;
				}
				return originalRmMany(cids);
			});

		try {
			const replacement = await store.docs.put(
				new Document({ id: "trim-new", name: "trim-new" }),
				{ unique: true },
			);
			expect(sharedLog.log.length).equal(1);
			expect(
				(await sharedLog.log.getHeads().all()).map(
					(entry: { hash: string }) => entry.hash,
				),
			).deep.equal([replacement.entry.hash]);
			expect(await sharedLog.log.entryIndex.has(first.entry.hash)).equal(false);
			expect(await sharedLog.log.entryIndex.has(replacement.entry.hash)).equal(
				true,
			);
			expect(await store.docs.get("trim-old")).equal(undefined);
			expect((await store.docs.get("trim-new"))?.name).equal("trim-new");
			expect(changes).to.have.length(1);
			expect(changes[0]!.added.map((document) => document.id)).deep.equal([
				"trim-new",
			]);
			expect(changes[0]!.removed.map((document) => document.id)).deep.equal([
				"trim-old",
			]);
			expect(wrapper.pendingNativeDeleteCleanup.size).equal(1);
			expect(await durable.has(first.entry.hash)).equal(true);

			await store.close();
			expect(durableRmManyStub.callCount).equal(3);
			expect(wrapper.pendingNativeDeleteCleanup.size).equal(0);
			expect(durable.status()).equal("closed");
		} finally {
			durableRmManyStub.restore();
		}
	});

	it("completes an exact poisoned close with conservative trim debt", async () => {
		const { store, wrapper, durable } = await openStore(true);
		await store.docs.put(
			new Document({ id: "poison-debt-old", name: "poison-debt-old" }),
			{ unique: true, target: "none", replicate: false },
		);
		const cleanupFailure = new Error("persistent trim cleanup failure");
		const durableRmManyStub = sinon
			.stub(durable, "rmMany")
			.rejects(cleanupFailure);
		try {
			const replacement = await store.docs.put(
				new Document({ id: "poison-debt-new", name: "poison-debt-new" }),
				{ unique: true, target: "none", replicate: false },
			);
			expect(wrapper.pendingNativeDeleteCleanup.size).to.be.greaterThan(0);
			const bytes = await durable.get(replacement.entry.hash);
			if (!bytes) throw new Error("Expected durable replacement block");

			const mirrorFailure = new Error("durable mirror poison with trim debt");
			const durablePutStub = sinon
				.stub(durable, "putKnown")
				.rejects(mirrorFailure);
			const poison = await wrapper.putKnown(replacement.entry.hash, bytes).then(
				() => undefined,
				(error: unknown) => error,
			);
			durablePutStub.restore();
			expect(poison).to.be.instanceOf(NativeDurableCommitError);

			const closeFailure = await store.close().then(
				() => undefined,
				(error: unknown) => error,
			);
			expect(closeFailure).equal(poison);
			expect(wrapper.pendingNativeDeleteCleanup.size).to.be.greaterThan(0);
			expect(wrapper.stopCompleted).equal(true);
			expect(durable.status()).equal("closed");

			await store.close();
			expect(store.closed).equal(true);
			expect(wrapper.pendingNativeDeleteCleanup.size).to.be.greaterThan(0);
			expect(durable.status()).equal("closed");
		} finally {
			durableRmManyStub.restore();
		}
	});

	it("keeps the new batch state through partial trim cleanup and reopen", async () => {
		const { store, sharedLog, wrapper, durable } = await openStore(true);
		const old = await store.docs.put(
			new Document({ id: "partial-old", name: "partial-old" }),
			{ unique: true },
		);
		const changes: Array<{
			added: Array<{ id: string }>;
			removed: Array<{ id: string }>;
		}> = [];
		store.docs.events.addEventListener("change", (event) =>
			changes.push(event.detail as (typeof changes)[number]),
		);
		const originalRmMany = durable.rmMany.bind(durable);
		const cleanupFailure = new Error("partial durable trim cleanup failed");
		const durableRmManyStub = sinon
			.stub(durable, "rmMany")
			.callsFake(async (...args: unknown[]) => {
				const [cids] = args as [string[]];
				if (cids.length > 0) await originalRmMany([cids[0]!]);
				throw cleanupFailure;
			});

		const replacements = await store.docs.putMany(
			[
				new Document({ id: "partial-new-1", name: "partial-new-1" }),
				new Document({ id: "partial-new-2", name: "partial-new-2" }),
				new Document({ id: "partial-new-3", name: "partial-new-3" }),
			],
			{ unique: true, target: "none" },
		);
		const kept = replacements.entries.at(-1)!;
		expect(sharedLog.log.length).equal(1);
		expect(
			(await sharedLog.log.getHeads().all()).map(
				(entry: { hash: string }) => entry.hash,
			),
		).deep.equal([kept.hash]);
		expect(await sharedLog.log.entryIndex.has(old.entry.hash)).equal(false);
		for (const removed of replacements.entries.slice(0, -1)) {
			expect(await sharedLog.log.entryIndex.has(removed.hash)).equal(false);
		}
		expect(await sharedLog.log.entryIndex.has(kept.hash)).equal(true);
		expect(changes).to.have.length(1);
		expect(changes[0]!.added.map((document) => document.id)).deep.equal([
			"partial-new-1",
			"partial-new-2",
			"partial-new-3",
		]);
		expect(changes[0]!.removed).deep.equal([]);
		expect(wrapper.pendingNativeDeleteCleanup.size).to.be.greaterThan(0);
		expect(wrapper.nativeCommitOwnerships.size).equal(0);

		await store.close();
		expect(durable.status()).equal("closed");
		durableRmManyStub.restore();
		await client!.open(store, {
			args: createOpenArgs(directory!, true),
		});
		expect(store.docs.log.log.length).equal(1);
		expect(
			(await store.docs.log.log.getHeads().all()).map((entry) => entry.hash),
		).deep.equal([kept.hash]);
		expect((await store.docs.get("partial-new-3"))?.name).equal(
			"partial-new-3",
		);
	});

	it("rechecks poison after waiting for tracked writes before trim cleanup", async () => {
		const { store, wrapper, durable } = await openStore();
		const put = await store.docs.put(
			new Document({ id: "cleanup-race", name: "cleanup-race" }),
		);
		const cid = put.entry.hash;
		const bytes = await durable.get(cid);
		if (!bytes) throw new Error("Expected durable test block");
		let releaseWrite!: () => void;
		const writeGate = new Promise<void>((resolve) => {
			releaseWrite = resolve;
		});
		let markWriteStarted!: () => void;
		const writeStarted = new Promise<void>((resolve) => {
			markWriteStarted = resolve;
		});
		const durablePutStub = sinon
			.stub(durable, "putKnownMany")
			.callsFake(async () => {
				markWriteStarted();
				await writeGate;
				throw new Error("tracked mirror failed");
			});
		const durableRmManySpy = sinon.spy(durable, "rmMany");

		try {
			expect(wrapper.putKnownManyColumns([cid], [bytes])).deep.equal([cid]);
			await within(writeStarted, "tracked mirror start");
			const cleanup = wrapper.rmManyAfterNativeDelete([cid]).then(
				() => undefined,
				(error: unknown) => error,
			);
			releaseWrite();
			const failure = await cleanup;
			expect(failure).to.be.instanceOf(NativeDurableCommitError);
			expect(durableRmManySpy.callCount).equal(0);
		} finally {
			releaseWrite();
			durableRmManySpy.restore();
			durablePutStub.restore();
		}
	});

	it("mirrors and then removes rows trimmed inside one native batch", async () => {
		const { store, durable, backbone } = await openStore(true);
		const docs = [
			new Document({ id: "trim-batch-1", name: "trim-batch-1" }),
			new Document({ id: "trim-batch-2", name: "trim-batch-2" }),
			new Document({ id: "trim-batch-3", name: "trim-batch-3" }),
		];
		const appended = await store.docs.putMany(docs, {
			unique: true,
			target: "none",
		});

		expect(appended.entries).to.have.length(docs.length);
		expect(store.docs.log.log.length).equal(1);
		for (const entry of appended.entries.slice(0, -1)) {
			expect(backbone.blocks.has(entry.hash)).equal(false);
			expect(await durable.has(entry.hash)).equal(false);
		}
		const kept = appended.entries.at(-1)!;
		expect(backbone.blocks.has(kept.hash)).equal(true);
		expect(await durable.has(kept.hash)).equal(true);
	});

	it("does not repopulate native after a delayed durable get races poison", async () => {
		const { store, wrapper, durable, backbone } = await openStore();
		const readEntry = await store.docs.put(
			new Document({ id: "delayed-get", name: "delayed-get" }),
		);
		const poisonEntry = await store.docs.put(
			new Document({ id: "get-poison", name: "get-poison" }),
		);
		const readBytes = await durable.get(readEntry.entry.hash);
		const poisonBytes = await durable.get(poisonEntry.entry.hash);
		if (!readBytes || !poisonBytes)
			throw new Error("Expected durable test blocks");
		backbone.blocks.rm(readEntry.entry.hash);
		let releaseRead!: () => void;
		const readGate = new Promise<void>((resolve) => {
			releaseRead = resolve;
		});
		let markReadStarted!: () => void;
		const readStarted = new Promise<void>((resolve) => {
			markReadStarted = resolve;
		});
		const originalGet = durable.get.bind(durable);
		const durableGetStub = sinon
			.stub(durable, "get")
			.callsFake(async (...args: unknown[]) => {
				const [cid] = args as [string];
				if (cid === readEntry.entry.hash) {
					markReadStarted();
					await readGate;
				}
				return originalGet(cid);
			});
		const durablePutStub = sinon
			.stub(durable, "putKnown")
			.rejects(new Error("concurrent get poison"));
		const repopulateSpy = sinon.spy(backbone.blocks, "putKnownManyColumns");

		try {
			const pendingRead = wrapper.get(readEntry.entry.hash);
			await within(readStarted, "delayed durable get");
			const failure = await wrapper
				.putKnown(poisonEntry.entry.hash, poisonBytes)
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			expect(failure).to.be.instanceOf(NativeDurableCommitError);
			releaseRead();
			expect(await pendingRead).deep.equal(readBytes);
			expect(repopulateSpy.callCount).equal(0);
			expect(backbone.blocks.has(readEntry.entry.hash)).equal(false);
		} finally {
			releaseRead();
			repopulateSpy.restore();
			durablePutStub.restore();
			durableGetStub.restore();
		}
	});

	it("does not repopulate native after a delayed durable getMany races poison", async () => {
		const { store, wrapper, durable, backbone } = await openStore();
		const readEntry = await store.docs.put(
			new Document({ id: "delayed-many", name: "delayed-many" }),
		);
		const poisonEntry = await store.docs.put(
			new Document({ id: "many-poison", name: "many-poison" }),
		);
		const readBytes = await durable.get(readEntry.entry.hash);
		const poisonBytes = await durable.get(poisonEntry.entry.hash);
		if (!readBytes || !poisonBytes)
			throw new Error("Expected durable test blocks");
		backbone.blocks.rm(readEntry.entry.hash);
		let releaseRead!: () => void;
		const readGate = new Promise<void>((resolve) => {
			releaseRead = resolve;
		});
		let markReadStarted!: () => void;
		const readStarted = new Promise<void>((resolve) => {
			markReadStarted = resolve;
		});
		const originalGetMany = durable.getMany.bind(durable);
		let getManyCall = 0;
		const durableGetManyStub = sinon
			.stub(durable, "getMany")
			.callsFake(async (...args: unknown[]) => {
				const [cids] = args as [string[]];
				if (getManyCall++ === 0) {
					markReadStarted();
					await readGate;
				}
				return originalGetMany(cids);
			});
		const durablePutStub = sinon
			.stub(durable, "putKnown")
			.rejects(new Error("concurrent getMany poison"));
		const repopulateSpy = sinon.spy(backbone.blocks, "putKnownManyColumns");

		try {
			const pendingRead = wrapper.getMany([readEntry.entry.hash]);
			await within(readStarted, "delayed durable getMany");
			const failure = await wrapper
				.putKnown(poisonEntry.entry.hash, poisonBytes)
				.then(
					() => undefined,
					(error: unknown) => error,
				);
			expect(failure).to.be.instanceOf(NativeDurableCommitError);
			releaseRead();
			expect(await pendingRead).deep.equal([readBytes]);
			expect(durableGetManyStub.callCount).equal(2);
			expect(repopulateSpy.callCount).equal(0);
			expect(backbone.blocks.has(readEntry.entry.hash)).equal(false);
		} finally {
			releaseRead();
			repopulateSpy.restore();
			durablePutStub.restore();
			durableGetManyStub.restore();
		}
	});

	it("keeps staged and pending same-CID cleanup when native re-add throws", async () => {
		const { store, wrapper, durable, backbone } = await openStore();
		const put = await store.docs.put(
			new Document({ id: "failed-readd", name: "failed-readd" }),
		);
		const cid = put.entry.hash;
		const bytes = await durable.get(cid);
		if (!bytes) throw new Error("Expected durable test block");

		const stagedToken = wrapper.beginNativeDeleteCleanup([cid]);
		const stagedFailure = new Error("staged native re-add failed");
		const nativePutStub = sinon
			.stub(backbone.blocks, "putKnown")
			.throws(stagedFailure);
		try {
			expect(
				await wrapper.putKnown(cid, bytes).then(
					() => undefined,
					(error: unknown) => error,
				),
			).equal(stagedFailure);
			expect(
				wrapper.stagedNativeDeleteCleanups.get(stagedToken).has(cid),
			).equal(true);
			expect(wrapper.nativeDeleteTombstones.has(cid)).equal(true);
		} finally {
			nativePutStub.restore();
			wrapper.cancelNativeDeleteCleanup(stagedToken);
		}

		const retryStub = sinon
			.stub(wrapper, "retryNativeDeleteCleanup")
			.resolves();
		await wrapper.rmManyAfterNativeDelete([cid]);
		retryStub.restore();
		expect(wrapper.pendingNativeDeleteCleanup.has(cid)).equal(true);
		const pendingFailure = new Error("pending native re-add failed");
		const nativePutManyStub = sinon
			.stub(backbone.blocks, "putKnownMany")
			.throws(pendingFailure);
		try {
			expect(
				await wrapper.putKnownMany([[cid, bytes]]).then(
					() => undefined,
					(error: unknown) => error,
				),
			).equal(pendingFailure);
			expect(wrapper.pendingNativeDeleteCleanup.has(cid)).equal(true);
			expect(wrapper.nativeDeleteTombstones.has(cid)).equal(true);
		} finally {
			nativePutManyStub.restore();
			await wrapper.retryNativeDeleteCleanup();
		}
	});

	it("guards join/index mutations and resets poison only after same-object reopen", async () => {
		const { store, sharedLog, wrapper, durable } = await openStore();
		const durablePutStub = sinon
			.stub(durable, "putKnown")
			.rejects(new Error("reopen poison"));
		const failure = await store.docs
			.put(new Document({ id: "poison-reopen", name: "poison-reopen" }))
			.then(
				() => undefined,
				(error: unknown) => error,
			);
		expect(failure).to.be.instanceOf(NativeDurableCommitError);
		const failedHash = (failure as NativeDurableCommitError).committedCids[0]!;
		expect(await store.docs.get("poison-reopen")).equal(undefined);
		expect(sharedLog.log.length).equal(0);
		expect(
			sharedLog._residentEntryCoordinatesByHash?.has(failedHash) ?? false,
		).equal(false);
		expect(await durable.has(failedHash)).equal(false);

		const guardedMutations = [
			["shared join", () => sharedLog.join([])],
			["lower join", () => sharedLog.log.join([])],
			["lower trim", () => sharedLog.log.trim()],
			["lower delete", () => sharedLog.log.delete("missing")],
		];
		for (const [name, mutate] of guardedMutations) {
			expect(
				await (mutate as () => Promise<unknown>)().then(
					() => undefined,
					(error: unknown) => error,
				),
				name as string,
			).equal(failure);
		}

		const oldRemote = sharedLog.remoteBlocks;
		const durableStopSpy = sinon.spy(durable, "stop");
		const rangeStopSpy = sinon.spy(sharedLog._replicationRangeIndex, "stop");
		const coordinateStopSpy = sinon.spy(
			sharedLog._entryCoordinatesIndex,
			"stop",
		);
		const logCloseSpy = sinon.spy(sharedLog.log, "close");
		durablePutStub.restore();
		try {
			const closeFailure = await store.close().then(
				() => undefined,
				(error: unknown) => error,
			);
			expect(closeFailure).equal(failure);
			expect(durableStopSpy.calledOnce).equal(true);
			expect(rangeStopSpy.calledOnce).equal(true);
			expect(coordinateStopSpy.calledOnce).equal(true);
			expect(logCloseSpy.calledOnce).equal(true);
			expect(oldRemote.status).equal("closed");
			expect(durable.status()).equal("closed");
			// Program parents retain their own open flag when a child close rejects.
			// Retry through the same parent operation so Handler can resume every
			// committed child release with its original owner identity.
			await store.close();
			expect(store.closed).equal(true);
			expect(durableStopSpy.calledOnce).equal(true);

			const reopened = await client!.open(store, {
				args: createOpenArgs(directory!),
			});
			expect(reopened).equal(store);
			const freshWrapper = sharedLog.remoteBlocks.localStore;
			expect(freshWrapper).not.equal(wrapper);
			expect(sharedLog._nativeDurableCommitFailure).equal(undefined);
			expect(await store.docs.get("poison-reopen")).equal(undefined);
			expect(sharedLog.log.length).equal(0);
			expect(await sharedLog.remoteBlocks.localStore.has(failedHash)).equal(
				false,
			);
			await store.docs.put(
				new Document({ id: "after-reopen", name: "after-reopen" }),
			);
		} finally {
			logCloseSpy.restore();
			coordinateStopSpy.restore();
			rangeStopSpy.restore();
			durableStopSpy.restore();
		}
	});

	it("does not let a queued trim delete a same-CID re-add", async () => {
		const { store, wrapper, durable, backbone } = await openStore();
		const first = await store.docs.put(
			new Document({ id: "re-add", name: "re-add" }),
		);
		const cid = first.entry.hash;
		const bytes = await durable.get(cid);
		if (!bytes) {
			throw new Error("Expected the block in durable storage");
		}
		backbone.blocks.rm(cid);
		const originalRmMany = durable.rmMany.bind(durable);
		let releaseRemove!: () => void;
		const removeGate = new Promise<void>((resolve) => {
			releaseRemove = resolve;
		});
		let markRemoveStarted!: () => void;
		const removeStarted = new Promise<void>((resolve) => {
			markRemoveStarted = resolve;
		});
		const durableRmManyStub = sinon
			.stub(durable, "rmMany")
			.callsFake(async (...args: unknown[]) => {
				const [cids] = args as [string[]];
				markRemoveStarted();
				await removeGate;
				return originalRmMany(cids);
			});

		try {
			const cleanup = wrapper.rmManyAfterNativeDelete([cid]);
			await within(removeStarted, "same-CID durable trim");
			expect(wrapper.putKnownManyColumns([cid], [bytes])).deep.equal([cid]);
			expect(backbone.blocks.has(cid)).equal(true);

			releaseRemove();
			await cleanup;
			await wrapper.waitForDurableWrites();
			await wrapper.completeCommittedNativeDeleteCleanup([cid]);
			expect(durableRmManyStub.callCount).equal(1);
			expect(backbone.blocks.has(cid)).equal(true);
			expect(await durable.has(cid)).equal(true);
			expect(await wrapper.has(cid)).equal(true);
		} finally {
			releaseRemove();
			durableRmManyStub.restore();
		}
	});
});
