import { serialize } from "@dao-xyz/borsh";
import { calculateRawCid } from "@peerbit/blocks-interface";
import {
	Ed25519Keypair,
	PreHash,
	sha256Sync,
	toHexString,
} from "@peerbit/crypto";
import { createDatabase } from "@peerbit/indexer-sqlite3";
import {
	type NativeDurabilityLock,
	acquireNativeDurabilityNodeLock,
} from "@peerbit/native-backbone";
import { expect } from "chai";
import * as nodeCrypto from "node:crypto";
import {
	copyFile,
	mkdir,
	mkdtemp,
	realpath,
	rm,
	symlink,
} from "node:fs/promises";
import * as nodeFs from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import * as nodePath from "node:path";
import {
	type CanonicalCustodyHandoffManifest,
	createCustodyHandoffManifestV1,
} from "../src/custody-handoff-codec.js";
import {
	type CustodyRecordNodeModules,
	type CustodyRecordNodePersistenceDependencies,
	type CustodyRecordNodePersistenceFacts,
	openNodeCustodyRecordStore,
} from "../src/custody-record-persistence.js";
import {
	type CustodyRecordPersistence,
	CustodyRecordStore,
} from "../src/custody-store.js";

const digest = (byte: number) => byte.toString(16).padStart(2, "0").repeat(32);
const databaseFile = (namespace: string) => join(namespace, "db.sqlite");
type SqliteDatabase = Awaited<ReturnType<typeof createDatabase>>;

type Opened = Readonly<{
	store: CustodyRecordStore;
	persistence: CustodyRecordPersistence;
	facts: CustodyRecordNodePersistenceFacts;
}>;

type Gate = Readonly<{
	entered: Promise<void>;
	release(): void;
}>;

type CheckpointFault =
	| Readonly<{ kind: "busy" }>
	| Readonly<{ kind: "error"; error: Error }>;

class SqliteFaultController {
	readonly readPrefixBounds: number[] = [];
	checkpointCalls = 0;
	private checkpointFault?: CheckpointFault;
	private readTransform?: (rows: unknown) => unknown;
	private checkpointGate?: {
		entered(): void;
		wait: Promise<void>;
	};

	failNextCheckpoint(fault: CheckpointFault) {
		this.checkpointFault = fault;
	}

	transformNextRead(transform: (rows: unknown) => unknown) {
		this.readTransform = transform;
	}

	async read<T>(fallback: () => Promise<T>): Promise<T> {
		const rows = await fallback();
		const transform = this.readTransform;
		this.readTransform = undefined;
		return (transform ? transform(rows) : rows) as T;
	}

	blockNextCheckpoint(): Gate {
		let entered!: () => void;
		let release!: () => void;
		const enteredPromise = new Promise<void>((resolve) => {
			entered = resolve;
		});
		const wait = new Promise<void>((resolve) => {
			release = resolve;
		});
		this.checkpointGate = { entered, wait };
		return { entered: enteredPromise, release };
	}

	async checkpoint<T>(fallback: () => Promise<T>): Promise<T> {
		this.checkpointCalls++;
		const gate = this.checkpointGate;
		if (gate) {
			this.checkpointGate = undefined;
			gate.entered();
			await gate.wait;
		}
		const fault = this.checkpointFault;
		this.checkpointFault = undefined;
		if (fault?.kind === "error") throw fault.error;
		if (fault?.kind === "busy") {
			return [{ busy: 1n, log: 1n, checkpointed: 0n }] as T;
		}
		return fallback();
	}
}

const instrumentedDependencies = (
	controller: SqliteFaultController,
): CustodyRecordNodePersistenceDependencies => ({
	async loadNodeModules() {
		const native = await import("@peerbit/native-backbone");
		return {
			fs: nodeFs,
			path: nodePath,
			crypto: nodeCrypto,
			native,
			sqlite: {
				async createDatabase(...args: Parameters<typeof createDatabase>) {
					const database = await createDatabase(...args);
					return new Proxy(database, {
						get(target, property) {
							if (property === "prepare") {
								return async (sql: string, id?: string) => {
									const statement = await target.prepare(sql, id);
									if (
										id !== "custody-record-checkpoint" &&
										id !== "custody-record-read"
									) {
										return statement;
									}
									return new Proxy(statement, {
										get(statementTarget, statementProperty) {
											if (statementProperty === "all") {
												return async (values: unknown[]) => {
													if (id === "custody-record-read") {
														controller.readPrefixBounds.push(
															values[0] as number,
														);
													}
													const fallback = async () =>
														statementTarget.all(values as never);
													return id === "custody-record-checkpoint"
														? controller.checkpoint(fallback)
														: controller.read(fallback);
												};
											}
											const value = Reflect.get(
												statementTarget,
												statementProperty,
											);
											return typeof value === "function"
												? value.bind(statementTarget)
												: value;
										},
									});
								};
							}
							const value = Reflect.get(target, property);
							return typeof value === "function" ? value.bind(target) : value;
						},
					});
				},
			},
		} as unknown as CustodyRecordNodeModules;
	},
});

const faultInjectedDependencies = (options: {
	fs?: CustodyRecordNodeModules["fs"];
	wrapDatabase?: (database: SqliteDatabase) => SqliteDatabase;
	wrapLock?: (lock: NativeDurabilityLock) => NativeDurabilityLock;
}): CustodyRecordNodePersistenceDependencies => ({
	async loadNodeModules() {
		const native = await import("@peerbit/native-backbone");
		return {
			fs: options.fs ?? nodeFs,
			path: nodePath,
			crypto: nodeCrypto,
			native: {
				async acquireNativeDurabilityNodeLock(namespace: string) {
					const lock = await native.acquireNativeDurabilityNodeLock(namespace);
					return options.wrapLock?.(lock) ?? lock;
				},
			},
			sqlite: {
				async createDatabase(...args: Parameters<typeof createDatabase>) {
					const database = await createDatabase(...args);
					return options.wrapDatabase?.(database) ?? database;
				},
			},
		} as unknown as CustodyRecordNodeModules;
	},
});

const proxyDatabase = (
	database: SqliteDatabase,
	overrides: Partial<Pick<SqliteDatabase, "exec" | "close">>,
): SqliteDatabase =>
	new Proxy(database, {
		get(target, property) {
			const override = overrides[property as "exec" | "close"];
			if (override) return override;
			const value = Reflect.get(target, property);
			return typeof value === "function" ? value.bind(target) : value;
		},
	});

const withDatabase = async <T>(
	namespace: string,
	operation: (
		database: Awaited<ReturnType<typeof createDatabase>>,
	) => Promise<T>,
): Promise<T> => {
	const database = await createDatabase(namespace, {
		pragmas: {
			synchronous: "FULL",
			lockingMode: "EXCLUSIVE",
			tempStore: "MEMORY",
		},
	});
	await database.open();
	try {
		return await operation(database);
	} finally {
		await database.close();
	}
};

describe("custody record persistence", function () {
	this.timeout(60_000);

	const directories = new Set<string>();
	const stores = new Set<CustodyRecordStore>();
	let source: Ed25519Keypair;
	let destination: Ed25519Keypair;
	let sourceBytes: Uint8Array;
	let destinationBytes: Uint8Array;
	let entryHash: string;

	before(async () => {
		[source, destination] = await Promise.all([
			Ed25519Keypair.create(),
			Ed25519Keypair.create(),
		]);
		sourceBytes = serialize(source.publicKey);
		destinationBytes = serialize(destination.publicKey);
		entryHash = (await calculateRawCid(new Uint8Array([1, 2, 3]))).cid;
	});

	beforeEach(function () {
		if (process.platform === "win32") this.skip();
	});

	afterEach(async () => {
		await Promise.allSettled([...stores].map((store) => store.close()));
		stores.clear();
		await Promise.all(
			[...directories].map((directory) =>
				rm(directory, { recursive: true, force: true }),
			),
		);
		directories.clear();
	});

	const temporaryDirectory = async (suffix = "node"): Promise<string> => {
		const parent = await mkdtemp(
			join(tmpdir(), `peerbit-custody-record-${suffix}-`),
		);
		directories.add(parent);
		const directory = join(parent, "peerbit-node");
		await mkdir(directory);
		return directory;
	};

	const open = async (
		nodeDirectory: string,
		options: {
			logId?: Uint8Array;
			localPublicKey?: Uint8Array;
			role?: "source" | "destination";
			maxFrameBytes?: number;
			dependencies?: CustodyRecordNodePersistenceDependencies;
		} = {},
	): Promise<Opened> => {
		let persistence: CustodyRecordPersistence | undefined;
		let facts: CustodyRecordNodePersistenceFacts | undefined;
		const supplied = options.dependencies;
		const store = await openNodeCustodyRecordStore(
			{
				nodeDirectory,
				logId: options.logId ?? new Uint8Array([1, 2, 3]),
				localPublicKey: options.localPublicKey ?? sourceBytes,
				role: options.role ?? "source",
				...(options.maxFrameBytes
					? { limits: { maxFrameBytes: options.maxFrameBytes } }
					: {}),
			},
			{
				...supplied,
				onPersistenceCreated(value, valueFacts) {
					persistence = value;
					facts = valueFacts;
					supplied?.onPersistenceCreated?.(value, valueFacts);
				},
			},
		);
		stores.add(store);
		if (!persistence || !facts) {
			throw new Error("Custody persistence creation hook was not called");
		}
		return { store, persistence, facts };
	};

	const close = async (opened: Opened) => {
		await opened.store.close();
		stores.delete(opened.store);
	};

	const manifest = async (
		input: {
			logId?: Uint8Array;
			sourceKey?: Ed25519Keypair;
			attempt?: number;
		} = {},
	): Promise<CanonicalCustodyHandoffManifest> => {
		const sourceKey = input.sourceKey ?? source;
		return createCustodyHandoffManifestV1(
			{
				logId: input.logId ?? new Uint8Array([1, 2, 3]),
				entryHash,
				entryByteLength: 3n,
				source: sourceKey.publicKey,
				destination: destination.publicKey,
				visit: {
					viewId: digest(10),
					planDigest: digest(11),
					installSequence: 7n,
					taskOrdinal: 2,
					resolution: "u32",
					hashNumber: 99,
				},
				ownerPlanId: digest(12),
				attemptGeneration: new Uint8Array(32).fill(input.attempt ?? 1),
			},
			sourceKey.signer(PreHash.SHA_256),
		);
	};

	it("round-trips point rows across reopen with a stable namespace and advancing writer fence", async () => {
		const directory = await temporaryDirectory("roundtrip");
		const value = await manifest();
		const key = value.moveKey;
		const first = await open(directory);
		await first.store.prepareSource(0n, value.bytes);
		const firstRead = (await first.persistence.read(key, "a", 16 * 1024))!;
		const expected = new Uint8Array(firstRead);
		firstRead.fill(0);
		const firstFacts = first.facts;
		await close(first);

		const second = await open(directory);
		expect(await second.persistence.read(key, "a", 16 * 1024)).to.deep.equal(
			expected,
		);
		expect(second.facts.namespace).to.equal(firstFacts.namespace);
		expect(second.facts.namespaceEpoch).to.equal(firstFacts.namespaceEpoch);
		expect(second.facts.domainId).to.equal(firstFacts.domainId);
		expect(second.facts.writerEpoch).to.equal(firstFacts.writerEpoch + 1n);
		expect(second.facts.writerOwner).to.not.equal(firstFacts.writerOwner);
		await close(second);
	});

	it("uses intrinsic byte bounds for SQLite reads and public writes", async () => {
		const directory = await temporaryDirectory("intrinsic-bytes");
		const controller = new SqliteFaultController();
		const opened = await open(directory, {
			dependencies: instrumentedDependencies(controller),
		});
		const value = await manifest();
		const key = value.moveKey;
		await opened.store.prepareSource(0n, value.bytes);

		let shadowReads = 0;
		controller.transformNextRead((value) => {
			const rows = value as Record<string, unknown>[];
			const oversized = new Uint8Array(128);
			Object.defineProperty(oversized, "byteLength", {
				get() {
					shadowReads++;
					return 1;
				},
			});
			return [
				{
					...rows[0],
					frame_prefix: oversized,
					frame_bytes: 1n,
				},
			];
		});
		await expect(
			opened.persistence.read(key, "a", 16 * 1024),
		).to.be.rejectedWith("Invalid bounded custody record frame");
		expect(shadowReads).to.equal(0);

		let prototypeReads = 0;
		const proxied = new Proxy(new Uint8Array([4]), {
			getPrototypeOf() {
				prototypeReads++;
				throw new Error("unexpected prototype hook");
			},
		});
		await expect(
			opened.persistence.write(key, "b", proxied),
		).to.be.rejectedWith("Invalid custody persistence write");
		expect(prototypeReads).to.equal(0);
		await close(opened);
	});

	it("uses exact log, local-key, and role namespaces without aliasing them", async () => {
		const directory = await temporaryDirectory("namespaces");
		const canonicalRoot = await realpath(directory);
		const logA = new Uint8Array([1, 2, 3]);
		const logB = new Uint8Array([1, 2, 4]);
		const cases = [
			{ logId: logA, localPublicKey: sourceBytes, role: "source" as const },
			{ logId: logB, localPublicKey: sourceBytes, role: "source" as const },
			{
				logId: logA,
				localPublicKey: destinationBytes,
				role: "source" as const,
			},
			{
				logId: logA,
				localPublicKey: sourceBytes,
				role: "destination" as const,
			},
		];
		const opened = await Promise.all(
			cases.map((value) => open(directory, value)),
		);
		for (let index = 0; index < cases.length; index++) {
			const value = cases[index]!;
			expect(opened[index]!.facts.namespace).to.equal(
				join(
					canonicalRoot,
					"custody-records-v1",
					toHexString(sha256Sync(value.logId)),
					toHexString(sha256Sync(value.localPublicKey)),
					value.role,
				),
			);
		}
		expect(
			new Set(opened.map((value) => value.facts.namespace)),
		).to.have.property("size", cases.length);
		await Promise.all(opened.map(close));
	});

	it("rejects a non-canonical local public key before creating a namespace", async () => {
		const directory = await temporaryDirectory("invalid-key");
		await expect(
			open(directory, { localPublicKey: new Uint8Array([0]) }),
		).to.be.rejectedWith("canonical public key");
		await expect(
			nodeFs.stat(join(directory, "custody-records-v1")),
		).to.be.rejectedWith("ENOENT");
	});

	it("retains undefined directory sync and close rejections", async () => {
		const directory = await temporaryDirectory("directory-undefined");
		let injected = false;
		const fs = {
			realpath: nodeFs.realpath,
			stat: nodeFs.stat,
			lstat: nodeFs.lstat,
			mkdir: nodeFs.mkdir,
			async open(path: string, flags: "r") {
				const handle = await nodeFs.open(path, flags);
				if (injected) return handle;
				injected = true;
				return {
					sync: () => Promise.reject(undefined),
					async close() {
						await handle.close();
						return Promise.reject(undefined);
					},
				};
			},
		} as unknown as CustodyRecordNodeModules["fs"];
		const [outcome] = await Promise.allSettled([
			openNodeCustodyRecordStore(
				{
					nodeDirectory: directory,
					logId: new Uint8Array([1, 2, 3]),
					localPublicKey: sourceBytes,
					role: "source",
				},
				faultInjectedDependencies({ fs }),
			),
		]);
		expect(injected).to.equal(true);
		expect(outcome!.status).to.equal("rejected");
		if (outcome!.status === "rejected") {
			expect(outcome.reason).to.be.instanceOf(AggregateError);
			expect((outcome.reason as AggregateError).errors).to.deep.equal([
				undefined,
				undefined,
			]);
		}
	});

	it("retains an undefined rollback rejection after commit failure", async () => {
		const directory = await temporaryDirectory("rollback-undefined");
		const commitError = new Error("injected commit failure");
		const dependencies = faultInjectedDependencies({
			wrapDatabase(database) {
				let commitFailed = false;
				return proxyDatabase(database, {
					async exec(sql: string) {
						if (sql === "COMMIT") {
							commitFailed = true;
							return Promise.reject(commitError);
						}
						if (sql === "ROLLBACK" && commitFailed) {
							return Promise.reject(undefined);
						}
						return database.exec(sql);
					},
				});
			},
		});
		const [outcome] = await Promise.allSettled([
			openNodeCustodyRecordStore(
				{
					nodeDirectory: directory,
					logId: new Uint8Array([1, 2, 3]),
					localPublicKey: sourceBytes,
					role: "source",
				},
				dependencies,
			),
		]);
		expect(outcome!.status).to.equal("rejected");
		if (outcome!.status === "rejected") {
			expect(outcome.reason).to.be.instanceOf(AggregateError);
			expect((outcome.reason as AggregateError).errors).to.deep.equal([
				commitError,
				undefined,
			]);
		}
	});

	it("aggregates undefined database and lock close rejections", async () => {
		const directory = await temporaryDirectory("close-undefined");
		const dependencies = faultInjectedDependencies({
			wrapDatabase: (database) =>
				proxyDatabase(database, {
					async close() {
						await database.close();
						return Promise.reject(undefined);
					},
				}),
			wrapLock: (lock) => ({
				assertHeld: () => lock.assertHeld(),
				runWhileHeld: (operation) => lock.runWhileHeld(operation),
				async close() {
					await lock.close();
					return Promise.reject(undefined);
				},
			}),
		});
		const opened = await open(directory, { dependencies });
		const [outcome] = await Promise.allSettled([opened.store.close()]);
		stores.delete(opened.store);
		expect(outcome!.status).to.equal("rejected");
		if (outcome!.status === "rejected") {
			expect(outcome.reason).to.be.instanceOf(AggregateError);
			expect((outcome.reason as AggregateError).errors).to.deep.equal([
				undefined,
				undefined,
			]);
		}
	});

	it("aggregates an undefined close rejection after open fails", async () => {
		const directory = await temporaryDirectory("open-close-undefined");
		const primary = new Error("injected open failure");
		const dependencies = faultInjectedDependencies({
			wrapDatabase: (database) =>
				proxyDatabase(database, {
					async close() {
						await database.close();
						return Promise.reject(undefined);
					},
				}),
		});
		const [outcome] = await Promise.allSettled([
			openNodeCustodyRecordStore(
				{
					nodeDirectory: directory,
					logId: new Uint8Array([1, 2, 3]),
					localPublicKey: sourceBytes,
					role: "source",
				},
				{
					...dependencies,
					onPersistenceCreated() {
						throw primary;
					},
				},
			),
		]);
		expect(outcome!.status).to.equal("rejected");
		if (outcome!.status === "rejected") {
			expect(outcome.reason).to.be.instanceOf(AggregateError);
			expect((outcome.reason as AggregateError).errors).to.deep.equal([
				primary,
				undefined,
			]);
		}
	});

	it("excludes the same canonical namespace and its node-directory alias until close", async () => {
		const directory = await temporaryDirectory("lease");
		const alias = join(dirname(directory), "peerbit-node-alias");
		await symlink(directory, alias, "dir");
		const first = await open(directory);

		await expect(open(directory)).to.be.rejectedWith("already open");
		await expect(open(alias)).to.be.rejectedWith("already open");
		await close(first);

		const reopened = await open(alias);
		expect(reopened.facts.namespace).to.equal(first.facts.namespace);
		await close(reopened);
	});

	it("rejects role, log, and local-key binding mismatches before any row is written", async () => {
		const directory = await temporaryDirectory("binding");
		const opened = await open(directory);
		const correct = await manifest();
		const otherSource = await Ed25519Keypair.create();
		const wrongKey = await manifest({ sourceKey: otherSource });
		const wrongLog = await manifest({ logId: new Uint8Array([9, 9, 9]) });

		await expect(
			opened.store.beginDestination(0n, correct.bytes),
		).to.be.rejectedWith("role");
		await expect(
			opened.store.prepareSource(0n, wrongKey.bytes),
		).to.be.rejectedWith("namespace identity");
		await expect(
			opened.store.prepareSource(0n, wrongLog.bytes),
		).to.be.rejectedWith("log");
		expect(
			await opened.persistence.read(correct.moveKey, "a", 16 * 1024),
		).to.equal(undefined);
		expect(
			await opened.persistence.read(correct.moveKey, "b", 16 * 1024),
		).to.equal(undefined);

		const prepared = await opened.store.prepareSource(0n, correct.bytes);
		expect(prepared.durableCommit).to.deep.include({
			moveKey: correct.moveKey,
			revision: 2n,
			state: "source-prepared",
		});
		await close(opened);

		const destinationStore = await open(directory, {
			localPublicKey: destinationBytes,
			role: "destination",
		});
		await expect(
			destinationStore.store.prepareSource(0n, correct.bytes),
		).to.be.rejectedWith("role");
		expect(
			await destinationStore.persistence.read(correct.moveKey, "a", 16 * 1024),
		).to.equal(undefined);
		await close(destinationStore);
	});

	it("rejects an oversized database row from a cap-plus-one SQL prefix", async () => {
		const directory = await temporaryDirectory("oversized");
		const key = digest(41);
		const first = await open(directory, { maxFrameBytes: 64 });
		const namespace = first.facts.namespace;
		await close(first);

		await withDatabase(namespace, async (database) => {
			await database.exec(
				`INSERT INTO custody_records (move_key, slot, frame, domain_id, writer_epoch, writer_owner) SELECT '${key}', 'a', zeroblob(16384), domain_id, writer_epoch, writer_owner FROM custody_meta`,
			);
		});

		const controller = new SqliteFaultController();
		const reopened = await open(directory, {
			maxFrameBytes: 64,
			dependencies: instrumentedDependencies(controller),
		});
		await expect(reopened.persistence.read(key, "a", 64)).to.be.rejectedWith(
			"exceeds read byte bound",
		);
		expect(controller.readPrefixBounds).to.deep.equal([65]);
		await expect(reopened.store.read(key)).to.be.rejectedWith(
			"Failed to read every custody record generation",
		);
		expect(controller.readPrefixBounds.slice(-2)).to.deep.equal([65, 65]);
		await close(reopened);
	});

	it("fails closed on missing and corrupt metadata and releases the lease", async () => {
		for (const mutation of [
			"DELETE FROM custody_meta",
			"UPDATE custody_meta SET namespace_epoch = zeroblob(32)",
		]) {
			const directory = await temporaryDirectory("bad-meta");
			const first = await open(directory);
			const namespace = first.facts.namespace;
			await close(first);
			await withDatabase(namespace, async (database) => {
				await database.exec(mutation);
			});

			await expect(open(directory)).to.be.rejected;
			const lock = await acquireNativeDurabilityNodeLock(namespace);
			await lock.close();
		}
	});

	it("does not adopt an empty database first observed while acquiring the namespace lock", async () => {
		const directory = await temporaryDirectory("genesis-race");
		let lockedNamespace: string | undefined;
		let materialized = false;
		const native = await import("@peerbit/native-backbone");
		const dependencies: CustodyRecordNodePersistenceDependencies = {
			async loadNodeModules() {
				return {
					fs: nodeFs,
					path: nodePath,
					crypto: nodeCrypto,
					native: {
						async acquireNativeDurabilityNodeLock(namespace: string) {
							lockedNamespace = namespace;
							// The adapter has completed its pre-lock lstat before it calls
							// this injected acquisition. Materialize only the empty main DB,
							// then return real crash-released exclusive ownership.
							const foreign = await createDatabase(namespace, {
								pragmas: {
									synchronous: "FULL",
									lockingMode: "EXCLUSIVE",
									tempStore: "MEMORY",
								},
							});
							await foreign.open();
							await foreign.close();
							materialized = true;
							return native.acquireNativeDurabilityNodeLock(namespace);
						},
					},
					sqlite: { createDatabase },
				} as unknown as CustodyRecordNodeModules;
			},
		};

		await expect(
			openNodeCustodyRecordStore(
				{
					nodeDirectory: directory,
					logId: new Uint8Array([1, 2, 3]),
					localPublicKey: sourceBytes,
					role: "source",
				},
				dependencies,
			),
		).to.be.rejectedWith("genesis is not a new empty database");
		expect(materialized).to.equal(true);
		expect(lockedNamespace).to.be.a("string");

		const lock = await acquireNativeDurabilityNodeLock(lockedNamespace!);
		await lock.close();
		await withDatabase(lockedNamespace!, async (database) => {
			const statement = await database.prepare(
				"SELECT count(*) AS count FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
			);
			const rows = (await statement.all([])) as { count: bigint }[];
			expect(rows).to.deep.equal([{ count: 0n }]);
		});
	});

	it("rejects transplanted namespace metadata, releases its lease, and accepts the restored database", async () => {
		const directory = await temporaryDirectory("transplant");
		const first = await open(directory, { logId: new Uint8Array([1]) });
		const second = await open(directory, { logId: new Uint8Array([2]) });
		const firstDatabase = databaseFile(first.facts.namespace);
		const secondDatabase = databaseFile(second.facts.namespace);
		const backup = join(dirname(directory), "second-db-backup.sqlite");
		await Promise.all([close(first), close(second)]);
		await copyFile(secondDatabase, backup);
		await copyFile(firstDatabase, secondDatabase);

		await expect(
			open(directory, { logId: new Uint8Array([2]) }),
		).to.be.rejectedWith("namespace binding mismatch");
		const lock: NativeDurabilityLock = await acquireNativeDurabilityNodeLock(
			second.facts.namespace,
		);
		await lock.close();

		await copyFile(backup, secondDatabase);
		const restored = await open(directory, { logId: new Uint8Array([2]) });
		expect(restored.facts.namespaceEpoch).to.equal(second.facts.namespaceEpoch);
		await close(restored);
	});

	it("falls back from a corrupt latest slot and rejects a valid frame transplanted to the wrong slot", async () => {
		const directory = await temporaryDirectory("frames");
		const value = await manifest();
		const first = await open(directory);
		await first.store.prepareSource(0n, value.bytes);
		const namespace = first.facts.namespace;
		await close(first);

		await withDatabase(namespace, async (database) => {
			await database.exec(
				`UPDATE custody_records SET frame = x'010203' WHERE move_key = '${value.moveKey}' AND slot = 'a'`,
			);
		});
		const fallback = await open(directory);
		expect((await fallback.store.read(value.moveKey)).snapshot).to.deep.equal({
			moveKey: value.moveKey,
			revision: 1n,
			durability: "strict",
			state: "absent",
		});
		await close(fallback);

		await withDatabase(namespace, async (database) => {
			await database.exec(
				`UPDATE custody_records SET frame = (SELECT frame FROM custody_records WHERE move_key = '${value.moveKey}' AND slot = 'b') WHERE move_key = '${value.moveKey}' AND slot = 'a'; DELETE FROM custody_records WHERE move_key = '${value.moveKey}' AND slot = 'b'`,
			);
		});
		const wrongSlot = await open(directory);
		await expect(wrongSlot.store.read(value.moveKey)).to.be.rejectedWith(
			"No valid custody record generation remains",
		);
		await close(wrongSlot);
	});

	it("poisons on checkpoint errors and incomplete busy checkpoints", async () => {
		for (const fault of [
			{ kind: "error", error: new Error("injected checkpoint failure") },
			{ kind: "busy" },
		] satisfies CheckpointFault[]) {
			const directory = await temporaryDirectory("checkpoint");
			const controller = new SqliteFaultController();
			const opened = await open(directory, {
				dependencies: instrumentedDependencies(controller),
			});
			const value = await manifest({ attempt: controller.checkpointCalls + 1 });
			controller.failNextCheckpoint(fault);
			await expect(opened.store.prepareSource(0n, value.bytes)).to.be.rejected;
			await expect(opened.store.read(value.moveKey)).to.be.rejectedWith(
				"Custody record store is poisoned",
			);
			await expect(opened.store.close()).to.be.rejectedWith(
				"Custody record store is poisoned",
			);
			stores.delete(opened.store);
			const lock = await acquireNativeDurabilityNodeLock(
				opened.facts.namespace,
			);
			await lock.close();
		}
	});

	it("fences post-close admission and drains a checkpoint already admitted through the store", async () => {
		const directory = await temporaryDirectory("drain");
		const controller = new SqliteFaultController();
		const opened = await open(directory, {
			dependencies: instrumentedDependencies(controller),
		});
		const value = await manifest();
		const gate = controller.blockNextCheckpoint();
		const preparing = opened.store.prepareSource(0n, value.bytes);
		await gate.entered;

		let closed = false;
		const closePromise = opened.store.close();
		const closing = closePromise.then(() => {
			closed = true;
		});
		try {
			expect(opened.store.close()).to.equal(closePromise);
			await expect(opened.store.read(value.moveKey)).to.be.rejectedWith(
				"Custody record store is closing",
			);
			await Promise.resolve();
			expect(closed).to.equal(false);
		} finally {
			gate.release();
		}
		expect((await preparing).snapshot.state).to.equal("source-prepared");
		await closing;
		stores.delete(opened.store);
		expect(() => opened.persistence.read(value.moveKey, "a", 64)).to.throw(
			"Custody SQLite persistence is closing",
		);
		expect(() =>
			opened.persistence.write(value.moveKey, "a", new Uint8Array([1])),
		).to.throw("Custody SQLite persistence is closing");
		expect(() =>
			opened.persistence.durableBarrier!(value.moveKey, "a"),
		).to.throw("Custody SQLite persistence is closing");

		const reopened = await open(directory);
		expect((await reopened.store.read(value.moveKey)).snapshot.state).to.equal(
			"source-prepared",
		);
		await close(reopened);
	});
});
