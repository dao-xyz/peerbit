import { serialize } from "@dao-xyz/borsh";
import { calculateRawCid } from "@peerbit/blocks-interface";
import { Ed25519Keypair, PreHash } from "@peerbit/crypto";
import { createDatabase } from "@peerbit/indexer-sqlite3";
import { expect } from "chai";
import * as nodeCrypto from "node:crypto";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import * as nodeFs from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as nodePath from "node:path";
import {
	CUSTODY_HANDOFF_PROFILE_ID,
	CUSTODY_HANDOFF_PROFILE_MASK,
	type CanonicalCustodyHandoffManifest,
	type CanonicalCustodyHandoffReceipt,
	createCustodyHandoffManifestV1,
	createCustodyHandoffReceiptV1,
} from "../src/custody-handoff-codec.js";
import {
	type CustodyRecordNodeModules,
	type CustodyRecordNodePersistenceDependencies,
	type CustodyRecordNodePersistenceFacts,
	openNodeCustodyRecordStore,
} from "../src/custody-record-persistence.js";
import {
	type CustodyRecordCatalogCandidate,
	type CustodyRecordCatalogCursor,
	type CustodyRecordCatalogFence,
	type CustodyRecordCatalogPage,
	type CustodyRecordPersistence,
	type CustodyRecordState,
	CustodyRecordStore,
	MemoryCustodyRecordPersistence,
	issueCustodyDestinationPinEvidenceForTest,
} from "../src/custody-store.js";
import { MAX_U64 } from "../src/integers.js";

const digest = (byte: number) => byte.toString(16).padStart(2, "0").repeat(32);

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

class SqliteFaultController {
	private readonly statementFailures = new Map<
		string,
		{ remaining: number; error: Error }
	>();
	private directorySyncFailure?: { remaining: number; error: Error };
	private readonly statementGates = new Map<
		string,
		{ entered: () => void; wait: Promise<void> }
	>();

	failNextStatement(
		id: string,
		error = new Error(`injected ${id} failure`),
		afterSuccessfulCalls = 0,
	) {
		this.statementFailures.set(id, {
			remaining: afterSuccessfulCalls,
			error,
		});
	}

	failNextDirectorySync(
		error = new Error("injected custody catalog directory sync failure"),
		afterSuccessfulCalls = 0,
	) {
		this.directorySyncFailure = {
			remaining: afterSuccessfulCalls,
			error,
		};
	}

	blockNextStatement(id: string): {
		entered: Promise<void>;
		release(): void;
	} {
		let entered!: () => void;
		let release!: () => void;
		const enteredPromise = new Promise<void>((resolve) => {
			entered = resolve;
		});
		const wait = new Promise<void>((resolve) => {
			release = resolve;
		});
		this.statementGates.set(id, { entered, wait });
		return { entered: enteredPromise, release };
	}

	async statement<T>(id: string | undefined, fallback: () => Promise<T>) {
		const gate = id ? this.statementGates.get(id) : undefined;
		if (id && gate) {
			this.statementGates.delete(id);
			gate.entered();
			await gate.wait;
		}
		const fault = id ? this.statementFailures.get(id) : undefined;
		if (id && fault && fault.remaining === 0) {
			this.statementFailures.delete(id);
			throw fault.error;
		}
		if (fault) fault.remaining--;
		return fallback();
	}

	takeDirectorySyncFailure() {
		const fault = this.directorySyncFailure;
		if (!fault) return undefined;
		if (fault.remaining > 0) {
			fault.remaining--;
			return undefined;
		}
		this.directorySyncFailure = undefined;
		return fault.error;
	}
}

const instrumentedDependencies = (
	controller: SqliteFaultController,
): CustodyRecordNodePersistenceDependencies => ({
	async loadNodeModules() {
		const native = await import("@peerbit/native-backbone");
		return {
			fs: {
				...nodeFs,
				async open(...args: Parameters<typeof nodeFs.open>) {
					const handle = await nodeFs.open(...args);
					return new Proxy(handle, {
						get(target, property) {
							if (property === "sync") {
								return async () => {
									const failure = controller.takeDirectorySyncFailure();
									if (failure) throw failure;
									return target.sync();
								};
							}
							const value = Reflect.get(target, property);
							return typeof value === "function" ? value.bind(target) : value;
						},
					});
				},
			},
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
									return new Proxy(statement, {
										get(statementTarget, statementProperty) {
											if (
												statementProperty === "run" ||
												statementProperty === "all"
											) {
												return (values: unknown[]) =>
													controller.statement(id, () =>
														statementTarget[statementProperty](values as never),
													);
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

type CatalogPersistence = CustodyRecordPersistence &
	Required<
		Pick<
			CustodyRecordPersistence,
			| "readCatalogStatus"
			| "captureCatalogFence"
			| "scanRecoveryPage"
			| "scanEntryPinsPage"
			| "readCatalogCandidate"
			| "migrateCatalogPage"
		>
	>;

type Opened = Readonly<{
	store: CustodyRecordStore;
	persistence: CatalogPersistence;
	facts: CustodyRecordNodePersistenceFacts;
}>;

describe("custody head catalog", function () {
	this.timeout(60_000);

	const directories = new Set<string>();
	const stores = new Set<CustodyRecordStore>();
	let source: Ed25519Keypair;
	let destination: Ed25519Keypair;
	let sourceBytes: Uint8Array;
	let destinationBytes: Uint8Array;
	let entryHash: string;
	let otherEntryHash: string;

	before(async () => {
		[source, destination] = await Promise.all([
			Ed25519Keypair.create(),
			Ed25519Keypair.create(),
		]);
		sourceBytes = serialize(source.publicKey);
		destinationBytes = serialize(destination.publicKey);
		entryHash = (await calculateRawCid(new Uint8Array([1, 2, 3]))).cid;
		otherEntryHash = (await calculateRawCid(new Uint8Array([9, 8, 7]))).cid;
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

	const temporaryDirectory = async (suffix: string): Promise<string> => {
		const parent = await mkdtemp(
			join(tmpdir(), `peerbit-custody-catalog-${suffix}-`),
		);
		directories.add(parent);
		const directory = join(parent, "peerbit-node");
		await mkdir(directory);
		return directory;
	};

	const open = async (
		nodeDirectory: string,
		options: {
			role?: "source" | "destination";
			maxFrameBytes?: number;
			dependencies?: CustodyRecordNodePersistenceDependencies;
		} = {},
	): Promise<Opened> => {
		let persistence: CustodyRecordPersistence | undefined;
		let facts: CustodyRecordNodePersistenceFacts | undefined;
		const supplied = options.dependencies;
		const role = options.role ?? "source";
		const store = await openNodeCustodyRecordStore(
			{
				nodeDirectory,
				logId: new Uint8Array([1, 2, 3]),
				localPublicKey: role === "source" ? sourceBytes : destinationBytes,
				role,
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
		for (const method of [
			"readCatalogStatus",
			"captureCatalogFence",
			"scanRecoveryPage",
			"scanEntryPinsPage",
			"readCatalogCandidate",
			"migrateCatalogPage",
		] as const) {
			if (typeof persistence[method] !== "function") {
				throw new Error(`Custody catalog method ${method} is unavailable`);
			}
		}
		return { store, persistence: persistence as CatalogPersistence, facts };
	};

	const close = async (opened: Opened) => {
		await opened.store.close();
		stores.delete(opened.store);
	};

	const manifest = async (
		attempt: number,
		valueEntryHash = entryHash,
	): Promise<CanonicalCustodyHandoffManifest> =>
		createCustodyHandoffManifestV1(
			{
				logId: new Uint8Array([1, 2, 3]),
				entryHash: valueEntryHash,
				entryByteLength: 3n,
				source: source.publicKey,
				destination: destination.publicKey,
				visit: {
					viewId: digest(10),
					planDigest: digest(11),
					installSequence: 7n,
					taskOrdinal: attempt,
					resolution: "u32",
					hashNumber: 99,
				},
				ownerPlanId: digest(12),
				attemptGeneration: new Uint8Array(32).fill(attempt),
			},
			source.signer(PreHash.SHA_256),
		);

	const receipt = async (
		value: CanonicalCustodyHandoffManifest,
		pinSequence = 5n,
	): Promise<CanonicalCustodyHandoffReceipt> =>
		createCustodyHandoffReceiptV1(
			{
				manifest: value,
				custodyEpoch: new Uint8Array(32).fill(21),
				pinSequence,
			},
			destination.signer(PreHash.SHA_256),
		);

	const pinEvidence = (
		value: CanonicalCustodyHandoffManifest,
		valueReceipt: CanonicalCustodyHandoffReceipt,
	) =>
		issueCustodyDestinationPinEvidenceForTest({
			moveKey: value.moveKey,
			handoffId: value.handoffId,
			custodyProfileId: CUSTODY_HANDOFF_PROFILE_ID,
			custodyProfileMask: CUSTODY_HANDOFF_PROFILE_MASK,
			custodyEpoch: valueReceipt.custodyEpoch,
			pinSequence: valueReceipt.pinSequence,
			compositeCommitId: digest(31),
		});

	const scanRecovery = (
		persistence: CatalogPersistence,
		fence: CustodyRecordCatalogFence,
		state: CustodyRecordState,
		after?: CustodyRecordCatalogCursor,
		maxRows = 64,
		maxBytes = 64 * 1024,
	): Promise<CustodyRecordCatalogPage> =>
		persistence.scanRecoveryPage({
			fence,
			state,
			...(after ? { after } : {}),
			maxRows,
			maxBytes,
		});

	const collectRecovery = async (
		persistence: CatalogPersistence,
		fence: CustodyRecordCatalogFence,
		state: CustodyRecordState,
		maxRows = 1,
	): Promise<CustodyRecordCatalogCandidate[]> => {
		const candidates: CustodyRecordCatalogCandidate[] = [];
		let after: CustodyRecordCatalogCursor | undefined;
		do {
			const page = await scanRecovery(
				persistence,
				fence,
				state,
				after,
				maxRows,
			);
			expect(page.fence).to.equal(fence);
			candidates.push(...page.candidates);
			after = page.next;
		} while (after);
		return candidates;
	};

	it("atomically follows the exact selected frame and does not bump on an idempotent retry", async () => {
		const directory = await temporaryDirectory("exact-head");
		const opened = await open(directory);
		const value = await manifest(1);
		const valueReceipt = await receipt(value);
		const initial = await opened.persistence.readCatalogStatus();
		expect(initial).to.include({
			lastMutationSequence: 0n,
			migrationState: "ready",
		});

		const prepared = await opened.store.prepareSource(0n, value.bytes);
		const preparedStatus = await opened.persistence.readCatalogStatus();
		expect(preparedStatus.lastMutationSequence).to.equal(2n);
		const preparedFence = await opened.persistence.captureCatalogFence();
		const preparedRows = await collectRecovery(
			opened.persistence,
			preparedFence,
			"source-prepared",
		);
		expect(preparedRows).to.have.length(1);
		const candidate = preparedRows[0]!;
		expect(candidate).to.deep.include({
			catalogEpoch: initial.catalogEpoch,
			mutationSequence: 2n,
			moveKey: value.moveKey,
			recordSequence: 2n,
			state: "source-prepared",
			entryHash,
			handoffId: value.handoffId,
			frameChecksum: prepared.durableCommit!.frameChecksum,
			domainId: opened.facts.domainId,
			writerEpoch: opened.facts.writerEpoch,
			writerOwner: opened.facts.writerOwner,
		});

		const current = await opened.persistence.readCatalogCandidate(candidate);
		expect(current.status).to.equal("current");
		if (current.status !== "current") throw new Error("Expected current head");
		expect(current.candidate).to.deep.equal(candidate);
		const savedFirstByte = current.frame[0]!;
		current.frame[0] ^= 0xff;
		const reread = await opened.persistence.readCatalogCandidate(candidate);
		expect(reread.status).to.equal("current");
		if (reread.status !== "current") throw new Error("Expected current head");
		expect(reread.frame[0]).to.equal(savedFirstByte);

		const retry = await opened.store.prepareSource(0n, value.bytes);
		expect(retry.durableCommit).to.deep.equal(prepared.durableCommit);
		expect(
			(await opened.persistence.readCatalogStatus()).lastMutationSequence,
		).to.equal(preparedStatus.lastMutationSequence);

		const receipted = await opened.store.markSourceReceiptDurable(
			2n,
			value.bytes,
			valueReceipt.bytes,
		);
		expect(receipted.snapshot.state).to.equal("source-receipt-durable");
		expect(
			(await opened.persistence.readCatalogStatus()).lastMutationSequence,
		).to.equal(3n);
		expect(
			await opened.persistence.readCatalogCandidate(candidate),
		).to.deep.equal({ status: "changed" });
		await close(opened);
	});

	it("routes catalog operations through store admission, read-once capture, and close draining", async () => {
		const directory = await temporaryDirectory("store-api");
		const controller = new SqliteFaultController();
		const opened = await open(directory, {
			dependencies: instrumentedDependencies(controller),
		});
		const value = await manifest(40);
		await opened.store.prepareSource(0n, value.bytes);
		const status = await opened.store.readCatalogStatus();
		expect(status.migrationState).to.equal("ready");
		const fence = await opened.store.captureCatalogFence();

		const once = <T>(name: string, value: T, reads: Map<string, number>) => ({
			enumerable: true,
			get() {
				const count = (reads.get(name) ?? 0) + 1;
				reads.set(name, count);
				if (count > 1) throw new Error(`catalog input ${name} read twice`);
				return value;
			},
		});
		const scanReads = new Map<string, number>();
		const scanInput = Object.defineProperties(
			{},
			{
				fence: once("fence", fence, scanReads),
				state: once("state", "source-prepared" as const, scanReads),
				after: once("after", undefined, scanReads),
				maxRows: once("maxRows", 1, scanReads),
				maxBytes: once("maxBytes", 64 * 1024, scanReads),
			},
		) as {
			fence: CustodyRecordCatalogFence;
			state: CustodyRecordState;
			after?: CustodyRecordCatalogCursor;
			maxRows: number;
			maxBytes: number;
		};
		const page = await opened.store.scanRecoveryPage(scanInput);
		expect(page.candidates).to.have.length(1);
		expect([...scanReads.values()]).to.deep.equal([1, 1, 1, 1, 1]);

		const candidateReads = new Map<string, number>();
		const candidateDescriptors = Object.fromEntries(
			Object.entries(page.candidates[0]!).map(([name, candidateValue]) => [
				name,
				once(name, candidateValue, candidateReads),
			]),
		);
		const hostileCandidate = Object.defineProperties(
			{},
			candidateDescriptors,
		) as CustodyRecordCatalogCandidate;
		const candidateRead =
			await opened.store.readCatalogCandidate(hostileCandidate);
		expect(candidateRead.status).to.equal("current");
		expect([...candidateReads.values()].every((count) => count === 1)).to.equal(
			true,
		);

		const oversizedEpochReads = new Map<string, number>();
		const oversizedEpochCandidate = Object.defineProperties(
			{},
			Object.fromEntries(
				Object.entries({
					...page.candidates[0]!,
					writerEpoch: MAX_U64 + 1n,
				}).map(([name, candidateValue]) => [
					name,
					once(name, candidateValue, oversizedEpochReads),
				]),
			),
		) as CustodyRecordCatalogCandidate;
		await expect(
			opened.store.readCatalogCandidate(oversizedEpochCandidate),
		).to.be.rejectedWith("candidate sequence");
		expect(
			[...oversizedEpochReads.values()].every((count) => count === 1),
		).to.equal(true);
		await expect(
			opened.store.readCatalogCandidate({
				...page.candidates[0]!,
				mutationSequence: fence.upperMutationSequence + 1n,
			}),
		).to.be.rejectedWith("waterline");

		const pinReads = new Map<string, number>();
		expect(
			(
				await opened.store.scanEntryPinsPage(
					Object.defineProperties(
						{},
						{
							fence: once("fence", fence, pinReads),
							entryHash: once("entryHash", entryHash, pinReads),
							after: once("after", undefined, pinReads),
							maxRows: once("maxRows", 1, pinReads),
							maxBytes: once("maxBytes", 64 * 1024, pinReads),
						},
					) as {
						fence: CustodyRecordCatalogFence;
						entryHash: string;
						maxRows: number;
						maxBytes: number;
					},
				)
			).candidates,
		).to.deep.equal([]);
		expect([...pinReads.values()]).to.deep.equal([1, 1, 1, 1, 1]);

		const migrationReads = new Map<string, number>();
		expect(
			await opened.store.migrateCatalogPage(
				Object.defineProperties(
					{},
					{
						maxMoveKeys: once("maxMoveKeys", 1, migrationReads),
						maxBytes: once("maxBytes", 64 * 1024, migrationReads),
					},
				) as { maxMoveKeys: number; maxBytes: number },
			),
		).to.deep.equal({ migrationState: "ready", processed: 0 });
		expect([...migrationReads.values()]).to.deep.equal([1, 1]);

		await expect(
			opened.store.scanRecoveryPage({
				fence: { ...fence } as CustodyRecordCatalogFence,
				state: "source-prepared",
			}),
		).to.be.rejectedWith("fence");
		expect((await opened.store.readCatalogStatus()).migrationState).to.equal(
			"ready",
		);

		const gate = controller.blockNextStatement("custody-catalog-status");
		const admitted = opened.store.readCatalogStatus();
		await gate.entered;
		let closed = false;
		const closing = opened.store.close().then(() => {
			closed = true;
		});
		try {
			await expect(opened.store.captureCatalogFence()).to.be.rejectedWith(
				"closing",
			);
			await Promise.resolve();
			expect(closed).to.equal(false);
		} finally {
			gate.release();
		}
		expect((await admitted).migrationState).to.equal("ready");
		await closing;
		stores.delete(opened.store);
	});

	it("rejects every store-facing catalog route on unsupported memory persistence", async () => {
		const store = await CustodyRecordStore.open({
			persistence: new MemoryCustodyRecordPersistence(),
			durability: "memory",
		});
		for (const operation of [
			() => store.readCatalogStatus(),
			() => store.captureCatalogFence(),
			() =>
				store.scanRecoveryPage({
					fence: {} as CustodyRecordCatalogFence,
					state: "absent",
				}),
			() =>
				store.scanEntryPinsPage({
					fence: {} as CustodyRecordCatalogFence,
					entryHash,
				}),
			() => store.readCatalogCandidate({} as CustodyRecordCatalogCandidate),
			() => store.migrateCatalogPage(),
		]) {
			await expect(operation()).to.be.rejectedWith(
				"does not expose a bounded catalog",
			);
		}
		await store.close();
		await expect(store.readCatalogStatus()).to.be.rejectedWith("closing");
	});

	it("indexes every move for an entry but exposes only pinned or receipted destination heads in pin scans", async () => {
		const directory = await temporaryDirectory("entry-pins");
		const opened = await open(directory, { role: "destination" });
		const first = await manifest(2);
		const second = await manifest(3);
		const [firstReceipt, secondReceipt] = await Promise.all([
			receipt(first, 7n),
			receipt(second, 8n),
		]);
		await opened.store.beginDestination(0n, first.bytes);
		await opened.store.beginDestination(0n, second.bytes);

		let fence = await opened.persistence.captureCatalogFence();
		expect(
			(await opened.persistence.scanEntryPinsPage({ fence, entryHash }))
				.candidates,
		).to.deep.equal([]);

		await opened.store.markDestinationPinned(
			2n,
			first.bytes,
			pinEvidence(first, firstReceipt),
		);
		fence = await opened.persistence.captureCatalogFence();
		expect(
			(
				await opened.persistence.scanEntryPinsPage({ fence, entryHash })
			).candidates.map((candidate) => [candidate.moveKey, candidate.state]),
		).to.deep.equal([[first.moveKey, "destination-pinned"]]);

		await opened.store.markDestinationReceipted(
			3n,
			first.bytes,
			firstReceipt.bytes,
		);
		await opened.store.markDestinationPinned(
			2n,
			second.bytes,
			pinEvidence(second, secondReceipt),
		);
		fence = await opened.persistence.captureCatalogFence();
		const pinned = await opened.persistence.scanEntryPinsPage({
			fence,
			entryHash,
		});
		expect(
			new Map(
				pinned.candidates.map((candidate) => [
					candidate.moveKey,
					candidate.state,
				]),
			),
		).to.deep.equal(
			new Map([
				[first.moveKey, "destination-receipted"],
				[second.moveKey, "destination-pinned"],
			]),
		);
		expect(
			(
				await opened.persistence.scanEntryPinsPage({
					fence,
					entryHash: otherEntryHash,
				})
			).candidates,
		).to.deep.equal([]);

		const passive = pinned.candidates[0]!;
		expect(passive).to.not.have.any.keys(
			"durableCommit",
			"pinEvidence",
			"receipt",
			"prunePermit",
		);
		await expect(
			opened.store.markDestinationPinned(4n, first.bytes, passive as never),
		).to.be.rejectedWith("Invalid custody destination pin evidence");
		await close(opened);
	});

	it("paginates by mutation sequence under an opaque fence without skipping concurrent writes", async () => {
		const directory = await temporaryDirectory("fence");
		const opened = await open(directory);
		const values = await Promise.all([manifest(4), manifest(5), manifest(6)]);
		await opened.store.prepareSource(0n, values[0]!.bytes);
		await opened.store.prepareSource(0n, values[1]!.bytes);
		const oldFence = await opened.persistence.captureCatalogFence();
		await opened.store.prepareSource(0n, values[2]!.bytes);

		const oldRows = await collectRecovery(
			opened.persistence,
			oldFence,
			"source-prepared",
			1,
		);
		expect(oldRows.map((candidate) => candidate.moveKey)).to.have.members(
			values.slice(0, 2).map((value) => value.moveKey),
		);
		expect(
			oldRows.every(
				(candidate) =>
					candidate.mutationSequence <= oldFence.upperMutationSequence,
			),
		).to.equal(true);

		const newFence = await opened.persistence.captureCatalogFence();
		const allRows = await collectRecovery(
			opened.persistence,
			newFence,
			"source-prepared",
			1,
		);
		expect(allRows.map((candidate) => candidate.moveKey)).to.have.members(
			values.map((value) => value.moveKey),
		);
		for (let index = 1; index < allRows.length; index++) {
			expect(
				allRows[index]!.mutationSequence > allRows[index - 1]!.mutationSequence,
			).to.equal(true);
		}

		await expect(
			scanRecovery(
				opened.persistence,
				{ ...newFence } as CustodyRecordCatalogFence,
				"source-prepared",
			),
		).to.be.rejectedWith("fence");
		await close(opened);
	});

	it("enforces hard row and byte ceilings with a cap-plus-one page probe", async () => {
		const directory = await temporaryDirectory("bounds");
		const opened = await open(directory);
		const values = await Promise.all([manifest(7), manifest(8)]);
		for (const value of values) {
			await opened.store.prepareSource(0n, value.bytes);
		}
		const fence = await opened.persistence.captureCatalogFence();
		const first = await scanRecovery(
			opened.persistence,
			fence,
			"source-prepared",
			undefined,
			1,
		);
		expect(first.candidates).to.have.length(1);
		expect(first.next).to.deep.equal({
			mutationSequence: first.candidates[0]!.mutationSequence,
			moveKey: first.candidates[0]!.moveKey,
		});
		const second = await scanRecovery(
			opened.persistence,
			fence,
			"source-prepared",
			first.next,
			1,
		);
		expect(second.candidates).to.have.length(1);
		expect(second.next).to.equal(undefined);

		for (const maxRows of [0, 257]) {
			expect(() =>
				opened.persistence.scanRecoveryPage({
					fence,
					state: "source-prepared",
					maxRows,
				}),
			).to.throw("row");
		}
		for (const maxBytes of [0, 256 * 1024 + 1]) {
			expect(() =>
				opened.persistence.scanRecoveryPage({
					fence,
					state: "source-prepared",
					maxBytes,
				}),
			).to.throw("byte");
		}
		await expect(
			opened.persistence.scanRecoveryPage({
				fence,
				state: "source-prepared",
				maxRows: 1,
				maxBytes: 1,
			}),
		).to.be.rejectedWith("byte");
		await close(opened);
	});

	it("orders full-width u64 mutation blobs and rejects exhaustion before creating an orphan", async () => {
		const directory = await temporaryDirectory("u64");
		const genesis = await open(directory);
		const namespace = genesis.facts.namespace;
		await close(genesis);
		const initial = MAX_U64 - 4n;
		await withDatabase(namespace, async (database) => {
			await database.exec(
				`UPDATE custody_catalog_meta SET last_mutation_sequence = x'${initial
					.toString(16)
					.padStart(16, "0")}' WHERE id = 1`,
			);
		});

		const opened = await open(directory);
		const values = await Promise.all([manifest(9), manifest(10), manifest(11)]);
		await opened.store.prepareSource(0n, values[0]!.bytes);
		await opened.store.prepareSource(0n, values[1]!.bytes);
		expect(
			(await opened.persistence.readCatalogStatus()).lastMutationSequence,
		).to.equal(MAX_U64);
		const fence = await opened.persistence.captureCatalogFence();
		const rows = await collectRecovery(
			opened.persistence,
			fence,
			"source-prepared",
			1,
		);
		expect(rows.map((candidate) => candidate.mutationSequence)).to.deep.equal([
			MAX_U64 - 2n,
			MAX_U64,
		]);

		let exhaustion: unknown;
		try {
			await opened.store.prepareSource(0n, values[2]!.bytes);
		} catch (error) {
			exhaustion = error;
		}
		expect(exhaustion).to.be.instanceOf(Error);
		expect((exhaustion as Error).message).to.include("persist custody record");
		expect(((exhaustion as Error).cause as Error).message).to.include(
			"exhaust",
		);
		await expect(opened.store.close()).to.be.rejectedWith("poisoned");
		stores.delete(opened.store);
		await withDatabase(namespace, async (database) => {
			const statement = await database.prepare(
				"SELECT (SELECT count(*) FROM custody_records) AS records, (SELECT count(*) FROM custody_heads) AS heads",
			);
			expect(await statement.all([])).to.deep.equal([
				{ records: 4n, heads: 2n },
			]);
		});
	});

	it("rolls back the frame, head, and catalog sequence together when head indexing fails", async () => {
		const directory = await temporaryDirectory("atomic-rollback");
		const controller = new SqliteFaultController();
		const opened = await open(directory, {
			dependencies: instrumentedDependencies(controller),
		});
		const value = await manifest(12);
		controller.failNextStatement("custody-catalog-head-write");
		await expect(
			opened.store.prepareSource(0n, value.bytes),
		).to.be.rejectedWith("persist custody record frame");
		await expect(opened.store.close()).to.be.rejectedWith("poisoned");
		stores.delete(opened.store);

		await withDatabase(opened.facts.namespace, async (database) => {
			const statement = await database.prepare(
				"SELECT (SELECT count(*) FROM custody_records) AS records, (SELECT count(*) FROM custody_heads) AS heads, last_mutation_sequence AS mutation FROM custody_catalog_meta WHERE id = 1",
			);
			const rows = (await statement.all([])) as Array<{
				records: bigint;
				heads: bigint;
				mutation: Uint8Array;
			}>;
			expect(rows).to.have.length(1);
			expect(rows[0]!.records).to.equal(0n);
			expect(rows[0]!.heads).to.equal(0n);
			expect([...rows[0]!.mutation]).to.deep.equal(new Array(8).fill(0));
		});

		const reopened = await open(directory);
		expect(
			(await reopened.store.prepareSource(0n, value.bytes)).snapshot.state,
		).to.equal("source-prepared");
		await close(reopened);
	});

	it("recovers the same exact indexed head after checkpoint and directory-fsync cuts", async () => {
		for (const kind of ["checkpoint", "directory"] as const) {
			const directory = await temporaryDirectory(`barrier-cut-${kind}`);
			const controller = new SqliteFaultController();
			const opened = await open(directory, {
				dependencies: instrumentedDependencies(controller),
			});
			const value = await manifest(kind === "checkpoint" ? 13 : 14);
			if (kind === "checkpoint") {
				controller.failNextStatement(
					"custody-record-checkpoint",
					new Error("injected catalog checkpoint cut"),
					1,
				);
			} else {
				controller.failNextDirectorySync(
					new Error("injected catalog directory cut"),
					1,
				);
			}
			await expect(opened.store.prepareSource(0n, value.bytes)).to.be.rejected;
			await expect(opened.store.close()).to.be.rejectedWith("poisoned");
			stores.delete(opened.store);

			const reopened = await open(directory);
			const recovered = await reopened.store.read(value.moveKey);
			expect(recovered.snapshot).to.deep.include({
				moveKey: value.moveKey,
				revision: 2n,
				state: "source-prepared",
			});
			const fence = await reopened.persistence.captureCatalogFence();
			const candidates = await collectRecovery(
				reopened.persistence,
				fence,
				"source-prepared",
			);
			expect(candidates).to.have.length(1);
			expect(candidates[0]).to.deep.include({
				moveKey: value.moveKey,
				recordSequence: 2n,
				frameChecksum: recovered.durableCommit!.frameChecksum,
			});
			await close(reopened);
		}
	});

	it("fails closed when the durable selected frame has a missing or transplanted catalog head", async () => {
		const mutations = [
			"DELETE FROM custody_heads",
			"UPDATE custody_heads SET frame_checksum = zeroblob(32)",
			"UPDATE custody_heads SET handoff_id = zeroblob(32)",
		];
		for (let index = 0; index < mutations.length; index++) {
			const directory = await temporaryDirectory(`corrupt-head-${index}`);
			const first = await open(directory);
			const value = await manifest(20 + index);
			await first.store.prepareSource(0n, value.bytes);
			const namespace = first.facts.namespace;
			await close(first);
			await withDatabase(namespace, async (database) => {
				await database.exec(mutations[index]!);
			});

			const reopened = await open(directory);
			await expect(reopened.store.read(value.moveKey)).to.be.rejectedWith(
				"confirm recovered custody record frame",
			);
			await expect(reopened.store.close()).to.be.rejectedWith("poisoned");
			stores.delete(reopened.store);
		}
	});

	it("rejects corrupt and oversized candidate frames instead of treating them as benign catalog churn", async () => {
		for (const kind of ["corrupt", "oversized"] as const) {
			const directory = await temporaryDirectory(`candidate-${kind}`);
			const first = await open(directory);
			const value = await manifest(kind === "corrupt" ? 23 : 24);
			await first.store.prepareSource(0n, value.bytes);
			const namespace = first.facts.namespace;
			await close(first);
			if (kind === "corrupt") {
				await withDatabase(namespace, async (database) => {
					await database.exec(
						"UPDATE custody_records SET frame = x'010203' WHERE move_key = (SELECT lower(hex(move_key)) FROM custody_heads LIMIT 1) AND slot = (SELECT CASE slot WHEN 0 THEN 'a' ELSE 'b' END FROM custody_heads LIMIT 1)",
					);
				});
			}

			const reopened = await open(directory, {
				...(kind === "oversized" ? { maxFrameBytes: 64 } : {}),
			});
			const fence = await reopened.persistence.captureCatalogFence();
			const page = await reopened.persistence.scanRecoveryPage({
				fence,
				state: "source-prepared",
			});
			expect(page.candidates).to.have.length(1);
			await expect(
				reopened.persistence.readCatalogCandidate(page.candidates[0]!),
			).to.be.rejected;
			await close(reopened);
		}
	});

	it("migrates a populated v1 namespace in bounded restartable pages and retains a new key below the cursor", async () => {
		const directory = await temporaryDirectory("migration");
		const values = await Promise.all(
			Array.from({ length: 8 }, (_, index) => manifest(30 + index)),
		);
		values.sort((left, right) => left.moveKey.localeCompare(right.moveKey));
		const newBelowCursor = values[0]!;
		const legacy = values.slice(-2);
		const first = await open(directory);
		for (const value of legacy) {
			await first.store.prepareSource(0n, value.bytes);
		}
		const namespace = first.facts.namespace;
		await close(first);
		await withDatabase(namespace, async (database) => {
			await database.exec(
				"DROP INDEX custody_heads_entry_pin; DROP INDEX custody_heads_recovery; DROP TABLE custody_heads; DROP TABLE custody_catalog_meta; PRAGMA user_version = 1",
			);
		});

		const migrationController = new SqliteFaultController();
		let migrating = await open(directory, {
			dependencies: instrumentedDependencies(migrationController),
		});
		expect(await migrating.store.readCatalogStatus()).to.deep.include({
			lastMutationSequence: 0n,
			migrationState: "building",
		});
		await expect(migrating.store.captureCatalogFence()).to.be.rejectedWith(
			"building",
		);
		await expect(
			migrating.store.migrateCatalogPage({
				maxMoveKeys: 1,
				maxBytes: 1,
			}),
		).to.be.rejectedWith("durably migrate");
		expect(await migrating.store.readCatalogStatus()).to.deep.include({
			lastMutationSequence: 0n,
			migrationState: "building",
		});
		await close(migrating);
		migrating = await open(directory, {
			dependencies: instrumentedDependencies(migrationController),
		});
		expect(await migrating.store.readCatalogStatus()).to.deep.include({
			lastMutationSequence: 0n,
			migrationState: "building",
		});
		migrationController.failNextStatement("custody-catalog-head-write");
		await expect(
			migrating.store.migrateCatalogPage({ maxMoveKeys: 1 }),
		).to.be.rejectedWith("durably migrate");
		expect(await migrating.store.readCatalogStatus()).to.deep.include({
			lastMutationSequence: 0n,
			migrationState: "building",
		});
		migrationController.failNextStatement("custody-record-checkpoint");
		await expect(
			migrating.store.migrateCatalogPage({
				maxMoveKeys: 1,
				maxBytes: 2 * 1024 * 1024,
			}),
		).to.be.rejectedWith("durably migrate");
		await expect(migrating.store.readCatalogStatus()).to.be.rejectedWith(
			"record store is poisoned",
		);
		await expect(migrating.store.close()).to.be.rejectedWith("poisoned");
		stores.delete(migrating.store);

		migrating = await open(directory);
		const committedPage = await migrating.store.readCatalogStatus();
		expect(committedPage).to.deep.include({
			lastMutationSequence: 1n,
			migrationState: "building",
		});
		expect(committedPage.migrationAfter).to.be.a("string");
		expect(newBelowCursor.moveKey < committedPage.migrationAfter!).to.equal(
			true,
		);
		await migrating.store.prepareSource(0n, newBelowCursor.bytes);
		for (let page = 0; page < 4; page++) {
			const result = await migrating.store.migrateCatalogPage({
				maxMoveKeys: 1,
			});
			if (result.migrationState === "ready") break;
		}
		expect(await migrating.store.readCatalogStatus()).to.deep.include({
			migrationState: "ready",
		});
		const fence = await migrating.store.captureCatalogFence();
		const candidates = await collectRecovery(
			migrating.persistence,
			fence,
			"source-prepared",
			1,
		);
		expect(candidates.map((candidate) => candidate.moveKey)).to.have.members([
			newBelowCursor.moveKey,
			...legacy.map((value) => value.moveKey),
		]);
		for (const candidate of candidates) {
			const recovered = await migrating.store.read(candidate.moveKey);
			expect(recovered.snapshot).to.deep.include({
				moveKey: candidate.moveKey,
				revision: candidate.recordSequence,
				state: "source-prepared",
			});
			const candidateRead =
				await migrating.store.readCatalogCandidate(candidate);
			expect(candidateRead.status).to.equal("current");
			if (candidateRead.status === "current") {
				expect(candidateRead.candidate).to.deep.equal(candidate);
			}
		}

		for (const input of [
			{ maxMoveKeys: 65 },
			{ maxBytes: 2 * 1024 * 1024 + 1 },
		]) {
			await expect(migrating.store.migrateCatalogPage(input)).to.be.rejected;
		}
		await close(migrating);
	});

	it("lazily indexes one legacy point read while bounded migration remains building", async () => {
		const directory = await temporaryDirectory("lazy-migration-read");
		const value = await manifest(61);
		const first = await open(directory);
		await first.store.prepareSource(0n, value.bytes);
		const namespace = first.facts.namespace;
		await close(first);
		await withDatabase(namespace, async (database) => {
			await database.exec(
				"DROP INDEX custody_heads_entry_pin; DROP INDEX custody_heads_recovery; DROP TABLE custody_heads; DROP TABLE custody_catalog_meta; PRAGMA user_version = 1",
			);
		});

		let migrating = await open(directory);
		expect(await migrating.store.readCatalogStatus()).to.deep.include({
			lastMutationSequence: 0n,
			migrationState: "building",
		});
		expect(
			(await migrating.store.read(value.moveKey)).snapshot,
		).to.deep.include({
			moveKey: value.moveKey,
			revision: 2n,
			state: "source-prepared",
		});
		expect(await migrating.store.readCatalogStatus()).to.deep.include({
			lastMutationSequence: 1n,
			migrationState: "building",
		});
		await close(migrating);
		await withDatabase(namespace, async (database) => {
			const statement = await database.prepare(
				"SELECT count(*) AS heads, min(record_sequence) AS record_sequence, min(mutation_sequence) AS mutation_sequence FROM custody_heads",
			);
			const rows = (await statement.all([])) as Array<{
				heads: bigint;
				record_sequence: Uint8Array;
				mutation_sequence: Uint8Array;
			}>;
			expect(rows).to.have.length(1);
			expect(rows[0]!.heads).to.equal(1n);
			expect([...rows[0]!.record_sequence]).to.deep.equal([
				0, 0, 0, 0, 0, 0, 0, 2,
			]);
			expect([...rows[0]!.mutation_sequence]).to.deep.equal([
				0, 0, 0, 0, 0, 0, 0, 1,
			]);
		});
		migrating = await open(directory);
		expect(await migrating.store.readCatalogStatus()).to.deep.include({
			lastMutationSequence: 1n,
			migrationState: "building",
		});
		expect(
			await migrating.store.migrateCatalogPage({ maxMoveKeys: 1 }),
		).to.deep.include({ migrationState: "ready", processed: 1 });
		expect(await migrating.store.readCatalogStatus()).to.deep.include({
			lastMutationSequence: 1n,
			migrationState: "ready",
		});
		await close(migrating);
	});

	it("uses the ordered recovery and partial entry-pin indexes for bounded scans", async () => {
		const directory = await temporaryDirectory("query-plan");
		const opened = await open(directory);
		const namespace = opened.facts.namespace;
		await close(opened);
		await withDatabase(namespace, async (database) => {
			const statements = [
				{
					index: "custody_heads_recovery",
					sql: "EXPLAIN QUERY PLAN SELECT move_key, record_sequence, mutation_sequence, slot, state_tag, entry_hash, handoff_id, frame_checksum, domain_id, writer_epoch, writer_owner FROM custody_heads WHERE state_tag = ? AND mutation_sequence <= ? AND (mutation_sequence > ? OR (mutation_sequence = ? AND move_key > ?)) ORDER BY mutation_sequence, move_key LIMIT ?",
					values: [
						1,
						new Uint8Array(8).fill(0xff),
						new Uint8Array(8),
						new Uint8Array(8),
						new Uint8Array(32),
						2,
					],
				},
				{
					index: "custody_heads_entry_pin",
					sql: "EXPLAIN QUERY PLAN SELECT move_key, record_sequence, mutation_sequence, slot, state_tag, entry_hash, handoff_id, frame_checksum, domain_id, writer_epoch, writer_owner FROM custody_heads WHERE entry_hash = ? AND state_tag IN (4, 5) AND mutation_sequence <= ? AND (mutation_sequence > ? OR (mutation_sequence = ? AND move_key > ?)) ORDER BY mutation_sequence, move_key LIMIT ?",
					values: [
						new TextEncoder().encode(entryHash),
						new Uint8Array(8).fill(0xff),
						new Uint8Array(8),
						new Uint8Array(8),
						new Uint8Array(32),
						2,
					],
				},
			];
			for (const value of statements) {
				const statement = await database.prepare(value.sql);
				const rows = (await statement.all(value.values)) as Array<{
					detail: string;
				}>;
				expect(rows.map((row) => row.detail).join("\n")).to.include(
					value.index,
				);
			}
		});
	});
});
