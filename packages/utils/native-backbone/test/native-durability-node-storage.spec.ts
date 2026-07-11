import { expect } from "chai";
import { createHash } from "node:crypto";
import {
	appendFile,
	mkdir,
	mkdtemp,
	open,
	readFile,
	realpath,
	rm,
	stat,
	writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import {
	type NativeDurabilityJournalCodec,
	NativeDurabilityJournalCorruptionError,
	type NativeDurabilityJournalRecord,
	NativeDurabilityOperationKind,
	NativeDurabilityPhase,
	createNativeDurabilityJournalCodec,
} from "../src/durability/codec.js";
import { NativeDurabilityLeaseUnavailableError } from "../src/durability/lease.js";
import {
	NodeNativeDurabilityStorage,
	createNodeNativeDurabilityStorage,
} from "../src/durability/node-storage.js";
import {
	type NativeDurabilityCheckpointRequest,
	NativeDurabilityDigestMismatchError,
	type NativeDurabilityJournalAppendRequest,
	NativeDurabilityMigrationRequiredError,
	type NativeDurabilityStageRequest,
	NativeDurabilityStorageClosedError,
	NativeDurabilityStorageCorruptionError,
	encodeNativeDurabilityCanonical,
} from "../src/durability/storage.js";

const PROGRAM_ID = new Uint8Array([0x70, 0x65, 0x65, 0x72, 0x62, 0x69, 0x74]);
const NAMESPACE = "native-durability-v1";
const CHECKPOINTS = "checkpoints";
const STAGING = "staging";
const JOURNAL = "journal.bin";

const digest = (bytes: Uint8Array): Uint8Array =>
	new Uint8Array(createHash("sha256").update(bytes).digest());

const digestHex = (bytes: Uint8Array): string =>
	createHash("sha256").update(bytes).digest("hex");

const bytesEqual = (left: Uint8Array, right: Uint8Array): boolean =>
	left.byteLength === right.byteLength &&
	left.every((byte, index) => byte === right[index]);

const concatBytes = (chunks: readonly Uint8Array[]): Uint8Array => {
	const bytes = new Uint8Array(
		chunks.reduce((length, chunk) => length + chunk.byteLength, 0),
	);
	let offset = 0;
	for (const chunk of chunks) {
		bytes.set(chunk, offset);
		offset += chunk.byteLength;
	}
	return bytes;
};

const rejectedWith = async (promise: Promise<unknown>): Promise<unknown> => {
	try {
		await promise;
	} catch (error) {
		return error;
	}
	throw new Error("Expected promise to reject");
};

const pathExists = async (path: string): Promise<boolean> => {
	try {
		await stat(path);
		return true;
	} catch (error) {
		if ((error as { code?: string }).code === "ENOENT") return false;
		throw error;
	}
};

const writeFileAndSync = async (
	path: string,
	bytes: Uint8Array,
): Promise<void> => {
	await writeFile(path, bytes);
	const file = await open(path, "r+");
	try {
		await file.sync();
	} finally {
		await file.close();
	}
	const directory = await open(dirname(path), "r");
	try {
		await directory.sync();
	} finally {
		await directory.close();
	}
};

const syncDirectoryPath = async (path: string): Promise<void> => {
	const directory = await open(path, "r");
	try {
		await directory.sync();
	} finally {
		await directory.close();
	}
};

const durabilityPath = (directory: string, ...parts: string[]): string =>
	join(directory, NAMESPACE, ...parts);

const transactionDirectory = (
	directory: string,
	transactionId: string,
): string =>
	durabilityPath(
		directory,
		STAGING,
		`tx-${digestHex(new TextEncoder().encode(transactionId))}`,
	);

const encodeManifestEnvelope = (payload: unknown): Uint8Array => {
	const payloadText = JSON.stringify(payload);
	return new TextEncoder().encode(
		JSON.stringify({
			payload: payloadText,
			checksum: digestHex(new TextEncoder().encode(payloadText)),
		}),
	);
};

type TransactionFrames = {
	codec: NativeDurabilityJournalCodec;
	frames: Uint8Array;
	individualFrames: Uint8Array[];
	records: NativeDurabilityJournalRecord[];
	planDigest: Uint8Array;
};

const transactionFrames = async (
	storage: NodeNativeDurabilityStorage,
	transactionId: string,
	txSequence: bigint,
	phases: readonly NativeDurabilityPhase[],
	firstRecordLsn = 1n,
): Promise<TransactionFrames> => {
	const checkpoint = await storage.readLatestCheckpoint();
	if (!checkpoint) throw new Error("Storage has no checkpoint authority");
	const codec = await createNativeDurabilityJournalCodec({
		checkpointLsn: checkpoint.checkpointLsn,
		checkpointTxSequenceHighwater: checkpoint.txSequenceHighwater,
		expectedProgramId: PROGRAM_ID,
		expectedWriterDomainId: storage.domainId,
		checkpointWriterEpoch: checkpoint.originFence.epoch,
		checkpointWriterOwnerId: checkpoint.originFence.ownerId,
		currentWriterEpoch: storage.fence.epoch,
		currentWriterOwnerId: storage.fence.ownerId,
		retainedTransactions: checkpoint.retainedTransactions,
	});
	const planDigest = digest(new TextEncoder().encode(`plan:${transactionId}`));
	const records = phases.map(
		(phase, index): NativeDurabilityJournalRecord => ({
			recordLsn: firstRecordLsn + BigInt(index),
			txSequence,
			writerEpoch: storage.fence.epoch,
			writerOwnerId: storage.fence.ownerId,
			writerDomainId: storage.domainId,
			phase,
			operationKind: NativeDurabilityOperationKind.Append,
			programId: new Uint8Array(PROGRAM_ID),
			transactionId,
			planDigest: new Uint8Array(planDigest),
			payload: new Uint8Array([Number(phase), index, 0xa5]),
		}),
	);
	const individualFrames = records.map((record) => codec.encode(record));
	return {
		codec,
		frames: concatBytes(individualFrames),
		individualFrames,
		records,
		planDigest,
	};
};

const appendRequest = (
	transaction: TransactionFrames,
	expectedOffset = 0,
): NativeDurabilityJournalAppendRequest => ({
	transactionId: transaction.records[0]!.transactionId,
	txSequence: transaction.records[0]!.txSequence,
	firstRecordLsn: transaction.records[0]!.recordLsn,
	lastRecordLsn: transaction.records.at(-1)!.recordLsn,
	expectedOffset,
	frames: transaction.frames,
	framesDigest: digest(transaction.frames),
});

describe("native durability Node storage", function () {
	this.timeout(180_000);

	const directories: string[] = [];
	const storages = new Set<NodeNativeDurabilityStorage>();

	const temporaryDirectory = async (): Promise<string> => {
		const directory = await mkdtemp(
			join(tmpdir(), "peerbit-native-durability-storage-"),
		);
		directories.push(directory);
		return directory;
	};

	const openStorage = async (
		directory: string,
	): Promise<NodeNativeDurabilityStorage> => {
		const storage = await createNodeNativeDurabilityStorage({
			directory,
			programId: PROGRAM_ID,
		});
		storages.add(storage);
		return storage;
	};

	const closeStorage = async (
		storage: NodeNativeDurabilityStorage,
	): Promise<void> => {
		await storage.close();
		storages.delete(storage);
	};

	afterEach(async () => {
		await Promise.allSettled([...storages].map((storage) => storage.close()));
		storages.clear();
		await Promise.all(
			directories
				.splice(0)
				.map((directory) => rm(directory, { recursive: true, force: true })),
		);
	});

	it("creates canonical empty genesis state and reopens under a higher fence", async () => {
		const directory = await temporaryDirectory();
		const first = await openStorage(directory);

		expect(first.kind).to.equal("node-fsync");
		expect(first.crashSafe).to.equal(true);
		expect(first.version).to.equal(1);
		const genesis = await first.readLatestCheckpoint();
		expect(genesis?.generation).to.equal(0n);
		expect(genesis?.checkpointLsn).to.equal(0n);
		expect(genesis?.txSequenceHighwater).to.equal(0n);
		expect(genesis?.manifestSlot).to.equal("a");
		expect(genesis?.bytes).to.deep.equal(new Uint8Array());
		expect(genesis?.stagingCoverage).to.deep.equal([]);
		expect(genesis?.retainedTransactions).to.deep.equal([]);
		expect(await first.readJournal()).to.deep.equal(new Uint8Array());
		expect(await first.listStagingTransactionIds()).to.deep.equal([]);
		expect(await first.stats()).to.deep.equal({
			kind: "node-fsync",
			domainId: first.domainId,
			strictBarrierCount: 0n,
			strictDeleteCount: 0n,
			journalBytes: 0,
			stagingTransactions: 0,
			stagedBlocks: 0,
			stagedBytes: 0,
			checkpointGenerations: 1,
			checkpointBytes: 0,
		});

		const firstFence = first.fence;
		await closeStorage(first);
		const reopened = await openStorage(directory);
		const reopenedGenesis = await reopened.readLatestCheckpoint();
		expect(reopened.domainId).to.equal(firstFence.domainId);
		expect(reopened.fence.epoch).to.equal(firstFence.epoch + 1n);
		expect(reopened.fence.ownerId).to.not.equal(firstFence.ownerId);
		expect(reopenedGenesis?.generation).to.equal(0n);
		expect(reopenedGenesis?.originFence).to.deep.equal(firstFence);
	});

	it("rejects direct JavaScript construction outside the production factory", () => {
		const UnsafeStorage = NodeNativeDurabilityStorage as unknown as new (
			...args: unknown[]
		) => NodeNativeDurabilityStorage;
		expect(() => new UnsafeStorage()).to.throw(
			TypeError,
			"must be created by createNodeNativeDurabilityStorage",
		);
	});

	it("requires explicit migration for legacy data without creating a namespace", async () => {
		const directory = await temporaryDirectory();
		await writeFile(join(directory, "legacy-program.db"), new Uint8Array([1]));

		const error = await rejectedWith(
			createNodeNativeDurabilityStorage({ directory, programId: PROGRAM_ID }),
		);
		expect(error).to.be.instanceOf(NativeDurabilityMigrationRequiredError);
		expect(
			(error as NativeDurabilityMigrationRequiredError).directory,
		).to.equal(await realpath(directory));
		expect(await pathExists(durabilityPath(directory))).to.equal(false);

		const retryError = await rejectedWith(
			createNodeNativeDurabilityStorage({ directory, programId: PROGRAM_ID }),
		);
		expect(retryError).to.be.instanceOf(NativeDurabilityMigrationRequiredError);
	});

	it("rejects a second production factory while the first owns the directory", async () => {
		const directory = await temporaryDirectory();
		const first = await openStorage(directory);

		const error = await rejectedWith(
			createNodeNativeDurabilityStorage({ directory, programId: PROGRAM_ID }),
		);
		expect(error).to.be.instanceOf(NativeDurabilityLeaseUnavailableError);

		const firstFence = first.fence;
		await closeStorage(first);
		const reopened = await openStorage(directory);
		expect(reopened.fence.epoch).to.equal(firstFence.epoch + 1n);
	});

	it("stages isolated snapshots, rejects bad digests, and completes exact orphan retries", async () => {
		const directory = await temporaryDirectory();
		const storage = await openStorage(directory);
		const originalA = new Uint8Array([1, 2, 3, 4]);
		const digestA = digest(originalA);

		const digestError = await rejectedWith(
			storage.stageAndSync({
				scope: { transactionId: "bad-digest", txSequence: 1n, recordLsn: 1n },
				blocks: [
					{
						ordinal: 0,
						cid: "cid-bad",
						bytes: originalA,
						digest: new Uint8Array(32),
					},
				],
			}),
		);
		expect(digestError).to.be.instanceOf(NativeDurabilityDigestMismatchError);
		expect(await storage.listStagingTransactionIds()).to.deep.equal([]);

		const orphanDirectory = transactionDirectory(directory, "tx-a");
		await mkdir(orphanDirectory);
		await writeFile(join(orphanDirectory, "000000000000.block"), originalA);
		await writeFile(
			join(orphanDirectory, "000000000000.block.tmp-interrupted"),
			originalA.slice(0, 1),
		);

		const requestA: NativeDurabilityStageRequest = {
			scope: { transactionId: "tx-a", txSequence: 1n, recordLsn: 1n },
			blocks: [
				{
					ordinal: 0,
					cid: "cid-a",
					bytes: originalA,
					digest: new Uint8Array(digestA),
				},
			],
		};
		const stagedA = storage.stageAndSync(requestA);
		requestA.scope.transactionId = "mutated-transaction";
		requestA.blocks[0]!.cid = "mutated-cid";
		requestA.blocks[0]!.bytes.fill(0xff);
		requestA.blocks[0]!.digest.fill(0xee);
		const receiptA = await stagedA;
		expect(receiptA.transactionId).to.equal("tx-a");
		expect(receiptA.blocks[0]?.cid).to.equal("cid-a");
		expect(receiptA.blocks[0]?.digest).to.deep.equal(digestA);

		const originalB = new Uint8Array([9, 8, 7]);
		await storage.stageAndSync({
			scope: { transactionId: "tx-b", txSequence: 2n, recordLsn: 2n },
			blocks: [
				{
					ordinal: 0,
					cid: "cid-b",
					bytes: originalB,
					digest: digest(originalB),
				},
			],
		});

		expect(await storage.listStagingTransactionIds()).to.deep.equal([
			"tx-a",
			"tx-b",
		]);
		expect(await storage.readStagedBlock("tx-a", 0)).to.deep.equal(
			new Uint8Array([1, 2, 3, 4]),
		);
		expect(await storage.readStagedBlock("tx-b", 0)).to.deep.equal(originalB);
		expect(await storage.readStagedBlock("tx-a", 1)).to.equal(undefined);
		const stats = await storage.stats();
		expect(stats.strictBarrierCount).to.equal(2n);
		expect(stats.stagingTransactions).to.equal(2);
		expect(stats.stagedBlocks).to.equal(2);
		expect(stats.stagedBytes).to.equal(7);

		await closeStorage(storage);
		const reopened = await openStorage(directory);
		expect(await reopened.listStagingTransactionIds()).to.deep.equal([]);
	});

	it("derives append receipts from official frames and rejects untrusted metadata before writing", async () => {
		const directory = await temporaryDirectory();
		const storage = await openStorage(directory);
		const transaction = await transactionFrames(storage, "journal-tx", 1n, [
			NativeDurabilityPhase.DurablePrepared,
		]);
		const request = appendRequest(transaction);

		const digestError = await rejectedWith(
			storage.appendJournalAndSync({
				...request,
				framesDigest: new Uint8Array(32),
			}),
		);
		expect(digestError).to.be.instanceOf(NativeDurabilityDigestMismatchError);
		expect(await storage.readJournal()).to.deep.equal(new Uint8Array());

		const metadataError = await rejectedWith(
			storage.appendJournalAndSync({
				...request,
				transactionId: "caller-lie",
			}),
		);
		expect(metadataError).to.be.instanceOf(
			NativeDurabilityStorageCorruptionError,
		);
		expect(await storage.readJournal()).to.deep.equal(new Uint8Array());

		const originalFrames = new Uint8Array(request.frames);
		const originalFramesDigest = new Uint8Array(request.framesDigest);
		const append = storage.appendJournalAndSync(request);
		request.transactionId = "mutated-after-admission";
		request.txSequence = 99n;
		request.firstRecordLsn = 99n;
		request.lastRecordLsn = 99n;
		request.frames.fill(0xcc);
		request.framesDigest.fill(0xdd);
		const receipt = await append;
		const durable = await storage.readJournal();
		const decoded = transaction.codec.scan(durable).records[0]!;

		expect(durable).to.deep.equal(originalFrames);
		expect(receipt.transactionId).to.equal(decoded.transactionId);
		expect(receipt.txSequence).to.equal(decoded.txSequence);
		expect(receipt.firstRecordLsn).to.equal(decoded.recordLsn);
		expect(receipt.lastRecordLsn).to.equal(decoded.recordLsn);
		expect(receipt.fence).to.deep.equal(storage.fence);
		expect(receipt.framesDigest).to.deep.equal(originalFramesDigest);
		expect(receipt.offset).to.equal(0);
		expect(receipt.endOffset).to.equal(originalFrames.byteLength);
		const stats = await storage.stats();
		expect(stats.strictBarrierCount).to.equal(1n);
		expect(stats.journalBytes).to.equal(originalFrames.byteLength);
	});

	it("strictly confirms complete retries and resumes exact multi-frame prefixes", async () => {
		const completeDirectory = await temporaryDirectory();
		const first = await openStorage(completeDirectory);
		const completeTransaction = await transactionFrames(
			first,
			"complete-retry",
			1n,
			[
				NativeDurabilityPhase.DurablePrepared,
				NativeDurabilityPhase.NativeApplied,
			],
		);
		const completeRequest = appendRequest(completeTransaction);
		await writeFile(
			durabilityPath(completeDirectory, JOURNAL),
			completeTransaction.frames,
		);
		await closeStorage(first);

		const completeRetry = await openStorage(completeDirectory);
		const confirmed = await completeRetry.appendJournalAndSync(completeRequest);
		expect(confirmed.firstRecordLsn).to.equal(1n);
		expect(confirmed.lastRecordLsn).to.equal(2n);
		expect(await completeRetry.readJournal()).to.deep.equal(
			completeTransaction.frames,
		);
		await closeStorage(completeRetry);

		const boundaryDirectory = await temporaryDirectory();
		const boundaryFirst = await openStorage(boundaryDirectory);
		const boundaryTransaction = await transactionFrames(
			boundaryFirst,
			"boundary-retry",
			1n,
			[
				NativeDurabilityPhase.DurablePrepared,
				NativeDurabilityPhase.NativeApplied,
			],
		);
		await writeFile(
			durabilityPath(boundaryDirectory, JOURNAL),
			boundaryTransaction.individualFrames[0]!,
		);
		await closeStorage(boundaryFirst);
		const boundaryRetry = await openStorage(boundaryDirectory);
		await boundaryRetry.appendJournalAndSync(
			appendRequest(boundaryTransaction),
		);
		expect(await boundaryRetry.readJournal()).to.deep.equal(
			boundaryTransaction.frames,
		);
		await closeStorage(boundaryRetry);

		const partialDirectory = await temporaryDirectory();
		const partialFirst = await openStorage(partialDirectory);
		const partialTransaction = await transactionFrames(
			partialFirst,
			"partial-retry",
			1n,
			[
				NativeDurabilityPhase.DurablePrepared,
				NativeDurabilityPhase.NativeApplied,
			],
		);
		const firstFrame = partialTransaction.individualFrames[0]!;
		const partialSecond = partialTransaction.individualFrames[1]!.slice(0, 17);
		await writeFile(
			durabilityPath(partialDirectory, JOURNAL),
			concatBytes([firstFrame, partialSecond]),
		);
		await closeStorage(partialFirst);

		const partialRetry = await openStorage(partialDirectory);
		const truncated = await partialRetry.reconcileIncompleteJournalTailAndSync({
			transactionId: "partial-retry",
			txSequence: 1n,
		});
		expect(truncated.validLength).to.equal(firstFrame.byteLength);
		const resumed = await partialRetry.appendJournalAndSync(
			appendRequest(partialTransaction),
		);
		expect(resumed.lastRecordLsn).to.equal(2n);
		expect(await partialRetry.readJournal()).to.deep.equal(
			partialTransaction.frames,
		);
		const confirmedTruncation =
			await partialRetry.reconcileIncompleteJournalTailAndSync({
				transactionId: "partial-retry",
				txSequence: 1n,
			});
		expect(confirmedTruncation.previousLength).to.equal(
			partialTransaction.frames.byteLength,
		);
		expect(confirmedTruncation.validLength).to.equal(
			partialTransaction.frames.byteLength,
		);
	});

	it("reopens and reconciles only a structurally incomplete journal tail", async () => {
		const directory = await temporaryDirectory();
		const first = await openStorage(directory);
		const transaction = await transactionFrames(first, "torn-tx", 1n, [
			NativeDurabilityPhase.DurablePrepared,
			NativeDurabilityPhase.NativeApplied,
		]);
		const firstFrame = transaction.individualFrames[0]!;
		const secondFrame = transaction.individualFrames[1]!;
		await first.appendJournalAndSync({
			...appendRequest(transaction),
			lastRecordLsn: 1n,
			frames: firstFrame,
			framesDigest: digest(firstFrame),
		});
		await closeStorage(first);

		const journalPath = durabilityPath(directory, JOURNAL);
		const tornPrefix = secondFrame.subarray(0, 10);
		await appendFile(journalPath, tornPrefix);
		const reopened = await openStorage(directory);
		const receipt = await reopened.reconcileIncompleteJournalTailAndSync({
			transactionId: "torn-tx",
			txSequence: 1n,
		});

		expect(receipt.previousLength).to.equal(
			firstFrame.byteLength + tornPrefix.byteLength,
		);
		expect(receipt.validLength).to.equal(firstFrame.byteLength);
		expect(receipt.firstRecordLsn).to.equal(1n);
		expect(receipt.lastRecordLsn).to.equal(1n);
		expect(await reopened.readJournal()).to.deep.equal(firstFrame);
		expect((await reopened.stats()).strictBarrierCount).to.equal(1n);
	});

	it("never truncates a complete frame with checksum corruption", async () => {
		const directory = await temporaryDirectory();
		const storage = await openStorage(directory);
		const transaction = await transactionFrames(storage, "corrupt-tx", 1n, [
			NativeDurabilityPhase.DurablePrepared,
			NativeDurabilityPhase.NativeApplied,
		]);
		await storage.appendJournalAndSync(appendRequest(transaction));
		await closeStorage(storage);

		const journalPath = durabilityPath(directory, JOURNAL);
		const corrupt = new Uint8Array(await readFile(journalPath));
		corrupt[corrupt.byteLength - 20] ^= 1;
		await writeFile(journalPath, corrupt);

		const error = await rejectedWith(
			createNodeNativeDurabilityStorage({ directory, programId: PROGRAM_ID }),
		);
		expect(error).to.be.instanceOf(NativeDurabilityJournalCorruptionError);
		const after = new Uint8Array(await readFile(journalPath));
		expect(bytesEqual(after, corrupt)).to.equal(true);
		expect(after.byteLength).to.equal(corrupt.byteLength);
	});

	it("fails closed when an established journal is missing and preserves staging", async () => {
		const directory = await temporaryDirectory();
		const storage = await openStorage(directory);
		const transactionId = "missing-journal";
		const block = new Uint8Array([1, 4, 9]);
		const stageReceipt = await storage.stageAndSync({
			scope: { transactionId, txSequence: 1n, recordLsn: 1n },
			blocks: [
				{
					ordinal: 0,
					cid: "missing-journal-cid",
					bytes: block,
					digest: digest(block),
				},
			],
		});
		const transaction = await transactionFrames(storage, transactionId, 1n, [
			NativeDurabilityPhase.DurablePrepared,
			NativeDurabilityPhase.NativeApplied,
			NativeDurabilityPhase.Published,
			NativeDurabilityPhase.Committed,
			NativeDurabilityPhase.Clean,
		]);
		await storage.appendJournalAndSync(appendRequest(transaction));
		const checkpointBytes = new Uint8Array([5]);
		await storage.writeCheckpointAndSync({
			scope: { transactionId, txSequence: 1n, recordLsn: 5n },
			checkpointLsn: 5n,
			txSequenceHighwater: 1n,
			bytes: checkpointBytes,
			digest: digest(checkpointBytes),
			stagingCoverage: [
				{
					transactionId,
					txSequence: 1n,
					coveredThroughLsn: 5n,
					stagingManifestDigest: stageReceipt.manifestDigest,
				},
			],
			retainedTransactions: [
				{
					txSequence: 1n,
					transactionId,
					phase: NativeDurabilityPhase.Clean,
					operationKind: NativeDurabilityOperationKind.Append,
					planDigest: transaction.planDigest,
				},
			],
		});
		await closeStorage(storage);

		await rm(durabilityPath(directory, JOURNAL));
		await syncDirectoryPath(durabilityPath(directory));
		const error = await rejectedWith(openStorage(directory));
		expect(error).to.be.instanceOf(NativeDurabilityStorageCorruptionError);
		expect(
			await pathExists(transactionDirectory(directory, transactionId)),
		).to.equal(true);

		await writeFileAndSync(
			durabilityPath(directory, JOURNAL),
			new Uint8Array(),
		);
		const shortenedError = await rejectedWith(openStorage(directory));
		expect(shortenedError).to.be.instanceOf(
			NativeDurabilityStorageCorruptionError,
		);
		expect(
			await pathExists(transactionDirectory(directory, transactionId)),
		).to.equal(true);
	});

	it("reconciles checkpoint cut points and protects CLEAN deletion authorities", async () => {
		const directory = await temporaryDirectory();
		let storage = await openStorage(directory);
		const transactionId = "checkpoint-tx-1";
		const stagedBytes = new Uint8Array([4, 5, 6]);
		const stageReceipt = await storage.stageAndSync({
			scope: { transactionId, txSequence: 1n, recordLsn: 1n },
			blocks: [
				{
					ordinal: 0,
					cid: "checkpoint-cid",
					bytes: stagedBytes,
					digest: digest(stagedBytes),
				},
			],
		});
		const transactionOne = await transactionFrames(storage, transactionId, 1n, [
			NativeDurabilityPhase.DurablePrepared,
			NativeDurabilityPhase.NativeApplied,
			NativeDurabilityPhase.Published,
			NativeDurabilityPhase.Committed,
			NativeDurabilityPhase.Clean,
		]);
		await storage.appendJournalAndSync(appendRequest(transactionOne));

		const checkpointBytesOne = new Uint8Array([0xc0, 0xff, 0x01]);
		const retainedOne = {
			txSequence: 1n,
			transactionId,
			phase: NativeDurabilityPhase.Clean,
			operationKind: NativeDurabilityOperationKind.Append,
			planDigest: transactionOne.planDigest,
		};
		const coverage = {
			transactionId,
			txSequence: 1n,
			coveredThroughLsn: 5n,
			stagingManifestDigest: stageReceipt.manifestDigest,
		};
		const checkpointRequestOne: NativeDurabilityCheckpointRequest = {
			scope: { transactionId, txSequence: 1n, recordLsn: 5n },
			checkpointLsn: 5n,
			txSequenceHighwater: 1n,
			bytes: checkpointBytesOne,
			digest: digest(checkpointBytesOne),
			stagingCoverage: [coverage],
			retainedTransactions: [retainedOne],
		};

		const nonCleanError = await rejectedWith(
			storage.writeCheckpointAndSync({
				...checkpointRequestOne,
				retainedTransactions: [],
			}),
		);
		expect(nonCleanError).to.be.instanceOf(TypeError);
		const generationOne =
			await storage.writeCheckpointAndSync(checkpointRequestOne);
		expect(generationOne.generation).to.equal(1n);
		expect(
			(await storage.writeCheckpointAndSync(checkpointRequestOne)).generation,
		).to.equal(1n);
		const sameScopeConflict = await rejectedWith(
			storage.writeCheckpointAndSync({
				...checkpointRequestOne,
				bytes: new Uint8Array([9]),
				digest: digest(new Uint8Array([9])),
			}),
		);
		expect(sameScopeConflict).to.be.instanceOf(
			NativeDurabilityStorageCorruptionError,
		);

		const transactionTwo = await transactionFrames(
			storage,
			"checkpoint-tx-2",
			2n,
			[
				NativeDurabilityPhase.DurablePrepared,
				NativeDurabilityPhase.NativeApplied,
				NativeDurabilityPhase.Published,
				NativeDurabilityPhase.Committed,
				NativeDurabilityPhase.Clean,
			],
			6n,
		);
		await storage.appendJournalAndSync(
			appendRequest(transactionTwo, (await storage.readJournal()).byteLength),
		);
		const retainedTwo = {
			txSequence: 2n,
			transactionId: "checkpoint-tx-2",
			phase: NativeDurabilityPhase.Clean,
			operationKind: NativeDurabilityOperationKind.Append,
			planDigest: transactionTwo.planDigest,
		};
		const checkpointBytesTwo = new Uint8Array([0xc0, 0xff, 0x02]);
		const checkpointRequestTwo: NativeDurabilityCheckpointRequest = {
			scope: {
				transactionId: "checkpoint-tx-2",
				txSequence: 2n,
				recordLsn: 10n,
			},
			checkpointLsn: 10n,
			txSequenceHighwater: 2n,
			bytes: checkpointBytesTwo,
			digest: digest(checkpointBytesTwo),
			stagingCoverage: [coverage],
			retainedTransactions: [retainedOne, retainedTwo],
		};
		const checkpointDirectory = durabilityPath(directory, CHECKPOINTS);
		const requestDigestOne = digest(
			encodeNativeDurabilityCanonical(checkpointRequestOne),
		);
		const requestDigestTwo = digest(
			encodeNativeDurabilityCanonical(checkpointRequestTwo),
		);
		const checkpointFence = storage.fence;
		await closeStorage(storage);

		await writeFileAndSync(
			join(checkpointDirectory, "generation-highwater.json"),
			encodeManifestEnvelope({
				version: 1,
				generation: "2",
				pending: {
					generation: "2",
					requestDigest: Buffer.from(requestDigestTwo).toString("hex"),
					transactionId: "checkpoint-tx-2",
					txSequence: "2",
				},
				completed: {
					generation: "1",
					requestDigest: Buffer.from(requestDigestOne).toString("hex"),
					transactionId,
					txSequence: "1",
				},
			}),
		);
		await writeFileAndSync(
			join(checkpointDirectory, "checkpoint-2.bin"),
			checkpointBytesTwo,
		);
		await writeFileAndSync(
			join(checkpointDirectory, "manifest-a.json"),
			encodeManifestEnvelope({
				version: 1,
				programId: Buffer.from(PROGRAM_ID).toString("hex"),
				generation: "2",
				checkpointLsn: "10",
				txSequenceHighwater: "2",
				file: "checkpoint-2.bin",
				byteLength: checkpointBytesTwo.byteLength,
				digest: Buffer.from(digest(checkpointBytesTwo)).toString("hex"),
				originFence: {
					epoch: checkpointFence.epoch.toString(),
					ownerId: checkpointFence.ownerId,
					domainId: checkpointFence.domainId,
				},
				stagingCoverage: [
					{
						transactionId,
						txSequence: "1",
						coveredThroughLsn: "5",
						stagingManifestDigest: Buffer.from(
							coverage.stagingManifestDigest,
						).toString("hex"),
					},
				],
				retainedTransactions: [retainedOne, retainedTwo].map((retained) => ({
					txSequence: retained.txSequence.toString(),
					transactionId: retained.transactionId,
					phase: retained.phase,
					operationKind: retained.operationKind,
					planDigest: Buffer.from(retained.planDigest).toString("hex"),
				})),
			}),
		);
		await rm(
			join(transactionDirectory(directory, transactionId), "manifest.json"),
		);
		await syncDirectoryPath(transactionDirectory(directory, transactionId));

		storage = await openStorage(directory);
		expect(
			await pathExists(transactionDirectory(directory, transactionId)),
		).to.equal(false);
		expect(
			(await storage.writeCheckpointAndSync(checkpointRequestTwo)).generation,
		).to.equal(2n);

		const transactionThree = await transactionFrames(
			storage,
			"checkpoint-tx-3",
			3n,
			[
				NativeDurabilityPhase.DurablePrepared,
				NativeDurabilityPhase.NativeApplied,
				NativeDurabilityPhase.Published,
				NativeDurabilityPhase.Committed,
				NativeDurabilityPhase.Clean,
			],
			11n,
		);
		await storage.appendJournalAndSync(
			appendRequest(transactionThree, (await storage.readJournal()).byteLength),
		);
		const retainedThree = {
			txSequence: 3n,
			transactionId: "checkpoint-tx-3",
			phase: NativeDurabilityPhase.Clean,
			operationKind: NativeDurabilityOperationKind.Append,
			planDigest: transactionThree.planDigest,
		};
		const checkpointBytesThree = new Uint8Array([0xc0, 0xff, 0x03]);
		const generationThree = await storage.writeCheckpointAndSync({
			scope: {
				transactionId: "checkpoint-tx-3",
				txSequence: 3n,
				recordLsn: 15n,
			},
			checkpointLsn: 15n,
			txSequenceHighwater: 3n,
			bytes: checkpointBytesThree,
			digest: digest(checkpointBytesThree),
			stagingCoverage: [],
			retainedTransactions: [retainedOne, retainedTwo, retainedThree],
		});
		expect(generationThree.generation).to.equal(3n);

		for (const generation of [3n, 2n, 0n]) {
			const error = await rejectedWith(
				storage.deleteAndSync({
					scope: {
						transactionId: "checkpoint-tx-3",
						txSequence: 3n,
						recordLsn: 15n,
					},
					targets: [{ kind: "checkpoint", generation }],
				}),
			);
			expect(error).to.be.instanceOf(NativeDurabilityStorageCorruptionError);
		}
		await storage.deleteAndSync({
			scope: {
				transactionId: "checkpoint-tx-3",
				txSequence: 3n,
				recordLsn: 15n,
			},
			targets: [{ kind: "checkpoint", generation: 1n }],
		});
		await storage.deleteAndSync({
			scope: {
				transactionId: "checkpoint-tx-3",
				txSequence: 3n,
				recordLsn: 15n,
			},
			targets: [{ kind: "staging", transactionId }],
		});

		const stats = await storage.stats();
		expect(stats.strictBarrierCount).to.equal(5n);
		expect(stats.strictDeleteCount).to.equal(2n);
		expect(stats.stagingTransactions).to.equal(0);
		expect(stats.checkpointGenerations).to.equal(3);
		expect(stats.checkpointBytes).to.equal(
			checkpointBytesTwo.byteLength + checkpointBytesThree.byteLength,
		);

		await closeStorage(storage);
		const reopened = await openStorage(directory);
		const latest = await reopened.readLatestCheckpoint();
		expect(latest?.generation).to.equal(3n);
		expect(latest?.bytes).to.deep.equal(checkpointBytesThree);
		expect(await reopened.listStagingTransactionIds()).to.deep.equal([]);
	});

	it("abandons an unselected pending checkpoint without reusing its generation", async () => {
		const directory = await temporaryDirectory();
		let storage = await openStorage(directory);
		const phases = [
			NativeDurabilityPhase.DurablePrepared,
			NativeDurabilityPhase.NativeApplied,
			NativeDurabilityPhase.Published,
			NativeDurabilityPhase.Committed,
			NativeDurabilityPhase.Clean,
		];
		const transactionOne = await transactionFrames(
			storage,
			"pending-tx-1",
			1n,
			phases,
		);
		await storage.appendJournalAndSync(appendRequest(transactionOne));
		const retainedOne = {
			txSequence: 1n,
			transactionId: "pending-tx-1",
			phase: NativeDurabilityPhase.Clean,
			operationKind: NativeDurabilityOperationKind.Append,
			planDigest: transactionOne.planDigest,
		};
		const bytesOne = new Uint8Array([1]);
		const requestOne: NativeDurabilityCheckpointRequest = {
			scope: { transactionId: "pending-tx-1", txSequence: 1n, recordLsn: 5n },
			checkpointLsn: 5n,
			txSequenceHighwater: 1n,
			bytes: bytesOne,
			digest: digest(bytesOne),
			stagingCoverage: [],
			retainedTransactions: [retainedOne],
		};
		expect(
			(await storage.writeCheckpointAndSync(requestOne)).generation,
		).to.equal(1n);

		const transactionTwo = await transactionFrames(
			storage,
			"pending-tx-2",
			2n,
			phases,
			6n,
		);
		await storage.appendJournalAndSync(
			appendRequest(transactionTwo, (await storage.readJournal()).byteLength),
		);
		const retainedTwo = {
			txSequence: 2n,
			transactionId: "pending-tx-2",
			phase: NativeDurabilityPhase.Clean,
			operationKind: NativeDurabilityOperationKind.Append,
			planDigest: transactionTwo.planDigest,
		};
		const bytesTwo = new Uint8Array([2]);
		const requestTwo: NativeDurabilityCheckpointRequest = {
			scope: { transactionId: "pending-tx-2", txSequence: 2n, recordLsn: 10n },
			checkpointLsn: 10n,
			txSequenceHighwater: 2n,
			bytes: bytesTwo,
			digest: digest(bytesTwo),
			stagingCoverage: [],
			retainedTransactions: [retainedOne, retainedTwo],
		};
		await closeStorage(storage);

		const checkpointDirectory = durabilityPath(directory, CHECKPOINTS);
		await writeFileAndSync(
			join(checkpointDirectory, "generation-highwater.json"),
			encodeManifestEnvelope({
				version: 1,
				generation: "2",
				pending: {
					generation: "2",
					requestDigest: Buffer.from(
						digest(encodeNativeDurabilityCanonical(requestTwo)),
					).toString("hex"),
					transactionId: "pending-tx-2",
					txSequence: "2",
				},
				completed: {
					generation: "1",
					requestDigest: Buffer.from(
						digest(encodeNativeDurabilityCanonical(requestOne)),
					).toString("hex"),
					transactionId: "pending-tx-1",
					txSequence: "1",
				},
			}),
		);
		const interruptedTemporary = join(
			checkpointDirectory,
			"checkpoint-2.bin.tmp-interrupted",
		);
		await writeFileAndSync(interruptedTemporary, new Uint8Array([2, 2]));

		storage = await openStorage(directory);
		expect(await pathExists(interruptedTemporary)).to.equal(false);
		expect(
			await pathExists(join(checkpointDirectory, "checkpoint-2.bin")),
		).to.equal(false);
		expect((await storage.readLatestCheckpoint())?.generation).to.equal(1n);
		const recovered = await storage.writeCheckpointAndSync(requestTwo);
		expect(recovered.generation).to.equal(4n);
		expect(recovered.manifestSlot).to.equal("a");
	});

	it("fails closed when an A/B manifest is corrupt or the completed active slot is missing", async () => {
		const directory = await temporaryDirectory();
		const storage = await openStorage(directory);
		const transaction = await transactionFrames(
			storage,
			"manifest-corruption",
			1n,
			[
				NativeDurabilityPhase.DurablePrepared,
				NativeDurabilityPhase.NativeApplied,
				NativeDurabilityPhase.Published,
				NativeDurabilityPhase.Committed,
				NativeDurabilityPhase.Clean,
			],
		);
		await storage.appendJournalAndSync(appendRequest(transaction));
		const checkpointBytes = new Uint8Array([7]);
		await storage.writeCheckpointAndSync({
			scope: {
				transactionId: "manifest-corruption",
				txSequence: 1n,
				recordLsn: 5n,
			},
			checkpointLsn: 5n,
			txSequenceHighwater: 1n,
			bytes: checkpointBytes,
			digest: digest(checkpointBytes),
			stagingCoverage: [],
			retainedTransactions: [
				{
					txSequence: 1n,
					transactionId: "manifest-corruption",
					phase: NativeDurabilityPhase.Clean,
					operationKind: NativeDurabilityOperationKind.Append,
					planDigest: transaction.planDigest,
				},
			],
		});
		await closeStorage(storage);
		const manifestA = durabilityPath(directory, CHECKPOINTS, "manifest-a.json");
		const originalManifestA = new Uint8Array(await readFile(manifestA));
		const corrupt = new Uint8Array(originalManifestA);
		corrupt[corrupt.byteLength - 2] ^= 1;
		await writeFileAndSync(manifestA, corrupt);

		const error = await rejectedWith(openStorage(directory));
		expect(error).to.be.instanceOf(NativeDurabilityStorageCorruptionError);

		await writeFileAndSync(manifestA, originalManifestA);
		await rm(durabilityPath(directory, CHECKPOINTS, "manifest-b.json"), {
			force: true,
		});
		await syncDirectoryPath(durabilityPath(directory, CHECKPOINTS));
		const missingActiveError = await rejectedWith(openStorage(directory));
		expect(missingActiveError).to.be.instanceOf(
			NativeDurabilityStorageCorruptionError,
		);
	});

	it("drains admitted snapshots on close and rejects operations admitted afterward", async () => {
		const directory = await temporaryDirectory();
		const storage = await openStorage(directory);
		const original = new Uint8Array(2 * 1024 * 1024).fill(0x5a);
		const expected = new Uint8Array(original);
		const request: NativeDurabilityStageRequest = {
			scope: { transactionId: "closing-tx", txSequence: 1n, recordLsn: 1n },
			blocks: [
				{
					ordinal: 0,
					cid: "closing-cid",
					bytes: original,
					digest: digest(original),
				},
			],
		};

		const admitted = storage.stageAndSync(request);
		request.blocks[0]!.bytes.fill(0);
		request.blocks[0]!.digest.fill(0);
		const closing = storage.close();
		expect(storage.close()).to.equal(closing);
		const lateError = await rejectedWith(storage.stats());
		expect(lateError).to.be.instanceOf(NativeDurabilityStorageClosedError);
		const receipt = await admitted;
		await closing;
		storages.delete(storage);
		expect(receipt.transactionId).to.equal("closing-tx");
		expect(
			new Uint8Array(
				await readFile(
					join(
						transactionDirectory(directory, "closing-tx"),
						"000000000000.block",
					),
				),
			),
		).to.deep.equal(expected);

		const reopened = await openStorage(directory);
		const durable = await reopened.readStagedBlock("closing-tx", 0);
		expect(durable).to.equal(undefined);
		expect(await reopened.listStagingTransactionIds()).to.deep.equal([]);
	});
});
