import { expect } from "chai";
import {
	type NativeDurabilityCheckpointTransactionState,
	NativeDurabilityOperationKind,
	NativeDurabilityPhase,
} from "../src/durability/codec.js";
import type { NativeDurabilityLease } from "../src/durability/lease.js";
import { createMemoryNativeDurabilityStorage } from "../src/durability/memory-storage.js";
import {
	type NativeDurabilityCheckpointRequest,
	NativeDurabilityIncompleteTailMismatchError,
	type NativeDurabilityJournalClassification,
	type NativeDurabilityJournalClassifier,
	type NativeDurabilityOperationScope,
	type NativeDurabilityStagingCoverage,
	NativeDurabilityStorageClosedError,
	copyNativeDurabilityBytes,
	sha256NativeDurability,
} from "../src/durability/storage.js";

const bytes = (...values: number[]): Uint8Array => new Uint8Array(values);

const concat = (...chunks: Uint8Array[]): Uint8Array => {
	const result = new Uint8Array(
		chunks.reduce((length, chunk) => length + chunk.byteLength, 0),
	);
	let offset = 0;
	for (const chunk of chunks) {
		result.set(chunk, offset);
		offset += chunk.byteLength;
	}
	return result;
};

const rejected = async (promise: Promise<unknown>): Promise<unknown> => {
	try {
		await promise;
		expect.fail("Expected promise to reject");
	} catch (error) {
		return error;
	}
};

class TestLease implements NativeDurabilityLease {
	readonly fence = Object.freeze({
		epoch: 1n,
		ownerId: "memory-writer",
		domainId: "memory-domain",
	});

	private held = true;
	private nextGate?: {
		entered: () => void;
		wait: Promise<void>;
	};

	blockNextOperation(): { entered: Promise<void>; release: () => void } {
		let markEntered!: () => void;
		let release!: () => void;
		const entered = new Promise<void>((resolve) => {
			markEntered = resolve;
		});
		const wait = new Promise<void>((resolve) => {
			release = resolve;
		});
		this.nextGate = { entered: markEntered, wait };
		return { entered, release };
	}

	async assertHeld(): Promise<void> {
		if (!this.held) throw new Error("lease closed");
	}

	async runWhileHeld<T>(operation: () => Promise<T>): Promise<T> {
		await this.assertHeld();
		const gate = this.nextGate;
		this.nextGate = undefined;
		if (gate) {
			gate.entered();
			await gate.wait;
		}
		return operation();
	}

	async close(): Promise<void> {
		this.held = false;
	}
}

const completeClassifier: NativeDurabilityJournalClassifier = {
	classify: (journal) => ({
		kind: "complete",
		validLength: journal.byteLength,
		lastRecordLsn: 0n,
	}),
};

const scope = (
	transactionId: string,
	txSequence: bigint,
	recordLsn: bigint,
): NativeDurabilityOperationScope => ({ transactionId, txSequence, recordLsn });

const stage = async (
	storage: ReturnType<typeof createMemoryNativeDurabilityStorage>,
	transactionId: string,
	txSequence: bigint,
	recordLsn: bigint,
	value: number,
) => {
	const block = bytes(value, value + 1);
	return storage.stageAndSync({
		scope: scope(transactionId, txSequence, recordLsn),
		blocks: [
			{
				ordinal: 0,
				cid: `cid-${transactionId}`,
				bytes: block,
				digest: await sha256NativeDurability(block),
			},
		],
	});
};

const checkpointRequest = async (options: {
	transactionId: string;
	txSequence: bigint;
	recordLsn: bigint;
	checkpointLsn: bigint;
	txSequenceHighwater: bigint;
	value: number;
	stagingCoverage?: NativeDurabilityStagingCoverage[];
	retainedTransactions?: NativeDurabilityCheckpointTransactionState[];
}): Promise<NativeDurabilityCheckpointRequest> => {
	const checkpointBytes = bytes(options.value, options.value + 1);
	return {
		scope: scope(options.transactionId, options.txSequence, options.recordLsn),
		checkpointLsn: options.checkpointLsn,
		txSequenceHighwater: options.txSequenceHighwater,
		bytes: checkpointBytes,
		digest: await sha256NativeDurability(checkpointBytes),
		stagingCoverage: options.stagingCoverage ?? [],
		retainedTransactions: options.retainedTransactions ?? [],
	};
};

const retainedClean = (
	transactionId: string,
	txSequence: bigint,
	value: number,
): NativeDurabilityCheckpointTransactionState => ({
	txSequence,
	transactionId,
	phase: NativeDurabilityPhase.Clean,
	operationKind: NativeDurabilityOperationKind.Append,
	planDigest: new Uint8Array(32).fill(value),
});

describe("native durability memory transaction storage", () => {
	it("snapshots and deeply clones checkpoint highwaters and retained transactions", async () => {
		const storage = createMemoryNativeDurabilityStorage(
			new TestLease(),
			completeClassifier,
		);
		const staged = await stage(storage, "tx-1", 1n, 1n, 10);
		const coverageDigest = copyNativeDurabilityBytes(staged.manifestDigest);
		const retainedPlanDigest = new Uint8Array(32).fill(7);
		const request = await checkpointRequest({
			transactionId: "tx-1",
			txSequence: 1n,
			recordLsn: 1n,
			checkpointLsn: 5n,
			txSequenceHighwater: 3n,
			value: 21,
			stagingCoverage: [
				{
					transactionId: "tx-1",
					txSequence: 1n,
					coveredThroughLsn: 5n,
					stagingManifestDigest: coverageDigest,
				},
			],
			retainedTransactions: [
				{
					txSequence: 1n,
					transactionId: "tx-1",
					phase: NativeDurabilityPhase.Clean,
					operationKind: NativeDurabilityOperationKind.Append,
					planDigest: retainedPlanDigest,
				},
			],
		});
		const originalBytes = copyNativeDurabilityBytes(request.bytes);
		const originalDigest = copyNativeDurabilityBytes(request.digest);
		const originalCoverageDigest = copyNativeDurabilityBytes(coverageDigest);
		const originalPlanDigest = copyNativeDurabilityBytes(retainedPlanDigest);

		const writing = storage.writeCheckpointAndSync(request);
		request.bytes.fill(99);
		request.digest.fill(98);
		coverageDigest.fill(97);
		retainedPlanDigest.fill(96);
		const receipt = await writing;

		expect(receipt.checkpointDigest).to.deep.equal(originalDigest);
		const first = await storage.readLatestCheckpoint();
		expect(first?.txSequenceHighwater).to.equal(3n);
		expect(first?.bytes).to.deep.equal(originalBytes);
		expect(first?.digest).to.deep.equal(originalDigest);
		expect(first?.stagingCoverage[0]?.stagingManifestDigest).to.deep.equal(
			originalCoverageDigest,
		);
		expect(first?.retainedTransactions[0]?.planDigest).to.deep.equal(
			originalPlanDigest,
		);

		first!.bytes.fill(1);
		first!.digest.fill(2);
		first!.stagingCoverage[0]!.stagingManifestDigest.fill(3);
		first!.retainedTransactions[0]!.planDigest.fill(4);
		receipt.checkpointDigest.fill(5);
		const second = await storage.readLatestCheckpoint();
		expect(second?.bytes).to.deep.equal(originalBytes);
		expect(second?.digest).to.deep.equal(originalDigest);
		expect(second?.stagingCoverage[0]?.stagingManifestDigest).to.deep.equal(
			originalCoverageDigest,
		);
		expect(second?.retainedTransactions[0]?.planDigest).to.deep.equal(
			originalPlanDigest,
		);
	});

	it("requires exact active-checkpoint coverage and validates a delete batch before mutation", async () => {
		const storage = createMemoryNativeDurabilityStorage(
			new TestLease(),
			completeClassifier,
		);
		const tx1 = await stage(storage, "tx-1", 1n, 1n, 1);
		await stage(storage, "tx-2", 2n, 2n, 2);
		const invalidCheckpoint = await rejected(
			storage.writeCheckpointAndSync(
				await checkpointRequest({
					transactionId: "tx-2",
					txSequence: 2n,
					recordLsn: 2n,
					checkpointLsn: 3n,
					txSequenceHighwater: 2n,
					value: 29,
					stagingCoverage: [
						{
							transactionId: "tx-1",
							txSequence: 1n,
							coveredThroughLsn: 3n,
							stagingManifestDigest: tx1.manifestDigest,
						},
					],
				}),
			),
		);
		expect(String(invalidCheckpoint)).to.contain("exact retained CLEAN");
		expect((await storage.stats()).checkpointGenerations).to.equal(0);
		const generation1 = await storage.writeCheckpointAndSync(
			await checkpointRequest({
				transactionId: "tx-2",
				txSequence: 2n,
				recordLsn: 2n,
				checkpointLsn: 3n,
				txSequenceHighwater: 2n,
				value: 30,
				stagingCoverage: [
					{
						transactionId: "tx-1",
						txSequence: 1n,
						coveredThroughLsn: 3n,
						stagingManifestDigest: tx1.manifestDigest,
					},
				],
				retainedTransactions: [retainedClean("tx-1", 1n, 1)],
			}),
		);

		const uncovered = await rejected(
			storage.deleteAndSync({
				scope: scope("tx-2", 2n, 4n),
				targets: [{ kind: "staging", transactionId: "tx-2" }],
			}),
		);
		expect(String(uncovered)).to.contain("does not cover");
		expect(await storage.readStagingManifest("tx-2")).to.not.equal(undefined);

		await storage.deleteAndSync({
			scope: scope("tx-1", 1n, 4n),
			targets: [{ kind: "staging", transactionId: "tx-1" }],
		});
		expect(await storage.readStagingManifest("tx-1")).to.equal(undefined);

		const generation2 = await storage.writeCheckpointAndSync(
			await checkpointRequest({
				transactionId: "tx-2",
				txSequence: 2n,
				recordLsn: 4n,
				checkpointLsn: 4n,
				txSequenceHighwater: 2n,
				value: 31,
			}),
		);
		const generation3 = await storage.writeCheckpointAndSync(
			await checkpointRequest({
				transactionId: "tx-2",
				txSequence: 2n,
				recordLsn: 5n,
				checkpointLsn: 5n,
				txSequenceHighwater: 2n,
				value: 32,
			}),
		);
		for (const protectedGeneration of [
			generation2.generation,
			generation3.generation,
		]) {
			const error = await rejected(
				storage.deleteAndSync({
					scope: scope("tx-2", 2n, 6n),
					targets: [{ kind: "checkpoint", generation: protectedGeneration }],
				}),
			);
			expect(String(error)).to.contain("active or previous");
		}
		await storage.deleteAndSync({
			scope: scope("tx-2", 2n, 6n),
			targets: [{ kind: "checkpoint", generation: generation1.generation }],
		});

		const tx3 = await stage(storage, "tx-3", 3n, 6n, 3);
		const generation4 = await storage.writeCheckpointAndSync(
			await checkpointRequest({
				transactionId: "tx-3",
				txSequence: 3n,
				recordLsn: 6n,
				checkpointLsn: 7n,
				txSequenceHighwater: 3n,
				value: 33,
				stagingCoverage: [
					{
						transactionId: "tx-3",
						txSequence: 3n,
						coveredThroughLsn: 7n,
						stagingManifestDigest: tx3.manifestDigest,
					},
				],
				retainedTransactions: [retainedClean("tx-3", 3n, 3)],
			}),
		);
		await rejected(
			storage.deleteAndSync({
				scope: scope("tx-3", 3n, 8n),
				targets: [
					{ kind: "staging", transactionId: "tx-3" },
					{ kind: "checkpoint", generation: generation4.generation },
				],
			}),
		);
		expect(await storage.readStagingManifest("tx-3")).to.not.equal(undefined);
	});

	it("serializes admitted operations and drains them before closing", async () => {
		const lease = new TestLease();
		const storage = createMemoryNativeDurabilityStorage(
			lease,
			completeClassifier,
		);
		const firstBytes = bytes(1, 2);
		const secondBytes = bytes(3, 4, 5);
		const gate = lease.blockNextOperation();
		const first = storage.appendJournalAndSync({
			transactionId: "tx-1",
			txSequence: 1n,
			firstRecordLsn: 1n,
			lastRecordLsn: 1n,
			expectedOffset: 0,
			frames: firstBytes,
			framesDigest: await sha256NativeDurability(firstBytes),
		});
		const second = storage.appendJournalAndSync({
			transactionId: "tx-2",
			txSequence: 2n,
			firstRecordLsn: 2n,
			lastRecordLsn: 2n,
			expectedOffset: firstBytes.byteLength,
			frames: secondBytes,
			framesDigest: await sha256NativeDurability(secondBytes),
		});
		const queuedRead = storage.readJournal();
		let closeResolved = false;
		const closing = storage.close().then(() => {
			closeResolved = true;
		});

		await gate.entered;
		expect(closeResolved).to.equal(false);
		const lateError = await rejected(storage.readJournal());
		expect(lateError).to.be.instanceOf(NativeDurabilityStorageClosedError);
		gate.release();
		const [, , journal] = await Promise.all([
			first,
			second,
			queuedRead,
			closing,
		]);
		expect(journal).to.deep.equal(concat(firstBytes, secondBytes));
		expect(closeResolved).to.equal(true);
	});

	it("accepts truncation only from a defensively validated classifier result", async () => {
		let classify = (
			_journal: Uint8Array,
		): NativeDurabilityJournalClassification => ({
			kind: "incomplete-tail",
			validLength: 1,
			lastRecordLsn: "1" as unknown as bigint,
			reason: "short-body",
		});
		const classifier: NativeDurabilityJournalClassifier = {
			classify: (journal) => classify(journal),
		};
		const storage = createMemoryNativeDurabilityStorage(
			new TestLease(),
			classifier,
		);
		const original = bytes(1, 2, 3);
		await storage.appendJournalAndSync({
			transactionId: "tx-1",
			txSequence: 1n,
			firstRecordLsn: 1n,
			lastRecordLsn: 1n,
			expectedOffset: 0,
			frames: original,
			framesDigest: await sha256NativeDurability(original),
		});

		for (const invalid of [
			() => classify,
			() => (_journal: Uint8Array) =>
				({
					kind: "incomplete-tail",
					validLength: 1,
					lastRecordLsn: 1n,
					reason: "checksum-corruption",
				}) as unknown as NativeDurabilityJournalClassification,
		]) {
			classify = invalid() as typeof classify;
			const error = await rejected(
				storage.reconcileIncompleteJournalTailAndSync({
					transactionId: "tx-1",
					txSequence: 1n,
				}),
			);
			expect(error).to.be.instanceOf(
				NativeDurabilityIncompleteTailMismatchError,
			);
			expect(await storage.readJournal()).to.deep.equal(original);
		}

		classify = (journal) => {
			journal[0] = 99;
			return {
				kind: "incomplete-tail",
				validLength: 2,
				lastRecordLsn: 1n,
				reason: "short-body",
			};
		};
		const receipt = await storage.reconcileIncompleteJournalTailAndSync({
			transactionId: "tx-1",
			txSequence: 1n,
		});
		expect(receipt.validLength).to.equal(2);
		expect(await storage.readJournal()).to.deep.equal(bytes(1, 2));
	});
});
