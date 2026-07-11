import {
	NATIVE_DURABILITY_MAX_U64,
	type NativeDurabilityLease,
} from "./lease.js";
import {
	NATIVE_DURABILITY_STORAGE_VERSION,
	type NativeDurabilityCheckpoint,
	type NativeDurabilityCheckpointReceipt,
	type NativeDurabilityCheckpointRequest,
	type NativeDurabilityDeleteReceipt,
	type NativeDurabilityDeleteRequest,
	NativeDurabilityDigestMismatchError,
	NativeDurabilityIncompleteTailMismatchError,
	type NativeDurabilityIncompleteTailReconciliationRequest,
	type NativeDurabilityJournalAppendRequest,
	type NativeDurabilityJournalClassifier,
	NativeDurabilityJournalOffsetConflictError,
	type NativeDurabilityJournalReceipt,
	type NativeDurabilityJournalReconciliationReceipt,
	type NativeDurabilityStageReceipt,
	type NativeDurabilityStageRequest,
	type NativeDurabilityStagedBlockReference,
	type NativeDurabilityStagingManifest,
	type NativeDurabilityStorage,
	NativeDurabilityStorageClosedError,
	type NativeDurabilityStorageStats,
	assertNativeDurabilityCheckpointRequest,
	assertNativeDurabilityDeleteRequest,
	assertNativeDurabilityFence,
	assertNativeDurabilityJournalAppendRequest,
	assertNativeDurabilityOperationScope,
	assertNativeDurabilityStageRequest,
	copyNativeDurabilityBytes,
	nativeDurabilityBytesEqual,
	nativeDurabilityScopeDigest,
	sha256NativeDurability,
} from "./storage.js";

const cloneFence = (
	fence: NativeDurabilityLease["fence"],
): NativeDurabilityLease["fence"] => ({ ...fence });

const cloneScope = <T extends { transactionId: string; txSequence: bigint }>(
	scope: T,
): T => ({ ...scope });

const cloneRetainedTransactions = (
	transactions: NativeDurabilityCheckpointRequest["retainedTransactions"],
): NativeDurabilityCheckpoint["retainedTransactions"] =>
	transactions.map((transaction) => ({
		...transaction,
		planDigest: copyNativeDurabilityBytes(transaction.planDigest),
	}));

const snapshotStageRequest = (
	request: NativeDurabilityStageRequest,
): NativeDurabilityStageRequest => {
	assertNativeDurabilityStageRequest(request);
	return {
		scope: { ...request.scope },
		blocks: [...request.blocks]
			.sort((left, right) => left.ordinal - right.ordinal)
			.map((block) => ({
				...block,
				bytes: copyNativeDurabilityBytes(block.bytes),
				digest: copyNativeDurabilityBytes(block.digest),
			})),
	};
};

const snapshotJournalRequest = (
	request: NativeDurabilityJournalAppendRequest,
): NativeDurabilityJournalAppendRequest => {
	assertNativeDurabilityJournalAppendRequest(request);
	return {
		...request,
		frames: copyNativeDurabilityBytes(request.frames),
		framesDigest: copyNativeDurabilityBytes(request.framesDigest),
	};
};

const snapshotCheckpointRequest = (
	request: NativeDurabilityCheckpointRequest,
): NativeDurabilityCheckpointRequest => {
	assertNativeDurabilityCheckpointRequest(request);
	return {
		...request,
		scope: { ...request.scope },
		bytes: copyNativeDurabilityBytes(request.bytes),
		digest: copyNativeDurabilityBytes(request.digest),
		stagingCoverage: request.stagingCoverage.map((coverage) => ({
			...coverage,
			stagingManifestDigest: copyNativeDurabilityBytes(
				coverage.stagingManifestDigest,
			),
		})),
		retainedTransactions: cloneRetainedTransactions(
			request.retainedTransactions,
		),
	};
};

const snapshotDeleteRequest = (
	request: NativeDurabilityDeleteRequest,
): NativeDurabilityDeleteRequest => {
	assertNativeDurabilityDeleteRequest(request);
	return {
		scope: { ...request.scope },
		targets: request.targets.map((target) => ({ ...target })),
	};
};

export class MemoryNativeDurabilityStorage implements NativeDurabilityStorage {
	readonly version = NATIVE_DURABILITY_STORAGE_VERSION;
	readonly kind = "memory" as const;
	readonly crashSafe = false;
	readonly domainId: string;
	readonly fence: NativeDurabilityLease["fence"];

	private barrierOrdinal = 0n;
	private strictDeleteCount = 0n;
	private journal = new Uint8Array();
	private readonly staging = new Map<
		string,
		{
			manifest: NativeDurabilityStagingManifest;
			blocks: Map<number, Uint8Array>;
		}
	>();
	private readonly checkpoints = new Map<bigint, NativeDurabilityCheckpoint>();
	private generationHighwater = 0n;
	private operationTail: Promise<void> = Promise.resolve();
	private closing = false;
	private closed = false;

	constructor(
		private readonly lease: NativeDurabilityLease,
		private readonly journalClassifier: NativeDurabilityJournalClassifier,
	) {
		assertNativeDurabilityFence(lease.fence);
		this.domainId = lease.fence.domainId;
		this.fence = Object.freeze({ ...lease.fence });
	}

	private barrier(): bigint {
		this.barrierOrdinal++;
		return this.barrierOrdinal;
	}

	private enqueue<T>(operation: () => Promise<T>): Promise<T> {
		if (this.closing || this.closed) {
			return Promise.reject(new NativeDurabilityStorageClosedError());
		}
		let resolveResult!: (value: T | PromiseLike<T>) => void;
		let rejectResult!: (reason?: unknown) => void;
		const result = new Promise<T>((resolve, reject) => {
			resolveResult = resolve;
			rejectResult = reject;
		});
		this.operationTail = this.operationTail.then(async () => {
			try {
				resolveResult(await this.lease.runWhileHeld(operation));
			} catch (error) {
				rejectResult(error);
			}
		});
		return result;
	}

	async stageAndSync(
		unsafeRequest: NativeDurabilityStageRequest,
	): Promise<NativeDurabilityStageReceipt> {
		const request = snapshotStageRequest(unsafeRequest);
		return this.enqueue(async () => {
			const references: NativeDurabilityStagedBlockReference[] = [];
			const blocks = new Map<number, Uint8Array>();
			const seenOrdinals = new Set<number>();
			for (const block of request.blocks) {
				if (!Number.isSafeInteger(block.ordinal) || block.ordinal < 0) {
					throw new RangeError(
						"Staging ordinal must be a non-negative safe integer",
					);
				}
				if (seenOrdinals.has(block.ordinal)) {
					throw new Error(`Duplicate staging ordinal ${block.ordinal}`);
				}
				seenOrdinals.add(block.ordinal);
				const actual = await sha256NativeDurability(block.bytes);
				if (!nativeDurabilityBytesEqual(actual, block.digest)) {
					throw new NativeDurabilityDigestMismatchError(
						`staged block ${block.ordinal}`,
					);
				}
				blocks.set(block.ordinal, copyNativeDurabilityBytes(block.bytes));
				references.push({
					ordinal: block.ordinal,
					cid: block.cid,
					byteLength: block.bytes.byteLength,
					digest: copyNativeDurabilityBytes(block.digest),
				});
			}
			references.sort((left, right) => left.ordinal - right.ordinal);
			const manifestPayload = {
				version: this.version,
				scope: request.scope,
				fence: this.lease.fence,
				blocks: references,
			};
			const manifestDigest = await nativeDurabilityScopeDigest(manifestPayload);
			const manifest: NativeDurabilityStagingManifest = {
				...manifestPayload,
				scope: cloneScope(request.scope),
				fence: cloneFence(this.lease.fence),
				blocks: references,
				manifestDigest,
			};
			const existing = this.staging.get(request.scope.transactionId);
			if (
				existing &&
				!nativeDurabilityBytesEqual(
					existing.manifest.manifestDigest,
					manifest.manifestDigest,
				)
			) {
				throw new Error(
					`Staging transaction ${request.scope.transactionId} already has a different manifest`,
				);
			}
			this.staging.set(request.scope.transactionId, { manifest, blocks });
			const receipt: NativeDurabilityStageReceipt = {
				version: this.version,
				kind: "stage",
				domainId: this.domainId,
				fence: cloneFence(this.lease.fence),
				transactionId: request.scope.transactionId,
				txSequence: request.scope.txSequence,
				firstRecordLsn: request.scope.recordLsn,
				lastRecordLsn: request.scope.recordLsn,
				scopeDigest: await nativeDurabilityScopeDigest(request),
				barrierOrdinal: this.barrier(),
				blocks: references.map((block) => ({
					...block,
					digest: copyNativeDurabilityBytes(block.digest),
				})),
				manifestDigest: copyNativeDurabilityBytes(manifestDigest),
			};
			return receipt;
		});
	}

	async readStagingManifest(
		transactionId: string,
	): Promise<NativeDurabilityStagingManifest | undefined> {
		return this.enqueue(async () => {
			const manifest = this.staging.get(transactionId)?.manifest;
			if (!manifest) return undefined;
			return {
				...manifest,
				scope: cloneScope(manifest.scope),
				fence: cloneFence(manifest.fence),
				blocks: manifest.blocks.map((block) => ({
					...block,
					digest: copyNativeDurabilityBytes(block.digest),
				})),
				manifestDigest: copyNativeDurabilityBytes(manifest.manifestDigest),
			};
		});
	}

	async readStagedBlock(
		transactionId: string,
		ordinal: number,
	): Promise<Uint8Array | undefined> {
		return this.enqueue(async () => {
			const bytes = this.staging.get(transactionId)?.blocks.get(ordinal);
			return bytes && copyNativeDurabilityBytes(bytes);
		});
	}

	async listStagingTransactionIds(): Promise<string[]> {
		return this.enqueue(async () => [...this.staging.keys()].sort());
	}

	async appendJournalAndSync(
		unsafeRequest: NativeDurabilityJournalAppendRequest,
	): Promise<NativeDurabilityJournalReceipt> {
		const request = snapshotJournalRequest(unsafeRequest);
		return this.enqueue(async () => {
			if (request.expectedOffset !== this.journal.byteLength) {
				throw new NativeDurabilityJournalOffsetConflictError(
					request.expectedOffset,
					this.journal.byteLength,
				);
			}
			const actual = await sha256NativeDurability(request.frames);
			if (!nativeDurabilityBytesEqual(actual, request.framesDigest)) {
				throw new NativeDurabilityDigestMismatchError("journal frames");
			}
			const next = new Uint8Array(
				this.journal.byteLength + request.frames.byteLength,
			);
			next.set(this.journal);
			next.set(request.frames, this.journal.byteLength);
			this.journal = next;
			return {
				version: this.version,
				kind: "journal-append",
				domainId: this.domainId,
				fence: cloneFence(this.lease.fence),
				transactionId: request.transactionId,
				txSequence: request.txSequence,
				firstRecordLsn: request.firstRecordLsn,
				lastRecordLsn: request.lastRecordLsn,
				scopeDigest: await nativeDurabilityScopeDigest(request),
				barrierOrdinal: this.barrier(),
				offset: request.expectedOffset,
				endOffset: this.journal.byteLength,
				framesDigest: copyNativeDurabilityBytes(request.framesDigest),
			};
		});
	}

	async readJournal(): Promise<Uint8Array> {
		return this.enqueue(async () => copyNativeDurabilityBytes(this.journal));
	}

	async reconcileIncompleteJournalTailAndSync(
		unsafeRequest: NativeDurabilityIncompleteTailReconciliationRequest,
	): Promise<NativeDurabilityJournalReconciliationReceipt> {
		const request = { ...unsafeRequest };
		try {
			assertNativeDurabilityOperationScope({
				...request,
				recordLsn: 0n,
			});
		} catch (error) {
			return Promise.reject(error);
		}
		if (request.txSequence === 0n) {
			return Promise.reject(
				new TypeError("Invalid incomplete-tail reconciliation request"),
			);
		}
		return this.enqueue(async () => {
			const observed = copyNativeDurabilityBytes(this.journal);
			const observedDigest = await sha256NativeDurability(observed);
			const classified = await this.journalClassifier.classify(
				copyNativeDurabilityBytes(observed),
			);
			const classification = { ...classified };
			if (
				classification.kind !== "incomplete-tail" ||
				!Number.isSafeInteger(classification.validLength) ||
				classification.validLength < 0 ||
				classification.validLength >= observed.byteLength ||
				typeof classification.lastRecordLsn !== "bigint" ||
				classification.lastRecordLsn < 0n ||
				classification.lastRecordLsn > NATIVE_DURABILITY_MAX_U64 ||
				(classification.reason !== "short-header" &&
					classification.reason !== "short-body" &&
					classification.reason !== "short-trailer")
			) {
				throw new NativeDurabilityIncompleteTailMismatchError(
					"The exact current journal does not end in a structurally incomplete frame",
				);
			}
			this.journal = observed.slice(0, classification.validLength);
			return {
				version: this.version,
				kind: "journal-tail-reconciliation",
				domainId: this.domainId,
				fence: cloneFence(this.lease.fence),
				transactionId: request.transactionId,
				txSequence: request.txSequence,
				firstRecordLsn: classification.lastRecordLsn,
				lastRecordLsn: classification.lastRecordLsn,
				scopeDigest: await nativeDurabilityScopeDigest({
					...request,
					classification,
					observedDigest,
				}),
				barrierOrdinal: this.barrier(),
				previousLength: observed.byteLength,
				validLength: classification.validLength,
				observedDigest,
			};
		});
	}

	async writeCheckpointAndSync(
		unsafeRequest: NativeDurabilityCheckpointRequest,
	): Promise<NativeDurabilityCheckpointReceipt> {
		const request = snapshotCheckpointRequest(unsafeRequest);
		return this.enqueue(async () => {
			const actual = await sha256NativeDurability(request.bytes);
			if (!nativeDurabilityBytesEqual(actual, request.digest)) {
				throw new NativeDurabilityDigestMismatchError("checkpoint");
			}
			for (const coverage of request.stagingCoverage) {
				const staged = this.staging.get(coverage.transactionId)?.manifest;
				if (
					!staged ||
					staged.scope.txSequence !== coverage.txSequence ||
					coverage.coveredThroughLsn < staged.scope.recordLsn ||
					!nativeDurabilityBytesEqual(
						staged.manifestDigest,
						coverage.stagingManifestDigest,
					)
				) {
					throw new Error(
						`Checkpoint coverage does not match staging transaction ${coverage.transactionId}`,
					);
				}
			}
			const generation = ++this.generationHighwater;
			const manifestSlot = generation % 2n === 1n ? "a" : "b";
			this.checkpoints.set(generation, {
				version: this.version,
				generation,
				checkpointLsn: request.checkpointLsn,
				txSequenceHighwater: request.txSequenceHighwater,
				bytes: copyNativeDurabilityBytes(request.bytes),
				digest: copyNativeDurabilityBytes(request.digest),
				originFence: cloneFence(this.lease.fence),
				manifestSlot,
				stagingCoverage: request.stagingCoverage.map((coverage) => ({
					...coverage,
					stagingManifestDigest: copyNativeDurabilityBytes(
						coverage.stagingManifestDigest,
					),
				})),
				retainedTransactions: cloneRetainedTransactions(
					request.retainedTransactions,
				),
			});
			return {
				version: this.version,
				kind: "checkpoint",
				domainId: this.domainId,
				fence: cloneFence(this.lease.fence),
				transactionId: request.scope.transactionId,
				txSequence: request.scope.txSequence,
				firstRecordLsn: request.scope.recordLsn,
				lastRecordLsn: request.scope.recordLsn,
				scopeDigest: await nativeDurabilityScopeDigest(request),
				barrierOrdinal: this.barrier(),
				generation,
				checkpointLsn: request.checkpointLsn,
				checkpointDigest: copyNativeDurabilityBytes(request.digest),
				manifestSlot,
				stagingCoverageDigest: await nativeDurabilityScopeDigest(
					request.stagingCoverage,
				),
			};
		});
	}

	async readLatestCheckpoint(): Promise<
		NativeDurabilityCheckpoint | undefined
	> {
		return this.enqueue(async () => {
			const latest = [...this.checkpoints.keys()].sort((left, right) =>
				left < right ? 1 : left > right ? -1 : 0,
			)[0];
			const checkpoint =
				latest == null ? undefined : this.checkpoints.get(latest);
			return (
				checkpoint && {
					...checkpoint,
					bytes: copyNativeDurabilityBytes(checkpoint.bytes),
					digest: copyNativeDurabilityBytes(checkpoint.digest),
					originFence: cloneFence(checkpoint.originFence),
					stagingCoverage: checkpoint.stagingCoverage.map((coverage) => ({
						...coverage,
						stagingManifestDigest: copyNativeDurabilityBytes(
							coverage.stagingManifestDigest,
						),
					})),
					retainedTransactions: cloneRetainedTransactions(
						checkpoint.retainedTransactions,
					),
				}
			);
		});
	}

	async deleteAndSync(
		unsafeRequest: NativeDurabilityDeleteRequest,
	): Promise<NativeDurabilityDeleteReceipt> {
		const request = snapshotDeleteRequest(unsafeRequest);
		return this.enqueue(async () => {
			const checkpointGenerations = [...this.checkpoints.keys()].sort(
				(left, right) => (left < right ? 1 : left > right ? -1 : 0),
			);
			const active = checkpointGenerations[0];
			const previous = checkpointGenerations[1];
			const activeCheckpoint =
				active == null ? undefined : this.checkpoints.get(active);
			// Validate every target before applying any of them.
			for (const target of request.targets) {
				if (target.kind === "staging") {
					const staged = this.staging.get(target.transactionId)?.manifest;
					if (staged) {
						const covered = activeCheckpoint?.stagingCoverage.some(
							(coverage) =>
								coverage.transactionId === staged.scope.transactionId &&
								coverage.txSequence === staged.scope.txSequence &&
								coverage.coveredThroughLsn >= staged.scope.recordLsn &&
								nativeDurabilityBytesEqual(
									coverage.stagingManifestDigest,
									staged.manifestDigest,
								),
						);
						if (!covered) {
							throw new Error(
								`Active checkpoint does not cover staging transaction ${target.transactionId}`,
							);
						}
					}
				} else {
					if (target.generation === active || target.generation === previous) {
						throw new Error(
							`Cannot delete active or previous checkpoint generation ${target.generation}`,
						);
					}
				}
			}
			for (const target of request.targets) {
				if (target.kind === "staging") {
					this.staging.delete(target.transactionId);
				} else {
					this.checkpoints.delete(target.generation);
				}
			}
			this.strictDeleteCount++;
			return {
				version: this.version,
				kind: "delete",
				domainId: this.domainId,
				fence: cloneFence(this.lease.fence),
				transactionId: request.scope.transactionId,
				txSequence: request.scope.txSequence,
				firstRecordLsn: request.scope.recordLsn,
				lastRecordLsn: request.scope.recordLsn,
				scopeDigest: await nativeDurabilityScopeDigest(request),
				barrierOrdinal: this.barrier(),
				targets: request.targets.map((target) => ({ ...target })),
			};
		});
	}

	async stats(): Promise<NativeDurabilityStorageStats> {
		return this.enqueue(async () => {
			let stagedBlocks = 0;
			let stagedBytes = 0;
			for (const transaction of this.staging.values()) {
				stagedBlocks += transaction.blocks.size;
				for (const block of transaction.blocks.values()) {
					stagedBytes += block.byteLength;
				}
			}
			let checkpointBytes = 0;
			for (const checkpoint of this.checkpoints.values()) {
				checkpointBytes += checkpoint.bytes.byteLength;
			}
			return {
				kind: this.kind,
				domainId: this.domainId,
				strictBarrierCount: this.barrierOrdinal,
				strictDeleteCount: this.strictDeleteCount,
				journalBytes: this.journal.byteLength,
				stagingTransactions: this.staging.size,
				stagedBlocks,
				stagedBytes,
				checkpointGenerations: this.checkpoints.size,
				checkpointBytes,
			};
		});
	}

	async close(): Promise<void> {
		if (this.closed) return;
		this.closing = true;
		await this.operationTail;
		this.closed = true;
	}
}

export const createMemoryNativeDurabilityStorage = (
	lease: NativeDurabilityLease,
	journalClassifier: NativeDurabilityJournalClassifier,
): MemoryNativeDurabilityStorage =>
	new MemoryNativeDurabilityStorage(lease, journalClassifier);
