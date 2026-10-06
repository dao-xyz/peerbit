import type { RemoteBlocks } from "@peerbit/blocks";
import type { Cache } from "@peerbit/cache";
import {
	type DeleteOptions,
	type Index,
	Or,
	StringMatch,
	toId,
} from "@peerbit/indexer-interface";
import {
	Entry,
	type EntryIndexHashMutationLockOwner,
	EntryType,
	type Log,
	type PreparedAppendJoinFacts,
	type ShallowOrFullEntry,
} from "@peerbit/log";
import type {
	NativeBackboneAppendProfile,
	NativeBackboneCoordinateCommitColumns,
	NativeBackboneLogCommitEntry,
	NativePeerbitBackbone,
} from "@peerbit/native-backbone";
import type {
	NativeAppendCoordinatePlan,
	SharedLogNativeState,
	SharedLogRangePlanner,
} from "@peerbit/shared-log-rust";
import { getPreparedRawExchangeTimestamp } from "./exchange-heads.js";
import type {
	CoordinatePersistBatchItem,
	DecodedReplicaCountMap,
	EntryLeaderPlan,
	EntryWithMetaBytes,
	IndexableDomain,
	NativeBackboneCoordinatePersistenceAdapter,
	NativeBackboneCoordinateRollback,
	NativeBackboneReceiveCoordinateBatch,
	NativeBackboneReceiveCoordinateRow,
	PreparedCoordinatePersistence,
	PutAndDeleteIndex,
	RepairDispatchEntry,
	ResidentCoordinateEntry,
	ReusableReceiveCoordinatePlan,
	SharedLogCoordinateNativeFields,
} from "./index.js";
import type { NumberFromType } from "./integers.js";
import {
	type EntryReplicated,
	isEntryReplicated,
	shouldAssigneToRangeBoundary as shouldAssignToRangeBoundary,
} from "./ranges.js";
import type { ReplicationDomain } from "./replication-domain.js";
import { decodeReplicas } from "./replication.js";
import {
	type SyncProfileFn,
	emitSyncProfileDuration,
	emitSyncProfileEvent,
	syncProfileStart,
} from "./sync/profile.js";

// Moved from src/index.ts with the stage-4.5 method move (byte-identical
// bodies): the maybe-promise idiom and the coordinate delete-hash helpers the
// moved methods depend on. index.ts re-imports the shared ones.
export type MaybePromise<T> = T | Promise<T>;

export const isPromiseLike = <T>(value: MaybePromise<T>): value is Promise<T> =>
	!!value && typeof (value as Promise<T>).then === "function";

export const mapMaybePromise = <T, R>(
	value: MaybePromise<T>,
	fn: (value: T) => MaybePromise<R>,
): MaybePromise<R> => (isPromiseLike(value) ? value.then(fn) : fn(value));

const EMPTY_HASHES: string[] = [];
// Match the lower entry index's bounded predicate fallback. Exact-ID adapters
// do not need this limit, but an OR per hash can exceed SQLite expression depth.
const COORDINATE_DELETE_QUERY_BATCH_SIZE = 64;

const coordinateDeleteOptions = (hashes: string[]): DeleteOptions => ({
	query:
		hashes.length === 1
			? { hash: hashes[0] }
			: new Or(
					hashes.map((hash) => new StringMatch({ key: "hash", value: hash })),
				),
});

type PreparedCoordinateWrite<R extends "u32" | "u64"> = {
	prepared: PreparedCoordinatePersistence<R>;
	hash: string;
	nextHashes: string[];
	coordinates: NumberFromType<R>[];
	replicas: number;
	commitNative?: boolean;
	commitNativeBackbone?: boolean;
	deleteHashes?: string[];
};

export const normalizedHashValues = (hashes: Iterable<string>): string[] => {
	if (Array.isArray(hashes)) {
		if (hashes.length === 0) {
			return EMPTY_HASHES;
		}
		if (hashes.length === 1) {
			return hashes[0] ? hashes : EMPTY_HASHES;
		}
	}
	const values: string[] = [];
	const seen = new Set<string>();
	for (const hash of hashes) {
		if (!hash || seen.has(hash)) {
			continue;
		}
		seen.add(hash);
		values.push(hash);
	}
	return values;
};

export const combineCoordinateDeleteHashes = (
	nextHashes: string[],
	deleteHashes?: string[],
): string[] => {
	if (!deleteHashes || deleteHashes.length === 0) {
		return nextHashes;
	}
	if (nextHashes.length === 0) {
		return deleteHashes;
	}
	const combined: string[] = [];
	const seen = new Set<string>();
	for (const hash of nextHashes) {
		if (!seen.has(hash)) {
			seen.add(hash);
			combined.push(hash);
		}
	}
	for (const hash of deleteHashes) {
		if (!seen.has(hash)) {
			seen.add(hash);
			combined.push(hash);
		}
	}
	return combined;
};

/**
 * Host dependencies of the moved methods, every one a LATE-BOUND closure into
 * the live SharedLog instance: `_nativeBackbone`, `_nativeRangePlanner`,
 * `_nativeSharedLogState`, the coordinate index, `remoteBlocks`,
 * `coordinateToHash`, `timeUntilRoleMaturity`, and the durability/drop flags
 * are all re-assigned across open/close/drop cycles, so nothing may be
 * captured by value; the ownership-lifecycle helpers stay host methods so
 * sinon spies and the fold onto InstanceLifecycle keep working. Prepared-join
 * commit closures returned by createNativeBackbonePreparedJoinCommit execute
 * later inside the ExchangeHeads branch; their lifecycle/poison checks route
 * through these deps to HOST state, so the poison surface is unchanged.
 */
export interface CoordinatePersistenceDeps<R extends "u32" | "u64"> {
	/** Owner-routed so tests can stub the coordinator predicate while forcing
	 *  the direct-fallback path. */
	canUseNativeBackboneResidentCoordinateState: () => boolean;
	/** The SharedLog instance itself — only for `decodeReplicas(x).getValue(host)`. */
	host(): any;
	nativeBackbone(): NativePeerbitBackbone | undefined;
	nativeRangePlanner(): SharedLogRangePlanner | undefined;
	nativeSharedLogState(): SharedLogNativeState | undefined;
	/** The host getter (throws ClosedError when stopped, like every legacy read). */
	entryCoordinatesIndex(): Index<EntryReplicated<R>>;
	log(): Log<any>;
	remoteBlocks(): RemoteBlocks | undefined;
	domain(): ReplicationDomain<any, any, R>;
	indexableDomain(): IndexableDomain<R>;
	coordinateToHash(): Cache<string>;
	timeUntilRoleMaturity(): number;
	getEntryGid(entry: ShallowOrFullEntry<any> | EntryReplicated<R>): string;
	getEntryNext(entry: ShallowOrFullEntry<any> | EntryReplicated<R>): string[];
	getEntryHashNumber(
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>,
	): NumberFromType<R>;
	canPlanNativeHashGid(
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>,
	): boolean;
	hasCustomFindLeaders(): boolean;
	captureReplicationOwnershipLifecycle(): AbortController;
	throwIfReplicationOwnershipLifecycleInactive(
		controller: AbortController,
	): void;
	throwIfReplicationOwnershipPoisoned(): void;
	isDropStarted(): boolean;
	getDurableCommitFailure(): unknown;
	isDurableRecoveryReadyForReopen(): boolean;
	setDurableRecoveryReadyForReopen(value: boolean): void;
}

/**
 * Stage 4.5 (PR-1): the coordinate-persistence module.
 *
 * The four coordinate state fields moved here in the state-ownership commit;
 * this commit moves the 39 persistence methods (34 main-cluster + 5 early
 * helpers) with byte-identical bodies — the only edits are the mechanical
 * glue `this._coordinates.<field>` -> `this.<field>` for the owned state and
 * `this.<hostDep>` -> `this.deps.<hostDep>()` for host state (plus the single
 * `_nativeDurableRecoveryReadyForReopen = true` write, which becomes
 * `deps.setDurableRecoveryReadyForReopen(true)`). SharedLog callers now use
 * the coordinator directly; only the three compatibility state accessors
 * remain on the host.
 *
 * Reset discipline is unchanged from the legacy host fields: each open/close
 * site resets exactly the subset of fields it always reset (the resident
 * mirror survives openNativeBackbone, the persistence adapter does not, and
 * the mutation-generation ratchet deliberately survives everything but a
 * fresh instance).
 */
export class CoordinatePersistenceCoordinator<R extends "u32" | "u64"> {
	/**
	 * Resident mirror of the coordinate rows (hash -> entry or native
	 * fields). Established by the native hydration paths; `undefined` means
	 * the resident fast path is unavailable.
	 */
	_residentEntryCoordinatesByHash?: Map<string, ResidentCoordinateEntry<R>>;

	/** The (optional) durable native-backbone coordinate journal adapter. */
	_nativeBackboneCoordinatePersistence?: NativeBackboneCoordinatePersistenceAdapter;

	// Moved from SharedLog src/index.ts (same name — the sanctioned
	// file-to-file ratchet move; see scripts/ci/check-fence-ratchet.mjs
	// TARGETS). Per-hash mutation-generation ratchet baseline: rollback
	// snapshots capture the generation current at snapshot time and later
	// roll back only while that generation is still current. Independent
	// mutations are serialized by the snapshot's owner; generations distinguish
	// nested before-images within that scope. The map deliberately
	// survives open/close cycles of the same instance.
	//
	// Each row is hold-counted: `snapshotResidentCoordinateEntries` takes one
	// hold per hash per token, and `settleResidentCoordinateSnapshot` releases
	// it once the token can no longer be rolled back, deleting the row at zero
	// holds. That bounds the map by the outstanding rollback tokens instead of
	// by lifetime throughput. Deleting a row that still has a live token would
	// make its rollback fail open (or, worse, ABA-clobber newer state), so
	// eviction is refcount-driven only — never TTL, LRU or a size cap. A
	// MISSED settle merely retains a row, which is the pre-refcount behavior
	// and therefore never a regression.
	_nativeCoordinateMutationGenerations?: Map<
		string,
		{ generation: number; holds: number }
	>;

	/**
	 * Wall-clock watermark of the last settled native coordinate journal
	 * flush; drives the `flushIntervalMs` on-append flush threshold.
	 */
	_nativeBackboneCoordinateJournalLastFlushMs = 0;

	constructor(private readonly deps: CoordinatePersistenceDeps<R>) {}

	withCoordinateMutationOwner<T>(
		hashes: Iterable<string>,
		operation: (
			owner: EntryIndexHashMutationLockOwner,
			assertOwned: () => void,
		) => MaybePromise<T>,
		owner?: EntryIndexHashMutationLockOwner,
	): MaybePromise<T> {
		const index = this.deps.log().entryIndex;
		const values = normalizedHashValues(hashes);
		const run = (owned: EntryIndexHashMutationLockOwner) => {
			const assertOwned = () => {
				if (this.deps.log().entryIndex !== index) {
					throw new Error(
						"Coordinate mutation belongs to an old log generation",
					);
				}
				index.assertHashMutationLocks(owned, values);
			};
			assertOwned();
			return operation(owned, assertOwned);
		};
		// Borrowing preserves the synchronous native transaction path. Only the
		// scope that acquired an owner may release it.
		if (owner) return run(owner);
		return index.acquireHashMutationLocks(values).then(async (owned) => {
			try {
				return await run(owned);
			} finally {
				index.releaseHashMutationLocks(owned);
			}
		});
	}

	/** Delete only absent lower rows. The same owner fences receive completion,
	 * so a delayed trim notification cannot erase a re-admitted coordinate. */
	prepareLowerLogCoordinateRemoval() {
		const index = this.deps.log().entryIndex;
		const resources = this.coordinateWriteResources();
		return async (
			hashes: readonly string[],
			owner?: EntryIndexHashMutationLockOwner,
		) => {
			this.deps.throwIfReplicationOwnershipPoisoned();
			if (this.deps.log().entryIndex !== index) {
				throw new Error(
					"Lower coordinate removal belongs to an old log generation",
				);
			}
			const owned = owner ?? (await index.acquireHashMutationLocks(hashes));
			try {
				index.assertHashMutationLocks(owned, hashes);
				const absent: string[] = [];
				for (const hash of hashes) {
					if (!(await index.getShallow(hash))) absent.push(hash);
				}
				if (absent.length === 0) return;
				resources.nativeState?.deleteEntryCoordinatesBatch(absent);
				resources.backbone?.deleteEntryCoordinatesBatch(absent);
				for (const hash of absent) resources.resident?.delete(hash);
				await this.deleteCoordinateIndexHashes(resources.index, absent, () => {
					index.assertHashMutationLocks(owned, absent);
					this.deps.throwIfReplicationOwnershipPoisoned();
				});
				this.deps.throwIfReplicationOwnershipPoisoned();
			} finally {
				if (!owner) index.releaseHashMutationLocks(owned);
			}
		};
	}

	private coordinateWriteResources() {
		return {
			index: this.deps.entryCoordinatesIndex() as PutAndDeleteIndex<
				EntryReplicated<R>
			>,
			nativeState: this.deps.nativeSharedLogState(),
			backbone: this.deps.nativeBackbone(),
			persistence: this._nativeBackboneCoordinatePersistence,
			resident: this._residentEntryCoordinatesByHash,
			coordinateToHash: this.deps.coordinateToHash(),
		};
	}

	private deleteCoordinateIndexHashes(
		index: PutAndDeleteIndex<EntryReplicated<R>>,
		hashes: string[],
		assertOwned: () => void,
	): MaybePromise<void> {
		assertOwned();
		if (hashes.length === 0) return;
		if (index.delIdsNoReturn) {
			return mapMaybePromise(index.delIdsNoReturn(hashes), assertOwned);
		}
		if (index.delIds) {
			return mapMaybePromise(index.delIds(hashes), assertOwned);
		}
		let offset = 0;
		const deleteNext = (): MaybePromise<void> => {
			while (offset < hashes.length) {
				assertOwned();
				const batch = hashes.slice(
					offset,
					offset + COORDINATE_DELETE_QUERY_BATCH_SIZE,
				);
				offset += batch.length;
				const result = index.del(coordinateDeleteOptions(batch));
				if (isPromiseLike(result)) {
					return result.then(() => {
						assertOwned();
						return deleteNext();
					});
				}
				assertOwned();
			}
		};
		return deleteNext();
	}

	/** Private, receive-scoped completion capability. Preparation grants no
	 * admission: only the lower committed callback supplies hashes to complete.
	 * Close drains that physical receive before these captured stores close. */
	prepareReceiveCoordinateCompletion(items: CoordinatePersistBatchItem<R>[]) {
		this.deps.captureReplicationOwnershipLifecycle();
		const index = this.deps.log().entryIndex;
		const resources = this.coordinateWriteResources();
		const backboneOnly = this.canUseBackboneOnlyCoordinatePersistence();
		const plans = new Map<string, PreparedCoordinateWrite<R>>();
		for (const item of items) {
			const prepared =
				item.prepared ?? this.createCoordinatePersistenceEntry(item);
			if (!prepared) continue;
			plans.set(item.entry.hash, {
				hash: item.entry.hash,
				nextHashes: [...this.deps.getEntryNext(item.entry)],
				coordinates: [...item.coordinates],
				replicas: item.replicas,
				commitNative: item.commitNative,
				commitNativeBackbone: item.commitNativeBackbone,
				prepared: {
					assignedToRangeBoundary: prepared.assignedToRangeBoundary,
					fields: {
						...prepared.fields,
						coordinates: [...prepared.fields.coordinates],
						coordinateStrings: prepared.fields.coordinateStrings?.slice(),
						metaBytes: prepared.fields.metaBytes.slice(),
					},
				},
			});
		}
		for (const plan of plans.values()) {
			// Capture the row constructor's result while the generation is live too.
			this.materializePreparedCoordinateEntry(plan.prepared);
		}
		let released = false;
		const handledHashes = new Set<string>();
		const assertOwned = () => {
			this.deps.throwIfReplicationOwnershipPoisoned();
			if (released || this.deps.log().entryIndex !== index) {
				throw new Error(
					"Receive coordinate completion no longer owns its stores",
				);
			}
		};
		return {
			handledHashes,
			complete: async (
				hashes: readonly string[],
				owner?: EntryIndexHashMutationLockOwner,
			) => {
				assertOwned();
				const pending = [...new Set(hashes)].filter(
					(hash) => plans.has(hash) && !handledHashes.has(hash),
				);
				if (pending.length === 0) return;
				const lockedHashes = pending.flatMap((hash) => [
					hash,
					...plans.get(hash)!.nextHashes,
				]);
				const owned =
					owner ?? (await index.acquireHashMutationLocks(lockedHashes));
				try {
					index.assertHashMutationLocks(owned, lockedHashes);
					assertOwned();
					const currentHeads: PreparedCoordinateWrite<R>[] = [];
					for (const hash of pending) {
						// getShallow reads pending/physical rows without acquiring another owner.
						const current = await index.getShallow(hash);
						if (current?.value.head) {
							currentHeads.push(plans.get(hash)!);
						}
					}
					await this.persistPreparedCoordinatesOwned(
						currentHeads,
						() => {
							assertOwned();
							index.assertHashMutationLocks(owned, lockedHashes);
						},
						() => resources,
						backboneOnly,
					);
					// Obsolete heads are handled too: the outer pipeline must not upsert them.
					for (const hash of pending) handledHashes.add(hash);
				} finally {
					if (!owner) index.releaseHashMutationLocks(owned);
				}
			},
			release: () => {
				released = true;
				plans.clear();
			},
		};
	}

	/** Revalidate delayed cleanup under the same owner as coordinate writes. */
	async deleteNonHeadCoordinatesForHashes(
		hashes: Iterable<string>,
		ownershipLifecycleController?: AbortController,
		owner?: EntryIndexHashMutationLockOwner,
	): Promise<void> {
		const values = normalizedHashValues(hashes);
		if (values.length === 0) return;
		await this.withCoordinateMutationOwner(
			values,
			async (owned, assertOwned) => {
				if (ownershipLifecycleController) {
					this.deps.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
				}
				const index = this.deps.log().entryIndex;
				const nonHeads: string[] = [];
				for (const hash of values) {
					if (!(await index.getShallow(hash))?.value.head) nonHeads.push(hash);
				}
				assertOwned();
				await this.deleteCoordinatesForHashes(
					nonHeads,
					ownershipLifecycleController,
					owned,
				);
			},
			owner,
		);
	}

	deleteCoordinatesForHashes(
		hashes: Iterable<string>,
		ownershipLifecycleController?: AbortController,
		owner?: EntryIndexHashMutationLockOwner,
	): MaybePromise<void> {
		if (ownershipLifecycleController) {
			this.deps.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
		const values = normalizedHashValues(hashes);
		if (values.length === 0) {
			return;
		}
		if (!owner) {
			return this.withCoordinateMutationOwner(values, (owned) =>
				this.deleteCoordinatesForHashes(
					values,
					ownershipLifecycleController,
					owned,
				),
			);
		}
		this.forgetCoordinateStateForHashValues(values, owner);
		const coordinateIndex =
			this.deps.entryCoordinatesIndex() as PutAndDeleteIndex<
				EntryReplicated<R>
			>;
		return this.deleteCoordinateIndexHashes(coordinateIndex, values, () => {
			this.deps.log().entryIndex.assertHashMutationLocks(owner, values);
			if (ownershipLifecycleController) {
				this.deps.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
			}
		});
	}

	forgetCoordinateStateForHashes(
		hashes: Iterable<string>,
		owner: EntryIndexHashMutationLockOwner,
	) {
		const values = normalizedHashValues(hashes);
		if (values.length === 0) {
			return;
		}
		this.forgetCoordinateStateForHashValues(values, owner);
	}

	forgetCoordinateStateForHashValues(
		values: string[],
		owner: EntryIndexHashMutationLockOwner,
	) {
		this.deps.log().entryIndex.assertHashMutationLocks(owner, values);
		this.deps.nativeSharedLogState()?.deleteEntryCoordinatesBatch(values);
		this.deps.nativeBackbone()?.deleteEntryCoordinatesBatch(values);
		this.forgetResidentCoordinateStateForHashValues(values, owner);
	}

	forgetResidentCoordinateStateForHashes(
		hashes: Iterable<string>,
		owner: EntryIndexHashMutationLockOwner,
	) {
		const values = normalizedHashValues(hashes);
		if (values.length === 0) {
			return;
		}
		this.forgetResidentCoordinateStateForHashValues(values, owner);
	}

	forgetResidentCoordinateStateForHashValues(
		values: string[],
		owner: EntryIndexHashMutationLockOwner,
	) {
		this.deps.log().entryIndex.assertHashMutationLocks(owner, values);
		if (this._residentEntryCoordinatesByHash) {
			for (const hash of values) {
				this._residentEntryCoordinatesByHash.delete(hash);
			}
		}
	}
	async createCoordinates(
		entry: ShallowOrFullEntry<any> | EntryReplicated<R> | NumberFromType<R>,
		minReplicas: number,
	) {
		if (
			typeof entry !== "number" &&
			typeof entry !== "bigint" &&
			this.deps.canPlanNativeHashGid(entry)
		) {
			const nativeCoordinates = (
				this.deps.nativeBackbone() ?? this.deps.nativeRangePlanner()
			)?.getGidCoordinates(entry.meta.gid, minReplicas) as
				| NumberFromType<R>[]
				| undefined;
			if (nativeCoordinates) {
				return nativeCoordinates;
			}
		}

		const cursor =
			typeof entry === "number" || typeof entry === "bigint"
				? entry
				: await this.deps.domain().fromEntry(entry);
		const nativeGrid = (
			this.deps.nativeBackbone() ?? this.deps.nativeRangePlanner()
		)?.getGrid(cursor, minReplicas) as NumberFromType<R>[] | undefined;
		return (
			nativeGrid ??
			this.deps.indexableDomain().numbers.getGrid(cursor, minReplicas)
		);
	}

	async getCoordinates(entry: { hash: string }) {
		const nativeCoordinates = (
			this.deps.nativeBackbone() ?? this.deps.nativeSharedLogState()
		)?.getEntryCoordinates(entry.hash);
		if (nativeCoordinates) {
			return nativeCoordinates as NumberFromType<R>[];
		}
		const result = await this.deps
			.entryCoordinatesIndex()
			.iterate({ query: { hash: entry.hash } })
			.all();
		return result[0].value.coordinates;
	}

	getNativeLogEntryMetadataBatch(hashes: Iterable<string>) {
		const normalized = [...hashes];
		if (normalized.length === 0) {
			return [];
		}
		const backboneMetadata =
			this.deps.nativeBackbone()?.graph.entryMetadataHintsBatch(normalized) ??
			this.deps.nativeBackbone()?.graph.entryMetadataBatch(normalized);
		if (backboneMetadata?.every((entry) => entry != null)) {
			return backboneMetadata;
		}
		const indexMetadata =
			this.deps.log().entryIndex.getNativeEntryMetadataHintsBatch(normalized) ??
			this.deps.log().entryIndex.getNativeEntryMetadataBatch(normalized);
		if (!backboneMetadata) {
			return indexMetadata;
		}
		if (!indexMetadata) {
			return backboneMetadata;
		}
		return backboneMetadata.map(
			(entry, index) => entry ?? indexMetadata[index],
		);
	}

	createReusableReceiveCoordinatePlans(
		receiveGroups: Array<{
			latestEntry: ShallowOrFullEntry<any>;
			maxMaxReplicas: number;
			leaderPlan?: EntryLeaderPlan<R>;
		}>,
		options?: {
			decodedReplicaCounts?: DecodedReplicaCountMap;
			allowRoleAgeZeroPlans?: boolean;
		},
	): Map<string, ReusableReceiveCoordinatePlan<R>> {
		const reusablePlans = new Map<string, ReusableReceiveCoordinatePlan<R>>();
		if (
			this.deps.timeUntilRoleMaturity() > 0 &&
			!options?.allowRoleAgeZeroPlans
		) {
			return reusablePlans;
		}

		for (const group of receiveGroups) {
			const plan = group.leaderPlan;
			if (!plan) {
				continue;
			}
			const replicas =
				options?.decodedReplicaCounts?.get(group.latestEntry.hash) ??
				decodeReplicas(group.latestEntry).getValue(this.deps.host());
			if (replicas !== group.maxMaxReplicas) {
				continue;
			}
			const prepared = this.createCoordinatePersistenceEntryFromLeaderPlan({
				entry: group.latestEntry,
				plan,
				replicas,
			});
			if (!prepared) {
				continue;
			}
			reusablePlans.set(group.latestEntry.hash, {
				plan,
				replicas,
				prepared,
			});
		}
		return reusablePlans;
	}

	createBackboneOnlyReceiveCoordinateBatch(
		items: CoordinatePersistBatchItem<R>[],
	): NativeBackboneReceiveCoordinateBatch<R> | undefined {
		if (
			!this.deps.nativeBackbone() ||
			items.length === 0 ||
			!this.canUseBackboneOnlyCoordinatePersistence()
		) {
			return undefined;
		}

		const rows = items
			.filter((item) => item.prepared)
			.map((item) => {
				const prepared = item.prepared!;
				const deleteHashes = this.deps.getEntryNext(item.entry);
				return {
					item,
					prepared,
					fields: prepared.fields,
					deleteHashes,
				};
			});
		if (rows.length === 0) {
			return undefined;
		}

		// Only a standalone coordinate batch may roll coordinates back. A fused
		// receive must not undo them independently of its committed lower entries.
		return { rows };
	}

	nativeBackboneReceiveCoordinateRowsToColumns(
		rows: NativeBackboneReceiveCoordinateRow<R>[],
	): NativeBackboneCoordinateCommitColumns {
		const hashes = new Array<string>(rows.length);
		const gids = new Array<string>(rows.length);
		const hashNumberValues = new BigUint64Array(rows.length);
		const coordinateCounts = new Uint32Array(rows.length);
		const coordinateValues = new BigUint64Array(
			rows.reduce((sum, row) => sum + row.fields.coordinates.length, 0),
		);
		const nextHashBatches = new Array<string[]>(rows.length);
		const assignedToRangeBoundaries = new Uint8Array(rows.length);
		const requestedReplicaValues = new Uint32Array(rows.length);
		let coordinateOffset = 0;
		for (let i = 0; i < rows.length; i++) {
			const { item, prepared, fields, deleteHashes } = rows[i]!;
			hashes[i] = item.entry.hash;
			gids[i] = fields.gid;
			hashNumberValues[i] =
				typeof fields.hashNumber === "bigint"
					? fields.hashNumber
					: BigInt(fields.hashNumberString ?? fields.hashNumber);
			coordinateCounts[i] = fields.coordinates.length;
			for (const coordinate of fields.coordinates) {
				coordinateValues[coordinateOffset++] =
					typeof coordinate === "bigint" ? coordinate : BigInt(coordinate);
			}
			nextHashBatches[i] = deleteHashes;
			assignedToRangeBoundaries[i] =
				prepared.assignedToRangeBoundary === true ? 1 : 0;
			requestedReplicaValues[i] = item.replicas;
		}
		return {
			hashes,
			gids,
			hashNumberValues,
			coordinateCounts,
			coordinateValues,
			nextHashBatches,
			assignedToRangeBoundaries,
			requestedReplicaValues,
		};
	}

	async finishBackboneOnlyReceiveCoordinateBatch(
		batch: NativeBackboneReceiveCoordinateBatch<R>,
		profile?: SyncProfileFn,
	): Promise<Set<string>> {
		const mirrorStartedAt = syncProfileStart(profile);
		const persistedHashes = new Set<string>();
		const coordinateToHashRows: [NumberFromType<R>, string][] = [];
		let deleteCount = 0;
		for (const { item, prepared, fields, deleteHashes } of batch.rows) {
			persistedHashes.add(item.entry.hash);
			this._residentEntryCoordinatesByHash?.set(
				item.entry.hash,
				prepared.coordinateEntry ?? fields,
			);
			for (const deletedHash of deleteHashes) {
				this._residentEntryCoordinatesByHash?.delete(deletedHash);
				deleteCount++;
			}
			for (const coordinate of item.coordinates) {
				coordinateToHashRows.push([coordinate, item.entry.hash]);
			}
		}
		this.deps.coordinateToHash().addMany(coordinateToHashRows);
		emitSyncProfileDuration(profile, mirrorStartedAt, {
			name: "sharedLog.receive.coordinateResidentMirror",
			component: "shared-log",
			entries: batch.rows.length,
			count: coordinateToHashRows.length,
			messages: 1,
			details: { deletes: deleteCount },
		});

		const flushStartedAt = syncProfileStart(profile);
		const flushed = this.flushNativeBackboneCoordinateJournalOnAppend();
		if (isPromiseLike(flushed)) {
			await flushed;
		}
		emitSyncProfileDuration(profile, flushStartedAt, {
			name: "sharedLog.receive.coordinateJournalFlush",
			component: "shared-log",
			entries: batch.rows.length,
			messages: 1,
		});
		return persistedHashes;
	}

	rollbackBackboneOnlyReceiveCoordinateBatch(
		batch: NativeBackboneReceiveCoordinateBatch<R>,
	): void {
		for (const { item } of batch.rows) {
			this.rollbackNativeBackboneCoordinateAppend(
				item.entry.hash,
				batch.rollbackCoordinateEntries,
			);
		}
	}

	async persistBackboneOnlyReceiveCoordinateBatch(
		items: CoordinatePersistBatchItem<R>[],
		owner?: EntryIndexHashMutationLockOwner,
	): Promise<Set<string> | undefined> {
		if (
			items.length === 0 ||
			!this.deps.nativeBackbone() ||
			!this.canUseBackboneOnlyCoordinatePersistence()
		) {
			return undefined;
		}
		const lifecycle = this.deps.captureReplicationOwnershipLifecycle();
		return this.withCoordinateMutationOwner(
			items.flatMap((item) => [
				item.entry.hash,
				...this.deps.getEntryNext(item.entry),
			]),
			async (owned, assertOwned) => {
				this.deps.throwIfReplicationOwnershipLifecycleInactive(lifecycle);
				const backbone = this.deps.nativeBackbone();
				const batch = this.createBackboneOnlyReceiveCoordinateBatch(items);
				if (!backbone || !batch) {
					return undefined;
				}
				batch.rollbackCoordinateEntries =
					this.snapshotResidentCoordinateEntries(
						batch.rows.flatMap((row) => [
							row.item.entry.hash,
							...row.deleteHashes,
						]),
						owned,
					);
				try {
					backbone.commitEntryCoordinatesColumnsBatch(
						this.nativeBackboneReceiveCoordinateRowsToColumns(batch.rows),
					);
					const result =
						await this.finishBackboneOnlyReceiveCoordinateBatch(batch);
					assertOwned();
					return result;
				} catch (error) {
					this.rollbackBackboneOnlyReceiveCoordinateBatch(batch);
					await this.flushNativeBackboneCoordinateJournal();
					throw error;
				} finally {
					// The token cannot escape this function: both outcomes are
					// observed here, and the catch arm has already consumed it by the
					// time this runs.
					this.settleResidentCoordinateSnapshot(
						batch.rollbackCoordinateEntries,
					);
				}
			},
			owner,
		);
	}

	emitNativeBackboneRawCommitProfile(
		profile: SyncProfileFn | undefined,
		nativeProfile: NativeBackboneAppendProfile | undefined,
		entries: number,
		verifyCount: number,
	): void {
		if (!profile || !nativeProfile) {
			return;
		}
		const events: Array<[name: string, durationMs: number, count?: number]> = [
			[
				"sharedLog.receive.nativeRawCommit.pendingCheck",
				nativeProfile.nativeBackboneRawReceivePendingCheckMs,
			],
			[
				"sharedLog.receive.nativeRawCommit.verify",
				nativeProfile.nativeBackboneRawReceiveVerifyMs,
				verifyCount,
			],
			[
				"sharedLog.receive.nativeRawCommit.verifyStatus",
				nativeProfile.nativeBackboneRawReceiveVerifyStatusMs,
			],
			[
				"sharedLog.receive.nativeRawCommit.joinPlan",
				nativeProfile.nativeBackboneRawReceiveJoinPlanMs,
			],
			[
				"sharedLog.receive.nativeRawCommit.removePending",
				nativeProfile.nativeBackboneRawReceiveRemoveMs,
			],
			[
				"sharedLog.receive.nativeRawCommit.blockPut",
				nativeProfile.nativeBackboneRawReceiveBlockPutMs,
			],
			[
				"sharedLog.receive.nativeRawCommit.graphPut",
				nativeProfile.nativeBackboneRawReceiveGraphPutMs,
			],
			[
				"sharedLog.receive.nativeRawCommit.coordinateCommit",
				nativeProfile.nativeBackboneRawReceiveCoordinateCommitMs,
			],
		];
		for (const [name, durationMs, count] of events) {
			if (durationMs > 0) {
				emitSyncProfileEvent(profile, {
					name,
					component: "shared-log",
					durationMs,
					entries,
					count,
					messages: 1,
				});
			}
		}
	}

	createNativeBackbonePreparedJoinCommit(
		coordinateBatch?: NativeBackboneReceiveCoordinateBatch<R>,
		onCoordinatesCommitted?: (
			batch: NativeBackboneReceiveCoordinateBatch<R>,
		) => void,
		verifyHashes?: string[],
		verifyAllHashes = false,
		profile?: SyncProfileFn,
		onPreparedEntriesCommitted?: (hashes: string[]) => void,
	):
		| ((input: {
				entries: PreparedAppendJoinFacts[];
				hashes: string[];
				headFlags: boolean[];
				headFlagsBytes: Uint8Array;
				trustedMissing: boolean;
				validatePlan?: boolean;
		  }) => boolean)
		| undefined {
		const backbone = this.deps.nativeBackbone();
		if (
			!backbone ||
			this.deps.remoteBlocks()?.localStore !== backbone.blocks ||
			(verifyHashes &&
				verifyHashes.length > 0 &&
				!backbone.graph.commitVerifiedPreparedRawReceiveJoinBatch)
		) {
			return undefined;
		}
		return ({
			entries,
			hashes,
			headFlags,
			headFlagsBytes,
			trustedMissing,
			validatePlan,
		}) => {
			this.deps.throwIfReplicationOwnershipPoisoned();
			if (!trustedMissing || entries.length === 0) {
				return false;
			}
			const coordinateColumns =
				coordinateBatch && coordinateBatch.rows.length > 0
					? this.nativeBackboneReceiveCoordinateRowsToColumns(
							coordinateBatch.rows,
						)
					: undefined;
			if (validatePlan) {
				const verifiedCommitStartedAt = syncProfileStart(profile);
				const profileNativeBackbone =
					!!profile &&
					!!backbone.resetAppendProfile &&
					!!backbone.setAppendProfileEnabled &&
					!!backbone.appendProfile;
				if (profileNativeBackbone) {
					backbone.resetAppendProfile();
					backbone.setAppendProfileEnabled(true);
				}
				let committed: boolean | undefined;
				try {
					if (verifyHashes && verifyHashes.length > 0) {
						if (verifyAllHashes) {
							committed =
								backbone.graph.commitVerifiedAllPreparedRawReceiveJoinBatch?.(
									hashes,
									headFlagsBytes,
									coordinateColumns,
								);
						}
						committed ??=
							backbone.graph.commitVerifiedPreparedRawReceiveJoinBatch?.(
								hashes,
								headFlagsBytes,
								verifyHashes,
								coordinateColumns,
							);
					} else {
						committed = backbone.graph.commitPreparedRawReceiveJoinBatch?.(
							hashes,
							headFlagsBytes,
							coordinateColumns,
						);
					}
				} finally {
					if (profileNativeBackbone) {
						backbone.setAppendProfileEnabled(false);
						this.emitNativeBackboneRawCommitProfile(
							profile,
							backbone.appendProfile(),
							entries.length,
							verifyHashes?.length ?? 0,
						);
					}
				}
				if (verifyHashes && verifyHashes.length > 0 && profile) {
					emitSyncProfileDuration(profile, verifiedCommitStartedAt, {
						name: "sharedLog.receive.nativeVerifiedCommit",
						component: "shared-log",
						entries: entries.length,
						count: verifyHashes.length,
						messages: 1,
					});
				}
				if (committed === true) {
					onPreparedEntriesCommitted?.(hashes);
					if (coordinateBatch) {
						onCoordinatesCommitted?.(coordinateBatch);
					}
					return true;
				}
				if (committed === false) {
					backbone.graph.clearPreparedRawReceiveEntries?.(hashes);
					return false;
				}
			}
			if (
				backbone.graph.commitPreparedRawReceiveBatch(
					hashes,
					headFlagsBytes,
					coordinateColumns,
				)
			) {
				onPreparedEntriesCommitted?.(hashes);
				if (coordinateBatch) {
					onCoordinatesCommitted?.(coordinateBatch);
				}
				return true;
			}
			const commitEntries = new Array<NativeBackboneLogCommitEntry>(
				entries.length,
			);
			for (let i = 0; i < entries.length; i++) {
				const entry = entries[i]!;
				if (
					!entry.bytes ||
					!entry.nativeEntry ||
					entry.meta.type !== EntryType.APPEND
				) {
					return false;
				}
				commitEntries[i] = {
					...entry.nativeEntry,
					head: headFlags[i] ?? true,
					bytes: entry.bytes,
				};
			}
			if (coordinateBatch && coordinateBatch.rows.length > 0) {
				backbone.graph.commitBlocksGraphAndCoordinatesBatch(
					commitEntries,
					coordinateColumns!,
				);
				onCoordinatesCommitted?.(coordinateBatch);
			} else {
				backbone.graph.commitBlocksAndGraphBatch(commitEntries);
			}
			onPreparedEntriesCommitted?.(hashes);
			return true;
		};
	}

	createCoordinatePersistenceEntryFromLeaderPlan(properties: {
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>;
		plan: EntryLeaderPlan<R>;
		replicas: number;
	}): PreparedCoordinatePersistence<R> | false {
		const assignedToRangeBoundary =
			properties.plan.assignedToRangeBoundary ??
			shouldAssignToRangeBoundary(properties.plan.leaders, properties.replicas);
		const hashNumber = this.deps.getEntryHashNumber(properties.entry);
		const metaBytes = (properties.entry as EntryWithMetaBytes).getMetaBytes?.();
		if (metaBytes) {
			const rawTimestamp =
				properties.entry instanceof Entry
					? getPreparedRawExchangeTimestamp(properties.entry)
					: undefined;
			const wallTime =
				rawTimestamp?.wallTime ??
				properties.entry.meta.clock.timestamp.wallTime;
			return {
				assignedToRangeBoundary,
				fields: {
					hash: properties.entry.hash,
					hashNumber,
					hashNumberString: hashNumber.toString(),
					gid: this.deps.getEntryGid(properties.entry),
					coordinates: properties.plan.coordinates,
					coordinateStrings:
						properties.plan.coordinateStrings ??
						properties.plan.coordinates.map((coordinate) =>
							coordinate.toString(),
						),
					wallTime,
					wallTimeString: wallTime.toString(),
					assignedToRangeBoundary,
					metaBytes,
				},
			};
		}
		return this.createCoordinatePersistenceEntry({
			coordinates: properties.plan.coordinates,
			entry: properties.entry,
			leaders: properties.plan.leaders,
			replicas: properties.replicas,
			assignedToRangeBoundary,
			hashNumber,
		});
	}

	createCoordinatePersistenceEntry(properties: {
		coordinates: NumberFromType<R>[];
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>;
		leaders:
			| Map<
					string,
					{
						intersecting: boolean;
					}
			  >
			| false;
		replicas: number;
		prev?: EntryReplicated<R>;
		assignedToRangeBoundary?: boolean;
		hashNumber?: NumberFromType<R>;
	}): PreparedCoordinatePersistence<R> | false {
		const assignedToRangeBoundary =
			properties.assignedToRangeBoundary ??
			shouldAssignToRangeBoundary(properties.leaders, properties.replicas);

		if (
			properties.prev &&
			properties.prev.assignedToRangeBoundary === assignedToRangeBoundary
		) {
			return false;
		}

		const metaBytes = (properties.entry as EntryWithMetaBytes).getMetaBytes?.();
		const coordinateEntry = new (this.deps.indexableDomain().constructorEntry)({
			assignedToRangeBoundary,
			coordinates: properties.coordinates,
			meta: properties.entry.meta,
			metaBytes,
			hash: properties.entry.hash,
			hashNumber:
				properties.hashNumber ?? this.deps.getEntryHashNumber(properties.entry),
		});
		return {
			coordinateEntry,
			assignedToRangeBoundary,
			fields: {
				hash: coordinateEntry.hash,
				hashNumber: coordinateEntry.hashNumber,
				gid: coordinateEntry.gid,
				coordinates: coordinateEntry.coordinates,
				wallTime: coordinateEntry.wallTime,
				assignedToRangeBoundary: coordinateEntry.assignedToRangeBoundary,
				metaBytes: coordinateEntry.getMetaBytes(),
			},
		};
	}

	createCoordinatePersistenceEntryFromNativePlan(properties: {
		entry: ShallowOrFullEntry<any> | EntryReplicated<R>;
		plan: NativeAppendCoordinatePlan;
		prev?: EntryReplicated<R>;
	}): PreparedCoordinatePersistence<R> | false {
		if (
			properties.plan.hash !== properties.entry.hash ||
			properties.plan.gid !== this.deps.getEntryGid(properties.entry)
		) {
			return false;
		}

		const assignedToRangeBoundary = properties.plan.assignedToRangeBoundary;
		if (
			properties.prev &&
			properties.prev.assignedToRangeBoundary === assignedToRangeBoundary
		) {
			return false;
		}

		const coordinates = properties.plan.coordinates as NumberFromType<R>[];
		const hashNumber = properties.plan.hashNumber as NumberFromType<R>;
		const metaBytes = (properties.entry as EntryWithMetaBytes).getMetaBytes?.();
		if (metaBytes) {
			const rawTimestamp =
				properties.entry instanceof Entry
					? getPreparedRawExchangeTimestamp(properties.entry)
					: undefined;
			const wallTime =
				rawTimestamp?.wallTime ??
				properties.entry.meta.clock.timestamp.wallTime;
			return {
				assignedToRangeBoundary,
				fields: {
					hash: properties.plan.hash,
					hashNumber,
					hashNumberString: properties.plan.hashNumberString,
					gid: properties.plan.gid,
					coordinates,
					coordinateStrings: properties.plan.coordinateStrings,
					wallTime,
					wallTimeString: wallTime.toString(),
					assignedToRangeBoundary,
					metaBytes,
				},
			};
		}
		const entryMeta = properties.entry.meta;
		const coordinateEntry = new (this.deps.indexableDomain().constructorEntry)({
			assignedToRangeBoundary,
			coordinates,
			meta: entryMeta,
			hash: properties.plan.hash,
			hashNumber,
		});
		return {
			coordinateEntry,
			assignedToRangeBoundary,
			fields: {
				hash: properties.plan.hash,
				hashNumber,
				hashNumberString: properties.plan.hashNumberString,
				gid: properties.plan.gid,
				coordinates,
				coordinateStrings: properties.plan.coordinateStrings,
				wallTime: coordinateEntry.wallTime,
				wallTimeString: coordinateEntry.wallTime.toString(),
				assignedToRangeBoundary,
				metaBytes: coordinateEntry.getMetaBytes(),
			},
		};
	}

	createCoordinateEntryFromNativeFields(
		fields: SharedLogCoordinateNativeFields<R>,
	): EntryReplicated<R> {
		return new (this.deps.indexableDomain().constructorEntry)({
			assignedToRangeBoundary: fields.assignedToRangeBoundary,
			coordinates: fields.coordinates,
			metaBytes: fields.metaBytes,
			gid: fields.gid,
			wallTime: fields.wallTime,
			hash: fields.hash,
			hashNumber: fields.hashNumber,
		});
	}

	materializePreparedCoordinateEntry(
		prepared: PreparedCoordinatePersistence<R>,
	): EntryReplicated<R> {
		return (prepared.coordinateEntry ??=
			this.createCoordinateEntryFromNativeFields(prepared.fields));
	}

	materializeResidentCoordinateEntry(
		entry: ResidentCoordinateEntry<R>,
	): EntryReplicated<R> {
		return isEntryReplicated(entry)
			? entry
			: this.createCoordinateEntryFromNativeFields(entry);
	}

	/**
	 * Read the coordinate row from the state that is authoritative for receipt
	 * presence. Native backbone-only receives deliberately do not duplicate their
	 * coordinate rows into the generic index, so the hydrated resident mirror is
	 * authoritative while that mode is active. Its absence is also authoritative:
	 * falling back to the generic index could resurrect a stale row after a native
	 * delete. Callers still own journal flushing and crash-safe barriers before
	 * treating this presence check as a durable receipt.
	 */
	async getAuthoritativeCoordinateEntryForReceipt(
		hash: string,
	): Promise<EntryReplicated<R> | undefined> {
		if (this.canUseBackboneOnlyCoordinatePersistence()) {
			const resident = this._residentEntryCoordinatesByHash?.get(hash);
			return resident
				? this.materializeResidentCoordinateEntry(resident)
				: undefined;
		}
		return (await this.deps.entryCoordinatesIndex().get(toId(hash)))?.value;
	}

	materializeRepairDispatchEntries(
		entries: ReadonlyMap<string, RepairDispatchEntry<R>>,
	): Map<string, EntryReplicated<R>> {
		const materialized = new Map<string, EntryReplicated<R>>();
		for (const [hash, entry] of entries) {
			materialized.set(hash, this.materializeResidentCoordinateEntry(entry));
		}
		return materialized;
	}

	snapshotResidentCoordinateEntries(
		hashes: Iterable<string>,
		owner: EntryIndexHashMutationLockOwner,
	): NativeBackboneCoordinateRollback<R> | undefined {
		const uniqueHashes = new Set([...hashes].filter(Boolean));
		if (uniqueHashes.size === 0) {
			return undefined;
		}
		this.deps.log().entryIndex.assertHashMutationLocks(owner, uniqueHashes);
		const entries = new Map<string, ResidentCoordinateEntry<R>>();
		const generations = new Map<string, number>();
		const mutationGenerations = (this._nativeCoordinateMutationGenerations ??=
			new Map());
		for (const hash of uniqueHashes) {
			// `uniqueHashes` is a Set, so this is exactly one hold per token
			// per hash; `settleResidentCoordinateSnapshot` releases it.
			const row = mutationGenerations.get(hash);
			const generation = (row?.generation ?? 0) + 1;
			mutationGenerations.set(hash, {
				generation,
				holds: (row?.holds ?? 0) + 1,
			});
			generations.set(hash, generation);
			const entry = this._residentEntryCoordinatesByHash?.get(hash);
			if (entry) {
				entries.set(hash, entry);
			}
		}
		return { hashes: uniqueHashes, entries, generations, owner };
	}

	/**
	 * Release the holds a rollback token took on the mutation-generation map.
	 *
	 * Call this only where the token is TERMINAL — after the last point at
	 * which any code path could still roll it back. A premature settle deletes
	 * a row that a live token still needs; rollback rejects settled tokens
	 * rather than risking a later generation collision. A missed settle only
	 * retains the row, which is the pre-refcount behavior.
	 *
	 * Idempotent by design: `settled` is set FIRST so a second settle of the
	 * same token cannot consume another token's hold on a shared hash.
	 */
	settleResidentCoordinateSnapshot(
		rollback?: NativeBackboneCoordinateRollback<R>,
	): void {
		if (!rollback || rollback.settled) {
			return;
		}
		rollback.settled = true;
		const mutationGenerations = this._nativeCoordinateMutationGenerations;
		if (!mutationGenerations) {
			return;
		}
		for (const hash of rollback.hashes) {
			const row = mutationGenerations.get(hash);
			if (!row) {
				continue;
			}
			if (row.holds <= 1) {
				mutationGenerations.delete(hash);
			} else {
				row.holds -= 1;
			}
		}
	}

	rollbackNativeBackboneCoordinateAppend(
		appendHash: string,
		rollback?: NativeBackboneCoordinateRollback<R>,
	): void {
		if (!rollback) {
			throw new Error("Coordinate rollback requires an owned snapshot");
		}
		if (rollback.settled) {
			throw new Error("Coordinate rollback snapshot is already settled");
		}
		this.deps
			.log()
			.entryIndex.assertHashMutationLocks(rollback.owner, rollback.hashes);
		const backbone = this.deps.nativeBackbone();
		if (!backbone) {
			return;
		}
		const hashes = rollback?.hashes ?? new Set([appendHash]);
		const mutationGenerations = (this._nativeCoordinateMutationGenerations ??=
			new Map());
		for (const hash of hashes) {
			const expectedGeneration = rollback?.generations.get(hash);
			if (
				expectedGeneration !== undefined &&
				mutationGenerations.get(hash)?.generation !== expectedGeneration
			) {
				continue;
			}
			backbone.deleteEntryCoordinates(hash);
			this.deps.nativeSharedLogState()?.deleteEntryCoordinates(hash);
			this._residentEntryCoordinatesByHash?.delete(hash);
			const entry = rollback?.entries.get(hash);
			if (!entry) {
				continue;
			}
			const fields = isEntryReplicated(entry)
				? {
						hash: entry.hash,
						gid: entry.gid,
						coordinates: entry.coordinates,
						assignedToRangeBoundary: entry.assignedToRangeBoundary,
						hashNumber: entry.hashNumber,
					}
				: entry;
			const requestedReplicas = isEntryReplicated(entry)
				? decodeReplicas(entry).getValue(this.deps.host())
				: fields.coordinates.length;
			backbone.putEntryCoordinates(
				fields.hash,
				fields.gid,
				fields.coordinates,
				fields.assignedToRangeBoundary,
				requestedReplicas,
				fields.hashNumber,
			);
			this.deps
				.nativeSharedLogState()
				?.putEntryCoordinates(
					fields.hash,
					fields.gid,
					fields.coordinates,
					fields.assignedToRangeBoundary,
					requestedReplicas,
					fields.hashNumber,
				);
			this._residentEntryCoordinatesByHash?.set(hash, entry);
		}
	}

	async rollbackNativeBackboneCoordinateAppendDurably(
		appendHash: string,
		rollback?: NativeBackboneCoordinateRollback<R>,
	): Promise<void> {
		this.rollbackNativeBackboneCoordinateAppend(appendHash, rollback);
		const coordinateIndex =
			this.deps.entryCoordinatesIndex() as PutAndDeleteIndex<
				EntryReplicated<R>
			>;
		const hashes = rollback?.hashes ?? new Set([appendHash]);
		const mutationGenerations = (this._nativeCoordinateMutationGenerations ??=
			new Map());
		for (const hash of hashes) {
			const expectedGeneration = rollback?.generations.get(hash);
			if (
				expectedGeneration !== undefined &&
				mutationGenerations.get(hash)?.generation !== expectedGeneration
			) {
				continue;
			}
			const previous = rollback?.entries.get(hash);
			if (previous) {
				await coordinateIndex.put(
					this.materializeResidentCoordinateEntry(previous),
				);
			} else if (coordinateIndex.delIds) {
				await coordinateIndex.delIds([hash]);
			} else if (coordinateIndex.delIdsNoReturn) {
				await coordinateIndex.delIdsNoReturn([hash]);
			} else {
				await coordinateIndex.del({ query: { hash } });
			}
		}
		const flushed = this.flushNativeBackboneCoordinateJournal();
		if (isPromiseLike(flushed)) {
			await flushed;
		}
	}

	persistPreparedCoordinate(
		properties: PreparedCoordinateWrite<R>,
		ownershipLifecycleController = this.deps.captureReplicationOwnershipLifecycle(),
		owner?: EntryIndexHashMutationLockOwner,
	): MaybePromise<boolean> {
		return this.withCoordinateMutationOwner(
			[
				properties.hash,
				...properties.nextHashes,
				...(properties.deleteHashes ?? []),
			],
			(_owned, assertOwned) =>
				this.persistPreparedCoordinateOwned(
					properties,
					() => {
						assertOwned();
						this.deps.throwIfReplicationOwnershipLifecycleInactive(
							ownershipLifecycleController,
						);
					},
					() => this.coordinateWriteResources(),
				),
			owner,
		);
	}

	private persistPreparedCoordinateOwned(
		properties: PreparedCoordinateWrite<R>,
		assertOwned: () => void,
		resources: () => ReturnType<
			CoordinatePersistenceCoordinator<R>["coordinateWriteResources"]
		>,
	): MaybePromise<boolean> {
		assertOwned();
		const { assignedToRangeBoundary, fields } = properties.prepared;
		const deleteHashes = combineCoordinateDeleteHashes(
			properties.nextHashes,
			properties.deleteHashes,
		);
		const coordinateIndex = resources().index;
		let pendingDeleteHashes = EMPTY_HASHES;
		let putResult: MaybePromise<unknown>;
		if (coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn) {
			putResult =
				coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn(
					fields,
					deleteHashes,
				);
		} else if (coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashes) {
			putResult = coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashes(
				fields,
				deleteHashes,
			);
		} else if (coordinateIndex.putSharedLogCoordinateFieldsAndDeleteIds) {
			putResult = coordinateIndex.putSharedLogCoordinateFieldsAndDeleteIds(
				fields,
				deleteHashes,
				toId(fields.hash),
			);
		} else if (coordinateIndex.putSharedLogCoordinateAndDeleteIds) {
			const coordinateEntry = this.materializePreparedCoordinateEntry(
				properties.prepared,
			);
			putResult = coordinateIndex.putSharedLogCoordinateAndDeleteIds(
				coordinateEntry,
				fields,
				deleteHashes,
				toId(fields.hash),
			);
		} else if (deleteHashes.length > 0 && coordinateIndex.putAndDeleteIds) {
			const coordinateEntry = this.materializePreparedCoordinateEntry(
				properties.prepared,
			);
			putResult = coordinateIndex.putAndDeleteIds(
				coordinateEntry,
				deleteHashes,
			);
		} else {
			const coordinateEntry = this.materializePreparedCoordinateEntry(
				properties.prepared,
			);
			if (deleteHashes.length > 0 && coordinateIndex.putAndDelete) {
				const batch = deleteHashes.slice(0, COORDINATE_DELETE_QUERY_BATCH_SIZE);
				putResult = coordinateIndex.putAndDelete(
					coordinateEntry,
					coordinateDeleteOptions(batch),
				);
				pendingDeleteHashes = deleteHashes.slice(batch.length);
			} else {
				putResult = coordinateIndex.put(coordinateEntry);
				pendingDeleteHashes = deleteHashes;
			}
		}

		const finish = (): MaybePromise<boolean> => {
			assertOwned();
			const current = resources();
			const nativeDeleteHashes = combineCoordinateDeleteHashes(
				properties.nextHashes,
				properties.deleteHashes,
			);
			if (properties.commitNative !== false) {
				current.nativeState?.commitEntryCoordinates(
					properties.hash,
					fields.gid,
					properties.coordinates,
					nativeDeleteHashes,
					assignedToRangeBoundary,
					properties.replicas,
					fields.hashNumber,
				);
			}
			if (properties.commitNativeBackbone !== false) {
				current.backbone?.commitEntryCoordinates(
					properties.hash,
					fields.gid,
					properties.coordinates,
					nativeDeleteHashes,
					assignedToRangeBoundary,
					properties.replicas,
					fields.hashNumber,
				);
			}
			if (current.resident) {
				current.resident.set(
					properties.hash,
					properties.prepared.coordinateEntry ?? fields,
				);
				for (const nextHash of nativeDeleteHashes) {
					current.resident.delete(nextHash);
				}
			}

			for (const coordinate of properties.coordinates) {
				current.coordinateToHash.add(coordinate, properties.hash);
			}

			return true;
		};
		return mapMaybePromise(putResult, () =>
			mapMaybePromise(
				this.deleteCoordinateIndexHashes(
					coordinateIndex,
					pendingDeleteHashes,
					assertOwned,
				),
				finish,
			),
		);
	}

	persistPreparedCoordinateNativeTransaction(
		properties: {
			coordinateIndex: PutAndDeleteIndex<EntryReplicated<R>>;
			prepared: PreparedCoordinatePersistence<R>;
			hash: string;
			nextHashes: string[];
			coordinates: NumberFromType<R>[];
			deleteHashes?: string[];
			commitNative?: boolean;
			commitNativeBackbone?: boolean;
		},
		ownershipLifecycleController = this.deps.captureReplicationOwnershipLifecycle(),
		owner?: EntryIndexHashMutationLockOwner,
	): MaybePromise<boolean> {
		const hashes = [
			properties.hash,
			...properties.nextHashes,
			...(properties.deleteHashes ?? []),
		];
		if (!owner) {
			return this.withCoordinateMutationOwner(hashes, (owned) =>
				this.persistPreparedCoordinateNativeTransaction(
					properties,
					ownershipLifecycleController,
					owned,
				),
			);
		}
		this.deps.log().entryIndex.assertHashMutationLocks(owner, hashes);
		this.deps.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const { fields } = properties.prepared;
		const putNative =
			properties.coordinateIndex
				.putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn ??
			properties.coordinateIndex
				.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn;
		if (!putNative) {
			return false;
		}
		const putResult = putNative.call(
			properties.coordinateIndex,
			fields,
			combineCoordinateDeleteHashes(
				properties.nextHashes,
				properties.deleteHashes,
			),
		);
		const finish = () => {
			this.deps.log().entryIndex.assertHashMutationLocks(owner, hashes);
			this.deps.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			const nativeDeleteHashes = combineCoordinateDeleteHashes(
				properties.nextHashes,
				properties.deleteHashes,
			);
			if (properties.commitNative !== false) {
				this.deps
					.nativeSharedLogState()
					?.commitEntryCoordinates(
						properties.hash,
						fields.gid,
						properties.coordinates,
						nativeDeleteHashes,
						properties.prepared.assignedToRangeBoundary,
						properties.coordinates.length,
						fields.hashNumber,
					);
			}
			if (properties.commitNativeBackbone !== false) {
				this.deps
					.nativeBackbone()
					?.commitEntryCoordinates(
						properties.hash,
						fields.gid,
						properties.coordinates,
						nativeDeleteHashes,
						properties.prepared.assignedToRangeBoundary,
						properties.coordinates.length,
						fields.hashNumber,
					);
			}
			if (this._residentEntryCoordinatesByHash) {
				this._residentEntryCoordinatesByHash.set(
					properties.hash,
					properties.prepared.coordinateEntry ?? fields,
				);
				for (const nextHash of nativeDeleteHashes) {
					this._residentEntryCoordinatesByHash.delete(nextHash);
				}
			}
			for (const coordinate of properties.coordinates) {
				this.deps.coordinateToHash().add(coordinate, properties.hash);
			}
			return true;
		};
		return mapMaybePromise(putResult, finish);
	}

	persistBackboneCoordinateFieldsNativeTransaction(
		properties: {
			coordinateIndex: PutAndDeleteIndex<EntryReplicated<R>>;
			fields: SharedLogCoordinateNativeFields<R>;
			hash: string;
			coordinates: NumberFromType<R>[];
			deleteHashes: string[];
			skipGenericTransientCoordinateIndex?: boolean;
		},
		ownershipLifecycleController = this.deps.captureReplicationOwnershipLifecycle(),
		owner?: EntryIndexHashMutationLockOwner,
	): MaybePromise<boolean> {
		const hashes = [properties.hash, ...properties.deleteHashes];
		if (!owner) {
			return this.withCoordinateMutationOwner(hashes, (owned) =>
				this.persistBackboneCoordinateFieldsNativeTransaction(
					properties,
					ownershipLifecycleController,
					owned,
				),
			);
		}
		this.deps.log().entryIndex.assertHashMutationLocks(owner, hashes);
		this.deps.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const { fields } = properties;
		const useBackboneOnlyCoordinatePersistence =
			this.canUseBackboneOnlyCoordinatePersistence();
		const finish = (): MaybePromise<boolean> => {
			this.deps.log().entryIndex.assertHashMutationLocks(owner, hashes);
			this.deps.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			this.deps
				.nativeSharedLogState()
				?.commitEntryCoordinates(
					properties.hash,
					fields.gid,
					properties.coordinates,
					properties.deleteHashes,
					fields.assignedToRangeBoundary,
					properties.coordinates.length,
					fields.hashNumber,
				);
			if (this._residentEntryCoordinatesByHash) {
				this._residentEntryCoordinatesByHash.set(properties.hash, fields);
				for (const deletedHash of properties.deleteHashes) {
					this._residentEntryCoordinatesByHash.delete(deletedHash);
				}
			}
			for (const coordinate of properties.coordinates) {
				this.deps.coordinateToHash().add(coordinate, properties.hash);
			}
			if (this._nativeBackboneCoordinatePersistence) {
				const flushed = this.flushNativeBackboneCoordinateJournalOnAppend();
				if (isPromiseLike(flushed)) {
					return mapMaybePromise(flushed, () => true);
				}
			}
			return true;
		};
		if (
			(properties.skipGenericTransientCoordinateIndex &&
				this.canUseRuntimeOnlyNativeBackboneCoordinates(
					properties.coordinateIndex,
				)) ||
			useBackboneOnlyCoordinatePersistence
		) {
			return finish();
		}

		const putNative =
			properties.coordinateIndex
				.putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn ??
			properties.coordinateIndex
				.putSharedLogCoordinateFieldsAndDeleteHashesNoReturn;
		if (!putNative) {
			return false;
		}
		const putResult = putNative.call(
			properties.coordinateIndex,
			fields,
			properties.deleteHashes,
		);
		return mapMaybePromise(putResult, finish);
	}

	flushNativeBackboneCoordinateJournal(
		backbone = this.deps.nativeBackbone(),
		persistence = this._nativeBackboneCoordinatePersistence,
	): MaybePromise<void> {
		if (!backbone || !persistence || this.deps.isDropStarted()) {
			return undefined;
		}
		if (
			backbone.coordinatePendingJournalLength === 0 &&
			backbone.documentPendingJournalLength === 0 &&
			backbone.documentSignerPendingJournalLength === 0
		) {
			return undefined;
		}
		return mapMaybePromise(persistence.flushJournal(backbone), () => {
			this._nativeBackboneCoordinateJournalLastFlushMs = Date.now();
			return undefined;
		});
	}

	flushNativeBackboneCoordinateJournalOnAppend(
		backbone = this.deps.nativeBackbone(),
		persistence = this._nativeBackboneCoordinatePersistence,
	): MaybePromise<void> {
		if (!backbone || !persistence || this.deps.isDropStarted()) {
			return undefined;
		}
		if (persistence.flushJournalOnAppend) {
			const flushed = persistence.flushJournalOnAppend(backbone);
			if (!isPromiseLike(flushed)) {
				return undefined;
			}
			return mapMaybePromise(flushed, () => {
				return undefined;
			});
		}
		if (
			!this.shouldFlushNativeBackboneCoordinateJournalOnAppend(
				backbone,
				persistence,
			)
		) {
			return undefined;
		}
		return this.flushNativeBackboneCoordinateJournal(backbone, persistence);
	}

	shouldFlushNativeBackboneCoordinateJournalOnAppend(
		backbone = this.deps.nativeBackbone(),
		persistence = this._nativeBackboneCoordinatePersistence,
	): boolean {
		if (!persistence || persistence.flushOnAppend !== false) {
			return true;
		}
		if (!backbone || backbone.coordinatePendingJournalLength === 0) {
			return false;
		}
		if (
			persistence.flushMaxPendingBytes != null &&
			backbone.coordinatePendingJournalByteLength >=
				persistence.flushMaxPendingBytes
		) {
			return true;
		}
		return (
			persistence.flushIntervalMs != null &&
			Date.now() - this._nativeBackboneCoordinateJournalLastFlushMs >=
				persistence.flushIntervalMs
		);
	}

	async closeNativeBackboneCoordinatePersistence(): Promise<void> {
		const persistence = this._nativeBackboneCoordinatePersistence;
		if (!persistence) {
			return;
		}
		if (this.deps.isDropStarted()) {
			// `drop()` owns the durable namespace lifecycle. Never flush the live wasm
			// journals or invoke an ordinary custom close after its tombstone/erase has
			// started: a close implementation that rewrites cached state could resurrect
			// files after a successful terminal drop.
			return;
		}
		if (
			this.deps.getDurableCommitFailure() &&
			!this.deps.isDurableRecoveryReadyForReopen()
		) {
			// The failed native transaction was never published by the lower log.
			// Its coordinate/document/signer records are still only in the wasm
			// pending journals. Closing without flushing discards that generation;
			// the next backbone hydrates the last acknowledged checkpoint.
			await persistence.close?.();
			this.deps.setDurableRecoveryReadyForReopen(true);
			return;
		}
		await this.flushNativeBackboneCoordinateJournal();
		await persistence.close?.();
	}

	canUseBackboneOnlyCoordinatePersistence(): boolean {
		return (
			!!this._nativeBackboneCoordinatePersistence &&
			this.deps.canUseNativeBackboneResidentCoordinateState()
		);
	}

	canUseNativeBackboneResidentCoordinateState(): boolean {
		return (
			!!this.deps.nativeBackbone() &&
			!!this._residentEntryCoordinatesByHash &&
			!this.deps.hasCustomFindLeaders()
		);
	}

	canUseRuntimeOnlyNativeBackboneCoordinates(
		coordinateIndex: PutAndDeleteIndex<EntryReplicated<R>>,
	): boolean {
		if (
			!this.deps.canUseNativeBackboneResidentCoordinateState() ||
			Object.prototype.hasOwnProperty.call(
				coordinateIndex,
				"putSharedLogCoordinateFieldsEncodedAndDeleteHashesNoReturn",
			) ||
			Object.prototype.hasOwnProperty.call(
				coordinateIndex,
				"putSharedLogCoordinateFieldsAndDeleteHashesNoReturn",
			)
		) {
			return false;
		}
		const persisted = (
			coordinateIndex as PutAndDeleteIndex<EntryReplicated<R>> & {
				persisted?: () => MaybePromise<boolean>;
			}
		).persisted?.();
		return persisted === false;
	}

	async persistCoordinate(
		properties: {
			coordinates: NumberFromType<R>[];
			entry: ShallowOrFullEntry<any> | EntryReplicated<R>;
			leaders:
				| Map<
						string,
						{
							intersecting: boolean;
						}
				  >
				| false;
			replicas: number;
			prev?: EntryReplicated<R>;
			assignedToRangeBoundary?: boolean;
			commitNative?: boolean;
			commitNativeBackbone?: boolean;
			deleteHashes?: string[];
			hashNumber?: NumberFromType<R>;
			nextHashes?: string[];
			prepared?: PreparedCoordinatePersistence<R>;
		},
		ownershipLifecycleController = this.deps.captureReplicationOwnershipLifecycle(),
		owner?: EntryIndexHashMutationLockOwner,
	) {
		this.deps.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const prepared =
			properties.prepared ?? this.createCoordinatePersistenceEntry(properties);
		if (!prepared) {
			return false;
		}
		return this.persistPreparedCoordinate(
			{
				prepared,
				hash: properties.entry.hash,
				nextHashes: properties.nextHashes ?? properties.entry.meta.next,
				coordinates: properties.coordinates,
				replicas: properties.replicas,
				commitNative: properties.commitNative,
				commitNativeBackbone: properties.commitNativeBackbone,
				deleteHashes: properties.deleteHashes,
			},
			ownershipLifecycleController,
			owner,
		);
	}

	async persistCoordinatesBatch(
		items: CoordinatePersistBatchItem<R>[],
		ownershipLifecycleController = this.deps.captureReplicationOwnershipLifecycle(),
		owner?: EntryIndexHashMutationLockOwner,
	): Promise<boolean[]> {
		this.deps.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		if (items.length === 0) {
			return [];
		}

		const prepared = items.map((item) => ({
			item,
			prepared: item.prepared ?? this.createCoordinatePersistenceEntry(item),
		}));
		const changed = prepared.filter(
			(
				entry,
			): entry is {
				item: (typeof items)[number];
				prepared: PreparedCoordinatePersistence<R>;
			} => entry.prepared !== false,
		);
		if (changed.length === 0) {
			return items.map(() => false);
		}

		const writes = changed.map(({ item, prepared }) => ({
			prepared,
			hash: item.entry.hash,
			nextHashes: item.entry.meta.next,
			deleteHashes: item.deleteHashes,
			coordinates: item.coordinates,
			replicas: item.replicas,
			commitNative: item.commitNative,
			commitNativeBackbone: item.commitNativeBackbone,
		}));
		await this.withCoordinateMutationOwner(
			writes.flatMap((write) => [
				write.hash,
				...write.nextHashes,
				...(write.deleteHashes ?? []),
			]),
			(_owned, assertOwned) => {
				const resources = this.coordinateWriteResources();
				return this.persistPreparedCoordinatesOwned(
					writes,
					() => {
						assertOwned();
						this.deps.throwIfReplicationOwnershipLifecycleInactive(
							ownershipLifecycleController,
						);
					},
					() => resources,
				);
			},
			owner,
		);
		const changedHashes = new Set(
			changed.map(({ prepared }) => prepared.fields.hash),
		);
		return items.map((item) => changedHashes.has(item.entry.hash));
	}

	private async persistPreparedCoordinatesOwned(
		writes: PreparedCoordinateWrite<R>[],
		assertOwned: () => void,
		resources: () => ReturnType<
			CoordinatePersistenceCoordinator<R>["coordinateWriteResources"]
		>,
		backboneOnly = false,
	): Promise<void> {
		assertOwned();
		if (writes.length === 0) return;
		const current = resources();
		const coordinateIndex = current.index;
		const deleteHashes = (write: PreparedCoordinateWrite<R>) =>
			combineCoordinateDeleteHashes(write.nextHashes, write.deleteHashes);
		const canUseGenericPutBatch =
			typeof coordinateIndex.putBatch === "function" &&
			writes.every((write) => deleteHashes(write).length === 0);

		if (backboneOnly) {
			// The native journal and resident mirror are authoritative in this mode.
		} else if (
			coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn
		) {
			await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn(
				writes.map((write) => ({
					fields: write.prepared.fields,
					deleteHashes: deleteHashes(write),
				})),
			);
		} else if (
			coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesBatch
		) {
			await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesBatch(
				writes.map((write) => ({
					fields: write.prepared.fields,
					deleteHashes: deleteHashes(write),
				})),
			);
		} else if (coordinateIndex.putSharedLogCoordinateFieldsAndDeleteIdsBatch) {
			await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteIdsBatch(
				writes.map((write) => ({
					fields: write.prepared.fields,
					deleteIds: deleteHashes(write),
					id: toId(write.prepared.fields.hash),
				})),
			);
		} else if (coordinateIndex.putSharedLogCoordinatesAndDeleteIdsBatch) {
			await coordinateIndex.putSharedLogCoordinatesAndDeleteIdsBatch(
				writes.map((write) => ({
					value: this.materializePreparedCoordinateEntry(write.prepared),
					fields: write.prepared.fields,
					deleteIds: deleteHashes(write),
					id: toId(write.prepared.fields.hash),
				})),
			);
		} else if (canUseGenericPutBatch) {
			await coordinateIndex.putBatch!(
				writes.map(({ prepared }) =>
					this.materializePreparedCoordinateEntry(prepared),
				),
			);
		} else {
			for (const write of writes) {
				await this.persistPreparedCoordinateOwned(
					write,
					assertOwned,
					resources,
				);
				assertOwned();
			}
			return;
		}
		assertOwned();

		const nativeCoordinateCommits = writes.filter(
			(write) => write.commitNative !== false,
		);
		const nativeSharedLogState = current.nativeState;
		if (nativeCoordinateCommits.length > 0 && nativeSharedLogState) {
			if (nativeSharedLogState.commitEntryCoordinatesBatch) {
				nativeSharedLogState.commitEntryCoordinatesBatch(
					nativeCoordinateCommits.map((write) => ({
						hash: write.hash,
						gid: write.prepared.fields.gid,
						coordinates: write.coordinates,
						nextHashes: deleteHashes(write),
						assignedToRangeBoundary: write.prepared.assignedToRangeBoundary,
						requestedReplicas: write.replicas,
						hashNumber: write.prepared.fields.hashNumber,
					})),
				);
			} else {
				for (const write of nativeCoordinateCommits) {
					nativeSharedLogState.commitEntryCoordinates(
						write.hash,
						write.prepared.fields.gid,
						write.coordinates,
						deleteHashes(write),
						write.prepared.assignedToRangeBoundary,
						write.replicas,
						write.prepared.fields.hashNumber,
					);
				}
			}
		}

		const nativeBackboneCoordinateCommits = writes.filter(
			(write) => write.commitNativeBackbone !== false,
		);
		const nativeBackboneForBatch = current.backbone;
		if (nativeBackboneCoordinateCommits.length > 0 && nativeBackboneForBatch) {
			if (nativeBackboneForBatch.commitEntryCoordinatesBatch) {
				nativeBackboneForBatch.commitEntryCoordinatesBatch(
					nativeBackboneCoordinateCommits.map((write) => ({
						hash: write.hash,
						gid: write.prepared.fields.gid,
						coordinates: write.coordinates,
						nextHashes: deleteHashes(write),
						assignedToRangeBoundary: write.prepared.assignedToRangeBoundary,
						requestedReplicas: write.replicas,
						hashNumber: write.prepared.fields.hashNumber,
					})),
				);
			} else {
				for (const write of nativeBackboneCoordinateCommits) {
					nativeBackboneForBatch.commitEntryCoordinates(
						write.hash,
						write.prepared.fields.gid,
						write.coordinates,
						deleteHashes(write),
						write.prepared.assignedToRangeBoundary,
						write.replicas,
						write.prepared.fields.hashNumber,
					);
				}
			}
		}

		for (const write of writes) {
			if (current.resident) {
				current.resident.set(
					write.hash,
					write.prepared.coordinateEntry ?? write.prepared.fields,
				);
				for (const nextHash of deleteHashes(write)) {
					current.resident.delete(nextHash);
				}
			}
			for (const coordinate of write.coordinates) {
				current.coordinateToHash.add(coordinate, write.hash);
			}
		}

		if (backboneOnly) {
			await this.flushNativeBackboneCoordinateJournalOnAppend(
				current.backbone,
				current.persistence,
			);
			assertOwned();
		}
	}

	async deleteCoordinates(
		properties: { hash: string },
		ownershipLifecycleController?: AbortController,
		owner?: EntryIndexHashMutationLockOwner,
	) {
		await this.deleteCoordinatesForHashes(
			[properties.hash],
			ownershipLifecycleController,
			owner,
		);
	}
}
