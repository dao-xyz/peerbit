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
	// roll back only while that generation is still current, so a rollback
	// superseded by a newer mutation is a strict no-op. The map deliberately
	// survives open/close cycles of the same instance.
	_nativeCoordinateMutationGenerations?: Map<string, number>;

	/**
	 * Wall-clock watermark of the last settled native coordinate journal
	 * flush; drives the `flushIntervalMs` on-append flush threshold.
	 */
	_nativeBackboneCoordinateJournalLastFlushMs = 0;

	constructor(private readonly deps: CoordinatePersistenceDeps<R>) {}

	deleteCoordinatesForHashes(
		hashes: Iterable<string>,
		ownershipLifecycleController?: AbortController,
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
		this.forgetCoordinateStateForHashValues(values);
		const coordinateIndex = this.deps.entryCoordinatesIndex() as PutAndDeleteIndex<
			EntryReplicated<R>
		>;
		if (coordinateIndex.delIdsNoReturn) {
			return mapMaybePromise(coordinateIndex.delIdsNoReturn(values), () => {
				if (ownershipLifecycleController) {
					this.deps.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
				}
			});
		}
		if (coordinateIndex.delIds) {
			return mapMaybePromise(coordinateIndex.delIds(values), () => {
				if (ownershipLifecycleController) {
					this.deps.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
				}
			});
		}
		return mapMaybePromise(
			this.deps.entryCoordinatesIndex().del({
				query:
					values.length === 1
						? { hash: values[0] }
						: new Or(
								values.map(
									(hash) => new StringMatch({ key: "hash", value: hash }),
								),
							),
			}),
			() => {
				if (ownershipLifecycleController) {
					this.deps.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
				}
			},
		);
	}

	forgetCoordinateStateForHashes(hashes: Iterable<string>) {
		const values = normalizedHashValues(hashes);
		if (values.length === 0) {
			return;
		}
		this.forgetCoordinateStateForHashValues(values);
	}

	forgetCoordinateStateForHashValues(values: string[]) {
		this.deps.nativeSharedLogState()?.deleteEntryCoordinatesBatch(values);
		this.deps.nativeBackbone()?.deleteEntryCoordinatesBatch(values);
		this.forgetResidentCoordinateStateForHashValues(values);
	}

	forgetResidentCoordinateStateForHashes(hashes: Iterable<string>) {
		const values = normalizedHashValues(hashes);
		if (values.length === 0) {
			return;
		}
		this.forgetResidentCoordinateStateForHashValues(values);
	}

	forgetResidentCoordinateStateForHashValues(values: string[]) {
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
			nativeGrid ?? this.deps.indexableDomain().numbers.getGrid(cursor, minReplicas)
		);
	}

	async getCoordinates(entry: { hash: string }) {
		const nativeCoordinates = (
			this.deps.nativeBackbone() ?? this.deps.nativeSharedLogState()
		)?.getEntryCoordinates(entry.hash);
		if (nativeCoordinates) {
			return nativeCoordinates as NumberFromType<R>[];
		}
		const result = await this.deps.entryCoordinatesIndex()
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
		if (this.deps.timeUntilRoleMaturity() > 0 && !options?.allowRoleAgeZeroPlans) {
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

		return {
			rows,
			rollbackCoordinateEntries: this.snapshotResidentCoordinateEntries(
				rows.flatMap((row) => [row.item.entry.hash, ...row.deleteHashes]),
			),
		};
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
	): Promise<Set<string> | undefined> {
		const backbone = this.deps.nativeBackbone();
		const batch = this.createBackboneOnlyReceiveCoordinateBatch(items);
		if (!backbone || !batch) {
			return undefined;
		}
		try {
			backbone.commitEntryCoordinatesColumnsBatch(
				this.nativeBackboneReceiveCoordinateRowsToColumns(batch.rows),
			);
			return await this.finishBackboneOnlyReceiveCoordinateBatch(batch);
		} catch (error) {
			this.rollbackBackboneOnlyReceiveCoordinateBatch(batch);
			throw error;
		}
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
	): NativeBackboneCoordinateRollback<R> | undefined {
		const uniqueHashes = new Set([...hashes].filter(Boolean));
		if (uniqueHashes.size === 0) {
			return undefined;
		}
		const entries = new Map<string, ResidentCoordinateEntry<R>>();
		const generations = new Map<string, number>();
		const mutationGenerations = (this._nativeCoordinateMutationGenerations ??=
			new Map());
		for (const hash of uniqueHashes) {
			const generation = (mutationGenerations.get(hash) ?? 0) + 1;
			mutationGenerations.set(hash, generation);
			generations.set(hash, generation);
			const entry = this._residentEntryCoordinatesByHash?.get(hash);
			if (entry) {
				entries.set(hash, entry);
			}
		}
		return { hashes: uniqueHashes, entries, generations };
	}

	rollbackNativeBackboneCoordinateAppend(
		appendHash: string,
		rollback?: NativeBackboneCoordinateRollback<R>,
	): void {
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
				mutationGenerations.get(hash) !== expectedGeneration
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
			this.deps.nativeSharedLogState()?.putEntryCoordinates(
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
		const coordinateIndex = this.deps.entryCoordinatesIndex() as PutAndDeleteIndex<
			EntryReplicated<R>
		>;
		const hashes = rollback?.hashes ?? new Set([appendHash]);
		const mutationGenerations = (this._nativeCoordinateMutationGenerations ??=
			new Map());
		for (const hash of hashes) {
			const expectedGeneration = rollback?.generations.get(hash);
			if (
				expectedGeneration !== undefined &&
				mutationGenerations.get(hash) !== expectedGeneration
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
		properties: {
			prepared: PreparedCoordinatePersistence<R>;
			hash: string;
			nextHashes: string[];
			coordinates: NumberFromType<R>[];
			replicas: number;
			commitNative?: boolean;
			commitNativeBackbone?: boolean;
			deleteHashes?: string[];
		},
		ownershipLifecycleController = this.deps.captureReplicationOwnershipLifecycle(),
	): MaybePromise<boolean> {
		this.deps.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const { assignedToRangeBoundary, fields } = properties.prepared;
		const deleteHashes = combineCoordinateDeleteHashes(
			properties.nextHashes,
			properties.deleteHashes,
		);
		const coordinateIndex = this.deps.entryCoordinatesIndex() as PutAndDeleteIndex<
			EntryReplicated<R>
		>;
		let deleteNextOptions: DeleteOptions | undefined;
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
			deleteNextOptions =
				deleteHashes.length === 0
					? undefined
					: deleteHashes.length === 1
						? { query: { hash: deleteHashes[0] } }
						: {
								query: new Or(
									deleteHashes.map(
										(x) => new StringMatch({ key: "hash", value: x }),
									),
								),
							};
			if (deleteNextOptions && coordinateIndex.putAndDelete) {
				putResult = coordinateIndex.putAndDelete(
					coordinateEntry,
					deleteNextOptions,
				);
			} else {
				putResult = this.deps.entryCoordinatesIndex().put(coordinateEntry);
			}
		}

		const finish = (): MaybePromise<boolean> => {
			this.deps.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			const nativeDeleteHashes = combineCoordinateDeleteHashes(
				properties.nextHashes,
				properties.deleteHashes,
			);
			if (properties.commitNative !== false) {
				this.deps.nativeSharedLogState()?.commitEntryCoordinates(
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
				this.deps.nativeBackbone()?.commitEntryCoordinates(
					properties.hash,
					fields.gid,
					properties.coordinates,
					nativeDeleteHashes,
					assignedToRangeBoundary,
					properties.replicas,
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

			if (deleteNextOptions && !coordinateIndex.putAndDelete) {
				return mapMaybePromise(
					this.deps.entryCoordinatesIndex().del(deleteNextOptions),
					() => true,
				);
			}
			return true;
		};
		return mapMaybePromise(putResult, finish);
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
	): MaybePromise<boolean> {
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
			this.deps.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			const nativeDeleteHashes = combineCoordinateDeleteHashes(
				properties.nextHashes,
				properties.deleteHashes,
			);
			if (properties.commitNative !== false) {
				this.deps.nativeSharedLogState()?.commitEntryCoordinates(
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
				this.deps.nativeBackbone()?.commitEntryCoordinates(
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
	): MaybePromise<boolean> {
		this.deps.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);
		const { fields } = properties;
		const useBackboneOnlyCoordinatePersistence =
			this.canUseBackboneOnlyCoordinatePersistence();
		const finish = (): MaybePromise<boolean> => {
			this.deps.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
			this.deps.nativeSharedLogState()?.commitEntryCoordinates(
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

	flushNativeBackboneCoordinateJournal(): MaybePromise<void> {
		const backbone = this.deps.nativeBackbone();
		const persistence = this._nativeBackboneCoordinatePersistence;
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

	flushNativeBackboneCoordinateJournalOnAppend(): MaybePromise<void> {
		const backbone = this.deps.nativeBackbone();
		const persistence = this._nativeBackboneCoordinatePersistence;
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
		if (!this.shouldFlushNativeBackboneCoordinateJournalOnAppend()) {
			return undefined;
		}
		return this.flushNativeBackboneCoordinateJournal();
	}

	shouldFlushNativeBackboneCoordinateJournalOnAppend(): boolean {
		const persistence = this._nativeBackboneCoordinatePersistence;
		if (!persistence || persistence.flushOnAppend !== false) {
			return true;
		}
		const backbone = this.deps.nativeBackbone();
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
		);
	}

	async persistCoordinatesBatch(
		items: CoordinatePersistBatchItem<R>[],
		ownershipLifecycleController = this.deps.captureReplicationOwnershipLifecycle(),
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

		const coordinateIndex = this.deps.entryCoordinatesIndex() as PutAndDeleteIndex<
			EntryReplicated<R>
		>;
		const canUseGenericPutBatch =
			typeof coordinateIndex.putBatch === "function" &&
			changed.every(({ item }) => item.entry.meta.next.length === 0);

		if (
			coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn
		) {
			await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesBatchNoReturn(
				changed.map(({ item, prepared }) => ({
					fields: prepared.fields,
					deleteHashes: item.entry.meta.next,
				})),
			);
		} else if (
			coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesBatch
		) {
			await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteHashesBatch(
				changed.map(({ item, prepared }) => ({
					fields: prepared.fields,
					deleteHashes: item.entry.meta.next,
				})),
			);
		} else if (coordinateIndex.putSharedLogCoordinateFieldsAndDeleteIdsBatch) {
			await coordinateIndex.putSharedLogCoordinateFieldsAndDeleteIdsBatch(
				changed.map(({ item, prepared }) => ({
					fields: prepared.fields,
					deleteIds: item.entry.meta.next,
					id: toId(prepared.fields.hash),
				})),
			);
		} else if (coordinateIndex.putSharedLogCoordinatesAndDeleteIdsBatch) {
			await coordinateIndex.putSharedLogCoordinatesAndDeleteIdsBatch(
				changed.map(({ item, prepared }) => ({
					value: this.materializePreparedCoordinateEntry(prepared),
					fields: prepared.fields,
					deleteIds: item.entry.meta.next,
					id: toId(prepared.fields.hash),
				})),
			);
		} else if (canUseGenericPutBatch) {
			await coordinateIndex.putBatch!(
				changed.map(({ prepared }) =>
					this.materializePreparedCoordinateEntry(prepared),
				),
			);
		} else {
			const results: boolean[] = [];
			for (const item of items) {
				results.push(
					await this.persistCoordinate(item, ownershipLifecycleController),
				);
				this.deps.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
			}
			return results;
		}
		this.deps.throwIfReplicationOwnershipLifecycleInactive(
			ownershipLifecycleController,
		);

		const nativeCoordinateCommits = changed.filter(
			({ item }) => item.commitNative !== false,
		);
		const nativeSharedLogState = this.deps.nativeSharedLogState();
		if (nativeCoordinateCommits.length > 0 && nativeSharedLogState) {
			if (nativeSharedLogState.commitEntryCoordinatesBatch) {
				nativeSharedLogState.commitEntryCoordinatesBatch(
					nativeCoordinateCommits.map(({ item, prepared }) => ({
						hash: item.entry.hash,
						gid: prepared.fields.gid,
						coordinates: item.coordinates,
						nextHashes: item.entry.meta.next,
						assignedToRangeBoundary: prepared.assignedToRangeBoundary,
						requestedReplicas: item.replicas,
						hashNumber: prepared.fields.hashNumber,
					})),
				);
			} else {
				for (const { item, prepared } of nativeCoordinateCommits) {
					nativeSharedLogState.commitEntryCoordinates(
						item.entry.hash,
						prepared.fields.gid,
						item.coordinates,
						item.entry.meta.next,
						prepared.assignedToRangeBoundary,
						item.replicas,
						prepared.fields.hashNumber,
					);
				}
			}
		}

		const nativeBackboneCoordinateCommits = changed.filter(
			({ item }) => item.commitNativeBackbone !== false,
		);
		const nativeBackboneForBatch = this.deps.nativeBackbone();
		if (nativeBackboneCoordinateCommits.length > 0 && nativeBackboneForBatch) {
			if (nativeBackboneForBatch.commitEntryCoordinatesBatch) {
				nativeBackboneForBatch.commitEntryCoordinatesBatch(
					nativeBackboneCoordinateCommits.map(({ item, prepared }) => ({
						hash: item.entry.hash,
						gid: prepared.fields.gid,
						coordinates: item.coordinates,
						nextHashes: item.entry.meta.next,
						assignedToRangeBoundary: prepared.assignedToRangeBoundary,
						requestedReplicas: item.replicas,
						hashNumber: prepared.fields.hashNumber,
					})),
				);
			} else {
				for (const { item, prepared } of nativeBackboneCoordinateCommits) {
					nativeBackboneForBatch.commitEntryCoordinates(
						item.entry.hash,
						prepared.fields.gid,
						item.coordinates,
						item.entry.meta.next,
						prepared.assignedToRangeBoundary,
						item.replicas,
						prepared.fields.hashNumber,
					);
				}
			}
		}

		for (const { item, prepared } of changed) {
			if (this._residentEntryCoordinatesByHash) {
				this._residentEntryCoordinatesByHash.set(
					item.entry.hash,
					prepared.coordinateEntry ?? prepared.fields,
				);
				for (const nextHash of item.entry.meta.next) {
					this._residentEntryCoordinatesByHash.delete(nextHash);
				}
			}
			for (const coordinate of item.coordinates) {
				this.deps.coordinateToHash().add(coordinate, item.entry.hash);
			}
		}

		const changedHashes = new Set(
			changed.map(({ prepared }) => prepared.fields.hash),
		);
		return items.map((item) => changedHashes.has(item.entry.hash));
	}

	async deleteCoordinates(
		properties: { hash: string },
		ownershipLifecycleController?: AbortController,
	) {
		if (ownershipLifecycleController) {
			this.deps.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
		this.deps.nativeSharedLogState()?.deleteEntryCoordinates(properties.hash);
		this.deps.nativeBackbone()?.deleteEntryCoordinates(properties.hash);
		this._residentEntryCoordinatesByHash?.delete(properties.hash);
		const coordinateIndex = this.deps.entryCoordinatesIndex() as PutAndDeleteIndex<
			EntryReplicated<R>
		>;
		if (coordinateIndex.delIds) {
			await coordinateIndex.delIds([properties.hash]);
		} else {
			await this.deps.entryCoordinatesIndex().del({ query: properties });
		}
		if (ownershipLifecycleController) {
			this.deps.throwIfReplicationOwnershipLifecycleInactive(
				ownershipLifecycleController,
			);
		}
	}
}
