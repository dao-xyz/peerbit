import type { Cache } from "@peerbit/cache";
import type { PublicSignKey } from "@peerbit/crypto";
import type { SyncableKey } from "./index.js";

// The keyed record store for pending simple-sync claims (stage-4 sync
// unification). One record per retained key replaces the former lockstep
// map family (claimants, claimant indexes, round-robin cursors and key
// expiry nodes); the expiry heap holds the records themselves, unioned with
// admission-reservation expiry nodes. The public queue maps survive as the
// store's physical indexes — the SAME Map instances tests and integrations
// have always seeded and observed — and directly seeded keys hydrate into
// records on first access, exactly like the former defensive branches.
//
// Lifetimes: a record exists while its key is retained (one add path:
// addPendingSyncClaim; unified removal: clearSyncProcessKey /
// removePendingSyncClaim / TTL expiry / close). Admission reservations keep
// their quota charged until their originating lookup settles — invalidation
// (expiry/disconnect/close) deliberately does not return the quota early.

// Late coordinate-to-hash cache fills are discovered incrementally. Keep this
// independent of retained queue size so an empty or repeated request cannot
// force an O(global pending keys) reverse-alias rebuild.
const MAX_PENDING_SIMPLE_SYNC_ALIAS_REFRESH_PER_MESSAGE = 128;
export const QUEUED_SYNC_ALIAS_REFRESH_PENDING = Symbol(
	"queued-sync-alias-refresh-pending",
);
// This is an absolute first-seen lifetime. Repeated claims and additional peers
// deliberately do not slide the deadline.
export const PENDING_SIMPLE_SYNC_KEY_TTL_MS = 60_000;

export type PendingSyncAdmissionReservation = {
	peer: string;
	remaining: number;
	active: boolean;
	released: boolean;
	expiresAt: number;
	identities: Set<SyncableKey>;
	retainedSettled: number;
};

export type PendingSyncRecord = {
	kind: "key";
	key: SyncableKey;
	peers: PublicSignKey[];
	claimants: Set<string>;
	claimantIndexes: Map<string, number>;
	rrCursor: number;
	expiresAt: number;
	heapIndex: number;
};

type PendingSyncAdmissionExpiryNode = {
	kind: "admission";
	reservation: PendingSyncAdmissionReservation;
	expiresAt: number;
	heapIndex: number;
};

type PendingSyncExpiryNode = PendingSyncRecord | PendingSyncAdmissionExpiryNode;

export type PendingSyncStoreDeps = {
	coordinateToHash: Cache<string>;
	isDispatchEpochCurrent: (peer: string, epoch: unknown) => boolean;
};

export class PendingSyncStore {
	// map of hash to public keys that we can ask for entries. The public
	// legacy view instances: the same Maps the synchronizer has always
	// exposed, kept as the store's physical indexes.
	syncInFlightQueue: Map<SyncableKey, PublicSignKey[]>;
	syncInFlightQueueInverted: Map<string, Set<SyncableKey>>;
	syncInFlightQueueExpiresAt: Map<SyncableKey, number>;
	records: Map<SyncableKey, PendingSyncRecord>;
	pendingSyncExpiryHeap: PendingSyncExpiryNode[];
	syncInFlightQueueExpiryTimer?: ReturnType<typeof setTimeout>;
	pendingSyncAdmissionExpiryNodes: Map<
		PendingSyncAdmissionReservation,
		PendingSyncAdmissionExpiryNode
	>;
	syncInFlightQueuedCoordinates: Set<bigint>;
	syncInFlightQueuedHashByCoordinate: Map<bigint, string>;
	syncInFlightQueuedCoordinatesByHash: Map<string, Set<bigint>>;
	syncInFlightQueuedCoordinateRefreshIterator?: IterableIterator<bigint>;
	pendingSyncClaimCount: number;
	pendingSyncAdmissionCount: number;
	pendingSyncActiveAdmissionReservations: number;
	pendingSyncAdmissionCountByPeer: Map<string, number>;
	pendingSyncAdmissionIdentitiesByPeer: Map<string, Set<SyncableKey>>;
	pendingSyncAdmissionReservations: Set<PendingSyncAdmissionReservation>;
	pendingSyncAdmissionReservationsByPeer: Map<
		string,
		Set<PendingSyncAdmissionReservation>
	>;
	pendingSyncAdmissionReservationsByIdentity: Map<
		SyncableKey,
		Set<PendingSyncAdmissionReservation>
	>;

	// map of hash to public keys that we have asked for entries
	syncInFlight: Map<string, Map<SyncableKey, { timestamp: number }>>;
	syncInFlightTargetsByKey: Map<SyncableKey, Set<string>>;

	constructor(private readonly deps: PendingSyncStoreDeps) {
		this.syncInFlightQueue = new Map();
		this.syncInFlightQueueInverted = new Map();
		this.syncInFlightQueueExpiresAt = new Map();
		this.records = new Map();
		this.pendingSyncExpiryHeap = [];
		this.pendingSyncAdmissionExpiryNodes = new Map();
		this.syncInFlightQueuedCoordinates = new Set();
		this.syncInFlightQueuedHashByCoordinate = new Map();
		this.syncInFlightQueuedCoordinatesByHash = new Map();
		this.pendingSyncClaimCount = 0;
		this.pendingSyncAdmissionCount = 0;
		this.pendingSyncActiveAdmissionReservations = 0;
		this.pendingSyncAdmissionCountByPeer = new Map();
		this.pendingSyncAdmissionIdentitiesByPeer = new Map();
		this.pendingSyncAdmissionReservations = new Set();
		this.pendingSyncAdmissionReservationsByPeer = new Map();
		this.pendingSyncAdmissionReservationsByIdentity = new Map();
		this.syncInFlight = new Map();
		this.syncInFlightTargetsByKey = new Map();
	}

	// Defensive compatibility for callers/tests that seed the public queue
	// maps directly. Internally every retained key gets its record at
	// creation; a seeded key hydrates here (counting its claimants) and never
	// enters the expiry heap, exactly like the former hydration branches.
	private hydrateRecordFromQueue(
		key: SyncableKey,
		peers: PublicSignKey[],
	): PendingSyncRecord {
		const claimants = new Set(peers.map((peer) => peer.hashcode()));
		const claimantIndexes = new Map<string, number>();
		for (let index = 0; index < peers.length; index += 1) {
			claimantIndexes.set(peers[index]!.hashcode(), index);
		}
		const record: PendingSyncRecord = {
			kind: "key",
			key,
			peers,
			claimants,
			claimantIndexes,
			rrCursor: 0,
			expiresAt:
				this.syncInFlightQueueExpiresAt.get(key) ?? Number.POSITIVE_INFINITY,
			heapIndex: -1,
		};
		this.records.set(key, record);
		this.pendingSyncClaimCount += claimants.size;
		return record;
	}

	getPendingSyncKeyIdentity(key: SyncableKey): SyncableKey {
		if (typeof key === "string") {
			return key;
		}
		const hash = this.deps.coordinateToHash.get(key);
		return hash ?? key;
	}

	private removeQueuedSyncCoordinateAlias(key: bigint): void {
		this.syncInFlightQueuedCoordinates.delete(key);
		if (this.syncInFlightQueuedCoordinates.size === 0) {
			this.syncInFlightQueuedCoordinateRefreshIterator = undefined;
		}
		const previousHash = this.syncInFlightQueuedHashByCoordinate.get(key);
		this.syncInFlightQueuedHashByCoordinate.delete(key);
		if (previousHash != null) {
			const coordinates =
				this.syncInFlightQueuedCoordinatesByHash.get(previousHash);
			coordinates?.delete(key);
			if (coordinates?.size === 0) {
				this.syncInFlightQueuedCoordinatesByHash.delete(previousHash);
			}
		}
	}

	private refreshQueuedSyncCoordinateAlias(key: bigint): void {
		if (!this.syncInFlightQueue.has(key)) {
			this.removeQueuedSyncCoordinateAlias(key);
			return;
		}
		this.syncInFlightQueuedCoordinates.add(key);
		const hash = this.deps.coordinateToHash.get(key) ?? undefined;
		const previousHash = this.syncInFlightQueuedHashByCoordinate.get(key);
		if (previousHash !== hash) {
			if (previousHash != null) {
				const coordinates =
					this.syncInFlightQueuedCoordinatesByHash.get(previousHash);
				coordinates?.delete(key);
				if (coordinates?.size === 0) {
					this.syncInFlightQueuedCoordinatesByHash.delete(previousHash);
				}
			}
			if (hash == null) {
				this.syncInFlightQueuedHashByCoordinate.delete(key);
			} else {
				this.syncInFlightQueuedHashByCoordinate.set(key, hash);
			}
		}
		if (hash != null) {
			let coordinates = this.syncInFlightQueuedCoordinatesByHash.get(hash);
			if (!coordinates) {
				coordinates = new Set();
				this.syncInFlightQueuedCoordinatesByHash.set(hash, coordinates);
			}
			coordinates.add(key);
			this.reconcileQueuedSyncCoordinateAlias(key, hash);
		}
	}

	private reconcileQueuedSyncCoordinateAlias(
		coordinate: bigint,
		hash: string,
	): void {
		const now = Date.now();
		const coordinateExpiresAt = this.syncInFlightQueueExpiresAt.get(coordinate);
		if (coordinateExpiresAt != null && coordinateExpiresAt <= now) {
			this.clearSyncProcessKey(coordinate);
			return;
		}
		const hashExpiresAt = this.syncInFlightQueueExpiresAt.get(hash);
		if (hashExpiresAt != null && hashExpiresAt <= now) {
			this.clearSyncProcessKey(hash);
			return;
		}
		const hashClaimants = this.syncInFlightQueue.get(hash);
		if (!hashClaimants || !this.syncInFlightQueue.has(coordinate)) {
			return;
		}
		const expiresAt = Math.min(
			this.syncInFlightQueueExpiresAt.get(coordinate) ?? Infinity,
			this.syncInFlightQueueExpiresAt.get(hash) ?? Infinity,
		);
		for (const claimant of [...hashClaimants]) {
			this.addPendingSyncClaim(coordinate, claimant, expiresAt);
		}
		if (Number.isFinite(expiresAt)) {
			this.movePendingSyncKeyExpiryEarlier(coordinate, expiresAt);
		}
		for (const target of [...(this.syncInFlightTargetsByKey.get(hash) ?? [])]) {
			const state = this.syncInFlight.get(target)?.get(hash);
			if (state) {
				this.setSyncInFlightTargetKey(target, coordinate, state.timestamp);
			}
		}
		this.clearSyncProcessKey(hash);
	}

	refreshQueuedSyncCoordinateAliases(): void {
		if (
			this.syncInFlightQueuedCoordinates.size === 0 &&
			this.records.size < this.syncInFlightQueue.size
		) {
			// Defensive compatibility for callers/tests that seed the public queue
			// directly. Keep hydration bounded; internal writes register coordinates
			// when the key is first admitted.
			let inspected = 0;
			for (const key of this.syncInFlightQueue.keys()) {
				if (typeof key === "bigint") {
					this.syncInFlightQueuedCoordinates.add(key);
				}
				inspected += 1;
				if (inspected >= MAX_PENDING_SIMPLE_SYNC_ALIAS_REFRESH_PER_MESSAGE) {
					break;
				}
			}
		}
		if (this.syncInFlightQueuedCoordinates.size === 0) {
			this.syncInFlightQueuedCoordinateRefreshIterator = undefined;
			return;
		}
		this.syncInFlightQueuedCoordinateRefreshIterator ??=
			this.syncInFlightQueuedCoordinates.values();
		for (
			let refreshed = 0;
			refreshed < MAX_PENDING_SIMPLE_SYNC_ALIAS_REFRESH_PER_MESSAGE;
			refreshed += 1
		) {
			const next = this.syncInFlightQueuedCoordinateRefreshIterator.next();
			if (next.done) {
				this.syncInFlightQueuedCoordinateRefreshIterator = undefined;
				break;
			}
			this.refreshQueuedSyncCoordinateAlias(next.value);
		}
	}

	getQueuedSyncKeyForAdmission(
		key: SyncableKey,
	): SyncableKey | typeof QUEUED_SYNC_ALIAS_REFRESH_PENDING | undefined {
		const getValidQueuedKey = (
			candidate: SyncableKey,
		): SyncableKey | undefined => {
			if (!this.syncInFlightQueue.has(candidate)) {
				return undefined;
			}
			const expiresAt = this.syncInFlightQueueExpiresAt.get(candidate);
			if (expiresAt != null && expiresAt <= Date.now()) {
				this.clearSyncProcessKey(candidate);
				return undefined;
			}
			return candidate;
		};
		if (getValidQueuedKey(key) != null) {
			if (typeof key === "bigint") {
				this.refreshQueuedSyncCoordinateAlias(key);
				return getValidQueuedKey(key);
			}
			return key;
		}
		if (typeof key === "string") {
			const aliases = this.syncInFlightQueuedCoordinatesByHash.get(key);
			if (aliases) {
				let inspected = 0;
				for (const alias of aliases) {
					if (inspected >= MAX_PENDING_SIMPLE_SYNC_ALIAS_REFRESH_PER_MESSAGE) {
						return QUEUED_SYNC_ALIAS_REFRESH_PENDING;
					}
					inspected += 1;
					this.refreshQueuedSyncCoordinateAlias(alias);
					if (
						this.syncInFlightQueuedCoordinatesByHash.get(key)?.has(alias) ===
						true
					) {
						const validAlias = getValidQueuedKey(alias);
						if (validAlias != null) {
							return validAlias;
						}
					}
				}
				if (
					(this.syncInFlightQueuedCoordinatesByHash.get(key)?.size ?? 0) > 0
				) {
					return QUEUED_SYNC_ALIAS_REFRESH_PENDING;
				}
			}
			return undefined;
		}
		const hash = this.deps.coordinateToHash.get(key);
		return hash != null ? getValidQueuedKey(hash) : undefined;
	}

	addPendingSyncClaim(
		key: SyncableKey,
		from: PublicSignKey,
		expiresAt?: number,
	): boolean {
		const fromHash = from.hashcode();
		let peers = this.syncInFlightQueue.get(key);
		let record = this.records.get(key);
		if (!peers) {
			peers = [];
			this.syncInFlightQueue.set(key, peers);
			const deadline = expiresAt ?? Date.now() + PENDING_SIMPLE_SYNC_KEY_TTL_MS;
			this.syncInFlightQueueExpiresAt.set(key, deadline);
			record = {
				kind: "key",
				key,
				peers,
				claimants: new Set(),
				claimantIndexes: new Map(),
				rrCursor: 0,
				expiresAt: deadline,
				heapIndex: -1,
			};
			this.records.set(key, record);
			this.pushPendingSyncExpiry(record);
			if (typeof key === "bigint") {
				this.refreshQueuedSyncCoordinateAlias(key);
			}
		} else if (!record) {
			record = this.hydrateRecordFromQueue(key, peers);
		}
		if (record.claimants.has(fromHash)) {
			return false;
		}

		record.claimantIndexes.set(fromHash, peers.length);
		peers.push(from);
		record.claimants.add(fromHash);
		let inverted = this.syncInFlightQueueInverted.get(fromHash);
		if (!inverted) {
			inverted = new Set();
			this.syncInFlightQueueInverted.set(fromHash, inverted);
		}
		inverted.add(key);
		this.pendingSyncClaimCount += 1;
		this.schedulePendingSyncKeyExpiry();
		return true;
	}

	hasPendingSyncClaim(key: SyncableKey, peer: string): boolean {
		const record = this.records.get(key);
		if (record) {
			return record.claimants.has(peer);
		}
		const peers = this.syncInFlightQueue.get(key);
		if (!peers) {
			return false;
		}
		return this.hydrateRecordFromQueue(key, peers).claimants.has(peer);
	}

	filterDispatchablePendingSyncClaims(
		keys: SyncableKey[],
		peer: string,
		epoch: unknown,
	): SyncableKey[] {
		if (!this.deps.isDispatchEpochCurrent(peer, epoch)) {
			return [];
		}
		const now = Date.now();
		const dispatchable: SyncableKey[] = [];
		for (const key of keys) {
			const expiresAt = this.syncInFlightQueueExpiresAt.get(key);
			if (expiresAt != null && expiresAt <= now) {
				this.clearSyncProcessKey(key);
				continue;
			}
			if (
				this.syncInFlightQueue.has(key) &&
				this.hasPendingSyncClaim(key, peer)
			) {
				dispatchable.push(key);
			}
		}
		return dispatchable;
	}

	getRoundRobinCursor(key: SyncableKey): number {
		return this.records.get(key)?.rrCursor ?? 0;
	}

	setRoundRobinCursor(key: SyncableKey, cursor: number): void {
		const peers = this.syncInFlightQueue.get(key);
		if (!peers) {
			return;
		}
		const record =
			this.records.get(key) ?? this.hydrateRecordFromQueue(key, peers);
		record.rrCursor = cursor;
	}

	reservePendingSyncAdmission(
		peer: string,
		identities: SyncableKey[],
	): PendingSyncAdmissionReservation | undefined {
		const count = identities.length;
		if (count <= 0) {
			return undefined;
		}
		const reservation: PendingSyncAdmissionReservation = {
			peer,
			remaining: count,
			active: true,
			released: false,
			expiresAt: Date.now() + PENDING_SIMPLE_SYNC_KEY_TTL_MS,
			identities: new Set(identities),
			retainedSettled: 0,
		};
		this.pendingSyncAdmissionReservations.add(reservation);
		let peerReservations =
			this.pendingSyncAdmissionReservationsByPeer.get(peer);
		if (!peerReservations) {
			peerReservations = new Set();
			this.pendingSyncAdmissionReservationsByPeer.set(peer, peerReservations);
		}
		peerReservations.add(reservation);
		this.pendingSyncActiveAdmissionReservations += 1;
		this.pendingSyncAdmissionCount += count;
		this.pendingSyncAdmissionCountByPeer.set(
			peer,
			(this.pendingSyncAdmissionCountByPeer.get(peer) ?? 0) + count,
		);
		let reservedIdentities =
			this.pendingSyncAdmissionIdentitiesByPeer.get(peer);
		if (!reservedIdentities) {
			reservedIdentities = new Set();
			this.pendingSyncAdmissionIdentitiesByPeer.set(peer, reservedIdentities);
		}
		for (const identity of identities) {
			reservedIdentities.add(identity);
			let reservationsForIdentity =
				this.pendingSyncAdmissionReservationsByIdentity.get(identity);
			if (!reservationsForIdentity) {
				reservationsForIdentity = new Set();
				this.pendingSyncAdmissionReservationsByIdentity.set(
					identity,
					reservationsForIdentity,
				);
			}
			reservationsForIdentity.add(reservation);
		}
		const expiryNode: PendingSyncAdmissionExpiryNode = {
			kind: "admission",
			reservation,
			expiresAt: reservation.expiresAt,
			heapIndex: -1,
		};
		this.pendingSyncAdmissionExpiryNodes.set(reservation, expiryNode);
		this.pushPendingSyncExpiry(expiryNode);
		this.schedulePendingSyncKeyExpiry();
		return reservation;
	}

	private removePendingSyncAdmissionIdentity(
		reservation: PendingSyncAdmissionReservation,
		identity: SyncableKey,
		options?: { retainQuota?: boolean },
	): boolean {
		if (!reservation.identities.delete(identity)) {
			return false;
		}
		if (options?.retainQuota === true) {
			reservation.retainedSettled += 1;
		} else {
			reservation.remaining -= 1;
			this.pendingSyncAdmissionCount -= 1;
			const peerCount =
				(this.pendingSyncAdmissionCountByPeer.get(reservation.peer) ?? 0) - 1;
			if (peerCount === 0) {
				this.pendingSyncAdmissionCountByPeer.delete(reservation.peer);
			} else {
				this.pendingSyncAdmissionCountByPeer.set(reservation.peer, peerCount);
			}
		}
		const reservedIdentities = this.pendingSyncAdmissionIdentitiesByPeer.get(
			reservation.peer,
		);
		reservedIdentities?.delete(identity);
		if (reservedIdentities?.size === 0) {
			this.pendingSyncAdmissionIdentitiesByPeer.delete(reservation.peer);
		}
		const reservationsForIdentity =
			this.pendingSyncAdmissionReservationsByIdentity.get(identity);
		reservationsForIdentity?.delete(reservation);
		if (reservationsForIdentity?.size === 0) {
			this.pendingSyncAdmissionReservationsByIdentity.delete(identity);
		}
		return true;
	}

	clearPendingSyncAdmissionIdentity(identity: SyncableKey): void {
		const reservations =
			this.pendingSyncAdmissionReservationsByIdentity.get(identity);
		if (!reservations) {
			return;
		}
		for (const reservation of [...reservations]) {
			this.removePendingSyncAdmissionIdentity(reservation, identity, {
				retainQuota: true,
			});
		}
	}

	consumePendingSyncAdmission(
		reservation: PendingSyncAdmissionReservation,
		identity: SyncableKey,
	): "consumed" | "settled" | "invalid" {
		if (!reservation.active || reservation.expiresAt <= Date.now()) {
			if (reservation.expiresAt <= Date.now()) {
				this.invalidatePendingSyncAdmission(reservation);
			}
			return "invalid";
		}
		if (!reservation.identities.has(identity)) {
			return "settled";
		}
		this.removePendingSyncAdmissionIdentity(reservation, identity);
		if (reservation.remaining === 0) {
			this.removePendingSyncAdmissionExpiry(reservation);
			reservation.active = false;
			reservation.released = true;
			this.pendingSyncActiveAdmissionReservations -= 1;
			this.pendingSyncAdmissionReservations.delete(reservation);
			const peerReservations = this.pendingSyncAdmissionReservationsByPeer.get(
				reservation.peer,
			);
			peerReservations?.delete(reservation);
			if (peerReservations?.size === 0) {
				this.pendingSyncAdmissionReservationsByPeer.delete(reservation.peer);
			}
			this.clearPendingSyncExpiryTimerIfIdle();
		}
		return "consumed";
	}

	transferPendingSyncAdmissionIdentity(
		peer: string,
		identity: SyncableKey,
	): number | undefined {
		const reservations =
			this.pendingSyncAdmissionReservationsByIdentity.get(identity);
		if (!reservations) {
			return undefined;
		}
		for (const reservation of reservations) {
			if (
				reservation.peer !== peer ||
				reservation.released ||
				reservation.expiresAt <= Date.now() ||
				!this.removePendingSyncAdmissionIdentity(reservation, identity, {
					retainQuota: true,
				})
			) {
				continue;
			}
			// The original resolver may be non-abortable and still retains its input
			// arrays. Keep that reservation charged until its queueSync finally
			// settles; the queued claim is counted separately and can disappear
			// without returning the resolver's quota early.
			return reservation.expiresAt;
		}
		return undefined;
	}

	invalidatePendingSyncAdmission(
		reservation?: PendingSyncAdmissionReservation,
	): void {
		if (!reservation || reservation.released || !reservation.active) {
			return;
		}
		// Expiry/disconnect invalidates late lookup results, but it must not return
		// the quota slot while the underlying storage/index work is still alive.
		// Those lookups are not generally abortable; only queueSync's finally block
		// may release their active-work accounting.
		this.removePendingSyncAdmissionExpiry(reservation);
		reservation.active = false;
		this.pendingSyncActiveAdmissionReservations -= 1;
		this.clearPendingSyncExpiryTimerIfIdle();
	}

	releasePendingSyncAdmission(
		reservation?: PendingSyncAdmissionReservation,
	): void {
		if (!reservation || reservation.released) {
			return;
		}
		this.removePendingSyncAdmissionExpiry(reservation);
		for (const identity of [...reservation.identities]) {
			this.removePendingSyncAdmissionIdentity(reservation, identity);
		}
		if (reservation.retainedSettled > 0) {
			const retainedSettled = reservation.retainedSettled;
			reservation.retainedSettled = 0;
			reservation.remaining -= retainedSettled;
			this.pendingSyncAdmissionCount -= retainedSettled;
			const peerCount =
				(this.pendingSyncAdmissionCountByPeer.get(reservation.peer) ?? 0) -
				retainedSettled;
			if (peerCount === 0) {
				this.pendingSyncAdmissionCountByPeer.delete(reservation.peer);
			} else {
				this.pendingSyncAdmissionCountByPeer.set(reservation.peer, peerCount);
			}
		}
		if (reservation.active) {
			this.pendingSyncActiveAdmissionReservations -= 1;
		}
		reservation.active = false;
		reservation.released = true;
		this.pendingSyncAdmissionReservations.delete(reservation);
		const peerReservations = this.pendingSyncAdmissionReservationsByPeer.get(
			reservation.peer,
		);
		peerReservations?.delete(reservation);
		if (peerReservations?.size === 0) {
			this.pendingSyncAdmissionReservationsByPeer.delete(reservation.peer);
		}
		this.clearPendingSyncExpiryTimerIfIdle();
	}

	clearPendingSyncAdmissions(peer?: string): void {
		const reservations =
			peer == null
				? this.pendingSyncAdmissionReservations
				: this.pendingSyncAdmissionReservationsByPeer.get(peer);
		if (!reservations) {
			return;
		}
		for (const reservation of [...reservations]) {
			this.invalidatePendingSyncAdmission(reservation);
		}
	}

	private swapPendingSyncExpiry(left: number, right: number): void {
		const leftNode = this.pendingSyncExpiryHeap[left]!;
		const rightNode = this.pendingSyncExpiryHeap[right]!;
		this.pendingSyncExpiryHeap[left] = rightNode;
		this.pendingSyncExpiryHeap[right] = leftNode;
		rightNode.heapIndex = left;
		leftNode.heapIndex = right;
	}

	private pushPendingSyncExpiry(node: PendingSyncExpiryNode): void {
		node.heapIndex = this.pendingSyncExpiryHeap.length;
		this.pendingSyncExpiryHeap.push(node);
		let index = node.heapIndex;
		while (index > 0) {
			const parent = Math.floor((index - 1) / 2);
			if (
				this.pendingSyncExpiryHeap[parent]!.expiresAt <=
				this.pendingSyncExpiryHeap[index]!.expiresAt
			) {
				break;
			}
			this.swapPendingSyncExpiry(parent, index);
			index = parent;
		}
	}

	private removePendingSyncExpiry(node: PendingSyncExpiryNode): void {
		const index = node.heapIndex;
		if (
			index < 0 ||
			index >= this.pendingSyncExpiryHeap.length ||
			this.pendingSyncExpiryHeap[index] !== node
		) {
			return;
		}
		const last = this.pendingSyncExpiryHeap.pop()!;
		node.heapIndex = -1;
		if (index >= this.pendingSyncExpiryHeap.length) {
			return;
		}
		this.pendingSyncExpiryHeap[index] = last;
		last.heapIndex = index;

		let current = index;
		while (current > 0) {
			const parent = Math.floor((current - 1) / 2);
			if (
				this.pendingSyncExpiryHeap[parent]!.expiresAt <=
				this.pendingSyncExpiryHeap[current]!.expiresAt
			) {
				break;
			}
			this.swapPendingSyncExpiry(parent, current);
			current = parent;
		}
		for (;;) {
			const left = current * 2 + 1;
			const right = left + 1;
			let smallest = current;
			if (
				left < this.pendingSyncExpiryHeap.length &&
				this.pendingSyncExpiryHeap[left]!.expiresAt <
					this.pendingSyncExpiryHeap[smallest]!.expiresAt
			) {
				smallest = left;
			}
			if (
				right < this.pendingSyncExpiryHeap.length &&
				this.pendingSyncExpiryHeap[right]!.expiresAt <
					this.pendingSyncExpiryHeap[smallest]!.expiresAt
			) {
				smallest = right;
			}
			if (smallest === current) {
				break;
			}
			this.swapPendingSyncExpiry(current, smallest);
			current = smallest;
		}
	}

	movePendingSyncKeyExpiryEarlier(key: SyncableKey, expiresAt: number): void {
		const current = this.syncInFlightQueueExpiresAt.get(key);
		if (current == null || current <= expiresAt) {
			return;
		}
		this.syncInFlightQueueExpiresAt.set(key, expiresAt);
		const record = this.records.get(key);
		if (!record) {
			return;
		}
		this.removePendingSyncExpiry(record);
		record.expiresAt = expiresAt;
		this.pushPendingSyncExpiry(record);
		this.schedulePendingSyncKeyExpiry();
	}

	private removePendingSyncAdmissionExpiry(
		reservation: PendingSyncAdmissionReservation,
	): void {
		const node = this.pendingSyncAdmissionExpiryNodes.get(reservation);
		if (!node) {
			return;
		}
		this.pendingSyncAdmissionExpiryNodes.delete(reservation);
		this.removePendingSyncExpiry(node);
	}

	expirePendingSyncKeys(now = Date.now()): void {
		for (;;) {
			const node = this.pendingSyncExpiryHeap[0];
			if (!node || node.expiresAt > now) {
				break;
			}
			this.removePendingSyncExpiry(node);
			if (node.kind === "key") {
				if (this.records.get(node.key) !== node) {
					continue;
				}
				this.clearSyncProcessKey(node.key);
			} else {
				if (
					this.pendingSyncAdmissionExpiryNodes.get(node.reservation) !== node
				) {
					continue;
				}
				this.pendingSyncAdmissionExpiryNodes.delete(node.reservation);
				this.invalidatePendingSyncAdmission(node.reservation);
			}
		}
	}

	clearPendingSyncExpiryTimerIfIdle(): void {
		if (
			this.pendingSyncExpiryHeap.length === 0 &&
			this.syncInFlightQueueExpiryTimer != null
		) {
			clearTimeout(this.syncInFlightQueueExpiryTimer);
			this.syncInFlightQueueExpiryTimer = undefined;
		}
	}

	private schedulePendingSyncKeyExpiry(): void {
		if (
			this.syncInFlightQueueExpiryTimer != null ||
			this.pendingSyncExpiryHeap.length === 0
		) {
			return;
		}
		const expiresAt = this.pendingSyncExpiryHeap[0]!.expiresAt;
		this.syncInFlightQueueExpiryTimer = setTimeout(
			() => {
				this.syncInFlightQueueExpiryTimer = undefined;
				this.expirePendingSyncKeys();
				this.schedulePendingSyncKeyExpiry();
			},
			Math.max(0, expiresAt - Date.now()),
		);
		this.syncInFlightQueueExpiryTimer.unref?.();
	}

	hasSyncProcessState(): boolean {
		return (
			this.syncInFlightQueue.size > 0 ||
			this.syncInFlightQueueInverted.size > 0 ||
			this.pendingSyncAdmissionCount > 0 ||
			this.syncInFlight.size > 0
		);
	}

	clearSyncProcessKey(key: SyncableKey): void {
		const inflight = this.syncInFlightQueue.get(key);
		const record = this.records.get(key);
		if (inflight) {
			for (const peer of inflight) {
				const map = this.syncInFlightQueueInverted.get(peer.hashcode());
				if (map) {
					map.delete(key);
					if (map.size === 0) {
						this.syncInFlightQueueInverted.delete(peer.hashcode());
					}
				}
			}

			this.pendingSyncClaimCount = Math.max(
				0,
				this.pendingSyncClaimCount -
					(record?.claimants.size ?? inflight.length),
			);
			this.syncInFlightQueue.delete(key);
		}
		if (record) {
			this.records.delete(key);
			this.removePendingSyncExpiry(record);
		}
		if (typeof key === "bigint") {
			this.removeQueuedSyncCoordinateAlias(key);
		}
		this.syncInFlightQueueExpiresAt.delete(key);
		this.clearPendingSyncExpiryTimerIfIdle();

		this.clearSyncInFlightKey(key);
	}

	removePendingSyncClaim(key: SyncableKey, peer: string): void {
		const inflight = this.syncInFlightQueue.get(key);
		if (!inflight) {
			return;
		}
		const record =
			this.records.get(key) ?? this.hydrateRecordFromQueue(key, inflight);
		const index = record.claimantIndexes.get(peer);
		if (index == null) {
			return;
		}

		const lastIndex = inflight.length - 1;
		if (index !== lastIndex) {
			const lastClaimant = inflight[lastIndex]!;
			const lastClaimantHash = lastClaimant.hashcode();
			inflight[index] = lastClaimant;
			record.claimantIndexes.set(lastClaimantHash, index);
		}
		inflight.pop();
		record.claimantIndexes.delete(peer);
		record.claimants.delete(peer);
		this.pendingSyncClaimCount = Math.max(0, this.pendingSyncClaimCount - 1);
		const inverted = this.syncInFlightQueueInverted.get(peer);
		inverted?.delete(key);
		if (inverted?.size === 0) {
			this.syncInFlightQueueInverted.delete(peer);
		}
		if (inflight.length > 0) {
			const cursor = record.rrCursor;
			record.rrCursor =
				cursor === lastIndex
					? index % inflight.length
					: cursor % inflight.length;
			return;
		}

		this.syncInFlightQueue.delete(key);
		this.records.delete(key);
		if (typeof key === "bigint") {
			this.removeQueuedSyncCoordinateAlias(key);
		}
		this.removePendingSyncExpiry(record);
		this.syncInFlightQueueExpiresAt.delete(key);
		this.clearSyncInFlightKey(key);
		this.clearPendingSyncExpiryTimerIfIdle();
	}

	removeClaimsForPeer(publicKeyHash: string): void {
		const map = this.syncInFlightQueueInverted.get(publicKeyHash);
		if (map) {
			for (const hash of [...map]) {
				this.removePendingSyncClaim(hash, publicKeyHash);
			}
			this.syncInFlightQueueInverted.delete(publicKeyHash);
		}
	}

	removeSyncInFlightTargetKey(peer: string, key: SyncableKey): void {
		const map = this.syncInFlight.get(peer);
		if (!map?.delete(key)) {
			return;
		}
		if (map.size === 0) {
			this.syncInFlight.delete(peer);
		}
		const targets = this.syncInFlightTargetsByKey.get(key);
		targets?.delete(peer);
		if (targets?.size === 0) {
			this.syncInFlightTargetsByKey.delete(key);
		}
	}

	setSyncInFlightTargetKey(
		peer: string,
		key: SyncableKey,
		timestamp: number,
	): void {
		let map = this.syncInFlight.get(peer);
		if (!map) {
			map = new Map();
			this.syncInFlight.set(peer, map);
		}
		const existing = map.get(key);
		if (!existing || existing.timestamp < timestamp) {
			map.set(key, { timestamp });
		}
		let targets = this.syncInFlightTargetsByKey.get(key);
		if (!targets) {
			targets = new Set();
			this.syncInFlightTargetsByKey.set(key, targets);
		}
		targets.add(peer);
	}

	clearSyncInFlightTarget(peer: string): void {
		const map = this.syncInFlight.get(peer);
		if (!map) {
			return;
		}
		for (const key of map.keys()) {
			const targets = this.syncInFlightTargetsByKey.get(key);
			targets?.delete(peer);
			if (targets?.size === 0) {
				this.syncInFlightTargetsByKey.delete(key);
			}
		}
		this.syncInFlight.delete(peer);
	}

	private clearSyncInFlightKey(key: SyncableKey): void {
		const targets = this.syncInFlightTargetsByKey.get(key);
		if (!targets) {
			// Defensive compatibility for tests or integrations that seed the public
			// syncInFlight map directly. Internal writes always populate the index.
			for (const [peer, map] of this.syncInFlight) {
				if (map.has(key)) {
					this.removeSyncInFlightTargetKey(peer, key);
				}
			}
			return;
		}
		for (const peer of [...targets]) {
			this.removeSyncInFlightTargetKey(peer, key);
		}
	}

	private forEachKnownAlias(
		hash: string,
		callback: (key: SyncableKey) => void,
	): void {
		callback(hash);
		for (const coordinate of [
			...(this.syncInFlightQueuedCoordinatesByHash.get(hash) ?? []),
		]) {
			callback(coordinate);
		}
	}

	clearSyncInFlightForPeer(publicKeyHash: string, hash: string): void {
		const map = this.syncInFlight.get(publicKeyHash);
		if (!map) {
			return;
		}
		this.refreshQueuedSyncCoordinateAliases();
		this.forEachKnownAlias(hash, (key) =>
			this.removeSyncInFlightTargetKey(publicKeyHash, key),
		);
	}

	clearSyncInFlightForPeerHashes(
		publicKeyHash: string,
		hashes: string[],
	): void {
		const map = this.syncInFlight.get(publicKeyHash);
		if (!map || hashes.length === 0) {
			return;
		}
		this.refreshQueuedSyncCoordinateAliases();
		for (const hash of hashes) {
			this.forEachKnownAlias(hash, (key) =>
				this.removeSyncInFlightTargetKey(publicKeyHash, key),
			);
		}
	}

	clearSyncProcess(hash: string): void {
		this.refreshQueuedSyncCoordinateAliases();
		this.forEachKnownAlias(hash, (key) => this.clearSyncProcessKey(key));
	}

	clearSyncProcesses(hashes: string[]): void {
		if (hashes.length === 0) {
			return;
		}
		this.refreshQueuedSyncCoordinateAliases();
		const keys = new Set<SyncableKey>();
		for (const hash of hashes) {
			this.forEachKnownAlias(hash, (key) => keys.add(key));
		}
		for (const key of keys) {
			this.clearSyncProcessKey(key);
		}
	}

	clearForClose(): void {
		this.syncInFlightQueue.clear();
		this.syncInFlightQueueInverted.clear();
		this.syncInFlightQueueExpiresAt.clear();
		this.pendingSyncExpiryHeap.length = 0;
		this.records.clear();
		this.pendingSyncAdmissionExpiryNodes.clear();
		this.syncInFlightQueuedCoordinates.clear();
		this.syncInFlightQueuedHashByCoordinate.clear();
		this.syncInFlightQueuedCoordinatesByHash.clear();
		this.syncInFlightQueuedCoordinateRefreshIterator = undefined;
		this.pendingSyncClaimCount = 0;
		if (this.syncInFlightQueueExpiryTimer != null) {
			clearTimeout(this.syncInFlightQueueExpiryTimer);
			this.syncInFlightQueueExpiryTimer = undefined;
		}
		this.syncInFlight.clear();
		this.syncInFlightTargetsByKey.clear();
	}
}
