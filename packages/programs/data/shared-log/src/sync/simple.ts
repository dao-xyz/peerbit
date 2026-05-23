import { field, variant, vec } from "@dao-xyz/borsh";
import { Cache } from "@peerbit/cache";
import type { PublicSignKey } from "@peerbit/crypto";
import {
	Compare,
	type Index,
	IntegerCompare,
	Or,
} from "@peerbit/indexer-interface";
import { Entry, Log } from "@peerbit/log";
import type { RPC, RequestContext } from "@peerbit/rpc";
import {
	CONVERGENCE_MESSAGE_PRIORITY,
	SilentDelivery,
} from "@peerbit/stream-interface";
import {
	EntryWithRefs,
	createExchangeHeadsMessages,
	createRawExchangeHeadsMessages,
} from "../exchange-heads.js";
import { TransportMessage } from "../message.js";
import type { EntryReplicated } from "../ranges.js";
import type {
	HashSymbolResolver,
	HashSymbolHashListResolver,
	RepairSession,
	RepairSessionMode,
	RepairSessionResult,
	SyncEntryCoordinates,
	SyncOptions,
	SyncableKey,
	Syncronizer,
} from "./index.js";
import { emitSyncProfileDuration, syncProfileStart } from "./profile.js";

@variant([0, 1])
export class RequestMaybeSync extends TransportMessage {
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

@variant([0, 2])
export class ResponseMaybeSync extends TransportMessage {
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

@variant([0, 5])
export class RequestMaybeSyncCoordinate extends TransportMessage {
	@field({ type: vec("u64") })
	hashNumbers: bigint[];

	constructor(props: { hashNumbers: bigint[] }) {
		super();
		this.hashNumbers = props.hashNumbers;
	}
}

@variant([0, 6])
export class ConfirmEntriesMessage extends TransportMessage {
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

export const SIMPLE_SYNC_RAW_EXCHANGE_HEADS_CAPABILITY = 1;

@variant([0, 8])
export class ResponseMaybeSyncCapabilities extends TransportMessage {
	@field({ type: vec("string") })
	hashes: string[];

	@field({ type: "u32" })
	capabilities: number;

	constructor(props: { hashes: string[]; capabilities?: number }) {
		super();
		this.hashes = props.hashes;
		this.capabilities =
			props.capabilities ?? SIMPLE_SYNC_RAW_EXCHANGE_HEADS_CAPABILITY;
	}
}

@variant([0, 9])
export class RequestMaybeSyncCoordinateCapabilities extends TransportMessage {
	@field({ type: vec("u64") })
	hashNumbers: bigint[];

	@field({ type: "u32" })
	capabilities: number;

	constructor(props: { hashNumbers: bigint[]; capabilities?: number }) {
		super();
		this.hashNumbers = props.hashNumbers;
		this.capabilities =
			props.capabilities ?? SIMPLE_SYNC_RAW_EXCHANGE_HEADS_CAPABILITY;
	}
}

const canReceiveRawExchangeHeads = (
	message:
		| ResponseMaybeSync
		| ResponseMaybeSyncCapabilities
		| RequestMaybeSyncCoordinate
		| RequestMaybeSyncCoordinateCapabilities,
) =>
	(message instanceof ResponseMaybeSyncCapabilities ||
		message instanceof RequestMaybeSyncCoordinateCapabilities) &&
	(message.capabilities & SIMPLE_SYNC_RAW_EXCHANGE_HEADS_CAPABILITY) !== 0;

type KnownSyncKeys = {
	keys: Set<SyncableKey>;
	checkedCoordinates: boolean;
	checkedHashes: boolean;
};

const getHashesFromSymbols = async (
	symbols: bigint[],
	entryIndex: Index<EntryReplicated<any>, any>,
	coordinateToHash: Cache<string>,
	resolveHashesForSymbols?: HashSymbolResolver,
	resolveHashListForSymbols?: HashSymbolHashListResolver,
): Promise<Set<string> | string[]> => {
	let queries: IntegerCompare[] = [];
	let batchSize = 128; // TODO arg
	let results = new Set<string>();
	let missingSymbols: bigint[] = [];
	const addMissingUnlessCached = (symbol: bigint) => {
		const fromCache = coordinateToHash.get(symbol);
		if (fromCache) {
			results.add(fromCache);
			return;
		}
		missingSymbols.push(symbol);
	};
	const handleBatch = async (end = false) => {
		if (queries.length >= batchSize || (end && queries.length > 0)) {
			const entries = await entryIndex
				.iterate(
					{ query: queries.length > 1 ? new Or(queries) : queries },
					{ shape: { hash: true, hashNumber: true } },
				)
				.all();
			queries = [];

			for (const entry of entries) {
				results.add(entry.value.hash);
				coordinateToHash.add(entry.value.hashNumber, entry.value.hash);
			}
		}
	};

	if (resolveHashListForSymbols) {
		const resolvedHashes = await resolveHashListForSymbols(symbols);
		if (resolvedHashes) {
			const resolvedHashList = Array.isArray(resolvedHashes)
				? resolvedHashes
				: [...resolvedHashes];
			let mergedHashes: Set<string> | undefined;
			for (const symbol of symbols) {
				const fromCache = coordinateToHash.get(symbol);
				if (fromCache) {
					mergedHashes ??= new Set(resolvedHashList);
					mergedHashes.add(fromCache);
				}
			}
			return mergedHashes ?? resolvedHashList;
		}
	}

	if (resolveHashesForSymbols) {
		const resolved = await resolveHashesForSymbols(symbols);
		if (resolved) {
			for (const symbol of symbols) {
				const hashes = resolved.get(symbol);
				if (!hashes) {
					addMissingUnlessCached(symbol);
					continue;
				}
				let singleHash: string | undefined;
				let count = 0;
				for (const hash of hashes) {
					results.add(hash);
					singleHash = hash;
					count += 1;
				}
				if (count === 0) {
					addMissingUnlessCached(symbol);
				} else if (count === 1) {
					coordinateToHash.add(symbol, singleHash!);
				}
			}
		} else {
			for (const symbol of symbols) {
				addMissingUnlessCached(symbol);
			}
		}
	} else {
		for (const symbol of symbols) {
			addMissingUnlessCached(symbol);
		}
	}

	for (const symbol of missingSymbols) {
		const matchQuery = new IntegerCompare({
			key: "hashNumber",
			compare: Compare.Equal,
			value: symbol,
		});

		queries.push(matchQuery);
		await handleBatch();
	}
	await handleBatch(true);

	return results;
};

const hashLookupResultSize = (hashes: Set<string> | string[]) =>
	Array.isArray(hashes) ? hashes.length : hashes.size;

const DEFAULT_CONVERGENT_REPAIR_TIMEOUT_MS = 30_000;
const DEFAULT_CONVERGENT_RETRY_INTERVALS_MS = [0, 1_000, 3_000, 7_000];
const DEFAULT_BEST_EFFORT_RETRY_INTERVALS_MS = [0];
const SESSION_POLL_INTERVAL_MS = 100;
const DEFAULT_MAX_HASHES_PER_MESSAGE = 1_024;
const DEFAULT_MAX_COORDINATES_PER_MESSAGE = 1_024;
const DEFAULT_MAX_CONVERGENT_TRACKED_HASHES = 4_096;
// Keep convergence sync above the default/background lane. Dropping it to the
// background priority lets repair traffic starve behind foreground work.
export const SYNC_MESSAGE_PRIORITY = CONVERGENCE_MESSAGE_PRIORITY;
// Retry missing entry requests when the first response was lost (for example, due to
// pubsub stream warmup). Keep it coarse-grained so we do not hammer the network under
// large historical backfills.
const SIMPLE_SYNC_RETRY_AFTER_MS = 10_000;
const EXCHANGE_HEAD_RESPONSE_DEDUPE_TTL_MS = SIMPLE_SYNC_RETRY_AFTER_MS - 1_000;
const RECENT_KNOWN_EXCHANGE_HEAD_SUPPRESSION_MS = 30_000;

const createDeferred = <T>() => {
	let resolve!: (value: T | PromiseLike<T>) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<T>((res, rej) => {
		resolve = res;
		reject = rej;
	});
	return { promise, resolve, reject };
};

type RepairSessionTargetState = {
	unresolved: Set<string>;
	requestedCount: number;
	requestedTotalCount: number;
	attempts: number;
};

type RepairSessionState = {
	id: string;
	mode: RepairSessionMode;
	startedAt: number;
	timeoutMs: number;
	retryIntervalsMs: number[];
	targets: Map<string, RepairSessionTargetState>;
	truncated: boolean;
	deferred: ReturnType<typeof createDeferred<RepairSessionResult[]>>;
	cancelled: boolean;
	timer?: ReturnType<typeof setTimeout>;
};

export class SimpleSyncronizer<R extends "u32" | "u64">
	implements Syncronizer<R>
{
	// map of hash to public keys that we can ask for entries
	syncInFlightQueue: Map<SyncableKey, PublicSignKey[]>;
	syncInFlightQueueInverted: Map<string, Set<SyncableKey>>;

	// map of hash to public keys that we have asked for entries
	syncInFlight!: Map<string, Map<SyncableKey, { timestamp: number }>>;

	rpc: RPC<TransportMessage, TransportMessage>;
	log: Log<any>;
	entryIndex: Index<EntryReplicated<R>, any>;
	coordinateToHash: Cache<string>;
	private resolveHashesForSymbols?: HashSymbolResolver;
	private resolveHashListForSymbols?: HashSymbolHashListResolver;
	private syncOptions?: SyncOptions<R>;
	private isEntryRecentlyKnownByPeer?: (
		hash: string,
		peer: string,
		maxAgeMs: number,
	) => boolean;
	private recentlySentExchangeHeads: Map<string, Map<string, number>>;
	private repairSessionCounter: number;
	private repairSessions: Map<string, RepairSessionState>;

	// Syncing and dedeplucation work
	syncMoreInterval?: ReturnType<typeof setTimeout>;

	closed!: boolean;

	constructor(properties: {
		rpc: RPC<TransportMessage, TransportMessage>;
		entryIndex: Index<EntryReplicated<R>, any>;
		log: Log<any>;
		coordinateToHash: Cache<string>;
		resolveHashesForSymbols?: HashSymbolResolver;
		resolveHashListForSymbols?: HashSymbolHashListResolver;
		sync?: SyncOptions<R>;
		isEntryRecentlyKnownByPeer?: (
			hash: string,
			peer: string,
			maxAgeMs: number,
		) => boolean;
	}) {
		this.syncInFlightQueue = new Map();
		this.syncInFlightQueueInverted = new Map();
		this.syncInFlight = new Map();
		this.rpc = properties.rpc;
		this.log = properties.log;
		this.entryIndex = properties.entryIndex;
		this.coordinateToHash = properties.coordinateToHash;
		this.resolveHashesForSymbols = properties.resolveHashesForSymbols;
		this.resolveHashListForSymbols = properties.resolveHashListForSymbols;
		this.syncOptions = properties.sync;
		this.isEntryRecentlyKnownByPeer = properties.isEntryRecentlyKnownByPeer;
		this.recentlySentExchangeHeads = new Map();
		this.repairSessionCounter = 0;
		this.repairSessions = new Map();
	}

	private getPrioritizedHashes(
		entries: Map<string, SyncEntryCoordinates<R>>,
	): string[] {
		const priorityFn = this.syncOptions?.priority;
		if (!priorityFn) {
			return [...entries.keys()];
		}

		let index = 0;
		const scored: { hash: string; index: number; priority: number }[] = [];
		for (const [hash, entry] of entries) {
			const priorityValue = priorityFn(entry as EntryReplicated<R>);
			scored.push({
				hash,
				index,
				priority: Number.isFinite(priorityValue) ? priorityValue : 0,
			});
			index += 1;
		}
		scored.sort((a, b) => b.priority - a.priority || a.index - b.index);
		return scored.map((x) => x.hash);
	}

	private normalizeRetryIntervals(
		mode: RepairSessionMode,
		retryIntervalsMs?: number[],
	): number[] {
		const defaults =
			mode === "convergent"
				? DEFAULT_CONVERGENT_RETRY_INTERVALS_MS
				: DEFAULT_BEST_EFFORT_RETRY_INTERVALS_MS;
		if (!retryIntervalsMs || retryIntervalsMs.length === 0) {
			return [...defaults];
		}

		return [...retryIntervalsMs]
			.map((x) => Math.max(0, Math.floor(x)))
			.filter((x, i, arr) => arr.indexOf(x) === i)
			.sort((a, b) => a - b);
	}

	private get maxHashesPerMessage() {
		const value = this.syncOptions?.maxSimpleHashesPerMessage;
		return value && Number.isFinite(value) && value > 0
			? Math.floor(value)
			: DEFAULT_MAX_HASHES_PER_MESSAGE;
	}

	private get maxCoordinatesPerMessage() {
		const value = this.syncOptions?.maxSimpleCoordinatesPerMessage;
		return value && Number.isFinite(value) && value > 0
			? Math.floor(value)
			: DEFAULT_MAX_COORDINATES_PER_MESSAGE;
	}

	private get maxConvergentTrackedHashes() {
		const value = this.syncOptions?.maxConvergentTrackedHashes;
		return value && Number.isFinite(value) && value > 0
			? Math.floor(value)
			: DEFAULT_MAX_CONVERGENT_TRACKED_HASHES;
	}

	private chunk<T>(values: T[], size: number): T[][] {
		if (values.length === 0) {
			return [];
		}
		const out: T[][] = [];
		for (let i = 0; i < values.length; i += size) {
			out.push(values.slice(i, i + size));
		}
		return out;
	}

	private filterRecentlySentExchangeHeads(
		hashes: Iterable<string>,
		peer: PublicSignKey,
	): string[] {
		const peerHash = peer.hashcode();
		const now = Date.now();
		let recentlySent = this.recentlySentExchangeHeads.get(peerHash);
		if (!recentlySent) {
			recentlySent = new Map();
			this.recentlySentExchangeHeads.set(peerHash, recentlySent);
		}
		for (const [hash, timestamp] of recentlySent) {
			if (now - timestamp > EXCHANGE_HEAD_RESPONSE_DEDUPE_TTL_MS) {
				recentlySent.delete(hash);
			}
		}
		const out: string[] = [];
		const seen = new Set<string>();
		for (const hash of hashes) {
			if (seen.has(hash)) {
				continue;
			}
			seen.add(hash);
			if (recentlySent.has(hash)) {
				continue;
			}
			if (
				this.isEntryRecentlyKnownByPeer?.(
					hash,
					peerHash,
					RECENT_KNOWN_EXCHANGE_HEAD_SUPPRESSION_MS,
				)
			) {
				continue;
			}
			recentlySent.set(hash, now);
			out.push(hash);
		}
		return out;
	}

	private isRepairSessionComplete(session: RepairSessionState): boolean {
		for (const state of session.targets.values()) {
			if (state.unresolved.size > 0) {
				return false;
			}
		}
		return true;
	}

	private buildRepairSessionResult(
		session: RepairSessionState,
		completed: boolean,
	): RepairSessionResult[] {
		const durationMs = Date.now() - session.startedAt;
		const out: RepairSessionResult[] = [];
		for (const [target, state] of session.targets) {
			const unresolved = [...state.unresolved];
			out.push({
				target,
				requested: state.requestedCount,
				resolved: state.requestedCount - unresolved.length,
				unresolved,
				attempts: state.attempts,
				durationMs,
				completed,
				requestedTotal: state.requestedTotalCount,
				truncated: session.truncated,
			});
		}
		return out;
	}

	private finalizeRepairSession(sessionId: string, completed: boolean): void {
		const session = this.repairSessions.get(sessionId);
		if (!session) {
			return;
		}
		this.repairSessions.delete(sessionId);
		session.cancelled = true;
		if (session.timer) {
			clearTimeout(session.timer);
		}
		session.deferred.resolve(this.buildRepairSessionResult(session, completed));
	}

	private async refreshRepairSessionState(sessionId: string): Promise<void> {
		const session = this.repairSessions.get(sessionId);
		if (!session) {
			return;
		}
		for (const state of session.targets.values()) {
			const resolved =
				typeof this.log.hasMany === "function"
					? await this.log.hasMany(state.unresolved)
					: await this.getExistingRepairHashes(state.unresolved);
			for (const hash of resolved) {
				state.unresolved.delete(hash);
			}
		}
	}

	private async getExistingRepairHashes(
		hashes: Iterable<string>,
	): Promise<Set<string>> {
		const resolved = new Set<string>();
		for (const hash of hashes) {
			if (await this.log.has(hash)) {
				resolved.add(hash);
			}
		}
		return resolved;
	}

	private markRepairSessionResolvedHashes(hashes: string[]): void {
		if (hashes.length === 0 || this.repairSessions.size === 0) {
			return;
		}
		for (const [sessionId, session] of this.repairSessions) {
			for (const state of session.targets.values()) {
				for (const hash of hashes) {
					state.unresolved.delete(hash);
				}
			}
			if (this.isRepairSessionComplete(session)) {
				this.finalizeRepairSession(sessionId, true);
			}
		}
	}

	private markRepairSessionResolvedHash(hash: string): void {
		if (this.repairSessions.size === 0) {
			return;
		}
		for (const [sessionId, session] of this.repairSessions) {
			for (const state of session.targets.values()) {
				state.unresolved.delete(hash);
			}
			if (this.isRepairSessionComplete(session)) {
				this.finalizeRepairSession(sessionId, true);
			}
		}
	}

	private async runRepairSession(sessionId: string): Promise<void> {
		const session = this.repairSessions.get(sessionId);
		if (!session) {
			return;
		}

		let previousDelay = 0;
		for (const delayMs of session.retryIntervalsMs) {
			if (!this.repairSessions.has(sessionId) || this.closed) {
				return;
			}

			const waitMs = Math.max(0, delayMs - previousDelay);
			previousDelay = delayMs;
			if (waitMs > 0) {
				await new Promise<void>((resolve) => {
					const timer = setTimeout(resolve, waitMs);
					timer.unref?.();
				});
			}
			if (!this.repairSessions.has(sessionId) || this.closed) {
				return;
			}

			await this.refreshRepairSessionState(sessionId);
			const current = this.repairSessions.get(sessionId);
			if (!current) {
				return;
			}
			if (this.isRepairSessionComplete(current)) {
				this.finalizeRepairSession(sessionId, true);
				return;
			}

			for (const [target, state] of current.targets) {
				if (state.unresolved.size === 0) {
					continue;
				}
				state.attempts += 1;
				try {
					await this.requestSync([...state.unresolved], [target]);
				} catch {
					// Best-effort: keep unresolved and let retries/timeout determine outcome.
				}
			}

			await this.refreshRepairSessionState(sessionId);
			const afterSend = this.repairSessions.get(sessionId);
			if (!afterSend) {
				return;
			}
			if (this.isRepairSessionComplete(afterSend)) {
				this.finalizeRepairSession(sessionId, true);
				return;
			}

			if (afterSend.mode === "best-effort") {
				this.finalizeRepairSession(sessionId, false);
				return;
			}
		}

		for (;;) {
			if (!this.repairSessions.has(sessionId) || this.closed) {
				return;
			}
			await this.refreshRepairSessionState(sessionId);
			const current = this.repairSessions.get(sessionId);
			if (!current) {
				return;
			}
			if (this.isRepairSessionComplete(current)) {
				this.finalizeRepairSession(sessionId, true);
				return;
			}
			await new Promise<void>((resolve) => {
				const timer = setTimeout(resolve, SESSION_POLL_INTERVAL_MS);
				timer.unref?.();
			});
		}
	}

	private getQueuedSyncKey(key: SyncableKey): SyncableKey | undefined {
		if (this.syncInFlightQueue.has(key)) {
			return key;
		}
		if (typeof key === "string") {
			for (const queuedKey of this.syncInFlightQueue.keys()) {
				if (
					typeof queuedKey === "bigint" &&
					this.coordinateToHash.get(queuedKey) === key
				) {
					return queuedKey;
				}
			}
			return undefined;
		}
		const hash = this.coordinateToHash.get(key);
		return hash && this.syncInFlightQueue.has(hash) ? hash : undefined;
	}

	startRepairSession(properties: {
		entries: Map<string, SyncEntryCoordinates<R>>;
		targets: string[];
		mode?: RepairSessionMode;
		timeoutMs?: number;
		retryIntervalsMs?: number[];
	}): RepairSession {
		const mode = properties.mode ?? "best-effort";
		const startedAt = Date.now();
		const timeoutMs = Math.max(
			1,
			Math.floor(
				properties.timeoutMs ??
					(mode === "convergent"
						? DEFAULT_CONVERGENT_REPAIR_TIMEOUT_MS
						: DEFAULT_CONVERGENT_REPAIR_TIMEOUT_MS),
			),
		);
		const retryIntervalsMs = this.normalizeRetryIntervals(
			mode,
			properties.retryIntervalsMs,
		);
		const allHashes = this.getPrioritizedHashes(properties.entries);
		const trackedHashes =
			mode === "convergent" &&
			allHashes.length > this.maxConvergentTrackedHashes
				? allHashes.slice(0, this.maxConvergentTrackedHashes)
				: allHashes;
		const truncated = trackedHashes.length < allHashes.length;
		const targets = [...new Set(properties.targets)];
		const id = `repair-${++this.repairSessionCounter}`;
		const deferred = createDeferred<RepairSessionResult[]>();

		const targetStates = new Map<string, RepairSessionTargetState>();
		for (const target of targets) {
			targetStates.set(target, {
				unresolved: new Set(trackedHashes),
				requestedCount: trackedHashes.length,
				requestedTotalCount: allHashes.length,
				attempts: 0,
			});
		}

		const session: RepairSessionState = {
			id,
			mode,
			startedAt,
			timeoutMs,
			retryIntervalsMs,
			targets: targetStates,
			truncated,
			deferred,
			cancelled: false,
		};

		if (allHashes.length === 0 || targets.length === 0) {
			deferred.resolve(this.buildRepairSessionResult(session, true));
			return {
				id,
				done: deferred.promise,
				cancel: () => {
					// no-op
				},
			};
		}

		// For capped convergent sessions, still dispatch the full set once so large
		// repairs are not limited to tracked hashes.
		if (mode === "convergent" && truncated) {
			void this.onMaybeMissingEntries({
				entries: properties.entries,
				targets,
			}).catch(() => {
				// Best-effort: retries on tracked hashes continue via runRepairSession.
			});
		}

		session.timer = setTimeout(() => {
			this.finalizeRepairSession(id, false);
		}, timeoutMs);
		session.timer.unref?.();

		this.repairSessions.set(id, session);
		void this.runRepairSession(id).catch(() => {
			this.finalizeRepairSession(id, false);
		});

		return {
			id,
			done: deferred.promise,
			cancel: () => {
				this.finalizeRepairSession(id, false);
			},
		};
	}

	async onMaybeMissingEntries(properties: {
		entries: Map<string, SyncEntryCoordinates<R>>;
		targets: string[];
	}): Promise<void> {
		await this.onMaybeMissingHashes({
			hashes: this.getPrioritizedHashes(properties.entries),
			targets: properties.targets,
		});
	}

	async onMaybeMissingHashes(properties: {
		hashes: Iterable<string>;
		targets: string[];
	}): Promise<void> {
		const profile = this.syncOptions?.profile;
		const startedAt = syncProfileStart(profile);
		const hashes = [...properties.hashes];
		const chunks = this.chunk(hashes, this.maxHashesPerMessage);
		try {
			await chunks.reduce(
				(promise, chunk) =>
					promise.then(() =>
						this.rpc.send(new RequestMaybeSync({ hashes: chunk }), {
							priority: SYNC_MESSAGE_PRIORITY,
							mode: new SilentDelivery({
								to: properties.targets,
								redundancy: 1,
							}),
						}),
					),
				Promise.resolve(),
			);
		} finally {
			if (profile) {
				emitSyncProfileDuration(profile, startedAt, {
					name: "simple.onMaybeMissingEntries",
					entries: hashes.length,
					messages: chunks.length,
					targets: properties.targets.length,
				});
			}
		}
	}

	async onMessage(
		msg: TransportMessage,
		context: RequestContext,
	): Promise<boolean> {
		const from = context.from!;
		if (msg instanceof RequestMaybeSync) {
			await this.queueSync(msg.hashes, from);
			return true;
		} else if (
			msg instanceof ResponseMaybeSync ||
			msg instanceof ResponseMaybeSyncCapabilities
		) {
			// TODO perhaps send less messages to more receivers for performance reasons?
			// TODO wait for previous send to target before trying to send more?

			const profile = this.syncOptions?.profile;
			const startedAt = syncProfileStart(profile);
			const hashes = this.filterRecentlySentExchangeHeads(msg.hashes, from);
			let messages = 0;
			const createMessages = canReceiveRawExchangeHeads(msg)
				? createRawExchangeHeadsMessages
				: createExchangeHeadsMessages;
			try {
				for await (const message of createMessages(this.log, hashes)) {
					messages += 1;
					await this.rpc.send(message, {
						mode: new SilentDelivery({ to: [context.from!], redundancy: 1 }),
					});
				}
			} finally {
				if (profile) {
					emitSyncProfileDuration(profile, startedAt, {
						name: "simple.exchangeHeads",
						entries: hashes.length,
						messages,
						targets: 1,
						details: { source: "responseMaybeSync" },
					});
				}
			}
			return true;
		} else if (
			msg instanceof RequestMaybeSyncCoordinate ||
			msg instanceof RequestMaybeSyncCoordinateCapabilities
		) {
			const profile = this.syncOptions?.profile;
			const lookupStartedAt = syncProfileStart(profile);
			const hashes = await getHashesFromSymbols(
				msg.hashNumbers,
				this.entryIndex,
				this.coordinateToHash,
				this.resolveHashesForSymbols,
				this.resolveHashListForSymbols,
			);
			if (profile) {
				emitSyncProfileDuration(profile, lookupStartedAt, {
					name: "simple.coordinateLookup",
					entries: hashLookupResultSize(hashes),
					symbols: msg.hashNumbers.length,
				});
			}

			const exchangeStartedAt = syncProfileStart(profile);
			const hashesToSend = this.filterRecentlySentExchangeHeads(hashes, from);
			let messages = 0;
			const createMessages = canReceiveRawExchangeHeads(msg)
				? createRawExchangeHeadsMessages
				: createExchangeHeadsMessages;
			try {
				for await (const message of createMessages(this.log, hashesToSend)) {
					messages += 1;
					await this.rpc.send(message, {
						mode: new SilentDelivery({ to: [context.from!], redundancy: 1 }),
						// dont set priority 1 here because this will block other messages that should higher priority
					});
				}
			} finally {
				if (profile) {
					emitSyncProfileDuration(profile, exchangeStartedAt, {
						name: "simple.exchangeHeads",
						entries: hashesToSend.length,
						messages,
						targets: 1,
						details: { source: "requestMaybeSyncCoordinate" },
					});
				}
			}

			return true;
		} else {
			return false; // no message was consumed
		}
	}

	onReceivedEntries(properties: {
		entries: EntryWithRefs<any>[];
		from: PublicSignKey;
	}): Promise<void> | void {
		return this.onReceivedEntryHashes({
			hashes: properties.entries.map((entry) => entry.entry.hash),
			from: properties.from,
		});
	}

	onReceivedEntryHashes(properties: {
		hashes: string[];
		from: PublicSignKey;
	}): Promise<void> | void {
		this.clearSyncInFlightForPeerHashes(
			properties.from.hashcode(),
			properties.hashes,
		);
		this.markRepairSessionResolvedHashes(properties.hashes);
	}

	async queueSync(
		keys: SyncableKey[],
		from: PublicSignKey,
		options?: { skipCheck?: boolean },
	) {
		const requestHashes: SyncableKey[] = [];
		const profile = this.syncOptions?.profile;
		const startedAt = syncProfileStart(profile);
		const resolveKnownStartedAt = syncProfileStart(profile);
		const knownKeys =
			options?.skipCheck === true
				? undefined
				: await this.resolveKnownSyncKeys(keys);
		if (profile) {
			emitSyncProfileDuration(profile, resolveKnownStartedAt, {
				name: "simple.queueSync.resolveKnown",
				entries: keys.length,
				count: knownKeys?.keys.size ?? 0,
				details: {
					checkedCoordinates: knownKeys?.checkedCoordinates === true,
					checkedHashes: knownKeys?.checkedHashes === true,
					skipCheck: options?.skipCheck === true,
				},
			});
		}
		const fromHash = from.hashcode();
		let queuedHashAliases: Map<string, SyncableKey> | undefined;
		const getQueuedSyncKeyForBatch = (key: SyncableKey) => {
			if (this.syncInFlightQueue.has(key)) {
				return key;
			}
			if (typeof key === "string") {
				if (!queuedHashAliases) {
					queuedHashAliases = new Map();
					for (const queuedKey of this.syncInFlightQueue.keys()) {
						if (typeof queuedKey !== "bigint") {
							continue;
						}
						const hash = this.coordinateToHash.get(queuedKey);
						if (hash) {
							queuedHashAliases.set(hash, queuedKey);
						}
					}
				}
				return queuedHashAliases.get(key);
			}
			const hash = this.coordinateToHash.get(key);
			return hash && this.syncInFlightQueue.has(hash) ? hash : undefined;
		};

		try {
			const loopStartedAt = syncProfileStart(profile);
			for (const key of keys) {
				const coordinateOrHash = getQueuedSyncKeyForBatch(key) ?? key;
				const inFlight = this.syncInFlightQueue.get(coordinateOrHash);
				if (inFlight) {
					if (!inFlight.find((x) => x.hashcode() === fromHash)) {
						inFlight.push(from);
						let inverted = this.syncInFlightQueueInverted.get(fromHash);
						if (!inverted) {
							inverted = new Set();
							this.syncInFlightQueueInverted.set(fromHash, inverted);
						}
						inverted.add(coordinateOrHash);
					}
				} else if (
					options?.skipCheck ||
					!(await this.checkHasCoordinateOrHash(
						coordinateOrHash,
						knownKeys,
					))
				) {
					// Track the initial sender so we can retry if the first request is lost.
					this.syncInFlightQueue.set(coordinateOrHash, [from]);
					let inverted = this.syncInFlightQueueInverted.get(fromHash);
					if (!inverted) {
						inverted = new Set();
						this.syncInFlightQueueInverted.set(fromHash, inverted);
					}
					inverted.add(coordinateOrHash);
					requestHashes.push(coordinateOrHash); // request immediately (first time we have seen this hash)
					if (
						queuedHashAliases &&
						typeof coordinateOrHash === "bigint"
					) {
						const hash = this.coordinateToHash.get(coordinateOrHash);
						if (hash) {
							queuedHashAliases.set(hash, coordinateOrHash);
						}
					}
				}
			}
			if (profile) {
				emitSyncProfileDuration(profile, loopStartedAt, {
					name: "simple.queueSync.plan",
					entries: keys.length,
					count: requestHashes.length,
					targets: 1,
				});
			}

			requestHashes.length > 0 &&
				(await this.requestSync(requestHashes, [fromHash]));
		} finally {
			if (profile) {
				emitSyncProfileDuration(profile, startedAt, {
					name: "simple.queueSync",
					entries: keys.length,
					targets: 1,
					details: {
						requested: requestHashes.length,
						skipCheck: options?.skipCheck === true,
					},
				});
			}
		}
	}

	private async requestSync(hashes: SyncableKey[], to: Set<string> | string[]) {
		if (hashes.length === 0) {
			return;
		}
		const profile = this.syncOptions?.profile;
		const startedAt = syncProfileStart(profile);
		let coordinateMessages = 0;
		let stringMessages = 0;
		let coordinateHashCount = 0;
		let stringHashCount = 0;

		try {
			const now = +new Date();
			for (const node of to) {
				let map = this.syncInFlight.get(node);
				if (!map) {
					map = new Map();
					this.syncInFlight.set(node, map);
				}
				for (const hash of hashes) {
					map.set(hash, { timestamp: now });
				}
			}

			const coordinateHashes: bigint[] = [];
			const stringHashes: string[] = [];
			for (const hash of hashes) {
				if (typeof hash === "bigint") {
					coordinateHashes.push(hash);
				} else {
					stringHashes.push(hash);
				}
			}
			coordinateHashCount = coordinateHashes.length;
			stringHashCount = stringHashes.length;

				if (coordinateHashes.length > 0) {
					const chunks = this.chunk(
						coordinateHashes,
						this.maxCoordinatesPerMessage,
					);
					coordinateMessages = chunks.length;
					for (const chunk of chunks) {
						await this.rpc.send(
							this.syncOptions?.rawExchangeHeads
								? new RequestMaybeSyncCoordinateCapabilities({
										hashNumbers: chunk,
									})
								: new RequestMaybeSyncCoordinate({ hashNumbers: chunk }),
							{
								mode: new SilentDelivery({ to, redundancy: 1 }),
								priority: SYNC_MESSAGE_PRIORITY,
							},
						);
					}
				}
				if (stringHashes.length > 0) {
					const chunks = this.chunk(stringHashes, this.maxHashesPerMessage);
					stringMessages = chunks.length;
					for (const chunk of chunks) {
						await this.rpc.send(
							this.syncOptions?.rawExchangeHeads
								? new ResponseMaybeSyncCapabilities({ hashes: chunk })
								: new ResponseMaybeSync({ hashes: chunk }),
							{
								mode: new SilentDelivery({ to, redundancy: 1 }),
								priority: SYNC_MESSAGE_PRIORITY,
							},
						);
					}
				}
		} finally {
			if (profile) {
				emitSyncProfileDuration(profile, startedAt, {
					name: "simple.requestSync",
					entries: hashes.length,
					messages: coordinateMessages + stringMessages,
					targets: Array.isArray(to) ? to.length : to.size,
					details: {
						coordinateHashes: coordinateHashCount,
						stringHashes: stringHashCount,
					},
				});
			}
		}
	}
	private async resolveKnownSyncKeys(
		keys: SyncableKey[],
	): Promise<KnownSyncKeys | undefined> {
		const hashes: string[] = [];
		const coordinates: bigint[] = [];
		for (const key of keys) {
			if (typeof key === "bigint") {
				coordinates.push(key);
			} else {
				hashes.push(key);
			}
		}
		const known: KnownSyncKeys = {
			keys: new Set(),
			checkedCoordinates: false,
			checkedHashes: false,
		};
		if (hashes.length > 0) {
			for (const hash of await this.log.hasMany(hashes)) {
				known.keys.add(hash);
			}
			known.checkedHashes = true;
		}
		if (coordinates.length > 0 && this.resolveHashesForSymbols) {
			const resolved = await this.resolveHashesForSymbols(coordinates);
			if (resolved) {
				for (const coordinate of coordinates) {
					const hashes = resolved.get(coordinate);
					if (!hashes) {
						continue;
					}
					for (const _hash of hashes) {
						known.keys.add(coordinate);
						break;
					}
				}
				known.checkedCoordinates = true;
			}
		}
		return known.checkedCoordinates || known.checkedHashes ? known : undefined;
	}

	private async checkHasCoordinateOrHash(
		key: string | bigint,
		knownKeys?: KnownSyncKeys,
	) {
		if (knownKeys) {
			if (knownKeys.keys.has(key)) {
				return true;
			}
			if (typeof key === "bigint" && knownKeys.checkedCoordinates) {
				return false;
			}
			if (typeof key === "string" && knownKeys.checkedHashes) {
				return false;
			}
		}
		return typeof key === "bigint"
			? (await this.entryIndex.count({ query: { hashNumber: key } })) > 0
			: this.log.has(key);
	}
	async open() {
		this.closed = false;
		const requestSyncLoop = async () => {
			/**
			 * This method fetches entries that we potentially want.
			 * In a case in which we become replicator of a segment,
			 * multiple remote peers might want to send us entries
			 * This method makes sure that we only request on entry from the remotes at a time
			 * so we don't get flooded with the same entry
			 */

			const requestHashes: SyncableKey[] = [];
			const from: Set<string> = new Set();
			const now = Date.now();
			for (const [key, value] of this.syncInFlightQueue) {
				if (this.closed) {
					return;
				}

				const has = await this.checkHasCoordinateOrHash(key);

				if (!has) {
					if (value.length === 0) {
						// No remaining peers to ask; drop the pending key to avoid leaking.
						this.clearSyncProcessKey(key);
						continue;
					}

					// Ask one peer per key per loop. If a previous request is still considered
					// "recent", wait until the retry window elapses.
					const candidate = value[0]!;
					const publicKeyHash = candidate.hashcode();
					const inflightTimestamp = this.syncInFlight
						.get(publicKeyHash)
						?.get(key)?.timestamp;
					if (
						inflightTimestamp == null ||
						now - inflightTimestamp >= SIMPLE_SYNC_RETRY_AFTER_MS
					) {
						requestHashes.push(key);
						from.add(publicKeyHash);
						if (value.length > 1) {
							// Rotate for fairness across multiple possible sources.
							value.push(value.shift()!);
						}
					}
				} else {
					this.clearSyncProcessKey(key);
				}
			}

			const nowMin10s = +new Date() - 2e4;
			for (const [key, map] of this.syncInFlight) {
				// cleanup "old" missing syncs
				for (const [hash, { timestamp }] of map) {
					if (timestamp < nowMin10s) {
						map.delete(hash);
					}
				}
				if (map.size === 0) {
					this.syncInFlight.delete(key);
				}
			}
			this.requestSync(requestHashes, from).finally(() => {
				if (this.closed) {
					return;
				}
				this.syncMoreInterval = setTimeout(requestSyncLoop, 3e3);
			});
		};

		requestSyncLoop();
	}

	async close() {
		this.closed = true;
		this.syncInFlightQueue.clear();
		this.syncInFlightQueueInverted.clear();
		this.syncInFlight.clear();
		this.recentlySentExchangeHeads.clear();
		for (const sessionId of [...this.repairSessions.keys()]) {
			this.finalizeRepairSession(sessionId, false);
		}
		clearTimeout(this.syncMoreInterval);
	}
	onEntryAdded(entry: Entry<any>): void {
		this.onEntryAddedHash(entry.hash);
	}

	onEntryAddedHashes(hashes: string[]): void {
		this.clearSyncProcesses(hashes);
		this.markRepairSessionResolvedHashes(hashes);
	}

	onEntryAddedHash(hash: string): void {
		this.clearSyncProcess(hash);
		this.markRepairSessionResolvedHash(hash);
	}

	onEntryRemoved(hash: string): void {
		return this.clearSyncProcess(hash);
	}

	onEntryRemovedHashes(hashes: string[]): void {
		return this.clearSyncProcesses(hashes);
	}

	private clearSyncProcessKey(key: SyncableKey) {
		const inflight = this.syncInFlightQueue.get(key);
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

			this.syncInFlightQueue.delete(key);
		}

		this.clearSyncInFlightKey(key);
	}

	private clearSyncInFlightKey(key: SyncableKey) {
		for (const [peer, map] of this.syncInFlight) {
			map.delete(key);
			if (map.size === 0) {
				this.syncInFlight.delete(peer);
			}
		}
	}

	private forEachKnownAlias(
		hash: string,
		callback: (key: SyncableKey) => void,
	): void {
		callback(hash);
		if (this.syncInFlightQueue.size === 0 && this.syncInFlight.size === 0) {
			return;
		}
		for (const key of this.syncInFlightQueue.keys()) {
			if (typeof key === "bigint" && this.coordinateToHash.get(key) === hash) {
				callback(key);
			}
		}
		for (const map of this.syncInFlight.values()) {
			for (const key of map.keys()) {
				if (
					typeof key === "bigint" &&
					this.coordinateToHash.get(key) === hash
				) {
					callback(key);
				}
			}
		}
	}

	private clearSyncInFlightForPeer(publicKeyHash: string, hash: string) {
		const map = this.syncInFlight.get(publicKeyHash);
		if (!map) {
			return;
		}
		this.forEachKnownAlias(hash, (key) => map.delete(key));
		if (map.size === 0) {
			this.syncInFlight.delete(publicKeyHash);
		}
	}

	private clearSyncInFlightForPeerHashes(
		publicKeyHash: string,
		hashes: string[],
	) {
		const map = this.syncInFlight.get(publicKeyHash);
		if (!map || hashes.length === 0) {
			return;
		}
		const keys = new Set<SyncableKey>(hashes);
		const hashSet = new Set(hashes);
		for (const key of map.keys()) {
			if (typeof key !== "bigint") {
				continue;
			}
			const hash = this.coordinateToHash.get(key);
			if (hash != null && hashSet.has(hash)) {
				keys.add(key);
			}
		}
		for (const key of keys) {
			map.delete(key);
		}
		if (map.size === 0) {
			this.syncInFlight.delete(publicKeyHash);
		}
	}

	private clearSyncProcess(hash: string) {
		this.forEachKnownAlias(hash, (key) => this.clearSyncProcessKey(key));
	}

	private clearSyncProcesses(hashes: string[]) {
		if (hashes.length === 0) {
			return;
		}
		const keys = new Set<SyncableKey>(hashes);
		const hashSet = new Set(hashes);
		const maybeAddAlias = (key: SyncableKey) => {
			if (typeof key !== "bigint") {
				return;
			}
			const hash = this.coordinateToHash.get(key);
			if (hash != null && hashSet.has(hash)) {
				keys.add(key);
			}
		};
		for (const key of this.syncInFlightQueue.keys()) {
			maybeAddAlias(key);
		}
		for (const map of this.syncInFlight.values()) {
			for (const key of map.keys()) {
				maybeAddAlias(key);
			}
		}
		for (const key of keys) {
			this.clearSyncProcessKey(key);
		}
	}

	onPeerDisconnected(key: PublicSignKey | string): Promise<void> | void {
		const publicKeyHash = typeof key === "string" ? key : key.hashcode();
		return this.clearSyncProcessPublicKeyHash(publicKeyHash);
	}
	private clearSyncProcessPublicKeyHash(publicKeyHash: string) {
		this.syncInFlight.delete(publicKeyHash);
		this.recentlySentExchangeHeads.delete(publicKeyHash);
		const map = this.syncInFlightQueueInverted.get(publicKeyHash);
		if (map) {
			for (const hash of map) {
				const arr = this.syncInFlightQueue.get(hash);
				if (arr) {
					const filtered = arr.filter((x) => x.hashcode() !== publicKeyHash);
					if (filtered.length > 0) {
						this.syncInFlightQueue.set(hash, filtered);
					} else {
						this.syncInFlightQueue.delete(hash);
					}
				}
			}
			this.syncInFlightQueueInverted.delete(publicKeyHash);
		}
	}

	get pending() {
		return this.syncInFlightQueue.size;
	}
}
