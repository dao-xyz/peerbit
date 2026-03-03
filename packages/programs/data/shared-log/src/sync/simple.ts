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
import { SilentDelivery } from "@peerbit/stream-interface";
import {
	EntryWithRefs,
	createExchangeHeadsMessages,
} from "../exchange-heads.js";
import { TransportMessage } from "../message.js";
import type { EntryReplicated } from "../ranges.js";
import type {
	RepairSession,
	RepairSessionMode,
	RepairSessionResult,
	SyncOptions,
	SyncableKey,
	Syncronizer,
} from "./index.js";

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

const getHashesFromSymbols = async (
	symbols: bigint[],
	entryIndex: Index<EntryReplicated<any>, any>,
	coordinateToHash: Cache<string>,
) => {
	let queries: IntegerCompare[] = [];
	let batchSize = 128; // TODO arg
	let results = new Set<string>();
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
	for (let i = 0; i < symbols.length; i++) {
		const fromCache = coordinateToHash.get(symbols[i]);
		if (fromCache) {
			results.add(fromCache);
			continue;
		}
		const matchQuery = new IntegerCompare({
			key: "hashNumber",
			compare: Compare.Equal,
			value: symbols[i],
		});

		queries.push(matchQuery);
		await handleBatch();
	}
	await handleBatch(true);

	return results;
};

const DEFAULT_CONVERGENT_REPAIR_TIMEOUT_MS = 30_000;
const DEFAULT_CONVERGENT_RETRY_INTERVALS_MS = [0, 1_000, 3_000, 7_000];
const DEFAULT_BEST_EFFORT_RETRY_INTERVALS_MS = [0];
const SESSION_POLL_INTERVAL_MS = 100;
const DEFAULT_MAX_HASHES_PER_MESSAGE = 1_024;
const DEFAULT_MAX_COORDINATES_PER_MESSAGE = 1_024;
const DEFAULT_MAX_CONVERGENT_TRACKED_HASHES = 4_096;

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
	private syncOptions?: SyncOptions<R>;
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
		sync?: SyncOptions<R>;
	}) {
		this.syncInFlightQueue = new Map();
		this.syncInFlightQueueInverted = new Map();
		this.syncInFlight = new Map();
		this.rpc = properties.rpc;
		this.log = properties.log;
		this.entryIndex = properties.entryIndex;
		this.coordinateToHash = properties.coordinateToHash;
		this.syncOptions = properties.sync;
		this.repairSessionCounter = 0;
		this.repairSessions = new Map();
	}

	private getPrioritizedHashes(
		entries: Map<string, EntryReplicated<R>>,
	): string[] {
		const priorityFn = this.syncOptions?.priority;
		if (!priorityFn) {
			return [...entries.keys()];
		}

		let index = 0;
		const scored: { hash: string; index: number; priority: number }[] = [];
		for (const [hash, entry] of entries) {
			const priorityValue = priorityFn(entry);
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
			for (const hash of [...state.unresolved]) {
				if (await this.log.has(hash)) {
					state.unresolved.delete(hash);
				}
			}
		}
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

	startRepairSession(properties: {
		entries: Map<string, EntryReplicated<R>>;
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
			mode === "convergent" && allHashes.length > this.maxConvergentTrackedHashes
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

	onMaybeMissingEntries(properties: {
		entries: Map<string, EntryReplicated<R>>;
		targets: string[];
	}): Promise<void> {
		const hashes = this.getPrioritizedHashes(properties.entries);
		const chunks = this.chunk(hashes, this.maxHashesPerMessage);
		return chunks.reduce(
			(promise, chunk) =>
				promise.then(() =>
					this.rpc.send(new RequestMaybeSync({ hashes: chunk }), {
						priority: 1,
						mode: new SilentDelivery({
							to: properties.targets,
							redundancy: 1,
						}),
					}),
				),
			Promise.resolve(),
		);
	}

	async onMessage(
		msg: TransportMessage,
		context: RequestContext,
	): Promise<boolean> {
		const from = context.from!;
		if (msg instanceof RequestMaybeSync) {
			await this.queueSync(msg.hashes, from);
			return true;
		} else if (msg instanceof ResponseMaybeSync) {
			// TODO perhaps send less messages to more receivers for performance reasons?
			// TODO wait for previous send to target before trying to send more?

			for await (const message of createExchangeHeadsMessages(
				this.log,
				msg.hashes,
			)) {
				await this.rpc.send(message, {
					mode: new SilentDelivery({ to: [context.from!], redundancy: 1 }),
				});
			}
			return true;
		} else if (msg instanceof RequestMaybeSyncCoordinate) {
			const hashes = await getHashesFromSymbols(
				msg.hashNumbers,
				this.entryIndex,
				this.coordinateToHash,
			);
			for await (const message of createExchangeHeadsMessages(
				this.log,
				hashes,
			)) {
				await this.rpc.send(message, {
					mode: new SilentDelivery({ to: [context.from!], redundancy: 1 }),
					// dont set priority 1 here because this will block other messages that should higher priority
				});
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
		const resolvedHashes: string[] = [];
		for (const entry of properties.entries) {
			resolvedHashes.push(entry.entry.hash);
			const set = this.syncInFlight.get(properties.from.hashcode());
			if (set) {
				set.delete(entry.entry.hash);
				if (set?.size === 0) {
					this.syncInFlight.delete(properties.from.hashcode());
				}
			}
		}
		this.markRepairSessionResolvedHashes(resolvedHashes);
	}

	async queueSync(
		keys: string[] | bigint[],
		from: PublicSignKey,
		options?: { skipCheck?: boolean },
	) {
		const requestHashes: SyncableKey[] = [];

		for (const coordinateOrHash of keys) {
			const inFlight = this.syncInFlightQueue.get(coordinateOrHash);
			if (inFlight) {
				if (!inFlight.find((x) => x.hashcode() === from.hashcode())) {
					inFlight.push(from);
					let inverted = this.syncInFlightQueueInverted.get(from.hashcode());
					if (!inverted) {
						inverted = new Set();
						this.syncInFlightQueueInverted.set(from.hashcode(), inverted);
					}
					inverted.add(coordinateOrHash);
				}
			} else if (
				options?.skipCheck ||
				!(await this.checkHasCoordinateOrHash(coordinateOrHash))
			) {
				this.syncInFlightQueue.set(coordinateOrHash, []);
				requestHashes.push(coordinateOrHash); // request immediately (first time we have seen this hash)
			}
		}

		requestHashes.length > 0 &&
			(await this.requestSync(requestHashes as string[] | bigint[], [
				from!.hashcode(),
			]));
	}

	private async requestSync(
		hashes: string[] | bigint[],
		to: Set<string> | string[],
	) {
		if (hashes.length === 0) {
			return;
		}

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

		const isBigInt = typeof hashes[0] === "bigint";
		if (isBigInt) {
			const chunks = this.chunk(
				hashes as bigint[],
				this.maxCoordinatesPerMessage,
			);
			for (const chunk of chunks) {
				await this.rpc.send(
					new RequestMaybeSyncCoordinate({ hashNumbers: chunk }),
					{
						mode: new SilentDelivery({ to, redundancy: 1 }),
						priority: 1,
					},
				);
			}
		} else {
			const chunks = this.chunk(hashes as string[], this.maxHashesPerMessage);
			for (const chunk of chunks) {
				await this.rpc.send(new ResponseMaybeSync({ hashes: chunk }), {
					mode: new SilentDelivery({ to, redundancy: 1 }),
					priority: 1,
				});
			}
		}
	}
	private async checkHasCoordinateOrHash(key: string | bigint) {
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
			for (const [key, value] of this.syncInFlightQueue) {
				if (this.closed) {
					return;
				}

				const has = await this.checkHasCoordinateOrHash(key);

				if (!has) {
					// TODO test that this if statement actually does anymeaningfull
					if (value.length > 0) {
						requestHashes.push(key);
						const publicKeyHash = value.shift()!.hashcode();
						from.add(publicKeyHash);
						const invertedSet =
							this.syncInFlightQueueInverted.get(publicKeyHash);
						if (invertedSet) {
							if (invertedSet.delete(key)) {
								if (invertedSet.size === 0) {
									this.syncInFlightQueueInverted.delete(publicKeyHash);
								}
							}
						}
					}
					if (value.length === 0) {
						this.syncInFlightQueue.delete(key); // no-one more to ask for this entry
					}
				} else {
					this.syncInFlightQueue.delete(key);
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
			this.requestSync(requestHashes as string[] | bigint[], from).finally(
				() => {
					if (this.closed) {
						return;
					}
					this.syncMoreInterval = setTimeout(requestSyncLoop, 3e3);
				},
			);
		};

		requestSyncLoop();
	}

	async close() {
		this.closed = true;
		this.syncInFlightQueue.clear();
		this.syncInFlightQueueInverted.clear();
		this.syncInFlight.clear();
		for (const sessionId of [...this.repairSessions.keys()]) {
			this.finalizeRepairSession(sessionId, false);
		}
		clearTimeout(this.syncMoreInterval);
	}
	onEntryAdded(entry: Entry<any>): void {
		this.clearSyncProcess(entry.hash);
		this.markRepairSessionResolvedHashes([entry.hash]);
	}

	onEntryRemoved(hash: string): void {
		return this.clearSyncProcess(hash);
	}

	private clearSyncProcess(hash: string) {
		const inflight = this.syncInFlightQueue.get(hash);
		if (inflight) {
			for (const key of inflight) {
				const map = this.syncInFlightQueueInverted.get(key.hashcode());
				if (map) {
					map.delete(hash);
					if (map.size === 0) {
						this.syncInFlightQueueInverted.delete(key.hashcode());
					}
				}
			}

			this.syncInFlightQueue.delete(hash);
		}
	}

	onPeerDisconnected(key: PublicSignKey | string): Promise<void> | void {
		const publicKeyHash = typeof key === "string" ? key : key.hashcode();
		return this.clearSyncProcessPublicKeyHash(publicKeyHash);
	}
	private clearSyncProcessPublicKeyHash(publicKeyHash: string) {
		this.syncInFlight.delete(publicKeyHash);
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
