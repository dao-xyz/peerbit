import { deserialize, serialize } from "@dao-xyz/borsh";
import { createProxyFromService } from "@dao-xyz/borsh-rpc";
import {
	type CanonicalClient,
	createMessagePortTransport,
} from "@peerbit/canonical-client";
import type { PublicSignKey } from "@peerbit/crypto";
import {
	type CountOptions,
	IdKey,
	type IterateOptions,
	toQuery,
	toSort,
} from "@peerbit/indexer-interface";
import { Entry, NO_ENCODING } from "@peerbit/log";
import {
	type FixedReplicationOptions,
	type LogResultsIterator,
	type ReplicationOptions,
	type ReplicationRangeIndexable,
	ReplicationRangeIndexableU32,
	ReplicationRangeIndexableU64,
} from "@peerbit/shared-log";
import type { SharedLogLike } from "@peerbit/shared-log";
import {
	OpenSharedLogRequest,
	SharedLogBytes,
	SharedLogCoverageRequest,
	SharedLogEntriesBatch,
	SharedLogEntriesIteratorService,
	SharedLogEvent,
	SharedLogReplicateBool,
	SharedLogReplicateFactor,
	SharedLogReplicateFixed,
	SharedLogReplicateFixedList,
	SharedLogReplicateRequest,
	SharedLogReplicateValue,
	SharedLogReplicationBatch,
	SharedLogReplicationCountRequest,
	SharedLogReplicationIterateRequest,
	SharedLogReplicationIteratorService,
	SharedLogReplicationRange,
	SharedLogService,
	SharedLogUnreplicateRequest,
	SharedLogWaitForReplicatorRequest,
	SharedLogWaitForReplicatorsRequest,
} from "./protocol.js";

const ensureCustomEvent = () => {
	if (typeof (globalThis as any).CustomEvent === "function") {
		return;
	}

	class CustomEventPolyfill<T = any> extends Event {
		detail: T;
		constructor(type: string, params?: CustomEventInit<T>) {
			super(type, params);
			this.detail = params?.detail as T;
		}
	}

	(globalThis as any).CustomEvent = CustomEventPolyfill;
};

const toHex = (bytes: Uint8Array): string => {
	let out = "";
	for (const b of bytes) out += b.toString(16).padStart(2, "0");
	return out;
};

const toNumber = (value: bigint | number): number => {
	return typeof value === "bigint" ? Number(value) : value;
};

export type SharedLogProxyLog = {
	length: number;
	get: (hash: string) => Promise<Entry<any> | undefined>;
	has: (hash: string) => Promise<boolean>;
	getHeads: () => LogResultsIterator<Entry<any>>;
	toArray: () => Promise<Entry<any>[]>;
	blocks: {
		has: (hash: string) => Promise<boolean>;
	};
};

export type SharedLogProxyReplicationIndex = {
	iterate: (
		request?: IterateOptions,
	) => import("@peerbit/indexer-interface").IndexIterator<
		ReplicationRangeIndexable<any>,
		undefined
	>;
	count: (request?: CountOptions) => Promise<number>;
	getSize?: () => Promise<number>;
};

export type SharedLogProxy = SharedLogLike<any> & {
	raw: SharedLogService;
	log: SharedLogProxyLog;
	replicationIndex: SharedLogProxyReplicationIndex;
	node: { identity: { publicKey: PublicSignKey } };
	close: () => Promise<void>;
};

const decodeEntry = (bytes: SharedLogBytes): Entry<any> => {
	const entry = deserialize(bytes.value, Entry) as Entry<any>;
	return entry.init({ encoding: NO_ENCODING });
};

const toReplicationRange = (
	value: FixedReplicationOptions,
): SharedLogReplicationRange => {
	const factor = value.factor;
	if (factor === "all" || factor === "right") {
		return new SharedLogReplicationRange({
			id: value.id,
			factorMode: factor,
			offset: value.offset as number | undefined,
			normalized: value.normalized,
			strict: value.strict,
		});
	}
	if (typeof factor === "string") {
		throw new Error(`Unsupported replication factor '${factor}'`);
	}
	if (typeof factor === "bigint") {
		return new SharedLogReplicationRange({
			id: value.id,
			factor: Number(factor),
			offset: value.offset != null ? Number(value.offset) : undefined,
			normalized: value.normalized,
			strict: value.strict,
		});
	}
	return new SharedLogReplicationRange({
		id: value.id,
		factor: factor,
		offset: value.offset as number | undefined,
		normalized: value.normalized,
		strict: value.strict,
	});
};

const toReplicateValue = (
	input: ReplicationOptions<any> | undefined,
): SharedLogReplicateValue | undefined => {
	if (input === undefined) return undefined;
	if (typeof input === "boolean") {
		return new SharedLogReplicateBool(input);
	}
	if (typeof input === "number") {
		return new SharedLogReplicateFactor(input);
	}
	if (Array.isArray(input)) {
		return new SharedLogReplicateFixedList(
			input.map((range) =>
				toReplicationRange(range as FixedReplicationOptions),
			),
		);
	}
	if ((input as any)?.type === "resume") {
		return toReplicateValue((input as any).default as any);
	}
	const fixed = input as FixedReplicationOptions;
	if (fixed.factor == null && (fixed as any).factorMode == null) {
		throw new Error("Replication options missing factor");
	}
	return new SharedLogReplicateFixed(toReplicationRange(fixed));
};

export const createSharedLogProxyFromService = async (
	raw: SharedLogService,
): Promise<SharedLogProxy> => {
	ensureCustomEvent();

	const randomId = (): string => {
		if (
			typeof crypto !== "undefined" &&
			typeof crypto.randomUUID === "function"
		) {
			return crypto.randomUUID();
		}
		return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
	};

	const abortError = () => new Error("AbortError");

	const resolution = (await raw.resolution()) as "u32" | "u64";
	const decodeRange = (
		bytes: SharedLogBytes,
	): ReplicationRangeIndexable<any> => {
		if (resolution === "u32") {
			return deserialize(
				bytes.value,
				ReplicationRangeIndexableU32,
			) as ReplicationRangeIndexable<any>;
		}
		return deserialize(
			bytes.value,
			ReplicationRangeIndexableU64,
		) as ReplicationRangeIndexable<any>;
	};

	const publicKey = await raw.publicKey();
	const node = { identity: { publicKey } };

	let length = toNumber(await raw.logLength());
	let closed = false;

	const events = new EventTarget();
	const eventTypes = [
		"join",
		"leave",
		"replicator:join",
		"replicator:leave",
		"replicator:mature",
		"replication:change",
	];
	const eventHandlers: Array<() => void> = [];

	for (const type of eventTypes) {
		const handler = (event: any) => {
			const detail = (event?.detail as SharedLogEvent | undefined)?.publicKey;
			if (!detail) return;
			const payload =
				type === "join" || type === "leave" ? detail : { publicKey: detail };
			events.dispatchEvent(new CustomEvent(type, { detail: payload }));
		};
		raw.events.addEventListener(type, handler as any);
		eventHandlers.push(() =>
			raw.events.removeEventListener(type, handler as any),
		);
	}

	const createEntriesIterator = (
		service: SharedLogEntriesIteratorService,
	): LogResultsIterator<Entry<any>> => {
		let done = false;
		const iterator: LogResultsIterator<Entry<any>> = {
			next: async (amount: number) => {
				const batch = (await service.next(amount)) as SharedLogEntriesBatch;
				done = batch.done;
				return (batch.entries ?? []).map(decodeEntry);
			},
			done: () => done,
			all: async () => {
				const out: Entry<any>[] = [];
				while (!done) {
					const next = await iterator.next(10);
					out.push(...next);
				}
				return out;
			},
			close: async () => {
				done = true;
				await service.close();
			},
		};
		return iterator;
	};

	const log: SharedLogProxyLog = {
		get: async (hash) => {
			const bytes = await raw.logGet(hash);
			return bytes ? decodeEntry(bytes) : undefined;
		},
		has: async (hash) => {
			return raw.logHas(hash);
		},
		getHeads: () => {
			const servicePromise = raw.logGetHeads();
			let iterator: LogResultsIterator<Entry<any>>;
			iterator = {
				next: async (amount: number) => {
					const service = await servicePromise;
					Object.assign(iterator, createEntriesIterator(service));
					return iterator.next(amount);
				},
				done: () => false,
				all: async () => {
					const service = await servicePromise;
					Object.assign(iterator, createEntriesIterator(service));
					return iterator.all();
				},
				close: async () => {
					const service = await servicePromise;
					Object.assign(iterator, createEntriesIterator(service));
					return iterator.close();
				},
			};
			return iterator;
		},
		toArray: async () => {
			const items = await raw.logToArray();
			length = items.length;
			return items.map(decodeEntry);
		},
		blocks: {
			has: async (hash) => {
				return raw.logBlockHas(hash);
			},
		},
		get length() {
			return length;
		},
	};

	const createReplicationIterator = (
		service: SharedLogReplicationIteratorService,
	) => {
		let done = false;
		const iterator: import("@peerbit/indexer-interface").IndexIterator<
			ReplicationRangeIndexable<any>,
			undefined
		> = {
			next: async (amount: number) => {
				const batch = (await service.next(amount)) as SharedLogReplicationBatch;
				done = batch.done;
				return (batch.results ?? []).map((result) => ({
					id: result.id as IdKey,
					value: decodeRange(result.value),
				}));
			},
			done: () => done,
			all: async () => {
				const out: Array<{ id: IdKey; value: ReplicationRangeIndexable<any> }> =
					[];
				while (!done) {
					const next = await iterator.next(10);
					out.push(...next);
				}
				return out as any;
			},
			pending: async () => {
				const pending = await service.pending();
				return pending != null ? Number(pending) : 0;
			},
			close: async () => {
				done = true;
				await service.close();
			},
		};
		return iterator;
	};

	const replicationIndex: SharedLogProxyReplicationIndex = {
		iterate: (request?: IterateOptions) => {
			const query = toQuery(request?.query);
			const sort = toSort(request?.sort);
			const iterateRequest = new SharedLogReplicationIterateRequest({
				query,
				sort,
			});
			const servicePromise = raw.replicationIterate(iterateRequest);
			let iterator: import("@peerbit/indexer-interface").IndexIterator<
				ReplicationRangeIndexable<any>,
				undefined
			>;
			iterator = {
				next: async (amount: number) => {
					const service = await servicePromise;
					Object.assign(iterator, createReplicationIterator(service));
					return iterator.next(amount);
				},
				done: () => false,
				all: async () => {
					const service = await servicePromise;
					Object.assign(iterator, createReplicationIterator(service));
					return iterator.all();
				},
				pending: async () => {
					const service = await servicePromise;
					Object.assign(iterator, createReplicationIterator(service));
					return iterator.pending();
				},
				close: async () => {
					const service = await servicePromise;
					Object.assign(iterator, createReplicationIterator(service));
					return iterator.close();
				},
			};
			return iterator;
		},
		count: async (request?: CountOptions) => {
			const query = toQuery(request?.query);
			const count = await raw.replicationCount(
				new SharedLogReplicationCountRequest({ query }),
			);
			return toNumber(count);
		},
		getSize: async () => {
			const count = await raw.replicationCount(
				new SharedLogReplicationCountRequest({ query: [] }),
			);
			return toNumber(count);
		},
	};

	const getReplicators = async (): Promise<Set<string>> => {
		const replicators = await raw.getReplicators();
		return new Set(replicators);
	};

	const waitForReplicator = async (
		publicKey: PublicSignKey,
		options?: {
			eager?: boolean;
			roleAge?: number;
			timeout?: number;
			signal?: AbortSignal;
		},
	) => {
		if (options?.signal?.aborted) {
			throw new Error("AbortError");
		}
		const signal = options?.signal;
		const requestId = signal ? randomId() : undefined;
		const request = new SharedLogWaitForReplicatorRequest({
			publicKey,
			eager: options?.eager,
			timeoutMs: options?.timeout,
			roleAgeMs: options?.roleAge,
			requestId,
		});
		const call = raw.waitForReplicator(request);
		if (!signal || !requestId) {
			await call;
			return;
		}

		return new Promise<void>((resolve, reject) => {
			const cleanup = () => {
				signal.removeEventListener("abort", onAbort);
			};
			const onAbort = () => {
				cleanup();
				void raw.cancelWait(requestId).catch(() => {});
				reject(abortError());
			};
			if (signal.aborted) {
				onAbort();
				return;
			}
			signal.addEventListener("abort", onAbort, { once: true });
			call.then(
				() => {
					cleanup();
					resolve();
				},
				(error) => {
					cleanup();
					reject(error);
				},
			);
		});
	};

	const waitForReplicators = async (options?: {
		timeout?: number;
		roleAge?: number;
		coverageThreshold?: number;
		waitForNewPeers?: boolean;
		signal?: AbortSignal;
	}) => {
		if (options?.signal?.aborted) {
			throw new Error("AbortError");
		}
		const signal = options?.signal;
		const requestId = signal ? randomId() : undefined;
		const request = new SharedLogWaitForReplicatorsRequest({
			timeoutMs: options?.timeout,
			roleAgeMs: options?.roleAge,
			coverageThreshold: options?.coverageThreshold,
			waitForNewPeers: options?.waitForNewPeers,
			requestId,
		});
		const call = raw.waitForReplicators(request);
		if (!signal || !requestId) {
			await call;
			return;
		}

		return new Promise<void>((resolve, reject) => {
			const cleanup = () => {
				signal.removeEventListener("abort", onAbort);
			};
			const onAbort = () => {
				cleanup();
				void raw.cancelWait(requestId).catch(() => {});
				reject(abortError());
			};
			if (signal.aborted) {
				onAbort();
				return;
			}
			signal.addEventListener("abort", onAbort, { once: true });
			call.then(
				() => {
					cleanup();
					resolve();
				},
				(error) => {
					cleanup();
					reject(error);
				},
			);
		});
	};

	const replicate = async (
		input?: ReplicationOptions<any>,
		options?: {
			reset?: boolean;
			checkDuplicates?: boolean;
			rebalance?: boolean;
			mergeSegments?: boolean;
		},
	) => {
		const value = toReplicateValue(input);
		const request =
			value || options
				? new SharedLogReplicateRequest({
						value,
						reset: options?.reset,
						checkDuplicates: options?.checkDuplicates,
						rebalance: options?.rebalance,
						mergeSegments: options?.mergeSegments,
					})
				: undefined;
		await raw.replicate(request);
	};

	const unreplicate = async (ranges?: { id: Uint8Array }[]) => {
		const request = ranges
			? new SharedLogUnreplicateRequest({ ids: ranges.map((r) => r.id) })
			: undefined;
		await raw.unreplicate(request);
	};

	const calculateCoverage = async (options?: {
		start?: number | bigint;
		end?: number | bigint;
		roleAge?: number;
	}) => {
		const request = options
			? new SharedLogCoverageRequest({
					start: options.start != null ? Number(options.start) : undefined,
					end: options.end != null ? Number(options.end) : undefined,
					roleAgeMs: options.roleAge,
				})
			: undefined;
		return raw.calculateCoverage(request);
	};

	const getMyReplicationSegments = async () => {
		const ranges = await raw.getMyReplicationSegments();
		return ranges.map(decodeRange);
	};

	const getAllReplicationSegments = async () => {
		const ranges = await raw.getAllReplicationSegments();
		return ranges.map(decodeRange);
	};

	const close = async () => {
		if (closed) return;
		closed = true;
		for (const off of eventHandlers) off();
		await raw.close();
	};

	const proxy = {
		raw,
		node,
		events,
		log,
		replicationIndex,
		getReplicators,
		waitForReplicator,
		waitForReplicators,
		replicate,
		unreplicate,
		calculateCoverage,
		getMyReplicationSegments,
		getAllReplicationSegments,
		close,
	} as SharedLogProxy;

	Object.defineProperty(proxy, "closed", {
		get: () => closed,
		set: (value: boolean) => {
			closed = value;
		},
		enumerable: true,
	});

	return proxy;
};

export const openSharedLog = async (properties: {
	client: CanonicalClient;
	id: Uint8Array;
}): Promise<SharedLogProxy> => {
	const channel = await properties.client.openPort(
		"@peerbit/shared-log",
		serialize(new OpenSharedLogRequest({ id: properties.id })),
	);

	const transport = createMessagePortTransport(channel, {
		requestTimeoutMs: (method) => {
			if (method === "waitForReplicator" || method === "waitForReplicators") {
				return undefined;
			}
			return 30_000;
		},
	});
	const raw = createProxyFromService(
		SharedLogService,
		transport,
	) as unknown as SharedLogService;

	const proxy = await createSharedLogProxyFromService(raw);
	const rawClose = proxy.close.bind(proxy);
	proxy.close = async () => {
		try {
			await rawClose();
		} finally {
			channel.close?.();
		}
	};
	return proxy;
};

export const createSharedLogCacheKey = (
	logId?: Uint8Array,
): string | undefined => {
	if (!logId) return undefined;
	return toHex(logId);
};
