import { deserialize, serialize } from "@dao-xyz/borsh";
import { type RpcTransport, bindService } from "@dao-xyz/borsh-rpc";
import {
	type CanonicalChannel,
	type CanonicalContext,
	type CanonicalModule,
	createMessagePortTransport,
} from "@peerbit/canonical-host";
import type { PublicSignKey } from "@peerbit/crypto";
import type { Query } from "@peerbit/indexer-interface";
import {
	type FixedReplicationOptions,
	type ReplicationOptions,
	SharedLog,
	type SharedLogEvents,
} from "@peerbit/shared-log";
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
	SharedLogReplicationIndexResult,
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

const openLogs: Map<string, { program: SharedLog<any>; refs: number }> =
	new Map();

export type SharedLogModuleStats = {
	total: number;
	entries: Array<{ key: string; refs: number }>;
};

export const getSharedLogModuleStats = (): SharedLogModuleStats => {
	return {
		total: openLogs.size,
		entries: [...openLogs.entries()].map(([key, value]) => ({
			key,
			refs: value.refs,
		})),
	};
};

const acquireSharedLog = async (properties: {
	ctx: CanonicalContext;
	id: Uint8Array;
}): Promise<{ program: SharedLog<any>; release: () => Promise<void> }> => {
	const key = toHex(properties.id);
	const existing = openLogs.get(key);
	if (existing) {
		existing.refs += 1;
		return {
			program: existing.program,
			release: async () => releaseSharedLog(key),
		};
	}

	const peer = await properties.ctx.peer();
	const program = await peer.open(new SharedLog({ id: properties.id }), {
		existing: "reuse",
		args: { replicate: { factor: 1 } } as any,
	});
	openLogs.set(key, { program, refs: 1 });
	return { program, release: async () => releaseSharedLog(key) };
};

const releaseSharedLog = async (key: string): Promise<void> => {
	const existing = openLogs.get(key);
	if (!existing) return;
	existing.refs -= 1;
	if (existing.refs > 0) return;
	openLogs.delete(key);
	await existing.program.close();
};

const toFixedReplicationOptions = (
	range: SharedLogReplicationRange,
): FixedReplicationOptions => {
	const factor =
		range.factorMode ?? (range.factor != null ? range.factor : undefined);
	if (factor == null) {
		throw new Error("Replication range missing factor");
	}

	const options: FixedReplicationOptions = {
		factor: factor as any,
	};
	if (range.id) options.id = range.id;
	if (range.offset != null) options.offset = range.offset;
	if (range.normalized != null) options.normalized = range.normalized;
	if (range.strict != null) options.strict = range.strict;
	return options;
};

const toReplicationOptions = (
	value?: SharedLogReplicateValue,
): ReplicationOptions<any> | undefined => {
	if (!value) return undefined;
	if (value instanceof SharedLogReplicateBool) {
		return value.value;
	}
	if (value instanceof SharedLogReplicateFactor) {
		return value.factor;
	}
	if (value instanceof SharedLogReplicateFixed) {
		return toFixedReplicationOptions(value.range);
	}
	if (value instanceof SharedLogReplicateFixedList) {
		return value.ranges.map((range) => toFixedReplicationOptions(range));
	}
	throw new Error("Unsupported replication value");
};

export const createSharedLogService = (
	log: SharedLog<any>,
	options?: { onClose?: () => Promise<void> | void },
): SharedLogService => {
	ensureCustomEvent();

	let closed = false;
	const unsubscribe: Array<() => void> = [];
	const waitControllers = new Map<string, AbortController>();
	let service: SharedLogService;

	service = new SharedLogService({
		logGet: async (hash) => {
			const entry = await log.log.get(hash);
			return entry
				? new SharedLogBytes({ value: serialize(entry as any) })
				: undefined;
		},
		logHas: async (hash) => {
			return log.log.has(hash);
		},
		logToArray: async () => {
			const entries = await log.log.toArray();
			return entries.map(
				(entry: any) => new SharedLogBytes({ value: serialize(entry as any) }),
			);
		},
		logGetHeads: async () => {
			let iterator: any;
			let done = false;

			const closeIterator = async () => {
				if (done) return;
				done = true;
				if (iterator) {
					await iterator.close();
				}
			};

			const updates = new SharedLogEntriesIteratorService({
				next: async (amount) => {
					if (!iterator) {
						throw new Error("Shared log iterator not ready");
					}
					const items = await iterator.next(amount);
					return new SharedLogEntriesBatch({
						entries: (items ?? []).map(
							(entry: any) =>
								new SharedLogBytes({ value: serialize(entry as any) }),
						),
						done: iterator.done() ?? false,
					});
				},
				pending: async () => undefined,
				done: async () => {
					if (!iterator) return false;
					return iterator.done() ?? false;
				},
				close: async () => {
					await closeIterator();
				},
			});

			iterator = log.log.getHeads(true);
			return updates;
		},
		logLength: async () => {
			return BigInt(log.log.length);
		},
		logBlockHas: async (hash) => {
			return log.log.blocks.has(hash);
		},
		replicationIterate: async (request: SharedLogReplicationIterateRequest) => {
			const query = request.query.length
				? (request.query as Query[])
				: undefined;
			const sort = request.sort.length ? request.sort : undefined;
			let iterator: any;
			let done = false;

			const closeIterator = async () => {
				if (done) return;
				done = true;
				if (iterator) {
					await iterator.close();
				}
			};

			const updates = new SharedLogReplicationIteratorService({
				next: async (amount) => {
					if (!iterator) {
						throw new Error("Replication iterator not ready");
					}
					const items = await iterator.next(amount);
					const results = (items ?? []).map(
						(item: any) =>
							new SharedLogReplicationIndexResult({
								id: item.id,
								value: new SharedLogBytes({
									value: serialize(item.value as any),
								}),
							}),
					);
					return new SharedLogReplicationBatch({
						results,
						done: iterator.done() ?? false,
					});
				},
				pending: async () => {
					if (!iterator) return undefined;
					const pending = await iterator.pending();
					return pending != null ? BigInt(pending) : undefined;
				},
				done: async () => {
					if (!iterator) return false;
					return iterator.done() ?? false;
				},
				close: async () => {
					await closeIterator();
				},
			});

			iterator = log.replicationIndex.iterate(
				query || sort ? { query, sort } : undefined,
			);
			return updates;
		},
		replicationCount: async (request: SharedLogReplicationCountRequest) => {
			const query = request.query.length ? request.query : undefined;
			const count = await log.replicationIndex.count(
				query ? { query } : undefined,
			);
			return BigInt(count);
		},
		getReplicators: async () => {
			const replicators = await log.getReplicators();
			return [...replicators];
		},
		waitForReplicator: async (request: SharedLogWaitForReplicatorRequest) => {
			const requestId = request.requestId;
			const controller = requestId ? new AbortController() : undefined;
			if (requestId && controller) {
				waitControllers.set(requestId, controller);
			}
			try {
				await log.waitForReplicator(request.publicKey, {
					eager: request.eager,
					timeout: request.timeoutMs,
					roleAge: request.roleAgeMs,
					signal: controller?.signal,
				});
			} finally {
				if (requestId) waitControllers.delete(requestId);
			}
		},
		waitForReplicators: async (
			request?: SharedLogWaitForReplicatorsRequest,
		) => {
			const requestId = request?.requestId;
			const controller = requestId ? new AbortController() : undefined;
			if (requestId && controller) {
				waitControllers.set(requestId, controller);
			}
			try {
				await log.waitForReplicators({
					timeout: request?.timeoutMs,
					roleAge: request?.roleAgeMs,
					coverageThreshold: request?.coverageThreshold,
					waitForNewPeers: request?.waitForNewPeers,
					signal: controller?.signal,
				});
			} finally {
				if (requestId) waitControllers.delete(requestId);
			}
		},
		cancelWait: async (requestId: string) => {
			const controller = waitControllers.get(requestId);
			if (!controller) return;
			waitControllers.delete(requestId);
			try {
				controller.abort(new Error("AbortError"));
			} catch {
				controller.abort();
			}
		},
		replicate: async (request?: SharedLogReplicateRequest) => {
			const range = toReplicationOptions(request?.value);
			const hasOptions =
				request?.reset != null ||
				request?.checkDuplicates != null ||
				request?.rebalance != null ||
				request?.mergeSegments != null;
			const options = hasOptions
				? {
						reset: request?.reset,
						checkDuplicates: request?.checkDuplicates,
						rebalance: request?.rebalance,
						mergeSegments: request?.mergeSegments,
					}
				: undefined;
			if (range === undefined) {
				await log.replicate(undefined, options);
				return;
			}
			await log.replicate(range, options);
		},
		unreplicate: async (request?: SharedLogUnreplicateRequest) => {
			if (!request || request.ids.length === 0) {
				await log.unreplicate();
				return;
			}
			await log.unreplicate(request.ids.map((id) => ({ id })));
		},
		calculateCoverage: async (request?: SharedLogCoverageRequest) => {
			return log.calculateCoverage({
				start: request?.start as any,
				end: request?.end as any,
				roleAge: request?.roleAgeMs,
			});
		},
		getMyReplicationSegments: async () => {
			const ranges = await log.getMyReplicationSegments();
			return ranges.map(
				(range: any) => new SharedLogBytes({ value: serialize(range as any) }),
			);
		},
		getAllReplicationSegments: async () => {
			const ranges = await log.getAllReplicationSegments();
			return ranges.map(
				(range: any) => new SharedLogBytes({ value: serialize(range as any) }),
			);
		},
		resolution: async () => {
			return log.domain.resolution;
		},
		publicKey: async () => {
			return log.node.identity.publicKey;
		},
		close: async () => {
			if (closed) return;
			closed = true;
			for (const controller of waitControllers.values()) {
				try {
					controller.abort(new Error("SharedLogService closed"));
				} catch {
					controller.abort();
				}
			}
			waitControllers.clear();
			for (const off of unsubscribe) {
				off();
			}
			await options?.onClose?.();
		},
	});

	const forwardEvent = (type: string, key: PublicSignKey | undefined) => {
		if (!key) return;
		service.events.dispatchEvent(
			new CustomEvent(type, {
				detail: new SharedLogEvent({ publicKey: key }),
			}),
		);
	};

	const attach = (type: keyof SharedLogEvents) => {
		const handler = (event: any) => {
			const detail = event?.detail;
			const key = detail?.publicKey ?? detail;
			forwardEvent(type, key as PublicSignKey | undefined);
		};
		log.events.addEventListener(type, handler as any);
		unsubscribe.push(() =>
			log.events.removeEventListener(type, handler as any),
		);
	};

	attach("join");
	attach("leave");
	attach("replicator:join");
	attach("replicator:leave");
	attach("replicator:mature");
	attach("replication:change");

	return service;
};

export const sharedLogModule: CanonicalModule = {
	name: "@peerbit/shared-log",
	open: async (
		ctx: CanonicalContext,
		port: CanonicalChannel,
		payload: Uint8Array,
	) => {
		ensureCustomEvent();

		const request = deserialize(payload, OpenSharedLogRequest);
		const acquired = await acquireSharedLog({ ctx, id: request.id });

		let unbind: (() => void) | undefined;
		const transport: RpcTransport = createMessagePortTransport(port);
		const service = createSharedLogService(acquired.program, {
			onClose: async () => {
				unbind?.();
				await acquired.release();
			},
		});

		unbind = bindService(SharedLogService, transport, service);
		port.onClose?.(() => {
			void service.close();
		});
	},
};

export const installSharedLogModule = (host: {
	registerModule: (module: CanonicalModule) => void;
}): CanonicalModule => {
	host.registerModule(sharedLogModule);
	return sharedLogModule;
};
