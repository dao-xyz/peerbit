import { type AbstractType, deserialize, serialize } from "@dao-xyz/borsh";
import { createProxyFromService } from "@dao-xyz/borsh-rpc";
import {
	type CanonicalClient,
	createMessagePortTransport,
} from "@peerbit/canonical-client";
import type {
	DocumentsLike,
	DocumentsLikeCountOptions,
	DocumentsLikeIndex,
	DocumentsLikeQuery,
	DocumentsLikeWaitForOptions,
	GetOptions,
	QueryOptions,
	ResultsIterator,
	SearchOptions,
	UpdateOptions,
	UpdateReason,
	ValueTypeFromRequest,
	WithContext,
} from "@peerbit/document";
import {
	Context,
	IterationRequest,
	PushUpdatesMode,
	ResultIndexedValue,
	ResultValue,
	Results,
} from "@peerbit/document-interface";
import * as indexerTypes from "@peerbit/indexer-interface";
import {
	type SharedLogProxy,
	createSharedLogProxyFromService,
} from "@peerbit/shared-log-proxy/client";
import {
	type PeerRefs,
	coercePeerRefsToHashes,
} from "@peerbit/stream-interface";
import {
	Bytes,
	DocumentsChange,
	DocumentsCountRequest,
	DocumentsGetRequest,
	DocumentsIndexPutRequest,
	DocumentsIndexResult,
	DocumentsIterateRequest,
	DocumentsIteratorBatch,
	DocumentsIteratorService,
	DocumentsIteratorUpdate,
	DocumentsPutWithContextRequest,
	DocumentsRemoteOptions,
	DocumentsService,
	DocumentsWaitForRequest,
	OpenDocumentsRequest,
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

const asInstanceOf = <T>(value: any, type: AbstractType<T>): T => {
	if (!value || typeof value !== "object") {
		return value as T;
	}
	if (value instanceof (type as any)) {
		return value as T;
	}
	return Object.assign(Object.create((type as any).prototype), value) as T;
};

export type DocumentsProxyChange<T> = {
	added: T[];
	removed: T[];
};

export type DocumentsProxyWaitForOptions = DocumentsLikeWaitForOptions;

export type DocumentsProxyCountOptions = DocumentsLikeCountOptions;

export type DocumentsProxyIndex<T, I = any> = DocumentsLikeIndex<T, I> & {
	putWithContext?: (
		value: T,
		id: indexerTypes.Ideable | indexerTypes.IdKey,
		context: Context,
	) => Promise<void>;
};

export type DocumentsProxy<T, I = any> = DocumentsLike<T, I> & {
	raw: DocumentsService;
	log: SharedLogProxy;
};

const coerceWithContext = <T>(value: T, context: any): T => {
	if (value && typeof value === "object") {
		(value as any).__context = context;
	}
	return value;
};

const coerceWithIndexed = <T>(value: T, indexed: any): T => {
	if (value && typeof value === "object") {
		(value as any).__indexed = indexed;
	}
	return value;
};

const normalizeUpdates = <Resolve extends boolean | undefined>(
	updates?: UpdateOptions<any, any, Resolve>,
): { push?: PushUpdatesMode; merge?: boolean; emitUpdates: boolean } => {
	if (!updates) {
		return { emitUpdates: false };
	}
	if (updates === true) {
		return { merge: true, emitUpdates: true };
	}
	if (typeof updates === "string") {
		if (updates === "local") {
			return { merge: true, emitUpdates: true };
		}
		if (updates === "remote") {
			return { push: PushUpdatesMode.STREAM, emitUpdates: true };
		}
		if (updates === "all") {
			return { merge: true, push: PushUpdatesMode.STREAM, emitUpdates: true };
		}
		return { emitUpdates: false };
	}
	if (typeof updates === "object") {
		const hasMerge = Object.prototype.hasOwnProperty.call(updates, "merge");
		const merge = hasMerge
			? updates.merge === false
				? undefined
				: true
			: true;
		const push =
			typeof updates.push === "number"
				? updates.push
				: updates.push
					? PushUpdatesMode.STREAM
					: undefined;
		const emitUpdates = !!(updates.notify || updates.onBatch || merge || push);
		return { push, merge, emitUpdates };
	}
	return { emitUpdates: false };
};

const toRemoteOptions = (remote: any): DocumentsRemoteOptions | undefined => {
	if (!remote || typeof remote === "boolean") return undefined;
	const waitTimeoutMs =
		typeof remote.wait === "object" ? remote.wait?.timeout : undefined;
	return new DocumentsRemoteOptions({
		strategy: remote.strategy,
		timeoutMs: remote.timeout,
		from: remote.from,
		reachEager: remote.reach?.eager,
		waitTimeoutMs,
	});
};

export const openDocuments = async <T, I = T>(properties: {
	client: CanonicalClient;
	id: Uint8Array;
	typeName: string;
	type: AbstractType<T>;
	indexType?: AbstractType<I>;
}): Promise<DocumentsProxy<T, I>> => {
	ensureCustomEvent();

	const channel = await properties.client.openPort(
		"@peerbit/document",
		serialize(
			new OpenDocumentsRequest({
				id: properties.id,
				type: properties.typeName,
			}),
		),
	);

	const transport = createMessagePortTransport(channel, {
		requestTimeoutMs: (method) => {
			if (method === "waitFor" || method === "indexWaitFor") return undefined;
			return 30_000;
		},
	});
	const raw = createProxyFromService(
		DocumentsService,
		transport,
	) as unknown as DocumentsService;
	const logService = await raw.openLog();
	const log = await createSharedLogProxyFromService(logService);
	let closed = false;

	const decodeChangeValue = (result: DocumentsIndexResult): T | undefined => {
		if (!result.value) return undefined;
		const value = deserialize(result.value, properties.type);
		coerceWithContext(value as any, result.context);
		const indexType = properties.indexType ?? properties.type;
		if (result.indexed && indexType) {
			const indexed = deserialize(result.indexed, indexType);
			coerceWithIndexed(value as any, indexed);
		}
		return value as T;
	};

	const decodeChange = (change: DocumentsChange): DocumentsProxyChange<T> => {
		return {
			added: (change.added ?? []).map(decodeChangeValue).filter(Boolean) as T[],
			removed: (change.removed ?? [])
				.map(decodeChangeValue)
				.filter(Boolean) as T[],
		};
	};

	const changes = new EventTarget();
	const onChange = (e: any) => {
		const detail = decodeChange(e.detail as DocumentsChange);
		changes.dispatchEvent(new CustomEvent("change", { detail }));
	};
	raw.changes.addEventListener("change", onChange);

	const coerceIdKey = (
		id: indexerTypes.Ideable | indexerTypes.IdKey,
	): indexerTypes.IdKey => {
		return id instanceof indexerTypes.IdKey ? id : indexerTypes.toId(id);
	};

	const createGetRequest = <Resolve extends boolean | undefined>(
		id: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: GetOptions<T, I, any, Resolve>,
		resolveOverride?: Resolve,
	) => {
		if (options?.signal?.aborted) {
			throw new Error("AbortError");
		}
		const resolve =
			resolveOverride !== undefined
				? resolveOverride !== false
				: options?.resolve !== false;
		const remoteOption = options?.remote;
		const remoteOptions = toRemoteOptions(remoteOption);
		const remote =
			typeof remoteOption === "boolean"
				? remoteOption
				: remoteOption
					? true
					: undefined;
		return {
			resolve: resolve as Resolve,
			request: new DocumentsGetRequest({
				id: coerceIdKey(id),
				resolve,
				local: options?.local,
				remote,
				remoteOptions,
				waitForMs: options?.waitFor,
			}),
		};
	};

	const fetchValue = async <Resolve extends boolean | undefined>(
		id: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: GetOptions<T, I, any, Resolve>,
		resolveOverride?: Resolve,
	): Promise<ValueTypeFromRequest<Resolve, T, I> | undefined> => {
		const { request, resolve } = createGetRequest(id, options, resolveOverride);
		const result = await raw.get(request);
		if (!result) return undefined;
		return decodeIndexResult(result, resolve) as ValueTypeFromRequest<
			Resolve,
			T,
			I
		>;
	};

	const get = async (
		id: indexerTypes.Ideable | indexerTypes.IdKey,
		options?: GetOptions<T, I, any, true | undefined>,
	) => {
		return fetchValue(id, options, true) as Promise<T | undefined>;
	};

	const idPath =
		indexerTypes.getIdProperty(properties.indexType ?? properties.type) ??
		(["id"] as string[]);
	const resolveId = (value: any): indexerTypes.IdKey => {
		let candidate = value;
		if (candidate && typeof candidate === "object") {
			if ("__indexed" in candidate && candidate.__indexed) {
				candidate = candidate.__indexed;
			}
		}
		const resolved = indexerTypes.extractFieldValue(
			candidate,
			idPath as string[],
		);
		return indexerTypes.toId(resolved as indexerTypes.Ideable);
	};

	const decodeIndexResult = <Resolve extends boolean | undefined>(
		result: DocumentsIndexResult,
		resolve: Resolve,
	): ValueTypeFromRequest<Resolve, T, I> | undefined => {
		const context = result.context;
		const indexType = properties.indexType ?? properties.type;
		if (resolve !== false) {
			if (!result.value) return undefined;
			const value = deserialize(result.value, properties.type);
			coerceWithContext(value as any, context);
			if (result.indexed && indexType) {
				const indexed = deserialize(result.indexed, indexType);
				coerceWithIndexed(value as any, indexed);
			}
			return value as ValueTypeFromRequest<Resolve, T, I>;
		}
		if (result.indexed && indexType) {
			const indexed = deserialize(result.indexed, indexType);
			coerceWithContext(indexed as any, context);
			return indexed as ValueTypeFromRequest<Resolve, T, I>;
		}
		return undefined;
	};

	const decodeIteratorBatch = <Resolve extends boolean | undefined>(
		batch: DocumentsIteratorBatch,
		resolve: Resolve,
	): {
		items: ValueTypeFromRequest<Resolve, T, I>[];
		done: boolean;
	} => {
		const items = (batch.results ?? [])
			.map((result) => decodeIndexResult(result, resolve))
			.filter(Boolean) as ValueTypeFromRequest<Resolve, T, I>[];
		return { items, done: batch.done };
	};

	const decodeUpdateResults = <Resolve extends boolean | undefined>(
		update: DocumentsIteratorUpdate,
		resolve: Resolve,
	): ValueTypeFromRequest<Resolve, T, I>[] => {
		return (update.results ?? [])
			.map((result) => decodeIndexResult(result, resolve))
			.filter(Boolean) as ValueTypeFromRequest<Resolve, T, I>[];
	};

	const toNumber = (value: bigint | number): number => {
		return typeof value === "bigint" ? Number(value) : value;
	};

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

	const createWaitForRequest = (
		peers: PeerRefs,
		options?: DocumentsProxyWaitForOptions,
		requestId?: string,
	) => {
		if (options?.signal?.aborted) {
			throw new Error("AbortError");
		}
		const hashes = coercePeerRefsToHashes(peers);
		return new DocumentsWaitForRequest({
			peers: hashes,
			seek: options?.seek,
			timeoutMs: options?.timeout,
			requestId,
		});
	};

	const waitFor = async (
		peers: PeerRefs,
		options?: DocumentsProxyWaitForOptions,
	): Promise<string[]> => {
		const signal = options?.signal;
		const requestId = signal ? randomId() : undefined;
		const request = createWaitForRequest(peers, options, requestId);
		const call = raw.waitFor(request);
		if (!signal || !requestId) {
			return call;
		}

		return new Promise<string[]>((resolve, reject) => {
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
				(value) => {
					cleanup();
					resolve(value);
				},
				(error) => {
					cleanup();
					reject(error);
				},
			);
		});
	};

	const indexWaitFor = async (
		peers: PeerRefs,
		options?: DocumentsProxyWaitForOptions,
	): Promise<string[]> => {
		const signal = options?.signal;
		const requestId = signal ? randomId() : undefined;
		const request = createWaitForRequest(peers, options, requestId);
		const call = raw.indexWaitFor(request);
		if (!signal || !requestId) {
			return call;
		}

		return new Promise<string[]>((resolve, reject) => {
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
				(value) => {
					cleanup();
					resolve(value);
				},
				(error) => {
					cleanup();
					reject(error);
				},
			);
		});
	};

	const indexSize = async (): Promise<number> => {
		const size = await raw.indexSize();
		return toNumber(size);
	};

	const index: DocumentsProxyIndex<T, I> = {
		get: async <Resolve extends boolean | undefined = true>(
			id: indexerTypes.Ideable | indexerTypes.IdKey,
			options?: GetOptions<T, I, any, Resolve>,
		) => {
			return fetchValue(id, options);
		},
		getDetailed: async <Resolve extends boolean | undefined = true>(
			id: indexerTypes.IdKey | indexerTypes.IdPrimitive,
			options?: QueryOptions<T, I, any, Resolve>,
		) => {
			const resolve =
				options?.resolve !== undefined ? options.resolve !== false : true;

			const remoteOption = options?.remote;
			const remoteOptions = toRemoteOptions(remoteOption);
			const remote =
				typeof remoteOption === "boolean"
					? remoteOption
					: remoteOption
						? true
						: undefined;

			const result = await raw.get(
				new DocumentsGetRequest({
					id: coerceIdKey(id as any),
					resolve,
					local: options?.local,
					remote,
					remoteOptions,
				}),
			);

			if (!result) return undefined;

			if (resolve) {
				const value = decodeIndexResult(result, true as Resolve);
				if (!value) return undefined;
				const entry = new ResultValue({
					source: result.value,
					context: result.context,
					value,
				});
				return [new Results({ results: [entry], kept: 0n })] as any;
			}

			const indexed = decodeIndexResult(result, false as Resolve);
			if (!indexed) return undefined;
			const entry = new ResultIndexedValue({
				source: result.indexed ?? new Uint8Array(),
				indexed,
				entries: [],
				context: result.context,
			});
			return [new Results({ results: [entry], kept: 0n })] as any;
		},
		resolveId,
		iterate: <Resolve extends boolean | undefined = true>(
			query?: DocumentsLikeQuery,
			options?: QueryOptions<T, I, any, Resolve>,
		): ResultsIterator<ValueTypeFromRequest<Resolve, T, I>> => {
			const updates = normalizeUpdates(options?.updates);
			const remoteOption = options?.remote;
			const remoteOptions = toRemoteOptions(remoteOption);
			let remote =
				typeof remoteOption === "boolean"
					? remoteOption
					: remoteOption
						? true
						: undefined;
			let replicate =
				typeof remoteOption === "object" && remoteOption?.replicate
					? true
					: false;
			if (updates.push && remote !== false) {
				remote = true;
				replicate = true;
			}
			const queryObject = query as any;
			const isQueryWrapper =
				!!queryObject &&
				typeof queryObject === "object" &&
				!Array.isArray(queryObject) &&
				("query" in queryObject ||
					("sort" in queryObject &&
						Object.keys(queryObject).every(
							(k) => k === "sort" || k === "query",
						)));
			const queryValue = isQueryWrapper ? queryObject.query : queryObject;
			const sortValue = isQueryWrapper ? queryObject.sort : undefined;
			const request =
				query instanceof IterationRequest
					? query
					: new IterationRequest({
							query: queryValue,
							sort: sortValue,
							fetch: (options as any)?.fetch ?? 10,
							resolve: options?.resolve !== false,
							replicate: options?.resolve !== false ? false : replicate,
						});
			const resolveBool =
				options?.resolve !== undefined
					? options.resolve !== false
					: request.resolve !== false;
			const resolve = resolveBool as Resolve;
			if (options?.resolve !== undefined) {
				request.resolve = resolveBool;
			}
			if (!resolve && replicate && request.replicate !== true) {
				request.replicate = true;
			}
			if (updates.emitUpdates) {
				request.pushUpdates = updates.push;
				request.mergeUpdates = updates.merge;
			}
			const emitUpdates =
				updates.emitUpdates ||
				request.pushUpdates != null ||
				request.mergeUpdates != null;
			const iterateRequest = new DocumentsIterateRequest({
				request,
				local: options?.local,
				remote,
				remoteOptions,
				closePolicy: (options as any)?.closePolicy,
				emitUpdates,
			});

			let done = false;
			let updatesListener: ((event: any) => void) | undefined;
			const abortSignal = options?.signal;
			let abortListener: (() => void) | undefined;

			const proxyPromise = raw.iterate(iterateRequest);

			const createIterator = (
				service: DocumentsIteratorService,
			): ResultsIterator<ValueTypeFromRequest<Resolve, T, I>> => {
				if (
					updates.emitUpdates &&
					options?.updates &&
					typeof options.updates === "object"
				) {
					const updateConfig = options.updates;
					updatesListener = (event: any) => {
						const detail = event?.detail as DocumentsIteratorUpdate;
						if (!detail) return;
						const reason = detail.reason as UpdateReason;
						if (updateConfig.notify) {
							updateConfig.notify(reason);
						}
						if (updateConfig.onBatch && detail.results?.length) {
							const items = decodeUpdateResults(detail, resolve);
							if (items.length) {
								updateConfig.onBatch(items as any, { reason });
							}
						}
					};
					service.updates.addEventListener("update", updatesListener);
				}

				const iterator: ResultsIterator<ValueTypeFromRequest<Resolve, T, I>> = {
					next: async (amount: number) => {
						if (abortSignal?.aborted) {
							throw abortError();
						}
						const batch = await service.next(amount);
						const decoded = decodeIteratorBatch(batch, resolve);
						done = decoded.done;
						return decoded.items;
					},
					pending: async () => {
						if (abortSignal?.aborted) {
							throw abortError();
						}
						const pending = await service.pending();
						return pending != null ? Number(pending) : undefined;
					},
					done: () => done,
					all: async () => {
						if (abortSignal?.aborted) {
							throw abortError();
						}
						const out: ValueTypeFromRequest<Resolve, T, I>[] = [];
						while (!done) {
							const next = await iterator.next((options as any)?.fetch ?? 10);
							out.push(...next);
						}
						return out;
					},
					first: async () => {
						if (abortSignal?.aborted) {
							throw abortError();
						}
						const next = await iterator.next(1);
						return next[0];
					},
					close: async () => {
						done = true;
						if (abortSignal && abortListener) {
							abortSignal.removeEventListener("abort", abortListener);
						}
						if (updatesListener) {
							service.updates.removeEventListener("update", updatesListener);
						}
						await service.close();
					},
					[Symbol.asyncIterator]: () => {
						return {
							next: async () => {
								const items = await iterator.next(1);
								if (items.length === 0) {
									return { done: true, value: undefined };
								}
								return { done: false, value: items[0] };
							},
							return: async () => {
								await iterator.close();
								return { done: true, value: undefined };
							},
						};
					},
				};
				return iterator;
			};

			let iterator: ResultsIterator<ValueTypeFromRequest<Resolve, T, I>>;
			abortListener = () => {
				void iterator.close();
			};
			iterator = {
				next: async (amount: number) => {
					if (abortSignal?.aborted) {
						throw abortError();
					}
					const service = await proxyPromise;
					Object.assign(iterator, createIterator(service));
					return iterator.next(amount);
				},
				pending: async () => {
					if (abortSignal?.aborted) {
						throw abortError();
					}
					const service = await proxyPromise;
					Object.assign(iterator, createIterator(service));
					return iterator.pending();
				},
				done: () => done,
				all: async () => {
					if (abortSignal?.aborted) {
						throw abortError();
					}
					const service = await proxyPromise;
					Object.assign(iterator, createIterator(service));
					return iterator.all();
				},
				first: async () => {
					if (abortSignal?.aborted) {
						throw abortError();
					}
					const service = await proxyPromise;
					Object.assign(iterator, createIterator(service));
					return iterator.first();
				},
				close: async () => {
					const service = await proxyPromise;
					Object.assign(iterator, createIterator(service));
					return iterator.close();
				},
				[Symbol.asyncIterator]: () => {
					return {
						next: async () => {
							if (abortSignal?.aborted) {
								throw abortError();
							}
							const service = await proxyPromise;
							Object.assign(iterator, createIterator(service));
							const iter = iterator[Symbol.asyncIterator]();
							return iter.next();
						},
						return: async () => {
							if (abortSignal?.aborted) {
								return { done: true, value: undefined };
							}
							const service = await proxyPromise;
							Object.assign(iterator, createIterator(service));
							const iter = iterator[Symbol.asyncIterator]();
							return iter.return
								? iter.return()
								: { done: true, value: undefined };
						},
					};
				},
			};

			if (abortSignal) {
				if (abortSignal.aborted) {
					throw abortError();
				}
				abortSignal.addEventListener("abort", abortListener, { once: true });
			}

			return iterator;
		},
		search: async <Resolve extends boolean | undefined = true>(
			query: DocumentsLikeQuery,
			options?: SearchOptions<T, I, any, Resolve>,
		) => {
			const iterator = index.iterate<Resolve>(query, {
				resolve: options?.resolve,
				local: options?.local,
				remote: options?.remote,
				fetch: (options as any)?.fetch ?? 0xffffffff,
			} as any);
			const out = await iterator.all();
			await iterator.close();
			const seen = new Set<string>();
			return out.filter((item) => {
				const id = resolveId(item).primitive;
				if (seen.has(String(id))) return false;
				seen.add(String(id));
				return true;
			}) as ValueTypeFromRequest<Resolve, T, I>[];
		},
		getSize: indexSize,
		waitFor: indexWaitFor,
	};

	const countByIterate = async (
		query?: indexerTypes.QueryLike | indexerTypes.Query[],
	): Promise<number> => {
		const iterator = index.iterate(query ? { query } : undefined, {
			resolve: false,
			fetch: 100,
			local: true,
			remote: false,
		} as any);
		let total = 0;
		while (!iterator.done()) {
			const batch = await iterator.next(100);
			total += batch.length;
		}
		await iterator.close();
		return total;
	};

	const count = async (
		options?: DocumentsProxyCountOptions,
	): Promise<number> => {
		if (options?.query) {
			return countByIterate(options.query as any);
		}
		const approximate = options?.approximate !== false;
		const total = await raw.count(new DocumentsCountRequest({ approximate }));
		return toNumber(total);
	};

	class WrappedIndexedType {
		__context: Context;
		constructor(value: I, context: Context) {
			Object.assign(this, value);
			this.__context = context;
		}
	}

	const putWithContext = async (
		value: T,
		id: indexerTypes.Ideable | indexerTypes.IdKey,
		context: Context,
	) => {
		const ctx =
			context instanceof Context ? context : new Context(context as any);
		const request = new DocumentsPutWithContextRequest({
			value: new Bytes({ value: serialize(value as any) }),
			id: coerceIdKey(id),
			context: ctx,
		});
		await raw.putWithContext(request);
	};

	const indexPut = async (value: any) => {
		const ctx = value?.__context;
		if (!ctx) {
			throw new Error("Missing __context for index.put");
		}
		const context = ctx instanceof Context ? ctx : new Context(ctx as any);
		const indexType = properties.indexType ?? properties.type;
		const stripped = { ...(value as any) };
		delete (stripped as any).__context;
		const indexed = asInstanceOf(stripped, indexType);
		const request = new DocumentsIndexPutRequest({
			indexed: serialize(indexed as any),
			context,
		});
		await raw.indexPut(request);
	};

	const createIndexIterator = (
		iterator: ResultsIterator<WithContext<I>>,
	): indexerTypes.IndexIterator<
		WithContext<I>,
		indexerTypes.Shape | undefined
	> => {
		const toIndexedResult = (value: WithContext<I>) => ({
			id: resolveId(value),
			value,
		});
		return {
			next: async (amount: number) => {
				const batch = await iterator.next(amount);
				return batch.map(toIndexedResult);
			},
			all: async () => {
				const batch = await iterator.all();
				return batch.map(toIndexedResult);
			},
			done: () => iterator.done(),
			pending: async () => {
				const pending = await iterator.pending();
				return pending ?? 0;
			},
			close: async () => {
				await iterator.close();
			},
		};
	};

	index.putWithContext = putWithContext;
	index.wrappedIndexedType = WrappedIndexedType as unknown as new (
		value: I,
		context: Context,
	) => WithContext<I>;
	index.index = {
		count: async (options?: indexerTypes.CountOptions) => {
			return countByIterate(options?.query as any);
		},
		getSize: indexSize,
		get: async (
			id: indexerTypes.IdKey,
			_options?: { shape?: indexerTypes.Shape },
		) => {
			const value = await fetchValue(id, {
				resolve: false,
				local: true,
				remote: false,
			} as any);
			return value
				? { id: coerceIdKey(id), value: value as WithContext<I> }
				: undefined;
		},
		iterate: (
			request?: indexerTypes.IterateOptions,
			_options?: { shape?: indexerTypes.Shape; reference?: boolean },
		) => {
			const iterator = index.iterate(
				request as any,
				{
					resolve: false,
					local: true,
					remote: false,
				} as any,
			) as unknown as ResultsIterator<WithContext<I>>;
			return createIndexIterator(iterator);
		},
		put: indexPut,
	};

	const close = async () => {
		if (closed) return;
		closed = true;
		raw.changes.removeEventListener("change", onChange);
		try {
			await log.close();
			await raw.close();
		} finally {
			channel.close?.();
		}
	};

	const proxy = {
		raw,
		log,
		events: changes,
		changes,
		index,
		put: async (doc) => raw.put(new Bytes({ value: serialize(doc as any) })),
		get,
		del: async (id) => raw.del(coerceIdKey(id)),
		count,
		waitFor,
		recover: async () => {
			await raw.recover();
		},
		close,
	} as DocumentsProxy<T, I>;

	Object.defineProperty(proxy, "closed", {
		get: () => closed,
		set: (value: boolean) => {
			closed = value;
		},
		enumerable: true,
	});

	return proxy;
};
