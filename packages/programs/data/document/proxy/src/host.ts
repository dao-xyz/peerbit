import {
	type AbstractType,
	deserialize,
	getSchema,
	serialize,
} from "@dao-xyz/borsh";
import { type RpcTransport, bindService } from "@dao-xyz/borsh-rpc";
import {
	type CanonicalChannel,
	type CanonicalContext,
	type CanonicalModule,
	createMessagePortTransport,
} from "@peerbit/canonical-host";
import { Documents } from "@peerbit/document";
import * as indexerTypes from "@peerbit/indexer-interface";
import type { SharedLogService } from "@peerbit/shared-log-proxy";
import { createSharedLogService } from "@peerbit/shared-log-proxy/host";
	import {
		DocumentsChange,
		DocumentsCountRequest,
		DocumentsGetRequest,
		DocumentsIndexPutRequest,
	DocumentsPutWithContextRequest,
	DocumentsRemoteOptions,
	DocumentsService,
	DocumentsWaitForRequest,
	OpenDocumentsRequest,
} from "./protocol.js";
import {
	DocumentsIndexResult,
	DocumentsIterateRequest,
	DocumentsIteratorBatch,
	DocumentsIteratorService,
	DocumentsIteratorUpdate,
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

const toRemoteOptions = (options?: DocumentsRemoteOptions) => {
	if (!options) return undefined;
	const remote: Record<string, any> = {};
	if (options.strategy) remote.strategy = options.strategy;
	if (options.timeoutMs != null) remote.timeout = options.timeoutMs;
	if (options.from?.length) remote.from = options.from;
	if (options.reachEager != null) {
		remote.reach = { eager: options.reachEager };
	}
	if (options.waitTimeoutMs != null) {
		remote.wait = { timeout: options.waitTimeoutMs };
	}
	return remote;
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

const registeredDocumentTypes: Map<string, AbstractType<any>> = new Map();

export function registerDocumentType<T>(type: AbstractType<T>): void;
export function registerDocumentType<T>(
	name: string,
	type: AbstractType<T>,
): void;
export function registerDocumentType<T>(
	nameOrType: string | AbstractType<T>,
	type?: AbstractType<T>,
): void {
	if (typeof nameOrType === "string") {
		if (!type) {
			throw new Error("registerDocumentType(name, type) requires a type");
		}
		registeredDocumentTypes.set(nameOrType, type);
		return;
	}

	const schema = getSchema(nameOrType);
	const variant = schema?.variant;
	if (!variant) {
		throw new Error("Document type is missing @variant() metadata");
	}
	registeredDocumentTypes.set(String(variant), nameOrType);
}

export const registerDocumentTypes = (
	entries:
		| Record<string, AbstractType<any>>
		| Iterable<readonly [string, AbstractType<any>]>,
): void => {
	if (!entries) return;
	if (Symbol.iterator in Object(entries)) {
		for (const [name, type] of entries as Iterable<
			readonly [string, AbstractType<any>]
		>) {
			registerDocumentType(name, type);
		}
		return;
	}

	for (const [name, type] of Object.entries(entries)) {
		registerDocumentType(name, type);
	}
};

export const getRegisteredDocumentType = (
	name: string,
): AbstractType<any> | undefined => {
	return registeredDocumentTypes.get(name);
};

const openDocuments: Map<string, { program: Documents<any>; refs: number }> =
	new Map();

export type DocumentModuleStats = {
	total: number;
	entries: Array<{ key: string; refs: number }>;
};

export const getDocumentModuleStats = (): DocumentModuleStats => {
	return {
		total: openDocuments.size,
		entries: [...openDocuments.entries()].map(([key, value]) => ({
			key,
			refs: value.refs,
		})),
	};
};

const acquireDocuments = async <T>(properties: {
	ctx: CanonicalContext;
	typeName: string;
	type: AbstractType<T>;
	id: Uint8Array;
}): Promise<{
	program: Documents<T>;
	release: () => Promise<void>;
}> => {
	const key = `${properties.typeName}:${toHex(properties.id)}`;
	const existing = openDocuments.get(key);
	if (existing) {
		existing.refs += 1;
		return {
			program: existing.program,
			release: async () => releaseDocuments(key),
		};
	}

	const peer = await properties.ctx.peer();
	const program = await peer.open(new Documents<T>({ id: properties.id }), {
		existing: "reuse",
		args: {
			type: properties.type,
			replicate: { factor: 1 },
		} as any,
	});
	openDocuments.set(key, { program, refs: 1 });
	return { program, release: async () => releaseDocuments(key) };
};

const releaseDocuments = async (key: string): Promise<void> => {
	const existing = openDocuments.get(key);
	if (!existing) return;
	existing.refs -= 1;
	if (existing.refs > 0) return;
	openDocuments.delete(key);
	await existing.program.close();
};

export const documentModule: CanonicalModule = {
	name: "@peerbit/document",
	open: async (
		ctx: CanonicalContext,
		port: CanonicalChannel,
		payload: Uint8Array,
	) => {
		ensureCustomEvent();

		const request = deserialize(payload, OpenDocumentsRequest);
		const type = getRegisteredDocumentType(request.type);
		if (!type) {
			throw new Error(
				`Unknown document type '${request.type}'. Register it in the worker with registerDocumentType(...)`,
			);
		}

		const acquired = await acquireDocuments({
			ctx,
			typeName: request.type,
			type,
			id: request.id,
		});

		let closed = false;
		let unbind: (() => void) | undefined;
		const iteratorClosers = new Set<() => Promise<void>>();
		const logServices = new Set<SharedLogService>();
		const waitControllers = new Map<string, AbortController>();
		const indexedType = (acquired.program.index as any).indexedType as
			| AbstractType<any>
			| undefined;

		const encodeResult = (
			value: any,
			resolve: boolean,
		): DocumentsIndexResult => {
			const context = value?.__context;
			let valueBytes: Uint8Array | undefined;
			let indexedBytes: Uint8Array | undefined;

			if (resolve) {
				const docInstance = asInstanceOf(value, type);
				valueBytes = serialize(docInstance as any);
				if (value?.__indexed && indexedType) {
					const indexedInstance = asInstanceOf(value.__indexed, indexedType);
					indexedBytes = serialize(indexedInstance as any);
				}
			} else if (indexedType) {
				const indexedInstance = asInstanceOf(value, indexedType);
				indexedBytes = serialize(indexedInstance as any);
			}

			return new DocumentsIndexResult({
				context,
				value: valueBytes,
				indexed: indexedBytes,
			});
		};

		const transport: RpcTransport = createMessagePortTransport(port);
		const service = new DocumentsService({
			put: async (doc) => {
				const decoded = deserialize(doc.value, type);
				await acquired.program.put(decoded);
			},
			get: async (request: DocumentsGetRequest) => {
				const key =
					request.id instanceof indexerTypes.IdKey
						? request.id
						: indexerTypes.toId(request.id as any);
				const remoteOptions = toRemoteOptions(request.remoteOptions);
				const options: any = {
					resolve: request.resolve !== false,
				};
				if (request.local !== undefined) {
					options.local = request.local;
				}
				if (remoteOptions) {
					options.remote = remoteOptions;
				} else if (request.remote !== undefined) {
					options.remote = request.remote;
				}
				if (request.waitForMs != null) {
					options.waitFor = request.waitForMs;
				}
				const value = await acquired.program.index.get(key, options);
				return value
					? encodeResult(value, options.resolve !== false)
					: undefined;
			},
			del: async (id) => {
				const ideable =
					id instanceof indexerTypes.IdKey
						? indexerTypes.toIdeable(id)
						: (id as any);
				await acquired.program.del(ideable);
			},
			iterate: async (iterateRequest: DocumentsIterateRequest) => {
				const resolve = iterateRequest.request.resolve !== false;
				const toResult = (value: any) => encodeResult(value, resolve);

				let iterator: any;
				let done = false;
				const closeIterator = async () => {
					if (done) return;
					done = true;
					iteratorClosers.delete(closeIterator);
					if (iterator) {
						await iterator.close();
					}
				};

				const updates = new DocumentsIteratorService({
					next: async (amount) => {
						if (!iterator) {
							throw new Error("Documents iterator not ready");
						}
						const items = await iterator.next(amount);
						return new DocumentsIteratorBatch({
							results: items.map(toResult),
							done: iterator.done(),
						});
					},
					pending: async () => {
						if (!iterator) {
							throw new Error("Documents iterator not ready");
						}
						const pending = await iterator.pending();
						return pending != null ? BigInt(pending) : undefined;
					},
					done: async () => {
						if (!iterator) return false;
						return iterator.done();
					},
					close: async () => {
						await closeIterator();
					},
				});

				const emitUpdate = (reason: string, items?: any[]) => {
					const results = (items ?? []).map(toResult);
					updates.updates.dispatchEvent(
						new CustomEvent("update", {
							detail: new DocumentsIteratorUpdate({ reason, results }),
						}),
					);
				};

				const remoteOptions = toRemoteOptions(iterateRequest.remoteOptions);
				const iterateOptions: any = {
					closePolicy: iterateRequest.closePolicy,
					updates: iterateRequest.emitUpdates
						? {
								push: iterateRequest.request.pushUpdates,
								merge: iterateRequest.request.mergeUpdates ? true : undefined,
								notify: (reason: string) => emitUpdate(reason),
								onBatch: (batch: any[], meta: { reason: string }) =>
									emitUpdate(meta.reason, batch),
							}
						: undefined,
				};
				if (iterateRequest.local !== undefined) {
					iterateOptions.local = iterateRequest.local;
				}
				if (remoteOptions) {
					iterateOptions.remote = remoteOptions;
				} else if (iterateRequest.remote !== undefined) {
					iterateOptions.remote = iterateRequest.remote;
				}

				iterator = acquired.program.index.iterate(
					iterateRequest.request,
					iterateOptions,
				);

				iteratorClosers.add(closeIterator);
				return updates;
			},
			putWithContext: async (request: DocumentsPutWithContextRequest) => {
				const decoded = deserialize(request.value.value, type);
				await acquired.program.index.putWithContext(
					asInstanceOf(decoded, type),
					request.id,
					request.context,
				);
			},
			indexPut: async (request: DocumentsIndexPutRequest) => {
				if (!indexedType) {
					throw new Error("Index type is missing");
				}
				const decoded = deserialize(request.indexed, indexedType);
				const wrapped = new (acquired.program.index as any).wrappedIndexedType(
					asInstanceOf(decoded, indexedType),
					request.context,
				);
				await (acquired.program.index as any).index.put(wrapped);
				},
				count: async (request: DocumentsCountRequest) => {
					const approximate = request?.approximate !== false;
					if (approximate) {
						const { estimate } = await acquired.program.count({ approximate: true });
						return BigInt(estimate);
					}
					const count = await acquired.program.index.getSize();
					return BigInt(count);
				},
				indexSize: async () => {
					const size = await acquired.program.index.getSize();
					return BigInt(size);
				},
			waitFor: async (request: DocumentsWaitForRequest) => {
				const requestId = request.requestId;
				const controller = requestId ? new AbortController() : undefined;
				if (requestId && controller) {
					waitControllers.set(requestId, controller);
				}
				try {
					return await acquired.program.waitFor(request.peers, {
						seek: request.seek,
						timeout: request.timeoutMs,
						signal: controller?.signal,
					});
				} finally {
					if (requestId) waitControllers.delete(requestId);
				}
			},
			indexWaitFor: async (request: DocumentsWaitForRequest) => {
				const requestId = request.requestId;
				const controller = requestId ? new AbortController() : undefined;
				if (requestId && controller) {
					waitControllers.set(requestId, controller);
				}
				try {
					return await acquired.program.index.waitFor(request.peers, {
						seek: request.seek,
						timeout: request.timeoutMs,
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
			recover: async () => {
				await acquired.program.recover();
			},
			openLog: async () => {
				let service: SharedLogService;
				service = createSharedLogService(acquired.program.log, {
					onClose: async () => {
						logServices.delete(service);
					},
				});
				logServices.add(service);
				return service;
			},
			close: async () => {
				if (closed) return;
				closed = true;
				for (const controller of waitControllers.values()) {
					try {
						controller.abort(new Error("DocumentsService closed"));
					} catch {
						controller.abort();
					}
				}
				waitControllers.clear();
				acquired.program.events.removeEventListener("change", onChange as any);
				unbind?.();
				for (const service of logServices) {
					await service.close();
				}
				for (const closeIterator of iteratorClosers) {
					await closeIterator();
				}
				await acquired.release();
			},
		});

		const onChange = (evt: CustomEvent<any>) => {
			const change = evt.detail as {
				added?: Array<any>;
				removed?: Array<any>;
			};
			const added = (change.added ?? []).map((x) => encodeResult(x, true));
			const removed = (change.removed ?? []).map((x) => encodeResult(x, true));
			service.changes.dispatchEvent(
				new CustomEvent("change", {
					detail: new DocumentsChange({ added, removed }),
				}),
			);
		};

		acquired.program.events.addEventListener("change", onChange as any);
		unbind = bindService(DocumentsService, transport, service);
		port.onClose?.(() => {
			void service.close();
		});
	},
};

export const installDocumentModule = (
	host: { registerModule: (module: CanonicalModule) => void },
	entries?:
		| Record<string, AbstractType<any>>
		| Iterable<readonly [string, AbstractType<any>]>,
): CanonicalModule => {
	if (entries) {
		registerDocumentTypes(entries);
	}
	host.registerModule(documentModule);
	return documentModule;
};
