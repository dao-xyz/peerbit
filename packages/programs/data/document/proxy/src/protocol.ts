import { field, fixedArray, option, variant, vec } from "@dao-xyz/borsh";
import { events, method, service } from "@dao-xyz/borsh-rpc";
import { Context, IterationRequest } from "@peerbit/document-interface";
import { IdKey } from "@peerbit/indexer-interface";
import { SharedLogService } from "@peerbit/shared-log-proxy";

@variant("bytes")
export class Bytes {
	@field({ type: Uint8Array })
	value: Uint8Array;

	constructor(properties?: { value?: Uint8Array }) {
		this.value = properties?.value ?? new Uint8Array();
	}
}

@variant("documents_index_result")
export class DocumentsIndexResult {
	@field({ type: Context })
	context: Context;

	@field({ type: option(Uint8Array) })
	value?: Uint8Array;

	@field({ type: option(Uint8Array) })
	indexed?: Uint8Array;

	constructor(properties: {
		context: Context;
		value?: Uint8Array;
		indexed?: Uint8Array;
	}) {
		this.context = properties.context;
		this.value = properties.value;
		this.indexed = properties.indexed;
	}
}

@variant("documents_change")
export class DocumentsChange {
	@field({ type: vec(DocumentsIndexResult) })
	added: DocumentsIndexResult[];

	@field({ type: vec(DocumentsIndexResult) })
	removed: DocumentsIndexResult[];

	constructor(properties?: {
		added?: DocumentsIndexResult[];
		removed?: DocumentsIndexResult[];
	}) {
		this.added = properties?.added ?? [];
		this.removed = properties?.removed ?? [];
	}
}

@variant("open_documents")
export class OpenDocumentsRequest {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array;

	@field({ type: "string" })
	type: string;

	constructor(properties?: { id?: Uint8Array; type?: string }) {
		this.id = properties?.id ?? new Uint8Array(32);
		this.type = properties?.type ?? "";
	}
}

@variant("documents_remote_options")
export class DocumentsRemoteOptions {
	@field({ type: option("string") })
	strategy?: "fallback";

	@field({ type: option("u32") })
	timeoutMs?: number;

	@field({ type: option(vec("string")) })
	from?: string[];

	@field({ type: option("bool") })
	reachEager?: boolean;

	@field({ type: option("u32") })
	waitTimeoutMs?: number;

	constructor(properties?: {
		strategy?: "fallback";
		timeoutMs?: number;
		from?: string[];
		reachEager?: boolean;
		waitTimeoutMs?: number;
	}) {
		this.strategy = properties?.strategy;
		this.timeoutMs = properties?.timeoutMs;
		this.from = properties?.from;
		this.reachEager = properties?.reachEager;
		this.waitTimeoutMs = properties?.waitTimeoutMs;
	}
}

@variant("documents_get_request")
export class DocumentsGetRequest {
	@field({ type: IdKey })
	id: IdKey;

	@field({ type: option("bool") })
	resolve?: boolean;

	@field({ type: option("bool") })
	local?: boolean;

	@field({ type: option("bool") })
	remote?: boolean;

	@field({ type: option(DocumentsRemoteOptions) })
	remoteOptions?: DocumentsRemoteOptions;

	@field({ type: option("u32") })
	waitForMs?: number;

	constructor(properties: {
		id: IdKey;
		resolve?: boolean;
		local?: boolean;
		remote?: boolean;
		remoteOptions?: DocumentsRemoteOptions;
		waitForMs?: number;
	}) {
		this.id = properties.id;
		this.resolve = properties.resolve;
		this.local = properties.local;
		this.remote = properties.remote;
		this.remoteOptions = properties.remoteOptions;
		this.waitForMs = properties.waitForMs;
	}
}

@variant("documents_iterator_batch")
export class DocumentsIteratorBatch {
	@field({ type: vec(DocumentsIndexResult) })
	results: DocumentsIndexResult[];

	@field({ type: "bool" })
	done: boolean;

	constructor(properties: {
		results?: DocumentsIndexResult[];
		done?: boolean;
	}) {
		this.results = properties.results ?? [];
		this.done = properties.done ?? false;
	}
}

@variant("documents_iterator_update")
export class DocumentsIteratorUpdate {
	@field({ type: "string" })
	reason: string;

	@field({ type: vec(DocumentsIndexResult) })
	results: DocumentsIndexResult[];

	constructor(properties: {
		reason: string;
		results?: DocumentsIndexResult[];
	}) {
		this.reason = properties.reason;
		this.results = properties.results ?? [];
	}
}

@variant("documents_iterate_request")
export class DocumentsIterateRequest {
	@field({ type: IterationRequest })
	request: IterationRequest;

	@field({ type: option("bool") })
	local?: boolean;

	@field({ type: option("bool") })
	remote?: boolean;

	@field({ type: option(DocumentsRemoteOptions) })
	remoteOptions?: DocumentsRemoteOptions;

	@field({ type: option("string") })
	closePolicy?: "onEmpty" | "manual";

	@field({ type: "bool" })
	emitUpdates: boolean;

	constructor(properties: {
		request: IterationRequest;
		local?: boolean;
		remote?: boolean;
		remoteOptions?: DocumentsRemoteOptions;
		closePolicy?: "onEmpty" | "manual";
		emitUpdates?: boolean;
	}) {
		this.request = properties.request;
		this.local = properties.local;
		this.remote = properties.remote;
		this.remoteOptions = properties.remoteOptions;
		this.closePolicy = properties.closePolicy;
		this.emitUpdates = properties.emitUpdates ?? false;
	}
}

@variant("documents_put_with_context")
export class DocumentsPutWithContextRequest {
	@field({ type: Bytes })
	value: Bytes;

	@field({ type: IdKey })
	id: IdKey;

	@field({ type: Context })
	context: Context;

	constructor(properties: { value: Bytes; id: IdKey; context: Context }) {
		this.value = properties.value;
		this.id = properties.id;
		this.context = properties.context;
	}
}

@variant("documents_index_put")
export class DocumentsIndexPutRequest {
	@field({ type: Uint8Array })
	indexed: Uint8Array;

	@field({ type: Context })
	context: Context;

	constructor(properties: { indexed: Uint8Array; context: Context }) {
		this.indexed = properties.indexed;
		this.context = properties.context;
	}
}

@variant("documents_count")
export class DocumentsCountRequest {
	@field({ type: "bool" })
	approximate: boolean;

	constructor(properties?: { approximate?: boolean }) {
		this.approximate = properties?.approximate ?? true;
	}
}

@variant("documents_wait_for")
export class DocumentsWaitForRequest {
	@field({ type: vec("string") })
	peers: string[];

	@field({ type: option("string") })
	seek?: "any" | "present";

	@field({ type: option("u32") })
	timeoutMs?: number;

	@field({ type: option("string") })
	requestId?: string;

	constructor(properties: {
		peers: string[];
		seek?: "any" | "present";
		timeoutMs?: number;
		requestId?: string;
	}) {
		this.peers = properties.peers;
		this.seek = properties.seek;
		this.timeoutMs = properties.timeoutMs;
		this.requestId = properties.requestId;
	}
}

@service()
export class DocumentsIteratorService {
	private _impl:
		| {
				next: (amount: number) => Promise<DocumentsIteratorBatch>;
				pending: () => Promise<bigint | undefined>;
				done: () => Promise<boolean>;
				close: () => Promise<void>;
		  }
		| undefined;

	@events(DocumentsIteratorUpdate)
	updates = new EventTarget();

	constructor(impl?: {
		next: (amount: number) => Promise<DocumentsIteratorBatch>;
		pending: () => Promise<bigint | undefined>;
		done: () => Promise<boolean>;
		close: () => Promise<void>;
	}) {
		this._impl = impl;
	}

	@method({ args: "u32", returns: DocumentsIteratorBatch })
	async next(amount: number): Promise<DocumentsIteratorBatch> {
		if (!this._impl) throw new Error("DocumentsIteratorService not bound");
		return this._impl.next(amount);
	}

	@method({ returns: option("u64") })
	async pending(): Promise<bigint | undefined> {
		if (!this._impl) throw new Error("DocumentsIteratorService not bound");
		return this._impl.pending();
	}

	@method({ returns: "bool" })
	async done(): Promise<boolean> {
		if (!this._impl) throw new Error("DocumentsIteratorService not bound");
		return this._impl.done();
	}

	@method({ returns: "void" })
	async close(): Promise<void> {
		if (!this._impl) throw new Error("DocumentsIteratorService not bound");
		return this._impl.close();
	}
}

@service()
export class DocumentsService {
	private _impl:
		| {
				put: (doc: Bytes) => Promise<void>;
				get: (
					request: DocumentsGetRequest,
				) => Promise<DocumentsIndexResult | undefined>;
				del: (id: IdKey) => Promise<void>;
				iterate: (
					request: DocumentsIterateRequest,
				) => Promise<DocumentsIteratorService>;
				putWithContext?: (
					request: DocumentsPutWithContextRequest,
				) => Promise<void>;
				indexPut?: (request: DocumentsIndexPutRequest) => Promise<void>;
				count?: (request: DocumentsCountRequest) => Promise<bigint>;
				indexSize?: () => Promise<bigint>;
				waitFor?: (request: DocumentsWaitForRequest) => Promise<string[]>;
				indexWaitFor?: (request: DocumentsWaitForRequest) => Promise<string[]>;
				cancelWait?: (requestId: string) => Promise<void>;
				recover?: () => Promise<void>;
				openLog?: () => Promise<SharedLogService>;
				close: () => Promise<void>;
		  }
		| undefined;

	@events(DocumentsChange)
	changes = new EventTarget();

	constructor(impl?: {
		put: (doc: Bytes) => Promise<void>;
		get: (
			request: DocumentsGetRequest,
		) => Promise<DocumentsIndexResult | undefined>;
		del: (id: IdKey) => Promise<void>;
		iterate: (
			request: DocumentsIterateRequest,
		) => Promise<DocumentsIteratorService>;
		putWithContext?: (request: DocumentsPutWithContextRequest) => Promise<void>;
		indexPut?: (request: DocumentsIndexPutRequest) => Promise<void>;
		count?: (request: DocumentsCountRequest) => Promise<bigint>;
		indexSize?: () => Promise<bigint>;
		waitFor?: (request: DocumentsWaitForRequest) => Promise<string[]>;
		indexWaitFor?: (request: DocumentsWaitForRequest) => Promise<string[]>;
		cancelWait?: (requestId: string) => Promise<void>;
		recover?: () => Promise<void>;
		openLog?: () => Promise<SharedLogService>;
		close: () => Promise<void>;
	}) {
		this._impl = impl;
	}

	@method({ args: Bytes, returns: "void" })
	async put(doc: Bytes): Promise<void> {
		if (!this._impl) throw new Error("DocumentsService not bound");
		return this._impl.put(doc);
	}

	@method({ args: DocumentsGetRequest, returns: option(DocumentsIndexResult) })
	async get(
		request: DocumentsGetRequest,
	): Promise<DocumentsIndexResult | undefined> {
		if (!this._impl) throw new Error("DocumentsService not bound");
		return this._impl.get(request);
	}

	@method({ args: IdKey, returns: "void" })
	async del(id: IdKey): Promise<void> {
		if (!this._impl) throw new Error("DocumentsService not bound");
		return this._impl.del(id);
	}

	@method({ args: DocumentsIterateRequest, returns: DocumentsIteratorService })
	async iterate(
		request: DocumentsIterateRequest,
	): Promise<DocumentsIteratorService> {
		if (!this._impl) throw new Error("DocumentsService not bound");
		return this._impl.iterate(request);
	}

	@method({ args: DocumentsPutWithContextRequest, returns: "void" })
	async putWithContext(request: DocumentsPutWithContextRequest): Promise<void> {
		if (!this._impl?.putWithContext) {
			throw new Error("DocumentsService not bound");
		}
		return this._impl.putWithContext(request);
	}

	@method({ args: DocumentsIndexPutRequest, returns: "void" })
	async indexPut(request: DocumentsIndexPutRequest): Promise<void> {
		if (!this._impl?.indexPut) {
			throw new Error("DocumentsService not bound");
		}
		return this._impl.indexPut(request);
	}

	@method({ args: DocumentsCountRequest, returns: "u64" })
	async count(request: DocumentsCountRequest): Promise<bigint> {
		if (!this._impl?.count) throw new Error("DocumentsService not bound");
		return this._impl.count(request);
	}

	@method({ returns: "u64" })
	async indexSize(): Promise<bigint> {
		if (!this._impl?.indexSize) throw new Error("DocumentsService not bound");
		return this._impl.indexSize();
	}

	@method({ args: DocumentsWaitForRequest, returns: vec("string") })
	async waitFor(request: DocumentsWaitForRequest): Promise<string[]> {
		if (!this._impl?.waitFor) throw new Error("DocumentsService not bound");
		return this._impl.waitFor(request);
	}

	@method({ args: DocumentsWaitForRequest, returns: vec("string") })
	async indexWaitFor(request: DocumentsWaitForRequest): Promise<string[]> {
		if (!this._impl?.indexWaitFor)
			throw new Error("DocumentsService not bound");
		return this._impl.indexWaitFor(request);
	}

	@method({ args: "string", returns: "void" })
	async cancelWait(requestId: string): Promise<void> {
		if (!this._impl?.cancelWait) throw new Error("DocumentsService not bound");
		return this._impl.cancelWait(requestId);
	}

	@method({ returns: "void" })
	async recover(): Promise<void> {
		if (!this._impl?.recover) throw new Error("DocumentsService not bound");
		return this._impl.recover();
	}

	@method({ returns: SharedLogService })
	async openLog(): Promise<SharedLogService> {
		if (!this._impl?.openLog) throw new Error("DocumentsService not bound");
		return this._impl.openLog();
	}

	@method({ returns: "void" })
	async close(): Promise<void> {
		if (!this._impl) throw new Error("DocumentsService not bound");
		return this._impl.close();
	}
}

const setStableServiceName = (ctor: Function, name: string): void => {
	try {
		Object.defineProperty(ctor, "name", { value: name, configurable: true });
	} catch {}
};

setStableServiceName(DocumentsIteratorService, "DocumentsIteratorService");
setStableServiceName(DocumentsService, "DocumentsService");
