import { field, fixedArray, option, variant, vec } from "@dao-xyz/borsh";
import { events, method, service } from "@dao-xyz/borsh-rpc";
import { PublicSignKey } from "@peerbit/crypto";
import { IdKey, Query, Sort } from "@peerbit/indexer-interface";

@variant("shared_log_bytes")
export class SharedLogBytes {
	@field({ type: Uint8Array })
	value: Uint8Array;

	constructor(properties?: { value?: Uint8Array }) {
		this.value = properties?.value ?? new Uint8Array();
	}
}

@variant("shared_log_event")
export class SharedLogEvent {
	@field({ type: PublicSignKey })
	publicKey: PublicSignKey;

	constructor(properties: { publicKey: PublicSignKey }) {
		this.publicKey = properties.publicKey;
	}
}

@variant("open_shared_log")
export class OpenSharedLogRequest {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array;

	constructor(properties?: { id?: Uint8Array }) {
		this.id = properties?.id ?? new Uint8Array(32);
	}
}

@variant("shared_log_replication_range")
export class SharedLogReplicationRange {
	@field({ type: option(Uint8Array) })
	id?: Uint8Array;

	@field({ type: option("f64") })
	factor?: number;

	@field({ type: option("string") })
	factorMode?: "all" | "right";

	@field({ type: option("f64") })
	offset?: number;

	@field({ type: option("bool") })
	normalized?: boolean;

	@field({ type: option("bool") })
	strict?: boolean;

	constructor(properties?: {
		id?: Uint8Array;
		factor?: number;
		factorMode?: "all" | "right";
		offset?: number;
		normalized?: boolean;
		strict?: boolean;
	}) {
		this.id = properties?.id;
		this.factor = properties?.factor;
		this.factorMode = properties?.factorMode;
		this.offset = properties?.offset;
		this.normalized = properties?.normalized;
		this.strict = properties?.strict;
	}
}

export abstract class SharedLogReplicateValue {}

@variant("shared_log_replicate_bool")
export class SharedLogReplicateBool extends SharedLogReplicateValue {
	@field({ type: "bool" })
	value: boolean;

	constructor(value: boolean) {
		super();
		this.value = value;
	}
}

@variant("shared_log_replicate_factor")
export class SharedLogReplicateFactor extends SharedLogReplicateValue {
	@field({ type: "f64" })
	factor: number;

	constructor(factor: number) {
		super();
		this.factor = factor;
	}
}

@variant("shared_log_replicate_fixed")
export class SharedLogReplicateFixed extends SharedLogReplicateValue {
	@field({ type: SharedLogReplicationRange })
	range: SharedLogReplicationRange;

	constructor(range: SharedLogReplicationRange) {
		super();
		this.range = range;
	}
}

@variant("shared_log_replicate_fixed_list")
export class SharedLogReplicateFixedList extends SharedLogReplicateValue {
	@field({ type: vec(SharedLogReplicationRange) })
	ranges: SharedLogReplicationRange[];

	constructor(ranges: SharedLogReplicationRange[]) {
		super();
		this.ranges = ranges;
	}
}

@variant("shared_log_replicate_request")
export class SharedLogReplicateRequest {
	@field({ type: option(SharedLogReplicateValue) })
	value?: SharedLogReplicateValue;

	@field({ type: option("bool") })
	reset?: boolean;

	@field({ type: option("bool") })
	checkDuplicates?: boolean;

	@field({ type: option("bool") })
	rebalance?: boolean;

	@field({ type: option("bool") })
	mergeSegments?: boolean;

	constructor(properties?: {
		value?: SharedLogReplicateValue;
		reset?: boolean;
		checkDuplicates?: boolean;
		rebalance?: boolean;
		mergeSegments?: boolean;
	}) {
		this.value = properties?.value;
		this.reset = properties?.reset;
		this.checkDuplicates = properties?.checkDuplicates;
		this.rebalance = properties?.rebalance;
		this.mergeSegments = properties?.mergeSegments;
	}
}

@variant("shared_log_unreplicate_request")
export class SharedLogUnreplicateRequest {
	@field({ type: vec(Uint8Array) })
	ids: Uint8Array[];

	constructor(properties?: { ids?: Uint8Array[] }) {
		this.ids = properties?.ids ?? [];
	}
}

@variant("shared_log_replication_iterate_request")
export class SharedLogReplicationIterateRequest {
	@field({ type: vec(Query) })
	query: Query[];

	@field({ type: vec(Sort) })
	sort: Sort[];

	constructor(properties?: { query?: Query[]; sort?: Sort[] }) {
		this.query = properties?.query ?? [];
		this.sort = properties?.sort ?? [];
	}
}

@variant("shared_log_replication_count_request")
export class SharedLogReplicationCountRequest {
	@field({ type: vec(Query) })
	query: Query[];

	constructor(properties?: { query?: Query[] }) {
		this.query = properties?.query ?? [];
	}
}

@variant("shared_log_replication_index_result")
export class SharedLogReplicationIndexResult {
	@field({ type: IdKey })
	id: IdKey;

	@field({ type: SharedLogBytes })
	value: SharedLogBytes;

	constructor(properties: { id: IdKey; value: SharedLogBytes }) {
		this.id = properties.id;
		this.value = properties.value;
	}
}

@variant("shared_log_entries_batch")
export class SharedLogEntriesBatch {
	@field({ type: vec(SharedLogBytes) })
	entries: SharedLogBytes[];

	@field({ type: "bool" })
	done: boolean;

	constructor(properties: { entries?: SharedLogBytes[]; done?: boolean }) {
		this.entries = properties.entries ?? [];
		this.done = properties.done ?? false;
	}
}

@variant("shared_log_replication_batch")
export class SharedLogReplicationBatch {
	@field({ type: vec(SharedLogReplicationIndexResult) })
	results: SharedLogReplicationIndexResult[];

	@field({ type: "bool" })
	done: boolean;

	constructor(properties: {
		results?: SharedLogReplicationIndexResult[];
		done?: boolean;
	}) {
		this.results = properties.results ?? [];
		this.done = properties.done ?? false;
	}
}

@variant("shared_log_wait_for_replicator")
export class SharedLogWaitForReplicatorRequest {
	@field({ type: PublicSignKey })
	publicKey: PublicSignKey;

	@field({ type: option("bool") })
	eager?: boolean;

	@field({ type: option("u32") })
	timeoutMs?: number;

	@field({ type: option("u32") })
	roleAgeMs?: number;

	@field({ type: option("string") })
	requestId?: string;

	constructor(properties: {
		publicKey: PublicSignKey;
		eager?: boolean;
		timeoutMs?: number;
		roleAgeMs?: number;
		requestId?: string;
	}) {
		this.publicKey = properties.publicKey;
		this.eager = properties.eager;
		this.timeoutMs = properties.timeoutMs;
		this.roleAgeMs = properties.roleAgeMs;
		this.requestId = properties.requestId;
	}
}

@variant("shared_log_wait_for_replicators")
export class SharedLogWaitForReplicatorsRequest {
	@field({ type: option("u32") })
	timeoutMs?: number;

	@field({ type: option("u32") })
	roleAgeMs?: number;

	@field({ type: option("f64") })
	coverageThreshold?: number;

	@field({ type: option("bool") })
	waitForNewPeers?: boolean;

	@field({ type: option("string") })
	requestId?: string;

	constructor(properties?: {
		timeoutMs?: number;
		roleAgeMs?: number;
		coverageThreshold?: number;
		waitForNewPeers?: boolean;
		requestId?: string;
	}) {
		this.timeoutMs = properties?.timeoutMs;
		this.roleAgeMs = properties?.roleAgeMs;
		this.coverageThreshold = properties?.coverageThreshold;
		this.waitForNewPeers = properties?.waitForNewPeers;
		this.requestId = properties?.requestId;
	}
}

@variant("shared_log_coverage_request")
export class SharedLogCoverageRequest {
	@field({ type: option("f64") })
	start?: number;

	@field({ type: option("f64") })
	end?: number;

	@field({ type: option("u32") })
	roleAgeMs?: number;

	constructor(properties?: {
		start?: number;
		end?: number;
		roleAgeMs?: number;
	}) {
		this.start = properties?.start;
		this.end = properties?.end;
		this.roleAgeMs = properties?.roleAgeMs;
	}
}

@service()
export class SharedLogEntriesIteratorService {
	private _impl:
		| {
				next: (amount: number) => Promise<SharedLogEntriesBatch>;
				pending: () => Promise<bigint | undefined>;
				done: () => Promise<boolean>;
				close: () => Promise<void>;
		  }
		| undefined;

	constructor(impl?: {
		next: (amount: number) => Promise<SharedLogEntriesBatch>;
		pending: () => Promise<bigint | undefined>;
		done: () => Promise<boolean>;
		close: () => Promise<void>;
	}) {
		this._impl = impl;
	}

	@method({ args: "u32", returns: SharedLogEntriesBatch })
	async next(amount: number): Promise<SharedLogEntriesBatch> {
		if (!this._impl)
			throw new Error("SharedLogEntriesIteratorService not bound");
		return this._impl.next(amount);
	}

	@method({ returns: option("u64") })
	async pending(): Promise<bigint | undefined> {
		if (!this._impl)
			throw new Error("SharedLogEntriesIteratorService not bound");
		return this._impl.pending();
	}

	@method({ returns: "bool" })
	async done(): Promise<boolean> {
		if (!this._impl)
			throw new Error("SharedLogEntriesIteratorService not bound");
		return this._impl.done();
	}

	@method({ returns: "void" })
	async close(): Promise<void> {
		if (!this._impl)
			throw new Error("SharedLogEntriesIteratorService not bound");
		return this._impl.close();
	}
}

@service()
export class SharedLogReplicationIteratorService {
	private _impl:
		| {
				next: (amount: number) => Promise<SharedLogReplicationBatch>;
				pending: () => Promise<bigint | undefined>;
				done: () => Promise<boolean>;
				close: () => Promise<void>;
		  }
		| undefined;

	constructor(impl?: {
		next: (amount: number) => Promise<SharedLogReplicationBatch>;
		pending: () => Promise<bigint | undefined>;
		done: () => Promise<boolean>;
		close: () => Promise<void>;
	}) {
		this._impl = impl;
	}

	@method({ args: "u32", returns: SharedLogReplicationBatch })
	async next(amount: number): Promise<SharedLogReplicationBatch> {
		if (!this._impl)
			throw new Error("SharedLogReplicationIteratorService not bound");
		return this._impl.next(amount);
	}

	@method({ returns: option("u64") })
	async pending(): Promise<bigint | undefined> {
		if (!this._impl)
			throw new Error("SharedLogReplicationIteratorService not bound");
		return this._impl.pending();
	}

	@method({ returns: "bool" })
	async done(): Promise<boolean> {
		if (!this._impl)
			throw new Error("SharedLogReplicationIteratorService not bound");
		return this._impl.done();
	}

	@method({ returns: "void" })
	async close(): Promise<void> {
		if (!this._impl)
			throw new Error("SharedLogReplicationIteratorService not bound");
		return this._impl.close();
	}
}

@service()
export class SharedLogService {
	private _impl:
		| {
				logGet: (hash: string) => Promise<SharedLogBytes | undefined>;
				logHas: (hash: string) => Promise<boolean>;
				logToArray: () => Promise<SharedLogBytes[]>;
				logGetHeads: () => Promise<SharedLogEntriesIteratorService>;
				logLength: () => Promise<bigint>;
				logBlockHas?: (hash: string) => Promise<boolean>;
				replicationIterate: (
					request: SharedLogReplicationIterateRequest,
				) => Promise<SharedLogReplicationIteratorService>;
				replicationCount: (
					request: SharedLogReplicationCountRequest,
				) => Promise<bigint>;
				getReplicators: () => Promise<string[]>;
				waitForReplicator: (
					request: SharedLogWaitForReplicatorRequest,
				) => Promise<void>;
				waitForReplicators: (
					request?: SharedLogWaitForReplicatorsRequest,
				) => Promise<void>;
				cancelWait?: (requestId: string) => Promise<void>;
				replicate: (request?: SharedLogReplicateRequest) => Promise<void>;
				unreplicate: (request?: SharedLogUnreplicateRequest) => Promise<void>;
				calculateCoverage: (
					request?: SharedLogCoverageRequest,
				) => Promise<number>;
				getMyReplicationSegments: () => Promise<SharedLogBytes[]>;
				getAllReplicationSegments: () => Promise<SharedLogBytes[]>;
				resolution: () => Promise<string>;
				publicKey: () => Promise<PublicSignKey>;
				close: () => Promise<void>;
		  }
		| undefined;

	@events(SharedLogEvent)
	events = new EventTarget();

	constructor(impl?: {
		logGet: (hash: string) => Promise<SharedLogBytes | undefined>;
		logHas: (hash: string) => Promise<boolean>;
		logToArray: () => Promise<SharedLogBytes[]>;
		logGetHeads: () => Promise<SharedLogEntriesIteratorService>;
		logLength: () => Promise<bigint>;
		logBlockHas?: (hash: string) => Promise<boolean>;
		replicationIterate: (
			request: SharedLogReplicationIterateRequest,
		) => Promise<SharedLogReplicationIteratorService>;
		replicationCount: (
			request: SharedLogReplicationCountRequest,
		) => Promise<bigint>;
		getReplicators: () => Promise<string[]>;
		waitForReplicator: (
			request: SharedLogWaitForReplicatorRequest,
		) => Promise<void>;
		waitForReplicators: (
			request?: SharedLogWaitForReplicatorsRequest,
		) => Promise<void>;
		cancelWait?: (requestId: string) => Promise<void>;
		replicate: (request?: SharedLogReplicateRequest) => Promise<void>;
		unreplicate: (request?: SharedLogUnreplicateRequest) => Promise<void>;
		calculateCoverage: (request?: SharedLogCoverageRequest) => Promise<number>;
		getMyReplicationSegments: () => Promise<SharedLogBytes[]>;
		getAllReplicationSegments: () => Promise<SharedLogBytes[]>;
		resolution: () => Promise<string>;
		publicKey: () => Promise<PublicSignKey>;
		close: () => Promise<void>;
	}) {
		this._impl = impl;
	}

	@method({ args: "string", returns: option(SharedLogBytes) })
	async logGet(hash: string): Promise<SharedLogBytes | undefined> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.logGet(hash);
	}

	@method({ args: "string", returns: "bool" })
	async logHas(hash: string): Promise<boolean> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.logHas(hash);
	}

	@method({ returns: vec(SharedLogBytes) })
	async logToArray(): Promise<SharedLogBytes[]> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.logToArray();
	}

	@method({ returns: SharedLogEntriesIteratorService })
	async logGetHeads(): Promise<SharedLogEntriesIteratorService> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.logGetHeads();
	}

	@method({ returns: "u64" })
	async logLength(): Promise<bigint> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.logLength();
	}

	@method({ args: "string", returns: "bool" })
	async logBlockHas(hash: string): Promise<boolean> {
		if (!this._impl?.logBlockHas) throw new Error("SharedLogService not bound");
		return this._impl.logBlockHas(hash);
	}

	@method({
		args: SharedLogReplicationIterateRequest,
		returns: SharedLogReplicationIteratorService,
	})
	async replicationIterate(
		request: SharedLogReplicationIterateRequest,
	): Promise<SharedLogReplicationIteratorService> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.replicationIterate(request);
	}

	@method({ args: SharedLogReplicationCountRequest, returns: "u64" })
	async replicationCount(
		request: SharedLogReplicationCountRequest,
	): Promise<bigint> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.replicationCount(request);
	}

	@method({ returns: vec("string") })
	async getReplicators(): Promise<string[]> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.getReplicators();
	}

	@method({ args: SharedLogWaitForReplicatorRequest, returns: "void" })
	async waitForReplicator(
		request: SharedLogWaitForReplicatorRequest,
	): Promise<void> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.waitForReplicator(request);
	}

	@method({ args: option(SharedLogWaitForReplicatorsRequest), returns: "void" })
	async waitForReplicators(
		request?: SharedLogWaitForReplicatorsRequest,
	): Promise<void> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.waitForReplicators(request);
	}

	@method({ args: "string", returns: "void" })
	async cancelWait(requestId: string): Promise<void> {
		if (!this._impl?.cancelWait) throw new Error("SharedLogService not bound");
		return this._impl.cancelWait(requestId);
	}

	@method({ args: option(SharedLogReplicateRequest), returns: "void" })
	async replicate(request?: SharedLogReplicateRequest): Promise<void> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.replicate(request);
	}

	@method({ args: option(SharedLogUnreplicateRequest), returns: "void" })
	async unreplicate(request?: SharedLogUnreplicateRequest): Promise<void> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.unreplicate(request);
	}

	@method({ args: option(SharedLogCoverageRequest), returns: "f64" })
	async calculateCoverage(request?: SharedLogCoverageRequest): Promise<number> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.calculateCoverage(request);
	}

	@method({ returns: vec(SharedLogBytes) })
	async getMyReplicationSegments(): Promise<SharedLogBytes[]> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.getMyReplicationSegments();
	}

	@method({ returns: vec(SharedLogBytes) })
	async getAllReplicationSegments(): Promise<SharedLogBytes[]> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.getAllReplicationSegments();
	}

	@method({ returns: "string" })
	async resolution(): Promise<string> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.resolution();
	}

	@method({ returns: PublicSignKey })
	async publicKey(): Promise<PublicSignKey> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.publicKey();
	}

	@method({ returns: "void" })
	async close(): Promise<void> {
		if (!this._impl) throw new Error("SharedLogService not bound");
		return this._impl.close();
	}
}
