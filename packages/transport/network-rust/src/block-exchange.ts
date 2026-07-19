// TS adapter for the native block-exchange components (`block_exchange`
// module of the peerbit_wire crate). Implements the `RustBlockExchange`
// surface consumed by `@peerbit/blocks` in rust-core mode: the BlockMessage
// codec, provider resolution/caching and eager-block bookkeeping run in
// wasm. Block bytes stay host-side (the eager index only tracks cids) or
// cross the boundary inside serialized payloads.
import type {
	RustBlockExchange,
	RustBlockProviderCache,
	RustDecodedBlockMessage,
	RustEagerBlockCache,
} from "@peerbit/stream";

const BLOCK_MESSAGE_REQUEST = 0;
const MAX_U32 = 0xffff_ffff;
const MAX_TIMER_DELAY_MS = 0x7fff_ffff;

const assertPositiveIntegerAtMost = (
	value: number,
	max: number,
	name: string,
): void => {
	if (!Number.isSafeInteger(value) || value <= 0 || value > max) {
		throw new RangeError(`${name} must be an integer between 1 and ${max}`);
	}
};

type WasmDecodedBlockMessage = {
	variant: number;
	cid: string;
	bytes_offset: number;
	bytes_length: number;
	free?: () => void;
};

type WasmProviderCacheInstance = {
	get(cid: string, nowMs: number): string[] | undefined;
	remember_provider(cid: string, provider: string, nowMs: number): void;
	remember_hints(cid: string, providers: string[], nowMs: number): void;
	clear(): void;
};

type WasmEagerIndexInstance = {
	add(cid: string, size: number, nowMs: number): string[];
	sweep(nowMs: number): string[];
	contains(cid: string): boolean;
	del(cid: string): void;
	len(): number;
	current_bytes(): number;
	clear(): void;
};

export type BlockExchangeWasmExports = {
	DirectBlockProviderCache: new (
		me: string,
		maxEntries: number,
		ttlMs: number,
		maxProvidersPerCid: number,
	) => WasmProviderCacheInstance;
	DirectBlockEagerIndex: new (
		maxEntries: number,
		maxBytes: number,
		ttlMs: number,
	) => WasmEagerIndexInstance;
	db_decode_block_message(frame: Uint8Array): WasmDecodedBlockMessage;
	db_encode_block_request(cid: string): Uint8Array;
	db_encode_block_response(cid: string, bytes: Uint8Array): Uint8Array;
	db_normalize_provider_hints(
		providers: string[],
		me: string,
		limit: number,
	): string[];
	db_pick_request_batch(
		providers: string[],
		me: string,
		attempt: number,
	): string[];
	db_default_provider_candidates(
		negotiated: string[],
		connected: string[],
		me: string,
	): string[];
};

class RustProviderCacheAdapter implements RustBlockProviderCache {
	private readonly wasm: WasmProviderCacheInstance;

	constructor(
		module: BlockExchangeWasmExports,
		init: {
			me: string;
			maxEntries: number;
			ttlMs: number;
			maxProvidersPerCid: number;
		},
	) {
		this.wasm = new module.DirectBlockProviderCache(
			init.me,
			init.maxEntries,
			init.ttlMs,
			init.maxProvidersPerCid,
		);
	}

	get(cid: string): string[] | undefined {
		return this.wasm.get(cid, Date.now());
	}

	rememberProvider(cid: string, provider: string): void {
		this.wasm.remember_provider(cid, provider, Date.now());
	}

	rememberHints(cid: string, providers: string[]): void {
		this.wasm.remember_hints(cid, providers, Date.now());
	}

	clear(): void {
		this.wasm.clear();
	}
}

/**
 * Eager-block cache with native retention/eviction bookkeeping. The block
 * bytes never cross the wasm boundary: the native index tracks cids and
 * reports evictions, this wrapper keeps the byte buffers.
 */
class RustEagerBlockCacheAdapter implements RustEagerBlockCache {
	private readonly bytes = new Map<string, Uint8Array>();
	private readonly expiries = new Map<string, number>();
	private readonly wasm: WasmEagerIndexInstance;
	private currentBytes = 0;
	private peakEntries = 0;
	private peakBytes = 0;
	private evictions = 0;
	private expirations = 0;
	private expiryTimer?: ReturnType<typeof setTimeout>;

	constructor(
		module: BlockExchangeWasmExports,
		private readonly init: {
			maxEntries: number;
			maxBytes: number;
			ttlMs: number;
		},
	) {
		assertPositiveIntegerAtMost(init.maxEntries, MAX_U32, "maxEntries");
		assertPositiveIntegerAtMost(init.maxBytes, MAX_U32, "maxBytes");
		assertPositiveIntegerAtMost(init.ttlMs, MAX_TIMER_DELAY_MS, "ttlMs");
		this.wasm = new module.DirectBlockEagerIndex(
			init.maxEntries,
			init.maxBytes,
			init.ttlMs,
		);
	}

	add(cid: string, value: Uint8Array): boolean {
		const now = Date.now();
		this.sweep(now);
		if (value.byteLength > this.init.maxBytes) {
			this.scheduleExpiry();
			return false;
		}
		const backing = value.buffer as ArrayBufferLike & {
			readonly resizable?: boolean;
			readonly growable?: boolean;
		};
		const retainedValue =
			value.byteOffset === 0 &&
			backing.byteLength === value.byteLength &&
			backing.resizable !== true &&
			backing.growable !== true
				? value
				: value.slice();

		this.removeHostBytes(cid);
		for (const evicted of this.wasm.add(cid, retainedValue.byteLength, now)) {
			this.removeHostBytes(evicted);
			this.evictions += 1;
		}
		this.bytes.set(cid, retainedValue);
		this.expiries.set(cid, now + this.init.ttlMs);
		this.currentBytes += retainedValue.byteLength;
		this.peakEntries = Math.max(this.peakEntries, this.bytes.size);
		this.peakBytes = Math.max(this.peakBytes, this.currentBytes);
		this.scheduleExpiry();
		return true;
	}

	get(cid: string): Uint8Array | undefined {
		this.sweep(Date.now());
		this.scheduleExpiry();
		if (!this.wasm.contains(cid)) {
			this.removeHostBytes(cid);
			return undefined;
		}
		return this.bytes.get(cid);
	}

	del(cid: string): void {
		this.wasm.del(cid);
		this.removeHostBytes(cid);
		this.scheduleExpiry();
	}

	clear(): void {
		if (this.expiryTimer) {
			clearTimeout(this.expiryTimer);
			this.expiryTimer = undefined;
		}
		this.wasm.clear();
		this.bytes.clear();
		this.expiries.clear();
		this.currentBytes = 0;
	}

	stats() {
		this.sweep(Date.now());
		this.scheduleExpiry();
		return {
			entries: this.wasm.len(),
			bytes: this.wasm.current_bytes(),
			peakEntries: this.peakEntries,
			peakBytes: this.peakBytes,
			evictions: this.evictions,
			expirations: this.expirations,
		};
	}

	private removeHostBytes(cid: string): boolean {
		const bytes = this.bytes.get(cid);
		if (!bytes) return false;
		this.bytes.delete(cid);
		this.expiries.delete(cid);
		this.currentBytes -= bytes.byteLength;
		return true;
	}

	private sweep(now: number): void {
		for (const expired of this.wasm.sweep(now)) {
			if (this.removeHostBytes(expired)) {
				this.expirations += 1;
			}
		}
	}

	private scheduleExpiry(): void {
		if (this.expiryTimer) {
			clearTimeout(this.expiryTimer);
			this.expiryTimer = undefined;
		}
		const expiresAt = this.expiries.values().next().value as number | undefined;
		if (expiresAt == null) return;
		this.expiryTimer = setTimeout(
			() => {
				this.expiryTimer = undefined;
				this.sweep(Date.now());
				this.scheduleExpiry();
			},
			Math.max(0, expiresAt - Date.now()),
		);
		if (typeof this.expiryTimer === "object" && "unref" in this.expiryTimer) {
			this.expiryTimer.unref();
		}
	}
}

export const createRustBlockExchange = (
	wasm: BlockExchangeWasmExports,
): RustBlockExchange => ({
	encodeBlockRequest: (cid) => wasm.db_encode_block_request(cid),
	encodeBlockResponse: (cid, bytes) =>
		wasm.db_encode_block_response(cid, bytes),
	decodeBlockMessage: (payload): RustDecodedBlockMessage => {
		const decoded = wasm.db_decode_block_message(payload);
		try {
			if (decoded.variant === BLOCK_MESSAGE_REQUEST) {
				return { type: "request", cid: decoded.cid };
			}
			return {
				type: "response",
				cid: decoded.cid,
				bytes: payload.subarray(
					decoded.bytes_offset,
					decoded.bytes_offset + decoded.bytes_length,
				),
			};
		} finally {
			decoded.free?.();
		}
	},
	normalizeProviderHints: (providers, me, limit) =>
		wasm.db_normalize_provider_hints(providers, me, limit),
	pickRequestBatch: (providers, me, attempt) =>
		wasm.db_pick_request_batch(providers, me, attempt),
	defaultProviderCandidates: (negotiated, connected, me) =>
		wasm.db_default_provider_candidates(negotiated, connected, me),
	createProviderCache: (init) => new RustProviderCacheAdapter(wasm, init),
	createEagerCache: (init) => new RustEagerBlockCacheAdapter(wasm, init),
});
