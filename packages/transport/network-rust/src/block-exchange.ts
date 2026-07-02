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
	add(cid: string, nowMs: number): string[];
	sweep(nowMs: number): string[];
	contains(cid: string): boolean;
	del(cid: string): void;
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
		max: number,
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
	private readonly wasm: WasmEagerIndexInstance;

	constructor(
		module: BlockExchangeWasmExports,
		init: { max: number; ttl: number },
	) {
		this.wasm = new module.DirectBlockEagerIndex(init.max, init.ttl);
	}

	add(cid: string, value: Uint8Array): void {
		for (const evicted of this.wasm.add(cid, Date.now())) {
			this.bytes.delete(evicted);
		}
		this.bytes.set(cid, value);
	}

	get(cid: string): Uint8Array | undefined {
		for (const evicted of this.wasm.sweep(Date.now())) {
			this.bytes.delete(evicted);
		}
		if (!this.wasm.contains(cid)) {
			this.bytes.delete(cid);
			return undefined;
		}
		return this.bytes.get(cid);
	}

	del(cid: string): void {
		this.wasm.del(cid);
		this.bytes.delete(cid);
	}

	clear(): void {
		this.wasm.clear();
		this.bytes.clear();
	}
}

export const createRustBlockExchange = (
	wasm: BlockExchangeWasmExports,
): RustBlockExchange => ({
	encodeBlockRequest: (cid) => wasm.db_encode_block_request(cid),
	encodeBlockResponse: (cid, bytes) => wasm.db_encode_block_response(cid, bytes),
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
