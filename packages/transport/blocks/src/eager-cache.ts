import type {
	RustEagerBlockCache,
	RustEagerBlockCacheStats,
} from "@peerbit/stream";

/** Recipient-side eager-response limits. `cacheSize` is kept for API compatibility. */
export type EagerBlocksOptions = {
	/** Maximum number of validated blocks retained. */
	cacheSize?: number;
	/** Maximum combined byte length of retained blocks. */
	maxBytes?: number;
	/** Maximum byte length of one unsolicited block. */
	maxBlockBytes?: number;
	/** Time-to-live for a validated eager block, in milliseconds. */
	ttlMs?: number;
	/** Maximum number of simultaneous eager integrity checks. */
	validationConcurrency?: number;
	/** Maximum copied bytes waiting for, or undergoing, integrity checks. */
	maxPendingBytes?: number;
	/** Maximum blocks waiting for, or undergoing, integrity checks. */
	maxPendingEntries?: number;
};

export type EagerBlocksSetting = boolean | EagerBlocksOptions;

export const DEFAULT_EAGER_BLOCK_CACHE_ENTRIES = 1_000;
export const DEFAULT_EAGER_BLOCK_CACHE_BYTES = 32 * 1024 * 1024;
export const DEFAULT_EAGER_BLOCK_MAX_BYTES = 10 * 1024 * 1024;
export const DEFAULT_EAGER_BLOCK_TTL_MS = 10_000;
export const DEFAULT_EAGER_BLOCK_VALIDATION_CONCURRENCY = 2;
export const DEFAULT_EAGER_BLOCK_PENDING_BYTES = 20 * 1024 * 1024;
export const DEFAULT_EAGER_BLOCK_PENDING_ENTRIES = 64;
export const MAX_EAGER_BLOCK_CID_LENGTH = 256;
export const MAX_EAGER_BLOCK_TTL_MS = 0x7fff_ffff;

export type NormalizedEagerBlocksOptions = {
	maxEntries: number;
	maxBytes: number;
	maxBlockBytes: number;
	ttlMs: number;
	validationConcurrency: number;
	maxPendingBytes: number;
	maxPendingEntries: number;
};

const positiveSafeInteger = (value: number, name: string): number => {
	if (!Number.isSafeInteger(value) || value <= 0) {
		throw new RangeError(`${name} must be a positive safe integer`);
	}
	return value;
};

const positiveUint32 = (value: number, name: string): number => {
	positiveSafeInteger(value, name);
	if (value > 0xffff_ffff) {
		throw new RangeError(`${name} must be at most 4294967295`);
	}
	return value;
};

const positiveTimerDelay = (value: number, name: string): number => {
	positiveSafeInteger(value, name);
	if (value > MAX_EAGER_BLOCK_TTL_MS) {
		throw new RangeError(`${name} must be at most ${MAX_EAGER_BLOCK_TTL_MS}`);
	}
	return value;
};

export const normalizeEagerBlocksOptions = (
	setting: Exclude<EagerBlocksSetting, false>,
): NormalizedEagerBlocksOptions => {
	const options = typeof setting === "boolean" ? {} : setting;
	const maxEntries = positiveUint32(
		options.cacheSize ?? DEFAULT_EAGER_BLOCK_CACHE_ENTRIES,
		"eagerBlocks.cacheSize",
	);
	const maxBytes = positiveUint32(
		options.maxBytes ?? DEFAULT_EAGER_BLOCK_CACHE_BYTES,
		"eagerBlocks.maxBytes",
	);
	const maxBlockBytes = positiveUint32(
		options.maxBlockBytes ?? DEFAULT_EAGER_BLOCK_MAX_BYTES,
		"eagerBlocks.maxBlockBytes",
	);
	const ttlMs = positiveTimerDelay(
		options.ttlMs ?? DEFAULT_EAGER_BLOCK_TTL_MS,
		"eagerBlocks.ttlMs",
	);
	const validationConcurrency = positiveSafeInteger(
		options.validationConcurrency ?? DEFAULT_EAGER_BLOCK_VALIDATION_CONCURRENCY,
		"eagerBlocks.validationConcurrency",
	);
	const maxPendingBytes = positiveSafeInteger(
		options.maxPendingBytes ?? DEFAULT_EAGER_BLOCK_PENDING_BYTES,
		"eagerBlocks.maxPendingBytes",
	);
	const maxPendingEntries = positiveSafeInteger(
		options.maxPendingEntries ?? DEFAULT_EAGER_BLOCK_PENDING_ENTRIES,
		"eagerBlocks.maxPendingEntries",
	);
	return {
		maxEntries,
		maxBytes,
		maxBlockBytes,
		ttlMs,
		validationConcurrency,
		maxPendingBytes,
		maxPendingEntries,
	};
};

export interface EagerBlockCache extends RustEagerBlockCache {}

type CacheEntry = {
	bytes: Uint8Array;
	expiresAt: number;
};

/**
 * Exact FIFO/TTL cache used when the native block-exchange core is disabled.
 * Deletion releases the byte buffer immediately and both entry and byte
 * accounting remain exact across replacement and delete/re-add cycles.
 */
export class BoundedEagerBlockCache implements EagerBlockCache {
	private readonly entries = new Map<string, CacheEntry>();
	private currentBytes = 0;
	private peakEntries = 0;
	private peakBytes = 0;
	private evictions = 0;
	private expirations = 0;
	private expiryTimer?: ReturnType<typeof setTimeout>;

	constructor(
		private readonly options: {
			maxEntries: number;
			maxBytes: number;
			ttlMs: number;
		},
	) {
		positiveUint32(options.maxEntries, "maxEntries");
		positiveUint32(options.maxBytes, "maxBytes");
		positiveTimerDelay(options.ttlMs, "ttlMs");
	}

	add(cid: string, bytes: Uint8Array): boolean {
		this.sweepExpired(Date.now());
		if (bytes.byteLength > this.options.maxBytes) {
			return false;
		}
		const backing = bytes.buffer as ArrayBufferLike & {
			readonly resizable?: boolean;
			readonly growable?: boolean;
		};
		const retainedBytes =
			bytes.byteOffset === 0 &&
			backing.byteLength === bytes.byteLength &&
			backing.resizable !== true &&
			backing.growable !== true
				? bytes
				: bytes.slice();

		this.remove(cid);
		while (
			this.entries.size >= this.options.maxEntries ||
			this.currentBytes + retainedBytes.byteLength > this.options.maxBytes
		) {
			const oldest = this.entries.keys().next().value as string | undefined;
			if (oldest == null) break;
			this.remove(oldest);
			this.evictions += 1;
		}

		this.entries.set(cid, {
			bytes: retainedBytes,
			expiresAt: Date.now() + this.options.ttlMs,
		});
		this.currentBytes += retainedBytes.byteLength;
		this.peakEntries = Math.max(this.peakEntries, this.entries.size);
		this.peakBytes = Math.max(this.peakBytes, this.currentBytes);
		this.scheduleExpiry();
		return true;
	}

	get(cid: string): Uint8Array | undefined {
		this.sweepExpired(Date.now());
		return this.entries.get(cid)?.bytes;
	}

	del(cid: string): void {
		if (this.remove(cid)) {
			this.scheduleExpiry();
		}
	}

	clear(): void {
		if (this.expiryTimer) {
			clearTimeout(this.expiryTimer);
			this.expiryTimer = undefined;
		}
		this.entries.clear();
		this.currentBytes = 0;
	}

	stats(): RustEagerBlockCacheStats {
		this.sweepExpired(Date.now());
		return {
			entries: this.entries.size,
			bytes: this.currentBytes,
			peakEntries: this.peakEntries,
			peakBytes: this.peakBytes,
			evictions: this.evictions,
			expirations: this.expirations,
		};
	}

	private remove(cid: string): boolean {
		const entry = this.entries.get(cid);
		if (!entry) return false;
		this.entries.delete(cid);
		this.currentBytes -= entry.bytes.byteLength;
		return true;
	}

	private sweepExpired(now: number): void {
		let expired = 0;
		for (const [cid, entry] of this.entries) {
			if (entry.expiresAt > now) break;
			this.remove(cid);
			expired += 1;
		}
		if (expired > 0) {
			this.expirations += expired;
			this.scheduleExpiry();
		}
	}

	private scheduleExpiry(): void {
		if (this.expiryTimer) {
			clearTimeout(this.expiryTimer);
			this.expiryTimer = undefined;
		}
		const oldest = this.entries.values().next().value as CacheEntry | undefined;
		if (!oldest) return;
		this.expiryTimer = setTimeout(
			() => {
				this.expiryTimer = undefined;
				this.sweepExpired(Date.now());
				this.scheduleExpiry();
			},
			Math.max(0, oldest.expiresAt - Date.now()),
		);
		if (typeof this.expiryTimer === "object" && "unref" in this.expiryTimer) {
			this.expiryTimer.unref();
		}
	}
}
