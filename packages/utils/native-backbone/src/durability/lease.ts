export type NativeDurabilityFence = Readonly<{
	epoch: bigint;
	ownerId: string;
	domainId: string;
}>;

export const NATIVE_DURABILITY_MAX_U64 = (1n << 64n) - 1n;
export const NATIVE_DURABILITY_MAX_WRITER_ID_BYTES = 1024;

/**
 * Exclusive, crash-released ownership capability for one native durability
 * directory.
 *
 * This capability deliberately has no persistent fence. It is suitable only
 * when the guarded storage supplies its own durable generation or other stale
 * writer protection. `assertHeld()` is diagnostic only; callers must wrap the
 * complete asynchronous mutation and its durability barrier in
 * `runWhileHeld()`.
 */
export interface NativeDurabilityLock {
	assertHeld(): Promise<void>;
	/**
	 * Keep the lock alive for the complete asynchronous operation. `close()`
	 * starts rejecting new operations immediately and waits for operations that
	 * already entered this guard before releasing the underlying OS lock.
	 */
	runWhileHeld<T>(operation: () => Promise<T>): Promise<T>;
	close(): Promise<void>;
}

/**
 * Exclusive ownership capability for one native durability directory.
 *
 * Implementations keep the underlying ownership primitive alive until
 * `close()` completes. Storage writers must wrap the complete asynchronous
 * mutation and its barrier in `runWhileHeld()` and persist the accompanying
 * fence with their records. `assertHeld()` is diagnostic only; it is not a
 * lifecycle guard for an operation that later awaits I/O.
 */
export interface NativeDurabilityLease extends NativeDurabilityLock {
	readonly fence: NativeDurabilityFence;
}

export class NativeDurabilityLockUnavailableError extends Error {
	readonly code = "NATIVE_DURABILITY_LOCK_UNAVAILABLE";

	constructor(
		readonly directory: string,
		options?: { cause?: unknown },
	) {
		super(`Native durability directory is already open: ${directory}`, options);
		this.name = "NativeDurabilityLockUnavailableError";
	}
}

export class NativeDurabilityLockClosedError extends Error {
	readonly code = "NATIVE_DURABILITY_LOCK_CLOSED";

	constructor(readonly directory: string) {
		super(`Native durability lock is no longer held: ${directory}`);
		this.name = "NativeDurabilityLockClosedError";
	}
}

export class NativeDurabilityLockStateError extends Error {
	readonly code = "NATIVE_DURABILITY_LOCK_STATE_INVALID";

	constructor(
		readonly directory: string,
		message: string,
		options?: { cause?: unknown },
	) {
		super(message, options);
		this.name = "NativeDurabilityLockStateError";
	}
}

export class NativeDurabilityLockDirectorySyncError extends Error {
	readonly code = "NATIVE_DURABILITY_LOCK_DIRECTORY_SYNC_FAILED";

	constructor(
		readonly directory: string,
		options?: { cause?: unknown },
	) {
		super(
			`Native durability requires directory fsync support: ${directory}`,
			options,
		);
		this.name = "NativeDurabilityLockDirectorySyncError";
	}
}

export class NativeDurabilityLeaseUnavailableError extends Error {
	readonly code = "NATIVE_DURABILITY_LEASE_UNAVAILABLE";

	constructor(
		readonly directory: string,
		options?: { cause?: unknown },
	) {
		super(`Native durability directory is already open: ${directory}`, options);
		this.name = "NativeDurabilityLeaseUnavailableError";
	}
}

export class NativeDurabilityLeaseClosedError extends Error {
	readonly code = "NATIVE_DURABILITY_LEASE_CLOSED";

	constructor(readonly fence: NativeDurabilityFence) {
		super(`Native durability lease is no longer held: ${fence.domainId}`);
		this.name = "NativeDurabilityLeaseClosedError";
	}
}

export class NativeDurabilityLeaseStateError extends Error {
	readonly code = "NATIVE_DURABILITY_LEASE_STATE_INVALID";

	constructor(
		readonly directory: string,
		message: string,
		options?: { cause?: unknown },
	) {
		super(message, options);
		this.name = "NativeDurabilityLeaseStateError";
	}
}

export class NativeDurabilityLeaseDirectorySyncError extends Error {
	readonly code = "NATIVE_DURABILITY_LEASE_DIRECTORY_SYNC_FAILED";

	constructor(
		readonly directory: string,
		options?: { cause?: unknown },
	) {
		super(
			`Native durability requires directory fsync support: ${directory}`,
			options,
		);
		this.name = "NativeDurabilityLeaseDirectorySyncError";
	}
}

export class NativeDurabilityFenceExhaustedError extends Error {
	readonly code = "NATIVE_DURABILITY_FENCE_EXHAUSTED";

	constructor(readonly directory: string) {
		super(`Native durability fence epoch is exhausted: ${directory}`);
		this.name = "NativeDurabilityFenceExhaustedError";
	}
}
