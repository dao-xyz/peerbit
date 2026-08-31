import type { AnyBlockStore } from "@peerbit/blocks";
import { logger as loggerFn } from "@peerbit/logger";
import type { NativePeerbitBackbone } from "@peerbit/native-backbone";
import { NativeDurableCommitError } from "./errors.js";

const warn = loggerFn("peerbit:shared-log").newScope("warn");

type MaybePromise<T> = T | Promise<T>;

const isPromiseLike = <T>(value: MaybePromise<T>): value is Promise<T> =>
	!!value && typeof (value as Promise<T>).then === "function";

/** The native backbone's in-wasm-memory block store. */
type NativeBackboneBlocks = NativePeerbitBackbone["blocks"];

type NativeCommitOwnershipToken = {
	id: number;
	rows: Map<
		string,
		{
			generation: number;
			durableExistedBefore?: boolean;
			shared: boolean;
		}
	>;
};

/**
 * Write-through block store bridging the native backbone's in-wasm-memory block
 * store to a durable per-program {@link AnyBlockStore}.
 *
 * WHY: when the native backbone is active the log's entry blocks live only in
 * the native wasm block map (`NativeBackboneBlockStore.persisted() === false`).
 * On a restart that map is empty, so the native graph cannot reload heads the
 * durable heads index still lists ("Failed to load entry from head"). This
 * wrapper mirrors every write into the same durable `blocks` sublevel the
 * non-native path uses. On a native miss, reads fall through to durable storage
 * and lazily repopulate the wasm map so the native graph can walk the DAG again.
 *
 * The native store stays the authoritative hot store the native graph reads
 * from: reads hit native first and only fall back to durable (repopulating
 * native on a hit so subsequent native-graph reads succeed).
 *
 * METHOD SURFACE (see #1006): `RemoteBlocks` and the log feature-detect the
 * optional batch methods (`putMany`/`putKnown`/`putKnownMany`/
 * `putKnownManyColumns`/`rmMany`). To keep the receive-fusion / columnar fast
 * paths engaged this wrapper preserves the native store's optional write
 * methods — including `putKnownManyColumns`, which `AnyBlockStore` does not have
 * — and delegates each. It adds only local durability/trim coordination hooks
 * consumed by `RemoteBlocks` and the log; protocol/native-handle capabilities
 * such as `getBlockResponsePayload`/`getNativeLogBlockStoreHandle` stay absent,
 * so their optional-chained probes keep the existing fallback behavior.
 */
export class NativeBackboneWriteThroughBlockStore {
	constructor(
		private readonly native: NativeBackboneBlocks,
		private readonly durable: AnyBlockStore,
	) {}

	// A durable mirror write that cannot be awaited at its call site (the
	// columnar putKnownManyColumns fast path must return a synchronous string[]
	// because RemoteBlocks.putKnownManyColumns treats the result as sync) is
	// tracked here instead of being silently `void`ed. Its rejection is stored
	// and re-thrown on the next awaited wrapper method (and on stop()), so a
	// failed durable write (IO/disk-full) surfaces as an error rather than
	// vanishing and leaving the block out of durable while native/log report
	// success. `.catch` also prevents unhandled-rejection noise.
	private readonly pendingDurableWrites = new Set<Promise<unknown>>();
	private nativeDurableCommitFailure?: NativeDurableCommitError;
	private stopCompleted = false;
	private readonly nativeDeleteTombstones = new Map<string, number>();
	private nativeDeleteEpoch = 0;
	private readonly nativeBlockWriteGenerations = new Map<string, number>();
	private readonly pendingNativeDeleteCleanup = new Map<string, number>();
	private readonly stagedNativeDeleteCleanups = new Map<
		number,
		Map<string, number>
	>();
	private nextNativeDeleteCleanupToken = 0;
	private nativeDeleteCleanupRunning: Promise<void> | undefined;
	private nextNativeCommitOwnershipToken = 0;
	private readonly nativeCommitOwnerships = new Map<
		number,
		NativeCommitOwnershipToken
	>();
	private readonly nativeCommitOwnershipsByCid = new Map<string, Set<number>>();

	getNativeDurableCommitFailure(): NativeDurableCommitError | undefined {
		return this.nativeDurableCommitFailure;
	}

	private recordNativeDurableCommitFailure(
		cause: unknown,
		options?: {
			committedCids?: Iterable<string>;
			failedCids?: Iterable<string>;
		},
	): NativeDurableCommitError {
		if (this.nativeDurableCommitFailure) {
			this.nativeDurableCommitFailure.addCommitContext(options);
			return this.nativeDurableCommitFailure;
		}
		this.nativeDurableCommitFailure =
			cause instanceof NativeDurableCommitError
				? cause
				: new NativeDurableCommitError(cause, options);
		if (cause instanceof NativeDurableCommitError) {
			cause.addCommitContext(options);
		}
		return this.nativeDurableCommitFailure;
	}

	private throwIfNativeDurableCommitFailed(): void {
		if (this.nativeDurableCommitFailure) {
			throw this.nativeDurableCommitFailure;
		}
	}

	throwIfDurableWritesFailed(): void {
		this.throwIfNativeDurableCommitFailed();
	}

	private async commitDurableMutation<T>(
		operation: () => MaybePromise<T>,
		committedCids: Iterable<string>,
		failedCids?: Iterable<string>,
	): Promise<T> {
		this.throwIfNativeDurableCommitFailed();
		const committedCidList = [...committedCids];
		const failedCidList = failedCids ? [...failedCids] : committedCidList;
		let result: T;
		const operationResult = Promise.resolve().then(operation);
		this.trackAwaitedDurable(operationResult);
		try {
			result = await operationResult;
		} catch (error) {
			throw this.recordNativeDurableCommitFailure(error, {
				committedCids: committedCidList,
				failedCids: failedCidList,
			});
		}
		// A different concurrent native mutation may have poisoned the wrapper
		// while this durable call was in flight. Include this operation among the
		// native-applied facts, but not among durable calls that actually failed.
		if (this.nativeDurableCommitFailure) {
			this.nativeDurableCommitFailure.addCommitContext({
				committedCids: committedCidList,
				failedCids: [],
			});
			throw this.nativeDurableCommitFailure;
		}
		return result;
	}

	private beginNativeDelete(cids: string[]): void {
		this.nativeDeleteEpoch++;
		for (const cid of cids) {
			this.nativeDeleteTombstones.set(
				cid,
				(this.nativeDeleteTombstones.get(cid) ?? 0) + 1,
			);
		}
	}

	private endNativeDelete(cids: string[]): void {
		for (const cid of cids) {
			const remaining = (this.nativeDeleteTombstones.get(cid) ?? 1) - 1;
			if (remaining <= 0) {
				this.nativeDeleteTombstones.delete(cid);
			} else {
				this.nativeDeleteTombstones.set(cid, remaining);
			}
		}
	}

	private isNativeDeletePending(cid: string): boolean {
		return this.nativeDeleteTombstones.has(cid);
	}

	// A CID can be legitimately re-added after a native trim (content addressing
	// makes the bytes identical, but its liveness is new). Cancel any queued trim
	// for that CID and advance its generation before the write is exposed. An
	// already-running cleanup uses the generation/pending map to avoid deleting
	// the new native value; synchronous columnar writes also chain their durable
	// mirror behind that cleanup below.
	private noteNativeBlockWrite(cids: string[]): Map<string, number> {
		const generations = new Map<string, number>();
		for (const cid of new Set(cids)) {
			const generation = (this.nativeBlockWriteGenerations.get(cid) ?? 0) + 1;
			this.nativeBlockWriteGenerations.set(cid, generation);
			generations.set(cid, generation);
			if (this.pendingNativeDeleteCleanup.delete(cid)) {
				this.endNativeDelete([cid]);
			}
			for (const staged of this.stagedNativeDeleteCleanups.values()) {
				if (staged.delete(cid)) {
					this.endNativeDelete([cid]);
				}
			}
		}
		return generations;
	}

	private beginNativeCommitOwnership(
		generations: Map<string, number>,
	): NativeCommitOwnershipToken | undefined {
		if (generations.size === 0) {
			return undefined;
		}
		const token: NativeCommitOwnershipToken = {
			id: ++this.nextNativeCommitOwnershipToken,
			rows: new Map(),
		};
		for (const [cid, generation] of generations) {
			const owners = this.nativeCommitOwnershipsByCid.get(cid) ?? new Set();
			const shared = owners.size > 0;
			for (const ownerId of owners) {
				const owner = this.nativeCommitOwnerships.get(ownerId);
				const row = owner?.rows.get(cid);
				if (row) row.shared = true;
			}
			owners.add(token.id);
			this.nativeCommitOwnershipsByCid.set(cid, owners);
			token.rows.set(cid, { generation, shared });
		}
		this.nativeCommitOwnerships.set(token.id, token);
		return token;
	}

	private releaseNativeCommitOwnership(token: unknown): void {
		if (
			!token ||
			typeof token !== "object" ||
			typeof (token as NativeCommitOwnershipToken).id !== "number"
		) {
			return;
		}
		const owned = this.nativeCommitOwnerships.get(
			(token as NativeCommitOwnershipToken).id,
		);
		if (owned !== token) {
			return;
		}
		this.nativeCommitOwnerships.delete(owned.id);
		for (const cid of owned.rows.keys()) {
			const owners = this.nativeCommitOwnershipsByCid.get(cid);
			owners?.delete(owned.id);
			if (owners?.size === 0) {
				this.nativeCommitOwnershipsByCid.delete(cid);
			}
		}
	}

	acknowledgeNativeCommitOwnership(token: unknown): void {
		this.releaseNativeCommitOwnership(token);
	}

	private enqueueNativeDeleteCleanup(cids: string[]): void {
		for (const cid of new Set(cids)) {
			if (!this.pendingNativeDeleteCleanup.has(cid)) {
				this.pendingNativeDeleteCleanup.set(
					cid,
					this.nativeBlockWriteGenerations.get(cid) ?? 0,
				);
				this.beginNativeDelete([cid]);
			}
		}
	}

	private releaseStagedNativeDeleteCleanup(token: number): boolean {
		const staged = this.stagedNativeDeleteCleanups.get(token);
		if (!staged) {
			return false;
		}
		this.stagedNativeDeleteCleanups.delete(token);
		for (const [cid] of staged) {
			this.endNativeDelete([cid]);
		}
		return true;
	}

	private discardStagedNativeDeleteCleanups(): void {
		for (const token of [...this.stagedNativeDeleteCleanups.keys()]) {
			this.releaseStagedNativeDeleteCleanup(token);
		}
	}

	// Native commit callbacks call this immediately after the native transaction
	// returns, before awaiting the new block's durable mirror. This stages read
	// tombstones only. Durable deletion is promoted later by the exact EntryIndex
	// consume token, so an unacknowledged/failed append cannot delete the old
	// durable head that the lower log still publishes.
	beginNativeDeleteCleanup(cids: string[]): number | undefined {
		this.throwIfNativeDurableCommitFailed();
		const uniqueCids = [...new Set(cids)];
		if (uniqueCids.length === 0) {
			return undefined;
		}
		const token = ++this.nextNativeDeleteCleanupToken;
		const staged = new Map(
			uniqueCids.map((cid) => [
				cid,
				this.nativeBlockWriteGenerations.get(cid) ?? 0,
			]),
		);
		this.stagedNativeDeleteCleanups.set(token, staged);
		this.beginNativeDelete(uniqueCids);
		return token;
	}

	cancelNativeDeleteCleanup(cleanupToken: unknown): void {
		if (typeof cleanupToken === "number") {
			this.releaseStagedNativeDeleteCleanup(cleanupToken);
		}
	}

	private async waitForNativeDeleteCleanup(): Promise<void> {
		while (this.nativeDeleteCleanupRunning) {
			await this.nativeDeleteCleanupRunning;
		}
	}

	private async waitForTrackedDurableWrites(): Promise<void> {
		while (this.pendingDurableWrites.size > 0) {
			await Promise.allSettled([...this.pendingDurableWrites]);
		}
	}

	private async retryNativeDeleteCleanup(options?: {
		allowPoisoned?: boolean;
		throwOnFailure?: boolean;
	}): Promise<void> {
		await this.waitForNativeDeleteCleanup();
		if (this.nativeDurableCommitFailure && !options?.allowPoisoned) {
			throw this.nativeDurableCommitFailure;
		}
		// A synchronous columnar write may already have scheduled its durable
		// mirror. Let it settle before deleting queued CIDs, but leave any recorded
		// error for drainDurable() to surface to its owning operation/stop.
		await this.waitForTrackedDurableWrites();
		await this.waitForNativeDeleteCleanup();
		// A tracked mirror or the cleanup we just waited for may have poisoned the
		// wrapper. Do not begin another durable mutation on the ordinary path after
		// that asynchronous boundary. stop() alone opts into one cleanup retry so a
		// transient delete failure can still release its tombstones and resources.
		if (this.nativeDurableCommitFailure && !options?.allowPoisoned) {
			throw this.nativeDurableCommitFailure;
		}
		if (this.pendingNativeDeleteCleanup.size === 0) {
			return;
		}
		const cleanupEntries = [...this.pendingNativeDeleteCleanup].filter(
			([cid, generation]) =>
				(this.nativeBlockWriteGenerations.get(cid) ?? 0) === generation,
		);
		if (cleanupEntries.length === 0) {
			return;
		}
		const cids = cleanupEntries.map(([cid]) => cid);
		let cleanupFailure: unknown;
		const running = (async () => {
			let durableRemoved = false;
			let nativeRemoved = false;
			try {
				await this.durable.rmMany(cids);
				durableRemoved = true;
			} catch (error) {
				cleanupFailure = error;
				// The new entry blocks are already durable at this point and the native
				// transaction has selected the new graph/index state. Old content-addressed
				// blocks are therefore harmless unreachable orphans. Keep this cleanup as
				// retryable debt instead of poisoning/rolling back a fully durable append;
				// a partial rmMany is safe for the same reason.
				warn(
					`Failed durable native-trim cleanup; retaining retry debt: ${String(error)}`,
				);
			} finally {
				// A read that began in the native-transaction -> cleanup-hook gap may
				// have repopulated native. Always repeat the native removal, even when
				// durable rm failed. Exclude CIDs re-added while durable IO was pending;
				// their generation change cancelled the queued delete.
				const stillDeleted = cids.filter((cid) =>
					this.pendingNativeDeleteCleanup.has(cid),
				);
				try {
					if (stillDeleted.length > 0) {
						await this.native.rmMany(stillDeleted);
					}
					nativeRemoved = true;
				} catch (error) {
					cleanupFailure ??= error;
					warn(
						`Failed to repeat native-trim hot block removal: ${String(error)}`,
					);
				}
			}
			if (durableRemoved && nativeRemoved) {
				for (const [cid, generation] of cleanupEntries) {
					if (this.pendingNativeDeleteCleanup.get(cid) === generation) {
						this.pendingNativeDeleteCleanup.delete(cid);
						this.nativeBlockWriteGenerations.delete(cid);
						this.endNativeDelete([cid]);
					}
				}
			}
		})();
		this.nativeDeleteCleanupRunning = running;
		try {
			await running;
		} finally {
			if (this.nativeDeleteCleanupRunning === running) {
				this.nativeDeleteCleanupRunning = undefined;
			}
		}
		if (options?.throwOnFailure && cleanupFailure !== undefined) {
			throw cleanupFailure;
		}
	}

	private trackDurable(result: unknown, cids: string[]): void {
		// The durable store may answer synchronously (an in-memory or already
		// resolved store); only a real pending promise needs tracking. A sync
		// success is already durable; a sync throw would have propagated already.
		if (!isPromiseLike(result)) {
			return;
		}
		const tracked = Promise.resolve(result).then(
			() => {
				this.pendingDurableWrites.delete(tracked);
			},
			(error) => {
				this.pendingDurableWrites.delete(tracked);
				this.recordNativeDurableCommitFailure(error, {
					committedCids: cids,
					failedCids: cids,
				});
			},
		);
		this.pendingDurableWrites.add(tracked);
	}

	// Awaited mirror writes surface their own rejection to the append that
	// created them. Track a non-rejecting settlement companion as well so stop()
	// and later wrapper operations cannot close/use durable storage while that
	// append barrier is still in flight.
	private trackAwaitedDurable(result: Promise<unknown>): void {
		const tracked = result.then(
			() => {
				this.pendingDurableWrites.delete(tracked);
			},
			() => {
				this.pendingDurableWrites.delete(tracked);
			},
		);
		this.pendingDurableWrites.add(tracked);
	}

	// Wait for every tracked (sync-path) durable write to settle, then surface
	// the first failure. Awaited methods call this so a prior columnar durable
	// failure propagates as back-pressure to the next caller.
	private async drainDurable(): Promise<void> {
		await this.waitForTrackedDurableWrites();
		this.throwIfNativeDurableCommitFailed();
	}

	// --- lifecycle -------------------------------------------------------
	// The native store's lifecycle hooks are no-ops; only the durable store
	// needs starting/stopping. The wasm map is NOT eagerly rehydrated from disk:
	// entry blocks are pulled back lazily on demand through the read fallback in
	// getMany()/get() (durable hit -> repopulate the wasm map), which is what the
	// log's DAG walk (EntryIndex.resolveMany -> store.getMany) exercises. Keeping
	// the wasm map cold on open is required by the strict-native resident
	// coordinate-state optimization: a reopened non-replicating native node must
	// report hasBlock(head) === false and answer a same-signer append from the
	// persisted coordinate + signer facts without resolving the entry block.
	async start(): Promise<void> {
		await this.durable.start();
		this.stopCompleted = false;
	}

	async stop(): Promise<void> {
		if (this.stopCompleted) {
			return;
		}
		// Surface any tracked (sync-path) durable write failure and ensure all
		// mirror writes have settled before the durable store is torn down. Closing
		// the durable store is unconditional: a prior columnar mirror failure must
		// not leak the store lifecycle resource.
		let firstError: unknown;
		let shutdownError: unknown;
		try {
			await this.drainDurable();
		} catch (error) {
			firstError = error;
		}
		// Tokens not consumed by EntryIndex belong to native prepares that never
		// published their lower-log trim. Release their read tombstones, but never
		// promote them to durable deletion during shutdown.
		this.discardStagedNativeDeleteCleanups();
		try {
			await this.retryNativeDeleteCleanup({ allowPoisoned: true });
		} catch (error) {
			firstError ??= error;
			shutdownError ??= error;
		}
		try {
			await this.durable.stop();
		} catch (error) {
			firstError ??= error;
			shutdownError ??= error;
		}
		if (shutdownError === undefined) {
			// A durable poison belongs to the generation being closed. Report it once
			// so the owning terminal call observes the failed append, but remember that
			// every mandatory shutdown stage completed. Conservative content-addressed
			// trim debt may remain in this retired wrapper after best-effort cleanup; a
			// fresh generation gets a new wrapper and must not be wedged by that debt.
			// The exact terminal retry may therefore finish parent bookkeeping without
			// rethrowing the same latched poison forever.
			this.stopCompleted = true;
		}
		if (firstError !== undefined) {
			throw firstError;
		}
	}

	status() {
		return this.durable.status();
	}

	waitFor(): Promise<string[]> {
		return Promise.resolve([]);
	}

	// Native commit APIs must keep their block-store callback synchronous. The
	// lower log calls this barrier after that callback reports committed blocks
	// and before publishing index/head facts.
	waitForDurableWrites(): Promise<void> {
		return this.drainDurable();
	}

	// Mirror a single already-committed block (present in the wasm map) to the
	// durable store ONLY. Used by the native commit-only append fast path: the
	// native prepare commits the entry block into the wasm map and returns no raw
	// bytes, and the strict-native resident-coordinate path deliberately does NOT
	// route the block through the log's finishBlocks/putKnown* (that would disturb
	// the commit-only append path the RCS optimization depends on). Instead the
	// caller reads the committed bytes back and calls this so the block lands in
	// durable directly. The caller awaits this method before acknowledging its
	// append, so a failed write rejects the append that produced the block.
	async mirrorToDurable(
		cid: string,
		bytes: Uint8Array,
		options?: { nativeTrimmed?: boolean },
	): Promise<unknown> {
		this.throwIfNativeDurableCommitFailed();
		// The native commit happened before this call. Mark the CID live before
		// awaiting anything so a concurrent retry of an older trim cannot remove
		// the newly committed hot block.
		let ownership: NativeCommitOwnershipToken | undefined;
		if (options?.nativeTrimmed !== true) {
			ownership = this.beginNativeCommitOwnership(
				this.noteNativeBlockWrite([cid]),
			);
			await this.waitForNativeDeleteCleanup();
		}
		try {
			await this.drainDurable();
			if (ownership) {
				const existed = await this.durable.hasMany([...ownership.rows.keys()]);
				let index = 0;
				for (const row of ownership.rows.values()) {
					row.durableExistedBefore = existed[index++] === true;
				}
			}
			const result = this.commitDurableMutation(
				() => this.durable.putKnown(cid, bytes),
				[cid],
			);
			await result;
			await this.retryNativeDeleteCleanup();
			this.throwIfNativeDurableCommitFailed();
			return ownership;
		} catch (error) {
			// The caller never receives an ownership token for an indeterminate write.
			// Release the in-memory claim and preserve any bytes the backend may have
			// applied before rejecting.
			this.releaseNativeCommitOwnership(ownership);
			throw error;
		}
	}

	async mirrorManyToDurable(
		blocks: Array<readonly [cid: string, bytes: Uint8Array]>,
		options?: { nativeTrimmedCids?: ReadonlySet<string> },
	): Promise<unknown> {
		this.throwIfNativeDurableCommitFailed();
		const cids = blocks.map(([cid]) => cid);
		// Rows trimmed later in the same native batch deliberately remain owned by
		// that batch's staged cleanup; only mark surviving rows live.
		const liveCids = cids.filter(
			(cid) => !options?.nativeTrimmedCids?.has(cid),
		);
		const ownership = this.beginNativeCommitOwnership(
			this.noteNativeBlockWrite(liveCids),
		);
		if (liveCids.length > 0) {
			await this.waitForNativeDeleteCleanup();
		}
		try {
			await this.drainDurable();
			if (ownership) {
				const existed = await this.durable.hasMany([...ownership.rows.keys()]);
				let index = 0;
				for (const row of ownership.rows.values()) {
					row.durableExistedBefore = existed[index++] === true;
				}
			}
			if (blocks.length > 0) {
				await this.commitDurableMutation(
					() => this.durable.putKnownMany(blocks),
					cids,
				);
			}
			await this.retryNativeDeleteCleanup();
			this.throwIfNativeDurableCommitFailed();
			return ownership;
		} catch (error) {
			this.releaseNativeCommitOwnership(ownership);
			throw error;
		}
	}

	async rollbackFailedNativeCommits(
		cids: string[],
		restoreNativeCids: string[] = [],
		ownershipToken?: unknown,
	): Promise<void> {
		// This is the sole mutation allowed after poison: it removes a native
		// transaction that the lower log never published. Do not route through the
		// guarded rmMany path, and settle the failed mirror before compensating it.
		await this.waitForTrackedDurableWrites();
		// A failed replacement never published its trim. Reassert the restored CIDs
		// as live before any compensation IO so staged/pending delete intents cannot
		// remove the last acknowledged blocks during stop or reopen.
		const ownership =
			ownershipToken && typeof ownershipToken === "object"
				? this.nativeCommitOwnerships.get(
						(ownershipToken as NativeCommitOwnershipToken).id,
					)
				: undefined;
		const verifiedOwnership =
			ownership && ownership === ownershipToken ? ownership : undefined;
		const restoreSet = new Set(restoreNativeCids);
		const safeDurableDeletes = verifiedOwnership
			? [...new Set(cids)].filter((cid) => {
					const row = verifiedOwnership.rows.get(cid);
					const owners = this.nativeCommitOwnershipsByCid.get(cid);
					return (
						!restoreSet.has(cid) &&
						row?.durableExistedBefore === false &&
						row.shared === false &&
						(this.nativeBlockWriteGenerations.get(cid) ?? 0) ===
							row.generation &&
						owners?.size === 1 &&
						owners.has(verifiedOwnership.id)
					);
				})
			: [];
		this.noteNativeBlockWrite(restoreNativeCids);
		let firstError: unknown;
		try {
			if (safeDurableDeletes.length > 0) {
				await this.durable.rmMany(safeDurableDeletes);
			}
		} catch (error) {
			firstError = error;
		}
		// A native prepare runs before ownership can observe the hot map, so it cannot
		// prove that a CID was absent there before this operation. Keep native bytes as
		// unreachable orphans rather than deleting acknowledged/shared/restored data.
		if (restoreNativeCids.length > 0) {
			try {
				const values = await this.durable.getMany(restoreNativeCids);
				const restore: Array<readonly [string, Uint8Array]> = [];
				for (let index = 0; index < restoreNativeCids.length; index++) {
					const value = values[index];
					if (value) restore.push([restoreNativeCids[index]!, value]);
				}
				if (restore.length > 0) {
					this.native.putKnownMany(restore);
				}
			} catch (error) {
				firstError ??= error;
			}
		}
		this.releaseNativeCommitOwnership(ownershipToken);
		if (firstError !== undefined) throw firstError;
	}

	/**
	 * Compensate a native prepare that failed before its durable mirror began.
	 * Durable presence proves a same-CID acknowledged owner; an active ownership
	 * token proves a concurrent mirror. Only an unowned, non-durable hot block is
	 * exclusively attributable to the failed prepare and safe to remove.
	 */
	async rollbackUnmirroredNativeCommits(
		cids: string[],
		restoreNativeCids: string[] = [],
	): Promise<void> {
		const unique = [...new Set(cids)];
		// Native prepares bypass this wrapper. Any observed wrapper generation is
		// therefore evidence of a generic/same-CID writer, not of the failed prepare.
		// Snapshot before the first await and require both absence and stability so a
		// write starting before or during durable.hasMany cannot lose its hot value to
		// a stale `false` result.
		const genericWriteGenerations = new Map(
			unique.map((cid) => [cid, this.nativeBlockWriteGenerations.get(cid)]),
		);
		await this.rollbackFailedNativeCommits(cids, restoreNativeCids);
		const durablePresence = await this.durable.hasMany(unique);
		const restore = new Set(restoreNativeCids);
		const safeNativeDeletes = unique.filter(
			(cid, index) =>
				!restore.has(cid) &&
				durablePresence[index] !== true &&
				genericWriteGenerations.get(cid) === undefined &&
				this.nativeBlockWriteGenerations.get(cid) === undefined &&
				(this.nativeCommitOwnershipsByCid.get(cid)?.size ?? 0) === 0,
		);
		if (safeNativeDeletes.length > 0) {
			await this.native.rmMany(safeNativeDeletes);
		}
	}

	// --- writes (apply to BOTH: native first for the hot path, then durable) ---
	async put(
		data: Uint8Array | { block: { bytes: Uint8Array }; cid: string },
	): Promise<string> {
		await this.drainDurable();
		const cid = await this.native.put(data as any);
		// The native store computes a raw-codec CID for a `Uint8Array`, storing
		// the bytes verbatim (raw codec is identity), and stores `block.bytes`
		// for the pre-CIDed object form. Either way the input bytes match what
		// native stored, so feed durable the known cid+bytes without recomputing.
		const value =
			data instanceof Uint8Array
				? data
				: (data as { block: { bytes: Uint8Array } }).block.bytes;
		this.noteNativeBlockWrite([cid]);
		await this.waitForNativeDeleteCleanup();
		// put() may have yielded while calculating the CID. Restore the hot value
		// after any older cleanup that was already in flight.
		this.throwIfNativeDurableCommitFailed();
		this.native.putKnown(cid, value);
		await this.commitDurableMutation(
			() => this.durable.putKnown(cid, value),
			[cid],
		);
		return cid;
	}

	async putMany(
		blocks: Array<Uint8Array | { block: { bytes: Uint8Array }; cid: string }>,
	): Promise<string[]> {
		await this.drainDurable();
		const cids = await this.native.putMany(blocks as any);
		const durableBlocks: Array<readonly [string, Uint8Array]> = cids.map(
			(cid, index) => {
				const block = blocks[index]!;
				const value =
					block instanceof Uint8Array
						? block
						: (block as { block: { bytes: Uint8Array } }).block.bytes;
				return [cid, value] as const;
			},
		);
		this.noteNativeBlockWrite(cids);
		await this.waitForNativeDeleteCleanup();
		this.throwIfNativeDurableCommitFailed();
		this.native.putKnownMany(durableBlocks);
		await this.commitDurableMutation(
			() => this.durable.putKnownMany(durableBlocks),
			cids,
		);
		return cids;
	}

	// Native put is synchronous (the authoritative hot store); the durable mirror
	// is awaited so the returned promise resolves only after BOTH native and
	// durable succeed and a durable IO/disk-full failure rejects here instead of
	// being swallowed. RemoteBlocks.putKnown and the log's putKnownEntryBytesBatch
	// both await this method, so returning a promise is compatible.
	async putKnown(cid: string, bytes: Uint8Array): Promise<string> {
		await this.drainDurable();
		await this.waitForNativeDeleteCleanup();
		this.throwIfNativeDurableCommitFailed();
		const stored = this.native.putKnown(cid, bytes);
		this.noteNativeBlockWrite([cid]);
		await this.commitDurableMutation(
			() => this.durable.putKnown(cid, bytes),
			[cid],
		);
		return stored;
	}

	async putKnownMany(
		blocks: Array<readonly [cid: string, bytes: Uint8Array]>,
	): Promise<string[]> {
		await this.drainDurable();
		await this.waitForNativeDeleteCleanup();
		this.throwIfNativeDurableCommitFailed();
		const cids = this.native.putKnownMany(blocks);
		this.noteNativeBlockWrite(cids);
		await this.commitDurableMutation(
			() => this.durable.putKnownMany(blocks),
			cids,
		);
		return cids;
	}

	putKnownManyColumns(cids: string[], bytes: Uint8Array[]): string[] {
		this.throwIfNativeDurableCommitFailed();
		if (cids.length !== bytes.length) {
			throw new Error("Expected equal block column lengths");
		}
		const cleanupBarrier = this.nativeDeleteCleanupRunning;
		const stored = this.native.putKnownManyColumns(cids, bytes);
		this.noteNativeBlockWrite(cids);
		// AnyBlockStore has no columnar method; mirror via putKnownMany, which
		// takes [cid, bytes] tuples and hits the same batched store path.
		// This method must return a synchronous string[] (RemoteBlocks.putKnownManyColumns
		// consumes the result synchronously), so the durable write cannot be awaited
		// inline. Track it instead of `void`ing it so a durable rejection surfaces
		// on the next awaited wrapper method / stop() rather than being swallowed.
		const durableBlocks = cids.map(
			(cid, index) => [cid, bytes[index]!] as const,
		);
		let durableResult: unknown;
		try {
			durableResult = cleanupBarrier
				? cleanupBarrier.then(() => this.durable.putKnownMany(durableBlocks))
				: this.durable.putKnownMany(durableBlocks);
		} catch (error) {
			throw this.recordNativeDurableCommitFailure(error, {
				committedCids: cids,
				failedCids: cids,
			});
		}
		this.trackDurable(durableResult, cids);
		return stored;
	}

	// --- reads (native/wasm first; on miss, durable fallback + repopulate) ---
	// RemoteBlocks.get awaits this single-get, so on a native miss consult the
	// durable store (like getMany does) and repopulate the native map, rather
	// than returning undefined and only scheduling a background repopulate. That
	// avoids a spurious miss (which would otherwise fall through to a remote
	// read) for a block that is present on disk.
	async get(cid: string, _options?: unknown): Promise<Uint8Array | undefined> {
		if (this.nativeDurableCommitFailure) {
			// After poison, durable storage is the last acknowledged authority. Do not
			// repopulate or otherwise mutate the native store until reopen.
			return this.isNativeDeletePending(cid)
				? undefined
				: this.durable.get(cid);
		}
		const deleteEpoch = this.nativeDeleteEpoch;
		if (this.isNativeDeletePending(cid)) {
			return undefined;
		}
		const local = this.native.get(cid);
		if (local != null) {
			return deleteEpoch === this.nativeDeleteEpoch ? local : this.get(cid);
		}
		const durableValue = await this.durable.get(cid);
		if (this.nativeDurableCommitFailure) {
			return this.isNativeDeletePending(cid) ? undefined : durableValue;
		}
		if (deleteEpoch !== this.nativeDeleteEpoch) {
			return this.get(cid);
		}
		if (durableValue != null) {
			// Repopulate the native map so the native graph reads it next time.
			this.native.putKnownManyColumns([cid], [durableValue]);
			return durableValue;
		}
		return undefined;
	}

	async getMany(cids: string[]): Promise<Array<Uint8Array | undefined>> {
		if (this.nativeDurableCommitFailure) {
			const values = await this.durable.getMany(cids);
			for (let index = 0; index < cids.length; index++) {
				if (this.isNativeDeletePending(cids[index]!)) {
					values[index] = undefined;
				}
			}
			return values;
		}
		const deleteEpoch = this.nativeDeleteEpoch;
		const results = await this.native.getMany(cids);
		if (deleteEpoch !== this.nativeDeleteEpoch) {
			return this.getMany(cids);
		}
		const missing: string[] = [];
		const missingIndexes: number[] = [];
		for (let i = 0; i < results.length; i++) {
			if (this.isNativeDeletePending(cids[i]!)) {
				results[i] = undefined;
			} else if (results[i] == null) {
				missing.push(cids[i]!);
				missingIndexes.push(i);
			}
		}
		if (missing.length === 0) {
			return results;
		}
		const durableValues = await this.durable.getMany(missing);
		if (this.nativeDurableCommitFailure) {
			const values = await this.durable.getMany(cids);
			for (let index = 0; index < cids.length; index++) {
				if (this.isNativeDeletePending(cids[index]!)) {
					values[index] = undefined;
				}
			}
			return values;
		}
		if (deleteEpoch !== this.nativeDeleteEpoch) {
			return this.getMany(cids);
		}
		const repopulateCids: string[] = [];
		const repopulateBytes: Uint8Array[] = [];
		for (
			let missingIndex = 0;
			missingIndex < missingIndexes.length;
			missingIndex++
		) {
			const i = missingIndexes[missingIndex]!;
			const value = durableValues[missingIndex];
			if (value != null) {
				results[i] = value;
				repopulateCids.push(cids[i]!);
				repopulateBytes.push(value);
			}
		}
		if (repopulateCids.length > 0) {
			// Repopulate the native map so the native graph sees these blocks.
			this.native.putKnownManyColumns(repopulateCids, repopulateBytes);
		}
		return results;
	}

	async has(cid: string): Promise<boolean> {
		if (this.nativeDurableCommitFailure) {
			return this.isNativeDeletePending(cid) ? false : this.durable.has(cid);
		}
		const deleteEpoch = this.nativeDeleteEpoch;
		if (this.isNativeDeletePending(cid)) {
			return false;
		}
		if (this.native.has(cid)) {
			return deleteEpoch === this.nativeDeleteEpoch ? true : this.has(cid);
		}
		// Mirror getMany/hasMany: a block absent from the native wasm map may still
		// be present in the durable store (e.g. persisted on disk but not yet
		// repopulated into wasm). Consult durable on a native miss so presence
		// checks agree with the resolves that getMany/hasMany already durable-fall
		// back on. `Blocks.has` is declared `MaybePromise<boolean>`, so returning a
		// promise here is contract-compatible.
		const durableHas = await this.durable.has(cid);
		return deleteEpoch === this.nativeDeleteEpoch ? durableHas : this.has(cid);
	}

	async hasMany(cids: string[]): Promise<boolean[]> {
		if (this.nativeDurableCommitFailure) {
			const values = await this.durable.hasMany(cids);
			for (let index = 0; index < cids.length; index++) {
				if (this.isNativeDeletePending(cids[index]!)) {
					values[index] = false;
				}
			}
			return values;
		}
		const deleteEpoch = this.nativeDeleteEpoch;
		const nativeHas = await this.native.hasMany(cids);
		if (deleteEpoch !== this.nativeDeleteEpoch) {
			return this.hasMany(cids);
		}
		const missing: string[] = [];
		const missingIndexes: number[] = [];
		for (let i = 0; i < nativeHas.length; i++) {
			if (this.isNativeDeletePending(cids[i]!)) {
				nativeHas[i] = false;
			} else if (!nativeHas[i]) {
				missing.push(cids[i]!);
				missingIndexes.push(i);
			}
		}
		if (missing.length === 0) {
			return nativeHas;
		}
		const durableHas = await this.durable.hasMany(missing);
		if (deleteEpoch !== this.nativeDeleteEpoch) {
			return this.hasMany(cids);
		}
		for (
			let missingIndex = 0;
			missingIndex < missingIndexes.length;
			missingIndex++
		) {
			nativeHas[missingIndexes[missingIndex]!] = durableHas[missingIndex]!;
		}
		return nativeHas;
	}

	// --- removes (apply to BOTH) ----------------------------------------
	// Native rm is synchronous; the durable rm is awaited so the returned promise
	// resolves only after both succeed and a durable failure rejects here rather
	// than being swallowed. All rm callers (RemoteBlocks.rm, the log) await it.
	async rm(cid: string): Promise<void> {
		this.throwIfNativeDurableCommitFailed();
		const writeGeneration = this.nativeBlockWriteGenerations.get(cid);
		this.beginNativeDelete([cid]);
		try {
			await this.drainDurable();
			this.native.rm(cid);
			await this.commitDurableMutation(() => this.durable.rm(cid), [cid]);
			// A durable read that began before the tombstone may have repopulated
			// native while durable rm was pending. Remove it idempotently again.
			this.native.rm(cid);
			if (
				this.nativeBlockWriteGenerations.get(cid) === writeGeneration &&
				!this.pendingNativeDeleteCleanup.has(cid)
			) {
				this.nativeBlockWriteGenerations.delete(cid);
			}
		} finally {
			this.endNativeDelete([cid]);
		}
	}

	del(cid: string): Promise<void> {
		return this.rm(cid);
	}

	async rmMany(cids: string[]): Promise<number> {
		this.throwIfNativeDurableCommitFailed();
		const writeGenerations = new Map(
			cids.map((cid) => [cid, this.nativeBlockWriteGenerations.get(cid)]),
		);
		this.beginNativeDelete(cids);
		try {
			await this.drainDurable();
			const removed = await this.native.rmMany(cids);
			await this.commitDurableMutation(() => this.durable.rmMany(cids), cids);
			await this.native.rmMany(cids);
			for (const cid of cids) {
				if (
					this.nativeBlockWriteGenerations.get(cid) ===
						writeGenerations.get(cid) &&
					!this.pendingNativeDeleteCleanup.has(cid)
				) {
					this.nativeBlockWriteGenerations.delete(cid);
				}
			}
			return removed;
		} finally {
			this.endNativeDelete(cids);
		}
	}

	// Native trim may already have removed the hot wasm blocks. Queue the durable
	// copy for cleanup, retaining read tombstones until removal succeeds; a
	// cleanup failure is retried and never fed into ordinary append rollback.
	// EntryIndex feature-detects this hook.
	async rmManyAfterNativeDelete(
		cids: string[],
		cleanupToken?: unknown,
	): Promise<void> {
		if (this.nativeDurableCommitFailure) {
			this.cancelNativeDeleteCleanup(cleanupToken);
			throw this.nativeDurableCommitFailure;
		}
		let preannounced = false;
		if (typeof cleanupToken === "number") {
			const staged = this.stagedNativeDeleteCleanups.get(cleanupToken);
			if (staged) {
				preannounced = true;
				this.stagedNativeDeleteCleanups.delete(cleanupToken);
				for (const [cid, generation] of staged) {
					if ((this.nativeBlockWriteGenerations.get(cid) ?? 0) !== generation) {
						this.endNativeDelete([cid]);
						continue;
					}
					if (this.pendingNativeDeleteCleanup.has(cid)) {
						// Another delete already owns a tombstone for this CID.
						this.endNativeDelete([cid]);
					} else {
						// Transfer the staged tombstone to the now-published cleanup.
						this.pendingNativeDeleteCleanup.set(cid, generation);
					}
				}
			}
		}
		if (!preannounced) {
			this.enqueueNativeDeleteCleanup(cids);
		}
		await this.retryNativeDeleteCleanup();
	}

	/** Finish committed trim GC before its durable recovery intent is retired. */
	async completeCommittedNativeDeleteCleanup(
		cids: string[],
		options?: { reconstructMissing?: boolean },
	): Promise<void> {
		this.throwIfNativeDurableCommitFailed();
		const uniqueCids = [...new Set(cids.filter(Boolean))];
		if (uniqueCids.length === 0) {
			return;
		}
		// Only restart recovery reconstructs missing debt. On the live path, an
		// absent row may mean the CID was legitimately re-added after its original
		// generation-owned trim completed or was cancelled; re-enqueueing it here
		// would capture the new generation and delete live content.
		if (options?.reconstructMissing) {
			this.enqueueNativeDeleteCleanup(uniqueCids);
		}
		if (!uniqueCids.some((cid) => this.pendingNativeDeleteCleanup.has(cid))) {
			return;
		}
		await this.retryNativeDeleteCleanup({ throwOnFailure: true });
		const remaining = uniqueCids.filter((cid) =>
			this.pendingNativeDeleteCleanup.has(cid),
		);
		if (remaining.length > 0) {
			throw new Error(
				`Committed native trim cleanup remains incomplete: ${remaining.join(", ")}`,
			);
		}
	}

	// --- misc ------------------------------------------------------------
	async *iterator(): AsyncGenerator<[string, Uint8Array], void, void> {
		if (this.nativeDurableCommitFailure) {
			for await (const block of this.durable.iterator()) {
				if (!this.isNativeDeletePending(block[0])) {
					yield block;
				}
			}
			return;
		}
		yield* this.native.iterator();
	}

	async size(): Promise<number> {
		// The hot wasm map is intentionally cold after reopen, so it cannot be the
		// storage-budget authority. Settle synchronous columnar mirrors first, then
		// report durable bytes; pending trim cleanup remains conservatively counted
		// until its durable deletion succeeds.
		await this.drainDurable();
		return this.durable.size();
	}

	persisted(): boolean {
		// The blocks are now mirrored to a durable store, so report persisted so
		// callers that gate durable-only behavior on this flag behave correctly.
		return true;
	}

	get crashSafeDurability() {
		const durable = this.durable.crashSafeDurability;
		if (!durable) {
			return undefined;
		}
		return {
			crashSafe: true as const,
			barrier: async () => {
				await this.drainDurable();
				await durable.barrier();
			},
		};
	}
}
