import type { PublicSignKey } from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import type { RPC } from "@peerbit/rpc";
import {
	AcknowledgeDelivery,
	CONVERGENCE_MESSAGE_PRIORITY,
	DeliveryError,
} from "@peerbit/stream-interface";
import { TimeoutError, debounceFixedInterval } from "@peerbit/time";
import { isNotStartedError } from "./errors.js";
import type { TransportMessage } from "./message.js";
import type { ReplicationRangeIndexable } from "./ranges.js";
import {
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
	StoppedReplicating,
} from "./replication.js";

const logger = loggerFn("peerbit:shared-log");

const REPLICATION_ANNOUNCEMENT_RETRY_INTERVAL = 1000;
const REPLICATION_ANNOUNCEMENT_REPAIR_INTERVAL = 1000;
const REPLICATION_ANNOUNCEMENT_REPAIR_MAX_ATTEMPTS = 3;
// Repair one bounded cohort per mutation generation. The subscriber snapshot
// is a best-effort cache and can contain thousands of entries, so attempting
// the whole cache after every role mutation would turn convergence repair into
// an unbounded burst of separately signed, acknowledged messages. A cursor
// retained across generations rotates best-effort coverage over later changes.
const REPLICATION_ANNOUNCEMENT_REPAIR_TARGETS_PER_GENERATION = 8;

/**
 * Replication announcements are best-effort convergence messages. A detached
 * fanout shard can time out even though the shared log itself remains open.
 * Keep retries deliberately limited to concrete TimeoutErrors: abort/close and
 * unexpected programming/data errors must retain their existing semantics.
 *
 * Exact constructor/name checks complement `instanceof` for errors crossing
 * worker or duplicate-package boundaries in browsers.
 */
export const isTransientReplicationAnnouncementError = (
	error: unknown,
	seen = new Set<unknown>(),
): boolean => {
	if (
		error != null &&
		(typeof error === "object" || typeof error === "function")
	) {
		if (seen.has(error)) {
			return false;
		}
		seen.add(error);
	}

	if (error instanceof TimeoutError) {
		return true;
	}

	const nested = (error as { errors?: unknown })?.errors;
	if (Array.isArray(nested) && nested.length > 0) {
		return nested.every((item) =>
			isTransientReplicationAnnouncementError(item, new Set(seen)),
		);
	}

	const cause = (error as { cause?: unknown })?.cause;
	if (cause != null && isTransientReplicationAnnouncementError(cause, seen)) {
		return true;
	}

	const constructorName =
		typeof (error as { constructor?: { name?: unknown } })?.constructor
			?.name === "string"
			? (error as { constructor: { name: string } }).constructor.name
			: "";
	const name =
		typeof (error as { name?: unknown })?.name === "string"
			? (error as { name: string }).name
			: "";
	return constructorName === "TimeoutError" || name === "TimeoutError";
};

/**
 * Directed transport-delivery repair is allowed to retry explicit delivery
 * failures in addition to timeouts. A DirectStream ACK confirms receipt of the
 * signed envelope, not successful application by the receiver. Keep this
 * separate from the primary fanout classifier above so replicate() rejection
 * semantics remain unchanged for programming, serialization, and lifecycle
 * errors.
 */
const isTransientReplicationAnnouncementRepairError = (
	error: unknown,
	seen = new Set<unknown>(),
): boolean => {
	if (
		error != null &&
		(typeof error === "object" || typeof error === "function")
	) {
		if (seen.has(error)) {
			return false;
		}
		seen.add(error);
	}

	if (error instanceof DeliveryError || error instanceof TimeoutError) {
		return true;
	}

	const nested = (error as { errors?: unknown })?.errors;
	if (Array.isArray(nested) && nested.length > 0) {
		return nested.every((item) =>
			isTransientReplicationAnnouncementRepairError(item, new Set(seen)),
		);
	}

	const cause = (error as { cause?: unknown })?.cause;
	if (
		cause != null &&
		isTransientReplicationAnnouncementRepairError(cause, seen)
	) {
		return true;
	}

	const constructorName =
		typeof (error as { constructor?: { name?: unknown } })?.constructor
			?.name === "string"
			? (error as { constructor: { name: string } }).constructor.name
			: "";
	const name =
		typeof (error as { name?: unknown })?.name === "string"
			? (error as { name: string }).name
			: "";
	return (
		constructorName === "DeliveryError" ||
		name === "DeliveryError" ||
		constructorName === "TimeoutError" ||
		name === "TimeoutError"
	);
};

/**
 * One session per announcement window — rotated at exactly the sites that
 * bumped the legacy retry-generation counter: construction, resetForOpen,
 * cancelCurrentReplicationStateAnnouncementRetry, and the pre-send bump in
 * sendReplicationAnnouncement. Identity comparison replaces every numeric
 * generation compare. Unlike the number, identity cannot alias across open
 * cycles (the legacy reset-to-0 was safe only by the convention that a
 * companion controller check accompanied every compare site).
 */
export class AnnouncementWorkerSession {
	readonly createdAt = Date.now(); // diagnostics only
}

type ReplicationAnnouncementRepairTarget = {
	key: PublicSignKey;
	session: AnnouncementWorkerSession;
	attempts: number;
	done: boolean;
};

/**
 * Repair-side adoption of a session. Replaces the legacy compound of the
 * repair generation number, its generation controller, and the repair
 * pending / targets / cohort-selected fields. One binding per adopted
 * session; every rotation aborts the old binding's controller FIRST, then
 * reassigns (abort listeners run synchronously and must observe
 * pre-rotation state, as before). The fair cursor hash and max attempts
 * deliberately stay OUTSIDE the binding: the cursor rotates best-effort
 * coverage ACROSS generations, and max-attempts is a setup-scoped tunable.
 */
export type ReplicationAnnouncementRepairBinding = {
	session: AnnouncementWorkerSession;
	controller: AbortController;
	pending: boolean;
	targets: Map<string, ReplicationAnnouncementRepairTarget>;
	cohortSelected: boolean;
};

export type ReplicationAnnouncementRepairWorkerContext = {
	session: AnnouncementWorkerSession;
	lifecycleController: AbortController;
	binding: ReplicationAnnouncementRepairBinding;
};

export type ReplicationAnnouncementDeps<R extends "u32" | "u64"> = {
	isClosed: () => boolean;
	getCloseSignal: () => AbortSignal;
	getMyReplicationSegments: () => Promise<ReplicationRangeIndexable<R>[]>;
	validatePersistedReplicationRangeSnapshot: (
		ranges: readonly { mode: unknown }[],
	) => void;
	getSubscribers: () =>
		| Promise<PublicSignKey[] | undefined>
		| PublicSignKey[]
		| undefined;
	getSelfHash: () => string;
	isBlockedPeer: (hash: string) => boolean;
	getRpc: () => RPC<TransportMessage, TransportMessage>;
	captureReplicationOwnershipLifecycle: () => AbortController;
	throwIfReplicationOwnershipLifecycleInactive: (
		controller: AbortController,
	) => void;
	isAdaptiveReplicating: () => boolean;
	callRebalanceParticipationDebounced: () => unknown;
	// Owner-routed so coordinator spies keep observing re-entrant queueing.
	queueCurrentReplicationStateAnnouncementRepair: () => void;
	queueCurrentReplicationStateAnnouncementRetry: (error: unknown) => boolean;
};

export class ReplicationAnnouncementCoordinator<R extends "u32" | "u64"> {
	replicationAnnouncementRetryDebounced:
		| ReturnType<typeof debounceFixedInterval>
		| undefined;
	_replicationAnnouncementRetryPending!: boolean;
	_announcementSession!: AnnouncementWorkerSession;
	_replicationAnnouncementRetryController!: AbortController;
	// Publish local ownership announcements in committed mutation order. This
	// prevents an older Added message with a delayed transport completion from
	// overtaking a newer authoritative empty snapshot.
	_replicationAnnouncementSendTails?: WeakMap<AbortController, Promise<void>>;
	replicationAnnouncementRepairDebounced:
		| ReturnType<typeof debounceFixedInterval>
		| undefined;
	_announcementRepairBinding!: ReplicationAnnouncementRepairBinding;
	_replicationAnnouncementRepairFairCursorHash!: string | undefined;
	_replicationAnnouncementRepairMaxAttempts!: number;
	_replicationAnnouncementRepairController!: AbortController;

	constructor(private readonly deps: ReplicationAnnouncementDeps<R>) {
		this._replicationAnnouncementRetryPending = false;
		this._announcementSession = new AnnouncementWorkerSession();
		this._replicationAnnouncementRetryController = new AbortController();
		this._replicationAnnouncementSendTails = new WeakMap();
		this._announcementRepairBinding = this.createRepairBinding(
			this._announcementSession,
		);
		this._replicationAnnouncementRepairFairCursorHash = undefined;
		this._replicationAnnouncementRepairMaxAttempts =
			REPLICATION_ANNOUNCEMENT_REPAIR_MAX_ATTEMPTS;
		this._replicationAnnouncementRepairController = new AbortController();
	}

	private rotateAnnouncementSession(): AnnouncementWorkerSession {
		return (this._announcementSession = new AnnouncementWorkerSession());
	}

	private createRepairBinding(
		session: AnnouncementWorkerSession,
	): ReplicationAnnouncementRepairBinding {
		return {
			session,
			controller: new AbortController(),
			pending: false,
			targets: new Map(),
			cohortSelected: false,
		};
	}

	resetForOpen(): void {
		this._replicationAnnouncementRetryPending = false;
		this.rotateAnnouncementSession();
	}

	queueCurrentReplicationStateAnnouncementRetry(error: unknown): boolean {
		if (
			this.deps.isClosed() ||
			this.deps.getCloseSignal().aborted ||
			this._replicationAnnouncementRetryController.signal.aborted ||
			!isTransientReplicationAnnouncementError(error)
		) {
			return false;
		}

		this._replicationAnnouncementRetryPending = true;
		void this.replicationAnnouncementRetryDebounced?.call();
		return true;
	}

	setupReplicationAnnouncementRetryFunction(
		interval = REPLICATION_ANNOUNCEMENT_RETRY_INTERVAL,
	): void {
		this.replicationAnnouncementRetryDebounced?.close();
		this._replicationAnnouncementRetryController?.abort();
		this._replicationAnnouncementRetryController = new AbortController();
		this.replicationAnnouncementRetryDebounced = debounceFixedInterval(
			() => this.retryCurrentReplicationStateAnnouncement(),
			interval,
			{
				leading: false,
				onError: (error) => {
					if (
						this.deps.isClosed() ||
						this.deps.getCloseSignal().aborted ||
						isNotStartedError(error)
					) {
						return;
					}
					logger.error(error);
				},
			},
		);
	}

	setupReplicationAnnouncementRepairFunction(
		interval = REPLICATION_ANNOUNCEMENT_REPAIR_INTERVAL,
		maxAttempts = REPLICATION_ANNOUNCEMENT_REPAIR_MAX_ATTEMPTS,
	): void {
		if (!Number.isSafeInteger(maxAttempts) || maxAttempts <= 0) {
			throw new RangeError(
				"Replication announcement repair attempts must be positive",
			);
		}
		this.replicationAnnouncementRepairDebounced?.close();
		this._replicationAnnouncementRepairController?.abort();
		this._announcementRepairBinding?.controller.abort();
		this._replicationAnnouncementRepairController = new AbortController();
		this._announcementRepairBinding = this.createRepairBinding(
			this._announcementSession,
		);
		this._replicationAnnouncementRepairFairCursorHash = undefined;
		this._replicationAnnouncementRepairMaxAttempts = maxAttempts;
		this.replicationAnnouncementRepairDebounced = debounceFixedInterval(
			() => this.runCurrentReplicationStateAnnouncementRepair(),
			interval,
			{
				leading: false,
				// The wrapper catches worker failures while it still owns the generation
				// context. Keep this boundary visibility-only: it must never mutate a
				// possibly newer generation's pending state.
				onError: (error) => logger.error(error),
			},
		);
	}

	cancelCurrentReplicationStateAnnouncementRepair(): void {
		this._announcementRepairBinding.pending = false;
		this._replicationAnnouncementRepairController?.abort();
		this._announcementRepairBinding.controller.abort();
		this.replicationAnnouncementRepairDebounced?.close();
		this._announcementRepairBinding.targets.clear();
	}

	advanceCurrentReplicationStateAnnouncementRepairGeneration(): void {
		const session = this._announcementSession;
		if (session === this._announcementRepairBinding.session) {
			return;
		}

		// Abort acknowledged sends carrying the old full-state snapshot before the
		// primary announcement for the new mutation waits on transport. Otherwise a
		// stale batch can hold the current state behind DirectStream's seek timeout.
		this._announcementRepairBinding.controller.abort();
		this._announcementRepairBinding = this.createRepairBinding(session);
	}

	queueCurrentReplicationStateAnnouncementRepair(): void {
		if (
			this.deps.isClosed() ||
			this.deps.getCloseSignal().aborted ||
			this._replicationAnnouncementRepairController.signal.aborted ||
			!this.replicationAnnouncementRepairDebounced
		) {
			return;
		}

		this.advanceCurrentReplicationStateAnnouncementRepairGeneration();
		this._announcementRepairBinding.pending = true;
		void this.replicationAnnouncementRepairDebounced.call();
	}

	/**
	 * Single validity predicate for announcement-repair workers. "stale":
	 * the store closed or a lifecycle controller aborted — exit silently.
	 * "superseded": a newer announcement session took over — the worker
	 * must requeue so the current session gets serviced. The binding
	 * identity comparison stays at the one call site that historically
	 * required it; folding it in here is a stage-5 semantic decision, not a
	 * consolidation.
	 */
	private announcementRepairWorkerStatus(
		worker: ReplicationAnnouncementRepairWorkerContext,
	): "current" | "stale" | "superseded" {
		if (
			this.deps.isClosed() ||
			this.deps.getCloseSignal().aborted ||
			worker.lifecycleController.signal.aborted ||
			worker.binding.controller.signal.aborted
		) {
			return "stale";
		}
		if (worker.session !== this._announcementSession) {
			return "superseded";
		}
		return "current";
	}

	async runCurrentReplicationStateAnnouncementRepair(): Promise<void> {
		const session = this._announcementSession;
		const lifecycleController = this._replicationAnnouncementRepairController;
		const binding = this._announcementRepairBinding;
		try {
			await this.repairCurrentReplicationStateAnnouncement({
				session,
				lifecycleController,
				binding,
			});
		} catch (error) {
			if (
				this.announcementRepairWorkerStatus({
					session,
					lifecycleController,
					binding,
				}) !== "current" ||
				binding !== this._announcementRepairBinding
			) {
				return;
			}
			if (isNotStartedError(error as Error)) {
				return;
			}

			// Only the worker that still owns the current session may conclude
			// that its repair failed. A stale worker must not clear a newer call's
			// pending flag or attribute its error to the new session.
			this._announcementRepairBinding.pending = false;
			logger.error(error);
		}
	}

	async repairCurrentReplicationStateAnnouncement(
		context?: ReplicationAnnouncementRepairWorkerContext,
	): Promise<void> {
		if (!this._announcementRepairBinding.pending) {
			return;
		}
		const session = context?.session ?? this._announcementSession;
		const lifecycleController =
			context?.lifecycleController ??
			this._replicationAnnouncementRepairController;
		const binding = context?.binding ?? this._announcementRepairBinding;
		const segments = (await this.deps.getMyReplicationSegments()).map((range) =>
			range.toReplicationRange(),
		);
		switch (
			this.announcementRepairWorkerStatus({
				session,
				lifecycleController,
				binding,
			})
		) {
			case "stale":
				return;
			case "superseded":
				this.queueCurrentReplicationStateAnnouncementRepair();
				return;
		}
		this.deps.validatePersistedReplicationRangeSnapshot(segments);

		const subscribers = (await this.deps.getSubscribers()) ?? [];
		switch (
			this.announcementRepairWorkerStatus({
				session,
				lifecycleController,
				binding,
			})
		) {
			case "stale":
				return;
			case "superseded":
				this.queueCurrentReplicationStateAnnouncementRepair();
				return;
		}

		const selfHash = this.deps.getSelfHash();
		const currentTargets = new Map<string, PublicSignKey>();
		for (const key of subscribers) {
			const hash = key.hashcode();
			if (
				hash !== selfHash &&
				!this.deps.isBlockedPeer(hash) &&
				!currentTargets.has(hash)
			) {
				currentTargets.set(hash, key);
			}
		}

		for (const [hash, target] of this._announcementRepairBinding.targets) {
			if (target.session !== session || !currentTargets.has(hash)) {
				this._announcementRepairBinding.targets.delete(hash);
			} else {
				target.key = currentTargets.get(hash)!;
			}
		}
		if (!this._announcementRepairBinding.cohortSelected) {
			const candidates = [...currentTargets.entries()].sort(([left], [right]) =>
				left.localeCompare(right),
			);
			const cursorIndex = this._replicationAnnouncementRepairFairCursorHash
				? candidates.findIndex(
						([hash]) =>
							hash.localeCompare(
								this._replicationAnnouncementRepairFairCursorHash!,
							) > 0,
					)
				: 0;
			const fairStart = cursorIndex < 0 ? 0 : cursorIndex;
			const fairOrder = [
				...candidates.slice(fairStart),
				...candidates.slice(0, fairStart),
			];
			const cohort = fairOrder.slice(
				0,
				REPLICATION_ANNOUNCEMENT_REPAIR_TARGETS_PER_GENERATION,
			);
			for (const [hash, key] of cohort) {
				this._announcementRepairBinding.targets.set(hash, {
					key,
					session,
					attempts: 0,
					done: false,
				});
			}
			if (cohort.length > 0) {
				this._replicationAnnouncementRepairFairCursorHash =
					cohort[cohort.length - 1][0];
			}
			this._announcementRepairBinding.cohortSelected = true;
		}

		const batch = [...this._announcementRepairBinding.targets.entries()].filter(
			([, target]) => !target.done,
		);
		const snapshot = new AllReplicatingSegmentsMessage({ segments });
		const results = await Promise.allSettled(
			batch.map(([, target]) =>
				this.deps.getRpc().send(snapshot, {
					mode: new AcknowledgeDelivery({
						to: [target.key],
						redundancy: 1,
					}),
					priority: CONVERGENCE_MESSAGE_PRIORITY,
					signal: binding.controller.signal,
				}),
			),
		);
		switch (
			this.announcementRepairWorkerStatus({
				session,
				lifecycleController,
				binding,
			})
		) {
			case "stale":
				return;
			case "superseded":
				this.queueCurrentReplicationStateAnnouncementRepair();
				return;
		}

		for (const [index, result] of results.entries()) {
			const [hash, attemptedTarget] = batch[index];
			const target = this._announcementRepairBinding.targets.get(hash);
			if (target !== attemptedTarget || target.session !== session) {
				continue;
			}
			if (result.status === "fulfilled") {
				// DirectStream ACKs confirm that the signed transport envelope reached
				// the target. Applying the contained replication state remains a
				// receiver-local, best-effort operation.
				target.done = true;
				continue;
			}

			target.attempts += 1;
			if (!isTransientReplicationAnnouncementRepairError(result.reason)) {
				target.done = true;
				logger.error(result.reason);
			} else if (
				target.attempts >= this._replicationAnnouncementRepairMaxAttempts
			) {
				target.done = true;
				logger.trace(
					"Acknowledged replication announcement repair exhausted for %s",
					hash,
				);
			}
		}

		if (session !== this._announcementSession) {
			this.queueCurrentReplicationStateAnnouncementRepair();
			return;
		}
		if (
			[...this._announcementRepairBinding.targets.values()].some(
				(target) => !target.done,
			)
		) {
			void this.replicationAnnouncementRepairDebounced?.call();
			return;
		}

		this._announcementRepairBinding.pending = false;
		this._announcementRepairBinding.targets.clear();
	}

	cancelCurrentReplicationStateAnnouncementRetry(): void {
		this.rotateAnnouncementSession();
		this._replicationAnnouncementRetryPending = false;
		this._replicationAnnouncementRetryController?.abort();
		this.replicationAnnouncementRetryDebounced?.close();
		this.cancelCurrentReplicationStateAnnouncementRepair();
	}

	async sendReplicationAnnouncement(
		message:
			| AllReplicatingSegmentsMessage
			| AddedReplicationSegmentMessage
			| StoppedReplicating,
		ownershipLifecycleController = this.deps.captureReplicationOwnershipLifecycle(),
		options?: { shouldSend?: () => boolean },
	): Promise<void> {
		const tails = (this._replicationAnnouncementSendTails ??= new WeakMap<
			AbortController,
			Promise<void>
		>());
		const previous =
			tails.get(ownershipLifecycleController) ?? Promise.resolve();
		const send = previous
			.catch(() => {})
			.then(async () => {
				this.deps.throwIfReplicationOwnershipLifecycleInactive(
					ownershipLifecycleController,
				);
				if (options?.shouldSend && !options.shouldSend()) {
					return;
				}
				// Advance before every post-mutation send, including successful ones. An
				// authoritative retry already in flight may have captured the previous
				// local state; the session mismatch forces one more current snapshot
				// after that stale send settles.
				this.rotateAnnouncementSession();
				this.advanceCurrentReplicationStateAnnouncementRepairGeneration();
				try {
					await this.deps.getRpc().send(message, {
						priority: CONVERGENCE_MESSAGE_PRIORITY,
						signal: ownershipLifecycleController.signal,
					});
					this.deps.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
					this.deps.queueCurrentReplicationStateAnnouncementRepair();
				} catch (error) {
					// An old send can reject only after poison or close has installed a new
					// ownership generation. Never enqueue its retry work into that generation.
					this.deps.throwIfReplicationOwnershipLifecycleInactive(
						ownershipLifecycleController,
					);
					// The local replication-index mutation precedes all calls to this
					// wrapper. Preserve the explicit caller's rejection, but independently
					// schedule an authoritative snapshot so peers eventually observe the
					// already-committed local state.
					this.deps.queueCurrentReplicationStateAnnouncementRetry(error);
					throw error;
				}
			});
		// Keep the ordering barrier usable after a caller-observed send rejection.
		tails.set(
			ownershipLifecycleController,
			send.catch(() => {}),
		);
		return send;
	}

	async retryCurrentReplicationStateAnnouncement(): Promise<void> {
		const session = this._announcementSession;
		const controller = this._replicationAnnouncementRetryController;
		try {
			const segments = (await this.deps.getMyReplicationSegments()).map(
				(range) => range.toReplicationRange(),
			);
			if (
				this.deps.isClosed() ||
				this.deps.getCloseSignal().aborted ||
				controller.signal.aborted
			) {
				return;
			}
			if (session !== this._announcementSession) {
				void this.replicationAnnouncementRetryDebounced?.call();
				return;
			}
			this.deps.validatePersistedReplicationRangeSnapshot(segments);

			await this.deps
				.getRpc()
				.send(new AllReplicatingSegmentsMessage({ segments }), {
					priority: CONVERGENCE_MESSAGE_PRIORITY,
					signal: controller.signal,
				});
			this.queueCurrentReplicationStateAnnouncementRepair();
		} catch (error) {
			if (
				this.deps.isClosed() ||
				this.deps.getCloseSignal().aborted ||
				controller.signal.aborted
			) {
				return;
			}
			if (this.queueCurrentReplicationStateAnnouncementRetry(error)) {
				return;
			}
			if (session === this._announcementSession) {
				this._replicationAnnouncementRetryPending = false;
			} else {
				void this.replicationAnnouncementRetryDebounced?.call();
			}
			throw error;
		}
		if (
			this.deps.isClosed() ||
			this.deps.getCloseSignal().aborted ||
			controller.signal.aborted
		) {
			return;
		}

		// A newer mutation announcement may have started while this snapshot was
		// in flight. In that case keep the repair pending so the newer current
		// state is also announced in full, regardless of whether its incremental
		// send succeeded or failed.
		if (session === this._announcementSession) {
			this._replicationAnnouncementRetryPending = false;
			if (
				!this.deps.isClosed() &&
				!this.deps.getCloseSignal().aborted &&
				!controller.signal.aborted &&
				this.deps.isAdaptiveReplicating()
			) {
				void this.deps.callRebalanceParticipationDebounced();
			}
		} else {
			void this.replicationAnnouncementRetryDebounced?.call();
		}
	}
}
