import type { PublicSignKey } from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import { waitForSubscribers } from "@peerbit/pubsub";
import type { RPC } from "@peerbit/rpc";
import {
	ACK_CONTROL_PRIORITY,
	AcknowledgeDelivery,
} from "@peerbit/stream-interface";
import { isNotStartedError } from "./errors.js";
import type { TransportMessage } from "./message.js";
import { ReplicationPingMessage } from "./replication.js";

const logger = loggerFn("peerbit:shared-log");

// In sparse topologies (browser/relay), peers can learn about replicators via broadcast
// replication announcements without having a direct connection that emits unsubscribe
// on abrupt churn. Probe conservatively so a single missed ACK does not evict a
// healthy replicator, and rely on replication-info refresh to recover membership.
const REPLICATOR_LIVENESS_SWEEP_INTERVAL_MS = 2_000;
const REPLICATOR_LIVENESS_IDLE_THRESHOLD_MS = 8_000;
const REPLICATOR_LIVENESS_PROBE_FAILURES_TO_EVICT = 2;

export type ReplicatorLivenessDeps = {
	isClosed: () => boolean;
	getCloseSignal: () => AbortSignal;
	getReplicationLifecycleController: () => AbortController | undefined;
	isReplicationLifecycleActive: (
		controller: AbortController | undefined,
	) => boolean;
	getSelfHash: () => string;
	getUniqueReplicators: () => Set<string>;
	getSubscriptionEpoch: (peerHash: string) => object | null;
	isCurrentSubscriptionEpoch: (
		peerHash: string,
		epoch: object | null,
	) => boolean;
	resolvePublicKeyFromHash: (
		hash: string,
	) => Promise<PublicSignKey | undefined>;
	removeReplicator: (
		key: PublicSignKey | string,
		options?: {
			noEvent?: boolean;
			onRemoved?: (state: { wasReplicator: boolean }) => void;
			replicationLifecycleController?: AbortController;
			shouldRemove?: () => boolean;
			subscriptionEpoch?: object | null;
		},
	) => Promise<boolean>;
	getRpc: () => RPC<TransportMessage, TransportMessage>;
	getPendingReplicatorLeaveByPeer: () => Set<string>;
	dispatchReplicatorLeave: (publicKey: PublicSignKey) => void;
	isBlockedPeer: (hash: string) => boolean;
	scheduleReplicationInfoRequests: (
		peer: PublicSignKey,
		replicationLifecycleController: AbortController,
	) => void;
	getTopicSubscribers: (
		topic: string,
	) => Promise<PublicSignKey[] | undefined>;
	// Late-bound through the SharedLog instance so tests can override the
	// presence check on the log while probes are in flight.
	confirmReplicatorSubscriberPresence: (peerHash: string) => Promise<boolean>;
	getNode: () => Parameters<typeof waitForSubscribers>[0];
	getWaitForReplicatorTimeout: () => number;
	// Host-routed dispatch so instance stubs/spies keep intercepting
	// sweep-driven probes and activity marks.
	probeReplicatorLiveness: (peerHash: string) => Promise<void> | void;
	markReplicatorActivity: (peerHash: string, now?: number) => void;
};

export class ReplicatorLivenessMonitor {
	_replicatorLivenessSweepRunning!: boolean;
	_replicatorLivenessTimer?: ReturnType<typeof setInterval>;
	_replicatorLivenessTargets!: string[];
	_replicatorLivenessTargetsSize!: number;
	_replicatorLivenessCursor!: number;
	_replicatorLivenessFailures!: Map<string, number>;
	_replicatorLastActivityAt!: Map<string, number>;

	constructor(private readonly deps: ReplicatorLivenessDeps) {
		this._replicatorLivenessSweepRunning = false;
		this._replicatorLivenessTargets = [];
		this._replicatorLivenessTargetsSize = 0;
		this._replicatorLivenessCursor = 0;
		this._replicatorLivenessFailures = new Map();
		this._replicatorLastActivityAt = new Map();
	}

	resetForOpen(): void {
		this._replicatorLivenessSweepRunning = false;
		this._replicatorLivenessTimer = undefined;
		this._replicatorLivenessTargets = [];
		this._replicatorLivenessTargetsSize = 0;
		this._replicatorLivenessCursor = 0;
		this._replicatorLivenessFailures = new Map();
		this._replicatorLastActivityAt = new Map();
	}

	startReplicatorLivenessSweep() {
		if (this._replicatorLivenessTimer) {
			return;
		}
		this._replicatorLivenessTimer = setInterval(() => {
			void this.runReplicatorLivenessSweep();
		}, REPLICATOR_LIVENESS_SWEEP_INTERVAL_MS);
		this._replicatorLivenessTimer.unref?.();
	}

	stopReplicatorLivenessSweep() {
		if (this._replicatorLivenessTimer) {
			clearInterval(this._replicatorLivenessTimer);
			this._replicatorLivenessTimer = undefined;
		}
		this._replicatorLivenessSweepRunning = false;
		this._replicatorLivenessTargets = [];
		this._replicatorLivenessTargetsSize = 0;
		this._replicatorLivenessCursor = 0;
		this._replicatorLivenessFailures.clear();
		this._replicatorLastActivityAt.clear();
	}

	private rebuildReplicatorLivenessTargets() {
		const selfHash = this.deps.getSelfHash();
		this._replicatorLivenessTargets = [
			...this.deps.getUniqueReplicators(),
		].filter((hash) => hash !== selfHash);
		this._replicatorLivenessTargetsSize = this.deps.getUniqueReplicators().size;
		if (
			this._replicatorLivenessCursor >= this._replicatorLivenessTargets.length
		) {
			this._replicatorLivenessCursor = 0;
		}
	}

	private getReplicatorLivenessTargets() {
		const selfHash = this.deps.getSelfHash();
		const uniqueReplicators = this.deps.getUniqueReplicators();
		const expected =
			uniqueReplicators.size - (uniqueReplicators.has(selfHash) ? 1 : 0);

		if (this._replicatorLivenessTargets.length > 0) {
			// Keep the cursor stable, but purge stale hashes (membership can change while
			// the total size stays constant).
			this._replicatorLivenessTargets = this._replicatorLivenessTargets.filter(
				(hash) => hash !== selfHash && uniqueReplicators.has(hash),
			);
		}

		if (
			this._replicatorLivenessTargetsSize !== uniqueReplicators.size ||
			this._replicatorLivenessTargets.length !== expected
		) {
			this.rebuildReplicatorLivenessTargets();
		}

		return this._replicatorLivenessTargets;
	}

	markReplicatorActivity(peerHash: string, now = Date.now()) {
		this._replicatorLastActivityAt.set(peerHash, now);
		// Any recent authenticated activity is positive liveness evidence. Reset the
		// consecutive miss streak immediately, including while an eviction is
		// waiting in the per-peer mutation lane.
		if (Date.now() - now < REPLICATOR_LIVENESS_IDLE_THRESHOLD_MS) {
			this._replicatorLivenessFailures.delete(peerHash);
		}
	}

	private hasRecentReplicatorActivity(peerHash: string, now = Date.now()) {
		const lastActivityAt = this._replicatorLastActivityAt.get(peerHash);
		if (
			lastActivityAt != null &&
			now - lastActivityAt < REPLICATOR_LIVENESS_IDLE_THRESHOLD_MS
		) {
			this._replicatorLivenessFailures.delete(peerHash);
			return true;
		}
		return false;
	}

	private async evictReplicatorFromLiveness(
		peerHash: string,
		publicKey: PublicSignKey,
		replicationLifecycleController: AbortController,
		subscriptionEpoch: object | null,
		observedActivityAt: number | undefined,
	) {
		try {
			await this.deps.removeReplicator(publicKey, {
				noEvent: true,
				replicationLifecycleController,
				shouldRemove: () =>
					this._replicatorLastActivityAt.get(peerHash) === observedActivityAt,
				subscriptionEpoch,
				onRemoved: ({ wasReplicator }) => {
					if (wasReplicator) {
						this.deps.getPendingReplicatorLeaveByPeer().add(peerHash);
					}
					// A newer subscription/lifecycle may have started while the admitted
					// removal was completing. Its reconnect barrier owns all later effects.
					if (
						!this.deps.isReplicationLifecycleActive(
							replicationLifecycleController,
						) ||
						!this.deps.isCurrentSubscriptionEpoch(peerHash, subscriptionEpoch)
					) {
						return;
					}
					if (
						this.deps.getPendingReplicatorLeaveByPeer().delete(peerHash)
					) {
						this.deps.dispatchReplicatorLeave(publicKey);
					}

					if (!this.deps.isBlockedPeer(peerHash)) {
						this.deps.scheduleReplicationInfoRequests(
							publicKey,
							replicationLifecycleController,
						);
					}
					this._replicatorLivenessTargetsSize = -1;
				},
			});
		} catch (error) {
			if (!isNotStartedError(error as Error)) {
				throw error;
			}
		}
	}

	async runReplicatorLivenessSweep() {
		const replicationLifecycleController =
			this.deps.getReplicationLifecycleController();
		if (
			this.deps.isClosed() ||
			this.deps.getCloseSignal().aborted ||
			!this.deps.isReplicationLifecycleActive(replicationLifecycleController)
		) {
			return;
		}
		if (this._replicatorLivenessSweepRunning) {
			return;
		}

		const targets = this.getReplicatorLivenessTargets();
		if (targets.length === 0) {
			return;
		}

		this._replicatorLivenessSweepRunning = true;
		try {
			if (this._replicatorLivenessCursor >= targets.length) {
				this._replicatorLivenessCursor = 0;
			}
			const peerHash = targets[this._replicatorLivenessCursor]!;
			this._replicatorLivenessCursor =
				(this._replicatorLivenessCursor + 1) % targets.length;
			await this.deps.probeReplicatorLiveness(peerHash);
		} catch (error) {
			if (!isNotStartedError(error as Error)) {
				logger.error((error as any)?.toString?.() ?? String(error));
			}
		} finally {
			if (
				this.deps.getReplicationLifecycleController() ===
				replicationLifecycleController
			) {
				this._replicatorLivenessSweepRunning = false;
			}
		}
	}

	async probeReplicatorLiveness(peerHash: string) {
		const replicationLifecycleController =
			this.deps.getReplicationLifecycleController();
		if (
			this.deps.isClosed() ||
			this.deps.getCloseSignal().aborted ||
			!replicationLifecycleController ||
			!this.deps.isReplicationLifecycleActive(replicationLifecycleController)
		) {
			return;
		}
		const subscriptionEpoch = this.deps.getSubscriptionEpoch(peerHash);
		const ownsProbe = () =>
			this.deps.isReplicationLifecycleActive(replicationLifecycleController) &&
			this.deps.isCurrentSubscriptionEpoch(peerHash, subscriptionEpoch);
		if (!this.deps.getUniqueReplicators().has(peerHash)) {
			this._replicatorLivenessFailures.delete(peerHash);
			return;
		}
		if (this.hasRecentReplicatorActivity(peerHash)) {
			return;
		}
		const observedActivityAt = this._replicatorLastActivityAt.get(peerHash);

		const publicKey = await this.deps.resolvePublicKeyFromHash(peerHash);
		if (!ownsProbe()) {
			return;
		}
		if (this.hasRecentReplicatorActivity(peerHash)) {
			return;
		}
		if (!publicKey) {
			try {
				await this.deps.removeReplicator(peerHash, {
					noEvent: true,
					replicationLifecycleController,
					shouldRemove: () =>
						this._replicatorLastActivityAt.get(peerHash) === observedActivityAt,
					subscriptionEpoch,
					onRemoved: () => {
						if (!ownsProbe()) {
							return;
						}
						this._replicatorLivenessTargetsSize = -1;
					},
				});
			} catch (error) {
				if (!isNotStartedError(error as Error)) {
					throw error;
				}
			}
			return;
		}

		try {
			// Explicit ping (ACKed) instead of RequestReplicationInfoMessage to avoid
			// triggering large segment snapshots just to prove liveness.
			await this.deps.getRpc().send(new ReplicationPingMessage(), {
				mode: new AcknowledgeDelivery({ redundancy: 1, to: [publicKey] }),
				priority: ACK_CONTROL_PRIORITY,
				responsePriority: ACK_CONTROL_PRIORITY,
			});
			if (!ownsProbe()) {
				return;
			}
			this.deps.markReplicatorActivity(peerHash);
			this._replicatorLivenessFailures.delete(peerHash);
			return;
		} catch (error) {
			if (isNotStartedError(error as Error)) {
				return;
			}
		}
		if (!ownsProbe()) {
			return;
		}
		if (this.hasRecentReplicatorActivity(peerHash)) {
			return;
		}

		// Relay-backed prod paths can keep a peer subscribed/reachable even if an
		// ACKed liveness ping gets delayed or dropped under load. Treat observed
		// topic presence as a positive liveness signal before evicting the peer.
		if (await this.deps.confirmReplicatorSubscriberPresence(peerHash)) {
			if (!ownsProbe()) {
				return;
			}
			this.deps.markReplicatorActivity(peerHash);
			this._replicatorLivenessFailures.delete(peerHash);
			return;
		}
		if (!ownsProbe()) {
			return;
		}
		if (this.hasRecentReplicatorActivity(peerHash)) {
			return;
		}

		const failures = (this._replicatorLivenessFailures.get(peerHash) ?? 0) + 1;
		this._replicatorLivenessFailures.set(peerHash, failures);
		this.deps.scheduleReplicationInfoRequests(
			publicKey,
			replicationLifecycleController,
		);

		if (failures < REPLICATOR_LIVENESS_PROBE_FAILURES_TO_EVICT) {
			return;
		}
		if (!ownsProbe() || !this.deps.getUniqueReplicators().has(peerHash)) {
			this._replicatorLivenessFailures.delete(peerHash);
			return;
		}

		await this.evictReplicatorFromLiveness(
			peerHash,
			publicKey,
			replicationLifecycleController,
			subscriptionEpoch,
			observedActivityAt,
		);
	}

	async confirmReplicatorSubscriberPresence(peerHash: string) {
		try {
			const subscribers = await this.deps.getTopicSubscribers(
				this.deps.getRpc().topic,
			);
			if (
				subscribers?.some((subscriber) => subscriber.hashcode() === peerHash)
			) {
				return true;
			}
		} catch (error) {
			if (isNotStartedError(error as Error)) {
				return false;
			}
		}

		try {
			await waitForSubscribers(
				this.deps.getNode(),
				peerHash,
				this.deps.getRpc().topic,
				{
					signal: this.deps.getCloseSignal(),
					timeout: Math.max(
						1_000,
						Math.min(
							5_000,
							Math.floor(this.deps.getWaitForReplicatorTimeout() / 4),
						),
					),
				},
			);
			return true;
		} catch (error) {
			if (isNotStartedError(error as Error)) {
				return false;
			}
			return false;
		}
	}
}
