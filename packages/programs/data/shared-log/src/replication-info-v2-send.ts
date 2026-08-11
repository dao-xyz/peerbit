import { type PublicSignKey, randomBytes } from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import type { RPC } from "@peerbit/rpc";
import {
	AcknowledgeDelivery,
	CONVERGENCE_MESSAGE_PRIORITY,
} from "@peerbit/stream-interface";
import type { TransportMessage } from "./message.js";
import type { ReplicationRangeIndexable } from "./ranges.js";
import { deriveReplicationInfoV2ReceiverBinding } from "./replication-info-v2-binding.js";
import {
	AddedReplicationInfoV2Message,
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
	FullReplicationInfoV2Message,
	RequestReplicationInfoV2Message,
	StoppedReplicating,
	StoppedReplicationInfoV2Message,
} from "./replication.js";

const logger = loggerFn("peerbit:shared-log:replication-info-v2-send");

const MAX_U64 = (1n << 64n) - 1n;
const DEFAULT_SEND_RETRY_MS = 1_000;
const DEFAULT_MAX_SEND_RETRY_MS = 30_000;
const MAX_BACKOFF_EXPONENT = 20;

export { deriveReplicationInfoV2ReceiverBinding } from "./replication-info-v2-binding.js";

export type LegacyReplicationInfoMessage =
	| AllReplicatingSegmentsMessage
	| AddedReplicationSegmentMessage
	| StoppedReplicating;

type SendRequest =
	| { kind: "snapshot" }
	| { kind: "message"; message: LegacyReplicationInfoMessage };

export type ReplicationInfoV2SendState = {
	peerHash: string;
	target: PublicSignKey;
	peerSession: object;
	receiverTransportSession: bigint;
	senderTransportSession: bigint;
	capabilityTimestamp: bigint;
	lastRequestTimestamp: bigint;
	receiverRequestChallenge: Uint8Array;
	receiverChallenge: Uint8Array;
	senderEpoch: Uint8Array;
	ownershipLifecycleController: AbortController;
	ownershipAbortListener: () => void;
	nextSequence: bigint;
	established: boolean;
	suspended: boolean;
	inFlightSequence?: bigint;
	retryTimer?: ReturnType<typeof setTimeout>;
	retryAttempts: number;
	controller: AbortController;
	pending?: SendRequest;
	worker?: Promise<void>;
};

export type ReplicationInfoV2SendDeps<R extends "u32" | "u64"> = {
	getRpc: () => RPC<TransportMessage, TransportMessage>;
	getSelfKey: () => PublicSignKey;
	getSenderTransportSession: () => bigint;
	getMyReplicationSegments: () => Promise<ReplicationRangeIndexable<R>[]>;
	validatePersistedReplicationRangeSnapshot: (
		ranges: readonly { mode: unknown }[],
	) => void;
	isClosed: () => boolean;
	isPeerSessionCurrent: (peerHash: string, peerSession: object) => boolean;
	isPeerSessionOpen: (peerHash: string, peerSession: object) => boolean;
	captureReplicationOwnershipLifecycle: () => AbortController;
	isReplicationOwnershipLifecycleActive: (
		controller: AbortController,
	) => boolean;
	sendRetryMs?: number;
	maxSendRetryMs?: number;
};

const bytesEqual = (left: Uint8Array, right: Uint8Array): boolean => {
	if (left.byteLength !== right.byteLength) {
		return false;
	}
	for (let index = 0; index < left.byteLength; index++) {
		if (left[index] !== right[index]) {
			return false;
		}
	}
	return true;
};

/**
 * Per-destination V2 sender streams. Each stream retains at most one in-flight
 * operation and one pending operation. A second pending mutation coalesces to
 * a freshly collected authoritative snapshot, bounding memory at O(peers).
 */
export class ReplicationInfoV2SendCoordinator<R extends "u32" | "u64"> {
	_senderEpoch!: Uint8Array;
	_sendStates!: Map<string, ReplicationInfoV2SendState>;
	_spentPeerSessions!: WeakSet<object>;
	_retiringWorkersByPeer!: Map<string, Promise<void>>;

	private readonly sendRetryMs: number;
	private readonly maxSendRetryMs: number;

	constructor(private readonly deps: ReplicationInfoV2SendDeps<R>) {
		this.sendRetryMs = Math.max(1, deps.sendRetryMs ?? DEFAULT_SEND_RETRY_MS);
		this.maxSendRetryMs = Math.max(
			this.sendRetryMs,
			deps.maxSendRetryMs ?? DEFAULT_MAX_SEND_RETRY_MS,
		);
		this._senderEpoch = randomBytes(32);
		this._sendStates = new Map();
		this._spentPeerSessions = new WeakSet();
		this._retiringWorkersByPeer = new Map();
	}

	resetForOpen(): void {
		this.clearForClose();
		this._senderEpoch = randomBytes(32);
		this._sendStates = new Map();
		this._spentPeerSessions = new WeakSet();
	}

	clearForClose(): void {
		for (const state of [...(this._sendStates?.values() ?? [])]) {
			this.clearState(state);
		}
		this._sendStates?.clear();
	}

	clearPeer(peerHash: string, expectedSession?: object): void {
		const state = this._sendStates.get(peerHash);
		if (!state || (expectedSession && state.peerSession !== expectedSession)) {
			return;
		}
		this.clearState(state);
	}

	advancePeerCapability(peerHash: string): void {
		const state = this._sendStates.get(peerHash);
		if (state) {
			this.clearState(state);
		}
	}

	private clearState(state: ReplicationInfoV2SendState): void {
		this.trackRetiringWorker(state);
		if (state.retryTimer) {
			clearTimeout(state.retryTimer);
			state.retryTimer = undefined;
		}
		state.ownershipLifecycleController.signal.removeEventListener(
			"abort",
			state.ownershipAbortListener,
		);
		state.controller.abort();
		if (this._sendStates.get(state.peerHash) === state) {
			this._sendStates.delete(state.peerHash);
		}
	}

	private trackRetiringWorker(state: ReplicationInfoV2SendState): void {
		const worker = state.worker;
		if (!worker) {
			return;
		}
		const previous = this._retiringWorkersByPeer.get(state.peerHash);
		if (previous === worker) {
			return;
		}
		const retirement = previous
			? Promise.allSettled([previous, worker]).then(() => undefined)
			: worker;
		this._retiringWorkersByPeer.set(state.peerHash, retirement);
		const forget = () => {
			if (this._retiringWorkersByPeer.get(state.peerHash) === retirement) {
				this._retiringWorkersByPeer.delete(state.peerHash);
			}
		};
		void retirement.then(forget, forget);
	}

	private isDestinationCurrent(state: ReplicationInfoV2SendState): boolean {
		return (
			!this.deps.isClosed() &&
			!state.controller.signal.aborted &&
			this._sendStates.get(state.peerHash) === state &&
			this.deps.isPeerSessionCurrent(state.peerHash, state.peerSession) &&
			this.deps.getSenderTransportSession() === state.senderTransportSession
		);
	}

	private isDestinationReady(state: ReplicationInfoV2SendState): boolean {
		return (
			this.isDestinationCurrent(state) &&
			this.deps.isPeerSessionOpen(state.peerHash, state.peerSession)
		);
	}

	private isCurrent(state: ReplicationInfoV2SendState): boolean {
		return (
			this.isDestinationReady(state) &&
			this.deps.isReplicationOwnershipLifecycleActive(
				state.ownershipLifecycleController,
			)
		);
	}

	private retireSpentState(state: ReplicationInfoV2SendState): void {
		this._spentPeerSessions.add(state.peerSession);
		this.clearState(state);
	}

	/**
	 * Collapse every interrupted normal-send path to one authoritative snapshot.
	 * A closed readiness gate is temporary while the exact PeerSession remains
	 * current, so it parks instead of destroying the receiver binding. Sequence
	 * exhaustion is terminal even when readiness or ownership changed at the same
	 * time as an in-flight transport attempt.
	 */
	private parkSnapshotForRetry(state: ReplicationInfoV2SendState): void {
		state.inFlightSequence = undefined;
		if (state.nextSequence > MAX_U64) {
			this.retireSpentState(state);
			return;
		}
		if (!this.isDestinationCurrent(state)) {
			this.clearState(state);
			return;
		}
		if (
			!this.deps.isReplicationOwnershipLifecycleActive(
				state.ownershipLifecycleController,
			)
		) {
			if (state.retryTimer) {
				clearTimeout(state.retryTimer);
				state.retryTimer = undefined;
			}
			state.pending = undefined;
			return;
		}

		state.suspended = true;
		state.pending = { kind: "snapshot" };
		this.scheduleRetry(state);
	}

	/**
	 * Accept a signed receiver request. An exact newer retry asks for another
	 * full snapshot without resetting the epoch/sequence. A different challenge
	 * can replace the binding only after a strictly newer signed capability;
	 * retiring work is drained before the replacement can start.
	 */
	acceptRequest(
		request: RequestReplicationInfoV2Message,
		properties: {
			from: PublicSignKey;
			peerSession: object;
			receiverTransportSession: bigint;
			capabilityTimestamp: bigint;
			requestTimestamp: bigint;
		},
	): boolean {
		const self = this.deps.getSelfKey();
		const senderTransportSession = this.deps.getSenderTransportSession();
		if (
			properties.from.equals(self) ||
			!request.intendedSender.equals(self) ||
			request.senderSession !== senderTransportSession
		) {
			return false;
		}

		const peerHash = properties.from.hashcode();
		if (
			this._retiringWorkersByPeer.has(peerHash) ||
			this._spentPeerSessions.has(properties.peerSession) ||
			!this.deps.isPeerSessionCurrent(peerHash, properties.peerSession) ||
			!this.deps.isPeerSessionOpen(peerHash, properties.peerSession)
		) {
			return false;
		}

		let previous = this._sendStates.get(peerHash);
		if (
			previous &&
			(previous.peerSession !== properties.peerSession ||
				previous.senderTransportSession !== senderTransportSession)
		) {
			this.clearState(previous);
			previous = undefined;
			if (this._retiringWorkersByPeer.has(peerHash)) {
				return false;
			}
		}
		if (
			previous &&
			!this.deps.isReplicationOwnershipLifecycleActive(
				previous.ownershipLifecycleController,
			)
		) {
			// Ownership teardown retains the binding only so close/drop can send its
			// terminal empty Full. A later request must not revive normal delivery.
			return false;
		}
		if (previous) {
			const sameBinding =
				previous.peerSession === properties.peerSession &&
				previous.receiverTransportSession ===
					properties.receiverTransportSession &&
				bytesEqual(
					previous.receiverRequestChallenge,
					request.receiverChallenge,
				);
			if (sameBinding) {
				if (properties.requestTimestamp <= previous.lastRequestTimestamp) {
					return false;
				}
				previous.lastRequestTimestamp = properties.requestTimestamp;
				previous.capabilityTimestamp = properties.capabilityTimestamp;
				if (previous.retryTimer) {
					clearTimeout(previous.retryTimer);
					previous.retryTimer = undefined;
				}
				previous.suspended = false;
				previous.pending = { kind: "snapshot" };
				this.enqueueState(previous, { kind: "snapshot" });
				return true;
			}

			if (properties.capabilityTimestamp <= previous.capabilityTimestamp) {
				return false;
			}
			this.clearState(previous);
			previous = undefined;
			if (this._retiringWorkersByPeer.has(peerHash)) {
				return false;
			}
		}

		const ownershipLifecycleController =
			this.deps.captureReplicationOwnershipLifecycle();
		if (
			!this.deps.isReplicationOwnershipLifecycleActive(
				ownershipLifecycleController,
			)
		) {
			return false;
		}
		const state: ReplicationInfoV2SendState = {
			peerHash,
			target: properties.from,
			peerSession: properties.peerSession,
			receiverTransportSession: properties.receiverTransportSession,
			senderTransportSession,
			capabilityTimestamp: properties.capabilityTimestamp,
			lastRequestTimestamp: properties.requestTimestamp,
			receiverRequestChallenge: request.receiverChallenge.slice(),
			receiverChallenge: deriveReplicationInfoV2ReceiverBinding({
				receiverChallenge: request.receiverChallenge,
				receiver: properties.from,
				receiverTransportSession: properties.receiverTransportSession,
				sender: self,
				senderTransportSession,
			}),
			senderEpoch: this._senderEpoch.slice(),
			ownershipAbortListener: () => {},
			nextSequence: 1n,
			established: false,
			suspended: false,
			retryAttempts: 0,
			controller: new AbortController(),
			ownershipLifecycleController,
		};
		state.ownershipAbortListener = () => {
			if (state.retryTimer) {
				clearTimeout(state.retryTimer);
				state.retryTimer = undefined;
			}
			state.pending = undefined;
		};
		ownershipLifecycleController.signal.addEventListener(
			"abort",
			state.ownershipAbortListener,
			{ once: true },
		);
		this._sendStates.set(peerHash, state);
		this.enqueueState(state, { kind: "snapshot" });
		return true;
	}

	enqueue(message: LegacyReplicationInfoMessage): void {
		for (const state of [...this._sendStates.values()]) {
			this.enqueueState(state, { kind: "message", message });
		}
	}

	enqueueSnapshotForPeer(peerHash: string): void {
		const state = this._sendStates.get(peerHash);
		if (state) {
			this.enqueueState(state, { kind: "snapshot" });
		}
	}

	private enqueueState(
		state: ReplicationInfoV2SendState,
		request: SendRequest,
	): void {
		if (state.nextSequence > MAX_U64 || !this.isCurrent(state)) {
			this.parkSnapshotForRetry(state);
			return;
		}
		if (state.suspended) {
			// Delivery is ambiguous while the backoff is armed. Never retain a
			// potentially stale delta: one authoritative Full represents every
			// mutation that arrives before the retry fires.
			this.parkSnapshotForRetry(state);
			return;
		}

		if (!state.worker) {
			state.pending = request;
			let worker: Promise<void>;
			worker = Promise.resolve()
				.then(() => this.runWorker(state))
				.catch((error) => {
					// Sequence cleanup and exhaustion fencing must happen before any
					// readiness/ownership classification in the recovery path.
					this.parkSnapshotForRetry(state);
					if (
						this._sendStates.get(state.peerHash) === state &&
						this.deps.isReplicationOwnershipLifecycleActive(
							state.ownershipLifecycleController,
						) &&
						!state.controller.signal.aborted &&
						!this.deps.isClosed()
					) {
						logger.trace(
							"Replication-info V2 destination stream failed for %s: %s",
							state.peerHash,
							(error as Error)?.message ?? String(error),
						);
					}
				})
				.finally(() => {
					if (state.worker === worker) {
						state.worker = undefined;
						// An enqueue can land after runWorker observes an empty slot but
						// before this promise reaction clears `worker`. Re-arm that item
						// here so the one-slot bound cannot become a stranded queue.
						const pending = state.pending;
						if (pending) {
							state.pending = undefined;
							this.enqueueState(state, pending);
						}
					}
				});
			state.worker = worker;
			return;
		}

		if (!state.pending) {
			state.pending = request;
			return;
		}

		// One pending item is the hard bound. Once another mutation arrives,
		// replace the pending delta with a current authoritative snapshot.
		state.pending = { kind: "snapshot" };
	}

	private retryDelay(state: ReplicationInfoV2SendState): number {
		const exponent = Math.max(0, state.retryAttempts - 1);
		return Math.min(
			this.maxSendRetryMs,
			this.sendRetryMs * 2 ** Math.min(exponent, MAX_BACKOFF_EXPONENT),
		);
	}

	private scheduleRetry(state: ReplicationInfoV2SendState): void {
		if (state.retryTimer) {
			return;
		}
		if (state.nextSequence > MAX_U64) {
			this.retireSpentState(state);
			return;
		}
		if (!this.isDestinationCurrent(state)) {
			this.clearState(state);
			return;
		}
		if (
			!this.deps.isReplicationOwnershipLifecycleActive(
				state.ownershipLifecycleController,
			)
		) {
			state.pending = undefined;
			return;
		}
		state.retryAttempts = Math.min(
			state.retryAttempts + 1,
			MAX_BACKOFF_EXPONENT + 1,
		);
		state.retryTimer = setTimeout(() => {
			state.retryTimer = undefined;
			if (state.nextSequence > MAX_U64 || !this.isCurrent(state)) {
				this.parkSnapshotForRetry(state);
				return;
			}
			state.suspended = false;
			state.pending = { kind: "snapshot" };
			this.enqueueState(state, { kind: "snapshot" });
		}, this.retryDelay(state));
		state.retryTimer.unref?.();
	}

	private async createMessage(
		state: ReplicationInfoV2SendState,
		request: SendRequest,
	): Promise<
		| FullReplicationInfoV2Message
		| AddedReplicationInfoV2Message
		| StoppedReplicationInfoV2Message
	> {
		const common = {
			receiverChallenge: state.receiverChallenge.slice(),
			senderEpoch: state.senderEpoch.slice(),
			sequence: state.nextSequence,
		};
		if (request.kind === "snapshot") {
			const segments = (await this.deps.getMyReplicationSegments()).map(
				(range) => range.toReplicationRange(),
			);
			this.deps.validatePersistedReplicationRangeSnapshot(segments);
			return new FullReplicationInfoV2Message({ ...common, segments });
		}
		if (request.message instanceof AllReplicatingSegmentsMessage) {
			const segments = request.message.segments;
			this.deps.validatePersistedReplicationRangeSnapshot(segments);
			return new FullReplicationInfoV2Message({ ...common, segments });
		}
		if (request.message instanceof AddedReplicationSegmentMessage) {
			return new AddedReplicationInfoV2Message({
				...common,
				segments: request.message.segments,
			});
		}
		return new StoppedReplicationInfoV2Message({
			...common,
			segmentIds: request.message.segmentIds,
		});
	}

	private async runWorker(state: ReplicationInfoV2SendState): Promise<void> {
		while (true) {
			if (state.nextSequence > MAX_U64 || !this.isCurrent(state)) {
				this.parkSnapshotForRetry(state);
				return;
			}
			const request = state.pending;
			if (!request) {
				return;
			}
			state.pending = undefined;
			const message = await this.createMessage(state, request);
			if (state.nextSequence > MAX_U64 || !this.isCurrent(state)) {
				this.parkSnapshotForRetry(state);
				return;
			}
			// Consume the sequence before the transport attempt. From this point on
			// delivery is ambiguous even if ownership aborts before the await
			// continuation runs, so `nextSequence` always remains the next value that
			// has never been attempted with different content.
			state.inFlightSequence = state.nextSequence;
			state.nextSequence += 1n;
			await this.deps.getRpc().send(message, {
				mode: new AcknowledgeDelivery({
					to: [state.target],
					redundancy: 1,
				}),
				priority: CONVERGENCE_MESSAGE_PRIORITY,
				signal: AbortSignal.any([
					state.controller.signal,
					state.ownershipLifecycleController.signal,
				]),
			});
			state.inFlightSequence = undefined;
			if (state.nextSequence > MAX_U64 || !this.isCurrent(state)) {
				this.parkSnapshotForRetry(state);
				return;
			}
			state.retryAttempts = 0;
			state.established = true;
		}
	}

	/**
	 * Terminal close/drop runs after the normal ownership generation is aborted.
	 * Send an authoritative empty Full directly from the retained destination
	 * bindings. `nextSequence` is consumed before every transport attempt, so it
	 * is always safe for different terminal content even if a just-aborted frame
	 * was delivered. Receivers admit Full gaps as authoritative resynchronization.
	 */
	async sendTerminalReset(signal: AbortSignal): Promise<void> {
		const sends: Promise<unknown>[] = [];
		for (const state of [...this._sendStates.values()]) {
			if (
				signal.aborted ||
				!this.isDestinationCurrent(state) ||
				state.nextSequence > MAX_U64
			) {
				continue;
			}
			state.pending = undefined;
			const sequence = state.nextSequence;
			state.nextSequence += 1n;
			const message = new FullReplicationInfoV2Message({
				receiverChallenge: state.receiverChallenge.slice(),
				senderEpoch: state.senderEpoch.slice(),
				sequence,
				segments: [],
			});
			if (sequence === MAX_U64) {
				// Retire the exhausted normal stream before transport invocation. The
				// terminal attempt remains valid because it carries its caller-owned
				// signal rather than the state controller that clearState aborts.
				this.retireSpentState(state);
			}
			try {
				sends.push(
					Promise.resolve(
						this.deps.getRpc().send(message, {
							mode: new AcknowledgeDelivery({
								to: [state.target],
								redundancy: 1,
							}),
							priority: CONVERGENCE_MESSAGE_PRIORITY,
							signal,
						}),
					),
				);
			} catch (error) {
				// Keep one synchronous transport failure isolated to its destination;
				// the sequence was already consumed and later peers must still reset.
				sends.push(Promise.reject(error));
			}
		}
		if (sends.length === 0 || signal.aborted) {
			return;
		}

		// Terminal reset is best-effort and the caller owns its close/drop bound.
		// A transport is expected to observe the signal, but a disconnected
		// acknowledgement path can leave its promise pending indefinitely. Do not
		// let such a transport retain the whole terminal operation past the bound.
		await new Promise<void>((resolve) => {
			let finished = false;
			const finish = () => {
				if (finished) return;
				finished = true;
				signal.removeEventListener("abort", finish);
				resolve();
			};
			signal.addEventListener("abort", finish, { once: true });
			void Promise.allSettled(sends).then(finish);
			// Close the check/listener race if the caller aborted synchronously.
			if (signal.aborted) finish();
		});
	}

	async drain(signal?: AbortSignal): Promise<void> {
		while (!signal?.aborted) {
			const workers = [
				...[...this._sendStates.values()]
					.map((state) => state.worker)
					.filter((worker): worker is Promise<void> => worker != null),
				...this._retiringWorkersByPeer.values(),
			];
			const uniqueWorkers = [...new Set(workers)];
			if (uniqueWorkers.length === 0) {
				return;
			}
			const settled = Promise.allSettled(uniqueWorkers).then(() => undefined);
			if (!signal) {
				await settled;
			} else {
				await new Promise<void>((resolve) => {
					const onAbort = () => resolve();
					signal.addEventListener("abort", onAbort, { once: true });
					void settled.then(() => {
						signal.removeEventListener("abort", onAbort);
						resolve();
					});
				});
			}
		}
	}
}
