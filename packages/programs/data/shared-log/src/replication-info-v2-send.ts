import { serialize } from "@dao-xyz/borsh";
import { type PublicSignKey, randomBytes, sha256Sync } from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import type { RPC } from "@peerbit/rpc";
import {
	AcknowledgeDelivery,
	CONVERGENCE_MESSAGE_PRIORITY,
} from "@peerbit/stream-interface";
import type { TransportMessage } from "./message.js";
import type { ReplicationRangeIndexable } from "./ranges.js";
import { concat, fromString } from "uint8arrays";
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
const RECEIVER_BINDING_DOMAIN = fromString(
	"peerbit/shared-log/replication-info-v2/receiver-binding/v1",
);

const lengthPrefixed = (bytes: Uint8Array): Uint8Array => {
	const length = new Uint8Array(4);
	new DataView(length.buffer).setUint32(0, bytes.byteLength, true);
	return concat([length, bytes]);
};

const u64LittleEndian = (value: bigint): Uint8Array => {
	const bytes = new Uint8Array(8);
	new DataView(bytes.buffer).setBigUint64(0, value, true);
	return bytes;
};

/**
 * Derive the token echoed by V2 data frames. Delivery recipients are unsigned,
 * so the receiver's nonce alone is not a destination binding. Folding both
 * authenticated identities and signed transport sessions into the 32-byte
 * field prevents a copied request nonce from creating an interchangeable
 * stream for another receiver.
 */
export const deriveReplicationInfoV2ReceiverBinding = (properties: {
	receiverChallenge: Uint8Array;
	receiver: PublicSignKey;
	receiverTransportSession: bigint;
	sender: PublicSignKey;
	senderTransportSession: bigint;
}): Uint8Array =>
	sha256Sync(
		concat([
			lengthPrefixed(RECEIVER_BINDING_DOMAIN),
			lengthPrefixed(properties.receiverChallenge),
			lengthPrefixed(serialize(properties.receiver)),
			u64LittleEndian(properties.receiverTransportSession),
			lengthPrefixed(serialize(properties.sender)),
			u64LittleEndian(properties.senderTransportSession),
		]),
	);

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
	lastRequestTimestamp: bigint;
	receiverRequestChallenge: Uint8Array;
	receiverChallenge: Uint8Array;
	senderEpoch: Uint8Array;
	ownershipLifecycleController: AbortController;
	nextSequence: bigint;
	established: boolean;
	suspended: boolean;
	inFlightSequence?: bigint;
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
	isPeerSessionOpen: (peerHash: string, peerSession: object) => boolean;
	captureReplicationOwnershipLifecycle: () => AbortController;
	isReplicationOwnershipLifecycleActive: (
		controller: AbortController,
	) => boolean;
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

	constructor(private readonly deps: ReplicationInfoV2SendDeps<R>) {
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
			this.deps.isPeerSessionOpen(state.peerHash, state.peerSession) &&
			this.deps.getSenderTransportSession() === state.senderTransportSession
		);
	}

	private isCurrent(state: ReplicationInfoV2SendState): boolean {
		return (
			this.isDestinationCurrent(state) &&
			this.deps.isReplicationOwnershipLifecycleActive(
				state.ownershipLifecycleController,
			)
		);
	}

	/**
	 * Accept a signed receiver request. An exact newer retry asks for another
	 * full snapshot without resetting the epoch/sequence. A different challenge
	 * cannot replace the first binding within one PeerSession; this prevents a
	 * signed request flood from spawning unbounded orphan snapshot reads.
	 */
	acceptRequest(
		request: RequestReplicationInfoV2Message,
		properties: {
			from: PublicSignKey;
			peerSession: object;
			receiverTransportSession: bigint;
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
				previous.suspended = false;
				this.enqueueState(previous, { kind: "snapshot" });
				return true;
			}

			return false;
		}

		const ownershipLifecycleController =
			this.deps.captureReplicationOwnershipLifecycle();
		const state: ReplicationInfoV2SendState = {
			peerHash,
			target: properties.from,
			peerSession: properties.peerSession,
			receiverTransportSession: properties.receiverTransportSession,
			senderTransportSession,
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
			nextSequence: 1n,
			established: false,
			suspended: false,
			controller: new AbortController(),
			ownershipLifecycleController,
		};
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
		if (!this.isCurrent(state)) {
			state.pending = undefined;
			if (
				!this.isDestinationCurrent(state) ||
				this.deps.isReplicationOwnershipLifecycleActive(
					state.ownershipLifecycleController,
				)
			) {
				this.clearState(state);
			}
			return;
		}
		if (state.suspended) {
			state.pending = undefined;
			return;
		}

		if (!state.worker) {
			state.pending = request;
			let worker: Promise<void>;
			worker = Promise.resolve()
				.then(() => this.runWorker(state))
				.catch((error) => {
					const ownershipActive =
						this.deps.isReplicationOwnershipLifecycleActive(
							state.ownershipLifecycleController,
						);
					if (
						ownershipActive &&
						!state.controller.signal.aborted &&
						!this.deps.isClosed()
					) {
						logger.trace(
							"Replication-info V2 destination stream failed for %s: %s",
							state.peerHash,
							(error as Error)?.message ?? String(error),
						);
					}
					state.pending = undefined;
					const ordinaryFailure =
						ownershipActive && this.isDestinationCurrent(state);
					if (ordinaryFailure) {
						if (state.inFlightSequence !== undefined) {
							state.inFlightSequence = undefined;
							if (state.nextSequence > MAX_U64) {
								this._spentPeerSessions.add(state.peerSession);
								this.clearState(state);
								return;
							}
						}
						// A delivery error is ambiguous: the receiver may already have
						// applied this sequence. Retain the exact grant, stop ordinary
						// deltas, and require a newer same-challenge request to resume
						// with an authoritative Full at the next safe sequence.
						state.suspended = true;
						return;
					}
					if (!this.isDestinationCurrent(state)) {
						this.clearState(state);
					}
				})
				.finally(() => {
					if (state.worker === worker) {
						state.worker = undefined;
						// An enqueue can land after runWorker observes an empty slot but
						// before this promise reaction clears `worker`. Re-arm that item
						// here so the one-slot bound cannot become a stranded queue.
						const pending = state.pending;
						if (pending && this.isCurrent(state)) {
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
		while (this.isCurrent(state)) {
			const request = state.pending;
			if (!request) {
				return;
			}
			state.pending = undefined;
			if (state.nextSequence > MAX_U64) {
				this.clearState(state);
				return;
			}

			const message = await this.createMessage(state, request);
			if (!this.isCurrent(state)) {
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
			if (!this.isCurrent(state)) {
				return;
			}
			state.established = true;
			if (state.nextSequence > MAX_U64) {
				this._spentPeerSessions.add(state.peerSession);
				this.clearState(state);
				return;
			}
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
			const message = new FullReplicationInfoV2Message({
				receiverChallenge: state.receiverChallenge.slice(),
				senderEpoch: state.senderEpoch.slice(),
				sequence: state.nextSequence,
				segments: [],
			});
			sends.push(
				this.deps.getRpc().send(message, {
					mode: new AcknowledgeDelivery({
						to: [state.target],
						redundancy: 1,
					}),
					priority: CONVERGENCE_MESSAGE_PRIORITY,
					signal,
				}),
			);
		}
		await Promise.allSettled(sends);
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
