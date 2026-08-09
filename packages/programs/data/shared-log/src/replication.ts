import {
	deserialize,
	field,
	fixedArray,
	option,
	serialize,
	variant,
	vec,
} from "@dao-xyz/borsh";
import { randomBytes } from "@peerbit/crypto";
import { type Index } from "@peerbit/indexer-interface";
import { TransportMessage } from "./message.js";
import {
	ReplicationIntent,
	type ReplicationRangeIndexable,
	ReplicationRangeMessage,
	ReplicationRangeMessageU32,
} from "./ranges.js";
import { Observer, Replicator, Role } from "./role.js";

export type ReplicationLimits = { min: MinReplicas; max?: MinReplicas };

interface SharedLog {
	replicas: Partial<ReplicationLimits>;
	replicationIndex: Index<ReplicationRangeIndexable<any>> | undefined;
}

export class MinReplicas {
	getValue(log: SharedLog): number {
		throw new Error("Not implemented");
	}
}

@variant(0)
export class AbsoluteReplicas extends MinReplicas {
	@field({ type: "u32" })
	_value: number;

	constructor(value: number) {
		super();
		this._value = value;
	}

	getValue(_log: SharedLog): number {
		return this._value;
	}
}

@variant([1, 0])
export class RequestReplicationInfoMessage extends TransportMessage {
	constructor() {
		super();
	}
}

// @deprecated remove when possible
@variant([1, 1])
export class ResponseRoleMessage extends TransportMessage {
	@field({ type: option(Role) })
	role: Observer | Replicator;

	constructor(properties: { role: Observer | Replicator }) {
		super();
		this.role = properties.role;
	}

	toReplicationInfoMessage(): AllReplicatingSegmentsMessage {
		return new AllReplicatingSegmentsMessage({
			segments:
				this.role instanceof Replicator
					? this.role.segments.map((x) => {
							return new ReplicationRangeMessageU32({
								id: randomBytes(32),
								offset: x.offset,
								factor: x.factor,
								timestamp: x.timestamp,
								mode: ReplicationIntent.NonStrict,
							});
						})
					: [],
		});
	}
}

@variant([1, 2])
export class AllReplicatingSegmentsMessage extends TransportMessage {
	@field({ type: vec(ReplicationRangeMessage) })
	segments: ReplicationRangeMessage<any>[];

	constructor(properties: { segments: ReplicationRangeMessage<any>[] }) {
		super();
		this.segments = properties.segments;
	}
}

@variant([1, 3])
export class AddedReplicationSegmentMessage extends TransportMessage {
	@field({ type: vec(ReplicationRangeMessage) })
	segments: ReplicationRangeMessage<any>[];

	constructor(properties: { segments: ReplicationRangeMessage<any>[] }) {
		super();
		this.segments = properties.segments;
	}
}

@variant([1, 4])
export class StoppedReplicating extends TransportMessage {
	@field({ type: vec(Uint8Array) })
	segmentIds: Uint8Array[];

	constructor(properties: { segmentIds: Uint8Array[] }) {
		super();
		this.segmentIds = properties.segmentIds;
	}
}

@variant([1, 5])
export class ReplicationPingMessage extends TransportMessage {
	// Lightweight unicast liveness probe for replicator membership.
	// This exists to avoid using RequestReplicationInfoMessage as a "ping"
	// (which can cause large replication segment snapshots to be sent).
	constructor() {
		super();
	}
}

/**
 * Decode-first replication-info V2 messages. This rollout only registers and
 * advertises the final wire schemas; it never sends or applies these messages.
 * A later negotiated phase must bind the signed transport session and an
 * outstanding receiver challenge before any V2 payload is used.
 */
@variant([1, 6])
export class FullReplicationInfoV2Message extends TransportMessage {
	@field({ type: fixedArray("u8", 32) })
	receiverChallenge: Uint8Array;

	@field({ type: fixedArray("u8", 32) })
	senderEpoch: Uint8Array;

	@field({ type: "u64" })
	sequence: bigint;

	@field({ type: vec(ReplicationRangeMessage) })
	segments: ReplicationRangeMessage<any>[];

	constructor(properties: {
		receiverChallenge: Uint8Array;
		senderEpoch: Uint8Array;
		sequence: bigint;
		segments: ReplicationRangeMessage<any>[];
	}) {
		super();
		this.receiverChallenge = properties.receiverChallenge;
		this.senderEpoch = properties.senderEpoch;
		this.sequence = properties.sequence;
		this.segments = properties.segments;
	}
}

@variant([1, 7])
export class AddedReplicationInfoV2Message extends TransportMessage {
	@field({ type: fixedArray("u8", 32) })
	receiverChallenge: Uint8Array;

	@field({ type: fixedArray("u8", 32) })
	senderEpoch: Uint8Array;

	@field({ type: "u64" })
	sequence: bigint;

	@field({ type: vec(ReplicationRangeMessage) })
	segments: ReplicationRangeMessage<any>[];

	constructor(properties: {
		receiverChallenge: Uint8Array;
		senderEpoch: Uint8Array;
		sequence: bigint;
		segments: ReplicationRangeMessage<any>[];
	}) {
		super();
		this.receiverChallenge = properties.receiverChallenge;
		this.senderEpoch = properties.senderEpoch;
		this.sequence = properties.sequence;
		this.segments = properties.segments;
	}
}

@variant([1, 8])
export class StoppedReplicationInfoV2Message extends TransportMessage {
	@field({ type: fixedArray("u8", 32) })
	receiverChallenge: Uint8Array;

	@field({ type: fixedArray("u8", 32) })
	senderEpoch: Uint8Array;

	@field({ type: "u64" })
	sequence: bigint;

	@field({ type: vec(Uint8Array) })
	segmentIds: Uint8Array[];

	constructor(properties: {
		receiverChallenge: Uint8Array;
		senderEpoch: Uint8Array;
		sequence: bigint;
		segmentIds: Uint8Array[];
	}) {
		super();
		this.receiverChallenge = properties.receiverChallenge;
		this.senderEpoch = properties.senderEpoch;
		this.sequence = properties.sequence;
		this.segmentIds = properties.segmentIds;
	}
}

export type ReplicationInfoV2Message =
	| FullReplicationInfoV2Message
	| AddedReplicationInfoV2Message
	| StoppedReplicationInfoV2Message;

export const isReplicationInfoV2Message = (
	message: TransportMessage,
): message is ReplicationInfoV2Message =>
	message instanceof FullReplicationInfoV2Message ||
	message instanceof AddedReplicationInfoV2Message ||
	message instanceof StoppedReplicationInfoV2Message;

/* 
@variant(1)
export class RelativeMinReplicas extends MinReplicas {
	_value: number; // (0, 1]
	
	constructor(value: number) {
		super();
		this._value = value;
	}
	getValue(log: SharedLog): number {
		return Math.ceil(this._value * log.getReplicatorsSorted()!.length); // TODO TYPES
	}
}
 */

export const encodeReplicas = (minReplicas: MinReplicas): Uint8Array => {
	return serialize(minReplicas);
};

export class ReplicationError extends Error {
	constructor(message: string) {
		super(message);
	}
}
export const decodeReplicas = (entry: {
	meta: { data?: Uint8Array };
}): MinReplicas => {
	if (!entry.meta.data) {
		throw new ReplicationError("Missing meta data from error");
	}
	return deserialize(entry.meta.data, MinReplicas);
};

export const maxReplicas = (
	log: SharedLog,
	entries:
		| { meta: { data?: Uint8Array } }[]
		| IterableIterator<{ meta: { data?: Uint8Array } }>,
) => {
	let max = 0;
	for (const entry of entries) {
		max = Math.max(decodeReplicas(entry).getValue(log), max);
	}
	const lower = log.replicas.min?.getValue(log) || 1;
	const higher = log.replicas.max?.getValue(log) ?? Number.MAX_SAFE_INTEGER;
	const numberOfLeaders = Math.max(Math.min(higher, max), lower);
	return numberOfLeaders;
};
