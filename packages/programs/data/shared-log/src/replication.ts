import {
	variant,
	deserialize,
	serialize,
	field,
	option,
	BinaryReader
} from "@dao-xyz/borsh";
import { TransportMessage } from "./message.js";
import { Observer, Replicator, Role } from "./role.js";
import { PublicSignKey } from "@peerbit/crypto";
import yallist from "yallist";

export type ReplicationLimits = { min: MinReplicas; max?: MinReplicas };

export type ReplicatorRect = {
	publicKey: PublicSignKey;
	role: Replicator;
};

interface SharedLog {
	replicas: Partial<ReplicationLimits>;
	getReplicatorsSorted(): yallist<ReplicatorRect> | undefined;
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
export class RequestRoleMessage extends TransportMessage {
	constructor() {
		super();
	}
}

@variant([1, 1])
export class ResponseRoleMessage extends TransportMessage {
	@field({ type: option(Role) })
	role: Observer | Replicator;

	constructor(properties: { role: Observer | Replicator }) {
		super();
		this.role = properties.role;
	}
}

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
		| IterableIterator<{ meta: { data?: Uint8Array } }>
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

export const hashToUniformNumber = (hash: Uint8Array) => {
	const seedNumber = new BinaryReader(
		hash.subarray(hash.length - 4, hash.length)
	).u32();
	return seedNumber / 0xffffffff; // bounded between 0 and 1
};
