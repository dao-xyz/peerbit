import { variant, deserialize, serialize, field } from "@dao-xyz/borsh";
import { TransportMessage } from "./message.js";
import { Role } from "./role.js";
export type ReplicationLimits = { min: MinReplicas; max?: MinReplicas };

interface SharedLog {
	replicas: Partial<ReplicationLimits>;
	getReplicatorsSorted(): { hash: string; timestamp: number }[] | undefined;
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

@variant([2, 0])
export class ResponseRoleMessage extends TransportMessage {
	@field({ type: Role })
	role: Role;

	constructor(role: Role) {
		super();
		this.role = role;
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
