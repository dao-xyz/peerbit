import { variant, deserialize, serialize, field } from "@dao-xyz/borsh";
import { Entry, Log } from "@peerbit/log";
import { ProgramClient } from "@peerbit/program";
import { RPC } from "@peerbit/rpc";

interface SharedLog {
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
	getValue(log: SharedLog): number {
		return this._value;
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

export const decodeReplicas = (entry: {
	meta: { data?: Uint8Array };
}): MinReplicas => {
	return deserialize(entry.meta.data!, MinReplicas);
};

export const maxReplicas = (
	log: SharedLog,
	entries: { meta: { data?: Uint8Array } }[]
) => {
	let max = 0;
	for (const entry of entries) {
		max = Math.max(decodeReplicas(entry).getValue(log), max);
	}
	return max;
};
