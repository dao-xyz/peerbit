import { field, option, variant, vec } from "@dao-xyz/borsh";
import type { PeerId } from "@libp2p/interface";
import { PublicSignKey, getPublicKeyFromPeerId } from "@peerbit/crypto";
import { Message } from "./message.js";

@variant(7)
export abstract class BlocksMessage extends Message {}

@variant(0)
export class REQ_PutBlock extends BlocksMessage {
	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(bytes: Uint8Array) {
		super();
		this.bytes = bytes;
	}
}
@variant(1)
export class RESP_PutBlock extends BlocksMessage {
	@field({ type: "string" })
	cid: string;

	constructor(cid: string) {
		super();
		this.cid = cid;
	}
}

class RemoteGetOptions {
	@field({ type: "bool" })
	replicate: boolean;

	@field({ type: option("u32") })
	timeout?: number;

	constructor(options?: { replicate?: boolean; timeout?: number }) {
		this.replicate = options?.replicate || false;
		this.timeout = options?.timeout;
	}
}
@variant(2)
export class REQ_GetBlock extends BlocksMessage {
	@field({ type: "string" })
	cid: string;

	@field({ type: option(RemoteGetOptions) })
	remote?: RemoteGetOptions;

	constructor(
		cid: string,
		options?: { remote?: { timeout?: number; replicate?: boolean } | boolean },
	) {
		super();
		this.cid = cid;
		const remoteOptions = options?.remote;
		if (typeof remoteOptions === "boolean") {
			this.remote = options ? new RemoteGetOptions() : undefined;
		} else {
			this.remote = remoteOptions
				? new RemoteGetOptions(remoteOptions)
				: undefined;
		}
	}
}

@variant(3)
export class RESP_GetBlock extends BlocksMessage {
	@field({ type: option(Uint8Array) })
	bytes?: Uint8Array;

	constructor(bytes?: Uint8Array) {
		super();
		this.bytes = bytes;
	}
}

@variant(4)
export class REQ_HasBlock extends BlocksMessage {
	@field({ type: "string" })
	cid: string;

	constructor(cid: string) {
		super();
		this.cid = cid;
	}
}

@variant(5)
export class RESP_HasBlock extends BlocksMessage {
	@field({ type: "bool" })
	has: boolean;

	constructor(has: boolean) {
		super();
		this.has = has;
	}
}

@variant(6)
export class REQ_RmBlock extends BlocksMessage {
	@field({ type: "string" })
	cid: string;

	constructor(cid: string) {
		super();
		this.cid = cid;
	}
}
@variant(7)
export class RESP_RmBlock extends BlocksMessage {}

@variant(8)
export class REQ_Iterator extends BlocksMessage {
	@field({ type: "u32" })
	step: number;

	constructor() {
		super();
		this.step = 1;
	}
}
@variant(9)
export class RESP_Iterator extends BlocksMessage {
	@field({ type: vec("string") })
	keys: string[];

	@field({ type: vec(Uint8Array) })
	values: Uint8Array[];

	constructor(keys: string[], values: Uint8Array[]) {
		super();
		this.keys = keys;
		this.values = values;
	}
}

@variant(10)
export class REQ_BlockWaitFor extends BlocksMessage {
	@field({ type: "string" })
	hash: string;

	constructor(publicKey: PeerId | PublicSignKey | string) {
		super();
		this.hash =
			typeof publicKey === "string"
				? publicKey
				: publicKey instanceof PublicSignKey
					? publicKey.hashcode()
					: getPublicKeyFromPeerId(publicKey).hashcode();
	}
}

@variant(11)
export class RESP_BlockWaitFor extends BlocksMessage {}

@variant(12)
export class REQ_BlockSize extends BlocksMessage {}

@variant(13)
export class RESP_BlockSize extends BlocksMessage {
	@field({ type: "u64" })
	private _size: bigint;

	constructor(size: number) {
		super();
		this._size = BigInt(size);
	}

	get size() {
		return Number(this._size);
	}
}

@variant(14)
export class REQ_Persisted extends BlocksMessage {}

@variant(15)
export class RESP_Persisted extends BlocksMessage {
	@field({ type: "bool" })
	private _persisted: boolean;

	constructor(properties: { persisted: boolean }) {
		super();
		this._persisted = properties.persisted;
	}
	get persisted(): boolean {
		return this._persisted;
	}
}
