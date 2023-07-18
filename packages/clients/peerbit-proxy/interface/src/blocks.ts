import type { PeerId } from "@libp2p/interface-peer-id";
import { getPublicKeyFromPeerId } from "@peerbit/crypto";
import { field, variant, option } from "@dao-xyz/borsh";
import { PublicSignKey } from "@peerbit/crypto";
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

@variant(2)
export class REQ_GetBlock extends BlocksMessage {
	@field({ type: "string" })
	cid: string;

	@field({ type: "bool" })
	replicate: boolean;

	@field({ type: option("u32") })
	timeout?: number;

	constructor(
		cid: string,
		options?: { timeout?: number; replicate?: boolean }
	) {
		super();
		this.cid = cid;
		this.timeout = options?.timeout;
		this.replicate = options?.replicate || false;
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
export class REQ_BlockWaitFor extends BlocksMessage {
	@field({ type: PublicSignKey })
	publicKey: PublicSignKey;
	constructor(publicKey: PeerId | PublicSignKey) {
		super();
		this.publicKey =
			publicKey instanceof PublicSignKey
				? publicKey
				: getPublicKeyFromPeerId(publicKey);
	}
}

@variant(9)
export class RESP_BlockWaitFor extends BlocksMessage {}
