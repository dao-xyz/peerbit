import { field, fixedArray, option, variant } from "@dao-xyz/borsh";
import { MaybeEncrypted, X25519PublicKey, randomBytes } from "@peerbit/crypto";

@variant(0)
export abstract class RPCMessage {}

@variant(0)
export class RequestV0 extends RPCMessage {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array;

	@field({ type: option(X25519PublicKey) })
	respondTo?: X25519PublicKey;

	@field({ type: MaybeEncrypted })
	request: MaybeEncrypted<any>;

	constructor(properties: {
		request: MaybeEncrypted<any>;
		respondTo?: X25519PublicKey;
	}) {
		super();
		this.id = randomBytes(32);
		this.respondTo = properties.respondTo;
		this.request = properties.request;
	}
}

@variant(1)
export class ResponseV0 extends RPCMessage {
	@field({ type: fixedArray("u8", 32) })
	requestId: Uint8Array;

	@field({ type: MaybeEncrypted })
	response: MaybeEncrypted<any>;

	constructor(properties: {
		response: MaybeEncrypted<any>;
		requestId: Uint8Array;
	}) {
		super();
		this.response = properties.response;
		this.requestId = properties.requestId;
	}
}
