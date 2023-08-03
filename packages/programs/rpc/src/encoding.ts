import { field, fixedArray, option, variant } from "@dao-xyz/borsh";
import { MaybeEncrypted, X25519PublicKey } from "@peerbit/crypto";

@variant(0)
export abstract class RPCMessage { }

@variant(0)
export class RequestV0 extends RPCMessage {
	@field({ type: option(X25519PublicKey) })
	respondTo?: X25519PublicKey;

	@field({ type: Uint8Array })
	request: Uint8Array;

	constructor(properties: {
		request: Uint8Array;
		respondTo?: X25519PublicKey;
	}) {
		super();
		this.respondTo = properties.respondTo;
		this.request = properties.request;
	}
}

@variant(1)
export class ResponseV0 extends RPCMessage {
	@field({ type: fixedArray("u8", 32) })
	requestId: Uint8Array;

	@field({ type: Uint8Array })
	response: Uint8Array;

	constructor(properties: {
		response: Uint8Array;
		requestId: Uint8Array;
	}) {
		super();
		this.response = properties.response;
		this.requestId = properties.requestId;
	}
}
