import { field, fixedArray, variant } from "@dao-xyz/borsh";
import { X25519PublicKey, randomBytes } from "@dao-xyz/peerbit-crypto";

@variant(0)
export abstract class RPCMessage {}

@variant(0)
export class RequestV0 extends RPCMessage {
	@field({ type: fixedArray("u8", 32) })
	id: Uint8Array;

	@field({ type: X25519PublicKey })
	respondTo: X25519PublicKey;

	@field({ type: Uint8Array })
	request: Uint8Array;

	constructor(properties: { request: Uint8Array; respondTo: X25519PublicKey }) {
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

	@field({ type: Uint8Array })
	response: Uint8Array;

	constructor(properties: { response: Uint8Array; requestId: Uint8Array }) {
		super();
		this.response = properties.response;
		this.requestId = properties.requestId;
	}
}
