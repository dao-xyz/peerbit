import { field, option, variant, vec } from "@dao-xyz/borsh";
import { X25519PublicKey } from "@dao-xyz/peerbit-crypto";
import { ProtocolMessage } from "@dao-xyz/peerbit-program";

@variant(0)
export abstract class RPCMessage extends ProtocolMessage {}

@variant(0)
export class RequestV0 extends RPCMessage {
	@field({ type: X25519PublicKey })
	respondTo: X25519PublicKey;

	@field({ type: option("string") })
	context?: string;

	@field({ type: Uint8Array })
	request: Uint8Array;

	constructor(properties: {
		request: Uint8Array;
		respondTo: X25519PublicKey;
		context?: string;
	}) {
		super();
		this.respondTo = properties.respondTo;
		this.request = properties.request;
		this.context = properties.context;
	}
}

@variant(1)
export class ResponseV0 extends RPCMessage {
	@field({ type: Uint8Array })
	response: Uint8Array;

	@field({ type: "string" })
	context: string;

	constructor(properties: { response: Uint8Array; context: string }) {
		super();
		this.response = properties.response;
		this.context = properties.context;
	}
}
