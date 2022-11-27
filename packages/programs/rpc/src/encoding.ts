import { field, option, variant, vec } from "@dao-xyz/borsh";
import { X25519PublicKey } from "@dao-xyz/peerbit-crypto";
import { UInt8ArraySerializer } from "@dao-xyz/peerbit-borsh-utils";
import { ProtocolMessage } from "@dao-xyz/peerbit-program";

@variant(0)
export abstract class RPCMessage extends ProtocolMessage {}

@variant(0)
export class RequestV0 extends RPCMessage {
    @field({ type: option(X25519PublicKey) })
    respondTo?: X25519PublicKey;

    @field({ type: option("string") })
    context?: string;

    @field(UInt8ArraySerializer)
    request: Uint8Array;

    constructor(properties?: {
        request: Uint8Array;
        respondTo?: X25519PublicKey;
        context?: string;
    }) {
        super();
        if (properties) {
            this.respondTo = properties.respondTo;
            this.request = properties.request;
            this.context = properties.context;
        }
    }
}

@variant(1)
export class ResponseV0 extends RPCMessage {
    @field(UInt8ArraySerializer)
    response: Uint8Array;

    @field({ type: "string" })
    context: string;

    constructor(properties?: { response: Uint8Array; context: string }) {
        super();
        if (properties) {
            this.response = properties.response;
            this.context = properties.context;
        }
    }
}
