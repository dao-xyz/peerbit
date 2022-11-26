import { field, option, variant, vec } from "@dao-xyz/borsh";
import { v4 as uuid } from "uuid";
import { X25519PublicKey } from "@dao-xyz/peerbit-crypto";
import { UInt8ArraySerializer } from "@dao-xyz/peerbit-borsh-utils";
import { ProtocolMessage } from "@dao-xyz/peerbit-program";

@variant(0)
export abstract class RPCMessage extends ProtocolMessage {}

@variant(0)
export class RequestV0 extends RPCMessage {
    @field({ type: "string" })
    id: string;

    @field({ type: vec(X25519PublicKey) })
    responseRecievers: X25519PublicKey[];

    @field(UInt8ArraySerializer)
    request: Uint8Array;

    @field({ type: option("string") })
    context?: string;

    constructor(properties?: {
        id?: string;
        request: Uint8Array;
        responseRecievers?: X25519PublicKey[];
        context?: string;
    }) {
        super();
        if (properties) {
            this.id = properties.id || uuid();
            this.responseRecievers = properties.responseRecievers || [];
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
