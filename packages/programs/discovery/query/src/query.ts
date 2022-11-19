import { field, option, variant, vec } from "@dao-xyz/borsh";
import { v4 as uuid } from "uuid";
import { X25519PublicKey } from "@dao-xyz/peerbit-crypto";
import { UInt8ArraySerializer } from "@dao-xyz/peerbit-borsh-utils";

@variant(0)
export class QueryRequestV0 {
    @field({ type: "string" })
    id: string;

    @field({ type: vec(X25519PublicKey) })
    responseRecievers: X25519PublicKey[];

    @field(UInt8ArraySerializer)
    query: Uint8Array;

    @field({ type: option("string") })
    context?: string;

    constructor(properties?: {
        id?: string;
        query: Uint8Array;
        responseRecievers?: X25519PublicKey[];
        context?: string;
    }) {
        if (properties) {
            this.id = properties.id || uuid();
            this.responseRecievers = properties.responseRecievers || [];
            this.query = properties.query;
            this.context = properties.context;
        }
    }

    getResponseTopic(topic: string): string {
        return topic + "/" + this.id;
    }
}

@variant(0)
export class QueryResponseV0 {
    @field(UInt8ArraySerializer)
    response: Uint8Array;

    @field({ type: "string" })
    context: string;

    constructor(properties?: { response: Uint8Array; context: string }) {
        if (properties) {
            this.response = properties.response;
            this.context = properties.context;
        }
    }
}
