import { field, option, variant, vec } from "@dao-xyz/borsh";
import { v4 as uuid } from "uuid";
import { X25519PublicKey } from "@dao-xyz/peerbit-crypto";
import { UInt8ArraySerializer } from "@dao-xyz/peerbit-borsh-utils";

export enum Compare {
    Equal = 0,
    Greater = 1,
    GreaterOrEqual = 2,
    Less = 3,
    LessOrEqual = 4,
}
export const compare = (
    test: bigint | number,
    compare: Compare,
    value: bigint | number
) => {
    switch (compare) {
        case Compare.Equal:
            return test == value; // == because with want bigint == number at some cases
        case Compare.Greater:
            return test > value;
        case Compare.GreaterOrEqual:
            return test >= value;
        case Compare.Less:
            return test < value;
        case Compare.LessOrEqual:
            return test <= value;
        default:
            console.warn("Unexpected compare");
            return false;
    }
};

@variant(0)
export class U64Compare {
    @field({ type: "u8" })
    compare: Compare;

    @field({ type: "u64" })
    value: bigint;

    constructor(props?: { value: bigint; compare: Compare }) {
        if (props) {
            this.compare = props.compare;
        }
    }
}

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

    @field({ type: vec(U64Compare) })
    created: U64Compare[];

    @field({ type: vec(U64Compare) })
    modified: U64Compare[];

    constructor(properties?: {
        id?: string;
        query: Uint8Array;
        responseRecievers?: X25519PublicKey[];
        context?: string;
        created?: U64Compare[];
        modified?: U64Compare[];
    }) {
        if (properties) {
            this.id = properties.id || uuid();
            this.responseRecievers = properties.responseRecievers || [];
            this.query = properties.query;
            this.context = properties.context;
            this.created = properties.created || [];
            this.modified = properties.modified || [];
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
