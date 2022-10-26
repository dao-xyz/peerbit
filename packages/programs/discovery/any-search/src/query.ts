import { field, variant, vec } from "@dao-xyz/borsh";
// @ts-ignore
import { v4 as uuid } from 'uuid';
import { QueryType } from './query-interface.js';
import { Result } from './result.js';
import { X25519PublicKey } from '@dao-xyz/peerbit-crypto'

/* 
@variant(0)
export class QueryRequestV0 {

    @field({ type: 'string' })
    id: string

    @field({ type: QueryType })
    type: QueryType

    @field({ type: vec(X25519PublicKey) })
    responseRecievers: X25519PublicKey[]

    constructor(properties?: {
        id?: string
        type: QueryType
        responseRecievers?: X25519PublicKey[]
    }) {
        if (properties) {
            this.id = properties.id || uuid();
            this.responseRecievers = properties.responseRecievers || [];
            this.type = properties.type;
        }
    }

    getResponseTopic(topic: string): string {
        return topic + '/' + this.id
    }

}

@variant(0)
export class QueryResponseV0 {

    @field({ type: vec(Result) })
    results: Result[]
    constructor(properties?: {
        results: Result[]

    }) {
        if (properties) {
            this.results = properties.results;

        }
    }
}

 */

