import { Constructor, deserialize, field, option, serialize, variant, vec } from "@dao-xyz/borsh";
import { v4 as uuid } from 'uuid';
import { QueryType } from './query-interface.js';
import { Result } from './result.js';
import { bufferSerializer } from '@dao-xyz/borsh-utils';
import { X25519PublicKey } from 'sodium-plus'
@variant(0)
export class QueryRequestV0 {

    @field({ type: 'string' })
    id: string

    @field({ type: QueryType })
    type: QueryType

    @field({ type: vec(bufferSerializer(X25519PublicKey)) })
    recievers: X25519PublicKey[]

    constructor(obj?: {
        id?: string
        type: QueryType
        recievers?: X25519PublicKey[]
    }) {
        if (obj) {
            Object.assign(this, obj);
            if (!this.id) {
                this.id = uuid();
            }
            if (!this.recievers) {
                this.recievers = [];
            }
        }
    }

    getResponseTopic(topic: string): string {
        return topic + '/' + this.id
    }

}

@variant(0)
export class QueryResponseV0 {

    @field({ type: vec(Result) })
    results: Result[] // base58 encoded
    constructor(obj?: {
        results: Result[]

    }) {
        if (obj) {
            Object.assign(this, obj);
        }
    }
}



