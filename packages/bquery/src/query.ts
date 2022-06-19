import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import { API as PubSubAPI, Message } from 'ipfs-core-types/src/pubsub';
import { v4 as uuid } from 'uuid';
import { QueryType } from "./query-type";
import { Result } from "./result";
import { delay } from "./utils";

@variant(0)
export class QueryRequestV0 {

    @field({ type: 'String' })
    id: string

    @field({ type: QueryType })
    type: QueryType

    constructor(obj?: {
        id?: string
        type: QueryType
    }) {
        if (obj) {
            Object.assign(this, obj);
            if (!this.id) {
                this.id = uuid();
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


export const query = async (pubsub: PubSubAPI, topic: string, query: QueryRequestV0, responseHandler: (response: QueryResponseV0) => void, maxAggregationTime: number = 30 * 1000) => {
    // send query and wait for replies in a generator like behaviour
    let responseTopic = query.getResponseTopic(topic);
    await pubsub.subscribe(responseTopic, (msg: Message) => {
        try {
            const result = deserialize(Buffer.from(msg.data), QueryResponseV0);
            responseHandler(result);
        } catch (error) {
            console.error("failed ot deserialize query response", error);
            throw error;
        }
    }, {
        timeout: maxAggregationTime
    });
    await pubsub.publish(topic, serialize(query));
    await delay(maxAggregationTime);
    await pubsub.unsubscribe(responseTopic);
}