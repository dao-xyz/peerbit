import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import { API as PubSubAPI, Message } from 'ipfs-core-types/src/pubsub';
import { v4 as uuid } from 'uuid';
import { QueryType } from "./query-interface";
import { Result } from "./result";
import { delay, waitFor } from "@dao-xyz/time";
@variant(0)
export class QueryRequestV0 {

    @field({ type: 'string' })
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


export const query = async (pubsub: PubSubAPI, topic: string, query: QueryRequestV0, responseHandler: (response: QueryResponseV0) => void, waitForAmount?: number, maxAggregationTime: number = 30 * 1000) => {
    // send query and wait for replies in a generator like behaviour
    let responseTopic = query.getResponseTopic(topic);
    let results = 0;
    const _responseHandler = (msg: Message) => {
        try {
            const result = deserialize(Buffer.from(msg.data), QueryResponseV0);
            responseHandler(result);
            results += 1;
        } catch (error) {
            console.error("failed ot deserialize query response", error);
            throw error;
        }
    };
    try {
        await pubsub.subscribe(responseTopic, _responseHandler, {
            timeout: maxAggregationTime
        });
    } catch (error) {
        // timeout
        if (error.constructor.name != "TimeoutError") {
            throw new Error("Got unexpected error when query");
        }
    }
    const signedMessage = await (new MaybeSigned({ data })).sign(await this.getSigner());
    const maybeEncryptedMessage = new DecryptedThing<MaybeSigned<Uint8Array>>({
        data: serialize(signedMessage)
    }).encrypt(reciever)

    await pubsub.publish(topic, serialize(query));
    if (waitForAmount != undefined) {
        await waitFor(() => results >= waitForAmount, { timeout: maxAggregationTime, delayInterval: 50 })
    }
    else {
        await delay(maxAggregationTime);

    }
    await pubsub.unsubscribe(responseTopic, _responseHandler);
}