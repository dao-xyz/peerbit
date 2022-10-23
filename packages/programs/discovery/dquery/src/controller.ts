import { Constructor, deserialize, field, option, serialize, variant } from '@dao-xyz/borsh';
import type { Message } from '@libp2p/interface-pubsub'
import { SignatureWithKey, SignKey } from '@dao-xyz/peerbit-crypto';
import { AccessError, decryptVerifyInto } from "@dao-xyz/peerbit-crypto";
import { QueryRequestV0, QueryResponseV0 } from './query.js';
import { query, QueryOptions, respond } from './io.js'
import { ComposableProgram, Program, ProgramInitializationOptions } from '@dao-xyz/peerbit-program'
import { IPFS } from 'ipfs-core-types';
import { Identity } from '@dao-xyz/ipfs-log';
import { Address } from '@dao-xyz/peerbit-store';

export const getQueryTopic = (region: string): string => {
    return region + '?';
}


export type DQueryInitializationOptions<Q, R> = { queryType: Constructor<Q>, responseType: Constructor<R>, canRead?(key: SignatureWithKey | undefined): Promise<boolean>, responseHandler: ResponseHandler<Q, R> };
export type ResponseHandler<Q, R> = (query: Q, from?: SignKey) => Promise<R | undefined> | R | undefined;

abstract class QueryTopic {

    abstract from(address: Address): string;
}

@variant(0)
class QueryRegion extends QueryTopic {

    @field({ type: 'string' })
    name: string
    constructor(properties?: { name: string }) {
        super();
        if (properties) {
            this.name = properties.name

        }
    }

    from(_address: Address) {
        return this.name;
    }
}


@variant(1)
class QueryAddressSuffix extends QueryTopic {

    @field({ type: 'string' })
    suffix: string
    constructor(properties?: { suffix: string }) {
        super();
        if (properties) {
            this.suffix = properties.suffix

        }
    }

    from(address: Address) {
        return address.toString() + '/' + this.suffix
    }
}
@variant([0, 1])
export class DQuery<Q, R> extends ComposableProgram {

    @field({ type: option(QueryTopic) })
    queryRegion?: QueryTopic;


    subscribeToQueries: boolean = true;

    _subscribed: boolean = false;
    _initializationPromise?: Promise<void>;
    _onQueryMessageBinded: any = undefined;
    _responseHandler: ResponseHandler<Q, R>
    _queryType: Constructor<Q>
    _responseType: Constructor<R>
    canRead: (key: SignatureWithKey | undefined) => Promise<boolean>

    constructor(properties: { name?: string, queryRegion?: string, queryAddressSuffix?: string }) {
        super(properties)
        if (properties) {
            if (!!properties.queryRegion && !!properties.queryRegion == !!properties.queryAddressSuffix) {
                throw new Error("Expected either queryRegion or queryAddressSuffix or none")
            }

            if (properties.queryRegion) {
                this.queryRegion = new QueryRegion({ name: properties.queryRegion })
            }
            else if (properties.queryAddressSuffix) {
                this.queryRegion = new QueryAddressSuffix({ suffix: properties.queryAddressSuffix })

            }
            // is this props ser or not??? 
        }
    }

    s: boolean = false;

    public async setup(options: DQueryInitializationOptions<Q, R>) {
        this.s = true;
        this._responseHandler = options.responseHandler;
        this._queryType = options.queryType;
        this._responseType = options.responseType;
        this.canRead = options.canRead || (() => Promise.resolve(true));

    }

    async init(ipfs: IPFS<{}>, identity: Identity, options: ProgramInitializationOptions): Promise<this> {
        await super.init(ipfs, identity, options);
        if (this.subscribeToQueries) {
            await this._subscribeToQueries();
        }
        return this;
    }

    public async close(): Promise<void> {
        await this._initializationPromise;
        await this._ipfs.pubsub.unsubscribe(this.queryTopic, this._onQueryMessageBinded);
        this._subscribed = false;
    }



    async _subscribeToQueries(): Promise<void> {
        this._initializationPromise = undefined;
        if (this._subscribed) {
            return
        }

        this._onQueryMessageBinded = this._onQueryMessage.bind(this);
        this._initializationPromise = this._ipfs.pubsub.subscribe(this.queryTopic, this._onQueryMessageBinded)
        await this._initializationPromise;
        this._subscribed = true;
    }

    async _onQueryMessage(msg: Message): Promise<void> {

        try {
            try {
                let { result: query, from } = await decryptVerifyInto(msg.data, QueryRequestV0, this._encryption?.getAnyKeypair || (() => Promise.resolve(undefined)), {
                    isTrusted: (key) => this.canRead(key.signature)
                })
                const response = await this._responseHandler(deserialize(query.query, this._queryType), from);
                if (response) {
                    await respond(this._ipfs, this.queryTopic, query, new QueryResponseV0({ response: serialize(response) }), {
                        encryption: this._encryption, signer: this._identity
                    })
                }

            } catch (error) {
                if (error instanceof AccessError) {
                    return;
                }
                throw error;
            }

        } catch (error) {
            console.error(error)
        }
    }

    public query(queryRequest: Q, responseHandler: (response: R, from?: SignKey) => void, options?: QueryOptions): Promise<void> {
        return query(this._ipfs, this.queryTopic, new QueryRequestV0({
            query: serialize(queryRequest),
            responseRecievers: options?.responseRecievers
        }), (response, from) => { responseHandler(deserialize(response.response, this._responseType), from) }, options);
    }



    public get queryTopic(): string {
        if (!this.parentProgram.address) {
            throw new Error("Not initialized");
        }
        const queryTopic = this.queryRegion ? this.queryRegion.from(this.parentProgram.address) : getQueryTopic(this.parentProgram.address.toString())
        return queryTopic;
    }
}