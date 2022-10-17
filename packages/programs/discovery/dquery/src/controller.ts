import { Store, IInitializationOptions } from '@dao-xyz/peerbit-dstore'
import { Constructor, deserialize, field, option, serialize, serializeField, variant } from '@dao-xyz/borsh';
import type { Message } from '@libp2p/interface-pubsub'
import { PublicKeyEncryptionResolver, PublicSignKey, SignatureWithKey, SignKey, X25519PublicKey } from '@dao-xyz/peerbit-crypto';
import { AccessError, decryptVerifyInto } from "@dao-xyz/peerbit-crypto";
import { IPFS } from 'ipfs-core-types';
import { QueryRequestV0, QueryResponseV0 } from './query.js';
import { query, QueryOptions, respond } from './io.js'
import { Identity } from '@dao-xyz/ipfs-log';
import { Program } from '@dao-xyz/peerbit-program'

export const getQueryTopic = (region: string): string => {
    return region + '/query';
}


export type DQueryInitializationOptions<Q, R> = IInitializationOptions<any> & { queryType: Constructor<Q>, responseType: Constructor<R>, canRead?(key: SignatureWithKey | undefined): Promise<boolean>, responseHandler: ResponseHandler<Q, R> };
export type ResponseHandler<Q, R> = (query: Q, from?: SignKey) => Promise<R | undefined> | R | undefined;

@variant([0, 1])
export class DQuery<Q, R> extends Program {

    @field({ type: option('string') })
    queryRegion?: string;

    subscribeToQueries: boolean = true;

    _subscribed: boolean = false;
    _initializationPromise?: Promise<void>;
    _onQueryMessageBinded: any = undefined;
    _ipfs: IPFS;
    _responseHandler: ResponseHandler<Q, R>
    _encryption?: PublicKeyEncryptionResolver;
    _identity: Identity;
    _queryType: Constructor<Q>
    _responseType: Constructor<R>
    canRead: (key: SignatureWithKey | undefined) => Promise<boolean>

    constructor(properties: { name?: string, queryRegion?: string }) {
        super(properties)
        if (properties) {
            this.queryRegion = properties.queryRegion;
            // is this props ser or not??? 
        }
    }

    public async init(ipfs: IPFS, identity: Identity, options: DQueryInitializationOptions<Q, R>) {
        await super.init(ipfs, identity, options)
        this._responseHandler = options.responseHandler;
        this._encryption = options.encryption;
        this._identity = identity;
        this._ipfs = ipfs;
        this._queryType = options.queryType;
        this._responseType = options.responseType;
        this.canRead = options.canRead || (() => Promise.resolve(true));
        if (this.subscribeToQueries) {
            this._subscribeToQueries();
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
        if (!this.address) {
            throw new Error("Not initialized");
        }
        if (this.queryRegion)
            return getQueryTopic(this.queryRegion); // this store is accessed through some shared query group
        else {
            return getQueryTopic(this.address.toString()); // this tore is accessed by querying the store directly
        }
    }
}