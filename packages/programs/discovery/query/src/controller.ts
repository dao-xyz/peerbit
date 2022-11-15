import {
    BinaryWriter,
    Constructor,
    deserialize,
    field,
    getSchemasBottomUp,
    serialize,
    variant,
} from "@dao-xyz/borsh";
import type { Message } from "@libp2p/interface-pubsub";
import { SignKey } from "@dao-xyz/peerbit-crypto";
import { AccessError, decryptVerifyInto } from "@dao-xyz/peerbit-crypto";
import { QueryRequestV0, QueryResponseV0, U64Compare } from "./query.js";
import { query, QueryOptions, respond } from "./io.js";
import {
    AbstractProgram,
    Address,
    ComposableProgram,
    Program,
    ProgramInitializationOptions,
} from "@dao-xyz/peerbit-program";
import { IPFS } from "ipfs-core-types";
import { Identity } from "@dao-xyz/ipfs-log";
import pino from "pino";
const logger = pino().child({ module: "anyearch" });

export type SearchContext = (() => Address) | AbstractProgram | string;

export const getDiscriminatorApproximation = (
    constructor: Constructor<any>
): Uint8Array => {
    const schemas = getSchemasBottomUp(constructor);
    // assume ordered
    const writer = new BinaryWriter();
    for (let i = 0; i < schemas.length; i++) {
        const clazz = schemas[i];
        const variant = clazz.schema.variant;
        if (variant == undefined) {
            continue;
        }
        if (typeof variant === "string") {
            writer.writeString(variant);
        } else if (typeof variant === "number") {
            writer.writeU8(variant);
        } else if (Array.isArray(variant)) {
            variant.forEach((v) => {
                writer.writeU8(v);
            });
        } else {
            throw new Error(
                "Can not resolve discriminator for variant with type: " +
                    typeof variant
            );
        }
    }

    return writer.toArray();
};

export const getQueryTopic = (
    parentProgram: Program,
    region: string
): string => {
    const disriminator = getDiscriminatorApproximation(
        parentProgram.constructor as Constructor<any>
    );
    return region + "/" + Buffer.from(disriminator).toString("base64") + "/?";
};

export type CanRead = (key?: SignKey) => Promise<boolean>;
export type QueryTopicOption =
    | { queryAddressSuffix: string }
    | { queryRegion: string };
export type DQueryInitializationOptions<Q, R> = {
    queryTopic?: QueryTopicOption;
    queryType: Constructor<Q>;
    responseType: Constructor<R>;
    canRead?: CanRead;
    context: SearchContext;
    responseHandler: ResponseHandler<Q, R>;
};
export type QueryContext = {
    from?: SignKey;
    address: string;
    created?: U64Compare[];
    modified?: U64Compare[];
};
export type ResponseHandler<Q, R> = (
    query: Q,
    context: QueryContext
) => Promise<R | undefined> | R | undefined;

export abstract class QueryTopic {
    abstract from(address: Address): string;
}

@variant(0)
export class QueryRegion extends QueryTopic {
    @field({ type: "string" })
    id: string;

    constructor(properties?: { id: string }) {
        super();
        if (properties) {
            this.id = properties.id;
        }
    }

    from(_address: Address) {
        return this.id;
    }
}

@variant(1)
export class QueryAddressSuffix extends QueryTopic {
    @field({ type: "string" })
    suffix: string;
    constructor(properties?: { suffix: string }) {
        super();
        if (properties) {
            this.suffix = properties.suffix;
        }
    }

    from(address: Address) {
        return address.toString() + "/" + this.suffix;
    }
}

@variant("dquery")
export class DQuery<Q, R> extends ComposableProgram {
    queryRegion?: QueryTopic;
    subscribeToQueries = true;
    canRead: CanRead;

    _subscribed = false;
    _onQueryMessageBinded: any = undefined;
    _responseHandler: ResponseHandler<Q, (R | undefined) | R>;
    _queryType: Constructor<Q>;
    _responseType: Constructor<R>;
    _replicationTopic: string;
    _context: SearchContext;

    public async setup(options: DQueryInitializationOptions<Q, R>) {
        if (options.queryTopic) {
            if (
                !!(options.queryTopic as { queryRegion }).queryRegion &&
                !!(options.queryTopic as { queryRegion }).queryRegion ==
                    !!(options.queryTopic as { queryAddressSuffix })
                        .queryAddressSuffix
            ) {
                throw new Error(
                    "Expected either queryRegion or queryAddressSuffix or none"
                );
            }
            if ((options.queryTopic as { queryRegion }).queryRegion) {
                this.queryRegion = new QueryRegion({
                    id: (options.queryTopic as { queryRegion }).queryRegion,
                });
            } else if (options.queryTopic as { queryAddressSuffix }) {
                this.queryRegion = new QueryAddressSuffix({
                    suffix: (options.queryTopic as { queryAddressSuffix })
                        .queryAddressSuffix,
                });
            }
        }
        this._context = options.context;
        this._responseHandler = options.responseHandler;
        this._queryType = options.queryType;
        this._responseType = options.responseType;
        this.canRead = options.canRead || (() => Promise.resolve(true));
    }

    async init(
        ipfs: IPFS,
        identity: Identity,
        options: ProgramInitializationOptions
    ): Promise<this> {
        await super.init(ipfs, identity, options);
        this._replicationTopic = options.replicationTopic;
        if (options.store.replicate) {
            await this._subscribeToQueries();
        }
        return this;
    }

    public async close(): Promise<void> {
        await this._initializationPromise;
        await this._ipfs.pubsub.unsubscribe(
            this.queryTopic,
            this._onQueryMessageBinded
        );
        this._subscribed = false;
    }

    async _subscribeToQueries(): Promise<void> {
        if (this._subscribed) {
            return;
        }

        this._onQueryMessageBinded = this._onQueryMessage.bind(this);
        this._initializationPromise = this._ipfs.pubsub.subscribe(
            this.queryTopic,
            this._onQueryMessageBinded
        );
        await this._initializationPromise;
        logger.debug("subscribing to query topic: " + this.queryTopic);
        this._subscribed = true;
    }

    async _onQueryMessage(msg: Message): Promise<void> {
        try {
            try {
                const { result: query, from } = await decryptVerifyInto(
                    msg.data,
                    QueryRequestV0,
                    this._encryption?.getAnyKeypair ||
                        (() => Promise.resolve(undefined)),
                    {
                        isTrusted: (key) =>
                            this.canRead(key.signature?.publicKey),
                    }
                );

                if (query.context != undefined) {
                    if (query.context != this.contextAddress) {
                        logger.debug("Recieved a query for another context");
                        return;
                    }
                }

                const response = await this._responseHandler(
                    deserialize(query.query, this._queryType),
                    {
                        address: this.contextAddress,
                        created: query.created,
                        modified: query.modified,
                        from,
                    }
                );

                if (response) {
                    await respond(
                        this._ipfs,
                        this.queryTopic,
                        query,
                        new QueryResponseV0({
                            response: serialize(response),
                            context: this.contextAddress,
                        }),
                        {
                            encryption: this._encryption,
                            signer: this._identity,
                        }
                    );
                }
            } catch (error) {
                if (error instanceof AccessError) {
                    return;
                }
                throw error;
            }
        } catch (error: any) {
            if (error.constructor.name === "BorshError") {
                return; // unknown message
            }
            console.error(error);
        }
    }

    public query(
        queryRequest: Q,
        responseHandler: (response: R, from?: SignKey) => void,
        options?: QueryOptions
    ): Promise<void> {
        logger.debug("querying topic: " + this.queryTopic);
        return query(
            this._ipfs,
            this.queryTopic,
            new QueryRequestV0({
                query: serialize(queryRequest),
                responseRecievers: options?.responseRecievers,
                context: options?.context || this.contextAddress.toString(),
                created: options?.createdAt,
                modified: options?.modifiedAt,
            }),
            (response, from) => {
                responseHandler(
                    deserialize(response.response, this._responseType),
                    from
                );
            },
            options
        );
    }

    get contextAddress(): string {
        if (typeof this._context === "string") {
            return this._context;
        }
        return this._context instanceof AbstractProgram
            ? this._context.address.toString()
            : this._context().toString();
    }
    public get queryTopic(): string {
        if (!this.parentProgram.address) {
            throw new Error("Not initialized");
        }
        const queryTopic = this.queryRegion
            ? this.queryRegion.from(this.parentProgram.address)
            : getQueryTopic(this.parentProgram, this._replicationTopic);
        return queryTopic;
    }
}
