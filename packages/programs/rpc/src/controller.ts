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
import { RequestV0, ReponseV0 } from "./encoding.js";
import { query, RPCOptions, respond } from "./io.js";
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

const logger = pino().child({ module: "query" });

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

export const getRPCTopic = (parentProgram: Program, region: string): string => {
    const disriminator = getDiscriminatorApproximation(
        parentProgram.constructor as Constructor<any>
    );
    return region + "/" + Buffer.from(disriminator).toString("base64") + "/?";
};

export type CanRead = (key?: SignKey) => Promise<boolean>;
export type RPCTopicOption =
    | { queryAddressSuffix: string }
    | { rpcRegion: string };
export type RPCInitializationOptions<Q, R> = {
    rpcTopic?: RPCTopicOption;
    queryType: Constructor<Q>;
    responseType: Constructor<R>;
    canRead?: CanRead;
    context: SearchContext;
    responseHandler: ResponseHandler<Q, R>;
};
export type QueryContext = {
    from?: SignKey;
    address: string;
};
export type ResponseHandler<Q, R> = (
    query: Q,
    context: QueryContext
) => Promise<R | undefined> | R | undefined;

export abstract class RPCTopic {
    abstract from(address: Address): string;
}

@variant(0)
export class RPCRegion extends RPCTopic {
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
export class RPCAddressSuffix extends RPCTopic {
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

@variant("rpc")
export class RPC<Q, R> extends ComposableProgram {
    rpcRegion?: RPCTopic;
    subscribeToQueries = true;
    canRead: CanRead;

    _subscribed = false;
    _onMessageBinded: any = undefined;
    _responseHandler: ResponseHandler<Q, (R | undefined) | R>;
    _requestType: Constructor<Q>;
    _responseType: Constructor<R>;
    _replicationTopic: string;
    _context: SearchContext;

    public async setup(options: RPCInitializationOptions<Q, R>) {
        if (options.rpcTopic) {
            if (
                !!(options.rpcTopic as { rpcRegion }).rpcRegion &&
                !!(options.rpcTopic as { rpcRegion }).rpcRegion ==
                    !!(options.rpcTopic as { queryAddressSuffix })
                        .queryAddressSuffix
            ) {
                throw new Error(
                    "Expected either rpcRegion or queryAddressSuffix or none"
                );
            }
            if ((options.rpcTopic as { rpcRegion }).rpcRegion) {
                this.rpcRegion = new RPCRegion({
                    id: (options.rpcTopic as { rpcRegion }).rpcRegion,
                });
            } else if (options.rpcTopic as { queryAddressSuffix }) {
                this.rpcRegion = new RPCAddressSuffix({
                    suffix: (options.rpcTopic as { queryAddressSuffix })
                        .queryAddressSuffix,
                });
            }
        }
        this._context = options.context;
        this._responseHandler = options.responseHandler;
        this._requestType = options.queryType;
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
            this.rpcTopic,
            this._onMessageBinded
        );
        this._subscribed = false;
    }

    async _subscribeToQueries(): Promise<void> {
        if (this._subscribed) {
            return;
        }

        this._onMessageBinded = this._onMessage.bind(this);
        this._initializationPromise = this._ipfs.pubsub.subscribe(
            this.rpcTopic,
            this._onMessageBinded
        );
        await this._initializationPromise;
        logger.debug("subscribing to query topic: " + this.rpcTopic);
        this._subscribed = true;
    }

    async _onMessage(msg: Message): Promise<void> {
        try {
            try {
                const { result: request, from } = await decryptVerifyInto(
                    msg.data,
                    RequestV0,
                    this._encryption?.getAnyKeypair ||
                        (() => Promise.resolve(undefined)),
                    {
                        isTrusted: (key) =>
                            this.canRead(key.signature?.publicKey),
                    }
                );

                if (request.context != undefined) {
                    if (request.context != this.contextAddress) {
                        logger.debug("Recieved a request for another context");
                        return;
                    }
                }

                const response = await this._responseHandler(
                    (this._requestType as any) === Uint8Array
                        ? (request.request as Q)
                        : deserialize(request.request, this._requestType),
                    {
                        address: this.contextAddress,
                        from,
                    }
                );

                if (response) {
                    await respond(
                        this._ipfs,
                        this.rpcTopic,
                        request,
                        new ReponseV0({
                            response: serialize(response),
                            context: this.contextAddress,
                        }),
                        {
                            encryption: this._encryption,
                            signer: this._identity,
                        }
                    );
                }
            } catch (error: any) {
                if (error instanceof AccessError) {
                    return;
                }
                logger.error(
                    "Error handling query: " +
                        (error?.message ? error?.message?.toString() : error)
                );
                throw error;
            }
        } catch (error: any) {
            if (error.constructor.name === "BorshError") {
                return; // unknown message
            }
            console.error(error);
        }
    }

    public send(
        request: Q,
        responseHandler: (response: R, from?: SignKey) => void,
        options?: RPCOptions
    ): Promise<void> {
        logger.debug("querying topic: " + this.rpcTopic);
        return query(
            this._ipfs,
            this.rpcTopic,
            new RequestV0({
                request:
                    (this._requestType as any) === Uint8Array
                        ? (request as Uint8Array)
                        : serialize(request),
                responseRecievers: options?.responseRecievers,
                context: options?.context || this.contextAddress.toString(),
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

    public get rpcTopic(): string {
        if (!this.parentProgram.address) {
            throw new Error("Not initialized");
        }
        const rpcTopic = this.rpcRegion
            ? this.rpcRegion.from(this.parentProgram.address)
            : getRPCTopic(this.parentProgram, this._replicationTopic);
        return rpcTopic;
    }
}
