import type { Client } from "./client.js";
import type { Manageable, ExtractArgs, OpenOptions } from "./handler.js";
import type { Blocks } from "@peerbit/blocks-interface";
import type { PubSub } from "@peerbit/pubsub-interface";
import type { Keychain } from "@peerbit/keychain";
import type { Indices } from "@peerbit/indexer-interface";
import type { AnyStore } from "@peerbit/any-store-interface";
import type { Identity, Ed25519PublicKey, PublicSignKey } from "@peerbit/crypto";
import type { Multiaddr } from "@multiformats/multiaddr";
import { multiaddr } from "@multiformats/multiaddr";
import type { PeerId as Libp2pPeerId, PeerId } from "@libp2p/interface";
import type { Address } from "./address.js";
import type { RpcProxy } from "@dao-xyz/borsh-rpc";
import type { ProgramClientRPCContract } from "./client.rpc.js";

export class ProgramClientProxy<T extends Manageable<any>> implements Client<T> {
    #rpc: RpcProxy<ProgramClientRPCContract<T>>;
    #peerId: Libp2pPeerId;
    #identity: Identity<Ed25519PublicKey>;
    #services: { pubsub: PubSub; blocks: Blocks; keychain: Keychain };
    #storage: AnyStore;
    #indexer: Indices;
    #multiaddrs: Multiaddr[] = [];

    constructor(
        rpc: RpcProxy<ProgramClientRPCContract<T>>,
        peerId: Libp2pPeerId,
        identity: Identity<Ed25519PublicKey>,
        services: { pubsub: PubSub; blocks: Blocks; keychain: Keychain },
        storage: AnyStore,
        indexer: Indices
    ) {
        this.#rpc = rpc;
        this.#peerId = peerId;
        this.#identity = identity;
        this.#services = services;
        this.#storage = storage;
        this.#indexer = indexer;
    }

    get peerId(): Libp2pPeerId { return this.#peerId; }
    get identity(): Identity<Ed25519PublicKey> { return this.#identity; }
    get services(): { pubsub: PubSub; blocks: Blocks; keychain: Keychain } { return this.#services; }
    get storage(): AnyStore { return this.#storage; }
    get indexer(): Indices { return this.#indexer; }

    async start(): Promise<void> {
        await this.#rpc.start();
        const strings = await this.#rpc.getMultiaddrs() as unknown as string[];
        this.#multiaddrs = strings.map(s => multiaddr(s));
    }
    async stop(): Promise<void> { await this.#rpc.stop(); }

    getMultiaddrs(): Multiaddr[] {
        return this.#multiaddrs;
    }
    async dial(address: string | Multiaddr | Multiaddr[]): Promise<boolean> {
        const addr = Array.isArray(address) ? address.map(a => a.toString()) : address.toString();
        return this.#rpc.dial(addr);
    }
    async hangUp(address: PeerId | PublicSignKey | string | Multiaddr): Promise<void> {
        const id = typeof address === 'string' ? address : address.toString();
        await this.#rpc.hangUp(id);
    }

    async open<S extends T & Manageable<ExtractArgs<S>>>(program: S | Address, options?: OpenOptions<S>): Promise<S> {
        const address = typeof program === 'string' ? program : program.address;
        const args = options?.args;
        const argsSchema = args ? args.constructor as any : undefined;
        const serializedArgs = args && argsSchema?.serialize ? argsSchema.serialize(args) : undefined;

        await this.#rpc.open({
            address,
            args: serializedArgs,
            argsSchema,
            timeout: options?.timeout,
            existing: options?.existing === 'replace' ? 0 : options?.existing === 'reject' ? 1 : options?.existing === 'reuse' ? 2 : undefined
        });

        return program as S;
    }
}


