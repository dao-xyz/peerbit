import type { RpcProxy } from "@dao-xyz/borsh-rpc";
import type { PeerId as Libp2pPeerId, PeerId } from "@libp2p/interface";
import type { Multiaddr } from "@multiformats/multiaddr";
import type { AnyStore } from "@peerbit/any-store-interface";
import type { Blocks } from "@peerbit/blocks-interface";
import type { Ed25519PublicKey, Identity, PublicSignKey } from "@peerbit/crypto";
import type { Indices } from "@peerbit/indexer-interface";
import type { Keychain } from "@peerbit/keychain";
import type { PubSub } from "@peerbit/pubsub-interface";
import type { ProgramClient } from "@peerbit/program";
import { multiaddr } from "@multiformats/multiaddr";
import { PeerbitProxyContract } from "./peerbit.service.js";

export class PeerbitRPCClient implements ProgramClient {
    #rpc: RpcProxy<PeerbitProxyContract>;
    #peerId: Libp2pPeerId;
    #identity: Identity<Ed25519PublicKey>;
    #multiaddrs: Multiaddr[] = [];

    constructor(
        rpc: RpcProxy<PeerbitProxyContract>,
        peerId: Libp2pPeerId,
        identity: Identity<Ed25519PublicKey>
    ) {
        this.#rpc = rpc;
        this.#peerId = peerId;
        this.#identity = identity;
    }

    get peerId(): Libp2pPeerId { return this.#peerId; }
    get identity(): Identity<Ed25519PublicKey> { return this.#identity; }

    async start(): Promise<void> {
        await this.#rpc.start();
        const strings = await this.#rpc.getMultiaddrs() as unknown as string[];
        this.#multiaddrs = strings.map(s => multiaddr(s));
    }
    async stop(): Promise<void> { await this.#rpc.stop(); }

    getMultiaddrs(): Multiaddr[] { return this.#multiaddrs; }
    async dial(address: string | Multiaddr | Multiaddr[]): Promise<boolean> {
        const addr = Array.isArray(address) ? address.map(a => a.toString()) : address.toString();
        return await this.#rpc.dial(addr);
    }
    async hangUp(address: PeerId | PublicSignKey | string | Multiaddr): Promise<void> {
        const id = typeof address === 'string' ? address : address.toString();
        await this.#rpc.hangUp(id);
    }

    get services(): { pubsub: PubSub; blocks: Blocks; keychain: Keychain } {
        return {
            pubsub: {
                publish: async (data: Uint8Array, options?: { topics?: string[]; mode?: number }) => {
                    await this.#rpc.publish({ data, topics: options?.topics || [], mode: options?.mode });
                    return new Uint8Array([1, 2, 3]); // Mock message ID
                },
                subscribe: async (topic: string) => {
                    await this.#rpc.subscribe({ topic });
                },
                unsubscribe: async (topic: string, options?: { force?: boolean; data?: Uint8Array }) => {
                    return await this.#rpc.unsubscribe({ topic });
                },
                requestSubscribers: async (topic: string) => {
                    await this.#rpc.requestSubscribers({ topic });
                },
                getSubscribers: async (topic: string) => {
                    const subs = await this.#rpc.getSubscribers({ topic });
                    return subs.map((bytes: Uint8Array) => ({ bytes } as any)); // Mock PublicSignKey
                },
                addEventListener: async () => { throw new Error("Not implemented in RPC client"); },
                removeEventListener: async () => { throw new Error("Not implemented in RPC client"); },
                dispatchEvent: async () => { throw new Error("Not implemented in RPC client"); },
                waitFor: async () => { throw new Error("Not implemented in RPC client"); },
                getPublicKey: async () => { throw new Error("Not implemented in RPC client"); },
            },
            blocks: {
                get: async (cid: string, options?: { remote?: boolean }) => {
                    return await this.#rpc.getBlock({ cid, remote: options?.remote });
                },
                has: async (cid: string) => {
                    return await this.#rpc.hasBlock({ cid });
                },
                put: async (bytesOrBlock: Uint8Array | any) => {
                    const bytes = bytesOrBlock instanceof Uint8Array ? bytesOrBlock : bytesOrBlock.bytes;
                    return await this.#rpc.putBlock({ bytes });
                },
                rm: async (cid: string) => {
                    await this.#rpc.rmBlock({ cid });
                },
                iterator: () => { throw new Error("Not implemented in RPC client"); },
                waitFor: async () => { throw new Error("Not implemented in RPC client"); },
                size: async () => {
                    return await this.#rpc.blockSize();
                },
                persisted: async () => {
                    return await this.#rpc.blockPersisted();
                },
            },
            keychain: {
                exportById: async (id: Uint8Array, type: any) => {
                    return await this.#rpc.exportKeypairById({ keyId: id, type });
                },
                exportByKey: async (publicKey: any) => {
                    return await this.#rpc.exportKeypairByKey({ publicKey });
                },
                import: async (properties: { keypair: any; id: Uint8Array }) => {
                    await this.#rpc.importKey({ keypair: properties.keypair, id: properties.id });
                },
            },
        };
    }

    get storage(): AnyStore {
        return {
            get: async (key: string) => {
                return await this.#rpc.storageGet({ level: [], key });
            },
            put: async (key: string, value: Uint8Array) => {
                await this.#rpc.storagePut({ level: [], key, bytes: value });
            },
            del: async (key: string) => {
                await this.#rpc.storageDel({ level: [], key });
            },
            clear: async () => {
                await this.#rpc.storageClear({ level: [] });
            },
            status: async () => { throw new Error("Not implemented in RPC client"); },
            sublevel: async () => { throw new Error("Not implemented in RPC client"); },
            iterator: () => { throw new Error("Not implemented in RPC client"); },
            close: async () => { throw new Error("Not implemented in RPC client"); },
            open: async () => { throw new Error("Not implemented in RPC client"); },
            size: async () => { throw new Error("Not implemented in RPC client"); },
            persisted: async () => { throw new Error("Not implemented in RPC client"); },
        };
    }

    get indexer(): Indices {
        // Return the RPC proxy for the indexer subservice
        return this.#rpc.indexer() as any;
    }

    async open<S extends any>(program: S | string, options?: any): Promise<S> {
        // Use the handler subservice for program management
        const handler = this.#rpc.handler() as any;
        return await handler.open(program, options);
    }
}
