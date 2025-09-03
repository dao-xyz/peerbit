import { service, method, struct, ctor, union } from "@dao-xyz/borsh-rpc";
import { OptionKind, vec } from "@dao-xyz/borsh";
import type { Peerbit } from "peerbit";
import type { Multiaddr } from "@multiformats/multiaddr";
import { IndicesRPCContract } from "@peerbit/indexer-proxy";
import { HandlerRPCContract } from "@peerbit/program/src/handler.proxy.js";
import { ProgramClientRPCContract } from "@peerbit/program/src/client.rpc.js";

@service()
export class PeerbitProxyContract {
    #client: Peerbit;
    constructor(client: Peerbit) { this.#client = client; }

    // Lifecycle
    @method({ returns: "void" })
    async start(): Promise<void> { await this.#client.start(); }
    @method({ returns: "void" })
    async stop(): Promise<void> { await this.#client.stop(); }

    // Network
    @method({ returns: Uint8Array as any })
    peerId(): Uint8Array { return (this.#client.peerId as any).toBytes(); }
    @method({ returns: vec("string") })
    getMultiaddrs(): string[] { return this.#client.getMultiaddrs().map((m: Multiaddr) => m.toString()); }
    @method({ args: union(["string", vec("string")]), returns: "bool" })
    async dial(addr: string | string[]): Promise<boolean> {
        const target = Array.isArray(addr) ? addr.map((a) => a) : addr;
        return await this.#client.dial(target as unknown as string);
    }
    @method(["string"], "void")
    async hangUp(id: string): Promise<void> { await this.#client.hangUp(id); }

    // PubSub methods
    @method(struct({ data: Uint8Array, topics: vec("string"), mode: new OptionKind("u8") }), "void")
    async publish(args: { data: Uint8Array; topics: string[]; mode?: number }): Promise<void> {
        await this.#client.services.pubsub.publish(args.data, { topics: args.topics, mode: args.mode as any });
    }
    @method(struct({ topic: "string" }), "void")
    async subscribe(args: { topic: string }): Promise<void> {
        await this.#client.services.pubsub.subscribe(args.topic);
    }
    @method(struct({ topic: "string" }), "bool")
    async unsubscribe(args: { topic: string }): Promise<boolean> {
        return await this.#client.services.pubsub.unsubscribe(args.topic);
    }
    @method(struct({ topic: "string" }), "void")
    async requestSubscribers(args: { topic: string }): Promise<void> {
        await this.#client.services.pubsub.requestSubscribers(args.topic);
    }
    @method(struct({ topic: "string" }), vec(Uint8Array))
    async getSubscribers(args: { topic: string }): Promise<Uint8Array[]> {
        const subs = await this.#client.services.pubsub.getSubscribers(args.topic);
        return (subs || []).map((k: any) => (k?.key ?? k?.bytes ?? Uint8Array.from([])) as Uint8Array);
    }

    // Blocks methods
    @method(struct({ cid: "string", remote: new OptionKind("bool") }), Uint8Array)
    async getBlock(args: { cid: string; remote?: boolean }): Promise<Uint8Array> {
        const result = await this.#client.services.blocks.get(args.cid, { remote: args.remote });
        return result || new Uint8Array();
    }
    @method(struct({ cid: "string" }), "bool")
    async hasBlock(args: { cid: string }): Promise<boolean> {
        return await this.#client.services.blocks.has(args.cid);
    }
    @method(struct({ bytes: Uint8Array }), "string")
    async putBlock(args: { bytes: Uint8Array }): Promise<string> {
        return await this.#client.services.blocks.put(args.bytes);
    }
    @method(struct({ cid: "string" }), "void")
    async rmBlock(args: { cid: string }): Promise<void> {
        await this.#client.services.blocks.rm(args.cid);
    }
    @method({}, "u64")
    async blockSize(): Promise<number> {
        return await this.#client.services.blocks.size();
    }
    @method({}, "bool")
    async blockPersisted(): Promise<boolean> {
        return await this.#client.services.blocks.persisted();
    }

    // Keychain methods
    @method(struct({ keyId: Uint8Array, type: ctor("any") }), new OptionKind(ctor("any")))
    async exportKeypairById(args: { keyId: Uint8Array; type: any }): Promise<any> {
        return await this.#client.services.keychain?.exportById(args.keyId, args.type);
    }
    @method(struct({ publicKey: ctor("any") }), new OptionKind(ctor("any")))
    async exportKeypairByKey(args: { publicKey: any }): Promise<any> {
        return await this.#client.services.keychain?.exportByKey(args.publicKey);
    }
    @method(struct({ keypair: ctor("any"), id: Uint8Array }), "void")
    async importKey(args: { keypair: any; id: Uint8Array }): Promise<void> {
        await this.#client.services.keychain?.import({ keypair: args.keypair, id: args.id });
    }

    // Storage methods
    @method(struct({ level: vec("string"), key: "string" }), new OptionKind(Uint8Array))
    async storageGet(args: { level: string[]; key: string }): Promise<Uint8Array | undefined> {
        const store = args.level.length === 0 ? this.#client.storage : this.#client.storage;
        return await store.get(args.key);
    }
    @method(struct({ level: vec("string"), key: "string", bytes: Uint8Array }), "void")
    async storagePut(args: { level: string[]; key: string; bytes: Uint8Array }): Promise<void> {
        const store = args.level.length === 0 ? this.#client.storage : this.#client.storage;
        await store.put(args.key, args.bytes);
    }
    @method(struct({ level: vec("string"), key: "string" }), "void")
    async storageDel(args: { level: string[]; key: string }): Promise<void> {
        const store = args.level.length === 0 ? this.#client.storage : this.#client.storage;
        await store.del(args.key);
    }
    @method(struct({ level: vec("string") }), "void")
    async storageClear(args: { level: string[] }): Promise<void> {
        const store = args.level.length === 0 ? this.#client.storage : this.#client.storage;
        await store.clear();
    }

    // Subservices - return RPC contracts for complex services
    @method({}, IndicesRPCContract as any)
    indexer(): any { return new IndicesRPCContract(this.#client.indexer); }

    @method({}, HandlerRPCContract as any)
    handler(): any { return new HandlerRPCContract((this.#client as any).handler); }

    @method({}, ProgramClientRPCContract as any)
    client(): any { return new ProgramClientRPCContract(this.#client as any); }
}


