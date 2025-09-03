import { expect } from "chai";
import { LoopbackPair, bindService, createProxyFromService } from "@dao-xyz/borsh-rpc";
import { PeerbitProxyContract } from "../src/rpc/peerbit.service.js";
import { PeerbitRPCClient } from "../src/rpc/peerbit.client.js";
import { multiaddr } from "@multiformats/multiaddr";
import { Ed25519PublicKey } from "@peerbit/crypto";

describe("PeerbitRPC", () => {
    it("PeerbitProxyContract basic functionality", async () => {
        const loop = new LoopbackPair();
        const calls: string[] = [];

        // Mock Peerbit client
        const mockClient = {
            async start() { calls.push("start"); },
            async stop() { calls.push("stop"); },
            get peerId() { return { toBytes: () => new Uint8Array([1, 2, 3, 4]) }; },
            getMultiaddrs() { return [multiaddr("/ip4/127.0.0.1/tcp/1234")]; },
            async dial(addr: any) { calls.push("dial:" + addr.toString()); return true; },
            async hangUp(id: string) { calls.push("hangUp:" + id); },
            get services() {
                return {
                    pubsub: {
                        async publish(data: Uint8Array, options?: any) { calls.push("publish:" + data.length + ":" + (options?.topics?.join(",") || "")); },
                        async subscribe(topic: string) { calls.push("subscribe:" + topic); },
                        async unsubscribe(topic: string, options?: any) { calls.push("unsubscribe:" + topic); },
                        async requestSubscribers(topic: string) { calls.push("requestSubscribers:" + topic); },
                        async getSubscribers(topic: string): Promise<any[]> { calls.push("getSubscribers:" + topic); return []; },
                    },
                    blocks: {
                        async get(cid: string, options?: any) { calls.push("getBlock:" + cid); return new Uint8Array([1, 2, 3]); },
                        async has(cid: string) { calls.push("hasBlock:" + cid); return true; },
                        async put(bytes: Uint8Array) { calls.push("putBlock:" + bytes.length); return "cid123"; },
                        async rm(cid: string) { calls.push("rmBlock:" + cid); },
                        async size() { calls.push("blockSize"); return 100; },
                        async persisted() { calls.push("blockPersisted"); return true; },
                    },
                    keychain: {
                        async exportById(id: Uint8Array, type: any): Promise<any> { calls.push("exportById:" + id.length); return null; },
                        async exportByKey(publicKey: any): Promise<any> { calls.push("exportByKey"); return null; },
                        async import(properties: any) { calls.push("import:" + properties.id.length); },
                    },
                };
            },
            get storage() {
                return {
                    async get(key: string) { calls.push("storageGet:" + key); return new Uint8Array([1, 2, 3]); },
                    async put(key: string, value: Uint8Array) { calls.push("storagePut:" + key + ":" + value.length); },
                    async del(key: string) { calls.push("storageDel:" + key); },
                    async clear() { calls.push("storageClear"); },
                };
            },
            get indexer() { return {}; },
            get handler() { return {}; },
        } as any;

        const unbind = bindService(PeerbitProxyContract, loop.a, new PeerbitProxyContract(mockClient));
        const rpc = createProxyFromService(PeerbitProxyContract, loop.b) as any;

        // Test lifecycle
        await rpc.start();
        await rpc.stop();
        expect(calls).to.include("start");
        expect(calls).to.include("stop");

        // Test network
        const peerId = await rpc.peerId();
        expect(peerId).to.deep.equal(new Uint8Array([1, 2, 3, 4]));

        const addrs = await rpc.getMultiaddrs();
        expect(addrs).to.deep.equal(["/ip4/127.0.0.1/tcp/1234"]);

        expect(await rpc.dial("/ip4/1.2.3.4/tcp/5678")).to.equal(true);
        expect(calls).to.include("dial:/ip4/1.2.3.4/tcp/5678");

        await rpc.hangUp("peer-id");
        expect(calls).to.include("hangUp:peer-id");

        // Test PubSub
        await rpc.publish({ data: new Uint8Array([1, 2, 3]), topics: ["topic1", "topic2"] });
        expect(calls).to.include("publish:3:topic1,topic2");

        await rpc.subscribe({ topic: "test-topic" });
        expect(calls).to.include("subscribe:test-topic");

        await rpc.unsubscribe({ topic: "test-topic" });
        expect(calls).to.include("unsubscribe:test-topic");

        // Test Blocks
        const blockData = await rpc.getBlock({ cid: "test-cid" });
        expect(blockData).to.deep.equal(new Uint8Array([1, 2, 3]));
        expect(calls).to.include("getBlock:test-cid");

        expect(await rpc.hasBlock({ cid: "test-cid" })).to.equal(true);
        expect(calls).to.include("hasBlock:test-cid");

        const cid = await rpc.putBlock({ bytes: new Uint8Array([1, 2, 3, 4]) });
        expect(cid).to.equal("cid123");
        expect(calls).to.include("putBlock:4");

        // Test Storage
        const storageData = await rpc.storageGet({ level: [], key: "test-key" });
        expect(storageData).to.deep.equal(new Uint8Array([1, 2, 3]));
        expect(calls).to.include("storageGet:test-key");

        await rpc.storagePut({ level: [], key: "test-key", bytes: new Uint8Array([1, 2, 3, 4]) });
        expect(calls).to.include("storagePut:test-key:4");

        unbind();
    });

    it("PeerbitRPCClient implements ProgramClient interface", async () => {
        const loop = new LoopbackPair();
        const calls: string[] = [];

        const mockClient = {
            async start() { calls.push("start"); },
            async stop() { calls.push("stop"); },
            get peerId() { return { toBytes: () => new Uint8Array([1, 2, 3, 4]) }; },
            getMultiaddrs() { return [multiaddr("/ip4/127.0.0.1/tcp/1234")]; },
            async dial(addr: any) { calls.push("dial:" + addr.toString()); return true; },
            async hangUp(id: string) { calls.push("hangUp:" + id); },
            get services() {
                return {
                    pubsub: {
                        async publish(data: Uint8Array, options?: any) { calls.push("publish:" + data.length); },
                        async subscribe(topic: string) { calls.push("subscribe:" + topic); },
                        async unsubscribe(topic: string, options?: any) { calls.push("unsubscribe:" + topic); },
                        async requestSubscribers(topic: string) { calls.push("requestSubscribers:" + topic); },
                        async getSubscribers(topic: string): Promise<any[]> { calls.push("getSubscribers:" + topic); return []; },
                    },
                    blocks: {
                        async get(cid: string, options?: any) { calls.push("getBlock:" + cid); return new Uint8Array([1, 2, 3]); },
                        async has(cid: string) { calls.push("hasBlock:" + cid); return true; },
                        async put(bytes: Uint8Array) { calls.push("putBlock:" + bytes.length); return "cid123"; },
                        async rm(cid: string) { calls.push("rmBlock:" + cid); },
                        async size() { calls.push("blockSize"); return 100; },
                        async persisted() { calls.push("blockPersisted"); return true; },
                    },
                    keychain: {
                        async exportById(id: Uint8Array, type: any): Promise<any> { calls.push("exportById:" + id.length); return null; },
                        async exportByKey(publicKey: any): Promise<any> { calls.push("exportByKey"); return null; },
                        async import(properties: any) { calls.push("import:" + properties.id.length); },
                    },
                };
            },
            get storage() {
                return {
                    async get(key: string) { calls.push("storageGet:" + key); return new Uint8Array([1, 2, 3]); },
                    async put(key: string, value: Uint8Array) { calls.push("storagePut:" + key + ":" + value.length); },
                    async del(key: string) { calls.push("storageDel:" + key); },
                    async clear() { calls.push("storageClear"); },
                };
            },
            get indexer() { return {}; },
            get handler() { return {}; },
        } as any;

        const unbind = bindService(PeerbitProxyContract, loop.a, new PeerbitProxyContract(mockClient));
        const rpc = createProxyFromService(PeerbitProxyContract, loop.b) as any;

        const peerId = { toString: () => "peer-id" } as any;
        const identity = { publicKey: new Ed25519PublicKey({ publicKey: new Uint8Array(32) }) } as any;
        const client = new PeerbitRPCClient(rpc, peerId, identity);

        expect(client.peerId).to.equal(peerId);
        expect(client.identity).to.equal(identity);

        await client.start();
        const addrs = client.getMultiaddrs();
        expect(addrs).to.deep.equal([multiaddr("/ip4/127.0.0.1/tcp/1234")]);
        expect(calls).to.include("start");

        const multiaddr1 = multiaddr("/ip4/1.2.3.4/tcp/5678");
        expect(await client.dial(multiaddr1)).to.equal(true);
        expect(calls).to.include("dial:/ip4/1.2.3.4/tcp/5678");

        await client.hangUp("peer-id");
        expect(calls).to.include("hangUp:peer-id");

        // Test services
        await client.services.pubsub.publish(new Uint8Array([1, 2, 3]), { topics: ["test"] });
        expect(calls).to.include("publish:3");

        await client.services.blocks.get("test-cid");
        expect(calls).to.include("getBlock:test-cid");

        await client.storage.get("test-key");
        expect(calls).to.include("storageGet:test-key");

        await client.stop();
        expect(calls).to.include("stop");

        unbind();
    });
});
