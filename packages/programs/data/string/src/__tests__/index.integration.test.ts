import { DString } from "../string-store.js";
import {
    StringQueryRequest,
    StringResult,
    StringMatchQuery,
    RangeMetadatas,
    RangeMetadata,
} from "../index.js";
import { Range } from "../range.js";
import {
    createStore,
    LSession,
    waitForPeers,
} from "@dao-xyz/peerbit-test-utils";
import { AbstractLevel } from "abstract-level";
import Cache from "@dao-xyz/peerbit-cache";
import path from "path";
import { Identity } from "@dao-xyz/ipfs-log";
import { Ed25519Keypair, X25519PublicKey } from "@dao-xyz/peerbit-crypto";
import { DefaultOptions } from "@dao-xyz/peerbit-store";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import { v4 as uuid } from "uuid";
import { jest } from "@jest/globals";
import { fileURLToPath } from "url";
import { Libp2p } from "libp2p";
import {
    DEFAULT_BLOCK_TRANSPORT_TOPIC,
    LibP2PBlockStore,
    MemoryLevelBlockStore,
    Blocks,
} from "@dao-xyz/peerbit-block";
const createIdentity = async () => {
    const ed = await Ed25519Keypair.create();
    return {
        publicKey: ed.publicKey,
        sign: (data) => ed.sign(data),
    } as Identity;
};

describe("query", () => {
    jest.setTimeout(120 * 1000);

    let session: LSession,
        observer: Libp2p,
        writer: Libp2p,
        writeStore: DString,
        observerStore: DString,
        cacheStore1: AbstractLevel<any, string, Uint8Array>,
        cacheStore2: AbstractLevel<any, string, Uint8Array>;

    beforeAll(async () => {
        session = await LSession.connected(2, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);
        observer = session.peers[0];
        writer = session.peers[1];
    });

    beforeEach(async () => {
        const __filename = fileURLToPath(import.meta.url);

        cacheStore1 = await createStore(
            path.join(__filename, "cache1" + uuid())
        );
        cacheStore2 = await createStore(
            path.join(__filename, "cache2" + uuid())
        );
        // Create store
        writeStore = new DString({});
        const topic = uuid();
        const blockStore = new Blocks(
            new LibP2PBlockStore(writer, new MemoryLevelBlockStore())
        );
        await writeStore.init(writer, blockStore, await createIdentity(), {
            topic,
            store: {
                ...DefaultOptions,
                encryption: {
                    getAnyKeypair: (_) => Promise.resolve(undefined),
                    getEncryptionKeypair: () => Ed25519Keypair.create(),
                },
                replicate: true,
                resolveCache: () => new Cache(cacheStore1),
            },
        });

        observerStore = (await DString.load(
            blockStore,
            writeStore.address!
        )) as DString;
        await observerStore.init(
            observer,
            new Blocks(
                new LibP2PBlockStore(observer, new MemoryLevelBlockStore())
            ),
            await createIdentity(),
            {
                topic,
                store: {
                    ...DefaultOptions,
                    resolveCache: () => new Cache(cacheStore2),
                },
            }
        );

        await waitForPeers(
            session.peers[0],
            [session.peers[1]],
            writeStore.query.rpcTopic
        );
    });
    afterEach(async () => {
        await cacheStore1.close();
        await cacheStore2.close();
    });

    afterAll(async () => {
        await session.stop();
    });

    it("match all", async () => {
        await writeStore.add(
            "hello",
            new Range({ offset: 0n, length: "hello".length })
        );
        await writeStore.add(
            "world",
            new Range({
                offset: BigInt("hello ".length),
                length: "world".length,
            })
        );

        let response: StringResult = undefined as any;

        await observerStore.query.send(
            new StringQueryRequest({
                queries: [],
            }),
            (r) => {
                response = r;
            },
            { amount: 1 }
        );
        expect(response).toBeDefined();
        expect(response).toMatchObject(
            new StringResult({
                string: "hello world",
                metadatas: undefined, //  because we are matching without any specific query
            })
        );
    });

    it("match part", async () => {
        await writeStore.add(
            "hello",
            new Range({ offset: 0n, length: "hello".length })
        );
        await writeStore.add(
            "world",
            new Range({
                offset: BigInt("hello ".length),
                length: "world".length,
            })
        );

        let response: StringResult = undefined as any;

        await observerStore.query.send(
            new StringQueryRequest({
                queries: [
                    new StringMatchQuery({
                        exactMatch: true,
                        value: "o w",
                    }),
                    new StringMatchQuery({
                        exactMatch: true,
                        value: "orld",
                    }),
                ],
            }),
            (r) => {
                response = r;
            },
            { amount: 1 }
        );
        expect(response).toBeDefined();
        expect(response).toMatchObject(
            new StringResult({
                string: "hello world",
                metadatas: new RangeMetadatas({
                    metadatas: [
                        new RangeMetadata({
                            length: BigInt("o w".length),
                            offset: BigInt("hell".length),
                        }),
                        new RangeMetadata({
                            length: BigInt("orld".length),
                            offset: BigInt("hello w".length),
                        }),
                    ],
                }),
            })
        );
    });

    it("toString remote", async () => {
        await writeStore.add(
            "hello",
            new Range({ offset: 0n, length: "hello".length })
        );
        await writeStore.add(
            "world",
            new Range({
                offset: BigInt("hello ".length),
                length: "world".length,
            })
        );

        let callbackValues: string[] = [];
        const string = await observerStore.toString({
            remote: {
                callback: (s) => callbackValues.push(s),
                queryOptions: { amount: 1 },
            },
        });
        expect(string).toEqual("hello world");
        expect(callbackValues).toEqual(["hello world"]);
    });

    it("handles AccessError gracefully", async () => {
        const store = new DString({});
        await store.init(
            writer,
            new Blocks(
                new LibP2PBlockStore(writer, new MemoryLevelBlockStore())
            ),
            await createIdentity(),
            {
                topic: uuid(),
                store: {
                    ...DefaultOptions,
                    encryption: {
                        getAnyKeypair: (_) => Promise.resolve(undefined),
                        getEncryptionKeypair: () => Ed25519Keypair.create(),
                    },
                    replicate: true,
                    resolveCache: () => new Cache(cacheStore1),
                },
            }
        );

        await store.add(
            "hello",
            new Range({ offset: 0n, length: "hello".length }),
            {
                reciever: {
                    metadata: undefined,
                    signatures: undefined,
                    next: undefined,
                    payload: [await X25519PublicKey.create()],
                },
            }
        );
        await store.close();
        await delay(3000); // TODO store is async?
        await store.load();
        await waitFor(() => store.store.oplog.values.length === 1);
    });
});
