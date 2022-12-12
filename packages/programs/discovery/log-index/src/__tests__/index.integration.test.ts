import {
    createStore,
    LSession,
    waitForPeers,
} from "@dao-xyz/peerbit-test-utils";
import {
    Ed25519Keypair,
    Ed25519PublicKey,
    EncryptedThing,
    X25519Keypair,
    X25519PublicKey,
} from "@dao-xyz/peerbit-crypto";
import { v4 as uuid } from "uuid";
import { DefaultOptions, Store } from "@dao-xyz/peerbit-store";
import Cache from "@dao-xyz/peerbit-cache";
import { AbstractLevel } from "abstract-level";
import path from "path";
import {
    HeadsMessage,
    LogEntryEncryptionQuery,
    LogIndex,
    LogQueryRequest,
} from "../controller";
import { fileURLToPath } from "url";
import { waitFor } from "@dao-xyz/peerbit-time";
import { RPC } from "@dao-xyz/peerbit-rpc";
import { Address } from "@dao-xyz/peerbit-program";
import { MemoryLevelBlockStore, Blocks } from "@dao-xyz/peerbit-block";

describe("query", () => {
    let session: LSession,
        cacheStores: AbstractLevel<any, string, Uint8Array>[],
        logIndices: LogIndex[],
        headsCount = 3,
        peersCount = 3;

    const __filename = fileURLToPath(import.meta.url);

    beforeEach(async () => {
        session = await LSession.connected(peersCount);
        cacheStores = [];
        logIndices = [];
        for (let i = 0; i < peersCount; i++) {
            cacheStores.push(
                await createStore(path.join(__filename, "cache-" + i))
            );
        }

        const topic = uuid();
        for (let i = 0; i < peersCount; i++) {
            const store = new Store({ storeIndex: 0 });
            const signKey = await Ed25519Keypair.create();
            const cache = new Cache(cacheStores[i]);
            const logIndex = new LogIndex({ query: new RPC() });
            const blockStore = new Blocks(new MemoryLevelBlockStore());
            logIndex.query.parentProgram = {
                address: new Address({ cid: "1" }),
            } as any; // because query topic needs a parent with address
            await logIndex.setup({
                store,
                rpcTopic: { rpcRegion: topic },
                context: "context",
            });
            logIndices.push(logIndex);
            const encryption = {
                getEncryptionKeypair: () => signKey,
                getAnyKeypair: async (publicKeys: X25519PublicKey[]) => {
                    for (let i = 0; i < publicKeys.length; i++) {
                        if (
                            publicKeys[i].equals(
                                await X25519PublicKey.from(
                                    signKey.publicKey as Ed25519PublicKey
                                )
                            )
                        ) {
                            return {
                                index: i,
                                keypair: await X25519Keypair.from(signKey),
                            };
                        }
                    }
                },
            };
            await store.init(
                blockStore,
                {
                    ...signKey,
                    sign: async (data: Uint8Array) => await signKey.sign(data),
                },
                {
                    ...DefaultOptions,
                    encryption,
                    replicate: i === 0,
                    resolveCache: () => Promise.resolve(cache),
                }
            );
            await logIndex.init(
                session.peers[i],
                blockStore,
                {
                    ...signKey,
                    sign: async (data: Uint8Array) => await signKey.sign(data),
                },
                {
                    topic,
                    store: {
                        ...DefaultOptions,
                        encryption,
                        replicate: i === 0,
                        resolveCache: () => Promise.resolve(cache),
                    },
                }
            );
        }
        await waitForPeers(session.peers[1], [session.peers[0].peerId], topic);
        await waitForPeers(session.peers[2], [session.peers[0].peerId], topic);
        expect(topic).toEqual(logIndices[0].query.rpcTopic);
        expect(logIndices[0].query.rpcTopic).toEqual(
            logIndices[1].query.rpcTopic
        );
        expect(logIndices[1].query.rpcTopic).toEqual(
            logIndices[2].query.rpcTopic
        );
    });

    afterEach(async () => {
        await session.stop();
        await Promise.all(logIndices.map((l) => l.drop()));
        await Promise.all(logIndices.map((l) => l._store.drop()));
        await Promise.all(cacheStores.map((l) => l.close()));
    });

    describe("any", () => {
        it("will return all entries if pass empty querey", async () => {
            for (let i = 0; i < headsCount; i++) {
                await logIndices[0].store._addOperation(i, { nexts: [] });
            }
            const responses: HeadsMessage[] = [];
            await logIndices[1].query.send(
                new LogQueryRequest({
                    queries: [],
                }),
                (m) => {
                    responses.push(m);
                },
                { amount: 1 }
            );
            expect(responses).toHaveLength(1);
            expect(responses[0].heads).toHaveLength(headsCount);
        });
    });

    describe("encryption query", () => {
        it("can query by payload key", async () => {
            // send from 1 to 2
            for (let i = 0; i < headsCount; i++) {
                await logIndices[1].store._addOperation(i, {
                    nexts: [],
                    reciever: {
                        payload: await X25519PublicKey.from(
                            logIndices[2].store.identity
                                .publicKey as Ed25519PublicKey
                        ),
                        next: undefined,
                        metadata: undefined,
                        signatures: undefined,
                    },
                });
            }

            // sync 1 with 0, so we can query from 0 (0 is the only one who is replicating, i.e. responding to queries)
            logIndices[1].store.oplog.heads.map(
                (h) => delete (h._payload as EncryptedThing<any>)._decrypted
            );
            await logIndices[0].store.sync(logIndices[1].store.oplog.heads);
            await waitFor(
                () => logIndices[0].store.oplog.values.length === headsCount
            );

            // read from observer 1
            const responses: HeadsMessage[] = [];

            await logIndices[2].query.send(
                new LogQueryRequest({
                    queries: [
                        new LogEntryEncryptionQuery({
                            payload: [
                                await X25519PublicKey.from(
                                    logIndices[2].store.identity
                                        .publicKey as Ed25519PublicKey
                                ),
                            ],
                            next: [],
                            metadata: [],
                            signatures: [],
                        }),
                    ],
                }),
                (m) => {
                    responses.push(m);
                },
                { amount: 1 }
            );
            expect(responses).toHaveLength(1);
            expect(responses[0].heads).toHaveLength(headsCount);
        });
    });
});
