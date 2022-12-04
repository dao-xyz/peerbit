import rmrf from "rimraf";
import fs from "fs-extra";
import { Log } from "../log.js";
import { createStore, Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { LSession, waitForPeers } from "@dao-xyz/peerbit-test-utils";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { dirname } from "path";
import { fileURLToPath } from "url";
import { Entry } from "../entry.js";
import path from "path";
import {
    LibP2PBlockStore,
    MemoryLevelBlockStore,
    Blocks,
    DEFAULT_BLOCK_TRANSPORT_TOPIC,
} from "@dao-xyz/peerbit-block";
import { signingKeysFixturesPath, testKeyStorePath } from "./utils.js";

const __filename = fileURLToPath(import.meta.url);
const __filenameBase = path.parse(__filename).base;
const __dirname = dirname(__filename);

describe("ipfs-log - Replication", function () {
    let session: LSession,
        store: Blocks,
        store2: Blocks,
        signKey: KeyWithMeta<Ed25519Keypair>,
        signKey2: KeyWithMeta<Ed25519Keypair>;

    let keystore: Keystore;

    beforeAll(async () => {
        rmrf.sync(testKeyStorePath(__filenameBase));

        await fs.copy(
            signingKeysFixturesPath(__dirname),
            testKeyStorePath(__filenameBase)
        );

        // Start two connected IPFS instances
        session = await LSession.connected(2, [DEFAULT_BLOCK_TRANSPORT_TOPIC]);

        keystore = new Keystore(
            await createStore(testKeyStorePath(__filenameBase))
        );

        // Create an identity for each peers
        // @ts-ignore
        signKey = await keystore.getKey(new Uint8Array([0]));
        // @ts-ignore
        signKey2 = await keystore.getKey(new Uint8Array([1]));

        // sort keys so that the output becomes deterministic
        if (
            signKey.keypair.publicKey.publicKey >
            signKey2.keypair.publicKey.publicKey
        ) {
            signKey = [signKey2, (signKey2 = signKey)][0];
        }
        store = new Blocks(
            new LibP2PBlockStore(session.peers[0], new MemoryLevelBlockStore())
        );
        await store.open();
        store2 = new Blocks(
            new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
        );
        await store2.open();
    });

    afterAll(async () => {
        await store.close();
        await store2.close();
        rmrf.sync(testKeyStorePath(__filenameBase));
        await session.stop();

        await keystore?.close();
    });

    describe("replicates logs deterministically", function () {
        const amount = 10 + 1;
        const channel = "XXX";
        const logId = "A";

        let log1: Log<string>,
            log2: Log<string>,
            input1: Log<string>,
            input2: Log<string>;
        const buffer1: string[] = [];
        const buffer2: string[] = [];
        let processing = 0;

        const handleMessage = async (message: any, topic: string) => {
            if (
                session.peers[0].peerId.equals(message.from) ||
                message.topic !== topic
            ) {
                return;
            }
            const hash = Buffer.from(message.data).toString();
            buffer1.push(hash);
            processing++;
            process.stdout.write("\r");
            process.stdout.write(
                `> Buffer1: ${buffer1.length} - Buffer2: ${buffer2.length}`
            );
            const log = await Log.fromMultihash<string>(
                store,
                {
                    ...signKey.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey.keypair.sign(data),
                },
                hash,
                {}
            );
            await log1.join(log);
            processing--;
        };

        const handleMessage2 = async (message: any, topic: string) => {
            if (
                session.peers[1].peerId.equals(message.from) ||
                message.topic !== topic
            ) {
                return;
            }
            const hash = Buffer.from(message.data).toString();
            buffer2.push(hash);
            processing++;
            process.stdout.write("\r");
            process.stdout.write(
                `> Buffer1: ${buffer1.length} - Buffer2: ${buffer2.length}`
            );
            const log = await Log.fromMultihash<string>(
                store2,
                {
                    ...signKey2.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey2.keypair.sign(data),
                },
                hash,
                {}
            );
            await log2.join(log);
            processing--;
        };

        beforeEach(async () => {
            log1 = new Log(
                store,
                {
                    ...signKey.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey.keypair.sign(data),
                },
                { logId }
            );
            log2 = new Log(
                store2,
                {
                    ...signKey2.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey2.keypair.sign(data),
                },
                { logId }
            );
            input1 = new Log(
                store,
                {
                    ...signKey.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey.keypair.sign(data),
                },
                { logId }
            );
            input2 = new Log(
                store2,
                {
                    ...signKey2.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey2.keypair.sign(data),
                },
                { logId }
            );
            session.peers[0].pubsub.subscribe(channel);
            session.peers[1].pubsub.subscribe(channel);

            await session.peers[0].pubsub.addEventListener("message", (evt) => {
                handleMessage(evt.detail, channel);
            });
            await session.peers[1].pubsub.addEventListener("message", (evt) => {
                handleMessage2(evt.detail, channel);
            });
        });

        afterEach(async () => {
            await session.peers[0].pubsub.unsubscribe(channel);
            await session.peers[1].pubsub.unsubscribe(channel);
        });
        // TODO why is this test doing a lot of unchaught rejections? (Reproduce in VSCODE tick `Uncaught exceptions`)
        it("replicates logs", async () => {
            await waitForPeers(
                session.peers[0],
                [session.peers[1].peerId],
                channel
            );
            let prev1: Entry<any> = undefined as any;
            let prev2: Entry<any> = undefined as any;
            for (let i = 1; i <= amount; i++) {
                prev1 = await input1.append("A" + i, {
                    nexts: prev1 ? [prev1] : undefined,
                });
                prev2 = await input2.append("B" + i, {
                    nexts: prev2 ? [prev2] : undefined,
                });
                const hash1 = await input1.toMultihash();
                const hash2 = await input2.toMultihash();
                await session.peers[0].pubsub.publish(
                    channel,
                    Buffer.from(hash1)
                );
                await session.peers[1].pubsub.publish(
                    channel,
                    Buffer.from(hash2)
                );
            }

            console.log("\nAll messages sent");
            const whileProcessingMessages = (timeoutMs: number) => {
                return new Promise<void>((resolve, reject) => {
                    const timeout = setTimeout(
                        () => reject(new Error("timeout")),
                        timeoutMs
                    );
                    const timer = setInterval(() => {
                        if (
                            buffer1.length + buffer2.length === amount * 2 &&
                            processing === 0
                        ) {
                            console.log("\nAll messages received");
                            clearInterval(timer);
                            clearTimeout(timeout);
                            resolve();
                        }
                    }, 200);
                });
            };

            console.log("Waiting for all to process");
            await whileProcessingMessages(5000);

            const result = new Log<string>(
                store,
                {
                    ...signKey.keypair,
                    sign: async (data: Uint8Array) =>
                        await signKey.keypair.sign(data),
                },
                { logId }
            );
            await result.join(log1);
            await result.join(log2);

            expect(buffer1.length).toEqual(amount);
            expect(buffer2.length).toEqual(amount);
            expect(result.length).toEqual(amount * 2);
            expect(log1.length).toEqual(amount);
            expect(log2.length).toEqual(amount);
            expect(
                [0, 1, 2, 3, 9, 10].map((i) =>
                    result.values[i].payload.getValue()
                )
            ).toEqual(["A1", "B1", "A2", "B2", "B5", "A6"]);
        });
    });
});
