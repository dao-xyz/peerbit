import { delay } from "@dao-xyz/peerbit-time";
import { LSession, waitForPeers } from "@dao-xyz/peerbit-test-utils";
import { Ed25519Keypair, SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { Ed25519Identity, Entry } from "@dao-xyz/peerbit-log";
import { Program } from "@dao-xyz/peerbit-program";
import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { ClockService } from "../controller";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { MemoryLevel } from "memory-level";
import { default as Cache } from "@dao-xyz/peerbit-cache";
import { v4 as uuid } from "uuid";
import {

    LibP2PBlockStore,
    MemoryLevelBlockStore,
    Blocks,
} from "@dao-xyz/peerbit-block";

const createIdentity = async () => {
    const ed = await Ed25519Keypair.create();
    return {
        publicKey: ed.publicKey,
        privateKey: ed.privateKey,
        sign: (data) => ed.sign(data),
    } as Ed25519Identity;
};

const maxTimeError = 3000;
@variant("clock-test")
class P extends Program {
    @field({ type: ClockService })
    clock: ClockService;

    constructor(properties?: { clock: ClockService }) {
        super();
        if (properties) {
            this.clock = properties.clock;
        }
    }

    async setup(): Promise<void> {
        await this.clock.setup({ maxTimeError });
    }
}

describe("clock", () => {
    let session: LSession, responder: P, reader: P, readerStore: Blocks;
    beforeAll(async () => {
        session = await LSession.connected(3);
        const responderIdentity = await createIdentity();
        const topic = uuid();
        responder = new P({
            clock: new ClockService({
                trustedNetwork: new TrustedNetwork({
                    rootTrust: responderIdentity.publicKey,
                }),
            }),
        });
        await responder.init(
            session.peers[0],
            new Blocks(
                new LibP2PBlockStore(
                    session.peers[0],
                    new MemoryLevelBlockStore()
                )
            ),
            responderIdentity,
            {
                topic,
                replicate: true,
                store: {
                    cacheId: "id",
                    resolveCache: () =>
                        Promise.resolve(new Cache(new MemoryLevel())),
                } as any,
            } as any
        );

        responder.clock._maxError = BigInt(maxTimeError * 1e6);

        reader = deserialize(serialize(responder), P);
        readerStore = new Blocks(
            new LibP2PBlockStore(session.peers[1], new MemoryLevelBlockStore())
        );
        await reader.init(
            session.peers[1],
            readerStore,
            await createIdentity(),
            {
                topic,
                store: {
                    cacheId: "id",
                    resolveCache: () =>
                        Promise.resolve(new Cache(new MemoryLevel())),
                } as any,
            } as any
        );

        await waitForPeers(session.peers[1], [session.peers[0]], topic);
    });
    afterAll(async () => {
        await session.stop();
    });

    it("signs and verifies", async () => {
        const entry = await Entry.create({
            data: "hello world",
            identity: reader.identity,
            store: readerStore,
            signers: [
                async (data: Uint8Array) =>
                    new SignatureWithKey({
                        publicKey: reader.identity.publicKey,
                        signature: await reader.identity.sign(data),
                    }),
                reader.clock.sign.bind(reader.clock),
            ],
        });
        expect(
            await Promise.all(
                entry.signatures.map((x) => x.publicKey.hashcode())
            )
        ).toContainAllValues(
            await Promise.all([
                reader.identity.publicKey.hashcode(),
                responder.identity.publicKey.hashcode(),
            ])
        );
        expect(await reader.clock.verify(entry)).toBeTrue();
    });

    it("reject old entry", async () => {
        await expect(
            Entry.create({
                data: "hello world",
                identity: reader.identity,
                store: readerStore,
                signers: [
                    async (data: Uint8Array) =>
                        new SignatureWithKey({
                            publicKey: reader.identity.publicKey,
                            signature: await reader.identity.sign(data),
                        }),
                    async (data: Uint8Array) => {
                        await delay(maxTimeError + 1000);
                        return reader.clock.sign(data);
                    },
                ],
            })
        ).rejects.toThrowError(
            new Error("Recieved an entry with an invalid timestamp")
        );
    });
});
