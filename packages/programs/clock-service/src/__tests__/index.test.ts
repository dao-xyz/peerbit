import { waitFor } from "@dao-xyz/peerbit-time";
import { Session, waitForPeers } from "@dao-xyz/peerbit-test-utils";
import { Ed25519Keypair, SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { Ed25519Identity, Entry } from "@dao-xyz/ipfs-log";
import { Program } from "@dao-xyz/peerbit-program";
import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { ClockService } from "../controller";
import { TrustedNetwork } from "@dao-xyz/peerbit-trusted-network";
import { MemoryLevel } from "memory-level";
import { default as Cache } from "@dao-xyz/peerbit-cache";

const createIdentity = async () => {
    const ed = await Ed25519Keypair.create();
    return {
        publicKey: ed.publicKey,
        privateKey: ed.privateKey,
        sign: (data) => ed.sign(data),
    } as Ed25519Identity;
};

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
        await this.clock.setup();
    }
}

describe("clock", () => {
    let session: Session, responder: P, reader: P;
    beforeAll(async () => {
        session = await Session.connected(3);
        const responderIdentity = await createIdentity();
        responder = new P({
            clock: new ClockService({
                trustedNetwork: new TrustedNetwork({
                    rootTrust: responderIdentity.publicKey,
                }),
            }),
        });
        await responder.init(session.peers[0].ipfs, responderIdentity, {
            store: {
                resolveCache: () =>
                    Promise.resolve(new Cache(new MemoryLevel())),
                replicate: true,
            } as any,
        } as any);
        reader = deserialize(serialize(responder), P);
        await reader.init(session.peers[1].ipfs, await createIdentity(), {
            store: {
                resolveCache: () =>
                    Promise.resolve(new Cache(new MemoryLevel())),
            } as any,
        } as any);

        await waitForPeers(
            session.peers[1].ipfs,
            [session.peers[0].id],
            responder.clock._remoteSigner.queryTopic
        );
    });
    afterAll(async () => {
        await session.stop();
    });

    it("signs and verifies", async () => {
        const entry = await Entry.create({
            data: "hello world",
            identity: reader.identity,
            ipfs: reader.ipfs,
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
            entry.signatures.map((x) => x.publicKey.hashCode())
        ).toContainAllValues([
            reader.identity.publicKey.hashCode(),
            responder.identity.publicKey.hashCode(),
        ]);
        expect(await reader.clock.verify(entry)).toBeTrue();
    });
});
