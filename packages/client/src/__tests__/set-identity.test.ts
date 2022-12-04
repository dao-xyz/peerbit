import { Peerbit } from "../peer";

import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { EventStore } from "./utils/stores";

import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";

// Include test utilities
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { MemoryLevel } from "memory-level";

export const createStore = (path = "./keystore"): MemoryLevel => {
    return new MemoryLevel({ valueEncoding: "view" });
};

describe(`Set identities`, function () {
    let session: LSession, orbitdb: Peerbit, keystore: Keystore, options: any;
    let signKey1: KeyWithMeta<Ed25519Keypair>,
        signKey2: KeyWithMeta<Ed25519Keypair>;

    beforeAll(async () => {
        session = await LSession.connected(1);

        const identityStore = await createStore();

        keystore = new Keystore(identityStore);
        signKey1 =
            (await keystore.createEd25519Key()) as KeyWithMeta<Ed25519Keypair>;
        signKey2 =
            (await keystore.createEd25519Key()) as KeyWithMeta<Ed25519Keypair>;
        orbitdb = await Peerbit.create(session.peers[0]);
    });

    afterAll(async () => {
        await keystore.close();
        if (orbitdb) await orbitdb.stop();

        await session.stop();
    });

    beforeEach(async () => {
        options = Object.assign({}, options, {});
    });

    it("sets identity", async () => {
        const db = await orbitdb.open(
            new EventStore<string>({
                id: "abc",
            }),
            options
        );
        expect(db.store.identity.publicKey.equals(orbitdb.identity.publicKey));
        db.store.setIdentity({
            publicKey: signKey1.keypair.publicKey,
            privateKey: signKey1.keypair.privateKey,
            sign: (data) => signKey1.keypair.sign(data),
        });
        expect(db.store.identity.publicKey.equals(signKey1.keypair.publicKey));
        await db.close();
    });
});
