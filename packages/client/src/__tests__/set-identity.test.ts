import { Peerbit } from "../peer";

import fs from "fs";
import rmrf from "rimraf";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { EventStore } from "./utils/stores";

import { Level } from "level";
import { Ed25519Keypair } from "@dao-xyz/peerbit-crypto";
import { jest } from "@jest/globals";

// Include test utilities
import { LSession } from "@dao-xyz/peerbit-test-utils";

const keysPath = "./orbitdb/identity/identitykeys";
const dbPath = "./orbitdb/tests/change-identity";

export const createStore = (path = "./keystore"): Level => {
    if (fs && fs.mkdirSync) {
        fs.mkdirSync(path, { recursive: true });
    }
    return new Level(path, { valueEncoding: "view" });
};

describe(`Set identities`, function () {
    let session: LSession, orbitdb: Peerbit, keystore: Keystore, options: any;
    let signKey1: KeyWithMeta<Ed25519Keypair>,
        signKey2: KeyWithMeta<Ed25519Keypair>;

    beforeAll(async () => {
        rmrf.sync(dbPath);
        session = await LSession.connected(1);

        if (fs && fs.mkdirSync) fs.mkdirSync(keysPath, { recursive: true });
        const identityStore = await createStore(keysPath);

        keystore = new Keystore(identityStore);
        signKey1 =
            (await keystore.createEd25519Key()) as KeyWithMeta<Ed25519Keypair>;
        signKey2 =
            (await keystore.createEd25519Key()) as KeyWithMeta<Ed25519Keypair>;
        orbitdb = await Peerbit.create(session.peers[0], { directory: dbPath });
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
