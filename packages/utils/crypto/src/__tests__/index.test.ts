import {
    Ed25519Keypair,
    X25519Keypair,
    verifySignatureSecp256k1,
    verifySignatureEd25519,
    Secp256k1PublicKey,
} from "../index.js";
import sodium from "libsodium-wrappers";
import { deserialize, serialize } from "@dao-xyz/borsh";
import { Wallet } from "@ethersproject/wallet";
import { LSession } from "@dao-xyz/peerbit-test-utils";
import { PeerIdAddress } from "../libp2p.js";
await sodium.ready;

describe("Ed25519", () => {
    it("ser/der", () => {
        const keypair = Ed25519Keypair.create();
        const derser = deserialize(serialize(keypair), Ed25519Keypair);
        expect(derser.publicKey.publicKey).toEqual(keypair.publicKey.publicKey);
    });

    /*   it('size', async () => {
          const kp = await Ed25519Keypair.create();
          expect(serialize(kp.publicKey)).toHaveLength(PUBLIC_KEY_WIDTH);
      }) */

    it("verify native", () => {
        const keypair = sodium.crypto_sign_keypair();
        const data = new Uint8Array([1, 2, 3]);
        const signature = sodium.crypto_sign_detached(data, keypair.privateKey);
        const isVerified = verifySignatureEd25519(
            signature,
            keypair.publicKey,
            data
        );
        expect(isVerified).toBeTrue();
    });

    it("verify primitve", () => {
        const keypair = Ed25519Keypair.create();
        const data = new Uint8Array([1, 2, 3]);
        const signature = keypair.sign(data);
        const isVerified = verifySignatureEd25519(
            signature,
            keypair.publicKey,
            data
        );
        expect(isVerified).toBeTrue();

        const isNotVerified = verifySignatureEd25519(
            signature,
            keypair.publicKey,
            data.reverse()
        );
        expect(isNotVerified).toBeFalse();
    });
});

describe("X25519", () => {
    it("ser/der", () => {
        const keypair = X25519Keypair.create();
        const derser = deserialize(serialize(keypair), X25519Keypair);
        expect(derser.publicKey.publicKey).toEqual(keypair.publicKey.publicKey);
    });

    /*  it('size', async () => {
         const kp = await X25519Keypair.create();
         expect(serialize(kp.publicKey)).toHaveLength(PUBLIC_KEY_WIDTH);
     }) */
});

describe("Sepck2561k1", () => {
    it("verify", async () => {
        const wallet = Wallet.createRandom();
        const data = new Uint8Array([1, 2, 3]);
        const pk = new Secp256k1PublicKey({
            address: await wallet.getAddress(),
        });
        const signature = await wallet.signMessage(data);
        let signatureBytes = new Uint8Array(Buffer.from(signature));
        const isVerified = await verifySignatureSecp256k1(
            signatureBytes,
            pk,
            data
        );
        expect(isVerified).toBeTrue();

        const isNotVerified = await verifySignatureSecp256k1(
            new Uint8Array(Buffer.from(signature)),
            pk,
            data.reverse()
        );
        expect(isNotVerified).toBeFalse();
    });

    it("ser/der", async () => {
        const wallet = await Wallet.createRandom();
        const pk = new Secp256k1PublicKey({
            address: await wallet.getAddress(),
        });
        const derser = deserialize(serialize(pk), Secp256k1PublicKey);
        expect(derser.address).toEqual(pk.address);
    });
});

describe("libp2p", () => {
    let session: LSession;
    beforeAll(async () => {
        session = await LSession.connected(1);
    });

    afterAll(async () => {
        await session.stop();
    });

    it("ser/der", async () => {
        const pk = new PeerIdAddress({
            address: session.peers[0].peerId.toString(),
        });
        const derser = deserialize(serialize(pk), PeerIdAddress);
        expect(derser.address).toEqual(pk.address);
    });

    /*  it('size', async () => {
         const pk = new PeerIdAddress({
             address: session.peers[0].id.toString()
         });
         expect(serialize(pk)).toHaveLength(PUBLIC_KEY_WIDTH);
     }) */
});
