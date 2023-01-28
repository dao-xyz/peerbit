import {
    Ed25519Keypair,
    X25519Keypair,
    verifySignatureSecp256k1,
    verifySignatureEd25519,
    Secp256k1Keccak256PublicKey,
    Sec256k1Keccak256Keypair,
    verify,
    Ed25519PublicKey,
} from "../index.js";
import sodium from "libsodium-wrappers";
import { deserialize, serialize } from "@dao-xyz/borsh";
import { Wallet } from "@ethersproject/wallet";
import { createSecp256k1PeerId } from "@libp2p/peer-id-factory";
import { supportedKeys } from "@libp2p/crypto/keys";
import crypto from "crypto";
await sodium.ready;

describe("Ed25519", () => {
    it("ser/der", () => {
        const keypair = Ed25519Keypair.create();
        const derser = deserialize(serialize(keypair), Ed25519Keypair);
        expect(derser.publicKey.publicKey).toEqual(keypair.publicKey.publicKey);
    });

    it("verify native", () => {
        const keypair = sodium.crypto_sign_keypair();
        const data = new Uint8Array([1, 2, 3]);
        const signature = sodium.crypto_sign_detached(data, keypair.privateKey);
        const isVerified = verifySignatureEd25519(
            signature,
            new Ed25519PublicKey({ publicKey: keypair.publicKey }),
            data
        );
        expect(isVerified).toBeTrue();
    });

    it("verify", () => {
        const kp = crypto
            .generateKeyPairSync("ed25519")
            .privateKey.export({ format: "der", type: "pkcs8" });
        const keypair = Ed25519Keypair.create();
        const data = new Uint8Array([1, 2, 3]);
        const signature = keypair.sign(data);
        const isVerified = verifySignatureEd25519(
            signature,
            keypair.publicKey,
            data
        );
        expect(isVerified).toBeTrue();

        const isNotVerified = verify(
            signature,
            keypair.publicKey,
            data.reverse()
        );
        expect(isNotVerified).toBeFalse();
    });

    it("verify hashed", () => {
        const keypair = Ed25519Keypair.create();
        const data = new Uint8Array([1, 2, 3]);
        const signature = keypair.sign(data, true);
        const isVerified = verify(signature, keypair.publicKey, data, true);
        expect(isVerified).toBeTrue();

        const isNotVerified = verifySignatureEd25519(
            signature,
            keypair.publicKey,
            data.reverse(),
            true
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
});

describe("Sepck2561k1Keccak256", () => {
    const data = new Uint8Array([1, 2, 3]);

    it("verify", async () => {
        const wallet = Wallet.createRandom();
        const pk = new Secp256k1Keccak256PublicKey({
            address: wallet.address,
        });
        const signature = await wallet.signMessage(data);
        let signatureBytes = Buffer.from(signature);
        const isVerified = await verifySignatureSecp256k1(
            signatureBytes,
            pk,
            data
        );
        expect(isVerified).toBeTrue();

        const isNotVerified = await verifySignatureSecp256k1(
            Buffer.from(signature),
            pk,
            data.reverse()
        );
        expect(isNotVerified).toBeFalse();
    });
    it("from PeerId", async () => {
        const peerId = await createSecp256k1PeerId();
        const keypair = Sec256k1Keccak256Keypair.from(peerId);
        const privateKey = new supportedKeys["secp256k1"].Secp256k1PrivateKey(
            peerId.privateKey!.slice(4)
        );
        const publicKeyComputed = privateKey.public;
        expect(publicKeyComputed.bytes).toEqual(peerId.publicKey);
        const wallet = new Wallet(peerId.privateKey!.slice(4));
        const signature = await keypair.sign(data);
        expect(
            verifySignatureSecp256k1(signature, keypair.publicKey, data)
        ).toBeTrue();
    });

    it("ser/der", async () => {
        const wallet = await Wallet.createRandom();
        const pk = new Secp256k1Keccak256PublicKey({
            address: wallet.address,
        });
        const derser = deserialize(serialize(pk), Secp256k1Keccak256PublicKey);
        expect(derser.address).toEqual(pk.address);
    });
});
