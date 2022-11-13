import {
  DecryptedThing,
  X25519PublicKey,
  PublicKeyEncryptionResolver,
  Ed25519PrivateKey,
  verifySignatureEd25519,
  Ed25519Keypair,
  X25519Keypair,
} from "../index.js";
import sodium from "libsodium-wrappers";

describe("encryption", function () {
  const config = (keypair: Ed25519Keypair | X25519Keypair) => {
    return {
      getEncryptionKeypair: () => Promise.resolve(keypair),
      getAnyKeypair: async (publicKeys: X25519PublicKey[]) => {
        const pk =
          keypair.publicKey instanceof X25519PublicKey
            ? keypair.publicKey
            : await X25519PublicKey.from(keypair.publicKey);
        for (let i = 0; i < publicKeys.length; i++) {
          if (publicKeys[i].equals(pk)) {
            return {
              index: i,
              keypair,
            };
          }
        }
      },
    } as PublicKeyEncryptionResolver;
  };
  it("encrypt", async () => {
    await sodium.ready;
    const senderKey = await X25519Keypair.create();
    const recieverKey1 = await X25519Keypair.create();
    const recieverKey2 = await X25519Keypair.create();

    const data = new Uint8Array([1, 2, 3]);
    const decrypted = new DecryptedThing({
      data,
    });

    const reciever1Config = config(recieverKey1);
    const reciever2Config = config(recieverKey2);

    const encrypted = await decrypted.encrypt(
      senderKey,
      recieverKey1.publicKey,
      recieverKey2.publicKey
    );
    encrypted._decrypted = undefined;

    const decryptedFromEncrypted1 = await encrypted.decrypt(
      reciever1Config.getAnyKeypair
    );
    expect(decryptedFromEncrypted1._data).toStrictEqual(data);

    const decryptedFromEncrypted2 = await encrypted.decrypt(
      reciever2Config.getAnyKeypair
    );
    expect(decryptedFromEncrypted2._data).toStrictEqual(data);
  });

  it("it can use ed25519 for encryption", async () => {
    const senderKey = await Ed25519Keypair.create();
    const recieverKey1 = await Ed25519Keypair.create();
    const recieverKey2 = await Ed25519Keypair.create();

    const reciever1Config = config(recieverKey1);
    const reciever2Config = config(recieverKey2);

    const data = new Uint8Array([1, 2, 3]);
    const decrypted = new DecryptedThing({
      data,
    });

    const encrypted = await decrypted.encrypt(
      senderKey,
      recieverKey1.publicKey,
      recieverKey2.publicKey
    );
    encrypted._decrypted = undefined;

    const decryptedFromEncrypted1 = await encrypted.decrypt(
      reciever1Config.getAnyKeypair
    );
    expect(decryptedFromEncrypted1._data).toStrictEqual(data);

    const decryptedFromEncrypted2 = await encrypted.decrypt(
      reciever2Config.getAnyKeypair
    );
    expect(decryptedFromEncrypted2._data).toStrictEqual(data);
  });
});

describe("ed25519", function () {
  it("can sign verify", async () => {
    await sodium.ready;
    const data = new Uint8Array([1, 2, 3]);
    const senderKey = await Ed25519Keypair.create();
    const signature = await senderKey.sign(data);
    expect(signature).toHaveLength(64); // detached
    const verify = await verifySignatureEd25519(
      signature,
      senderKey.publicKey.publicKey,
      data
    );
    expect(verify);
  });
});
