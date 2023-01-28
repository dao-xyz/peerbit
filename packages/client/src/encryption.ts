import {
    AccessError,
    Ed25519Keypair,
    PublicKeyEncryptionResolver,
    X25519Keypair,
} from "@dao-xyz/peerbit-crypto";
import { Keystore } from "@dao-xyz/peerbit-keystore";
import { Identity } from "@dao-xyz/peerbit-log";

export type StorePublicKeyEncryption = (
    replicationTopic: string
) => PublicKeyEncryptionResolver;

export const encryptionWithRequestKey = async (
    identity: Identity,
    keystore: Keystore
): Promise<PublicKeyEncryptionResolver> => {
    let defaultEncryptionKey = await keystore.getKey(identity.publicKey); // TODO add key rotation, potentially generate new key every call
    // TODO key rotation
    if (
        !defaultEncryptionKey ||
        (defaultEncryptionKey instanceof Ed25519Keypair === false &&
            defaultEncryptionKey instanceof X25519Keypair === false)
    ) {
        defaultEncryptionKey = await keystore.createEd25519Key();
    }

    return {
        getAnyKeypair: async (publicKeys) => {
            for (let i = 0; i < publicKeys.length; i++) {
                const key = await keystore.getKey(publicKeys[i]);
                if (
                    key &&
                    (key.keypair instanceof Ed25519Keypair ||
                        key.keypair instanceof X25519Keypair)
                ) {
                    return {
                        index: i,
                        keypair: key.keypair,
                    };
                }
            }
            throw new AccessError("Failed to access key");
        },

        getEncryptionKeypair: () => {
            return defaultEncryptionKey!.keypair as
                | Ed25519Keypair
                | X25519Keypair;
        },
    };
};
