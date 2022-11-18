import {
    AccessError,
    Ed25519Keypair,
    PublicKeyEncryptionResolver,
    X25519Keypair,
} from "@dao-xyz/peerbit-crypto";
import { Keystore, KeyWithMeta } from "@dao-xyz/peerbit-keystore";
import { X25519PublicKey } from "@dao-xyz/peerbit-crypto";
import { Identity } from "@dao-xyz/ipfs-log";

export type StorePublicKeyEncryption = (
    replicationTopic: string
) => PublicKeyEncryptionResolver;

/* export const replicationTopicEncryptionWithRequestKey = (identity: Identity, keystore: Keystore, requestKey: (key: X25519PublicKey) => Promise<KeyWithMeta<(Ed25519Keypair | X25519Keypair)>[] | undefined>): PublicKeyEncryptionResolver => {
    return encryptionWithRequestKey(identity, keystore, async (key) => requestKey(key))

}
 */

export const encryptionWithRequestKey = (
    identity: Identity,
    keystore: Keystore,
    requestKey?: (
        key: X25519PublicKey
    ) => Promise<KeyWithMeta<Ed25519Keypair | X25519Keypair>[] | undefined>
): PublicKeyEncryptionResolver => {
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
            if (requestKey) {
                for (let i = 0; i < publicKeys.length; i++) {
                    const newKeys = await requestKey(publicKeys[i]);
                    if (!newKeys || newKeys.length === 0) {
                        continue;
                    }
                    for (const key of newKeys) {
                        if (
                            key.keypair instanceof Ed25519Keypair ||
                            key.keypair instanceof X25519Keypair
                        ) {
                            return {
                                index: i,
                                keypair: newKeys[0].keypair,
                            };
                        }
                    }
                }
            }
            throw new AccessError("Failed to access key");
        },

        getEncryptionKeypair: async () => {
            // TODO key rotation
            let key = await keystore.getKey(identity.publicKey); // TODO add key rotation, potentially generate new key every call
            if (
                !key ||
                (key instanceof Ed25519Keypair === false &&
                    key instanceof X25519Keypair === false)
            ) {
                key = await keystore.createEd25519Key();
            }
            return key.keypair as Ed25519Keypair | X25519Keypair;
        },
    };
};
