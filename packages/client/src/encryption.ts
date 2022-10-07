import { AccessError, Ed25519Keypair, PublicKeyEncryptionResolver, X25519Keypair } from "@dao-xyz/peerbit-crypto";
import { Keystore } from '@dao-xyz/orbit-db-keystore';
import { StorePublicKeyEncryption } from '@dao-xyz/orbit-db-store';
import { X25519PublicKey } from "@dao-xyz/peerbit-crypto";
import { serialize } from '@dao-xyz/borsh'
import { Identity } from "@dao-xyz/ipfs-log";

export const replicationTopicEncryptionWithRequestKey = (identity: Identity, keystore: Keystore, requestKey: (key: X25519PublicKey, replicationTopic: string) => Promise<(Ed25519Keypair | X25519Keypair)[] | undefined>): StorePublicKeyEncryption => {
    return (replicationTopic: string) => {
        return encryptionWithRequestKey(identity, keystore, (key) => requestKey(key, replicationTopic))
    }
}


export const encryptionWithRequestKey = (identity: Identity, keystore: Keystore, requestKey?: (key: X25519PublicKey) => Promise<(Ed25519Keypair | X25519Keypair)[] | undefined>): PublicKeyEncryptionResolver => {

    return {
        getAnyKeypair: async (publicKeys) => {
            for (let i = 0; i < publicKeys.length; i++) {
                const key = await keystore.getKey(publicKeys[i]);
                if (key && (key.keypair instanceof Ed25519Keypair || key.keypair instanceof X25519Keypair)) {
                    return {
                        index: i,
                        keypair: key.keypair
                    }
                }
            }
            if (requestKey) {
                for (let i = 0; i < publicKeys.length; i++) {
                    const newKeys = await requestKey(publicKeys[i]);
                    if (!newKeys || newKeys.length === 0) {
                        continue;
                    }
                    for (const key of newKeys) {
                        if (key instanceof Ed25519Keypair || key instanceof X25519Keypair) {
                            return {
                                index: i,
                                keypair: newKeys[0]
                            }
                        }
                    }

                }
            }
            throw new AccessError("Failed to access key")
        },

        getEncryptionKeypair: async () => {
            // TODO key rotation
            const keyId = serialize(identity);
            let key = await keystore.getKey(keyId); // TODO add key rotation, potentially generate new key every call
            if (!key || key instanceof Ed25519Keypair === false && key instanceof X25519Keypair === false) {
                key = await keystore.createEd25519Key();
            }
            return key.keypair as (Ed25519Keypair | X25519Keypair);
        }
    }
}