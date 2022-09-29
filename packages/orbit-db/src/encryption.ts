import { AccessError, PublicKeyEncryption } from '@dao-xyz/encryption-utils';
import { BoxKeyWithMeta, Keystore, SignKeyWithMeta } from '@dao-xyz/orbit-db-keystore';
import { StorePublicKeyEncryption } from '@dao-xyz/orbit-db-store';
import { X25519PublicKey, Ed25519PublicKey } from 'sodium-plus';
import { PublicKey } from "@dao-xyz/identity";
import { serialize } from '@dao-xyz/borsh'
export const replicationTopicEncryptionWithRequestKey = (identity: PublicKey, keystore: Keystore, requestKey?: (key: X25519PublicKey, replicationTopic) => Promise<BoxKeyWithMeta[] | undefined>): StorePublicKeyEncryption => {
    return (replicationTopic: string) => {
        return encryptionWithRequestKey(identity, keystore, (key) => requestKey(key, replicationTopic))
    }
}


export const encryptionWithRequestKey = (identity: PublicKey, keystore: Keystore, requestKey?: (key: X25519PublicKey) => Promise<BoxKeyWithMeta[] | undefined>): PublicKeyEncryption => {

    return {
        getAnySecret: async (publicKeys) => {
            for (let i = 0; i < publicKeys.length; i++) {
                const key = await keystore.getKeyById(publicKeys[i]);
                if (key instanceof BoxKeyWithMeta && key.secretKey) {
                    return {
                        index: i,
                        secretKey: key.secretKey
                    }
                }
            }
            if (requestKey) {
                for (let i = 0; i < publicKeys.length; i++) {
                    const newKeys = await requestKey(publicKeys[i]);
                    if (!newKeys || newKeys.length === 0 || !newKeys[0].secretKey) {
                        continue;
                    }
                    return {
                        index: i,
                        secretKey: newKeys[0].secretKey
                    }
                }
            }
            throw new AccessError("Failed to access key")
        },
        getEncryptionKey: async () => {
            // TODO key rotation
            const keyId = serialize(identity);
            let key = await keystore.getKeyByPath(keyId, BoxKeyWithMeta); // TODO add key rotation, potentially generate new key every call
            if (!key) {
                key = await keystore.createKey(keyId, BoxKeyWithMeta);
            }

            // TODO can secretKey be missing?
            if (!key.secretKey) {
                throw new Error("Missing secret key using the sign key for retrieval")
            }
            return key.secretKey;
        }
    }
}