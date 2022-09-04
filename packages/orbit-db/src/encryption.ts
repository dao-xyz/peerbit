import { AccessError, PublicKeyEncryption } from '@dao-xyz/encryption-utils';
import { Identity } from '@dao-xyz/orbit-db-identity-provider';
import { BoxKeyWithMeta, Keystore } from '@dao-xyz/orbit-db-keystore';
import { StorePublicKeyEncryption } from '@dao-xyz/orbit-db-store';
import { X25519PublicKey } from 'sodium-plus';

export const replicationTopicEncryptionWithRequestKey = (identity: Identity, keystore: Keystore, requestKey?: (key: X25519PublicKey, replicationTopic) => Promise<BoxKeyWithMeta[] | undefined>): StorePublicKeyEncryption => {
    return (replicationTopic: string) => {
        return encryptionWithRequestKey(identity, keystore, (key) => requestKey(key, replicationTopic))
    }
}


export const encryptionWithRequestKey = (identity: Identity, keystore: Keystore, requestKey?: (key: X25519PublicKey) => Promise<BoxKeyWithMeta[] | undefined>): PublicKeyEncryption => {

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
            let key = await keystore.getKeyByPath(identity.id, BoxKeyWithMeta); // TODO add key rotation, potentially generate new key every call
            if (!key) {
                key = await keystore.createKey(identity.id, BoxKeyWithMeta);
            }
            // TODO can secretKey be missing?
            return key.secretKey;
        }
    }
}