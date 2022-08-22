import { Identity } from '@dao-xyz/orbit-db-identity-provider';
import { BoxKeyWithMeta, Keystore } from '@dao-xyz/orbit-db-keystore';
import { StorePublicKeyEncryption } from '@dao-xyz/orbit-db-store';
import { X25519PublicKey } from 'sodium-plus';

export const replicationTopicAsKeyGroupPublicKeyEncryption = (identity: Identity, keystore: Keystore, requestKey: (key: X25519PublicKey, replicationTopic: string) => Promise<BoxKeyWithMeta[] | undefined>): StorePublicKeyEncryption => {
    return (replicationTopic: string) => {
        return {
            encrypt: async (bytes: Uint8Array, reciever: X25519PublicKey) => {
                const key = await keystore.getKeyByPath(identity.id, BoxKeyWithMeta) || await keystore.createKey(identity.id, BoxKeyWithMeta)
                return {
                    data: await keystore.encrypt(bytes, key, reciever),
                    senderPublicKey: key.publicKey
                }
            },
            decrypt: async (data: Uint8Array, senderPublicKey: X25519PublicKey, recieverPublicKey: X25519PublicKey) => {
                let key = await keystore.getKeyById<BoxKeyWithMeta>(recieverPublicKey);
                if (!key) {
                    const newKeys = await requestKey(recieverPublicKey, replicationTopic);
                    if (!newKeys || newKeys.length === 0) {
                        return undefined;
                    }
                    key = newKeys[0];

                }
                if (key instanceof BoxKeyWithMeta && !key.secretKey) {
                    throw new Error("Can not open")
                }
                return keystore.decrypt(data, key, senderPublicKey)
            }
        }
    }
}
