import { Keystore, KeyWithMeta } from '@dao-xyz/orbit-db-keystore';
import { StorePublicKeyEncryption } from '@dao-xyz/orbit-db-store';
import { X25519PublicKey } from 'sodium-plus';
import { OrbitDB } from './orbit-db';

const replicationTopicAsKeyGroupPublicKeyEncryption = (keystore: Keystore, requestKey: (key: X25519PublicKey, replicationTopic: string) => Promise<KeyWithMeta | undefined>): StorePublicKeyEncryption => {
    return {
        encrypt: async (bytes: Uint8Array, reciever: X25519PublicKey, replicationTopic: string) => {
            const keyGroup = replicationTopic; // Assumption
            const keys = await keystore.getKeys(keyGroup, 'box')
            // TODO make smarter key choice
            const key = keys?.length > 0 ? keys[0] : await keystore.createKey(undefined, 'box', keyGroup)
            return {
                data: await keystore.encrypt(bytes, key.key, reciever),
                senderPublicKey: await Keystore.getPublicBox(key.key)
            }
        },
        decrypt: async (data: Uint8Array, senderPublicKey: X25519PublicKey, recieverPublicKey: X25519PublicKey, replicationTopic: string) => {
            let key = await keystore.getKeyById(recieverPublicKey);
            if (!key) {
                key = await requestKey(recieverPublicKey, replicationTopic);
                if (!key) {
                    return undefined;
                }
                keystore.saveKey(key.key, (await Keystore.getPublicBox(key.key)).getBuffer(), 'box', key.group, key.timestamp)

            }
            return keystore.decrypt(data, key.key, senderPublicKey)
        }
    }
}

export const replicationTopicEncryption = (orbitdb: OrbitDB): StorePublicKeyEncryption => {
    return replicationTopicAsKeyGroupPublicKeyEncryption(orbitdb.keystore, (key, replicationTopic) => orbitdb.requestAndWaitForKeys(key, replicationTopic))
}