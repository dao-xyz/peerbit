import { Keystore } from '@dao-xyz/orbit-db-keystore';
import { StoreCryptOptions } from '@dao-xyz/orbit-db-store';
import { X25519PublicKey } from 'sodium-plus';

export const replicationTopicAsKeyGroupCryptOptions = (keystore: Keystore): StoreCryptOptions => {
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
        decrypt: async (data: Uint8Array, senderPublicKey: X25519PublicKey, recieverPublicKey: X25519PublicKey, replicationTopic) => {
            const keyGroup = replicationTopic;
            const key = await keystore.getKey(recieverPublicKey.getBuffer(), 'box', keyGroup) || await keystore.getKey(recieverPublicKey.getBuffer(), 'box')
            return keystore.decrypt(data, key.key, senderPublicKey)
        }
    }
}