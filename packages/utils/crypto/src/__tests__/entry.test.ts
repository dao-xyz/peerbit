
import { variant, field } from '@dao-xyz/borsh';
import { DecryptedThing, X25519PublicKey, PublicKeyEncryption, X25519SecretKey } from '../index.js';
import { arraysCompare, U8IntArraySerializer } from '@dao-xyz/borsh-utils';
import sodium from 'libsodium-wrappers';

@variant(0)
export class EncryptedMessage {

    @field(U8IntArraySerializer)
    nonce: Uint8Array

    @field(U8IntArraySerializer)
    cipher: Uint8Array

    constructor(props?: EncryptedMessage) {
        if (props) {
            this.nonce = props.nonce;
            this.cipher = props.cipher;
        }
    }
}

describe('thing', function () {

    it('encrypt', async () => {
        await sodium.ready;
        const data = new Uint8Array([1, 2, 3]);
        const senderKey = new X25519SecretKey({
            secretKey: (await sodium.crypto_box_keypair()).privateKey
        })
        const recieverKey1 = new X25519SecretKey({
            secretKey: (await sodium.crypto_box_keypair()).privateKey
        })

        const recieverKey2 = new X25519SecretKey({
            secretKey: (await sodium.crypto_box_keypair()).privateKey
        })
        const decrypted = new DecryptedThing({
            data
        })
        const config = (key: X25519SecretKey) => {
            return {
                getEncryptionKey: () => Promise.resolve(key),
                getAnySecret: async (publicKeys: X25519PublicKey[]) => {
                    for (let i = 0; i < publicKeys.length; i++) {
                        if (publicKeys[i].equals(await key.publicKey())) {
                            return {
                                index: i,
                                secretKey: key
                            }
                        }
                    }
                }
            } as PublicKeyEncryption
        }
        const senderConfig = config(senderKey)
        const reciever1Config = config(recieverKey1)
        const reciever2Config = config(recieverKey2)


        const encrypted = await decrypted.init(senderConfig).encrypt(await recieverKey1.publicKey(), await recieverKey2.publicKey())
        encrypted._decrypted = undefined;

        const decryptedFromEncrypted1 = await encrypted.init(reciever1Config).decrypt();
        expect(decryptedFromEncrypted1._data).toStrictEqual(data)

        const decryptedFromEncrypted2 = await encrypted.init(reciever2Config).decrypt();
        expect(decryptedFromEncrypted2._data).toStrictEqual(data)
    })
});

