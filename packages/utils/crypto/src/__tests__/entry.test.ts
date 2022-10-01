
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
        const encSender: PublicKeyEncryption = {
            getEncryptionKey: () => Promise.resolve(senderKey),
            getAnySecret: async (publicKeys: X25519PublicKey[]) => {
                for (let i = 0; i < publicKeys.length; i++) {
                    if (publicKeys[i].equals(await senderKey.publicKey())) {
                        return {
                            index: i,
                            secretKey: senderKey
                        }
                    }
                    if (publicKeys[i].equals(await recieverKey1.publicKey())) {
                        return {
                            index: i,
                            secretKey: recieverKey1
                        }
                    }

                    if (publicKeys[i].equals(await recieverKey2.publicKey())) {
                        return {
                            index: i,
                            secretKey: recieverKey2
                        }
                    }

                }
            }
        };



        const encrypted = await decrypted.init(enc).encrypt(await recieverKey1.publicKey(), await recieverKey2.publicKey())
        encrypted._decrypted = undefined;
        const decryptedFromEncrypted = await encrypted.init(enc).decrypt();
        expect(decryptedFromEncrypted._data).toStrictEqual(data)
    })
});

