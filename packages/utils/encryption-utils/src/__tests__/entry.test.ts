
import { SodiumPlus, X25519PublicKey } from 'sodium-plus';

import { variant, field, serialize, deserialize } from '@dao-xyz/borsh';
import { DecryptedThing } from '..';
import { U8IntArraySerializer } from '@dao-xyz/borsh-utils';
const _crypto = SodiumPlus.auto();

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

const NONCE_LENGTH = 24;
describe('thing', function () {
    it('encrypt', async () => {
        const crypto = await _crypto;
        const data = new Uint8Array([1, 2, 3]);
        const senderKey = await crypto.crypto_box_keypair()
        const recieverKey = await crypto.crypto_box_keypair()
        const decrypted = new DecryptedThing({
            data
        })
        const enc = {
            getEncryptionKey: () => crypto.crypto_box_secretkey(senderKey),
            getAnySecret: async (publicKeys: X25519PublicKey[]) => {
                for (let i = 0; i < publicKeys.length; i++) {
                    if (Buffer.compare(publicKeys[i].getBuffer(), (await crypto.crypto_box_publickey(senderKey)).getBuffer()) === 0) {
                        return {
                            index: i,
                            secretKey: await crypto.crypto_box_secretkey(senderKey)
                        }
                    }
                    if (Buffer.compare(publicKeys[i].getBuffer(), (await crypto.crypto_box_publickey(recieverKey)).getBuffer()) === 0) {
                        return {
                            index: i,
                            secretKey: await crypto.crypto_box_secretkey(recieverKey)
                        }
                    }

                }
            }

        };
        const encrypted = await decrypted.init(enc).encrypt(await crypto.crypto_box_publickey(recieverKey))
        encrypted._decrypted = undefined;
        const decryptedFromEncrypted = await encrypted.init(enc).decrypt();
        expect(decryptedFromEncrypted._data).toStrictEqual(data)
    })
});

