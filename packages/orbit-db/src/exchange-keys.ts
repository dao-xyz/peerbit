import { variant, field, serialize, deserialize, vec } from '@dao-xyz/borsh';
import { Message } from './message';
import { U8IntArraySerializer } from '@dao-xyz/io-utils';
import { EthIdentityProvider, OrbitDBIdentityProvider, SolanaIdentityProvider } from '@dao-xyz/orbit-db-identity-provider';
import { X25519PublicKey, Ed25519PublicKey, CryptographyKey } from 'sodium-plus'
import Logger from 'logplease'
const logger = Logger.create('exchange-heads', { color: Logger.Colors.Yellow })
Logger.setLogLevel('ERROR')
export type KeyAccessCondition = (fromKey: {
    type: string,
    key: Uint8Array,
}, keyToAccess: {
    group: string
}) => Promise<boolean>;
export type KeyType = 'ethereum' | 'solana' | 'orbitdb';

@variant(0)
export class OrbitDBSignedMessage {

    @field(U8IntArraySerializer)
    signature: Uint8Array

    @field(U8IntArraySerializer)
    key: Uint8Array

    constructor(props?: {
        signature?: Uint8Array,
        key: Uint8Array
    }) {
        if (props) {
            this.signature = props.signature;
            this.key = props.key;
        }
    }

    async sign(data: Uint8Array, signer: (bytes: Uint8Array) => Promise<Uint8Array>): Promise<OrbitDBSignedMessage> {
        this.signature = await signer(data)
        return this;
    }


}


export class SignedX25519PublicKey {

    @field(U8IntArraySerializer)
    signature: Uint8Array

    @field(U8IntArraySerializer)
    publicKey: Uint8Array // Ed25519PublicKey 

    constructor(props?: {
        signature: Uint8Array,
        publicKey: Uint8Array
    }) {
        if (props) {
            this.signature = props.signature;
            this.publicKey = props.publicKey
        }
    }
}


export class PublicKeyMessage {

    @field(U8IntArraySerializer)
    message: Uint8Array

    @field(U8IntArraySerializer)
    key: Uint8Array

    constructor(props?: {
        message: Uint8Array,
        key: Uint8Array
    }) {
        if (props) {
            this.message = props.message;
            this.key = props.key;
        }
    }
}


@variant([1, 0])
export class KeyResponseMessage extends Message {

    // TODO nonce?

    @field(U8IntArraySerializer)
    keysEncryptedAndSigned: Uint8Array;

    @field(U8IntArraySerializer)
    encryptionPublicKey: Uint8Array;

    @field(U8IntArraySerializer)
    signerPublicKey: Uint8Array

    constructor(props?: {
        keysEncryptedAndSigned: Uint8Array
        signerPublicKey: Uint8Array;
        encryptionPublicKey: Uint8Array;
    }) {
        super();
        if (props) {
            this.keysEncryptedAndSigned = props.keysEncryptedAndSigned;
            this.encryptionPublicKey = props.encryptionPublicKey;
            this.signerPublicKey = props.signerPublicKey;
        }
    }
}


@variant([1, 1])
export class RequestKeysInGroupMessage extends Message {

    @field({ type: OrbitDBSignedMessage })
    signedKey: OrbitDBSignedMessage

    @field(U8IntArraySerializer)
    encryptionPublicKey: Uint8Array

    constructor(props?: {
        signedKey: OrbitDBSignedMessage;
        encryptionPublicKey: Uint8Array
    }) {
        super();
        if (props) {
            this.signedKey = props.signedKey
            this.encryptionPublicKey = props.encryptionPublicKey;

        }
    }
    async getGroup(open: (data: Uint8Array, publicKey: Ed25519PublicKey) => Promise<Uint8Array>): Promise<string> {
        return Buffer.from(await open(this.signedKey.signature, new Ed25519PublicKey(Buffer.from(this.signedKey.key)))).toString();
    }


}


export class Key {
    key: Uint8Array;

    constructor(props?: {
        key: Uint8Array
    }) {
        if (props) {
            this.key = props.key;
        }
    }
}

export class Keys {

    @field({ type: 'String' })
    group: string;

    @field({ type: vec(Key) })
    keys: Key[];

    constructor(props?: {
        group: string
        keys: Key[]
    }) {
        if (props) {
            this.group = props.group;
            this.keys = props.keys;
        }
    }
}
export const requestKeys = async (channel: any, request: RequestKeysInGroupMessage, canAccessKeys: KeyAccessCondition, getSymmetricKeys: (group: string) => Promise<CryptographyKey[]>, sign: (bytes: Uint8Array) => Promise<{ bytes: Uint8Array, publicKey: Ed25519PublicKey }>, open: (data: Uint8Array, signerPublicKey: Ed25519PublicKey) => Promise<Buffer>, encrypt: (data: Uint8Array, recieverPublicKey: X25519PublicKey) => Promise<{ publicKey: X25519PublicKey, bytes: Uint8Array }>) => { // 

    // Validate signature
    let group: string = undefined;
    try {
        group = await request.getGroup(open);
    } catch (error) {
        // Invalid signature 
        logger.info("Invalid signature found from key request")
        return;
    }

    if (!await canAccessKeys({ type: 'orbitdb', key: request.signedKey.key }, {
        group
    })) {
        return; // Do not send any keys
    }
    const secretKeys = await await getSymmetricKeys(group);
    const secretKeysBytes = serialize(new Keys({
        group,
        keys: secretKeys.map(x => new Key({
            key: new Uint8Array(x.getBuffer())
        }))
    }));
    const signatureResult = await sign(secretKeysBytes);
    const encryptionResult = await encrypt(signatureResult.bytes, new X25519PublicKey(Buffer.from(request.encryptionPublicKey)));
    const message = serialize(new KeyResponseMessage({ encryptionPublicKey: new Uint8Array(encryptionResult.publicKey.getBuffer()), keysEncryptedAndSigned: encryptionResult.bytes, signerPublicKey: new Uint8Array(signatureResult.publicKey.getBuffer()) }));
    await channel.send(message)
}

export const recieveKeys = async (response: KeyResponseMessage, isTrusted: (key: Ed25519PublicKey) => Promise<boolean>, setSymmetricKeys: (group: string, keys: CryptographyKey[]) => Promise<any[]>, open: (data: Uint8Array, signerPublicKey: Ed25519PublicKey) => Promise<Buffer>, decrypt: (data: Uint8Array, senderPublicKey: X25519PublicKey) => Promise<Uint8Array>) => { // 
    // Verify signer is trusted by opening the message and checking the public key
    const signer = new Ed25519PublicKey(Buffer.from(response.signerPublicKey));
    const trusted = await isTrusted(signer);
    if (!trusted) {
        throw new Error("Recieved keys from a not trusted party");
    }
    const decrypted = await decrypt(response.keysEncryptedAndSigned, new X25519PublicKey(Buffer.from(response.encryptionPublicKey)))
    const keys = deserialize(await open(decrypted, signer), Keys);
    await setSymmetricKeys(keys.group, keys.keys.map(x => new CryptographyKey(Buffer.from(x.key))))
}



export const verifySignature = (signature: Uint8Array, data: Uint8Array, type: KeyType, publicKey: Uint8Array): Promise<boolean> => {
    if (type === 'ethereum') {
        return EthIdentityProvider.verify(signature, data, publicKey)
    }
    if (type === 'solana') {
        return SolanaIdentityProvider.verify(signature, data, publicKey)
    }
    if (type === 'orbitdb') {
        return OrbitDBIdentityProvider.verify(signature, data, publicKey)
    }
    throw new Error("Unsupported keytype")
}
