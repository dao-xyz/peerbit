import { variant, field, serialize, deserialize } from '@dao-xyz/borsh';
import { Message } from './message';
import { joinUint8Arrays, U8IntArraySerializer } from '@dao-xyz/io-utils';
import { EthIdentityProvider, OrbitDBIdentityProvider, SolanaIdentityProvider } from '@dao-xyz/orbit-db-identity-provider';
import { X25519PublicKey, Ed25519PublicKey, CryptographyKey } from 'sodium-plus'
export type KeyAccessCondition = (fromKey: {
    type: string,
    key: Uint8Array,
    group: string
}, keyToAccess: { key: Uint8Array }) => Promise<boolean>;
export type KeyType = 'ethereum' | 'solana' | 'orbitdb';

export class SignedMessage {

    @field(U8IntArraySerializer)
    nonce: Uint8Array

    @field(U8IntArraySerializer)
    key: Uint8Array

    @field(U8IntArraySerializer)
    signature: Uint8Array

    @field({ type: 'String' })
    type: KeyType

    constructor(props?: {
        nonce: Uint8Array,
        signature: Uint8Array,
        key: Uint8Array,
        type: KeyType
    }) {
        if (props) {
            this.nonce = props.nonce;
            this.signature = props.signature;
            this.key = props.key;
            this.type = props.type
        }
    }

    get dataToSign() {
        return joinUint8Arrays([new Uint8Array(Buffer.from(this.type)), this.key, this.nonce]);
    }

    async verify(): Promise<boolean> {
        return await verifySignature(this.signature, this.dataToSign, this.type, this.key)
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
export class ExchangeKeysMessage extends Message {

    // TODO nonce?

    @field(U8IntArraySerializer)
    keyEncrypted: Uint8Array;

    @field(U8IntArraySerializer)
    encryptionPublicKey: Uint8Array;


    constructor(props?: {
        keyEncrypted: Uint8Array;
        encryptionPublicKey: Uint8Array
    }) {
        super();
        if (props) {
            this.keyEncrypted = props.keyEncrypted;
            this.encryptionPublicKey = props.encryptionPublicKey;
        }
    }
}


@variant([1, 1])
export class RequestAllKeysMessage extends Message {

    @field({ type: SignedMessage })
    signedKey: SignedMessage

    @field(U8IntArraySerializer)
    encryptionPublicKey: Uint8Array

    constructor(props?: {
        signedKey: SignedMessage;
    }) {
        super();
        if (props) {
            this.signedKey = props.signedKey
        }
    }


}


export const exchangeKeys = async (channel: any, request: RequestAllKeysMessage, canAccessKeys: KeyAccessCondition, getSymmetricKey: (key: Uint8Array) => Promise<CryptographyKey>, encrypt: (data: Uint8Array, recieverPublicKey: X25519PublicKey) => Promise<{ publicKey: X25519PublicKey, bytes: Uint8Array }>) => { // 

    // Validate signature
    const verified = await request.signedKey.verify();
    if (!verified) {
        throw new Error("Invalid signature")
    }

    if (!await canAccessKeys({ type: request.signedKey.type, key: request.signedKey.key }, {
        key: request.requestedKey
    })) {
        throw new Error("Not allowed to access key")
    }

    const secretKey = await getSymmetricKey(request.requestedKey);

    if (!secretKey) {
        throw new Error("Requested key does not exist")
    }

    if (secretKey.isPublicKey()) {
        throw new Error("Lookup did not resolve a secret key")
    }

    const encryptedSecretKey = await encrypt(new Uint8Array(secretKey.getBuffer()), new X25519PublicKey(Buffer.from(request.encryptionPublicKey)));
    const message = serialize(new ExchangeKeysMessage({ encryptionPublicKey: new Uint8Array(encryptedSecretKey.publicKey.getBuffer()), keyEncrypted: encryptedSecretKey.bytes }));
    await channel.send(message)
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
