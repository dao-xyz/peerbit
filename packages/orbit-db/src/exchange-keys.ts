import { variant, field, serialize, deserialize, vec } from '@dao-xyz/borsh';
import { Message } from './message';
import { U8IntArraySerializer } from '@dao-xyz/io-utils';
import { EthIdentityProvider, Identity, OrbitDBIdentityProvider, SolanaIdentityProvider } from '@dao-xyz/orbit-db-identity-provider';
import { X25519PublicKey, Ed25519PublicKey, CryptographyKey } from 'sodium-plus'
import Logger from 'logplease'
import { Keystore, KeyWithMeta } from '@dao-xyz/orbit-db-keystore';
import { SignedMessage, X25519PublicKeySerializer, CryptographyKeySerializer, MaybeEncrypted, UnsignedMessage, PublicKeyEncryption } from '@dao-xyz/encryption-utils';
import { DecryptedThing, EncryptedThing } from '@dao-xyz/encryption-utils';
import { TimeoutError, waitForAsync } from '@dao-xyz/time';

const logger = Logger.create('exchange-heads', { color: Logger.Colors.Yellow })

Logger.setLogLevel('ERROR')
export type KeyAccessCondition = (requester: Ed25519PublicKey, keyToAccess: KeyWithMeta) => Promise<boolean>;
export type KeyType = 'ethereum' | 'solana' | 'orbitdb';



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

    @field({ type: vec(KeyWithMeta) })
    keys: KeyWithMeta[]

    constructor(props?: {
        keys: KeyWithMeta[]
    }) {
        super();
        if (props) {
            this.keys = props.keys;
        }
    }
}

@variant([1, 1])
export class RequestKeyMessage extends Message {

    @field(X25519PublicKeySerializer)
    encryptionKey: X25519PublicKey
}

@variant(0)
export class RequestKeysInReplicationTopicMessage extends RequestKeyMessage {

    @field({ type: 'String' })
    replicationTopic: string

    constructor(props?: {
        replicationTopic: string,
        encryptionKey: X25519PublicKey
    }) {
        super();
        if (props) {
            this.replicationTopic = props.replicationTopic
            this.encryptionKey = props.encryptionKey

        }
    }
}

@variant(1)
export class RequestSingleKeyMessage extends RequestKeyMessage {

    @field(X25519PublicKeySerializer)
    key: X25519PublicKey

    constructor(props?: {
        key: X25519PublicKey,
        encryptionKey: X25519PublicKey
    }) {
        super();
        if (props) {
            this.key = props.key
            this.encryptionKey = props.encryptionKey
        }
    }
}




export const requestAndWaitForKeys = async (send: (message: Uint8Array) => void | Promise<void>, keystore: Keystore, myIdentity: Identity, request: { group: string } | { key: X25519PublicKey }): Promise<KeyWithMeta[]> => {
    await requestKeys(send, keystore, myIdentity, request);
    const group = request["group"];
    if (group) {
        try {
            // timeout
            return await waitForAsync(async () => {
                const keys = await keystore.getKeys(group)
                if (keys.length > 0) {
                    return keys;
                }
                return undefined
            }, {
                timeout: 10000,
                delayInterval: 1000
            });
        } catch (error) {
            if (error instanceof TimeoutError) {
                return;
            }
            throw error;
        }

    }
    else {
        const key: Ed25519PublicKey = request['key'];
        const keyId = new Uint8Array(key.getBuffer());
        try {
            return [await waitForAsync(() => keystore.getKeyById(keyId), {
                timeout: 10000,
                delayInterval: 1000
            })]

        } catch (error) {
            if (error instanceof TimeoutError) {
                return;
            }
            throw error;
        }
    }
}

export const requestKeys = async (send: (message: Uint8Array) => void | Promise<void>, keystore: Keystore, myIdentity: Identity, request: { group: string } | { key: X25519PublicKey }) => {
    const signKey = await keystore.getKeyByPath(myIdentity.id, 'sign'); // should exist
    let key = await keystore.getKeyByPath(myIdentity.id, 'box');
    if (!key) {
        key = await keystore.createKey(myIdentity.id, 'box');
    }
    const encryptionKey = await Keystore.getPublicBox((await keystore.getKeyByPath(myIdentity.id, 'box')).key);
    const requestMessage = request["group"] ? new RequestKeysInReplicationTopicMessage({ replicationTopic: request["group"], encryptionKey }) : new RequestSingleKeyMessage({ key: request["key"], encryptionKey })

    const signedMessage = await new UnsignedMessage<RequestKeysInReplicationTopicMessage>({
        data: serialize(requestMessage)
    }).sign(async (bytes) => {
        return {
            signature: await keystore.sign(bytes, signKey.key),
            publicKey: await Keystore.getPublicSign(signKey.key)
        }
    })
    const unencryptedMessage = new DecryptedThing(
        {
            data: serialize(signedMessage)
        }
    );
    await send(serialize(unencryptedMessage))
}

export const exchangeKeys = async (channel: any, request: RequestKeyMessage, requester: Ed25519PublicKey, canAccessKey: KeyAccessCondition, getKeyByPublicKey: (key: X25519PublicKey) => Promise<KeyWithMeta>, getKeysByGroup: (group: string) => Promise<KeyWithMeta[]>, sign: (bytes: Uint8Array) => Promise<{ signature: Uint8Array, publicKey: Ed25519PublicKey }>, encryption: PublicKeyEncryption) => { //  encrypt: (data: Uint8Array, recieverPublicKey: X25519PublicKey) => Promise<{ publicKey: X25519PublicKey, bytes: Uint8Array }>

    // Validate signature
    let secretKeys: KeyWithMeta[] = []
    let group: string = undefined;
    if (request instanceof RequestKeysInReplicationTopicMessage) {
        try {
            group = await request.replicationTopic
        } catch (error) {
            // Invalid signature 
            logger.info("Invalid signature found from key request")
            return;
        }

        secretKeys = await getKeysByGroup(group);
    }
    else if (request instanceof RequestSingleKeyMessage) {
        const key = await getKeyByPublicKey(request.key)
        if (key) {
            group = key.group
        }
        secretKeys = key ? [key] : []
    }

    secretKeys = (await Promise.all(secretKeys.map(async (key) => {
        return (await canAccessKey(requester, key)) ? key : undefined
    }))).filter(x => !!x);

    if (secretKeys.length === 0) {
        return
    }

    const secretKeyResponseMessage = serialize(new KeyResponseMessage({
        keys: secretKeys
    }));

    const signatureResult = await sign(secretKeyResponseMessage);
    await channel.send(serialize(await new DecryptedThing<KeyResponseMessage>({
        data: serialize(new SignedMessage({
            signature: signatureResult.signature,
            key: signatureResult.publicKey
        }))
    }).init(encryption).encrypt(request.encryptionKey)));

}

export const recieveKeys = async (msg: KeyResponseMessage, setKeys: (keys: KeyWithMeta[]) => Promise<any[]>) => { // 
    await setKeys(msg.keys);
}



/* export const verifySignature = (signature: Uint8Array, data: Uint8Array, type: KeyType, publicKey: Uint8Array): Promise<boolean> => {
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
} */
