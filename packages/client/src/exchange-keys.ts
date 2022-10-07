import { variant, field, serialize, vec } from '@dao-xyz/borsh';
import { ProtocolMessage } from './message.js';
import { bufferSerializer, U8IntArraySerializer } from '@dao-xyz/borsh-utils';
import { Ed25519Keypair, PublicKeyEncryptionResolver, X25519Keypair, X25519PublicKey } from '@dao-xyz/peerbit-crypto'
import { Keystore, KeyWithMeta } from '@dao-xyz/orbit-db-keystore';
import { PublicKeyEncryption } from "@dao-xyz/peerbit-crypto";
import { MaybeSigned, SignatureWithKey } from '@dao-xyz/peerbit-crypto';

import { DecryptedThing } from "@dao-xyz/peerbit-crypto";
import { TimeoutError, waitForAsync } from '@dao-xyz/time';

import { PublicSignKey } from '@dao-xyz/peerbit-crypto';

// @ts-ignore
import Logger from 'logplease'
import { Constructor } from '@dao-xyz/orbit-db-store';
import { Identity } from '@dao-xyz/ipfs-log';
const logger = Logger.create('exchange-heads', { color: Logger.Colors.Yellow })
Logger.setLogLevel('ERROR')

export type KeyAccessCondition = (requester: PublicSignKey, keyToAccess: KeyWithMeta<Ed25519Keypair | X25519Keypair>) => Promise<boolean>;
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

@variant(0)
export class RequestKeyCondition<T extends Ed25519Keypair | X25519Keypair> {

    @field({ type: 'u8' })
    _type: number;

    constructor(props?: { type: Constructor<T> }) {
        if (props) {
            if (props.type === Ed25519Keypair as any) { // TODO fix types
                this._type = 0;
            }
            else if (props.type === X25519Keypair as any) { // TODO fix types
                this._type = 1
            }
        }
    }

    get type(): Constructor<T> {
        if (this._type === 0) {
            return KeyWithMeta<Ed25519Keypair> as any as Constructor<T>
        }
        else if (this._type === 1) {
            return KeyWithMeta<X25519Keypair> as any as Constructor<T>
        }
        else {
            throw new Error("Unsupported")
        }
    }

    get hashcode(): string {
        throw new Error("Unsupported")
    }

}

@variant(0)
export class RequestKeysByReplicationTopic<T extends (Ed25519Keypair | X25519Keypair)> extends RequestKeyCondition<T> {

    @field({ type: 'string' })
    replicationTopic: string;

    constructor(props?: {
        type: Constructor<T>,
        replicationTopic: string
    }) {
        super({ type: props?.type as Constructor<T> });
        if (props) {
            this.replicationTopic = props.replicationTopic;
        }
    }

    get hashcode() {
        return this._type + this.replicationTopic
    }

}

@variant(1)
export class RequestKeysByKey<T extends (Ed25519Keypair | X25519Keypair)> extends RequestKeyCondition<T> {

    @field(U8IntArraySerializer)
    key: Uint8Array;

    constructor(props?: {
        type: Constructor<T>,
        key: Uint8Array
    }) {
        super({ type: props?.type as Constructor<T> });
        if (props) {
            this.key = props.key;
        }
    }

    get hashcode() {
        return this._type + Buffer.from(this.key).toString('base64');
    }

}

@variant([1, 0])
export class RequestKeyMessage<T extends (Ed25519Keypair | X25519Keypair)> extends ProtocolMessage {

    @field({ type: X25519PublicKey })
    encryptionKey: X25519PublicKey

    @field({ type: RequestKeyCondition })
    condition: RequestKeyCondition<T>



    // TODO peer info for sending repsonse directly

    constructor(props?: { encryptionKey: X25519PublicKey, condition: RequestKeyCondition<T> }) {
        super();
        if (props) {
            this.encryptionKey = props.encryptionKey;
            this.condition = props.condition;
        }
    }
}


@variant([1, 1])
export class KeyResponseMessage extends ProtocolMessage {

    @field({ type: vec(KeyWithMeta) })
    keys: KeyWithMeta<Ed25519Keypair | X25519Keypair>[]

    constructor(props?: {
        keys: KeyWithMeta<Ed25519Keypair | X25519Keypair>[]
    }) {
        super();
        if (props) {
            this.keys = props.keys;
        }
    }
}

export const requestAndWaitForKeys = async<T extends (Ed25519Keypair | X25519Keypair)>(condition: RequestKeyCondition<T>, send: (message: Uint8Array) => void | Promise<void>, keystore: Keystore, identity: Identity, timeout = 10000): Promise<KeyWithMeta<T>[] | undefined> => {
    await requestKeys(condition, send, keystore, identity);
    if (condition instanceof RequestKeysByReplicationTopic) {
        try {
            // timeout
            return await waitForAsync(async () => {
                const keys = await keystore.getKeys<T>(condition.replicationTopic)
                if (keys && keys.length > 0) {
                    return keys;
                }
                return undefined
            }, {
                timeout,
                delayInterval: 50
            });
        } catch (error) {
            if (error instanceof TimeoutError) {
                return;
            }
            throw error;
        }

    }
    else if (condition instanceof RequestKeysByKey) {
        try {
            const key = await waitForAsync(() => keystore.getKey<T>(condition.key), {
                timeout,
                delayInterval: 50
            });
            return key ? [key] : undefined;

        } catch (error) {
            if (error instanceof TimeoutError) {
                return;
            }
            throw error;
        }
    }
}



export const requestKeys = async <T extends (X25519Keypair | Ed25519Keypair)>(condition: RequestKeyCondition<T>, send: (message: Uint8Array) => void | Promise<void>, keystore: Keystore, identity: Identity) => {

    // TODO key rotation?
    let key = await keystore.getKey(identity.publicKey);
    if (!key) {
        key = await keystore.createKey(await Ed25519Keypair.create(), { id: identity.publicKey }); // TODO what if id is .hashcode? 
    }

    if (key.keypair instanceof Ed25519Keypair === false && key.keypair instanceof X25519Keypair === false) {
        logger.error("Invalid key type for identity, got: " + key.keypair.constructor.name)
        return;
    }
    const signedMessage = await new MaybeSigned<RequestKeyMessage<T>>({
        data: serialize(new RequestKeyMessage<T>({
            condition,
            encryptionKey: (key.keypair as (Ed25519Keypair | X25519Keypair)).publicKey
        }))
    }).sign(async (bytes) => {
        return {
            signature: await identity.sign(bytes),
            publicKey: identity.publicKey
        }
    })
    const unencryptedMessage = new DecryptedThing(
        {
            data: serialize(signedMessage)
        }
    );
    await send(serialize(unencryptedMessage))
}

export const exchangeKeys = async <T extends Ed25519Keypair | X25519Keypair>(
    send: (data: Uint8Array) => Promise<void>,
    request: RequestKeyMessage<T>,
    requester: PublicSignKey,
    canAccessKey: KeyAccessCondition,
    getKeyByPublicKey: (key: Uint8Array) => Promise<KeyWithMeta<T> | undefined>,
    getKeysByGroup: (group: string, type: Constructor<T>) => Promise<KeyWithMeta<T>[] | undefined>,
    identity: Identity,
    encryption: PublicKeyEncryptionResolver) => { //  encrypt: (data: Uint8Array, recieverPublicKey: X25519PublicKey) => Promise<{ publicKey: X25519PublicKey, bytes: Uint8Array }>

    // Validate signature
    let secretKeys: KeyWithMeta<T>[] = []
    let group: string;
    if (request.condition instanceof RequestKeysByReplicationTopic) {
        const keys = await getKeysByGroup(request.condition.replicationTopic, request.condition.type);
        if (!keys) {
            return;
        }
        secretKeys = keys;
    }
    else if (request.condition instanceof RequestKeysByKey) {
        const key = await getKeyByPublicKey(request.condition.key)
        if (key) {
            group = key.group
        }
        secretKeys = key ? [key] : []
    }
    else {
        throw new Error("Unsupported")
    }

    const mappedKeys = (await Promise.all(secretKeys.map(async (key) => {
        return (await canAccessKey(requester, key)) ? key : key.clone(false)
    }))).filter(x => !!x);

    if (mappedKeys.length === 0) {
        return
    }

    const secretKeyResponseMessage = serialize(new KeyResponseMessage({
        keys: mappedKeys
    }));

    const signatureResult = await identity.sign(secretKeyResponseMessage);
    await send(serialize(await new DecryptedThing<KeyResponseMessage>({
        data: serialize(new MaybeSigned({
            signature: new SignatureWithKey({
                signature: signatureResult,
                publicKey: identity.publicKey
            }),
            data: secretKeyResponseMessage
        }))
    }).init(encryption).encrypt(request.encryptionKey)));

}

export const recieveKeys = async (msg: KeyResponseMessage, setKeys: (keys: KeyWithMeta<any>[]) => Promise<any[]>) => { // 
    await setKeys(msg.keys);
}
