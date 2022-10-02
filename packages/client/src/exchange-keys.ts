import { variant, field, serialize, vec } from '@dao-xyz/borsh';
import { Message } from './message.js';
import { bufferSerializer, U8IntArraySerializer } from '@dao-xyz/borsh-utils';
import { X25519PublicKey } from 'sodium-plus'
import Logger from 'logplease'
import { BoxKeyWithMeta, Keystore, KeyWithMeta, SignKeyWithMeta, WithType } from '@dao-xyz/orbit-db-keystore';
import { PublicKeyEncryption } from '@dao-xyz/encryption-utils';
import { MaybeSigned, SignatureWithKey } from '@dao-xyz/identity';

import { DecryptedThing } from '@dao-xyz/encryption-utils';
import { TimeoutError, waitForAsync } from '@dao-xyz/time';
import { PublicKey } from '@dao-xyz/identity';

const logger = Logger.create('exchange-heads', { color: Logger.Colors.Yellow })

Logger.setLogLevel('ERROR')
export type KeyAccessCondition = (requester: PublicKey, keyToAccess: KeyWithMeta) => Promise<boolean>;
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
export class RequestKeyCondition<T extends KeyWithMeta> {

    @field({ type: 'u8' })
    _type: number;

    constructor(props?: { type: WithType<T> }) {
        if (props) {
            if (props.type === SignKeyWithMeta as any) { // TODO fix types
                this._type = 0;
            }
            else if (props.type === BoxKeyWithMeta as any) { // TODO fix types
                this._type = 1
            }
        }
    }

    get type(): WithType<T> {
        if (this._type === 0) {
            return SignKeyWithMeta as any as WithType<T>
        }
        else if (this._type === 1) {
            return BoxKeyWithMeta as any as WithType<T>
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
export class RequestKeysByReplicationTopic<T extends KeyWithMeta> extends RequestKeyCondition<T> {

    @field({ type: 'string' })
    replicationTopic: string;

    constructor(props?: {
        type: WithType<T>,
        replicationTopic: string
    }) {
        super({ type: props?.type });
        if (props) {
            this.replicationTopic = props.replicationTopic;
        }
    }

    get hashcode() {
        return this._type + this.replicationTopic
    }

}

@variant(1)
export class RequestKeysByKey<T extends KeyWithMeta> extends RequestKeyCondition<T> {

    @field(U8IntArraySerializer)
    key: Uint8Array;

    constructor(props?: {
        type: WithType<T>,
        key: Uint8Array
    }) {
        super({ type: props?.type });
        if (props) {
            this.key = props.key;
        }
    }

    get hashcode() {
        return this._type + Buffer.from(this.key).toString('base64');
    }

}

@variant([1, 0])
export class RequestKeyMessage<T extends KeyWithMeta> extends Message {

    @field(bufferSerializer(X25519PublicKey))
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

export const requestAndWaitForKeys = async<T extends KeyWithMeta>(condition: RequestKeyCondition<T>, send: (message: Uint8Array) => void | Promise<void>, keystore: Keystore, signPublicKey: PublicKey, sign: (data: Uint8Array) => Promise<Uint8Array>, timeout = 10000): Promise<T[]> => {
    await requestKeys(condition, send, keystore, signPublicKey, sign);
    if (condition instanceof RequestKeysByReplicationTopic) {
        try {
            // timeout
            return await waitForAsync(async () => {
                const keys = await keystore.getKeys<T>(condition.replicationTopic)
                if (keys.length > 0) {
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
            return [await waitForAsync(() => keystore.getKeyById(condition.key), {
                timeout,
                delayInterval: 50
            })]

        } catch (error) {
            if (error instanceof TimeoutError) {
                return;
            }
            throw error;
        }
    }
}



export const requestKeys = async <T extends KeyWithMeta>(condition: RequestKeyCondition<T>, send: (message: Uint8Array) => void | Promise<void>, keystore: Keystore, signPublicKey: PublicKey, sign: (data: Uint8Array) => Promise<Uint8Array>) => {

    // TODO key rotation?
    const keyId = serialize(signPublicKey);
    let key = await keystore.getKeyByPath(keyId, BoxKeyWithMeta);
    if (!key) {
        key = await keystore.createKey(keyId, BoxKeyWithMeta);
    }

    const encryptionKey = key.publicKey;
    const signedMessage = await new MaybeSigned<RequestKeyMessage<T>>({
        data: serialize(new RequestKeyMessage<T>({
            condition,
            encryptionKey
        }))
    }).sign(async (bytes) => {
        return {
            signature: await sign(bytes),
            publicKey: await signPublicKey
        }
    })
    const unencryptedMessage = new DecryptedThing(
        {
            data: serialize(signedMessage)
        }
    );
    await send(serialize(unencryptedMessage))
}

export const exchangeKeys = async <T extends KeyWithMeta>(send: (Uint8Array) => Promise<void>, request: RequestKeyMessage<T>, requester: PublicKey, canAccessKey: KeyAccessCondition, getKeyByPublicKey: (key: Uint8Array) => Promise<T>, getKeysByGroup: (group: string, type: WithType<T>) => Promise<T[]>, sign: (bytes: Uint8Array) => Promise<{ signature: Uint8Array, publicKey: PublicKey }>, encryption: PublicKeyEncryption) => { //  encrypt: (data: Uint8Array, recieverPublicKey: X25519PublicKey) => Promise<{ publicKey: X25519PublicKey, bytes: Uint8Array }>

    // Validate signature
    let secretKeys: KeyWithMeta[] = []
    let group: string = undefined;
    if (request.condition instanceof RequestKeysByReplicationTopic) {
        secretKeys = await getKeysByGroup(request.condition.replicationTopic, request.condition.type);
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

    const signatureResult = await sign(secretKeyResponseMessage);
    await send(serialize(await new DecryptedThing<KeyResponseMessage>({
        data: serialize(new MaybeSigned({
            signature: new SignatureWithKey({
                signature: signatureResult.signature,
                publicKey: signatureResult.publicKey
            }),
            data: secretKeyResponseMessage
        }))
    }).init(encryption).encrypt(request.encryptionKey)));

}

export const recieveKeys = async (msg: KeyResponseMessage, setKeys: (keys: KeyWithMeta[]) => Promise<any[]>) => { // 
    await setKeys(msg.keys);
}
