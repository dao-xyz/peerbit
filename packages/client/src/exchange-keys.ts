import { variant, field, option, serialize, vec } from "@dao-xyz/borsh";
import { ProtocolMessage } from "./message.js";
import { UInt8ArraySerializer } from "@dao-xyz/peerbit-borsh-utils";
import {
    Ed25519Keypair,
    Ed25519PublicKey,
    K,
    PublicKeyEncryptionKey,
    PublicKeyEncryptionResolver,
    toBase64,
    X25519Keypair,
    X25519PublicKey,
} from "@dao-xyz/peerbit-crypto";
import { Keystore, KeyWithMeta, StoreError } from "@dao-xyz/peerbit-keystore";
import { MaybeSigned, SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { DecryptedThing } from "@dao-xyz/peerbit-crypto";
import { TimeoutError, waitForAsync } from "@dao-xyz/peerbit-time";
import { PublicSignKey } from "@dao-xyz/peerbit-crypto";

import { Identity } from "@dao-xyz/ipfs-log";

import { logger as loggerFn } from "@dao-xyz/peerbit-logger";
const logger = loggerFn({ module: "exchange-keys" });

export type KeyAccessCondition = (
    keyToAccess: KeyWithMeta<Ed25519Keypair | X25519Keypair>
) => Promise<boolean>;

export class SignedX25519PublicKey {
    @field(UInt8ArraySerializer)
    signature: Uint8Array;

    @field(UInt8ArraySerializer)
    publicKey: Uint8Array; // Ed25519PublicKey

    constructor(props?: { signature: Uint8Array; publicKey: Uint8Array }) {
        if (props) {
            this.signature = props.signature;
            this.publicKey = props.publicKey;
        }
    }
}

export class PublicKeyMessage {
    @field(UInt8ArraySerializer)
    message: Uint8Array;

    @field(UInt8ArraySerializer)
    key: Uint8Array;

    constructor(props?: { message: Uint8Array; key: Uint8Array }) {
        if (props) {
            this.message = props.message;
            this.key = props.key;
        }
    }
}

export abstract class RequestKeyType {
    publicKey: PublicSignKey | PublicKeyEncryptionKey;
}

@variant(0)
export class RequestSignKeyType extends RequestKeyType {
    @field({ type: PublicSignKey })
    publicKey: PublicSignKey;
    constructor(properties?: { publicKey: PublicSignKey }) {
        super();
        if (properties) {
            this.publicKey = properties.publicKey;
        }
    }
}

@variant(1)
export class RequestEncrpytionKeyType extends RequestKeyType {
    @field({ type: PublicKeyEncryptionKey })
    publicKey: PublicKeyEncryptionKey;
    constructor(properties?: { publicKey: PublicKeyEncryptionKey }) {
        super();
        if (properties) {
            this.publicKey = properties.publicKey;
        }
    }
}

export abstract class RequestKeyCondition {
    get hashcode(): string {
        throw new Error("Not implemented");
    }
}

@variant(0)
export class RequestKeysByAddress extends RequestKeyCondition {
    @field({ type: "u8" })
    keyType: number; // 0 sign key, 1 encryption key
    @field({ type: "string" })
    address: string;

    constructor(props?: { type: "sign" | "encryption"; address: string }) {
        super();
        if (props) {
            if (props.type === "sign") {
                this.keyType = 0;
            } else if (props.type === "encryption") {
                this.keyType = 1;
            } else {
                throw new Error("Unexpected");
            }

            this.address = props.address;
        }
    }

    get hashcode() {
        return this.keyType + this.address;
    }
}

@variant(1)
export class RequestKeysByKey extends RequestKeyCondition {
    @field({ type: RequestKeyType })
    _key: RequestKeyType; // publci key

    constructor(props?: { key: X25519PublicKey | Ed25519PublicKey }) {
        super();
        if (props) {
            if (props.key instanceof X25519PublicKey) {
                this._key = new RequestEncrpytionKeyType({
                    publicKey: props.key,
                });
            } else if (props.key instanceof Ed25519PublicKey) {
                this._key = new RequestSignKeyType({
                    publicKey: props.key,
                });
            } else {
                throw new Error("Unexpected");
            }
        }
    }

    get key(): RequestKeyType {
        return this._key;
    }

    get hashcode() {
        return this._key.publicKey.toString();
    }
}

@variant([1, 0])
export class RequestKeyMessage extends ProtocolMessage {
    @field({ type: PublicKeyEncryptionKey })
    _encryptionKey: PublicKeyEncryptionKey;

    @field({ type: RequestKeyCondition })
    condition: RequestKeyCondition;

    // TODO peer info for sending repsonse directly

    constructor(props?: {
        encryptionKey: X25519PublicKey;
        condition: RequestKeyCondition;
    }) {
        super();
        if (props) {
            this._encryptionKey = props.encryptionKey;
            this.condition = props.condition;
        }
    }

    get encryptionKey(): X25519PublicKey {
        return this._encryptionKey as X25519PublicKey;
    }
}

@variant([1, 1])
export class KeyResponseMessage extends ProtocolMessage {
    @field({ type: vec(KeyWithMeta) })
    keys: KeyWithMeta<Ed25519Keypair | X25519Keypair>[];

    constructor(props?: {
        keys: KeyWithMeta<Ed25519Keypair | X25519Keypair>[];
    }) {
        super();
        if (props) {
            this.keys = props.keys;
        }
    }
}

export const requestAndWaitForKeys = async <
    T extends Ed25519Keypair | X25519Keypair
>(
    condition: RequestKeyCondition,
    send: (message: Uint8Array) => void | Promise<void>,
    keystore: Keystore,
    identity: Identity,
    timeout = 10000
): Promise<KeyWithMeta<T>[] | undefined> => {
    await requestKeys(condition, send, keystore, identity);
    if (condition instanceof RequestKeysByAddress) {
        try {
            // timeout
            return await waitForAsync(
                async () => {
                    const keys = await keystore.getKeys<T>(condition.address);
                    if (keys && keys.length > 0) {
                        return keys;
                    }
                    return undefined;
                },
                {
                    timeout,
                    delayInterval: 50,
                }
            );
        } catch (error) {
            if (error instanceof TimeoutError) {
                return;
            }
            throw error;
        }
    } else if (condition instanceof RequestKeysByKey) {
        try {
            const key = await waitForAsync(
                () => keystore.getKey<T>(condition.key.publicKey),
                {
                    timeout,
                    delayInterval: 50,
                }
            );
            return key ? [key] : undefined;
        } catch (error) {
            if (error instanceof TimeoutError) {
                return;
            }
            if (
                error instanceof StoreError &&
                (keystore._store.status === "closed" ||
                    keystore._store.status === "closing")
            ) {
                return;
            }
            throw error;
        }
    }
};

export const requestKeys = async (
    condition: RequestKeyCondition,
    send: (message: Uint8Array) => void | Promise<void>,
    keystore: Keystore,
    identity: Identity
) => {
    // TODO key rotation?
    let key = await keystore.getKey(identity.publicKey);
    if (!key) {
        key = await keystore.createKey(await Ed25519Keypair.create(), {
            id: identity.publicKey,
        }); // TODO what if id is .hashcode?
    }

    if (
        key.keypair instanceof Ed25519Keypair === false &&
        key.keypair instanceof X25519Keypair === false
    ) {
        logger.error(
            "Invalid key type for identity, got: " +
                key.keypair.constructor.name
        );
        return;
    }
    const pk = (key.keypair as Ed25519Keypair | X25519Keypair).publicKey;
    const signedMessage = await new MaybeSigned<RequestKeyMessage>({
        data: serialize(
            new RequestKeyMessage({
                condition,
                encryptionKey:
                    pk instanceof Ed25519PublicKey
                        ? await X25519PublicKey.from(pk)
                        : pk,
            })
        ),
    }).sign(async (bytes) => {
        return {
            signature: await identity.sign(bytes),
            publicKey: identity.publicKey,
        };
    });
    const unencryptedMessage = new DecryptedThing({
        data: serialize(signedMessage),
    });
    await send(serialize(unencryptedMessage));
};

export const exchangeKeys = async <T extends Ed25519Keypair | X25519Keypair>(
    send: (data: Uint8Array) => Promise<void>,
    request: RequestKeyMessage,
    canAccessKey: KeyAccessCondition,
    keystore: Keystore,
    identity: Identity,
    encryption: PublicKeyEncryptionResolver
) => {
    //  encrypt: (data: Uint8Array, recieverPublicKey: X25519PublicKey) => Promise<{ publicKey: X25519PublicKey, bytes: Uint8Array }>

    // Validate signature
    let secretKeys: KeyWithMeta<T>[] = [];
    let group: string;
    if (request.condition instanceof RequestKeysByAddress) {
        const keys = await keystore.getKeys<T>(request.condition.address);
        if (!keys) {
            return;
        }
        secretKeys = keys;
    } else if (request.condition instanceof RequestKeysByKey) {
        const key = await keystore.getKey<T>(request.condition.key.publicKey);
        if (key) {
            group = key.group;
        }
        secretKeys = key ? [key] : [];
    } else {
        throw new Error("Unsupported");
    }

    const mappedKeys = (
        await Promise.all(
            secretKeys.map(async (key) => {
                return (await canAccessKey(key)) ? key : undefined;
            })
        )
    ).filter((x) => !!x) as KeyWithMeta<T>[];

    if (mappedKeys.length === 0) {
        return;
    }

    const secretKeyResponseMessage = serialize(
        new KeyResponseMessage({
            keys: mappedKeys,
        })
    );

    const signatureResult = await identity.sign(secretKeyResponseMessage);
    await send(
        serialize(
            await new DecryptedThing<KeyResponseMessage>({
                data: serialize(
                    new MaybeSigned({
                        signature: new SignatureWithKey({
                            signature: signatureResult,
                            publicKey: identity.publicKey,
                        }),
                        data: secretKeyResponseMessage,
                    })
                ),
            }).encrypt(encryption.getEncryptionKeypair, request.encryptionKey)
        )
    );
};

export const recieveKeys = async (
    msg: KeyResponseMessage,
    setKeys: (keys: KeyWithMeta<any>[]) => Promise<any[]>
) => {
    //
    await setKeys(msg.keys);
};
