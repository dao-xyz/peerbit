import { variant, field, option, serialize, vec } from "@dao-xyz/borsh";
import { ProtocolMessage } from "./message.js";
import { UInt8ArraySerializer } from "@dao-xyz/peerbit-borsh-utils";
import {
  Ed25519Keypair,
  Ed25519PublicKey,
  K,
  PublicKeyEncryptionResolver,
  X25519Keypair,
  X25519PublicKey,
} from "@dao-xyz/peerbit-crypto";
import { Keystore, KeyWithMeta, StoreError } from "@dao-xyz/peerbit-keystore";
import { MaybeSigned, SignatureWithKey } from "@dao-xyz/peerbit-crypto";
import { DecryptedThing } from "@dao-xyz/peerbit-crypto";
import { TimeoutError, waitForAsync } from "@dao-xyz/peerbit-time";
import { Key } from "@dao-xyz/peerbit-crypto";
import { Constructor } from "@dao-xyz/borsh";

import { Identity } from "@dao-xyz/ipfs-log";
import { logger as parentLogger } from "./logger.js";
const logger = parentLogger.child({ module: "exchange-keys" });

export type KeyAccessCondition = (
  keyToAccess: KeyWithMeta<Ed25519Keypair | X25519Keypair>
) => boolean;

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

export enum RequestKeyType {
  Sign = 0,
  Encryption = 1,
}

@variant(0)
export class RequestKeyCondition<T extends Ed25519Keypair | X25519Keypair> {
  @field({ type: option("u8") })
  _type?: RequestKeyType;

  constructor(props?: { type?: Constructor<T> | RequestKeyType }) {
    if (props) {
      if (props.type === undefined) {
        return;
      }

      if ((props.type as number) in RequestKeyType) {
        this._type = props.type as RequestKeyType;
      } else {
        if (props.type === (Ed25519Keypair as any)) {
          // TODO fix types
          this._type = 0;
        } else if (props.type === (X25519Keypair as any)) {
          // TODO fix types
          this._type = 1;
        }
      }
    }
  }

  get type(): Constructor<T> | undefined {
    if (this._type === undefined) {
      return;
    }
    if (this._type === 0) {
      return KeyWithMeta as any as Constructor<T>;
    } else if (this._type === 1) {
      return KeyWithMeta as any as Constructor<T>;
    } else {
      throw new Error("Unsupported");
    }
  }

  get hashcode(): string {
    throw new Error("Unsupported");
  }
}

@variant(0)
export class RequestKeysByAddress<
  T extends Ed25519Keypair | X25519Keypair
> extends RequestKeyCondition<T> {
  @field({ type: "string" })
  address: string;

  constructor(props?: {
    type?: Constructor<T> | RequestKeyType;
    address: string;
  }) {
    super({ type: props?.type as Constructor<T> });
    if (props) {
      this.address = props.address;
    }
  }

  get hashcode() {
    return this._type + this.address;
  }
}

@variant(1)
export class RequestKeysByKey<
  T extends Ed25519Keypair | X25519Keypair
> extends RequestKeyCondition<T> {
  @field({ type: Key })
  _key: Key;

  constructor(props?: { key: X25519PublicKey | Ed25519PublicKey }) {
    super({});
    if (props) {
      this._key = props.key;
    }
  }

  get key(): X25519PublicKey | Ed25519PublicKey {
    return this._key as X25519PublicKey | Ed25519PublicKey;
  }

  get hashcode() {
    return this.key.hashCode();
  }
}

@variant([1, 0])
export class RequestKeyMessage<
  T extends Ed25519Keypair | X25519Keypair
> extends ProtocolMessage {
  @field({ type: Key })
  _encryptionKey: Key;

  @field({ type: RequestKeyCondition })
  condition: RequestKeyCondition<T>;

  // TODO peer info for sending repsonse directly

  constructor(props?: {
    encryptionKey: X25519PublicKey | Ed25519PublicKey;
    condition: RequestKeyCondition<T>;
  }) {
    super();
    if (props) {
      this._encryptionKey = props.encryptionKey;
      this.condition = props.condition;
    }
  }

  get encryptionKey(): X25519PublicKey | Ed25519PublicKey {
    return this._encryptionKey as X25519PublicKey | Ed25519PublicKey;
  }
}

@variant([1, 1])
export class KeyResponseMessage extends ProtocolMessage {
  @field({ type: vec(KeyWithMeta) })
  keys: KeyWithMeta<Ed25519Keypair | X25519Keypair>[];

  constructor(props?: { keys: KeyWithMeta<Ed25519Keypair | X25519Keypair>[] }) {
    super();
    if (props) {
      this.keys = props.keys;
    }
  }
}

export const requestAndWaitForKeys = async <
  T extends Ed25519Keypair | X25519Keypair
>(
  condition: RequestKeyCondition<T>,
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
      const key = await waitForAsync(() => keystore.getKey<T>(condition.key), {
        timeout,
        delayInterval: 50,
      });
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

export const requestKeys = async <T extends X25519Keypair | Ed25519Keypair>(
  condition: RequestKeyCondition<T>,
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
      "Invalid key type for identity, got: " + key.keypair.constructor.name
    );
    return;
  }
  const signedMessage = await new MaybeSigned<RequestKeyMessage<T>>({
    data: serialize(
      new RequestKeyMessage<T>({
        condition,
        encryptionKey: (key.keypair as Ed25519Keypair | X25519Keypair)
          .publicKey,
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
  request: RequestKeyMessage<T>,
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
    const key = await keystore.getKey<T>(request.condition.key);
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
        return (await canAccessKey(key)) ? key : key.clone();
      })
    )
  ).filter((x) => !!x);

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
