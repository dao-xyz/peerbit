import { AbstractLevel } from "abstract-level";
import LRU from "lru-cache";
import { variant, field, serialize, deserialize } from "@dao-xyz/borsh";
import { UInt8ArraySerializer } from "@dao-xyz/peerbit-borsh-utils";
import {
  X25519PublicKey,
  Ed25519PublicKey,
  Keypair,
  X25519Keypair,
  Ed25519Keypair,
  PublicSignKey,
  PublicKeyEncryptionKey,
} from "@dao-xyz/peerbit-crypto";
import { waitFor } from "@dao-xyz/peerbit-time";
import { createHash } from "crypto";
import sodium from "libsodium-wrappers";
import { StoreError } from "./errors";
import { Level } from "level";

export interface Type<T> extends Function {
  new (...args: any[]): T;
}
const PATH_KEY = ".";
const DEFAULT_KEY_GROUP = "_";
const getGroupKey = (group: string) =>
  group === DEFAULT_KEY_GROUP
    ? DEFAULT_KEY_GROUP
    : createHash("sha1").update(group).digest("base64");
const getIdKey = (id: string | Buffer | Uint8Array | PublicSignKey): string => {
  if (id instanceof PublicSignKey || id instanceof PublicKeyEncryptionKey) {
    return id.hashCode();
  }

  if (typeof id !== "string") {
    id = Buffer.isBuffer(id)
      ? id.toString("base64")
      : Buffer.from(id).toString("base64");
  } else {
    if (isPath(id)) {
      throw new Error("Ids can not contain path key: " + PATH_KEY);
    }
  }
  return id;
};

const isPath = (id: string) => id.indexOf(PATH_KEY) !== -1;

/* export type WithType<T> = Constructor<T> & { type: string };
 */
export const getPath = (group: string, key: string) => {
  return group + PATH_KEY + key;
};

const idFromKey = async (keypair: Keypair): Promise<string> => {
  return publicKeyFromKeyPair(keypair).hashCode();
};

const publicKeyFromKeyPair = (keypair: Keypair) => {
  if (keypair instanceof X25519Keypair) {
    return keypair.publicKey;
  } else if (keypair instanceof Ed25519Keypair) {
    return keypair.publicKey;
  }
  throw new Error("Unsupported");
};

/**
 * Node only
 * @param path
 * @returns
 */
export const createStore = async (
  path = "./keystore"
): Promise<AbstractLevel<any, string>> => {
  const fs = await import("fs");
  if (fs && fs.mkdirSync) {
    fs.mkdirSync(path, { recursive: true });
  }
  return new Level(path, { valueEncoding: "view" });
};

/* const verifiedCache: { get(string: string): { publicKey: Ed25519PublicKey, data: Uint8Array }, set(string: string, value: { publicKey: Ed25519PublicKey, data: Uint8Array }): void } = new LRU({ max: 1000 })
 */

const NONCE_LENGTH = 24;

/**
 * Enc MSG with Metadata
 */
@variant(0)
export class EncryptedMessage {
  @field(UInt8ArraySerializer)
  nonce: Uint8Array;

  @field(UInt8ArraySerializer)
  cipher: Uint8Array;

  constructor(props?: EncryptedMessage) {
    if (props) {
      this.nonce = props.nonce;
      this.cipher = props.cipher;
    }
  }
}

@variant(0)
export class KeyWithMeta<T extends Keypair> {
  @field({ type: "string" })
  group: string;

  @field({ type: "u64" })
  timestamp: bigint;

  @field({ type: Keypair })
  keypair: T;

  constructor(props?: { timestamp: bigint; group: string; keypair: T }) {
    if (props) {
      this.timestamp = props.timestamp;
      this.group = props.group;
      this.keypair = props.keypair;
    }
  }

  static get type(): string {
    throw new Error("Unsupported");
  }

  equals(other: KeyWithMeta<T>, ignoreMissingSecret = false) {
    return (
      this.timestamp === other.timestamp &&
      this.group === other.group &&
      this.keypair.equals(other.keypair)
    );
  }

  clone() {
    return new KeyWithMeta<T>({
      group: this.group,
      timestamp: this.timestamp,
      keypair: this.keypair,
    });
  }
  static async toX25519(from: KeyWithMeta<Ed25519Keypair>) {
    return new KeyWithMeta({
      ...from,
      keypair: await X25519Keypair.from(from.keypair),
    });
  }
}

export class Keystore {
  _store: AbstractLevel<any, string>;
  _cache: LRU<string, KeyWithMeta<any>>;

  constructor(store: AbstractLevel<any, string>, cache?: any) {
    this._store = store;
    if (!this.open && !this.opening && this._store.open) {
      this._store.open();
    }
    if (!this._store) {
      throw new Error("Store needs to be provided");
    }
    this._cache = cache || new LRU({ max: 100 });
  }

  async openStore() {
    if (this._store) {
      await this._store.open();
      return Promise.resolve();
    }
    return Promise.reject(new Error("Keystore: No store found to open"));
  }

  assertOpen() {
    if (!this.open) {
      throw new StoreError("Keystore not open");
    }
  }

  async close(): Promise<void> {
    if (!this._store) return;
    if (this._store.status === "closed") {
      return;
    }
    if (this._store.status !== "closing") {
      await this._store.close();
    }
    await waitFor(() => this._store.status === "closed");
  }

  get groupStore() {
    return this._store.sublevel("group");
  }

  get keyStore() {
    return this._store.sublevel("key");
  }

  async hasKey(
    id: string | Buffer | Uint8Array | X25519PublicKey | Ed25519PublicKey,
    group?: string
  ): Promise<boolean> {
    const getKey = await this.getKey(id, group);
    return !!getKey;
  }

  async createEd25519Key(
    options: {
      id?: string | Buffer | Uint8Array;
      group?: string;
      overwrite?: boolean;
    } = {}
  ): Promise<KeyWithMeta<Ed25519Keypair>> {
    return this.createKey(await Ed25519Keypair.create(), options);
  }
  async createX25519Key(
    options: {
      id?: string | Buffer | Uint8Array;
      group?: string;
      overwrite?: boolean;
    } = {}
  ): Promise<KeyWithMeta<X25519Keypair>> {
    return this.createKey(await X25519Keypair.create(), options);
  }
  async createKey<T extends Keypair>(
    keypair: T,
    options: {
      id?: string | Buffer | Uint8Array | PublicSignKey;
      group?: string;
      overwrite?: boolean;
    } = {}
  ): Promise<KeyWithMeta<T>> {
    await sodium.ready;
    /*  let key: { secretKey: X25519SecretKey, publicKey: X25519PublicKey } | { secretKey: Ed25519PrivateKey, publicKey: Ed25519PublicKey } = undefined; */
    /* 
        if (type as any === BoxKeyWithMeta) { // TODO fix types
          let kp = await sodium.crypto_box_keypair();
          key = {
            secretKey: new X25519SecretKey({ secretKey: kp.privateKey }),
            publicKey: new X25519PublicKey({ publicKey: kp.publicKey }),
          }
        }
        else if (type as any === KeyWithMeta<Ed25519Keypair>) { // TODO fix types
          let kp = await sodium.crypto_sign_keypair()
          key = {
            secretKey: new Ed25519PrivateKey({ secretKey: kp.privateKey }),
            publicKey: new Ed25519PublicKey({ publicKey: kp.publicKey }),
          }
        }
        else {
          throw new Error("Unuspported")
        } */
    const keyWithMeta = new KeyWithMeta({
      timestamp: BigInt(+new Date()),
      group: options.group || DEFAULT_KEY_GROUP,
      keypair,
    });

    await this.saveKey(keyWithMeta, options);
    return keyWithMeta;
  }
  get opening(): boolean {
    try {
      return this._store.status === "opening";
    } catch (error) {
      return false; // .status will throw error if not opening sometimes
    }
  }

  get open(): boolean {
    try {
      return this._store.status === "open";
    } catch (error) {
      return false; // .status will throw error if not opening sometimes
    }
  }
  async waitForOpen() {
    if (this._store.status === "closed" || this._store.status === "closing") {
      throw new StoreError("Keystore is closed or closing");
    }
    await waitFor(() => this.open);
  }

  async saveKey<T extends Keypair>(
    key: KeyWithMeta<T>,
    options: {
      id?: string | Buffer | Uint8Array | PublicSignKey;
      overwrite?: boolean;
    } = {}
  ): Promise<KeyWithMeta<T>> {
    // TODO fix types

    await this.waitForOpen();
    this.assertOpen();

    const idKey = options.id
      ? getIdKey(options.id)
      : await idFromKey(key.keypair);

    // Normalize group names
    const groupHash = getGroupKey(key.group);
    const path = getPath(groupHash, idKey);

    if (!options.overwrite) {
      const existingKey = await this.getKey(path);
      if (existingKey && !existingKey.equals(key)) {
        throw new Error("Key already exist with this id, and is different");
      }
    }

    const ser = serialize(key);
    const publicKeyString = publicKeyFromKeyPair(key.keypair).hashCode();
    await this.groupStore.put(path, Buffer.from(ser), {
      valueEncoding: "view",
    }); // TODO fix types, are just wrong
    await this.keyStore.put(publicKeyString, Buffer.from(ser), {
      valueEncoding: "view",
    }); // TODO fix types, are just wrong
    this._cache.set(path, key);
    this._cache.set(publicKeyString, key);

    if (key.keypair instanceof Ed25519Keypair) {
      await this.saveKey(
        new KeyWithMeta({
          group: key.group,
          keypair: await X25519Keypair.from(key.keypair),
          timestamp: key.timestamp,
        })
      );
    }
    return key;
  }

  async getKey<T extends Keypair>(
    id: string | Buffer | Uint8Array | PublicSignKey,
    group?: string
  ): Promise<KeyWithMeta<T> | undefined> {
    await this.waitForOpen();
    this.assertOpen();

    let path: string;
    if (typeof id === "string" && isPath(id)) {
      path = id;
      if (group !== undefined) {
        throw new Error("Id is already a path, group parameter is not needed");
      }
    } else {
      group = getGroupKey(group || DEFAULT_KEY_GROUP);
      path = getPath(group, getIdKey(id));
    }
    const cachedKey = this._cache.get(path);

    let loadedKey: KeyWithMeta<T>;
    if (cachedKey) loadedKey = cachedKey;
    else {
      let buffer: Uint8Array;
      try {
        buffer =
          id instanceof PublicSignKey
            ? await this.keyStore.get(id.hashCode(), { valueEncoding: "view" })
            : await this.groupStore.get(path, { valueEncoding: "view" });
      } catch (e: any) {
        // not found
        return;
      }
      loadedKey = deserialize(buffer, KeyWithMeta) as KeyWithMeta<T>;
    }

    if (!loadedKey) {
      return;
    }

    if (!cachedKey) {
      this._cache.set(path, loadedKey);
    }

    return loadedKey; // TODO fix types, we make assumptions here
  }

  async getKeys<T extends Keypair>(
    group: string
  ): Promise<KeyWithMeta<T>[] | undefined> {
    if (!this._store) {
      await this.openStore();
    }

    await this.waitForOpen();
    this.assertOpen();

    try {
      // Normalize group names
      const groupHash = getGroupKey(group);
      const prefix = groupHash;

      const iterator = this.groupStore.iterator<any, Uint8Array>({
        gte: prefix,
        lte: prefix + "\xFF",
        valueEncoding: "view",
      });
      const ret: KeyWithMeta<any>[] = [];

      for await (const [_key, value] of iterator) {
        ret.push(deserialize(value, KeyWithMeta));
      }

      return ret;
    } catch (e: any) {
      // not found
      return;
    }
  }
}
