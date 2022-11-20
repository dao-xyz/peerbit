import { HLC, LamportClock as Clock, Timestamp } from "./clock";
import { isDefined } from "./is-defined";
import {
    variant,
    field,
    serialize,
    deserialize,
    option,
    vec,
    fixedArray,
} from "@dao-xyz/borsh";
import io from "@dao-xyz/peerbit-io-utils";
import { IPFS } from "ipfs-core-types";
import {
    arraysCompare,
    arraysEqual,
    UInt8ArraySerializer,
} from "@dao-xyz/peerbit-borsh-utils";
import {
    DecryptedThing,
    MaybeEncrypted,
    PublicSignKey,
    X25519PublicKey,
    PublicKeyEncryptionResolver,
    SignatureWithKey,
    AccessError,
    Ed25519PublicKey,
} from "@dao-xyz/peerbit-crypto";
import { toBase64 } from "./utils.js";
import sodium from "libsodium-wrappers";
import { Encoding, JSON_ENCODING } from "./encoding";
import { Identity } from "./identity.js";
import { verify } from "@dao-xyz/peerbit-crypto";
import { StringArray } from "./types";

export type MaybeEncryptionPublicKey =
    | X25519PublicKey
    | X25519PublicKey[]
    | Ed25519PublicKey
    | Ed25519PublicKey[]
    | undefined;

export type EncryptionTemplateMaybeEncrypted = EntryEncryptionTemplate<
    MaybeEncryptionPublicKey,
    MaybeEncryptionPublicKey,
    MaybeEncryptionPublicKey,
    MaybeEncryptionPublicKey
>;
export interface EntryEncryption {
    reciever: EncryptionTemplateMaybeEncrypted;
    options: PublicKeyEncryptionResolver;
}

function arrayToHex(arr: Uint8Array): string {
    return [...new Uint8Array(arr)]
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("");
}

export function toBufferLE(num: bigint, width: number): Uint8Array {
    const hex = num.toString(16);
    const padded = hex.padStart(width * 2, "0").slice(0, width * 2);
    const arr = padded.match(/.{1,2}/g)?.map((byte) => parseInt(byte, 16));
    if (!arr) {
        throw new Error("Unexpected");
    }
    const buffer = Uint8Array.from(arr);
    buffer.reverse();
    return buffer;
}

export function toBigIntLE(buf: Uint8Array): bigint {
    const reversed = buf.reverse();
    const hex = arrayToHex(reversed);
    if (hex.length === 0) {
        return BigInt(0);
    }
    return BigInt(`0x${hex}`);
}

const IpfsNotDefinedError = () => new Error("Ipfs instance not defined");

export type CanAppend<T> = (canAppend: Entry<T>) => Promise<boolean> | boolean;

@variant(0)
export class Payload<T> {
    @field(UInt8ArraySerializer)
    data: Uint8Array;

    _value?: T;
    constructor(props?: { data: Uint8Array; value?: T }) {
        if (props) {
            this.data = props.data;
            this._value = props.value;
        }
    }

    equals(other: Payload<T>): boolean {
        return (
            Buffer.compare(Buffer.from(this.data), Buffer.from(other.data)) ===
            0
        );
    }

    getValue(encoding: Encoding<T> = JSON_ENCODING): T {
        if (this._value != undefined) {
            return this._value;
        }
        return encoding.decoder(this.data);
    }
}

export interface EntryEncryptionTemplate<A, B, C, D> {
    metadata: A;
    payload: B;
    signatures: C;
    next: D;
}

@variant(0)
export class Metadata {
    @field({ type: "string" })
    gid: string; // graph id

    @field({ type: Clock })
    clock: Clock;

    @field({ type: "u64" })
    maxChainLength: bigint; // longest chain/merkle tree path frmo this node. maxChainLength := max ( maxChainLength(this.next) , 1)

    constructor(properties?: {
        gid: string;
        clock: Clock;
        maxChainLength: bigint;
    }) {
        if (properties) {
            this.gid = properties.gid;
            this.clock = properties.clock;
            this.maxChainLength = properties.maxChainLength;
        }
    }
}

@variant(0)
export class Signatures {
    @field({ type: vec(SignatureWithKey) })
    signatures: SignatureWithKey[];

    constructor(properties?: { signatures: SignatureWithKey[] }) {
        if (properties) {
            this.signatures = properties.signatures;
        }
    }
}

@variant(0)
export class Entry<T>
    implements
        EntryEncryptionTemplate<
            Metadata,
            Payload<T>,
            SignatureWithKey[],
            Array<string>
        >
{
    @field({ type: MaybeEncrypted })
    _metadata: MaybeEncrypted<Metadata>;

    @field({ type: MaybeEncrypted })
    _payload: MaybeEncrypted<Payload<T>>;

    @field({ type: MaybeEncrypted })
    _next: MaybeEncrypted<StringArray>; // Array of hashes (the tree)

    @field({ type: MaybeEncrypted })
    _fork: MaybeEncrypted<StringArray>;

    @field({ type: fixedArray("u8", 4) })
    _reserved: number[];

    @field({ type: option(MaybeEncrypted) })
    _signatures?: MaybeEncrypted<Signatures>;

    @field({ type: option("string") }) // we do option because we serialize and store this in a block without the hash, to recieve the hash, which we later set
    hash: string; // "zd...Foo", we'll set the hash after persisting the entry

    _encryption?: PublicKeyEncryptionResolver;
    _encoding?: Encoding<T>;

    constructor(obj?: {
        payload: MaybeEncrypted<Payload<T>>;
        signatures?: MaybeEncrypted<Signatures>;
        metadata: MaybeEncrypted<Metadata>;
        next: MaybeEncrypted<StringArray>;
        fork?: MaybeEncrypted<StringArray>; //  (not used)
        reserved?: number[]; // intentational type 0  (not used)h
        hash?: string;
    }) {
        if (obj) {
            this._metadata = obj.metadata;
            this._payload = obj.payload;
            this._signatures = obj.signatures;
            this._next = obj.next;
            this._fork =
                obj.fork ||
                new DecryptedThing({
                    data: serialize(new StringArray({ arr: [] })),
                });
            this._reserved = obj.reserved || [0, 0, 0, 0];
        }
    }

    init(
        props:
            | {
                  encryption?: PublicKeyEncryptionResolver;
                  encoding: Encoding<T>;
              }
            | Entry<T>
    ): Entry<T> {
        const encryption =
            props instanceof Entry ? props._encryption : props.encryption;
        this._encryption = encryption;
        this._encoding =
            props instanceof Entry ? props._encoding : props.encoding;
        return this;
    }

    get encoding() {
        if (!this._encoding) {
            throw new Error("Not initialized");
        }
        return this._encoding;
    }

    get metadata(): Metadata {
        return this._metadata.decrypted.getValue(Metadata);
    }

    async getMetadata(): Promise<Metadata> {
        await this._metadata.decrypt(
            this._encryption?.getAnyKeypair ||
                (() => Promise.resolve(undefined))
        );
        return this.metadata;
    }

    get gid(): string {
        return this.metadata.gid;
    }
    async getGid(): Promise<string> {
        return (await this.getMetadata()).gid;
    }

    async getClock(): Promise<Clock> {
        return (await this.getMetadata()).clock;
    }

    get maxChainLength(): bigint {
        return this._metadata.decrypted.getValue(Metadata).maxChainLength;
    }

    async getMaxChainLength(): Promise<bigint> {
        return (await this.getMetadata()).maxChainLength;
    }

    get payload(): Payload<T> {
        const payload = this._payload.decrypted.getValue(Payload);
        return payload;
    }

    async getPayload(): Promise<Payload<T>> {
        await this._payload.decrypt(
            this._encryption?.getAnyKeypair ||
                (() => Promise.resolve(undefined))
        );
        return this.payload;
    }

    async getPayloadValue(): Promise<T> {
        const payload = await this.getPayload();
        return payload.getValue(this.encoding);
    }

    get publicKeys(): PublicSignKey[] {
        return this.signatures.map((x) => x.publicKey);
    }

    async getPublicKeys(): Promise<PublicSignKey[]> {
        await this.getSignatures();
        return this.publicKeys;
    }

    get next(): string[] {
        return this._next.decrypted.getValue(StringArray).arr;
    }

    async getNext(): Promise<string[]> {
        await this._next.decrypt(
            this._encryption?.getAnyKeypair ||
                (() => Promise.resolve(undefined))
        );
        return this.next;
    }

    get signatures(): SignatureWithKey[] {
        return this._signatures!.decrypted.getValue(Signatures).signatures;
    }

    async getSignatures(): Promise<SignatureWithKey[]> {
        await this._signatures!.decrypt(
            this._encryption?.getAnyKeypair ||
                (() => Promise.resolve(undefined))
        );
        return this.signatures;
    }

    async verifySignatures(): Promise<boolean> {
        const signatures = await this.getSignatures();
        for (const signature of signatures) {
            if (
                !(await verify(
                    signature.signature,
                    signature.publicKey,
                    Entry.toSignable(this)
                ))
            ) {
                return false;
            }
        }
        return true;
    }

    static toSignable(entry: Entry<any>): Uint8Array {
        // TODO fix types
        const trimmed = new Entry({
            metadata: entry._metadata,
            next: entry._next,
            payload: entry._payload,
            reserved: entry._reserved,
            fork: entry._fork,
            signatures: undefined,
            hash: undefined,
        });
        return serialize(trimmed);
    }

    toSignable(): Uint8Array {
        if (this._signatures) {
            throw new Error("Expected signatures to be undefined");
        }

        if (this.hash) {
            throw new Error("Expected hash to be undefined");
        }
        return Entry.toSignable(this);
    }

    equals(other: Entry<T>) {
        return (
            arraysEqual(this._reserved, other._reserved) &&
            this._metadata.equals(other._metadata) &&
            this._signatures!.equals(other._signatures!) &&
            this._next.equals(other._next) &&
            this._fork.equals(other._fork) &&
            this._payload.equals(other._payload)
        ); // dont compare hashes because the hash is a function of the other properties
    }

    async delete(ipfs: IPFS): Promise<void> {
        if (!this.hash) {
            throw new Error("Missing hash");
        }
        await io.rm(ipfs, this.hash);
    }

    static async createGid(seed?: string): Promise<string> {
        await sodium.ready;
        return toBase64(
            await sodium.crypto_generichash(
                32,
                seed || (await sodium.randombytes_buf(32))
            )
        );
    }

    static async create<T>(properties: {
        ipfs: IPFS;
        gid?: string;
        gidSeed?: string;
        data: T;
        encoding?: Encoding<T>;
        canAppend?: CanAppend<T>;
        next?: Entry<T>[];
        clock?: Clock;
        pin?: boolean;
        encryption?: EntryEncryption;
        identity: Identity;
        signers?: ((data: Uint8Array) => Promise<SignatureWithKey>)[];
    }): Promise<Entry<T>> {
        if (!properties.encoding || !properties.next) {
            properties = {
                ...properties,
                next: properties.next ? properties.next : [],
                encoding: properties.encoding
                    ? properties.encoding
                    : JSON_ENCODING,
            };
        }

        if (!properties.encoding) {
            throw new Error("Missing encoding options");
        }

        if (!isDefined(properties.ipfs)) throw IpfsNotDefinedError();
        if (!isDefined(properties.data)) throw new Error("Entry requires data");
        if (!isDefined(properties.next) || !Array.isArray(properties.next))
            throw new Error("'next' argument is not an array");

        // Clean the next objects and convert to hashes
        const nexts = properties.next;

        const payloadToSave = new Payload<T>({
            data: properties.encoding.encoder(properties.data),
            value: properties.data,
        });

        const maybeEncrypt = async <Q>(
            thing: Q,
            reciever?: MaybeEncryptionPublicKey
        ): Promise<MaybeEncrypted<Q>> => {
            const recievers = reciever
                ? Array.isArray(reciever)
                    ? reciever
                    : [reciever]
                : undefined;
            if (recievers?.length && recievers?.length > 0) {
                if (!properties.encryption) {
                    throw new Error("Encrpryption config not initialized");
                }
                return await new DecryptedThing<Q>({
                    data: serialize(thing),
                    value: thing,
                }).encrypt(
                    properties.encryption.options.getEncryptionKeypair,
                    ...recievers
                );
            }
            return new DecryptedThing<Q>({
                data: serialize(thing),
                value: thing,
            });
        };

        let clock: Clock | undefined = properties.clock;
        if (!clock) {
            /*  const newTime =
                 nexts?.length > 0
                     ? nexts.reduce(maxClockTimeReducer, new HLC().now())
                     : new HLC().now(); */
            const hlc = new HLC();
            nexts.forEach((next) => {
                hlc.update(next.metadata.clock.timestamp);
            });

            if (
                properties.encryption?.reciever.signatures &&
                properties.encryption?.reciever.metadata
            ) {
                throw new Error(
                    "Signature is to be encrypted yet the clock is not, which contains the publicKey as id. Either provide a custom Clock value that is not sensitive or set the reciever (encryption target) for the clock"
                );
            }
            clock = new Clock({
                id: new Uint8Array(serialize(properties.identity.publicKey)),
                timestamp: hlc.now(),
            });
        } else {
            const cv = clock;
            // check if nexts, that all nexts are happening BEFORE this clock value (else clock make no sense)
            nexts.forEach((n) => {
                if (
                    Timestamp.compare(
                        n.metadata.clock.timestamp,
                        cv.timestamp
                    ) >= 0
                ) {
                    throw new Error(
                        "Expecting next(s) to happen before entry, got: " +
                            n.metadata.clock.timestamp +
                            " > " +
                            cv.timestamp
                    );
                }
            });
        }

        const payload = await maybeEncrypt(
            payloadToSave,
            properties.encryption?.reciever.payload
        );

        const nextHashes: string[] = [];
        let gid!: string;
        let maxChainLength = 0n;
        const maxClock = new Timestamp({ wallTime: 0n, logical: 0 });
        if (nexts?.length > 0) {
            // take min gid as our gid
            nexts.forEach((n) => {
                if (!n.hash) {
                    throw new Error(
                        "Expecting hash to be defined to next entries"
                    );
                }
                nextHashes.push(n.hash);
                if (
                    maxChainLength < n.maxChainLength ||
                    maxChainLength == n.maxChainLength
                ) {
                    maxChainLength = n.maxChainLength;
                    if (!gid) {
                        gid = n.metadata.gid;
                        return;
                    }
                    // replace gid if next is from alonger chain, or from a later time, or same time but "smaller" gid
                    else if (
                        /*   maxChainLength < n.maxChainLength ||
                          maxClock < n.clock.logical ||
                          (maxClock == n.clock.logical && n.gid < gid) */ // Longest chain
                        Timestamp.compare(
                            n.metadata.clock.timestamp,
                            maxClock
                        ) > 0 ||
                        (Timestamp.compare(
                            n.metadata.clock.timestamp,
                            maxClock
                        ) == 0 &&
                            n.metadata.gid < gid)
                    ) {
                        gid = n.metadata.gid;
                    }
                }
            });
            if (!gid) {
                throw new Error("Unexpected behaviour, could not find gid");
            }
        } else {
            gid = properties.gid || (await Entry.createGid(properties.gidSeed));
        }

        maxChainLength += 1n; // include this

        const metadataEncrypted = await maybeEncrypt(
            new Metadata({
                maxChainLength,
                clock,
                gid,
            }),
            properties.encryption?.reciever.metadata
        );

        const next = nextHashes;
        next?.forEach((next) => {
            if (typeof next !== "string") {
                throw new Error("Unsupported next type");
            }
        });

        const nextEncrypted = await maybeEncrypt(
            new StringArray({
                arr: next,
            }),
            properties.encryption?.reciever.next
        );

        const forks = new DecryptedThing<StringArray>({
            data: serialize(new StringArray({ arr: [] })),
        });

        // Sign id, encrypted payload, clock, nexts, refs
        const entry: Entry<T> = new Entry<T>({
            payload,
            metadata: metadataEncrypted,
            signatures: undefined,
            fork: forks,
            next: nextEncrypted, // Array of hashes
            /* refs: properties.refs, */
        });

        const signers = properties.signers || [
            async (data: Uint8Array) =>
                new SignatureWithKey({
                    signature: await properties.identity.sign(data),
                    publicKey: properties.identity.publicKey,
                }),
        ];
        const signable = entry.toSignable();
        let signatures = await Promise.all(
            signers.map((signer) => signer(signable))
        );
        signatures = signatures.sort((a, b) =>
            arraysCompare(a.signature, b.signature)
        );

        const signatureEncrypted = await maybeEncrypt(
            new Signatures({
                signatures,
            }),
            properties.encryption?.reciever.signatures
        );

        entry._signatures = signatureEncrypted;
        entry.init({
            encryption: properties.encryption?.options,
            encoding: properties.encoding,
        });

        if (properties.canAppend) {
            if (!(await properties.canAppend(entry))) {
                throw new AccessError();
            }
        }
        // Append hash and signature
        entry.hash = await Entry.toMultihash(
            properties.ipfs,
            entry,
            properties.pin
        );
        return entry;
    }

    /**
     * Transforms an entry into a Buffer.
     * @param {Entry} entry The entry
     * @return {Buffer} The buffer
     */
    static toBuffer<T>(entry: Entry<T>) {
        return Buffer.from(serialize(entry));
    }

    /**
     * Get the multihash of an Entry.
     * @param {IPFS} ipfs An IPFS instance
     * @param {Entry} entry Entry to get a multihash for
     * @returns {Promise<string>}
     * @example
     * const multfihash = await Entry.toMultihash(ipfs, entry)
     * console.log(multihash)
     * // "Qm...Foo"
     * @deprecated
     */
    static async toMultihash<T>(ipfs: IPFS, entry: Entry<T>, pin = false) {
        if (!ipfs) throw IpfsNotDefinedError();

        if (entry.hash) {
            throw new Error("Expected hash to be missing");
        }

        return io.write(ipfs, "raw", serialize(entry), {
            pin,
        });
    }

    /**
     * Create an Entry from a hash.
     * @param {IPFS} ipfs An IPFS instance
     * @param {string} hash The hash to create an Entry from
     * @returns {Promise<Entry<T>>}
     * @example
     * const entry = await Entry.fromMultihash(ipfs, "zd...Foo")
     * console.log(entry)
     * // { hash: "Zd...Foo", payload: "hello", next: [] }
     */
    static async fromMultihash<T>(ipfs: IPFS, hash: string) {
        if (!ipfs) throw IpfsNotDefinedError();
        if (!hash) throw new Error(`Invalid hash: ${hash}`);
        const bytes = await io.read(ipfs, hash);
        const entry = deserialize(bytes, Entry);
        entry.hash = hash;
        return entry;
    }

    /**
     * Compares two entries.
     * @param {Entry} a
     * @param {Entry} b
     * @returns {number} 1 if a is greater, -1 is b is greater
     */
    static compare<T>(a: Entry<T>, b: Entry<T>) {
        const aClock = a.metadata.clock;
        const bClock = b.metadata.clock;
        const distance = Clock.compare(aClock, bClock);
        if (distance === 0) return aClock.id < bClock.id ? -1 : 1;
        return distance;
    }

    /**
     * Check if an entry equals another entry.
     * @param {Entry} a
     * @param {Entry} b
     * @returns {boolean}
     */
    static isEqual<T>(a: Entry<T>, b: Entry<T>) {
        return a.hash === b.hash;
    }

    /**
     * Check if an entry is a parent to another entry.
     * @param {Entry} entry1 Entry to check
     * @param {Entry} entry2 The parent Entry
     * @returns {boolean}
     */
    static isDirectParent<T>(entry1: Entry<T>, entry2: Entry<T>) {
        return entry2.next.indexOf(entry1.hash as any) > -1; // TODO fix types
    }

    /**
     * Find entry's children from an Array of entries.
     * Returns entry's children as an Array up to the last know child.
     * @param {Entry} entry Entry for which to find the parents
     * @param {Array<Entry<T>>} values Entries to search parents from
     * @returns {Array<Entry<T>>}
     */
    static findDirectChildren<T>(
        entry: Entry<T>,
        values: Entry<T>[]
    ): Entry<T>[] {
        let stack: Entry<T>[] = [];
        let parent = values.find((e) => Entry.isDirectParent(entry, e));
        let prev = entry;
        while (parent) {
            stack.push(parent);
            prev = parent;
            parent = values.find((e) => Entry.isDirectParent(prev, e));
        }
        stack = stack.sort((a, b) =>
            Clock.compare(a.metadata.clock, b.metadata.clock)
        );
        return stack;
    }
}
