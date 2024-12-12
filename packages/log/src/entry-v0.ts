import {
	field,
	fixedArray,
	option,
	serialize,
	variant,
	vec,
} from "@dao-xyz/borsh";
import { type Blocks } from "@peerbit/blocks-interface";
import {
	AccessError,
	DecryptedThing,
	Ed25519PublicKey,
	type Identity,
	MaybeEncrypted,
	PublicSignKey,
	SignatureWithKey,
	X25519Keypair,
	X25519PublicKey,
	randomBytes,
	sha256Base64,
	toBase64,
} from "@peerbit/crypto";
import { verify } from "@peerbit/crypto";
import { type Keychain } from "@peerbit/keychain";
import { compare } from "uint8arrays";
import { LamportClock as Clock, HLC, Timestamp } from "./clock.js";
import { type Encoding, NO_ENCODING } from "./encoding.js";
import { ShallowEntry, ShallowMeta } from "./entry-shallow.js";
import { EntryType } from "./entry-type.js";
import { type CanAppend, Entry } from "./entry.js";
import type { SortableEntry } from "./log-sorting.js";
import { logger } from "./logger.js";
import { Payload } from "./payload.js";
import { equals } from "./utils.js";

export type MaybeEncryptionPublicKey =
	| X25519PublicKey
	| X25519PublicKey[]
	| Ed25519PublicKey
	| Ed25519PublicKey[]
	| undefined;

const isMaybeEryptionPublicKey = (o: any) => {
	if (!o) {
		return true;
	}
	if (o instanceof X25519PublicKey || o instanceof Ed25519PublicKey) {
		return true;
	}
	if (Array.isArray(o)) {
		return true; // assume entries are either X25519PublicKey or Ed25519PublicKey
	}
	return false;
};

export type EncryptionTemplateMaybeEncrypted = EntryEncryptionTemplate<
	MaybeEncryptionPublicKey,
	MaybeEncryptionPublicKey,
	MaybeEncryptionPublicKey | { [key: string]: MaybeEncryptionPublicKey } // signature either all signature encrypted by same key, or each individually
>;
export interface EntryEncryption {
	receiver: EncryptionTemplateMaybeEncrypted;
	keypair: X25519Keypair;
}

export interface EntryEncryptionTemplate<A, B, C> {
	meta: A;
	payload: B;
	signatures: C;
}

@variant(0)
export class Meta {
	@field({ type: Clock })
	clock: Clock;

	@field({ type: "string" })
	gid: string; // graph id

	@field({ type: vec("string") })
	next: string[];

	@field({ type: "u8" })
	type: EntryType;

	@field({ type: option(Uint8Array) })
	data?: Uint8Array; // Optional metadata

	constructor(properties: {
		gid: string;
		clock: Clock;
		type: EntryType;
		data?: Uint8Array;
		next: string[];
	}) {
		this.gid = properties.gid;
		this.clock = properties.clock;
		this.type = properties.type;
		this.data = properties.data;
		this.next = properties.next;
	}
}

@variant(0)
export class Signatures {
	@field({ type: vec(MaybeEncrypted) })
	signatures!: MaybeEncrypted<SignatureWithKey>[];

	constructor(properties?: { signatures: MaybeEncrypted<SignatureWithKey>[] }) {
		if (properties) {
			this.signatures = properties.signatures;
		}
	}

	equals(other: Signatures) {
		if (this.signatures.length !== other.signatures.length) {
			return false;
		}
		for (let i = 0; i < this.signatures.length; i++) {
			if (!this.signatures[i].equals(other.signatures[i])) {
				return false;
			}
		}
		return true;
	}
}

const maybeEncrypt = <Q>(
	thing: Q,
	keypair?: X25519Keypair,
	receiver?: MaybeEncryptionPublicKey,
): Promise<MaybeEncrypted<Q>> | MaybeEncrypted<Q> => {
	const receivers = receiver
		? Array.isArray(receiver)
			? receiver
			: [receiver]
		: undefined;
	if (receivers?.length && receivers?.length > 0) {
		if (!keypair) {
			throw new Error("Keypair not provided");
		}
		return new DecryptedThing<Q>({
			data: serialize(thing),
			value: thing,
		}).encrypt(keypair, receivers);
	}
	return new DecryptedThing<Q>({
		data: serialize(thing),
		value: thing,
	});
};

@variant(0)
export class EntryV0<T>
	extends Entry<T>
	implements EntryEncryptionTemplate<Meta, Payload<T>, SignatureWithKey[]>
{
	@field({ type: MaybeEncrypted })
	_meta: MaybeEncrypted<Meta>;

	@field({ type: MaybeEncrypted })
	_payload: MaybeEncrypted<Payload<T>>;

	@field({ type: fixedArray("u8", 4) })
	_reserved?: Uint8Array;

	@field({ type: option(Signatures) })
	_signatures?: Signatures;

	@field({ type: option("string") }) // we do option because we serialize and store this in a block without the hash, to receive the hash, which we later set
	hash!: string; // "zd...Foo", we'll set the hash after persisting the entry

	createdLocally?: boolean;

	private _keychain?: Keychain;
	private _encoding?: Encoding<T>;

	constructor(obj: {
		payload: MaybeEncrypted<Payload<T>>;
		signatures?: Signatures;
		meta: MaybeEncrypted<Meta>;
		reserved?: Uint8Array; // intentational type 0  (not used)h
		hash?: string;
		createdLocally?: boolean;
	}) {
		super();
		this._meta = obj.meta;
		this._payload = obj.payload;
		this._signatures = obj.signatures;
		this._reserved = new Uint8Array([0, 0, 0, 0]);
		this.createdLocally = obj.createdLocally;
	}

	init(
		props:
			| {
					keychain?: Keychain;
					encoding: Encoding<T>;
			  }
			| EntryV0<T>,
	): this {
		if (props instanceof Entry) {
			this._keychain = props._keychain;
			this._encoding = props._encoding;
		} else {
			this._keychain = props.keychain;
			this._encoding = props.encoding;
		}
		return this;
	}

	get encoding() {
		if (!this._encoding) {
			throw new Error("Not initialized");
		}
		return this._encoding;
	}

	get meta(): Meta {
		return this._meta.decrypted.getValue(Meta);
	}

	async getMeta(): Promise<Meta> {
		await this._meta.decrypt(this._keychain);
		return this.meta;
	}

	async getClock(): Promise<Clock> {
		return (await this.getMeta()).clock;
	}

	get gid(): string {
		return this._meta.decrypted.getValue(Meta).gid;
	}

	async getGid(): Promise<string> {
		return (await this.getMeta()).gid;
	}

	get payload(): Payload<T> {
		const payload = this._payload.decrypted.getValue(Payload);
		payload.encoding = payload.encoding || this.encoding;
		return payload;
	}

	async getPayload(): Promise<Payload<T>> {
		if (this._payload instanceof DecryptedThing) {
			return this.payload;
		}

		await this._payload.decrypt(this._keychain);
		return this.payload;
	}

	async getPayloadValue(): Promise<T> {
		const payload = await this.getPayload();
		return payload.isDecoded ? payload.value : payload.getValue(this.encoding);
	}

	get publicKeys(): PublicSignKey[] {
		return this.signatures.map((x) => x.publicKey);
	}

	get next(): string[] {
		return this.meta.next;
	}

	async getNext(): Promise<string[]> {
		return (await this.getMeta()).next;
	}

	private _size!: number;

	set size(number: number) {
		this._size = number;
	}

	get size(): number {
		if (this._size == null) {
			throw new Error(
				"Size not set. Size is set when entry is, created, loaded or joined",
			);
		}
		return this._size;
	}

	/**
	 * Will only return signatures I can decrypt
	 * @returns signatures
	 */
	get signatures(): SignatureWithKey[] {
		const signatures = this._signatures!.signatures.filter((x) => {
			try {
				// eslint-disable-next-line @typescript-eslint/no-unused-expressions
				x.decrypted;
				return true;
			} catch (error) {
				return false;
			}
		}).map((x) => x.decrypted.getValue(SignatureWithKey));
		if (signatures.length === 0) {
			this._signatures?.signatures.forEach((x) => x.clear());
			throw new Error("Failed to resolve any signature");
		}
		return signatures;
	}
	/**
	 * Will only return signatures I can decrypt
	 * @returns signatures
	 */
	async getSignatures(): Promise<SignatureWithKey[]> {
		const results = await Promise.allSettled(
			this._signatures!.signatures.map((x) => x.decrypt(this._keychain)),
		);

		if (logger.level === "debug" || logger.level === "trace") {
			for (const [i, result] of results.entries()) {
				if (result.status === "rejected") {
					logger.debug("Failed to decrypt signature with index: " + i);
				}
			}
		}
		return this.signatures;
	}

	/**
	 * Will only verify signatures I can decrypt
	 * @returns true if all are verified
	 */
	async verifySignatures(): Promise<boolean> {
		const signatures = await this.getSignatures();

		if (signatures.length === 0) {
			return false;
		}

		const signable = EntryV0.toSignable(this);
		const signableBytes = serialize(signable);
		for (const signature of signatures) {
			if (!(await verify(signature, signableBytes))) {
				return false;
			}
		}
		return true;
	}

	static toSignable(entry: EntryV0<any>): Entry<any> {
		// TODO fix types
		const trimmed = new EntryV0({
			meta: entry._meta,
			payload: entry._payload,
			reserved: entry._reserved,
			signatures: undefined,
			hash: undefined,
		});
		return trimmed;
	}

	toSignable(): Entry<any> {
		if (this._signatures) {
			throw new Error("Expected signatures to be undefined");
		}

		if (this.hash) {
			throw new Error("Expected hash to be undefined");
		}
		return EntryV0.toSignable(this);
	}

	equals(other: Entry<T>) {
		if (other instanceof EntryV0) {
			return (
				equals(this._reserved, other._reserved) &&
				this._meta.equals(other._meta) &&
				this._signatures!.equals(other._signatures!) &&
				this._payload.equals(other._payload)
			); // dont compare hashes because the hash is a function of the other properties
		}

		return false;
	}

	async delete(store: Blocks): Promise<void> {
		if (!this.hash) {
			throw new Error("Missing hash");
		}
		await store.rm(this.hash);
	}

	static createGid(seed?: Uint8Array): Promise<string> | string {
		return seed ? sha256Base64(seed) : toBase64(randomBytes(32));
	}

	static async create<T>(properties: {
		store: Blocks;
		data: T;
		meta?: {
			clock?: Clock;
			gid?: string;
			type?: EntryType;
			gidSeed?: Uint8Array;
			data?: Uint8Array;
			next?: SortableEntry[];
		};
		encoding?: Encoding<T>;
		canAppend?: CanAppend<T>;
		encryption?: EntryEncryption;
		identity: Identity;
		signers?: ((
			data: Uint8Array,
		) => Promise<SignatureWithKey> | SignatureWithKey)[];
	}): Promise<Entry<T>> {
		if (!properties.encoding || !properties?.meta?.next) {
			properties = {
				...properties,
				meta: {
					...properties?.meta,
					next: properties.meta?.next ? properties.meta?.next : [],
				},
				encoding: properties.encoding ? properties.encoding : NO_ENCODING,
			};
		}

		if (!properties.encoding) {
			throw new Error("Missing encoding options");
		}

		if (properties.data == null) throw new Error("Entry requires data");
		if (properties.meta?.next == null || !Array.isArray(properties.meta.next))
			throw new Error("'next' argument is not an array");

		// Clean the next objects and convert to hashes
		const nexts = properties.meta?.next;

		const payloadToSave = new Payload<T>({
			data: properties.encoding.encoder(properties.data),
			value: properties.data,
			encoding: properties.encoding,
		});

		let clock: Clock | undefined = properties.meta?.clock;
		if (!clock) {
			const hlc = new HLC();
			for (const next of nexts) {
				hlc.update(next.meta.clock.timestamp);
			}

			if (
				properties.encryption?.receiver.signatures &&
				properties.encryption?.receiver.meta
			) {
				throw new Error(
					"Signature is to be encrypted yet the clock is not, which contains the publicKey as id. Either provide a custom Clock value that is not sensitive or set the receiver (encryption target) for the clock",
				);
			}
			clock = new Clock({
				id: properties.identity.publicKey.bytes,
				timestamp: hlc.now(),
			});
		} else {
			const cv = clock;
			// check if nexts, that all nexts are happening BEFORE this clock value (else clock make no sense)
			for (const n of nexts) {
				if (Timestamp.compare(n.meta.clock.timestamp, cv.timestamp) >= 0) {
					throw new Error(
						"Expecting next(s) to happen before entry, got: " +
							n.meta.clock.timestamp +
							" > " +
							cv.timestamp,
					);
				}
			}
		}

		const nextHashes: string[] = [];
		let maxChainLength = 0n;
		let gid: string | null = null;
		if (nexts?.length > 0) {
			// take min gid as our gid
			if (properties.meta?.gid) {
				throw new Error(
					"Expecting '.meta.gid' property to be undefined if '.meta.next' is provided",
				);
			}
			for (const n of nexts) {
				if (!n.hash) {
					throw new Error("Expecting hash to be defined to next entries");
				}
				nextHashes.push(n.hash);
				gid =
					gid == null
						? n.meta.gid
						: n.meta.gid < (gid as string)
							? n.meta.gid
							: gid;
			}
		} else {
			gid =
				properties.meta?.gid ||
				(await EntryV0.createGid(properties.meta?.gidSeed));
		}

		maxChainLength += 1n; // include this

		const metadataEncrypted = await maybeEncrypt(
			new Meta({
				clock,
				gid: gid!,
				type: properties.meta?.type ?? EntryType.APPEND,
				data: properties.meta?.data,
				next: nextHashes,
			}),
			properties.encryption?.keypair,
			properties.encryption?.receiver.meta,
		);

		const payload = await maybeEncrypt(
			payloadToSave,
			properties.encryption?.keypair,
			properties.encryption?.receiver.payload,
		);

		// Sign id, encrypted payload, clock, nexts, refs
		const entry: EntryV0<T> = new EntryV0<T>({
			meta: metadataEncrypted,
			payload,
			signatures: undefined,
			createdLocally: true,
		});

		const signers = properties.signers || [
			properties.identity.sign.bind(properties.identity),
		];

		const signableBytes = serialize(entry.toSignable());
		let signatures = await Promise.all(
			signers.map((signer) => signer(signableBytes)),
		);
		signatures = signatures.sort((a, b) => compare(a.signature, b.signature));

		const encryptedSignatures: MaybeEncrypted<SignatureWithKey>[] = [];
		const encryptAllSignaturesWithSameKey = isMaybeEryptionPublicKey(
			properties.encryption?.receiver?.signatures,
		);

		for (const signature of signatures) {
			const encryptionRecievers = encryptAllSignaturesWithSameKey
				? properties.encryption?.receiver?.signatures
				: (properties.encryption?.receiver?.signatures as any)?.[
						signature.publicKey.hashcode()
					]; // TODO types
			const signatureEncrypted = await maybeEncrypt(
				signature,
				properties.encryption?.keypair,
				encryptionRecievers,
			);
			encryptedSignatures.push(signatureEncrypted);
		}

		entry._signatures = new Signatures({
			signatures: encryptedSignatures,
		});

		if (properties.canAppend && !(await properties.canAppend(entry))) {
			throw new AccessError();
		}

		// Append hash
		entry.hash = await Entry.toMultihash(properties.store, entry);

		entry.init({ encoding: properties.encoding });

		return entry;
	}

	get payloadByteLength() {
		return this._payload.byteLength;
	}

	toShallow(isHead: boolean): ShallowEntry {
		return new ShallowEntry({
			hash: this.hash,
			payloadSize: this._payload.byteLength,
			head: isHead,
			meta: new ShallowMeta({
				gid: this.meta.gid,
				data: this.meta.data,
				clock: this.meta.clock,
				next: this.meta.next,
				type: this.meta.type,
			}),
		});
	}
}
