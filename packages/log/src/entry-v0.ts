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
	Ed25519Keypair,
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
import { type CryptoKeychain } from "@peerbit/keychain";
import { compare } from "uint8arrays";
import { LamportClock as Clock, HLC, Timestamp } from "./clock.js";
import { type Encoding, NO_ENCODING } from "./encoding.js";
import { ShallowEntry, ShallowMeta } from "./entry-shallow.js";
import { EntryType } from "./entry-type.js";
import { type CanAppend, Entry, type PreparedAppendChain } from "./entry.js";
import type { SortableEntry } from "./log-sorting.js";
import { logger as baseLogger } from "./logger.js";
import { Payload } from "./payload.js";
import { equals } from "./utils.js";

const log = baseLogger.newScope("entry-v0");
const traceLogger = log.trace as typeof log & { enabled?: boolean };

type NativePlainChainInput = {
	clockId: Uint8Array;
	privateKey: Uint8Array;
	publicKey: Uint8Array;
	wallTimes: Array<bigint | number | string>;
	logicals?: number[];
	gid: string;
	initialNext?: string[];
	type?: number;
	metaDatas?: Array<Uint8Array | undefined>;
	payloadDatas: Uint8Array[];
};

type NativePlainEntryInput = {
	clockId: Uint8Array;
	privateKey: Uint8Array;
	publicKey: Uint8Array;
	wallTime: bigint | number | string;
	logical?: number;
	gid: string;
	next?: string[];
	type?: number;
	metaData?: Uint8Array;
	payloadData: Uint8Array;
};

type NativePreparedPlainEntry = {
	bytes?: Uint8Array;
	cid: string;
	byteLength: number;
	signature: Uint8Array;
	next: string[];
	metaBytes: Uint8Array;
	payloadBytes: Uint8Array;
	signatureBytes: Uint8Array;
};

type NativeEntryV0Graph = {
	prepareEntryV0PlainChainAndPut?(
		input: NativePlainChainInput,
	): Promise<NativePreparedPlainEntry[]>;
	prepareEntryV0PlainEntryAndPut?(
		input: NativePlainEntryInput,
	): Promise<NativePreparedPlainEntry>;
	prepareEntryV0PlainChainCommit?(
		input: NativePlainChainInput,
		blockStore: unknown,
	): Promise<NativePreparedPlainEntry[] | undefined>;
	prepareEntryV0PlainEntryCommit?(
		input: NativePlainEntryInput,
		blockStore: unknown,
	): Promise<NativePreparedPlainEntry | undefined>;
};

type NativeEntryV0Encoder = {
	encodeEntryV0Signable(input: {
		clockId: Uint8Array;
		wallTime: bigint;
		logical?: number;
		gid: string;
		next?: string[];
		type?: number;
		metaData?: Uint8Array;
		payloadData: Uint8Array;
	}): Promise<Uint8Array>;
	encodeEntryV0Storage(input: {
		clockId: Uint8Array;
		wallTime: bigint;
		logical?: number;
		gid: string;
		next?: string[];
		type?: number;
		metaData?: Uint8Array;
		payloadData: Uint8Array;
		signature: Uint8Array;
		signaturePublicKey: Uint8Array;
		prehash?: number;
	}): Promise<Uint8Array>;
	encodeEntryV0StorageWithCid?(input: {
		clockId: Uint8Array;
		wallTime: bigint;
		logical?: number;
		gid: string;
		next?: string[];
		type?: number;
		metaData?: Uint8Array;
		payloadData: Uint8Array;
		signature: Uint8Array;
		signaturePublicKey: Uint8Array;
		prehash?: number;
	}): Promise<{ bytes: Uint8Array; cid: string }>;
	prepareEntryV0PlainChain?(
		input: NativePlainChainInput,
	): Promise<NativePreparedPlainEntry[]>;
	prepareEntryV0PlainEntry?(
		input: NativePlainEntryInput,
	): Promise<NativePreparedPlainEntry>;
	calculateRawCidV1(bytes: Uint8Array): Promise<string>;
};

let nativeEntryV0EncoderPromise:
	| Promise<NativeEntryV0Encoder | undefined>
	| undefined;

const loadNativeEntryV0Encoder = async () => {
	if (!nativeEntryV0EncoderPromise) {
		nativeEntryV0EncoderPromise = (async () => {
			try {
				const mod = (await import(["@peerbit", "log-rust"].join("/"))) as {
					encodeEntryV0Signable?: NativeEntryV0Encoder["encodeEntryV0Signable"];
					encodeEntryV0Storage?: NativeEntryV0Encoder["encodeEntryV0Storage"];
					encodeEntryV0StorageWithCid?: NativeEntryV0Encoder["encodeEntryV0StorageWithCid"];
					prepareEntryV0PlainChain?: NativeEntryV0Encoder["prepareEntryV0PlainChain"];
					prepareEntryV0PlainEntry?: NativeEntryV0Encoder["prepareEntryV0PlainEntry"];
					calculateRawCidV1?: NativeEntryV0Encoder["calculateRawCidV1"];
				};
				if (
					!mod.encodeEntryV0Signable ||
					!mod.encodeEntryV0Storage ||
					!mod.calculateRawCidV1
				) {
					return undefined;
				}
				return {
					encodeEntryV0Signable: mod.encodeEntryV0Signable,
					encodeEntryV0Storage: mod.encodeEntryV0Storage,
					encodeEntryV0StorageWithCid: mod.encodeEntryV0StorageWithCid,
					prepareEntryV0PlainChain: mod.prepareEntryV0PlainChain,
					prepareEntryV0PlainEntry: mod.prepareEntryV0PlainEntry,
					calculateRawCidV1: mod.calculateRawCidV1,
				};
			} catch {
				return undefined;
			}
		})();
	}
	return nativeEntryV0EncoderPromise;
};

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

	private _keychain?: CryptoKeychain;
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
					keychain?: CryptoKeychain;
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

		if ((traceLogger as any)?.enabled) {
			for (const [i, result] of results.entries()) {
				if (result.status === "rejected") {
					traceLogger("Failed to decrypt signature with index: " + i);
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

		const signableBytes = this.getSignableBytes();
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

	override getSignableBytes(): Uint8Array {
		return serialize(EntryV0.toSignable(this));
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

	static async createPlainAppendChain<T>(properties: {
		data: T[];
		meta?: {
			clocks: () => Clock[];
			gid?: string;
			type?: EntryType;
			gidSeed?: Uint8Array;
			data?: Uint8Array;
			next?: SortableEntry[];
		};
		encoding: Encoding<T>;
		identity: Identity;
		deferStore: boolean;
		cachePreparedEntries?: boolean;
		nativeGraph?: NativeEntryV0Graph;
		nativeBlockStore?: unknown;
	}): Promise<Entry<T>[] | undefined> {
		return (await EntryV0.createPlainAppendChainBatch(properties))?.entries;
	}

	static async createPlainAppendChainBatch<T>(properties: {
		data: T[];
		meta?: {
			clocks: () => Clock[];
			gid?: string;
			type?: EntryType;
			gidSeed?: Uint8Array;
			data?: Uint8Array;
			next?: SortableEntry[];
		};
		encoding: Encoding<T>;
		identity: Identity;
		deferStore: boolean;
		cachePreparedEntries?: boolean;
		nativeGraph?: NativeEntryV0Graph;
		nativeBlockStore?: unknown;
	}): Promise<PreparedAppendChain<T> | undefined> {
		if (!properties.deferStore) {
			return undefined;
		}
		if (!(properties.identity instanceof Ed25519Keypair)) {
			return undefined;
		}
		const nativeEncoder = await loadNativeEntryV0Encoder();
		if (!nativeEncoder?.prepareEntryV0PlainChain) {
			return undefined;
		}

		const nexts = properties.meta?.next ?? [];
		const nextHashes: string[] = [];
		let gid: string | null = null;
		if (nexts.length > 0) {
			if (properties.meta?.gid) {
				throw new Error(
					"Expecting '.meta.gid' property to be undefined if '.meta.next' is provided",
				);
			}
			for (const next of nexts) {
				if (!next.hash) {
					throw new Error("Expecting hash to be defined to next entries");
				}
				nextHashes.push(next.hash);
				gid =
					gid == null
						? next.meta.gid
						: next.meta.gid < (gid as string)
							? next.meta.gid
							: gid;
			}
		} else {
			gid =
				properties.meta?.gid ||
				(await EntryV0.createGid(properties.meta?.gidSeed));
		}

		const clocks = properties.meta?.clocks();
		if (!clocks || clocks.length !== properties.data.length) {
			throw new Error("Expected one clock per entry");
		}
		for (const next of nexts) {
			if (
				Timestamp.compare(next.meta.clock.timestamp, clocks[0]!.timestamp) >= 0
			) {
				throw new Error(
					"Expecting next(s) to happen before entry, got: " +
						next.meta.clock.timestamp +
						" > " +
						clocks[0]!.timestamp,
				);
			}
		}
		for (let i = 1; i < clocks.length; i++) {
			if (
				Timestamp.compare(clocks[i - 1]!.timestamp, clocks[i]!.timestamp) >= 0
			) {
				throw new Error(
					"Expecting generated clocks to increase across appendMany",
				);
			}
		}

		const entryType = properties.meta?.type ?? EntryType.APPEND;
		const clockId = properties.identity.publicKey.bytes;
		const privateKey = properties.identity.privateKey.privateKey;
		const publicKey = properties.identity.publicKey.publicKey;
		let nativeBlocksCommitted = false;
		let nativeGraphUpdated = false;
		let prepared: NativePreparedPlainEntry[] | undefined;
		let payloadDatas: Uint8Array[] | undefined;
		if (properties.data.length === 1) {
			const clock = clocks[0]!;
			const payloadData = properties.encoding.encoder(properties.data[0]!);
			const singleInput: NativePlainEntryInput = {
				clockId,
				privateKey,
				publicKey,
				wallTime: clock.timestamp.wallTime,
				logical: clock.timestamp.logical,
				gid: gid!,
				next: nextHashes,
				type: entryType,
				metaData: properties.meta?.data,
				payloadData,
			};
			const nativeCommit =
				properties.nativeGraph?.prepareEntryV0PlainEntryCommit;
			const preparedEntry = nativeCommit
				? await nativeCommit.call(
						properties.nativeGraph,
						singleInput,
						properties.nativeBlockStore,
					)
				: undefined;
			if (preparedEntry) {
				nativeBlocksCommitted = true;
				nativeGraphUpdated = true;
				prepared = [preparedEntry];
				payloadDatas = [payloadData];
			} else {
				const nativePrepareAndPut =
					properties.nativeGraph?.prepareEntryV0PlainEntryAndPut;
				const scalarPreparedEntry = nativePrepareAndPut
					? await nativePrepareAndPut.call(properties.nativeGraph, singleInput)
					: nativeEncoder.prepareEntryV0PlainEntry
						? await nativeEncoder.prepareEntryV0PlainEntry(singleInput)
						: undefined;
				if (scalarPreparedEntry) {
					nativeGraphUpdated = !!nativePrepareAndPut;
					prepared = [scalarPreparedEntry];
					payloadDatas = [payloadData];
				}
			}
		}

		if (!prepared) {
			const wallTimes = new Array<bigint | number | string>(
				properties.data.length,
			);
			const logicals = new Array<number>(properties.data.length);
			const metaDatas = new Array<Uint8Array | undefined>(
				properties.data.length,
			);
			payloadDatas = new Array<Uint8Array>(properties.data.length);
			for (let i = 0; i < properties.data.length; i++) {
				const clock = clocks[i]!;
				wallTimes[i] = clock.timestamp.wallTime;
				logicals[i] = clock.timestamp.logical;
				metaDatas[i] = properties.meta?.data;
				payloadDatas[i] = properties.encoding.encoder(properties.data[i]!);
			}
			const nativePlainChainInput: NativePlainChainInput = {
				clockId,
				privateKey,
				publicKey,
				wallTimes,
				logicals,
				gid: gid!,
				initialNext: nextHashes,
				type: entryType,
				metaDatas,
				payloadDatas,
			};
			const nativeCommit = properties.nativeGraph?.prepareEntryV0PlainChainCommit;
			prepared = nativeCommit
				? await nativeCommit.call(
						properties.nativeGraph,
						nativePlainChainInput,
						properties.nativeBlockStore,
					)
				: undefined;
			if (prepared) {
				nativeBlocksCommitted = true;
				nativeGraphUpdated = true;
			} else {
				const nativePrepareAndPut =
					properties.nativeGraph?.prepareEntryV0PlainChainAndPut;
				prepared = await (nativePrepareAndPut
					? nativePrepareAndPut.call(
							properties.nativeGraph,
							nativePlainChainInput,
						)
					: nativeEncoder.prepareEntryV0PlainChain(nativePlainChainInput));
				nativeGraphUpdated = !!nativePrepareAndPut;
			}
		}

		const entries: PreparedAppendChain<T>["entries"] = [];
		const blocks: NonNullable<PreparedAppendChain<T>["blocks"]> = [];
		const shallowEntries: PreparedAppendChain<T>["shallowEntries"] = [];
		const nativeEntries: NonNullable<PreparedAppendChain<T>["nativeEntries"]> =
			[];
		if (!payloadDatas) {
			throw new Error("Expected payload data for prepared append chain");
		}

		for (let index = 0; index < prepared.length; index++) {
			const preparedEntry = prepared[index]!;
			const meta = new Meta({
				clock: clocks[index]!,
				gid: gid!,
				type: entryType,
				data: properties.meta?.data,
				next: preparedEntry.next,
			});
			const payload = new Payload<T>({
				data: payloadDatas[index]!,
				value: properties.data[index],
				encoding: properties.encoding,
			});
			const signature = new SignatureWithKey({
				signature: preparedEntry.signature,
				publicKey: properties.identity.publicKey,
				prehash: 0,
			});
			const entry = new EntryV0<T>({
				meta: new DecryptedThing({
					data: preparedEntry.metaBytes,
					value: meta,
				}),
				payload: new DecryptedThing({
					data: preparedEntry.payloadBytes,
					value: payload,
				}),
				signatures: new Signatures({
					signatures: [
						new DecryptedThing({
							data: preparedEntry.signatureBytes,
							value: signature,
						}),
					],
				}),
				createdLocally: true,
			});
			const preparedBlock =
				preparedEntry.bytes && !nativeBlocksCommitted
					? Entry.preparedBlockFromBytes(preparedEntry.bytes, preparedEntry.cid)
					: undefined;
			if (properties.cachePreparedEntries === false) {
				entry.hash = preparedEntry.cid;
				entry.size = preparedEntry.byteLength;
			} else {
				if (!preparedEntry.bytes) {
					throw new Error("Missing prepared entry bytes");
				}
				entry.hash = Entry.prepareMultihashBytes(
					entry,
					preparedEntry.bytes,
					preparedEntry.cid,
				);
			}
			const shallowEntry = new ShallowEntry({
				hash: entry.hash,
				payloadSize: payload.byteLength,
				head: index === prepared.length - 1,
				meta: new ShallowMeta({
					gid: meta.gid,
					data: meta.data,
					clock: meta.clock,
					next: meta.next,
					type: meta.type,
				}),
			});
			const nativeEntry =
				nativeGraphUpdated && properties.cachePreparedEntries === false
					? undefined
					: {
							hash: entry.hash,
							gid: meta.gid,
							next: meta.next,
							type: meta.type,
							head: index === prepared.length - 1,
							payloadSize: payload.byteLength,
							data: meta.data,
							clock: {
								timestamp: {
									wallTime: meta.clock.timestamp.wallTime,
									logical: meta.clock.timestamp.logical,
								},
							},
						};
			if (properties.cachePreparedEntries !== false) {
				Entry.prepareShallowEntry(entry, shallowEntry);
				Entry.prepareNativeLogEntry(entry, nativeEntry!);
			}
			if (properties.cachePreparedEntries !== false) {
				entry.init({ encoding: properties.encoding });
			}
			entries.push(entry);
			if (preparedBlock) {
				blocks.push(preparedBlock);
			}
			shallowEntries.push(shallowEntry);
			if (nativeEntry) {
				nativeEntries.push(nativeEntry);
			}
		}
		return {
			entries,
			blocks: blocks.length > 0 ? blocks : undefined,
			shallowEntries,
			nativeEntries,
			nativeGraphUpdated,
			nativeBlocksCommitted,
		};
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
		deferStore?: boolean;
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
		const entryType = properties.meta?.type ?? EntryType.APPEND;

		const metadataEncrypted = await maybeEncrypt(
			new Meta({
				clock,
				gid: gid!,
				type: entryType,
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
		const nativeEncoder = properties.encryption
			? undefined
			: await loadNativeEntryV0Encoder();
		const nativePlainInput = nativeEncoder
			? {
					clockId: clock.id,
					wallTime: clock.timestamp.wallTime,
					logical: clock.timestamp.logical,
					gid: gid!,
					next: nextHashes,
					type: entryType,
					metaData: properties.meta?.data,
					payloadData: payloadToSave.data,
				}
			: undefined;

		// Sign id, encrypted payload, clock, nexts, refs
		const entry: EntryV0<T> = new EntryV0<T>({
			meta: metadataEncrypted,
			payload,
			signatures: undefined,
			createdLocally: true,
		});

		const signableBytes =
			nativeEncoder && nativePlainInput
				? await nativeEncoder.encodeEntryV0Signable(nativePlainInput)
				: entry.getSignableBytes();
		let signatures = properties.signers
			? properties.signers.length === 1
				? [await properties.signers[0]!(signableBytes)]
				: await Promise.all(
						properties.signers.map((signer) => signer(signableBytes)),
					)
			: [await properties.identity.sign(signableBytes)];
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
			throw new AccessError("Not allowed to append");
		}

		let nativeStorage:
			| {
					bytes: Uint8Array;
					cid: string;
			  }
			| undefined;
		if (
			nativeEncoder &&
			nativePlainInput &&
			!properties.canAppend &&
			signatures.length === 1 &&
			signatures[0]!.publicKey instanceof Ed25519PublicKey
		) {
			const storageInput = {
				...nativePlainInput,
				signature: signatures[0]!.signature,
				signaturePublicKey: signatures[0]!.publicKey.publicKey,
				prehash: signatures[0]!.prehash,
			};
			nativeStorage = nativeEncoder.encodeEntryV0StorageWithCid
				? await nativeEncoder.encodeEntryV0StorageWithCid(storageInput)
				: await (async () => {
						const storageBytes =
							await nativeEncoder.encodeEntryV0Storage(storageInput);
						return {
							bytes: storageBytes,
							cid: await nativeEncoder.calculateRawCidV1(storageBytes),
						};
					})();
		}

		// Append hash
		entry.hash = properties.deferStore
			? nativeStorage
				? Entry.prepareMultihashBytes(
						entry,
						nativeStorage.bytes,
						nativeStorage.cid,
					)
				: await Entry.prepareMultihash(entry)
			: nativeStorage
				? await Entry.toMultihashBytes(
						properties.store,
						entry,
						nativeStorage.bytes,
						nativeStorage.cid,
					)
				: await Entry.toMultihash(properties.store, entry);

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
