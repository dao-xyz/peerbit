import {
	field,
	option,
	serialize,
	variant,
	vec,
} from "@dao-xyz/borsh";
import { type Blocks } from "@peerbit/blocks-interface";
import {
	AccessError,
	type Identity,
	PublicSignKey,
	SignatureWithKey,
	randomBytes,
	sha256Base64,
} from "@peerbit/crypto";
import { verify } from "@peerbit/crypto";
import { compare } from "uint8arrays";
import { LamportClock as Clock, HLC, Timestamp } from "./clock.js";
import { type Encoding, NO_ENCODING } from "./encoding.js";
import { Meta } from "./entry-meta.js";
import { ShallowEntry, ShallowMeta } from "./entry-shallow.js";
import { EntryType } from "./entry-type.js";
import { type CanAppend, Entry } from "./entry.js";
import type { SortableEntry } from "./log-sorting.js";
import { Payload } from "./payload.js";

@variant(0)
export class Signatures {
	@field({ type: vec(SignatureWithKey) })
	signatures!: SignatureWithKey[];

	constructor(properties?: { signatures: SignatureWithKey[] }) {
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

@variant(1)
export class EntryV1<T> extends Entry<T> {
	@field({ type: Meta })
	meta: Meta;

	@field({ type: Payload })
	payload: Payload<T>;

	@field({ type: vec(SignatureWithKey) })
	signatures: SignatureWithKey[];

	@field({ type: option("string") }) // we do option because we serialize and store this in a block without the hash, to receive the hash, which we later set
	hash!: string; // "zd...Foo", we'll set the hash after persisting the entry

	createdLocally?: boolean;

	private _encoding?: Encoding<T>;

	constructor(properties: {
		payload: Payload<T>;
		signatures?: SignatureWithKey[];
		meta: Meta;
		hash?: string;
		createdLocally?: boolean;
	}) {
		super();
		this.meta = properties.meta;
		this.payload = properties.payload;
		this.signatures = properties.signatures || [];
		this.createdLocally = properties.createdLocally;
	}

	init(
		props:
			| {
				encoding: Encoding<T>;
			}
			| EntryV1<T>,
	): this {
		if (props instanceof Entry) {
			this._encoding = props._encoding;
		} else {
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

	getMeta(): Meta {
		return this.meta;
	}

	getClock(): Clock {
		return this.meta.clock;
	}

	getGid(): string {
		return this.getMeta().gid;
	}

	getPayload(): Payload<T> {
		return this.payload;
	}

	async getPayloadValue(): Promise<T> {
		const payload = await this.getPayload();
		return payload.isDecoded ? payload.value : payload.getValue(this.encoding);
	}

	get publicKeys(): PublicSignKey[] {
		return this.signatures.map((x) => x.publicKey) || [];
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
	getSignatures(): SignatureWithKey[] {
		return this.signatures || [];
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

		const signable = EntryV1.toSignable(this);
		const signableBytes = serialize(signable);
		for (const signature of signatures) {
			if (!(await verify(signature, signableBytes))) {
				return false;
			}
		}
		return true;
	}

	static toSignable(entry: EntryV1<any>): Entry<any> {
		// TODO fix types
		const trimmed = new EntryV1({
			meta: entry.meta,
			payload: entry.payload,
			signatures: undefined,
			hash: undefined,
		});
		return trimmed;
	}

	toSignable(): Entry<any> {
		if (this.signatures.length > 0) {
			throw new Error("Expected signatures to have length 0");
		}

		if (this.hash) {
			throw new Error("Expected hash to be undefined");
		}
		return EntryV1.toSignable(this);
	}

	equals(other: Entry<T>) {
		if (other instanceof EntryV1) {
			return (
				this.meta.equals(other.meta) &&
				this.signatures.length === other.signatures.length &&
				this.signatures.every((s, i) => s.equals(other.signatures[i])) &&
				this.payload.equals(other.payload)
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

	static createGid(seed?: Uint8Array): Promise<string> {
		return sha256Base64(seed || randomBytes(32));
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
				(await EntryV1.createGid(properties.meta?.gidSeed));
		}

		maxChainLength += 1n; // include this

		// Sign id, encrypted payload, clock, nexts, refs
		const entry: EntryV1<T> = new EntryV1<T>({
			meta: new Meta({
				clock,
				gid: gid!,
				type: properties.meta?.type ?? EntryType.APPEND,
				data: properties.meta?.data,
				next: nextHashes,
			}),
			payload: payloadToSave,
			signatures: undefined,
			createdLocally: true,
		});

		const signers = properties.signers || [
			properties.identity.sign.bind(properties.identity),
		];

		entry.signatures = []

		const signableBytes = serialize(entry.toSignable());
		let signatures = await Promise.all(
			signers.map((signer) => signer(signableBytes)),
		);
		signatures = signatures.sort((a, b) => compare(a.signature, b.signature));

		entry.signatures = signatures;

		if (properties.canAppend && !(await properties.canAppend(entry))) {
			throw new AccessError();
		}

		// Append hash
		entry.hash = await Entry.toMultihash(properties.store, entry);
		entry.init({ encoding: properties.encoding });
		return entry;
	}

	toShallow(isHead: boolean): ShallowEntry {
		return new ShallowEntry({
			hash: this.hash,
			payloadSize: this.payload.byteLength,
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
