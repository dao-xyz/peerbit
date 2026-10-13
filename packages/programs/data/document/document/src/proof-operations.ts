import {
	BinaryReader,
	BinaryWriter,
	type CustomField,
	deserialize,
	deserializeStruct,
	field,
	fixedArray,
	serialize,
	variant,
} from "@dao-xyz/borsh";
import {
	DecryptedThing,
	Ed25519PublicKey,
	PreHash,
	SignatureWithKey,
	sha256Sync,
} from "@peerbit/crypto";
import { IdKey, StringKey, Uint8ArrayKey } from "@peerbit/indexer-interface";
import {
	BORSH_ENCODING,
	Entry,
	EntryType,
	EntryV0,
	LamportClock,
	Meta,
	Signatures,
	calculateRawCidV1Batch,
} from "@peerbit/log";
import { equals, toString } from "uint8arrays";
import { MAX_BATCH_SIZE } from "./constants.js";
import {
	type ProofDeleteContract,
	copyProofDeleteContract,
	deriveProofDeleteScope,
} from "./proof-delete-manifest.js";

// Separate wire root from legacy Operation; selected only by the persisted
// owner-proofs Documents manifest, not a caller-supplied verification flag.
export const MAX_PROOF_HISTORY_BLOCKS = 256;
export const MAX_PROOF_HISTORY_EDGES = 1024;

function requireFact(condition: unknown, message: string): asserts condition {
	if (!condition) throw new Error(message);
}

const copyBytes = (source: Uint8Array, limit = MAX_BATCH_SIZE): Uint8Array => {
	requireFact(source instanceof Uint8Array, "Expected original bytes");
	const size = Reflect.get(
		Uint8Array.prototype,
		"byteLength",
		source,
	) as number;
	requireFact(size <= limit, "Proof byte limit exceeded");
	const copy = new Uint8Array(size);
	Uint8Array.prototype.set.call(copy, source);
	return copy;
};
const copy32 = (source: Uint8Array) => {
	const bytes = copyBytes(source, 32);
	requireFact(bytes.byteLength === 32, "Expected 32-byte proof field");
	return bytes;
};
const copyKey = (key: IdKey): IdKey => deserialize(serialize(key), IdKey);
const keyBytes = (key: IdKey) => serialize(key);

// V1 groups every version/conflict/retirement of one authoritative typed key.
// This reuses the lower log's gid index and co-locates that key under the default
// hash replication domain; a hot key therefore remains one placement unit.
export const deriveProofKeyGid = (scope: Uint8Array, key: IdKey): string => {
	const writer = new BinaryWriter();
	writer.string("peerbit/document/proof-delete/key/v1");
	writer.set(copy32(scope));
	writer.uint8Array(keyBytes(key));
	return toString(sha256Sync(writer.finalize()), "hex");
};

const readBytes = (reader: BinaryReader, limit = MAX_BATCH_SIZE) => {
	const size = reader.u32();
	requireFact(size <= limit, "Proof byte limit exceeded");
	requireFact(
		size <= reader._buf.byteLength - reader._offset,
		"Truncated proof bytes",
	);
	return reader.buffer(size);
};
const witnessCodec: CustomField<Uint8Array[]> = {
	serialize(witnesses, writer) {
		const owned = copyWitnesses(witnesses);
		writer.u32(owned.length);
		for (const bytes of owned) writer.uint8Array(bytes);
	},
	deserialize(reader) {
		const count = reader.u32();
		requireFact(
			count > 0 && count <= MAX_PROOF_HISTORY_BLOCKS,
			"Proof history block limit exceeded",
		);
		const witnesses: Uint8Array[] = [];
		let total = 0;
		for (let i = 0; i < count; i++) {
			const bytes = readBytes(reader, MAX_BATCH_SIZE - total);
			total += bytes.byteLength;
			witnesses.push(bytes);
		}
		return witnesses;
	},
};

// Bound supported V0 container counts before generic Borsh array decoding. This
// is only a shape/size preflight; canonical decoding and signatures below remain
// authoritative. No new Entry codec or permissive fallback is registered.
const preflightEntry = (bytes: Uint8Array) => {
	const reader = new BinaryReader(bytes);
	const plaintext = () => {
		requireFact(
			reader.u8() === 0 && reader.u8() === 0,
			"Expected plaintext proof envelope",
		);
		return readBytes(reader);
	};
	requireFact(reader.u8() === 0, "Unsupported proof entry");
	const metaBytes = plaintext();
	plaintext();
	requireFact(
		reader._buf.byteLength - reader._offset >= 4,
		"Truncated proof entry",
	);
	reader.buffer(4); // EntryV0's fixed reserved field.
	requireFact(
		reader.u8() === 1 && reader.u8() === 0 && reader.u32() === 1,
		"Expected plaintext entry with one Ed25519 owner signature",
	);
	plaintext();
	requireFact(
		reader.u8() === 0 && reader._offset === bytes.byteLength,
		"Embedded hash or noncanonical proof entry",
	);
	const meta = new BinaryReader(metaBytes);
	requireFact(meta.u8() === 0, "Unsupported proof metadata");
	deserializeStruct(LamportClock, false)(meta);
	readBytes(meta); // UTF-8 gid; canonical decoding validates its contents later.
	requireFact(
		meta.u32() <= MAX_PROOF_HISTORY_EDGES,
		"Proof history edge limit exceeded",
	);
};

@variant("documents_proof_operation_v1")
export class ProofOperation {}

@variant(0)
export class ProofPutOperation extends ProofOperation {
	@field({ type: fixedArray("u8", 32) })
	scope: Uint8Array;

	@field({ type: IdKey })
	key: IdKey;

	@field({ type: Uint8Array })
	data: Uint8Array;

	constructor(properties: { scope: Uint8Array; key: IdKey; data: Uint8Array }) {
		super();
		this.scope = copy32(properties.scope);
		this.key = copyKey(properties.key);
		this.data = copyBytes(properties.data);
	}
}

@variant(1)
export class ProofDeleteOperation extends ProofOperation {
	@field({ type: fixedArray("u8", 32) })
	scope: Uint8Array;

	@field({ type: IdKey })
	key: IdKey;

	@field({ type: "string" })
	target: string;

	@field(witnessCodec)
	witnesses: Uint8Array[];

	constructor(properties: {
		scope: Uint8Array;
		key: IdKey;
		target: string;
		witnesses: readonly Uint8Array[];
	}) {
		super();
		this.scope = copy32(properties.scope);
		this.key = copyKey(properties.key);
		this.target = properties.target;
		this.witnesses = copyWitnesses(properties.witnesses);
	}
}

export const BORSH_ENCODING_PROOF_OPERATION = BORSH_ENCODING(ProofOperation);

// Materialize already-authenticated original bytes; this is not a verifier.
export const materializeProofEntry = (bytes: Uint8Array, cid: string) => {
	const entry = deserialize(bytes, Entry) as Entry<ProofOperation>;
	entry.hash = Entry.prepareMultihashBytes(entry, bytes, cid);
	entry.init({ encoding: BORSH_ENCODING_PROOF_OPERATION });
	return entry;
};

const copyWitnesses = (supplied: readonly Uint8Array[]): Uint8Array[] => {
	const count = supplied.length;
	requireFact(
		Number.isInteger(count) && count > 0 && count <= MAX_PROOF_HISTORY_BLOCKS,
		"Proof history block limit exceeded",
	);
	const blocks: Uint8Array[] = [];
	let total = 0;
	for (let i = 0; i < count; i++) {
		const bytes = copyBytes(supplied[i]!, MAX_BATCH_SIZE - total);
		total += bytes.byteLength;
		blocks.push(bytes);
	}
	return blocks;
};

export type OriginalProofEntry = { cid: string; bytes: Uint8Array };

// ExchangeHeads carries EntryV0 with an assigned hash, unlike stored originals.
// Snapshot only its raw signed envelopes, never decoded caches or caller methods.
// The candidate becomes authoritative only after CID and profile verification.
const captureTransportCandidate = (
	input: Entry<ProofOperation>,
): Uint8Array => {
	requireFact(
		input.constructor === EntryV0,
		"Unsupported proof transport entry",
	);
	const entry = input as EntryV0<ProofOperation>;
	const signatures = entry._signatures;
	requireFact(
		signatures?.constructor === Signatures &&
			signatures.signatures.length === 1,
		"Expected one proof transport signature",
	);
	let remaining = MAX_BATCH_SIZE;
	const envelope = <T>(value: unknown): DecryptedThing<T> => {
		requireFact(
			value instanceof DecryptedThing && value.constructor === DecryptedThing,
			"Expected plaintext proof transport envelope",
		);
		const raw = value._data;
		requireFact(raw, "Missing proof transport envelope bytes");
		const data = copyBytes(raw, remaining);
		remaining -= data.byteLength;
		return new DecryptedThing<T>({ data });
	};
	const candidate = new EntryV0<ProofOperation>({
		meta: envelope(entry._meta),
		payload: envelope(entry._payload),
		signatures: new Signatures({
			signatures: [envelope(signatures.signatures[0])],
		}),
		reserved: copyBytes(entry._reserved!, 4),
	});
	return copyBytes(serialize(candidate));
};

export type VerifiedProofPut = {
	hash: string;
	bytes: Uint8Array;
	key: IdKey;
	data: Uint8Array;
	next: string[];
	gid: string;
	wallTime: bigint;
	logical: number;
};
export type VerifiedProofHistory = {
	targets: VerifiedProofPut[];
	boundaries: VerifiedProofPut[];
};
const clockAfter = (
	child: { wallTime: bigint; logical: number },
	parent: { wallTime: bigint; logical: number },
) =>
	child.wallTime > parent.wallTime ||
	(child.wallTime === parent.wallTime && child.logical > parent.logical);

/**
 * Captures the immutable V1 protocol descriptor; outer Documents/index objects
 * are views, not alternate authorization or key policies. Returned facts own
 * their bytes, but are not immutable;
 * they are internal protocol facts, never application-callback scratch space.
 * This authenticates contents/authority, not historical admission or durability.
 */
export const createProofOperationVerifier = (properties: {
	contract: ProofDeleteContract;
}) => {
	const contract = copyProofDeleteContract(properties.contract);
	const { owner, keyKind } = contract;
	const scope = deriveProofDeleteScope(contract);
	const checkKey = (key: IdKey) => {
		requireFact(
			key.constructor === (keyKind === 0 ? StringKey : Uint8ArrayKey),
			"Wrong proof key kind",
		);
	};
	const authenticate = async (bytes: Uint8Array, hash: string) => {
		preflightEntry(bytes);
		const entry = deserialize(bytes, Entry) as Entry<ProofOperation>;
		requireFact(
			entry instanceof EntryV0 && entry.constructor === EntryV0,
			"Unsupported proof entry",
		);
		requireFact(entry.hash === undefined, "Embedded proof hash");
		requireFact(equals(serialize(entry), bytes), "Noncanonical proof entry");
		const envelopes = entry._signatures?.signatures;
		requireFact(
			entry._meta instanceof DecryptedThing &&
				entry._meta.constructor === DecryptedThing &&
				entry._payload instanceof DecryptedThing &&
				entry._payload.constructor === DecryptedThing &&
				entry._signatures?.constructor === Signatures &&
				envelopes?.length === 1 &&
				envelopes[0] instanceof DecryptedThing &&
				envelopes[0].constructor === DecryptedThing,
			"Expected plaintext entry with one Ed25519 owner signature",
		);
		const envelope = envelopes[0];
		const signature = envelope.getValue(SignatureWithKey);
		requireFact(
			signature.constructor === SignatureWithKey &&
				signature.publicKey instanceof Ed25519PublicKey &&
				signature.publicKey.constructor === Ed25519PublicKey &&
				equals(signature.publicKey.publicKey, owner) &&
				signature.prehash === PreHash.NONE &&
				signature.signature.byteLength === 64 &&
				envelope._data != null &&
				equals(serialize(signature), envelope._data),
			"Expected canonical Ed25519 owner signature without prehash",
		);
		entry.init({ encoding: BORSH_ENCODING_PROOF_OPERATION });
		requireFact(
			entry._meta._data != null &&
				entry.meta.constructor === Meta &&
				equals(serialize(entry.meta), entry._meta._data) &&
				entry._payload._data != null &&
				equals(serialize(entry.payload), entry._payload._data),
			"Noncanonical proof metadata or payload",
		);
		requireFact(
			entry.meta.next.length <= MAX_PROOF_HISTORY_EDGES,
			"Proof history edge limit exceeded",
		);
		requireFact(await entry.verifySignatures(), "Invalid proof signature");
		const operation = await entry.getPayloadValue();
		requireFact(
			operation instanceof ProofPutOperation ||
				operation instanceof ProofDeleteOperation,
			"Unsupported proof operation",
		);
		requireFact(
			equals(serialize(operation), entry.payload.data),
			"Noncanonical proof operation",
		);
		requireFact(equals(operation.scope, scope), "Wrong proof store scope");
		checkKey(operation.key);
		requireFact(
			entry.meta.gid === deriveProofKeyGid(scope, operation.key),
			"Wrong proof key gid",
		);
		entry.hash = hash;
		return { entry, operation };
	};
	const authenticatePut = async (
		bytes: Uint8Array,
		hash: string,
	): Promise<VerifiedProofPut> => {
		const { entry, operation } = await authenticate(bytes, hash);
		requireFact(
			entry.meta.type === EntryType.APPEND &&
				operation instanceof ProofPutOperation,
			"Expected proof APPEND/PUT",
		);
		return {
			hash,
			bytes,
			key: operation.key,
			data: operation.data,
			next: [...entry.meta.next],
			gid: entry.meta.gid,
			wallTime: entry.meta.clock.timestamp.wallTime,
			logical: entry.meta.clock.timestamp.logical,
		};
	};
	const verifyHistory = async (
		target: string,
		key: IdKey,
		blocks: Uint8Array[],
	): Promise<VerifiedProofHistory> => {
		checkKey(key);
		const declaredKey = keyBytes(key);
		const hashes = await calculateRawCidV1Batch(blocks);
		const byHash = new Map<string, VerifiedProofPut>();
		let edges = 0;
		for (let i = 0; i < blocks.length; i++) {
			const hash = hashes[i]!;
			requireFact(!byHash.has(hash), "Duplicate proof witness");
			const fact = await authenticatePut(blocks[i]!, hash);
			edges += fact.next.length;
			requireFact(
				edges <= MAX_PROOF_HISTORY_EDGES,
				"Proof history edge limit exceeded",
			);
			byHash.set(hash, fact);
		}
		const root = byHash.get(target);
		requireFact(root, "Missing proof target witness");
		requireFact(
			equals(keyBytes(root.key), declaredKey),
			"Wrong proof target key",
		);
		const pending = [target];
		const visited = new Set<string>();
		const targets: VerifiedProofPut[] = [];
		const boundaries: VerifiedProofPut[] = [];
		while (pending.length > 0) {
			const hash = pending.pop()!;
			if (visited.has(hash)) continue;
			const fact = byHash.get(hash);
			requireFact(fact, "Missing proof ancestry witness");
			visited.add(hash);
			if (!equals(keyBytes(fact.key), declaredKey)) {
				boundaries.push(fact);
				continue;
			}
			targets.push(fact);
			for (const hash of fact.next) {
				const parent = byHash.get(hash);
				requireFact(parent, "Missing proof ancestry witness");
				requireFact(clockAfter(fact, parent), "Noncausal proof PUT edge");
			}
			pending.push(...fact.next.slice().sort().reverse());
		}
		requireFact(visited.size === byHash.size, "Unused proof witness");
		return { targets, boundaries };
	};
	const captureBlock = (block: OriginalProofEntry) => ({
		cid: block.cid,
		bytes: copyBytes(block.bytes),
	});
	const authenticateBlock = async (block: OriginalProofEntry) => {
		const [hash] = await calculateRawCidV1Batch([block.bytes]);
		requireFact(block.cid === hash, "Proof CID mismatch");
		return hash!;
	};
	const deletionHistory = async (
		entry: Entry<ProofOperation>,
		operation: ProofDeleteOperation,
	) => {
		const history = await verifyHistory(
			operation.target,
			operation.key,
			copyWitnesses(operation.witnesses),
		);
		const expected = history.targets.map((fact) => fact.hash).sort();
		for (const parent of history.targets) {
			requireFact(
				clockAfter(entry.meta.clock.timestamp, parent),
				"Noncausal proof CUT edge",
			);
		}
		requireFact(
			entry.meta.next.length === expected.length &&
				expected.every((hash, i) => entry.meta.next[i] === hash),
			"Proof CUT must name all exact targets in canonical order",
		);
		return history;
	};
	return {
		/**
		 * Captures incoming bytes before parent traversal. Prepared/local originals
		 * take priority; a decoded transport object supplies only a reconstructed
		 * canonical candidate. Every source is independently CID/profile checked,
		 * with complete CUT evidence validation, not trusted validation hints.
		 */
		async incoming(input: {
			entry: Entry<ProofOperation>;
			resolveLocal?: (hash: string) => Promise<Uint8Array | undefined>;
		}): Promise<OriginalProofEntry> {
			const { entry: supplied, resolveLocal } = input;
			const cid = supplied.hash;
			requireFact(
				typeof cid === "string" && cid.length > 0,
				"Missing proof CID",
			);
			const prepared = Entry.getPreparedStorageBytes(supplied);
			// Both alternatives must be owned before a local lookup can yield.
			let bytes = prepared
				? copyBytes(prepared)
				: captureTransportCandidate(supplied);
			if (!prepared && resolveLocal) {
				const stored = await resolveLocal(cid);
				if (stored !== undefined) {
					bytes = copyBytes(stored);
				}
			}
			const block = { cid, bytes };
			const { entry, operation } = await authenticate(
				bytes,
				await authenticateBlock(block),
			);
			if (operation instanceof ProofPutOperation) {
				requireFact(
					entry.meta.type === EntryType.APPEND,
					"Expected proof APPEND/PUT",
				);
			} else {
				requireFact(
					entry.meta.type === EntryType.CUT,
					"Expected proof CUT/DELETE",
				);
				await deletionHistory(entry, operation);
			}
			return block;
		},
		/** Parent kind/scope must also be resolved at ordinary admission. */
		async put(input: OriginalProofEntry): Promise<VerifiedProofPut> {
			const block = captureBlock(input);
			return authenticatePut(block.bytes, await authenticateBlock(block));
		},
		/** Preparation only: no store write, retirement or receipt is performed. */
		async history(input: {
			target: string;
			key: IdKey;
			witnesses: readonly Uint8Array[];
		}): Promise<VerifiedProofHistory> {
			const target = input.target;
			const key = copyKey(input.key);
			const blocks = copyWitnesses(input.witnesses);
			return verifyHistory(target, key, blocks);
		},
		/**
		 * Collects original PUT bytes sequentially, without admitting ancestors.
		 * A resolver may read local blocks or retained authenticated witnesses;
		 * this proves neither their historical admission nor durable retention.
		 */
		async collectHistory(input: {
			target: string;
			key: IdKey;
			resolve: (
				hash: string,
				signal?: AbortSignal,
			) => Promise<Uint8Array | undefined>;
			signal?: AbortSignal;
		}): Promise<VerifiedProofHistory> {
			const { target, resolve, signal } = input;
			const key = copyKey(input.key);
			checkKey(key);
			const declaredKey = keyBytes(key);
			const pending = [target];
			const scheduled = new Set(pending);
			const blocks: Uint8Array[] = [];
			let totalBytes = 0;
			let edges = 0;
			while (pending.length > 0) {
				signal?.throwIfAborted();
				const hash = pending.pop()!;
				const supplied = await resolve(hash, signal);
				signal?.throwIfAborted();
				requireFact(supplied, "Missing proof ancestry witness");
				// Own each result before hashing/signature verification can yield.
				const bytes = copyBytes(supplied, MAX_BATCH_SIZE - totalBytes);
				totalBytes += bytes.byteLength;
				const fact = await authenticatePut(
					bytes,
					await authenticateBlock({ cid: hash, bytes }),
				);
				signal?.throwIfAborted();
				edges += fact.next.length;
				requireFact(
					edges <= MAX_PROOF_HISTORY_EDGES,
					"Proof history edge limit exceeded",
				);
				blocks.push(bytes);
				const sameKey = equals(keyBytes(fact.key), declaredKey);
				requireFact(hash !== target || sameKey, "Wrong proof target key");
				if (!sameKey) continue;
				for (const next of fact.next.slice().sort().reverse()) {
					if (scheduled.has(next)) continue;
					requireFact(
						scheduled.size < MAX_PROOF_HISTORY_BLOCKS,
						"Proof history block limit exceeded",
					);
					scheduled.add(next);
					pending.push(next);
				}
			}
			// Apply the same complete-closure contract as caller-supplied evidence.
			const history = await verifyHistory(target, key, blocks);
			signal?.throwIfAborted();
			return history;
		},
		async deletion(input: OriginalProofEntry): Promise<
			VerifiedProofHistory & {
				hash: string;
				bytes: Uint8Array;
				key: IdKey;
				target: string;
			}
		> {
			// Full encoded entry is capped before parsing/copying embedded witnesses.
			const block = captureBlock(input);
			const hash = await authenticateBlock(block);
			const { entry, operation } = await authenticate(block.bytes, hash);
			requireFact(
				entry.meta.type === EntryType.CUT &&
					operation instanceof ProofDeleteOperation,
				"Expected proof CUT/DELETE",
			);
			const history = await deletionHistory(entry, operation);
			return {
				...history,
				hash,
				bytes: block.bytes,
				key: operation.key,
				target: operation.target,
			};
		},
	};
};
