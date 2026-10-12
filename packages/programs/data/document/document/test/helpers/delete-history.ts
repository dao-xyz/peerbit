import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	DecryptedThing,
	Ed25519PublicKey,
	PreHash,
	SignatureWithKey,
} from "@peerbit/crypto";
import { IdKey } from "@peerbit/indexer-interface";
import {
	Entry,
	EntryType,
	EntryV0,
	calculateRawCidV1Batch,
} from "@peerbit/log";
import { equals } from "uint8arrays";
import {
	BORSH_ENCODING_OPERATION,
	type Operation,
	PutOperation,
} from "../../src/operation.js";

// Staged implementation, excluded from npm with the rest of test/. These are
// aggregate verifier budgets, NOT an allocated wire format or a transport limit.
// Full encoded delete size, store binding, authorization and durable retirement
// must be integrated before this can become a production deletion path.
export const MAX_DELETE_HISTORY_BYTES = 5_000_000;
export const MAX_DELETE_HISTORY_BLOCKS = 256;
export const MAX_DELETE_HISTORY_EDGES = 1024;

function requireFact(condition: unknown, message: string): asserts condition {
	if (!condition) throw new Error(message);
}

const copyWitnessBytes = (source: Uint8Array, remainingBytes: number) => {
	requireFact(source instanceof Uint8Array, "Expected original block bytes");
	// An own byteLength property must not bypass the pre-allocation budget. Copy
	// into a bounded destination so a concurrently grown view cannot enlarge it.
	const size = Reflect.get(
		Uint8Array.prototype,
		"byteLength",
		source,
	) as number;
	requireFact(size <= remainingBytes, "Delete history byte limit exceeded");
	const copy = new Uint8Array(size);
	Uint8Array.prototype.set.call(copy, source);
	return copy;
};

const decodeCanonicalWitness = (bytes: Uint8Array): Entry<Operation> => {
	const entry = deserialize(bytes, Entry) as Entry<Operation>;
	requireFact(entry instanceof EntryV0, "Unsupported entry");
	requireFact(
		entry.hash === undefined,
		"Embedded hash is not canonical stored evidence",
	);
	requireFact(equals(serialize(entry), bytes), "Noncanonical entry bytes");
	requireFact(
		entry._meta instanceof DecryptedThing &&
			entry._payload instanceof DecryptedThing &&
			entry._signatures != null &&
			entry._signatures.signatures.length > 0 &&
			entry._signatures.signatures.every((s) => s instanceof DecryptedThing),
		"Expected plaintext signed evidence",
	);
	entry.init({ encoding: BORSH_ENCODING_OPERATION });
	return entry;
};

/** Authenticate copied original bytes; never trust an assigned entry.hash. */
export const authenticateCanonicalDeleteWitness = async (block: {
	cid: string;
	bytes: Uint8Array;
}): Promise<Entry<Operation>> => {
	const claimedCid = block.cid;
	const suppliedBytes = block.bytes;
	const bytes = copyWitnessBytes(suppliedBytes, MAX_DELETE_HISTORY_BYTES);
	const [cid] = await calculateRawCidV1Batch([bytes]);
	requireFact(cid === claimedCid, "CID mismatch");
	const entry = decodeCanonicalWitness(bytes);
	requireFact(await entry.verifySignatures(), "Invalid signature");
	entry.hash = cid;
	return entry;
};

/**
 * Private reference profile, applied to ordinary puts AND deletion evidence.
 * Call after authentication. This is not a generic unique-identity guarantee,
 * historical admission proof, or permission to delete an authenticated value.
 */
export const assertSupportedDeleteWitnessSignature = (
	entry: Entry<Operation>,
	owner: Uint8Array,
): void => {
	requireFact(entry instanceof EntryV0, "Unsupported entry");
	const envelopes = entry._signatures?.signatures;
	requireFact(
		envelopes?.length === 1 && envelopes[0] instanceof DecryptedThing,
		"Expected one plaintext Ed25519 owner signature",
	);
	const envelope = envelopes[0];
	const signature = envelope.getValue(SignatureWithKey);
	requireFact(
		signature.publicKey instanceof Ed25519PublicKey &&
			signature.prehash === PreHash.NONE &&
			signature.signature.byteLength === 64 &&
			equals(signature.publicKey.bytes, owner) &&
			envelope._data != null &&
			equals(serialize(signature), envelope._data),
		"Expected canonical Ed25519 owner signature without prehash",
	);
};

export type VerifiedDeleteHistoryPut = {
	hash: string;
	bytes: Uint8Array;
	key: IdKey;
};

/**
 * Resolve the complete continuous same-key put ancestry from original blocks.
 * This authenticates content/authorship, NOT historical admission or permission
 * to delete. The caller must verify the signed delete/store binding and authorize
 * EVERY returned target before making any mutation. Returned facts are owned,
 * not immutable: detach them before passing them to untrusted callbacks.
 *
 * Missing ancestors are unresolved. Prior proof reuse must supply their original
 * authenticated closure; there is deliberately no bare-CID retirement allowlist.
 */
export const verifyDocumentDeleteHistory = async (properties: {
	target: string;
	key: IdKey;
	blocks: readonly Uint8Array[];
	resolveKey: (copiedPutPayload: Uint8Array) => IdKey;
}): Promise<{
	targets: VerifiedDeleteHistoryPut[];
	boundaries: VerifiedDeleteHistoryPut[];
}> => {
	const target = properties.target;
	const keyBytes = new Uint8Array(serialize(properties.key));
	const resolveKey = properties.resolveKey;
	const supplied = properties.blocks;
	const blockCount = supplied.length;
	requireFact(
		Number.isInteger(blockCount) &&
			blockCount > 0 &&
			blockCount <= MAX_DELETE_HISTORY_BLOCKS,
		"Delete history block limit exceeded",
	);
	let totalBytes = 0;
	const blocks: Uint8Array[] = [];
	// Take ownership of ALL evidence before yielding, with limits before copying.
	for (let i = 0; i < blockCount; i++) {
		const block = copyWitnessBytes(
			supplied[i]!,
			MAX_DELETE_HISTORY_BYTES - totalBytes,
		);
		totalBytes += block.byteLength;
		blocks.push(block);
	}
	const hashes = await calculateRawCidV1Batch(blocks);
	const byHash = new Map<
		string,
		{ fact: VerifiedDeleteHistoryPut; next: string[]; sameKey: boolean }
	>();
	let totalEdges = 0;
	for (let i = 0; i < blocks.length; i++) {
		const hash = hashes[i]!;
		requireFact(!byHash.has(hash), "Duplicate history witness");
		const bytes = blocks[i]!;
		const entry = decodeCanonicalWitness(bytes);
		const next = [...entry.meta.next];
		totalEdges += next.length;
		requireFact(
			totalEdges <= MAX_DELETE_HISTORY_EDGES,
			"Delete history edge limit exceeded",
		);
		requireFact(await entry.verifySignatures(), "Invalid signature");
		const operation = await entry.getPayloadValue();
		requireFact(
			entry.meta.type === EntryType.APPEND && operation instanceof PutOperation,
			"Expected put witness",
		);
		// The application decoder/resolver gets a copy, never the signed witness.
		const resolvedKeyBytes = serialize(
			resolveKey(new Uint8Array(operation.data)),
		);
		const key = deserialize(resolvedKeyBytes, IdKey);
		byHash.set(hash, {
			fact: { hash, bytes, key },
			next,
			sameKey: equals(resolvedKeyBytes, keyBytes),
		});
	}

	const root = byHash.get(target);
	requireFact(root, "Missing target witness");
	requireFact(root.sameKey, "Wrong target key");
	const pending = [target];
	const visited = new Set<string>();
	const targets: VerifiedDeleteHistoryPut[] = [];
	const boundaries: VerifiedDeleteHistoryPut[] = [];
	while (pending.length > 0) {
		const hash = pending.pop()!;
		if (visited.has(hash)) continue;
		const candidate = byHash.get(hash);
		requireFact(candidate, "Missing ancestry witness");
		visited.add(hash);
		if (!candidate.sameKey) {
			boundaries.push(candidate.fact);
			continue;
		}
		targets.push(candidate.fact);
		// Evidence-array order must not change the retirement order. Traversal is
		// iterative and shared diamond ancestors are processed only once.
		pending.push(...candidate.next.slice().sort().reverse());
	}
	requireFact(visited.size === byHash.size, "Unused history witness");
	return { targets, boundaries };
};
