import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	calculateRawCid,
	cidifyString,
	codecMap,
	defaultHasher,
	stringifyCid,
} from "@peerbit/blocks-interface";
import { DecryptedThing } from "@peerbit/crypto";
import { NO_ENCODING } from "./encoding.js";
import { EntryV0, Meta } from "./entry-v0.js";
import { Entry } from "./entry.js";

type MaybePromise<T> = T | Promise<T>;

export type BoundedEntryV0CausalReachabilityLimits = {
	/** Maximum bytes accepted for any one EntryV0 block. */
	maxEntryBytes: number;
	/** Maximum direct-parent strings decoded from any one EntryV0 block. */
	maxDirectParents: number;
	/** Maximum EntryV0 CIDs admitted to traversal, including the descendant. */
	maxVisitedEntries: number;
	/** Maximum captured EntryV0 bytes across the traversal. */
	maxTotalBytes: number;
	/** Maximum direct-parent strings examined across the traversal. */
	maxParentLinks: number;
	/** Maximum sorted CIDs supplied to the resolver in one call. */
	maxResolveBatchSize: number;
};

export type BoundedEntryV0CausalReachabilityResolver = (
	cids: readonly string[],
	options: { signal: AbortSignal },
) => MaybePromise<ReadonlyMap<string, Uint8Array | undefined>>;

export type BoundedEntryV0CausalReachabilityVisited = {
	/** EntryV0 CIDs admitted to traversal, including the descendant. */
	entries: number;
	bytes: number;
	parentLinks: number;
	resolverCalls: number;
};

type BoundedEntryV0CausalReachabilityBase = {
	visited: BoundedEntryV0CausalReachabilityVisited;
};

export type BoundedEntryV0CausalReachabilityResult =
	| (BoundedEntryV0CausalReachabilityBase & { status: "ancestor" })
	| (BoundedEntryV0CausalReachabilityBase & { status: "not-ancestor" })
	| (BoundedEntryV0CausalReachabilityBase & {
			status: "incomplete";
			missingCids: string[];
	  })
	| (BoundedEntryV0CausalReachabilityBase & { status: "capacity" });

export type BoundedEntryV0CausalReachabilityInput = {
	ancestorCid: string;
	descendant: { cid: string; bytes: Uint8Array };
	resolve: BoundedEntryV0CausalReachabilityResolver;
	limits: BoundedEntryV0CausalReachabilityLimits;
	signal?: AbortSignal;
};

const TYPED_ARRAY_PROTOTYPE = Object.getPrototypeOf(Uint8Array.prototype);
const TYPED_ARRAY_BYTE_LENGTH = Object.getOwnPropertyDescriptor(
	TYPED_ARRAY_PROTOTYPE,
	"byteLength",
)!.get!;
const TYPED_ARRAY_TAG = Object.getOwnPropertyDescriptor(
	TYPED_ARRAY_PROTOTYPE,
	Symbol.toStringTag,
)!.get!;
const ARRAY_BUFFER_IS_VIEW = ArrayBuffer.isView;
const UINT8_ARRAY_SET = Uint8Array.prototype.set;
const REFLECT_APPLY = Reflect.apply;
const MINIMUM_MAYBE_ENCRYPTED_BYTES = 6;
const MINIMUM_KEY_RECIPIENT_BYTES = 43;
const MAX_CANONICAL_ENTRY_CID_CHARACTERS = 128;

class CapacityError extends Error {}

const compareCanonicalCids = (left: string, right: string): number =>
	left < right ? -1 : left > right ? 1 : 0;

/**
 * Deterministic frontier ordering without repeatedly sorting the full pending
 * set after every resolver batch. Canonical EntryV0 CIDs are ASCII strings, so
 * relational ordering is identical to Array.sort()'s prior default order.
 */
class CanonicalCidMinHeap {
	private readonly values: string[] = [];

	get size(): number {
		return this.values.length;
	}

	push(value: string): void {
		let index = this.values.length;
		this.values.push(value);
		while (index > 0) {
			const parentIndex = Math.floor((index - 1) / 2);
			const parent = this.values[parentIndex]!;
			if (compareCanonicalCids(parent, value) <= 0) break;
			this.values[index] = parent;
			index = parentIndex;
		}
		this.values[index] = value;
	}

	popBatch(maximum: number): string[] {
		const batch: string[] = [];
		const length = Math.min(maximum, this.values.length);
		for (let i = 0; i < length; i++) {
			batch.push(this.pop()!);
		}
		return batch;
	}

	snapshot(): string[] {
		return [...this.values];
	}

	private pop(): string | undefined {
		if (this.values.length === 0) return undefined;
		const first = this.values[0]!;
		const last = this.values.pop()!;
		if (this.values.length === 0) return first;

		let index = 0;
		while (true) {
			const leftIndex = index * 2 + 1;
			if (leftIndex >= this.values.length) break;
			const rightIndex = leftIndex + 1;
			const childIndex =
				rightIndex < this.values.length &&
				compareCanonicalCids(
					this.values[rightIndex]!,
					this.values[leftIndex]!,
				) < 0
					? rightIndex
					: leftIndex;
			const child = this.values[childIndex]!;
			if (compareCanonicalCids(child, last) >= 0) break;
			this.values[index] = child;
			index = childIndex;
		}
		this.values[index] = last;
		return first;
	}
}

class BoundsReader {
	private offset = 0;
	private readonly view: DataView;

	constructor(private readonly bytes: Uint8Array) {
		this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
	}

	get remaining(): number {
		return this.bytes.byteLength - this.offset;
	}

	readU8(label: string): number {
		return this.readExact(1, label)[0]!;
	}

	readU32(label: string): number {
		this.requireRemaining(4, label);
		const value = this.view.getUint32(this.offset, true);
		this.offset += 4;
		return value;
	}

	readBytes(label: string): Uint8Array {
		return this.readExact(this.readU32(`${label} length`), label);
	}

	readExact(byteLength: number, label: string): Uint8Array {
		this.requireRemaining(byteLength, label);
		const start = this.offset;
		this.offset += byteLength;
		return this.bytes.subarray(start, this.offset);
	}

	readBoolean(label: string): boolean {
		const value = this.readU8(label);
		if (value !== 0 && value !== 1) {
			throw new Error(`EntryV0 has an invalid ${label}`);
		}
		return value === 1;
	}

	expectU8(expected: number, label: string): void {
		if (this.readU8(label) !== expected) {
			throw new Error(`EntryV0 has an invalid ${label}`);
		}
	}

	expectDone(label: string): void {
		if (this.remaining !== 0) {
			throw new Error(`EntryV0 has trailing ${label} bytes`);
		}
	}

	assertInputBackedCount(
		count: number,
		minimumItemBytes: number,
		label: string,
	): void {
		if (count > Math.floor(this.remaining / minimumItemBytes)) {
			throw new Error(`EntryV0 has an unbacked ${label}`);
		}
	}

	private requireRemaining(byteLength: number, label: string): void {
		if (
			!Number.isSafeInteger(byteLength) ||
			byteLength < 0 ||
			byteLength > this.remaining
		) {
			throw new Error(`EntryV0 has truncated ${label}`);
		}
	}
}

const scanX25519PublicKey = (reader: BoundsReader, label: string): void => {
	reader.expectU8(0, `${label} public-key variant`);
	reader.readExact(32, `${label} public key`);
};

const scanEncryptedThing = (reader: BoundsReader, label: string): void => {
	reader.readBytes(`${label} ciphertext`);
	reader.readBytes(`${label} nonce`);
	const envelopeVariant = reader.readU8(`${label} key-envelope variant`);
	if (envelopeVariant === 1) {
		reader.readExact(32, `${label} key hash`);
		return;
	}
	if (envelopeVariant !== 0) {
		throw new Error(`EntryV0 has an unsupported ${label} key envelope`);
	}

	scanX25519PublicKey(reader, `${label} sender`);
	const recipientCount = reader.readU32(`${label} key-recipient count`);
	reader.assertInputBackedCount(
		recipientCount,
		MINIMUM_KEY_RECIPIENT_BYTES,
		`${label} key-recipient count`,
	);
	for (let i = 0; i < recipientCount; i++) {
		reader.expectU8(0, `${label} key-recipient variant`);
		reader.expectU8(0, `${label} encrypted-key variant`);
		reader.readBytes(`${label} encrypted-key nonce`);
		reader.readBytes(`${label} encrypted key`);
		scanX25519PublicKey(reader, `${label} recipient`);
	}
};

const scanMaybeEncrypted = (
	reader: BoundsReader,
	label: string,
	requirePublic: boolean,
): Uint8Array | undefined => {
	reader.expectU8(0, `${label} MaybeEncrypted variant`);
	const variant = reader.readU8(`${label} variant`);
	if (variant === 0) {
		return reader.readBytes(label);
	}
	if (variant !== 1) {
		throw new Error(`EntryV0 has an unsupported ${label} variant`);
	}
	if (requirePublic) {
		throw new Error(`EntryV0 ${label} must be public`);
	}
	scanEncryptedThing(reader, label);
	return undefined;
};

const scanEntryV0Storage = (bytes: Uint8Array): Uint8Array => {
	const reader = new BoundsReader(bytes);
	reader.expectU8(0, "entry variant");
	const metaBytes = scanMaybeEncrypted(reader, "metadata", true)!;
	scanMaybeEncrypted(reader, "payload", false);
	reader.readExact(4, "reserved bytes");
	if (reader.readBoolean("signatures option")) {
		reader.expectU8(0, "signatures variant");
		const signatureCount = reader.readU32("signature count");
		reader.assertInputBackedCount(
			signatureCount,
			MINIMUM_MAYBE_ENCRYPTED_BYTES,
			"signature count",
		);
		for (let i = 0; i < signatureCount; i++) {
			scanMaybeEncrypted(reader, "signature", false);
		}
	}
	if (reader.readBoolean("hash option")) {
		reader.readBytes("hash");
	}
	reader.expectDone("storage");
	return metaBytes;
};

const scanMetaParentCount = (
	metaBytes: Uint8Array,
	maximumParents: number,
	chargeParentCount: (parentCount: number) => void,
): number => {
	const reader = new BoundsReader(metaBytes);
	reader.expectU8(0, "metadata variant");
	reader.expectU8(0, "clock variant");
	reader.readBytes("clock id");
	reader.expectU8(0, "timestamp variant");
	reader.readExact(8, "timestamp wall time");
	reader.readExact(4, "timestamp logical time");
	reader.readBytes("gid");

	const parentCount = reader.readU32("direct-parent count");
	reader.assertInputBackedCount(parentCount, 4, "direct-parent count");
	if (parentCount > maximumParents) {
		throw new CapacityError("EntryV0 direct-parent capacity exceeded");
	}
	chargeParentCount(parentCount);
	for (let i = 0; i < parentCount; i++) {
		reader.readBytes("direct parent");
	}
	reader.readU8("entry type");
	if (reader.readBoolean("metadata data option")) {
		reader.readBytes("metadata data");
	}
	reader.expectDone("metadata");
	return parentCount;
};

const bytesEqual = (left: Uint8Array, right: Uint8Array): boolean => {
	if (left.byteLength !== right.byteLength) return false;
	for (let i = 0; i < left.byteLength; i++) {
		if (left[i] !== right[i]) return false;
	}
	return true;
};

const readCanonicalEntryV0Parents = (
	bytes: Uint8Array,
	maximumParents: number,
	chargeParentCount: (parentCount: number) => void,
): string[] => {
	const metaBytes = scanEntryV0Storage(bytes);
	const parentCount = scanMetaParentCount(
		metaBytes,
		maximumParents,
		chargeParentCount,
	);
	const entry = deserialize(bytes, Entry);
	if (!(entry instanceof EntryV0) || !(entry._meta instanceof DecryptedThing)) {
		throw new Error("Block is not a public-metadata EntryV0");
	}
	if (!bytesEqual(bytes, serialize(entry))) {
		throw new Error("EntryV0 storage is not canonical");
	}
	entry.init({ encoding: NO_ENCODING });
	const meta = entry.meta;
	if (!(meta instanceof Meta) || !bytesEqual(metaBytes, serialize(meta))) {
		throw new Error("EntryV0 metadata is not canonical");
	}
	if (meta.next.length !== parentCount) {
		throw new Error("EntryV0 direct-parent framing does not match metadata");
	}
	return meta.next;
};

const assertPositiveSafeInteger = (value: number, label: string): void => {
	if (!Number.isSafeInteger(value) || value < 1) {
		throw new RangeError(`${label} must be a positive safe integer`);
	}
};

const assertLimits = (limits: BoundedEntryV0CausalReachabilityLimits): void => {
	assertPositiveSafeInteger(limits.maxEntryBytes, "maxEntryBytes");
	assertPositiveSafeInteger(limits.maxDirectParents, "maxDirectParents");
	assertPositiveSafeInteger(limits.maxVisitedEntries, "maxVisitedEntries");
	assertPositiveSafeInteger(limits.maxTotalBytes, "maxTotalBytes");
	assertPositiveSafeInteger(limits.maxParentLinks, "maxParentLinks");
	assertPositiveSafeInteger(limits.maxResolveBatchSize, "maxResolveBatchSize");
};

const assertCanonicalEntryCid = (cid: string, label: string): void => {
	if (
		typeof cid !== "string" ||
		cid.length === 0 ||
		cid.length > MAX_CANONICAL_ENTRY_CID_CHARACTERS
	) {
		throw new TypeError(`${label} must be a canonical CIDv1/raw/sha2-256 CID`);
	}
	let parsed: ReturnType<typeof cidifyString>;
	try {
		parsed = cidifyString(cid);
	} catch {
		throw new TypeError(`${label} must be a canonical CIDv1/raw/sha2-256 CID`);
	}
	if (
		parsed.version !== 1 ||
		parsed.code !== codecMap.raw.code ||
		parsed.multihash.code !== defaultHasher.code ||
		parsed.multihash.digest.byteLength !== 32 ||
		stringifyCid(parsed) !== cid
	) {
		throw new TypeError(`${label} must be a canonical CIDv1/raw/sha2-256 CID`);
	}
};

const isCanonicalEntryCid = (cid: string): boolean => {
	try {
		assertCanonicalEntryCid(cid, "EntryV0 direct parent");
		return true;
	} catch {
		return false;
	}
};

const exactUint8ArrayByteLength = (value: unknown): number | undefined => {
	try {
		if (
			!ARRAY_BUFFER_IS_VIEW(value) ||
			TYPED_ARRAY_TAG.call(value) !== "Uint8Array"
		) {
			return undefined;
		}
		return TYPED_ARRAY_BYTE_LENGTH.call(value) as number;
	} catch {
		return undefined;
	}
};

const copyUint8Array = (
	value: unknown,
	byteLength: number,
): Uint8Array | undefined => {
	const copy = new Uint8Array(byteLength);
	try {
		UINT8_ARRAY_SET.call(copy, value as Uint8Array);
		return copy;
	} catch {
		return undefined;
	}
};

const cloneVisited = (
	visited: BoundedEntryV0CausalReachabilityVisited,
): BoundedEntryV0CausalReachabilityVisited => ({ ...visited });

const callResolver = async (
	resolve: BoundedEntryV0CausalReachabilityResolver,
	cids: readonly string[],
	signal: AbortSignal,
): Promise<ReadonlyMap<string, Uint8Array | undefined>> => {
	if (signal.aborted) throw new Error("Traversal aborted");
	return new Promise((resolveResult, rejectResult) => {
		let settled = false;
		const cleanup = (): boolean => {
			if (settled) return false;
			settled = true;
			signal.removeEventListener("abort", onAbort);
			return true;
		};
		const onAbort = () => {
			if (cleanup()) rejectResult(new Error("Traversal aborted"));
		};
		signal.addEventListener("abort", onAbort, { once: true });
		if (signal.aborted) {
			onAbort();
			return;
		}
		try {
			Promise.resolve(resolve(cids, { signal })).then(
				(value) => {
					if (cleanup()) resolveResult(value);
				},
				(error) => {
					if (cleanup()) rejectResult(error);
				},
			);
		} catch (error) {
			if (cleanup()) rejectResult(error);
		}
	});
};

/**
 * Check whether `ancestorCid` is a strict transitive ancestor of a captured
 * raw EntryV0 descendant without trusting resolver bytes. Every traversed
 * block is bounded and matched to its canonical CID before public `meta.next`
 * is decoded. This establishes hash-linked causality only; it does not verify
 * signatures, authorization, arrival order, policy, or durability.
 */
export const checkBoundedEntryV0CausalReachability = async (
	input: BoundedEntryV0CausalReachabilityInput,
): Promise<BoundedEntryV0CausalReachabilityResult> => {
	const ancestorCid = input.ancestorCid;
	const descendantInput = input.descendant;
	const descendantCid = descendantInput.cid;
	const descendantBytesInput = descendantInput.bytes;
	const resolve = input.resolve;
	const providedSignal = input.signal;
	const callerLimits = input.limits;
	const limits: Readonly<BoundedEntryV0CausalReachabilityLimits> =
		Object.freeze({
			maxEntryBytes: callerLimits.maxEntryBytes,
			maxDirectParents: callerLimits.maxDirectParents,
			maxVisitedEntries: callerLimits.maxVisitedEntries,
			maxTotalBytes: callerLimits.maxTotalBytes,
			maxParentLinks: callerLimits.maxParentLinks,
			maxResolveBatchSize: callerLimits.maxResolveBatchSize,
		});

	assertLimits(limits);
	assertCanonicalEntryCid(ancestorCid, "ancestorCid");
	assertCanonicalEntryCid(descendantCid, "descendant.cid");
	if (typeof resolve !== "function") {
		throw new TypeError("resolve must be a function");
	}

	const visited: BoundedEntryV0CausalReachabilityVisited = {
		entries: 0,
		bytes: 0,
		parentLinks: 0,
		resolverCalls: 0,
	};
	if (ancestorCid === descendantCid) {
		return { status: "not-ancestor", visited };
	}

	const signal = providedSignal ?? new AbortController().signal;
	const missing = new Set<string>();
	const seen = new Set<string>([descendantCid]);
	let capacity = false;
	const frontier = new CanonicalCidMinHeap();
	visited.entries = 1;

	const finalUnproven = (): BoundedEntryV0CausalReachabilityResult => {
		if (capacity) return { status: "capacity", visited: cloneVisited(visited) };
		if (missing.size > 0) {
			return {
				status: "incomplete",
				missingCids: [...missing].sort(),
				visited: cloneVisited(visited),
			};
		}
		return { status: "not-ancestor", visited: cloneVisited(visited) };
	};
	if (signal.aborted) {
		missing.add(descendantCid);
		return finalUnproven();
	}

	const scheduleParents = (
		entryCid: string,
		parents: string[],
	): BoundedEntryV0CausalReachabilityResult | undefined => {
		let malformedParent = false;
		const canonicalParents: string[] = [];
		for (const parent of parents) {
			if (!isCanonicalEntryCid(parent)) {
				malformedParent = true;
				continue;
			}
			if (parent === ancestorCid) {
				return { status: "ancestor", visited: cloneVisited(visited) };
			}
			canonicalParents.push(parent);
		}
		if (malformedParent) missing.add(entryCid);
		canonicalParents.sort();
		for (const parent of canonicalParents) {
			if (seen.has(parent)) continue;
			if (visited.entries >= limits.maxVisitedEntries) {
				capacity = true;
				continue;
			}
			seen.add(parent);
			visited.entries++;
			frontier.push(parent);
		}
		return undefined;
	};

	const processVerifiedBytes = (
		cid: string,
		bytes: Uint8Array,
	): BoundedEntryV0CausalReachabilityResult | undefined => {
		try {
			const remainingParentLinks = limits.maxParentLinks - visited.parentLinks;
			const parents = readCanonicalEntryV0Parents(
				bytes,
				Math.min(limits.maxDirectParents, remainingParentLinks),
				(parentCount) => {
					visited.parentLinks += parentCount;
				},
			);
			return scheduleParents(cid, parents);
		} catch (error) {
			if (error instanceof CapacityError) capacity = true;
			else missing.add(cid);
			return undefined;
		}
	};

	const descendantByteLength = exactUint8ArrayByteLength(descendantBytesInput);
	if (descendantByteLength === undefined || descendantByteLength < 1) {
		missing.add(descendantCid);
		return finalUnproven();
	}
	if (
		descendantByteLength > limits.maxEntryBytes ||
		descendantByteLength > limits.maxTotalBytes
	) {
		capacity = true;
		return finalUnproven();
	}
	const descendantBytes = copyUint8Array(
		descendantBytesInput,
		descendantByteLength,
	);
	if (!descendantBytes) {
		missing.add(descendantCid);
		return finalUnproven();
	}
	visited.bytes += descendantBytes.byteLength;
	let descendantHash: string;
	try {
		descendantHash = (await calculateRawCid(descendantBytes)).cid;
	} catch {
		missing.add(descendantCid);
		return finalUnproven();
	}
	if (descendantHash !== descendantCid) {
		missing.add(descendantCid);
		return finalUnproven();
	}
	if (signal.aborted) {
		missing.add(descendantCid);
		return finalUnproven();
	}
	const descendantResult = processVerifiedBytes(descendantCid, descendantBytes);
	if (descendantResult) return descendantResult;

	while (frontier.size > 0) {
		const batch = frontier.popBatch(limits.maxResolveBatchSize);
		if (signal.aborted) {
			for (const cid of [...batch, ...frontier.snapshot()]) missing.add(cid);
			return finalUnproven();
		}

		visited.resolverCalls++;
		let resolved: ReadonlyMap<string, Uint8Array | undefined>;
		try {
			const request = Object.freeze([...batch]);
			resolved = await callResolver(resolve, request, signal);
		} catch {
			for (const cid of [...batch, ...frontier.snapshot()]) missing.add(cid);
			return finalUnproven();
		}
		if (signal.aborted) {
			for (const cid of [...batch, ...frontier.snapshot()]) missing.add(cid);
			return finalUnproven();
		}

		let resolvedGet: (cid: string) => Uint8Array | undefined;
		try {
			const candidate = resolved.get;
			if (typeof candidate !== "function") {
				throw new TypeError("Resolver result must provide get");
			}
			resolvedGet = candidate;
		} catch {
			for (const cid of batch) missing.add(cid);
			continue;
		}

		const captured = new Map<string, Uint8Array>();
		let mapIsValid = true;
		for (const cid of batch) {
			let value: unknown;
			try {
				value = REFLECT_APPLY(resolvedGet, resolved, [cid]);
			} catch {
				mapIsValid = false;
				break;
			}
			if (value === undefined) {
				missing.add(cid);
				continue;
			}
			const byteLength = exactUint8ArrayByteLength(value);
			if (byteLength === undefined || byteLength < 1) {
				missing.add(cid);
				continue;
			}
			if (
				byteLength > limits.maxEntryBytes ||
				byteLength > limits.maxTotalBytes - visited.bytes
			) {
				capacity = true;
				continue;
			}
			const copy = copyUint8Array(value, byteLength);
			if (!copy) {
				missing.add(cid);
				continue;
			}
			visited.bytes += copy.byteLength;
			captured.set(cid, copy);
		}
		if (!mapIsValid) {
			for (const cid of batch) missing.add(cid);
			continue;
		}

		let hashes: Array<PromiseSettledResult<string>>;
		try {
			hashes = await Promise.allSettled(
				batch.map(async (cid) => {
					const bytes = captured.get(cid);
					return bytes ? (await calculateRawCid(bytes)).cid : "";
				}),
			);
		} catch {
			for (const cid of batch) missing.add(cid);
			continue;
		}
		if (signal.aborted) {
			for (const cid of [...batch, ...frontier.snapshot()]) missing.add(cid);
			return finalUnproven();
		}

		for (let i = 0; i < batch.length; i++) {
			const cid = batch[i]!;
			const bytes = captured.get(cid);
			if (!bytes) continue;
			const hash = hashes[i]!;
			if (hash.status !== "fulfilled" || hash.value !== cid) {
				missing.add(cid);
				continue;
			}
			const result = processVerifiedBytes(cid, bytes);
			if (result) return result;
		}
	}

	return finalUnproven();
};
