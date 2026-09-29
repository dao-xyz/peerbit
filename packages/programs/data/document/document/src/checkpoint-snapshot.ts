import {
	type Blocks,
	calculateRawCid,
	cidifyString,
	codecMap,
	defaultHasher,
	stringifyCid,
	verifyBlockBytes,
} from "@peerbit/blocks-interface";
import {
	Ed25519PublicKey,
	type Identity,
	PreHash,
	SignatureWithKey,
	verify,
} from "@peerbit/crypto";
import { equals, fromString, toString } from "uint8arrays";
import {
	boundCheckpointJSON,
	captureCheckpointBytes,
} from "./checkpoint-operation.js";

export const CHECKPOINT_MAX_FRONTIER_ENTRIES = 1_000_000;
export const CHECKPOINT_FRONTIER_CHUNK_ENTRIES = 128;
export const CHECKPOINT_MAX_CHUNK_BYTES = 64 * 1024;
export const CHECKPOINT_MAX_ROOT_BYTES = 2 * 1024 * 1024;
const PROFILE = "peerbit-document-checkpoint-v1";
const FREEZE_PROFILE = "peerbit-document-freeze-v1";
const MAX_FREEZES = 32;
const MAX_U64 = (1n << 64n) - 1n;
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

export type CheckpointFrontierRecord = Readonly<{
	cid: string;
	created: bigint;
}>;

export type CheckpointSnapshot = Readonly<{
	cid: string;
	epoch: bigint;
	previous: string | null;
	count: number;
	freezes: readonly string[];
	/** Validates every chunk before yielding its records; does not admit entries. */
	frontier(): AsyncIterable<CheckpointFrontierRecord>;
	/** Retains verified immutable blocks, not a published authority/watermark. */
	retain(): Promise<void>;
}>;

export type CheckpointFreeze = CheckpointSnapshot &
	Readonly<{ writer: Ed25519PublicKey }>;

type Root = {
	profile: typeof PROFILE | typeof FREEZE_PROFILE;
	resource: string;
	owner: string;
	epoch: string;
	previous: string | null;
	count: number;
	chunks: string[];
	freezes: string[];
};

const encode = (value: unknown): Uint8Array =>
	encoder.encode(JSON.stringify(value));

const digest = (bytes: Uint8Array): Uint8Array => {
	const captured = captureCheckpointBytes(bytes, 32);
	if (captured.byteLength !== 32) {
		throw new Error("Checkpoint resource/key must contain 32 bytes");
	}
	return captured;
};

const assertU64 = (value: bigint): void => {
	if (typeof value !== "bigint" || value < 0n || value > MAX_U64) {
		throw new Error("Invalid checkpoint u64");
	}
};

const readU64 = (value: unknown): bigint => {
	if (
		typeof value !== "string" ||
		value.length > 20 ||
		!/^(0|[1-9][0-9]*)$/.test(value)
	) {
		throw new Error("Invalid canonical checkpoint u64");
	}
	const result = BigInt(value);
	assertU64(result);
	return result;
};

const assertCid = (value: unknown): string => {
	if (typeof value !== "string" || value.length === 0 || value.length > 128) {
		throw new Error("Invalid checkpoint CID");
	}
	const cid = cidifyString(value);
	if (
		cid.version !== 1 ||
		cid.code !== codecMap.raw.code ||
		cid.multihash.code !== defaultHasher.code ||
		cid.multihash.digest.byteLength !== 32 ||
		stringifyCid(cid) !== value
	) {
		throw new Error("Checkpoint requires canonical CIDv1/raw/sha2-256");
	}
	return value;
};

const assertEpoch = (epoch: bigint, previous: string | null): void => {
	assertU64(epoch);
	if (previous !== null) assertCid(previous);
	if ((epoch === 0n) !== (previous === null)) {
		throw new Error("Checkpoint predecessor does not match its epoch");
	}
};

const captureFreezes = (values: unknown): string[] => {
	if (!Array.isArray(values) || values.length > MAX_FREEZES) {
		throw new Error("Invalid checkpoint freeze manifest count");
	}
	const freezes = values.map(assertCid).sort();
	if (new Set(freezes).size !== freezes.length) {
		throw new Error("Duplicate checkpoint freeze manifest");
	}
	return freezes;
};

const assertFreezes = (
	profile: Root["profile"],
	epoch: bigint,
	freezes: readonly string[],
): void => {
	if (
		((profile === FREEZE_PROFILE || epoch === 0n) && freezes.length > 0) ||
		(profile === FREEZE_PROFILE && epoch === 0n)
	) {
		throw new Error("Invalid freeze/genesis checkpoint manifest binding");
	}
};

const readBlock = async (
	blocks: Blocks,
	cid: string,
	maximumBytes: number,
	remote: boolean,
): Promise<Uint8Array> => {
	const bytes = await blocks.get(cid, {
		remote: remote ? { replicate: false } : false,
	});
	if (!bytes) throw new Error(`Missing checkpoint block: ${cid}`);
	const owned = captureCheckpointBytes(bytes, maximumBytes);
	await verifyBlockBytes(cid, owned, { codec: codecMap.raw });
	return owned;
};

const retainBlock = async (
	blocks: Blocks,
	cid: string,
	bytes: Uint8Array,
): Promise<void> => {
	// Never rely on a custom store's returned CID or its optional verification.
	const owned = new Uint8Array(bytes);
	const stored = blocks.putKnown
		? await blocks.putKnown(cid, owned)
		: await blocks.put(owned);
	if (stored !== cid)
		throw new Error("Checkpoint store returned a different CID");
};

const snapshot = (
	blocks: Blocks,
	cid: string,
	root: Root,
	rootBytes: Uint8Array,
	remote: boolean,
): CheckpointFreeze => {
	async function* records(retain: boolean) {
		let previous: string | undefined;
		let count = 0;
		for (let chunkIndex = 0; chunkIndex < root.chunks.length; chunkIndex++) {
			const chunkCid = root.chunks[chunkIndex]!;
			const bytes = await readBlock(
				blocks,
				chunkCid,
				CHECKPOINT_MAX_CHUNK_BYTES,
				remote,
			);
			boundCheckpointJSON(bytes);
			const chunk = JSON.parse(decoder.decode(bytes)) as unknown;
			const expected = Math.min(
				CHECKPOINT_FRONTIER_CHUNK_ENTRIES,
				root.count - count,
			);
			if (!Array.isArray(chunk) || chunk.length !== expected) {
				throw new Error(
					"Checkpoint chunk entry count differs from signed root",
				);
			}
			const validated: CheckpointFrontierRecord[] = [];
			for (const item of chunk) {
				if (!Array.isArray(item) || item.length !== 2) {
					throw new Error("Invalid checkpoint frontier record");
				}
				const entryCid = assertCid(item[0]);
				const created = readU64(item[1]);
				if (previous !== undefined && entryCid <= previous) {
					throw new Error("Checkpoint frontier must be strictly CID ordered");
				}
				previous = entryCid;
				validated.push({ cid: entryCid, created });
			}
			if (
				!equals(
					bytes,
					encode(validated.map((item) => [item.cid, item.created.toString()])),
				)
			) {
				throw new Error("Noncanonical checkpoint chunk");
			}
			if (retain) await retainBlock(blocks, chunkCid, bytes);
			count += validated.length;
			yield* validated;
		}
		if (count !== root.count) throw new Error("Incomplete checkpoint frontier");
		if (retain) await retainBlock(blocks, cid, rootBytes);
	}
	return Object.freeze({
		cid,
		epoch: BigInt(root.epoch),
		previous: root.previous,
		count: root.count,
		freezes: Object.freeze([...root.freezes]),
		get writer() {
			return new Ed25519PublicKey({
				publicKey: fromString(root.owner, "base16"),
			});
		},
		frontier: () => records(false),
		retain: async () => {
			for await (const _record of records(true)) {
				// Consume and verify the complete snapshot before retaining its root.
			}
		},
	});
};

/**
 * Build bounded immutable chunks before signing the complete root. Malformed
 * late input may leave unreferenced chunks, never an authority publication.
 * Entry authentication/causal closure remains the resource runtime's job.
 */
type CreateFrontierOptions = {
	blocks: Blocks;
	resource: Uint8Array;
	owner: Identity<Ed25519PublicKey>;
	epoch: bigint;
	previous: string | null;
	frontier: Iterable<CheckpointFrontierRecord>;
	freezes?: readonly string[];
};

const createFrontier = async (
	profile: Root["profile"],
	properties: CreateFrontierOptions,
): Promise<CheckpointFreeze> => {
	const { blocks, owner, epoch, previous, frontier } = properties;
	if (!(owner.publicKey instanceof Ed25519PublicKey)) {
		throw new Error("Checkpoint owner must use Ed25519");
	}
	const key = new Ed25519PublicKey({
		publicKey: digest(owner.publicKey.publicKey),
	});
	const resource = toString(digest(properties.resource), "base16");
	assertEpoch(epoch, previous);
	const freezes = captureFreezes(properties.freezes ?? []);
	assertFreezes(profile, epoch, freezes);
	const chunks: string[] = [];
	let chunk: Array<[string, string]> = [];
	let count = 0;
	let last: string | undefined;
	const flush = async () => {
		const bytes = encode(chunk);
		if (bytes.byteLength > CHECKPOINT_MAX_CHUNK_BYTES) {
			throw new Error("Checkpoint chunk capacity exceeded");
		}
		const { cid } = await calculateRawCid(bytes);
		await retainBlock(blocks, cid, bytes);
		chunks.push(cid);
		chunk = [];
	};
	for (const record of frontier) {
		const cid = assertCid(record.cid);
		const created = record.created;
		assertU64(created);
		if (last !== undefined && cid <= last) {
			throw new Error("Checkpoint frontier must be strictly CID ordered");
		}
		if (++count > CHECKPOINT_MAX_FRONTIER_ENTRIES || epoch === 0n) {
			throw new Error(
				"Checkpoint frontier capacity/genesis constraint violated",
			);
		}
		last = cid;
		chunk.push([cid, created.toString()]);
		if (chunk.length === CHECKPOINT_FRONTIER_CHUNK_ENTRIES) await flush();
	}
	if (chunk.length) await flush();
	const root: Root = {
		profile,
		resource,
		owner: toString(key.publicKey, "base16"),
		epoch: epoch.toString(),
		previous,
		count,
		chunks,
		freezes,
	};
	const signable = encode(root);
	const signed = await owner.sign(new Uint8Array(signable), PreHash.NONE);
	const signature = captureCheckpointBytes(signed.signature, 64);
	if (
		signature.byteLength !== 64 ||
		signed.prehash !== PreHash.NONE ||
		!key.equals(signed.publicKey) ||
		!(await verify(
			new SignatureWithKey({
				signature,
				publicKey: key,
				prehash: PreHash.NONE,
			}),
			signable,
		))
	) {
		throw new Error("Invalid checkpoint owner signature");
	}
	const bytes = encode({ root, signature: toString(signature, "base16") });
	if (bytes.byteLength > CHECKPOINT_MAX_ROOT_BYTES) {
		throw new Error("Checkpoint root capacity exceeded");
	}
	const { cid } = await calculateRawCid(bytes);
	await retainBlock(blocks, cid, bytes);
	return snapshot(blocks, cid, root, bytes, false);
};

export const createCheckpointSnapshot = (
	properties: CreateFrontierOptions,
): Promise<CheckpointSnapshot> => createFrontier(PROFILE, properties);

/** A signed freeze manifest is not a checkpoint proposal or an approval. */
export const createCheckpointFreeze = (
	properties: Omit<CreateFrontierOptions, "freezes">,
): Promise<CheckpointFreeze> => createFrontier(FREEZE_PROFILE, properties);

/**
 * Authenticate the bounded root; iteration/retention verifies its full chunks.
 * The caller must consume every record before publishing a recovered view.
 * A signed root alone does not establish freshness, predecessor continuity or
 * the validity of the referenced entries; those are resource admission rules.
 */
type ReadFrontierOptions = {
	blocks: Blocks;
	cid: string;
	resource: Uint8Array;
	remote?: boolean;
};

const captureOwners = (
	owners: readonly Ed25519PublicKey[],
): Map<string, Ed25519PublicKey> => {
	if (
		!Array.isArray(owners) ||
		owners.length === 0 ||
		owners.length > MAX_FREEZES
	) {
		throw new Error("Invalid checkpoint freeze writer roster");
	}
	const captured = new Map<string, Ed25519PublicKey>();
	for (const owner of owners) {
		if (!(owner instanceof Ed25519PublicKey)) {
			throw new Error("Checkpoint owner must use Ed25519");
		}
		const key = new Ed25519PublicKey({ publicKey: digest(owner.publicKey) });
		const encoded = toString(key.publicKey, "base16");
		if (captured.has(encoded)) {
			throw new Error("Duplicate checkpoint freeze writer");
		}
		captured.set(encoded, key);
	}
	return captured;
};

const readFrontier = async (
	profile: Root["profile"],
	properties: ReadFrontierOptions,
	owners: ReadonlyMap<string, Ed25519PublicKey>,
): Promise<CheckpointFreeze> => {
	const { blocks, remote = false } = properties;
	const cid = assertCid(properties.cid);
	const resource = toString(digest(properties.resource), "base16");
	const bytes = await readBlock(blocks, cid, CHECKPOINT_MAX_ROOT_BYTES, remote);
	boundCheckpointJSON(bytes);
	const parsed = JSON.parse(decoder.decode(bytes));
	const candidate = parsed?.root;
	const owner = owners.get(candidate?.owner);
	if (
		!candidate ||
		!owner ||
		candidate.profile !== profile ||
		candidate.resource !== resource ||
		candidate.owner !== toString(owner.publicKey, "base16") ||
		!Number.isSafeInteger(candidate.count) ||
		candidate.count < 0 ||
		candidate.count > CHECKPOINT_MAX_FRONTIER_ENTRIES ||
		!Array.isArray(candidate.chunks) ||
		candidate.chunks.length !==
			Math.ceil(candidate.count / CHECKPOINT_FRONTIER_CHUNK_ENTRIES) ||
		typeof parsed.signature !== "string" ||
		!/^[0-9a-f]{128}$/.test(parsed.signature)
	) {
		throw new Error("Invalid checkpoint root");
	}
	const epoch = readU64(candidate.epoch);
	const previous =
		candidate.previous === null ? null : assertCid(candidate.previous);
	assertEpoch(epoch, previous);
	const freezes = captureFreezes(candidate.freezes);
	assertFreezes(profile, epoch, freezes);
	if (epoch === 0n && candidate.count !== 0) {
		throw new Error("Genesis checkpoint must have an empty frontier");
	}
	const chunks = candidate.chunks.map(assertCid) as string[];
	if (new Set(chunks).size !== chunks.length) {
		throw new Error("Duplicate checkpoint chunk");
	}
	const root: Root = {
		profile,
		resource,
		owner: toString(owner.publicKey, "base16"),
		epoch: epoch.toString(),
		previous,
		count: candidate.count,
		chunks,
		freezes,
	};
	if (!equals(bytes, encode({ root, signature: parsed.signature }))) {
		throw new Error("Noncanonical checkpoint root");
	}
	if (
		!(await verify(
			new SignatureWithKey({
				signature: fromString(parsed.signature, "base16"),
				publicKey: owner,
				prehash: PreHash.NONE,
			}),
			encode(root),
		))
	) {
		throw new Error("Invalid checkpoint owner signature");
	}
	return snapshot(blocks, cid, root, bytes, remote);
};

export const readCheckpointSnapshot = async (
	properties: ReadFrontierOptions & { owner: Ed25519PublicKey },
): Promise<CheckpointSnapshot> =>
	readFrontier(PROFILE, properties, captureOwners([properties.owner]));

/** Checks the manifest signer against the fixed writer roster before verification. */
export const readCheckpointFreeze = async (
	properties: ReadFrontierOptions & {
		writers: readonly Ed25519PublicKey[];
	},
): Promise<CheckpointFreeze> =>
	readFrontier(FREEZE_PROFILE, properties, captureOwners(properties.writers));
