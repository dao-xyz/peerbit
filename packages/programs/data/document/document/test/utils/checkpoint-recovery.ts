// Executable recovery profile, deliberately excluded from the published package.
// One owner-certified finite input; no live admission, migration, or later-write
// durability. The only exposed result is a copied view, never mutable Documents.
import { deserialize, serialize } from "@dao-xyz/borsh";
import type { CrashSafeTwoSlotCheckpoint } from "@peerbit/any-store/checkpoint";
import {
	Ed25519PublicKey,
	PreHash,
	SignatureWithKey,
	sha256Sync,
	verify,
} from "@peerbit/crypto";
import { Context } from "@peerbit/document-interface";
import { toId } from "@peerbit/indexer-interface";
import { Entry, EntryType, scanCanonicalPublicEntryV0 } from "@peerbit/log";
import assert from "node:assert/strict";
import { Peerbit } from "peerbit";
import {
	DeleteOperation,
	Documents,
	type Operation,
	PutOperation,
} from "../../src/index.js";
import { Document, TestStore } from "../data.js";

export type RecoveryRecord = {
	cid: string;
	kind: "put" | "delete";
	id: string;
	name?: string;
};
export type RecoveryRow = {
	id: string;
	name: string;
	context: {
		created: string;
		modified: string;
		head: string;
		gid: string;
		size: number;
	};
};
export type RecoveryView = {
	rows: RecoveryRow[];
	entries: string[];
	heads: string[];
};
export type RecoveryManifest = {
	profile: "peerbit-checkpoint-recovery-fixture-v1";
	logId: string;
	records: RecoveryRecord[];
	boundary: string[];
	rows: RecoveryRow[];
	order: string[];
	expected: RecoveryView;
};
export type RecoveryInput = {
	manifest: Uint8Array;
	signature: Uint8Array;
	blocks: { cid: string; bytes: Uint8Array }[];
};
export type RecoveryTrust = {
	owner: Uint8Array;
	manifestDigest: string;
	logId: string;
};
export type RecoveryHooks = { phase?: (name: string) => void | Promise<void> };

const MAX_MANIFEST = 64 * 1024;
const MAX_BLOCK = 64 * 1024;
const MAX_BLOCKS = 32;
const MAX_TOTAL = 1024 * 1024;
export const MAX_RECOVERY_RECORD_BYTES = 3 * 1024 * 1024;
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype);
const byteLengthGetter = Object.getOwnPropertyDescriptor(
	typedArrayPrototype,
	"byteLength",
)!.get!;
const tagGetter = Object.getOwnPropertyDescriptor(
	typedArrayPrototype,
	Symbol.toStringTag,
)!.get!;
const copyBytes = Uint8Array.prototype.set;
export const hex = (bytes: Uint8Array): string =>
	Buffer.from(bytes).toString("hex");
const unhex = (value: string, maxBytes: number): Uint8Array => {
	assert(
		typeof value === "string" &&
			value.length <= 2 * maxBytes &&
			/^(?:[0-9a-f]{2})*$/.test(value),
		"Invalid bounded hex",
	);
	return new Uint8Array(Buffer.from(value, "hex"));
};
const capture = (bytes: Uint8Array, maxBytes: number): Uint8Array => {
	const length = byteLengthGetter.call(bytes);
	assert(
		tagGetter.call(bytes) === "Uint8Array" && length <= maxBytes,
		"Byte capacity exceeded",
	);
	const owned = new Uint8Array(length);
	copyBytes.call(owned, bytes);
	return owned;
};
function boundedString(value: unknown, max = 1024): asserts value is string {
	assert(
		typeof value === "string" &&
			value.length > 0 &&
			value.length <= max &&
			encoder.encode(value).length <= max,
		"Invalid bounded string",
	);
}
const unique = (values: string[]) => {
	assert(
		Array.isArray(values) && values.length <= MAX_BLOCKS,
		"Inventory capacity exceeded",
	);
	for (const value of values) boundedString(value, 128);
	assert.equal(new Set(values).size, values.length, "Duplicate inventory CID");
	return new Set(values);
};
export const encodeManifest = (manifest: RecoveryManifest): Uint8Array =>
	encoder.encode(JSON.stringify(manifest));
const encodeJSON = (value: unknown) => encoder.encode(JSON.stringify(value));

export const encodeRecoveryInput = (input: RecoveryInput): Uint8Array =>
	encodeJSON({
		manifest: hex(input.manifest),
		signature: hex(input.signature),
		blocks: input.blocks.map(({ cid, bytes }) => ({ cid, bytes: hex(bytes) })),
	});
export const decodeRecoveryInput = (bytes: Uint8Array): RecoveryInput => {
	const value = JSON.parse(
		decoder.decode(capture(bytes, MAX_RECOVERY_RECORD_BYTES)),
	);
	assert(
		Array.isArray(value.blocks) && value.blocks.length <= MAX_BLOCKS,
		"Inventory capacity exceeded",
	);
	return {
		manifest: unhex(value.manifest, MAX_MANIFEST),
		signature: unhex(value.signature, 64),
		blocks: value.blocks.map((block: { cid: string; bytes: string }) => ({
			cid: block.cid,
			bytes: unhex(block.bytes, MAX_BLOCK),
		})),
	};
};

export const readRecoveryView = async (
	docs: Documents<Document>,
): Promise<RecoveryView> => {
	const rows: RecoveryRow[] = [];
	for (const value of await docs.index.search(
		{},
		{ local: true, remote: false },
	)) {
		const indexed = await docs.index.index.get(toId(value.id));
		assert(indexed, "Missing document context");
		const context = indexed.value.__context;
		assert(typeof value.name === "string");
		rows.push({
			id: value.id,
			name: value.name,
			context: {
				created: context.created.toString(),
				modified: context.modified.toString(),
				head: context.head,
				gid: context.gid,
				size: context.size,
			},
		});
	}
	rows.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
	return {
		rows,
		entries: (await docs.log.log.toArray()).map((entry) => entry.hash).sort(),
		heads: (await docs.log.log.getHeads().all())
			.map((entry) => entry.hash)
			.sort(),
	};
};

export const verifyRecoveryInput = async (
	input: RecoveryInput,
	trust: RecoveryTrust,
) => {
	// Capture every caller-owned byte before the first asynchronous verification.
	const manifestBytes = capture(input.manifest, MAX_MANIFEST);
	const signatureBytes = capture(input.signature, 64);
	const ownerBytes = capture(trust.owner, 32);
	assert.equal(ownerBytes.length, 32);
	assert.equal(signatureBytes.length, 64);
	const owner = new Ed25519PublicKey({ publicKey: ownerBytes });
	const pinnedDigest = trust.manifestDigest;
	const pinnedLogId = trust.logId;
	const suppliedBlocks = input.blocks;
	assert(Array.isArray(suppliedBlocks), "Invalid block inventory");
	const blockCount = suppliedBlocks.length;
	assert(
		Number.isSafeInteger(blockCount) &&
			blockCount >= 0 &&
			blockCount <= MAX_BLOCKS,
		"Inventory capacity exceeded",
	);
	const blocks = new Map<string, Uint8Array>();
	let total = 0;
	for (let index = 0; index < blockCount; index++) {
		const block = suppliedBlocks[index];
		const cid = block.cid;
		boundedString(cid, 128);
		assert(!blocks.has(cid), "Duplicate supplied block");
		const owned = capture(block.bytes, Math.min(MAX_BLOCK, MAX_TOTAL - total));
		total += owned.byteLength;
		blocks.set(cid, owned);
	}
	assert.equal(
		hex(sha256Sync(manifestBytes)),
		pinnedDigest,
		"Wrong checkpoint digest",
	);
	assert(
		await verify(
			new SignatureWithKey({
				publicKey: owner,
				signature: signatureBytes,
				prehash: PreHash.NONE,
			}),
			manifestBytes,
		),
		"Invalid owner certificate",
	);
	const manifest: RecoveryManifest = JSON.parse(decoder.decode(manifestBytes));
	assert.equal(
		manifest.profile,
		"peerbit-checkpoint-recovery-fixture-v1",
		"Unsupported recovery profile",
	);
	assert.equal(manifest.logId, pinnedLogId, "Wrong resource");
	assert.equal(unhex(manifest.logId, 32).length, 32);
	assert(
		Array.isArray(manifest.records) && manifest.records.length <= MAX_BLOCKS,
		"Inventory capacity exceeded",
	);
	const inventory = unique(manifest.records.map((record) => record.cid));
	assert.deepEqual(
		new Set(blocks.keys()),
		inventory,
		"Incomplete or foreign block inventory",
	);
	const boundary = unique(manifest.boundary);
	const suffix = unique(manifest.order);
	assert.equal(
		boundary.size + suffix.size,
		inventory.size,
		"Inventory partition mismatch",
	);
	for (const cid of boundary)
		assert(
			inventory.has(cid) && !suffix.has(cid),
			"Invalid boundary inventory",
		);
	for (const cid of suffix) assert(inventory.has(cid), "Foreign suffix CID");
	const records = new Map(
		manifest.records.map((record) => [record.cid, record]),
	);
	const entries = new Map<string, Entry<Operation>>();
	for (const record of manifest.records) {
		boundedString(record.id);
		assert(
			record.kind === "put" || record.kind === "delete",
			"Unsupported operation",
		);
		if (record.kind === "put") boundedString(record.name);
		const bytes = blocks.get(record.cid)!;
		const scanned = scanCanonicalPublicEntryV0(bytes, {
			label: "Checkpoint fixture",
			minimumSignatures: 1,
			maximumSignatures: 1,
			maximumDirectParents: 1,
			maximumMetadataBytes: 1024,
		});
		assert(
			!scanned.hasHash && scanned.reservedBytes.every((byte) => byte === 0),
			"Unsupported entry framing",
		);
		assert.equal(
			await Entry.prepareMultihash(scanned.entry),
			record.cid,
			"Entry CID mismatch",
		);
		assert(
			scanned.signatures[0].publicKey.equals(owner) &&
				(await verify(scanned.signatures[0], scanned.signableBytes)),
			"Invalid entry signer/signature",
		);
		assert.equal(
			scanned.meta.type,
			record.kind === "put" ? EntryType.APPEND : EntryType.CUT,
			"Operation entry type mismatch",
		);
		const operation =
			record.kind === "put"
				? new PutOperation({
						data: serialize(new Document({ id: record.id, name: record.name })),
					})
				: new DeleteOperation({ key: toId(record.id) });
		assert.deepEqual(
			scanned.payloadBytes,
			new Uint8Array(serialize(operation)),
			"Operation payload mismatch",
		);
		const entry = scanned.entry as unknown as Entry<Operation>;
		entry.hash = record.cid;
		entries.set(record.cid, entry);
	}
	const admitted = new Set(boundary);
	for (const cid of manifest.order) {
		const entry = entries.get(cid)!;
		const record = records.get(cid)!;
		if (record.kind === "delete")
			assert.equal(entry.meta.next.length, 1, "Delete must name its victim");
		for (const parent of entry.meta.next) {
			assert(
				admitted.has(parent),
				"Suffix is not causally closed in certified order",
			);
			assert.equal(records.get(parent)!.kind, "put", "Parent must be a PUT");
			assert.equal(records.get(parent)!.id, record.id, "Cross-key predecessor");
			assert.equal(
				entries.get(parent)!.meta.gid,
				entry.meta.gid,
				"Cross-gid predecessor",
			);
		}
		admitted.add(cid);
	}
	const validateRows = (rows: RecoveryRow[], allowed: Set<string>) => {
		assert(
			Array.isArray(rows) && rows.length <= MAX_BLOCKS,
			"Row capacity exceeded",
		);
		const ids = new Set<string>();
		for (const row of rows) {
			boundedString(row.id);
			boundedString(row.name);
			assert(!ids.has(row.id), "Duplicate snapshot key");
			ids.add(row.id);
			const record = records.get(row.context.head);
			const entry = entries.get(row.context.head);
			assert(
				allowed.has(row.context.head) && entry && record?.kind === "put",
				"Uncertified row head",
			);
			assert.equal(record.id, row.id);
			assert.equal(record.name, row.name);
			assert(
				/^(0|[1-9][0-9]{0,19})$/.test(row.context.created),
				"Invalid creation time",
			);
			assert(
				/^(0|[1-9][0-9]{0,19})$/.test(row.context.modified),
				"Invalid modification time",
			);
			assert(BigInt(row.context.created) <= BigInt(row.context.modified));
			assert.equal(
				row.context.modified,
				entry.meta.clock.timestamp.wallTime.toString(),
			);
			assert.equal(row.context.gid, entry.meta.gid);
			assert.equal(row.context.size, entry.payload.byteLength);
		}
	};
	validateRows(manifest.rows, boundary);
	assert.equal(
		manifest.rows.length,
		boundary.size,
		"Every boundary must have one live row in this profile",
	);
	validateRows(manifest.expected.rows, inventory);
	const expectedEntries = unique(manifest.expected.entries);
	for (const cid of expectedEntries)
		assert(inventory.has(cid), "Foreign expected entry");
	for (const cid of unique(manifest.expected.heads))
		assert(expectedEntries.has(cid), "Foreign expected head");
	return {
		manifest,
		blocks,
		entries,
		records,
		boundary,
		owner,
		input: {
			manifest: manifestBytes,
			signature: signatureBytes,
			blocks: [...blocks].map(([cid, bytes]) => ({ cid, bytes })),
		},
	};
};

export class CheckpointRecoveryFixture {
	private view?: RecoveryView;
	private started = false;
	private forbiddenReads = 0;
	get omittedPrefixReads() {
		return this.forbiddenReads;
	}
	snapshot(): RecoveryView {
		assert(this.view, "Checkpoint view unavailable");
		return JSON.parse(JSON.stringify(this.view));
	}

	async recover(
		input: RecoveryInput | undefined,
		trust: RecoveryTrust,
		checkpoint?: CrashSafeTwoSlotCheckpoint,
		hooks?: RecoveryHooks,
	): Promise<RecoveryView> {
		assert(!this.started, "Recovery instance is single-use");
		this.started = true;
		const prior = checkpoint?.current;
		let retained:
			| {
					phase: "retained" | "published";
					input: string;
					watermark?: RecoveryView;
			  }
			| undefined;
		if (prior) {
			retained = JSON.parse(
				decoder.decode(capture(prior.payload, MAX_RECOVERY_RECORD_BYTES)),
			);
			assert(
				retained &&
					(retained.phase === "retained" || retained.phase === "published"),
				"Invalid recovery record",
			);
			assert.equal(
				input,
				undefined,
				"Initial-import fixture cannot replace an existing checkpoint",
			);
			assert(typeof retained.input === "string", "Invalid retained input");
			input = decodeRecoveryInput(encoder.encode(retained.input));
		}
		assert(input, "No retained checkpoint input");
		const validated = await verifyRecoveryInput(input, trust);
		const encoded = decoder.decode(encodeRecoveryInput(validated.input));
		if (retained?.phase === "published")
			assert.deepEqual(
				retained.watermark,
				validated.manifest.expected,
				"Projection watermark mismatch",
			);
		await hooks?.phase?.("verified");
		if (checkpoint && !prior)
			await checkpoint.commit(
				encodeJSON({ phase: "retained", input: encoded }),
			);
		await hooks?.phase?.("retained");
		const peer = await Peerbit.create();
		let recovered: RecoveryView;
		try {
			const { manifest, blocks, records, boundary, owner } = validated;
			const store = await peer.open(
				new TestStore({
					docs: new Documents<Document>({ id: unhex(manifest.logId, 32) }),
				}),
				{
					args: {
						mode: "compat",
						replicate: false,
						keep: "self",
						nativeGraph: false,
						nativeBackbone: false,
						nativeRangePlanner: false,
						index: { cache: { resolver: 0 } },
					},
				},
			);
			const docs = store.docs;
			// No store/network fallback may substitute for the certified inventory.
			const check = (hash: string) => {
				if (!blocks.has(hash)) {
					this.forbiddenReads++;
					throw new Error("Read outside certified inventory");
				}
			};
			for (const blockStore of new Set([
				peer.services.blocks,
				docs.log.log.blocks,
			])) {
				const get = blockStore.get.bind(blockStore);
				blockStore.get = async (hash, options) => {
					check(hash);
					return get(hash, options);
				};
				if (blockStore.getMany) {
					const getMany = blockStore.getMany.bind(blockStore);
					blockStore.getMany = async (hashes, options) => {
						for (const hash of hashes) check(hash);
						return getMany(hashes, options);
					};
				}
			}
			const freshEntry = (cid: string): Entry<Operation> => {
				const entry = deserialize(
					new Uint8Array(blocks.get(cid)!),
					Entry,
				) as Entry<Operation>;
				entry.hash = cid;
				entry.init(docs.log.log);
				return entry;
			};
			const restore = async (cid: string) => {
				assert(boundary.has(cid), "Only certified boundaries may be installed");
				await docs.log.log.entryIndex.put(freshEntry(cid), {
					unique: true,
					isHead: true,
					toMultiHash: true,
				});
				const row = manifest.rows.find(
					(candidate) => candidate.context.head === cid,
				)!;
				if (!(await docs.index.index.get(toId(row.id)))) {
					await docs.index.putWithContext(
						new Document({ id: row.id, name: row.name }),
						toId(row.id),
						new Context({
							...row.context,
							created: BigInt(row.context.created),
							modified: BigInt(row.context.modified),
						}),
						{ transformFacts: { entryPublicKeys: [owner] } },
					);
				}
			};
			for (const cid of boundary) await restore(cid);
			await hooks?.phase?.("boundary");
			for (const [index, cid] of manifest.order.entries()) {
				const entry = freshEntry(cid);
				for (const parent of entry.meta.next) {
					if (await docs.log.log.has(parent)) continue;
					assert(
						records.get(cid)!.kind === "put" && boundary.has(parent),
						"Unsupported pruned suffix dependency",
					);
					await restore(parent);
				}
				await docs.log.log.join([entry], { verifySignatures: true });
				assert(
					await docs.log.log.has(cid),
					"Certified suffix entry was not admitted",
				);
				await hooks?.phase?.(`entry:${index}`);
			}
			const view = await readRecoveryView(docs);
			assert.equal(
				this.forbiddenReads,
				0,
				"Replay attempted an omitted-prefix read",
			);
			assert.deepEqual(
				view,
				manifest.expected,
				"Recovered projection/frontier mismatch",
			);
			await hooks?.phase?.("replayed");
			if (checkpoint && retained?.phase !== "published")
				await checkpoint.commit(
					encodeJSON({ phase: "published", input: encoded, watermark: view }),
				);
			await hooks?.phase?.("published");
			recovered = view;
		} finally {
			// Closing scratch is part of recovery, not a post-publication action.
			await peer.stop();
		}
		this.view = recovered;
		return this.snapshot();
	}
}
