import { deserialize, field, fixedArray, variant, vec } from "@dao-xyz/borsh";
import {
	Entry,
	EntryType,
	LamportClock as Clock,
	Meta,
	type PreparedAppendJoinFacts,
	type PreparedNativeLogEntry,
	ShallowEntry,
	ShallowMeta,
	Timestamp,
	calculateRawCidV1Batch,
	prepareRawEntryV0Batch,
	type PreparedRawEntryV0Facts,
	verifyEntryV0Ed25519StorageBatch,
} from "@peerbit/log";
import { Log } from "@peerbit/log";
import { logger as loggerFn } from "@peerbit/logger";
import { TransportMessage } from "./message.js";
import type { SyncProfileFn } from "./sync/index.js";
import {
	emitSyncProfileDuration,
	syncProfileStart,
} from "./sync/profile.js";

const logger = loggerFn("peerbit:shared-log:exchange-heads");
const warn = logger.newScope("warn");

// Stored in the reserved bytes so older peers ignore the hint.
export const EXCHANGE_HEADS_REPAIR_HINT = 1;

/**
 * This thing allows use to faster sync since we can provide
 * references that can be read concurrently to
 * the entry when doing Log.fromEntry or Log.fromEntryHash
 */
@variant(0)
export class EntryWithRefs<T> {
	@field({ type: Entry })
	entry: Entry<T>;

	@field({ type: vec("string") })
	gidRefrences: string[]; // are some parents to the entry

	constructor(properties: { entry: Entry<T>; gidRefrences: string[] }) {
		this.entry = properties.entry;
		this.gidRefrences = properties.gidRefrences;
	}
}

@variant(1)
export class RawEntryWithRefs {
	@field({ type: "string" })
	hash: string;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	@field({ type: vec("string") })
	gidRefrences: string[];

	constructor(properties: {
		hash: string;
		bytes: Uint8Array;
		gidRefrences: string[];
	}) {
		this.hash = properties.hash;
		this.bytes = properties.bytes;
		this.gidRefrences = properties.gidRefrences;
	}
}

@variant([0, 0])
export class ExchangeHeadsMessage<T> extends TransportMessage {
	@field({ type: vec(EntryWithRefs) })
	heads: EntryWithRefs<T>[];

	@field({ type: fixedArray("u8", 4) })
	reserved: Uint8Array = new Uint8Array(4);

	constructor(props: { heads: EntryWithRefs<T>[] }) {
		super();
		this.heads = props.heads;
	}
}

@variant([0, 7])
export class RawExchangeHeadsMessage extends TransportMessage {
	@field({ type: vec(RawEntryWithRefs) })
	heads: RawEntryWithRefs[];

	@field({ type: fixedArray("u8", 4) })
	reserved: Uint8Array = new Uint8Array(4);

	constructor(props: { heads: RawEntryWithRefs[]; reserved?: Uint8Array }) {
		super();
		this.heads = props.heads;
		if (props.reserved) {
			this.reserved = props.reserved;
		}
	}
}

class PreparedRawExchangeEntry<T> extends Entry<T> {
	hash!: string;
	size: number;
	createdLocally?: boolean;
	meta: Meta;

	private materialized?: Entry<T>;
	private keychain?: unknown;
	private encodingValue?: Log<T>["encoding"];

	constructor(
		private readonly bytes: Uint8Array,
		private readonly facts: PreparedRawEntryV0Facts,
	) {
		super();
		this.size = facts.byteLength;
		this.meta = new Meta({
			gid: facts.gid,
			clock: new Clock({
				id: facts.clockId,
				timestamp: new Timestamp({
					wallTime: facts.wallTime,
					logical: facts.logical,
				}),
			}),
			next: facts.next,
			type: facts.type as EntryType,
			data: facts.metaData,
		});
	}

	init(props: any): this {
		this.keychain = props.keychain ?? props._keychain;
		this.encodingValue = props.encoding ?? props._encoding;
		this.materialized?.init(props);
		return this;
	}

	private get encoding(): Log<T>["encoding"] {
		if (!this.encodingValue) {
			throw new Error("Not initialized");
		}
		return this.encodingValue;
	}

	get payload() {
		return this.materialize().payload;
	}

	get signatures() {
		return this.materialize().signatures;
	}

	get publicKeys() {
		return this.materialize().publicKeys;
	}

	get __peerbitSignatureVerified() {
		return this.facts.signatureVerified;
	}

	getMeta(): Meta {
		return this.meta;
	}

	getMetaBytes(): Uint8Array | undefined {
		return this.facts.metaBytes;
	}

	getHashDigestBytes(): Uint8Array {
		return this.facts.hashDigestBytes;
	}

	getClock(): Clock {
		return this.meta.clock;
	}

	getNext(): string[] {
		return this.meta.next;
	}

	getSignatures() {
		return this.materialize().getSignatures();
	}

	getPayloadValue(): Promise<T> | T {
		return this.materialize().getPayloadValue();
	}

	override getStorageBytes(): Uint8Array {
		return this.bytes;
	}

	async verifySignatures(): Promise<boolean> {
		if (this.facts.signatureVerified) {
			return true;
		}
		try {
			const result = await verifyEntryV0Ed25519StorageBatch([this.bytes]);
			if (result) {
				return result.every(Boolean);
			}
		} catch {
			// Fall back to full materialization for encrypted or non-Ed25519 entries.
		}
		return this.materialize().verifySignatures();
	}

	equals(other: Entry<T>): boolean {
		return this.hash === other.hash || this.materialize().equals(other);
	}

	toSignable(): Entry<T> {
		return this.materialize().toSignable();
	}

	toShallow(isHead: boolean): ShallowEntry {
		return new ShallowEntry({
			hash: this.hash,
			payloadSize: this.facts.payloadByteLength,
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

	toPreparedAppendJoinFacts(): PreparedAppendJoinFacts {
		const shallowEntry = this.toShallow(true);
		const nativeEntry: PreparedNativeLogEntry = {
			hash: this.hash,
			gid: this.facts.gid,
			next: this.facts.next,
			type: this.facts.type,
			head: true,
			payloadSize: this.facts.payloadByteLength,
			data: this.facts.metaData,
			clock: {
				timestamp: {
					wallTime: this.facts.wallTime,
					logical: this.facts.logical,
				},
			},
		};
		return {
			hash: this.hash,
			bytes: this.bytes,
			byteLength: this.size,
			meta: this.meta,
			shallowEntry,
			nativeEntry,
			materializeEntry: () => this,
		};
	}

	private materialize(): Entry<T> {
		if (this.materialized) {
			return this.materialized;
		}
		const entry = deserialize(this.bytes, Entry) as Entry<T>;
		entry.hash = undefined as any;
		Entry.prepareMultihashBytes(entry, this.bytes, this.hash);
		entry.hash = this.hash;
		entry.size = this.size;
		entry.init({
			keychain: this.keychain as any,
			encoding: this.encoding,
		});
		prepareRawExchangeHeadEntryFacts(entry, {
			hash: this.hash,
			bytes: this.bytes,
			gidRefrences: [],
		});
		this.materialized = entry;
		return entry;
	}
}

export const getPreparedRawExchangeAppendFacts = (
	entry: Entry<any>,
): PreparedAppendJoinFacts | undefined =>
	entry instanceof PreparedRawExchangeEntry
		? entry.toPreparedAppendJoinFacts()
		: undefined;

@variant([0, 3])
export class RequestIPrune extends TransportMessage {
	// Hashes which I want to prune
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

@variant([0, 4])
export class ResponseIPrune extends TransportMessage {
	// Hashes I am allowed to prune
	@field({ type: vec("string") })
	hashes: string[];

	constructor(props: { hashes: string[] }) {
		super();
		this.hashes = props.hashes;
	}
}

const MAX_EXCHANGE_MESSAGE_SIZE = 1e5; // 100kb. Too large size might not be faster (even if we can do 5mb)
export const EXCHANGE_HEADS_RESOLVE_BATCH_SIZE = 256;

export const createExchangeHeadsMessages = async function* (
	log: Log<any>,
	heads: Entry<any>[] | string[] | Set<string>,
): AsyncGenerator<ExchangeHeadsMessage<any>, void, void> {
	let size = 0;
	let current: EntryWithRefs<any>[] = [];
	const visitedHeads = new Set<string>();
	const headArray = Array.isArray(heads) ? heads : [...heads];
	const canUseNativeReferenceGids = headArray.length === 1;

	for (
		let offset = 0;
		offset < headArray.length;
		offset += EXCHANGE_HEADS_RESOLVE_BATCH_SIZE
	) {
		const headBatch = headArray.slice(
			offset,
			offset + EXCHANGE_HEADS_RESOLVE_BATCH_SIZE,
		);
		const nativeReferenceRowsByPosition =
			canUseNativeReferenceGids === false
				? getNativeReferenceRowsByHeadInput(log, headBatch, visitedHeads)
				: undefined;
		const resolvedHeads = await resolveExchangeHeadEntries(
			log,
			headBatch,
			visitedHeads,
		);
		for (let i = 0; i < resolvedHeads.length; i++) {
			const entry = resolvedHeads[i];
			if (!entry) {
				continue; // missing this entry, could be deleted while iterating
			}

			if (visitedHeads.has(entry.hash)) {
				continue;
			}
			visitedHeads.add(entry.hash);

			const nativeGidReferences = canUseNativeReferenceGids
				? log.entryIndex.getUniqueReferenceGids(entry.hash)
				: undefined;
			if (nativeGidReferences) {
				if (nativeGidReferences.length > 1000) {
					warn("Large refs count: ", nativeGidReferences.length);
				}
				current.push(
					new EntryWithRefs({
						entry,
						gidRefrences: nativeGidReferences,
					}),
				);
				size += entry.size;
				if (size > MAX_EXCHANGE_MESSAGE_SIZE) {
					size = 0;
					yield new ExchangeHeadsMessage({
						heads: current,
					});
					current = [];
				}
				continue;
			}

			const nativeReferenceRows = nativeReferenceRowsByPosition?.[i];
			if (nativeReferenceRows) {
				const gidRefrences: string[] = [];
				for (const [hash, gid] of nativeReferenceRows) {
					if (visitedHeads.has(hash)) {
						continue;
					}
					visitedHeads.add(hash);
					gidRefrences.push(gid);
				}
				if (gidRefrences.length > 1000) {
					warn("Large refs count: ", gidRefrences.length);
				}
				current.push(
					new EntryWithRefs({
						entry,
						gidRefrences,
					}),
				);
				size += entry.size;
				if (size > MAX_EXCHANGE_MESSAGE_SIZE) {
					size = 0;
					yield new ExchangeHeadsMessage({
						heads: current,
					});
					current = [];
				}
				continue;
			}

			// TODO eventually we don't want to load all refs
			// since majority of the old leader would not be interested in these anymore
			const refs = (await allEntriesWithUniqueGids(log, entry)).filter((x) => {
				if (visitedHeads.has(x.hash)) {
					return false;
				}
				visitedHeads.add(x.hash);
				return true;
			});

			if (refs.length > 1000) {
				warn("Large refs count: ", refs.length);
			}
			current.push(
				new EntryWithRefs({
					entry,
					gidRefrences: refs.map((x) => x.meta.gid),
				}),
			);

			size += entry.size;
			if (size > MAX_EXCHANGE_MESSAGE_SIZE) {
				size = 0;
				yield new ExchangeHeadsMessage({
					heads: current,
				});
				current = [];
				continue;
			}
		}
	}
	if (current.length > 0) {
		yield new ExchangeHeadsMessage({
			heads: current,
		});
	}
};

export const createRawExchangeHeadsMessages = async function* (
	log: Log<any>,
	heads: string[] | Set<string>,
): AsyncGenerator<RawExchangeHeadsMessage | ExchangeHeadsMessage<any>, void, void> {
	let size = 0;
	let current: RawEntryWithRefs[] = [];
	const visitedHeads = new Set<string>();
	const headArray = Array.isArray(heads) ? heads : [...heads];

	for (let offset = 0; offset < headArray.length; offset += EXCHANGE_HEADS_RESOLVE_BATCH_SIZE) {
		const headBatch = headArray.slice(
			offset,
			offset + EXCHANGE_HEADS_RESOLVE_BATCH_SIZE,
		);
		const nativeReferenceRowsByPosition = getNativeReferenceRowsByHeadInput(
			log,
			headBatch,
			visitedHeads,
		);
		if (!nativeReferenceRowsByPosition) {
			for await (const message of createExchangeHeadsMessages(log, headBatch)) {
				yield message;
			}
			continue;
		}
		const blockRows = await resolveExchangeHeadBlocks(
			log,
			headBatch,
			visitedHeads,
		);
		if (!blockRows) {
			for await (const message of createExchangeHeadsMessages(log, headBatch)) {
				yield message;
			}
			continue;
		}

		for (let i = 0; i < blockRows.length; i++) {
			const block = blockRows[i];
			if (!block) {
				continue;
			}
			if (visitedHeads.has(block.hash)) {
				continue;
			}
			visitedHeads.add(block.hash);

			const nativeReferenceRows = nativeReferenceRowsByPosition[i];
			if (!nativeReferenceRows) {
				continue;
			}
			const gidRefrences: string[] = [];
			for (const [hash, gid] of nativeReferenceRows) {
				if (visitedHeads.has(hash)) {
					continue;
				}
				visitedHeads.add(hash);
				gidRefrences.push(gid);
			}
			if (gidRefrences.length > 1000) {
				warn("Large refs count: ", gidRefrences.length);
			}
			current.push(
				new RawEntryWithRefs({
					hash: block.hash,
					bytes: block.bytes,
					gidRefrences,
				}),
			);
			size += block.bytes.byteLength;
			if (size > MAX_EXCHANGE_MESSAGE_SIZE) {
				size = 0;
				yield new RawExchangeHeadsMessage({ heads: current });
				current = [];
			}
		}
	}
	if (current.length > 0) {
		yield new RawExchangeHeadsMessage({ heads: current });
	}
};

export const materializeRawExchangeHeadsMessage = (
	message: RawExchangeHeadsMessage,
	log: Log<any>,
): ExchangeHeadsMessage<any> => {
	const materialized = new ExchangeHeadsMessage({
		heads: message.heads.map((head) => {
			const entry = deserialize(head.bytes, Entry) as Entry<any>;
			entry.hash = head.hash;
			entry.size = head.bytes.byteLength;
			entry.init({
				keychain: log.keychain,
				encoding: log.encoding,
			});
			return new EntryWithRefs({
				entry,
				gidRefrences: head.gidRefrences,
			});
		}),
	});
	materialized.reserved = message.reserved;
	return materialized;
};

const prepareRawExchangeHeadEntryFacts = (
	entry: Entry<any>,
	head: RawEntryWithRefs,
) => {
	const meta = entry.meta;
	const payload = entry.payload;
	const payloadSize = payload.byteLength;
	const shallowEntry = new ShallowEntry({
		hash: head.hash,
		payloadSize,
		head: true,
		meta: new ShallowMeta({
			gid: meta.gid,
			data: meta.data,
			clock: meta.clock,
			next: meta.next,
			type: meta.type,
		}),
	});
	const nativeEntry: PreparedNativeLogEntry = {
		hash: head.hash,
		gid: meta.gid,
		next: meta.next,
		type: meta.type,
		head: true,
		payloadSize,
		data: meta.data,
		clock: {
			timestamp: {
				wallTime: meta.clock.timestamp.wallTime,
				logical: meta.clock.timestamp.logical,
			},
		},
	};
	Entry.prepareShallowEntry(entry, shallowEntry);
	Entry.prepareNativeLogEntry(entry, nativeEntry);
};

export const materializeVerifiedRawExchangeHeadsMessage = async (
	message: RawExchangeHeadsMessage,
	log: Log<any>,
	profile?: SyncProfileFn,
): Promise<ExchangeHeadsMessage<any>> => {
	const blocks = new Array<Uint8Array>(message.heads.length);
	let rawBytes = 0;
	for (let i = 0; i < message.heads.length; i++) {
		const bytes = message.heads[i]!.bytes;
		blocks[i] = bytes;
		rawBytes += bytes.byteLength;
	}
	const nativePrepareStartedAt = syncProfileStart(profile);
	const preparedFacts = await prepareRawEntryV0Batch(blocks).catch(
		() => undefined,
	);
	if (preparedFacts) {
		emitSyncProfileDuration(profile, nativePrepareStartedAt, {
			name: "sharedLog.rawReceive.prepareFacts",
			component: "shared-log",
			entries: message.heads.length,
			bytes: rawBytes,
			messages: 1,
			details: { native: true },
		});
		const wrapStartedAt = syncProfileStart(profile);
		const materialized = new ExchangeHeadsMessage({
			heads: message.heads.map((head, index) => {
				const facts = preparedFacts[index]!;
				if (facts.cid !== head.hash) {
					throw new Error("Raw exchange head hash did not match bytes");
				}
				const entry = new PreparedRawExchangeEntry(head.bytes, facts);
				Entry.prepareMultihashBytes(entry, head.bytes, head.hash);
				entry.hash = head.hash;
				entry.size = facts.byteLength;
				entry.init({
					keychain: log.keychain,
					encoding: log.encoding,
				});
				return new EntryWithRefs({
					entry,
					gidRefrences: head.gidRefrences,
				});
			}),
		});
		materialized.reserved = message.reserved;
		emitSyncProfileDuration(profile, wrapStartedAt, {
			name: "sharedLog.rawReceive.wrapPrepared",
			component: "shared-log",
			entries: message.heads.length,
			messages: 1,
		});
		return materialized;
	}
	emitSyncProfileDuration(profile, nativePrepareStartedAt, {
		name: "sharedLog.rawReceive.prepareFacts",
		component: "shared-log",
		entries: message.heads.length,
		bytes: rawBytes,
		messages: 1,
		details: { native: false },
	});
	const hashStartedAt = syncProfileStart(profile);
	const calculatedHashes = await calculateRawCidV1Batch(blocks);
	emitSyncProfileDuration(profile, hashStartedAt, {
		name: "sharedLog.rawReceive.calculateHashes",
		component: "shared-log",
		entries: message.heads.length,
		messages: 1,
	});
	const deserializeStartedAt = syncProfileStart(profile);
	const materialized = new ExchangeHeadsMessage({
		heads: message.heads.map((head, index) => {
			if (calculatedHashes[index] !== head.hash) {
				throw new Error("Raw exchange head hash did not match bytes");
			}
			const entry = deserialize(head.bytes, Entry) as Entry<any>;
			entry.hash = undefined as any;
			Entry.prepareMultihashBytes(entry, head.bytes, head.hash);
			entry.hash = head.hash;
			entry.size = head.bytes.byteLength;
			entry.init({
				keychain: log.keychain,
				encoding: log.encoding,
			});
			prepareRawExchangeHeadEntryFacts(entry, head);
			return new EntryWithRefs({
				entry,
				gidRefrences: head.gidRefrences,
			});
		}),
	});
	materialized.reserved = message.reserved;
	emitSyncProfileDuration(profile, deserializeStartedAt, {
		name: "sharedLog.rawReceive.deserializeFallback",
		component: "shared-log",
		entries: message.heads.length,
		messages: 1,
	});
	return materialized;
};

const getNativeReferenceRowsByHeadInput = (
	log: Log<any>,
	heads: Array<Entry<any> | string>,
	visitedHeads: Set<string>,
) => {
	const positions: number[] = [];
	const hashes: string[] = [];
	for (let i = 0; i < heads.length; i++) {
		const head = heads[i]!;
		const hash = head instanceof Entry ? head.hash : head;
		if (visitedHeads.has(hash)) {
			continue;
		}
		positions.push(i);
		hashes.push(hash);
	}
	if (hashes.length === 0) {
		return [];
	}
	const flatRows = log.entryIndex.getUniqueReferenceGidRowsFlatBatch(hashes);
	if (flatRows) {
		const byPosition: Array<Array<[string, string]> | undefined> = new Array(
			heads.length,
		);
		for (const position of positions) {
			byPosition[position] = [];
		}
		for (const [hashPosition, hash, gid] of flatRows) {
			const position = positions[hashPosition];
			if (position === undefined) {
				continue;
			}
			byPosition[position]!.push([hash, gid]);
		}
		return byPosition;
	}
	const rows = log.entryIndex.getUniqueReferenceGidRowsBatch(hashes);
	if (!rows) {
		return undefined;
	}
	const byPosition: Array<Array<[string, string]> | undefined> = new Array(
		heads.length,
	);
	for (let i = 0; i < rows.length; i++) {
		byPosition[positions[i]!] = rows[i];
	}
	return byPosition;
};

type BlocksWithGetMany = {
	getMany?: (
		cids: string[],
	) =>
		| Promise<Array<Uint8Array | undefined>>
		| Array<Uint8Array | undefined>;
	get: (cid: string) => Promise<Uint8Array | undefined> | Uint8Array | undefined;
};

const resolveExchangeHeadBlocks = async (
	log: Log<any>,
	headArray: string[],
	visitedHeads?: Set<string>,
): Promise<Array<{ hash: string; bytes: Uint8Array } | undefined> | undefined> => {
	const resolved: Array<{ hash: string; bytes: Uint8Array } | undefined> =
		new Array(headArray.length);
	const hashes: string[] = [];
	const positionsByHash = new Map<string, number[]>();
	for (let i = 0; i < headArray.length; i++) {
		const hash = headArray[i]!;
		if (visitedHeads?.has(hash)) {
			continue;
		}
		const positions = positionsByHash.get(hash);
		if (positions) {
			positions.push(i);
			continue;
		}
		hashes.push(hash);
		positionsByHash.set(hash, [i]);
	}
	if (hashes.length === 0) {
		return resolved;
	}

	const blocks = log.blocks as BlocksWithGetMany;
	const values =
		typeof blocks.getMany === "function"
			? await blocks.getMany(hashes)
			: await Promise.all(hashes.map((hash) => blocks.get(hash)));
	for (let i = 0; i < values.length; i++) {
		const hash = hashes[i]!;
		const bytes = values[i];
		if (!bytes) {
			return undefined;
		}
		for (const position of positionsByHash.get(hash)!) {
			resolved[position] = { hash, bytes };
		}
	}
	return resolved;
};

const resolveExchangeHeadEntries = async (
	log: Log<any>,
	headArray: Array<Entry<any> | string>,
	visitedHeads?: Set<string>,
): Promise<Array<Entry<any> | undefined>> => {
	const resolved: Array<Entry<any> | undefined> = new Array(headArray.length);
	const hashes: string[] = [];
	const positionsByHash = new Map<string, number[]>();
	for (let i = 0; i < headArray.length; i++) {
		const head = headArray[i]!;
		if (head instanceof Entry) {
			if (visitedHeads?.has(head.hash)) {
				continue;
			}
			resolved[i] = head;
			continue;
		}
		if (visitedHeads?.has(head)) {
			continue;
		}
		const positions = positionsByHash.get(head);
		if (positions) {
			positions.push(i);
			continue;
		}
		hashes.push(head);
		positionsByHash.set(head, [i]);
	}
	if (hashes.length === 0) {
		return resolved;
	}
	const entries =
		hashes.length === 1
			? [await log.get(hashes[0]!)]
			: await log.entryIndex.getMany(hashes, {
					type: "full",
					ignoreMissing: true,
				});
	for (let i = 0; i < entries.length; i++) {
		for (const position of positionsByHash.get(hashes[i]!)!) {
			resolved[position] = entries[i];
		}
	}
	return resolved;
};

export const allEntriesWithUniqueGids = async (
	log: Log<any>,
	entry: Entry<any>,
): Promise<Entry<any>[]> => {
	// TODO optimize this
	const map: Map<string, ShallowEntry | Entry<any>> = new Map();
	let curr: (Entry<any> | ShallowEntry)[] = [entry];
	while (curr.length > 0) {
		const nexts: (Entry<any> | ShallowEntry)[] = [];
		for (const element of curr) {
			if (!map.has(element.meta.gid)) {
				map.set(element.meta.gid, element);
				if (element.meta.type === EntryType.APPEND) {
					for (const next of element.meta.next) {
						const indexedEntry = await log.entryIndex.getShallow(next);
						if (!indexedEntry) {
							logger.error(
								"Failed to find indexed entry for hash when fetching references: " +
									next,
							);
						} else {
							nexts.push(indexedEntry.value);
						}
					}
				}
			}
			curr = nexts;
		}
	}
	const values = [...map.values()];
	const resolved: Array<Entry<any> | undefined> = new Array(values.length);
	const unresolvedHashes: string[] = [];
	const unresolvedPositions: number[] = [];
	for (let i = 0; i < values.length; i++) {
		const value = values[i]!;
		if (value instanceof Entry) {
			resolved[i] = value;
			continue;
		}
		unresolvedHashes.push(value.hash);
		unresolvedPositions.push(i);
	}
	if (unresolvedHashes.length > 0) {
		const entries = await log.entryIndex.getMany(unresolvedHashes, {
			type: "full",
			ignoreMissing: true,
		});
		for (let i = 0; i < entries.length; i++) {
			resolved[unresolvedPositions[i]!] = entries[i];
		}
	}
	return resolved.filter((x) => !!x) as Entry<any>[];
};
