import { deserialize, field, fixedArray, variant, vec } from "@dao-xyz/borsh";
import { cidifyString } from "@peerbit/blocks-interface";
import {
	LamportClock as Clock,
	Entry,
	EntryType,
	Meta,
	type PreparedAppendJoinFacts,
	type PreparedNativeLogEntry,
	type PreparedRawEntryV0Facts,
	ShallowEntry,
	ShallowMeta,
	Timestamp,
	calculateRawCidV1Batch,
	prepareRawEntryV0Batch,
	verifyEntryV0Ed25519StorageBatch,
} from "@peerbit/log";
import { Log } from "@peerbit/log";
import { logger as loggerFn } from "@peerbit/logger";
import { TransportMessage } from "./message.js";
import type { SyncProfileFn } from "./sync/index.js";
import {
	emitSyncProfileDuration,
	emitSyncProfileEvent,
	syncProfileStart,
} from "./sync/profile.js";

const logger = loggerFn("peerbit:shared-log:exchange-heads");
const warn = logger.newScope("warn");

type NativeBackboneAppendProfile = {
	nativeBackboneRawReceiveInputCopyMs: number;
	nativeBackboneRawReceivePrepareMs: number;
	nativeBackboneRawReceiveDigestMs: number;
	nativeBackboneRawReceiveCidStringMs: number;
	nativeBackboneRawReceiveExpectedCidMs: number;
	nativeBackboneRawReceiveStorageParseMs: number;
	nativeBackboneRawReceiveMetaParseMs: number;
	nativeBackboneRawReceivePayloadParseMs: number;
	nativeBackboneRawReceiveSignatureParseMs: number;
	nativeBackboneRawReceiveSignableMs: number;
	nativeBackboneRawReceiveVerifyBatchMs: number;
	nativeBackboneRawReceiveVerifyFallbackMs: number;
	nativeBackboneRawReceivePrepareColumnsMs: number;
};

type RawReceiveNativeBackbone = {
	prepareRawReceiveBatch(blocks: Uint8Array[]): PreparedRawEntryV0Facts[];
	prepareRawReceiveExpectedColumnsBatch?(
		blocks: Uint8Array[],
		hashes: string[],
		options?: { verifySignatures?: boolean },
	): PreparedRawEntryV0FactsColumns | undefined;
	prepareRawReceiveColumnsBatch?(
		blocks: Uint8Array[],
		hashes?: string[],
		options?: { verifySignatures?: boolean },
	): PreparedRawEntryV0FactsColumns | undefined;
	verifyPreparedRawReceiveEntries?(
		hashes: Iterable<string>,
	): boolean[] | undefined;
	clearPreparedRawReceiveEntries?(hashes: Iterable<string>): number;
	setAppendProfileEnabled?(enabled: boolean): void;
	resetAppendProfile?(): void;
	appendProfile?(): NativeBackboneAppendProfile;
};

const emitNativeBackboneRawPrepareProfile = (
	profile: SyncProfileFn | undefined,
	nativeProfile: NativeBackboneAppendProfile | undefined,
	entries: number,
	bytes: number,
) => {
	if (!profile || !nativeProfile) {
		return;
	}
	const events: Array<[name: string, durationMs: number]> = [
		[
			"sharedLog.rawReceive.nativePrepare.inputCopy",
			nativeProfile.nativeBackboneRawReceiveInputCopyMs,
		],
		[
			"sharedLog.rawReceive.nativePrepare.prepare",
			nativeProfile.nativeBackboneRawReceivePrepareMs,
		],
		[
			"sharedLog.rawReceive.nativePrepare.digest",
			nativeProfile.nativeBackboneRawReceiveDigestMs,
		],
		[
			"sharedLog.rawReceive.nativePrepare.cidString",
			nativeProfile.nativeBackboneRawReceiveCidStringMs,
		],
		[
			"sharedLog.rawReceive.nativePrepare.expectedCid",
			nativeProfile.nativeBackboneRawReceiveExpectedCidMs,
		],
		[
			"sharedLog.rawReceive.nativePrepare.storageParse",
			nativeProfile.nativeBackboneRawReceiveStorageParseMs,
		],
		[
			"sharedLog.rawReceive.nativePrepare.metaParse",
			nativeProfile.nativeBackboneRawReceiveMetaParseMs,
		],
		[
			"sharedLog.rawReceive.nativePrepare.payloadParse",
			nativeProfile.nativeBackboneRawReceivePayloadParseMs,
		],
		[
			"sharedLog.rawReceive.nativePrepare.signatureParse",
			nativeProfile.nativeBackboneRawReceiveSignatureParseMs,
		],
		[
			"sharedLog.rawReceive.nativePrepare.signable",
			nativeProfile.nativeBackboneRawReceiveSignableMs,
		],
		[
			"sharedLog.rawReceive.nativePrepare.verifyBatch",
			nativeProfile.nativeBackboneRawReceiveVerifyBatchMs,
		],
		[
			"sharedLog.rawReceive.nativePrepare.verifyFallback",
			nativeProfile.nativeBackboneRawReceiveVerifyFallbackMs,
		],
		[
			"sharedLog.rawReceive.nativePrepare.columns",
			nativeProfile.nativeBackboneRawReceivePrepareColumnsMs,
		],
	];
	for (const [name, durationMs] of events) {
		if (
			durationMs > 0 ||
			name === "sharedLog.rawReceive.nativePrepare.prepare"
		) {
			emitSyncProfileEvent(profile, {
				name,
				component: "shared-log",
				durationMs,
				entries,
				bytes,
				messages: 1,
			});
		}
	}
};

type PreparedRawEntryV0FactsColumns = [
	cids: string[],
	hashDigestBytes: Array<Uint8Array | undefined>,
	byteLengths: Uint32Array,
	clockIds: Uint8Array[],
	wallTimes: BigUint64Array,
	logicals: Uint32Array,
	gids: string[],
	nexts: string[][],
	types: Uint8Array,
	metaBytes: Uint8Array[],
	metaDatas: Array<Uint8Array | undefined>,
	payloadByteLengths: Uint32Array,
	signatureVerified: Uint8Array,
	requestedReplicas: Uint32Array,
	hashNumbers: string[] | BigUint64Array,
];

type PreparedRawEntryV0FactsSource =
	| PreparedRawEntryV0Facts
	| PreparedRawEntryV0FactsColumns;

const isPreparedRawEntryV0FactsColumns = (
	facts: PreparedRawEntryV0FactsSource,
): facts is PreparedRawEntryV0FactsColumns => Array.isArray(facts);

const preparedRawColumnValue = <T>(
	values: ArrayLike<T>,
	index: number,
	label: string,
): T => {
	const value = values[index];
	if (value === undefined) {
		throw new Error(`Missing prepared raw receive ${label}`);
	}
	return value;
};

const preparedRawCid = (facts: PreparedRawEntryV0FactsSource, index: number) =>
	isPreparedRawEntryV0FactsColumns(facts)
		? preparedRawColumnValue(facts[0], index, "cid")
		: facts.cid;

const preparedRawHashDigestBytes = (
	facts: PreparedRawEntryV0FactsSource,
	index: number,
	fallbackCid?: string,
) => {
	if (!isPreparedRawEntryV0FactsColumns(facts)) {
		return facts.hashDigestBytes;
	}
	const digest = facts[1][index];
	if (digest) {
		return digest;
	}
	const cid = facts[0][index] ?? fallbackCid;
	if (!cid) {
		throw new Error("Missing prepared raw receive cid");
	}
	return cidifyString(cid).multihash.digest;
};

const preparedRawByteLength = (
	facts: PreparedRawEntryV0FactsSource,
	index: number,
) =>
	isPreparedRawEntryV0FactsColumns(facts)
		? preparedRawColumnValue(facts[2], index, "byte length")
		: facts.byteLength;

const preparedRawClockId = (
	facts: PreparedRawEntryV0FactsSource,
	index: number,
) =>
	isPreparedRawEntryV0FactsColumns(facts)
		? preparedRawColumnValue(facts[3], index, "clock id")
		: facts.clockId;

const preparedRawWallTime = (
	facts: PreparedRawEntryV0FactsSource,
	index: number,
) =>
	isPreparedRawEntryV0FactsColumns(facts)
		? preparedRawColumnValue(facts[4], index, "wall time")
		: facts.wallTime;

const preparedRawLogical = (
	facts: PreparedRawEntryV0FactsSource,
	index: number,
) =>
	isPreparedRawEntryV0FactsColumns(facts)
		? preparedRawColumnValue(facts[5], index, "logical time")
		: facts.logical;

const preparedRawGid = (facts: PreparedRawEntryV0FactsSource, index: number) =>
	isPreparedRawEntryV0FactsColumns(facts)
		? preparedRawColumnValue(facts[6], index, "gid")
		: facts.gid;

const preparedRawNext = (
	facts: PreparedRawEntryV0FactsSource,
	index: number,
) =>
	isPreparedRawEntryV0FactsColumns(facts)
		? preparedRawColumnValue(facts[7], index, "next hashes")
		: facts.next;

const preparedRawType = (
	facts: PreparedRawEntryV0FactsSource,
	index: number,
) =>
	isPreparedRawEntryV0FactsColumns(facts)
		? preparedRawColumnValue(facts[8], index, "entry type")
		: facts.type;

const preparedRawMetaBytes = (
	facts: PreparedRawEntryV0FactsSource,
	index: number,
) =>
	isPreparedRawEntryV0FactsColumns(facts)
		? preparedRawColumnValue(facts[9], index, "meta bytes")
		: facts.metaBytes;

const preparedRawMetaData = (
	facts: PreparedRawEntryV0FactsSource,
	index: number,
) =>
	isPreparedRawEntryV0FactsColumns(facts) ? facts[10][index] : facts.metaData;

const preparedRawPayloadByteLength = (
	facts: PreparedRawEntryV0FactsSource,
	index: number,
) =>
	isPreparedRawEntryV0FactsColumns(facts)
		? preparedRawColumnValue(facts[11], index, "payload byte length")
		: facts.payloadByteLength;

const preparedRawSignatureVerified = (
	facts: PreparedRawEntryV0FactsSource,
	index: number,
) =>
	isPreparedRawEntryV0FactsColumns(facts)
		? preparedRawColumnValue(facts[12], index, "signature flag") !== 0
		: facts.signatureVerified;

const preparedRawRequestedReplicas = (
	facts: PreparedRawEntryV0FactsSource,
	index: number,
) => {
	const value = isPreparedRawEntryV0FactsColumns(facts)
		? facts[13]?.[index]
		: facts.requestedReplicas;
	return value && value > 0 ? value : undefined;
};

const preparedRawHashNumber = (
	facts: PreparedRawEntryV0FactsSource,
	index: number,
) =>
	isPreparedRawEntryV0FactsColumns(facts)
		? facts[14]?.[index]
		: facts.hashNumber;

const preparedRawFactsCount = (facts: PreparedRawEntryV0FactsColumns) =>
	facts[0].length || facts[2].length;

const preparedRawFactsHashes = (
	facts: PreparedRawEntryV0FactsColumns | PreparedRawEntryV0Facts[],
) =>
	Array.isArray(facts[0])
		? [...(facts as PreparedRawEntryV0FactsColumns)[0]]
		: (facts as PreparedRawEntryV0Facts[]).map((entry) => entry.cid);

const attachPreparedRawShallowFacts = (
	shallow: ShallowEntry,
	facts: PreparedRawEntryV0FactsSource,
	index: number,
	fallbackCid: string,
): ShallowEntry => {
	Object.defineProperties(shallow, {
		getMetaBytes: {
			value: () => preparedRawMetaBytes(facts, index),
			configurable: true,
		},
		getHashDigestBytes: {
			value: () => preparedRawHashDigestBytes(facts, index, fallbackCid),
			configurable: true,
		},
	});
	return shallow;
};

// Stored in the reserved bytes so older peers ignore the hint.
export const EXCHANGE_HEADS_REPAIR_HINT = 1;

/**
 * Capability bit: the peer can receive `RawExchangeHeadsMessage` ([0, 7]).
 * Advertised once per peer via {@link SyncCapabilitiesMessage} (and echoed by
 * the per-request capability messages of the simple synchronizer), so live
 * append gossip can pick the raw path without a per-request round trip.
 */
export const SYNC_CAPABILITY_RAW_EXCHANGE_HEADS = 1;

/**
 * One-shot capability advertisement, sent to a peer when it (or we) subscribe
 * to the program topic and `sync.rawExchangeHeads` is enabled. Peers that do
 * not know this message drop it as an unknown variant; peers that never
 * advertise keep receiving the plain `ExchangeHeadsMessage` path.
 */
@variant([0, 10])
export class SyncCapabilitiesMessage extends TransportMessage {
	@field({ type: "u32" })
	capabilities: number;

	constructor(props?: { capabilities?: number }) {
		super();
		this.capabilities =
			props?.capabilities ?? SYNC_CAPABILITY_RAW_EXCHANGE_HEADS;
	}
}

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

	preparedHashes?: string[];

	constructor(props: { heads: EntryWithRefs<T>[]; preparedHashes?: string[] }) {
		super();
		this.heads = props.heads;
		this.preparedHashes = props.preparedHashes;
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

/**
 * The stash surface a {@link StashBackedRawExchangeHeadsMessage} consumes:
 * entry block bytes kept in native (wasm) memory by the wire-level decoder,
 * keyed by the enclosing DataMessage id.
 */
export type RawExchangeHeadsStash = {
	stashedBlocks(
		id: Uint8Array,
		indexes?: Uint32Array,
	): Uint8Array[] | undefined;
	release(id: Uint8Array): boolean;
};

/**
 * A head of a stash-backed raw exchange message: hash/refs/length come from
 * stash metadata; `bytes` materializes lazily (and is counted) because the
 * fused receive path never needs the block bytes in JS.
 */
export class StashBackedRawEntryWithRefs {
	private bytesValue?: Uint8Array;

	constructor(
		private readonly message: StashBackedRawExchangeHeadsMessage,
		readonly stashIndex: number,
		readonly hash: string,
		readonly gidRefrences: string[],
		readonly byteLength: number,
	) {}

	get bytes(): Uint8Array {
		return (this.bytesValue ??= this.message.materializeHeadBytes(
			this.stashIndex,
			this.hash,
		));
	}
}

/**
 * A raw exchange-heads message resolved from the native wire stash instead of
 * a TS borsh decode: JS holds only head facts while the entry block bytes stay
 * in wasm memory until `prepareStashedRawReceive*` consumes them there — or a
 * fallback path materializes them per head.
 */
export class StashBackedRawExchangeHeadsMessage extends RawExchangeHeadsMessage {
	readonly messageId: Uint8Array;
	/** Wasm-to-JS head byte materializations (0 on the fully fused path). */
	bytesMaterializedCount = 0;
	private released = false;
	private readonly stash: RawExchangeHeadsStash;
	private readonly resolveReleasedBlock?: (
		hash: string,
	) => Uint8Array | undefined;

	constructor(properties: {
		messageId: Uint8Array;
		hashes: string[];
		gidRefrences: string[][];
		byteLengths: Uint32Array;
		reserved: Uint8Array;
		stash: RawExchangeHeadsStash;
		resolveReleasedBlock?: (hash: string) => Uint8Array | undefined;
	}) {
		const heads = new Array<RawEntryWithRefs>(properties.hashes.length);
		super({ heads, reserved: properties.reserved });
		this.messageId = properties.messageId;
		this.stash = properties.stash;
		this.resolveReleasedBlock = properties.resolveReleasedBlock;
		for (let i = 0; i < properties.hashes.length; i++) {
			heads[i] = new StashBackedRawEntryWithRefs(
				this,
				i,
				properties.hashes[i]!,
				properties.gidRefrences[i] ?? [],
				properties.byteLengths[i]!,
			) as unknown as RawEntryWithRefs;
		}
	}

	materializeHeadBytes(index: number, hash: string): Uint8Array {
		if (!this.released) {
			const bytes = this.stash.stashedBlocks(
				this.messageId,
				Uint32Array.of(index),
			)?.[0];
			if (bytes) {
				this.bytesMaterializedCount++;
				return bytes;
			}
		}
		const bytes = this.resolveReleasedBlock?.(hash);
		if (bytes) {
			this.bytesMaterializedCount++;
			return bytes;
		}
		throw new Error(
			"Stashed raw exchange head bytes are no longer available: " + hash,
		);
	}

	release(): boolean {
		if (this.released) {
			return false;
		}
		this.released = true;
		return this.stash.release(this.messageId);
	}
}

export const isStashBackedRawExchangeHeadsMessage = (
	message: TransportMessage,
): message is StashBackedRawExchangeHeadsMessage =>
	message instanceof StashBackedRawExchangeHeadsMessage;

export const getRawExchangeHeadByteLength = (head: RawEntryWithRefs): number =>
	head instanceof StashBackedRawEntryWithRefs
		? head.byteLength
		: head.bytes.byteLength;

/**
 * Stash indexes for a subset of a stash-backed message's heads (in subset
 * order), or undefined when any head is not stash-backed.
 */
export const getRawExchangeHeadStashIndexes = (
	heads: RawEntryWithRefs[],
): Uint32Array | undefined => {
	const indexes = new Uint32Array(heads.length);
	for (let i = 0; i < heads.length; i++) {
		const head = heads[i]!;
		if (!(head instanceof StashBackedRawEntryWithRefs)) {
			return undefined;
		}
		indexes[i] = head.stashIndex;
	}
	return indexes;
};

export type RawReceiveHashSelection =
	| Iterable<string>
	| {
			hashes?: Iterable<string>;
			indexes?: Iterable<number>;
			droppedIndexes?: Iterable<number>;
	  };

const isIterableRawReceiveHashSelection = (
	selection: RawReceiveHashSelection,
): selection is Iterable<string> =>
	typeof (selection as Iterable<string>)[Symbol.iterator] === "function";

type RawExchangeEntryMaterializeOptions<T> = {
	bytes: Uint8Array;
	hash: string;
	gidRefrences: string[];
	size: number;
	keychain: unknown;
	encoding: Log<T>["encoding"];
};

const materializeRawExchangeEntry = <T>(
	options: RawExchangeEntryMaterializeOptions<T>,
): Entry<T> => {
	const entry = deserialize(options.bytes, Entry) as Entry<T>;
	(entry as unknown as { hash: string | undefined }).hash = undefined;
	Entry.prepareMultihashBytes(entry, options.bytes, options.hash);
	entry.hash = options.hash;
	entry.size = options.size;
	entry.init({
		keychain: options.keychain as any,
		encoding: options.encoding,
	});
	prepareRawExchangeHeadEntryFacts(entry, {
		hash: options.hash,
		bytes: options.bytes,
		gidRefrences: options.gidRefrences,
	});
	return entry;
};

class PreparedRawExchangeEntry<T> extends Entry<T> {
	hash!: string;
	size: number;
	createdLocally?: boolean;

	private materialized?: Entry<T>;
	private metaValue?: Meta;
	private shallowHeadValue?: ShallowEntry;
	private keychain?: unknown;
	private encodingValue?: Log<T>["encoding"];
	private bytesValue?: Uint8Array;

	constructor(
		private readonly bytesSource: () => Uint8Array,
		private readonly facts: PreparedRawEntryV0FactsSource,
		private readonly factsIndex = 0,
		private readonly onJsEntryDecode?: () => void,
	) {
		super();
		this.size = preparedRawByteLength(facts, factsIndex);
	}

	/**
	 * Lazy: stash-backed heads keep the block bytes in wasm memory; they are
	 * copied out only when a consumer actually needs them (payload/signature
	 * materialization, storage bytes).
	 */
	private get bytes(): Uint8Array {
		return (this.bytesValue ??= this.bytesSource());
	}

	get meta(): Meta {
		return (this.metaValue ??= new Meta({
			gid: preparedRawGid(this.facts, this.factsIndex),
			clock: new Clock({
				id: preparedRawClockId(this.facts, this.factsIndex),
				timestamp: new Timestamp({
					wallTime: preparedRawWallTime(this.facts, this.factsIndex),
					logical: preparedRawLogical(this.facts, this.factsIndex),
				}),
			}),
			next: preparedRawNext(this.facts, this.factsIndex),
			type: preparedRawType(this.facts, this.factsIndex) as EntryType,
			data: preparedRawMetaData(this.facts, this.factsIndex),
		}));
	}

	set meta(value: Meta) {
		this.metaValue = value;
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
		return preparedRawSignatureVerified(this.facts, this.factsIndex);
	}

	get __peerbitRequestedReplicas() {
		return preparedRawRequestedReplicas(this.facts, this.factsIndex);
	}

	get __peerbitHashNumber() {
		return preparedRawHashNumber(this.facts, this.factsIndex);
	}

	get __peerbitGid() {
		return preparedRawGid(this.facts, this.factsIndex);
	}

	get __peerbitWallTime() {
		return preparedRawWallTime(this.facts, this.factsIndex);
	}

	get __peerbitLogical() {
		return preparedRawLogical(this.facts, this.factsIndex);
	}

	get __peerbitNext() {
		return preparedRawNext(this.facts, this.factsIndex);
	}

	getMeta(): Meta {
		return this.meta;
	}

	getMetaBytes(): Uint8Array | undefined {
		return preparedRawMetaBytes(this.facts, this.factsIndex);
	}

	getHashDigestBytes(): Uint8Array {
		return preparedRawHashDigestBytes(
			this.facts,
			this.factsIndex,
			this.hash,
		);
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
		if (preparedRawSignatureVerified(this.facts, this.factsIndex)) {
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

	/**
	 * Read boundary: when this lazy head is resolved from the entry-index cache
	 * a consumer is about to read it, so decode into a full {@link EntryV0}
	 * whose meta/payload/signature fields are populated. The entry index caches
	 * the returned entry, replacing this hollow wrapper, so subsequent reads and
	 * `EntryV0.equals` see the canonical entry. Only reached on the read path;
	 * the wire/sync fusion path never resolves cached heads, so laziness there
	 * is preserved.
	 */
	override toMaterialized(): Entry<T> {
		return this.materialize();
	}

	toSignable(): Entry<T> {
		return this.materialize().toSignable();
	}

	toShallow(isHead: boolean): ShallowEntry {
		if (isHead && this.shallowHeadValue) {
			this.shallowHeadValue.head = true;
			return this.shallowHeadValue;
		}
		const clock = this.getClock();
		const shallow = attachPreparedRawShallowFacts(
			new ShallowEntry({
				hash: this.hash,
				payloadSize: preparedRawPayloadByteLength(
					this.facts,
					this.factsIndex,
				),
				head: isHead,
				meta: new ShallowMeta({
					gid: preparedRawGid(this.facts, this.factsIndex),
					data: preparedRawMetaData(this.facts, this.factsIndex),
					clock,
					next: preparedRawNext(this.facts, this.factsIndex),
					type: preparedRawType(this.facts, this.factsIndex) as EntryType,
				}),
			}),
			this.facts,
			this.factsIndex,
			this.hash,
		);
		if (isHead) {
			this.shallowHeadValue = shallow;
		}
		return shallow;
	}

	toPreparedAppendJoinFacts(): PreparedAppendJoinFacts {
		const shallow = this.toShallow(true);
		// eslint-disable-next-line @typescript-eslint/no-this-alias
		const self = this;
		return {
			hash: this.hash,
			// Lazy: stash-backed heads keep entry bytes in wasm memory and the
			// native prepared-join commit never reads them in JS.
			get bytes() {
				return self.bytes;
			},
			byteLength: this.size,
			meta: shallow.meta,
			getShallowEntry: (isHead = true) => this.toShallow(isHead),
			materializeEntry: () => this,
		};
	}

	private materialize(): Entry<T> {
		if (this.materialized) {
			return this.materialized;
		}
		this.onJsEntryDecode?.();
		const entry = materializeRawExchangeEntry<T>({
			hash: this.hash,
			bytes: this.bytes,
			size: this.size,
			keychain: this.keychain,
			encoding: this.encoding,
			gidRefrences: [],
		});
		this.materialized = entry;
		return entry;
	}
}

class PreparedRawEntryWithRefs<T> {
	readonly gidRefrences: string[];
	private entryValue?: PreparedRawExchangeEntry<T>;
	private shallowHeadValue?: ShallowEntry;
	private keychain?: unknown;
	private encodingValue?: Log<T>["encoding"];

	constructor(
		private readonly head: RawEntryWithRefs,
		private readonly facts: PreparedRawEntryV0FactsSource,
		private readonly factsIndex = 0,
		private readonly onJsEntryDecode?: () => void,
	) {
		this.gidRefrences = head.gidRefrences;
	}

	get entry(): Entry<T> {
		if (!this.entryValue) {
			const head = this.head;
			const entry = new PreparedRawExchangeEntry<T>(
				() => head.bytes,
				this.facts,
				this.factsIndex,
				this.onJsEntryDecode,
			);
			// Lazy block registration: stash-backed heads keep the bytes in
			// wasm memory until a consumer (block store put, payload
			// materialization) actually pulls them.
			Entry.prepareMultihashBytesLazy(
				entry,
				head.hash,
				preparedRawByteLength(this.facts, this.factsIndex),
				() => head.bytes,
			);
			entry.hash = head.hash;
			entry.size = preparedRawByteLength(this.facts, this.factsIndex);
			if (this.keychain || this.encodingValue) {
				entry.init({
					keychain: this.keychain as any,
					encoding: this.encodingValue,
				});
			}
			this.entryValue = entry;
		}
		return this.entryValue;
	}

	set entry(value: Entry<T>) {
		if (value) {
			this.entryValue = value as PreparedRawExchangeEntry<T>;
		}
	}

	initEntry(props: any): void {
		this.keychain = props.keychain ?? props._keychain;
		this.encodingValue = props.encoding ?? props._encoding;
		this.entryValue?.init(props);
	}

	get hash(): string {
		return this.head.hash;
	}

	get preparedGid(): string {
		return preparedRawGid(this.facts, this.factsIndex);
	}

	get preparedRequestedReplicas(): number | undefined {
		return preparedRawRequestedReplicas(this.facts, this.factsIndex);
	}

	get preparedSignatureVerified(): boolean {
		return preparedRawSignatureVerified(this.facts, this.factsIndex);
	}

	toShallow(isHead = true): ShallowEntry {
		if (isHead && this.shallowHeadValue) {
			this.shallowHeadValue.head = true;
			return this.shallowHeadValue;
		}
		const clock = new Clock({
			id: preparedRawClockId(this.facts, this.factsIndex),
			timestamp: new Timestamp({
				wallTime: preparedRawWallTime(this.facts, this.factsIndex),
				logical: preparedRawLogical(this.facts, this.factsIndex),
			}),
		});
		const shallow = attachPreparedRawShallowFacts(
			new ShallowEntry({
				hash: this.head.hash,
				payloadSize: preparedRawPayloadByteLength(
					this.facts,
					this.factsIndex,
				),
				head: isHead,
				meta: new ShallowMeta({
					gid: preparedRawGid(this.facts, this.factsIndex),
					data: preparedRawMetaData(this.facts, this.factsIndex),
					clock,
					next: preparedRawNext(this.facts, this.factsIndex),
					type: preparedRawType(this.facts, this.factsIndex) as EntryType,
				}),
			}),
			this.facts,
			this.factsIndex,
			this.head.hash,
		);
		if (isHead) {
			this.shallowHeadValue = shallow;
		}
		return shallow;
	}

	toPreparedAppendJoinFacts(): PreparedAppendJoinFacts {
		const shallow = this.toShallow(true);
		const head = this.head;
		return {
			hash: head.hash,
			// Lazy: stash-backed heads keep entry bytes in wasm memory and the
			// native prepared-join commit never reads them in JS.
			get bytes() {
				return head.bytes;
			},
			byteLength: preparedRawByteLength(this.facts, this.factsIndex),
			meta: shallow.meta,
			getShallowEntry: (isHead = true) => this.toShallow(isHead),
			materializeEntry: () => this.entry,
		};
	}
}

export const isPreparedRawEntryWithRefs = (
	head: EntryWithRefs<any>,
): head is EntryWithRefs<any> & PreparedRawEntryWithRefs<any> =>
	head instanceof PreparedRawEntryWithRefs;

export const getExchangeHeadHash = (head: EntryWithRefs<any>): string =>
	isPreparedRawEntryWithRefs(head) ? head.hash : head.entry.hash;

export const initExchangeHeadEntry = (
	head: EntryWithRefs<any>,
	props: any,
): void => {
	if (isPreparedRawEntryWithRefs(head)) {
		head.initEntry(props);
		return;
	}
	head.entry.init(props);
};

export const getPreparedRawExchangeHeadGid = (
	head: EntryWithRefs<any>,
): string | undefined =>
	isPreparedRawEntryWithRefs(head) ? head.preparedGid : undefined;

export const getPreparedRawExchangeHeadRequestedReplicas = (
	head: EntryWithRefs<any>,
): number | undefined =>
	isPreparedRawEntryWithRefs(head) ? head.preparedRequestedReplicas : undefined;

export const getPreparedRawExchangeHeadSignatureVerified = (
	head: EntryWithRefs<any>,
): boolean | undefined =>
	isPreparedRawEntryWithRefs(head) ? head.preparedSignatureVerified : undefined;

export const getPreparedRawExchangeHeadShallowEntry = (
	head: EntryWithRefs<any>,
): ShallowEntry | undefined =>
	isPreparedRawEntryWithRefs(head) ? head.toShallow(true) : undefined;

export const getPreparedRawExchangeHeadAppendFacts = (
	head: EntryWithRefs<any>,
): PreparedAppendJoinFacts | undefined =>
	isPreparedRawEntryWithRefs(head)
		? head.toPreparedAppendJoinFacts()
		: getPreparedRawExchangeAppendFacts(head.entry);

export const getPreparedRawExchangeAppendFacts = (
	entry: Entry<any>,
): PreparedAppendJoinFacts | undefined =>
	entry instanceof PreparedRawExchangeEntry
		? entry.toPreparedAppendJoinFacts()
		: undefined;

export const getPreparedRawExchangeRequestedReplicas = (
	entry: Entry<any>,
): number | undefined =>
	entry instanceof PreparedRawExchangeEntry
		? entry.__peerbitRequestedReplicas
		: undefined;

export const getPreparedRawExchangeHashNumber = (
	entry: Entry<any>,
): string | bigint | undefined =>
	entry instanceof PreparedRawExchangeEntry
		? entry.__peerbitHashNumber
		: undefined;

export const getPreparedRawExchangeGid = (
	entry: Entry<any>,
): string | undefined =>
	entry instanceof PreparedRawExchangeEntry ? entry.__peerbitGid : undefined;

export const getPreparedRawExchangeTimestamp = (
	entry: Entry<any>,
): { wallTime: bigint; logical: number } | undefined =>
	entry instanceof PreparedRawExchangeEntry
		? {
				wallTime: entry.__peerbitWallTime,
				logical: entry.__peerbitLogical,
			}
		: undefined;

export const getPreparedRawExchangeNext = (
	entry: Entry<any>,
): string[] | undefined =>
	entry instanceof PreparedRawExchangeEntry ? entry.__peerbitNext : undefined;

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

@variant(0)
export class CheckedPruneRequest {
	@field({ type: "string" })
	hash: string;

	@field({ type: fixedArray("u8", 32) })
	requestId: Uint8Array;

	constructor(props: { hash: string; requestId: Uint8Array }) {
		this.hash = props.hash;
		this.requestId = props.requestId;
	}
}

@variant([0, 11])
export class RequestIPruneV2 extends TransportMessage {
	@field({ type: vec(CheckedPruneRequest) })
	requests: CheckedPruneRequest[];

	constructor(props: {
		requests: Array<
			CheckedPruneRequest | { hash: string; requestId: Uint8Array }
		>;
	}) {
		super();
		this.requests = props.requests.map((request) =>
			request instanceof CheckedPruneRequest
				? request
				: new CheckedPruneRequest(request),
		);
	}
}

@variant([0, 12])
export class ResponseIPruneV2 extends TransportMessage {
	@field({ type: vec(CheckedPruneRequest) })
	requests: CheckedPruneRequest[];

	constructor(props: {
		requests: Array<
			CheckedPruneRequest | { hash: string; requestId: Uint8Array }
		>;
	}) {
		super();
		this.requests = props.requests.map((request) =>
			request instanceof CheckedPruneRequest
				? request
				: new CheckedPruneRequest(request),
		);
	}
}

const MAX_EXCHANGE_MESSAGE_SIZE = 1e5; // 100kb. Too large size might not be faster (even if we can do 5mb)
export const MAX_RAW_EXCHANGE_MESSAGE_SIZE = 512 * 1024;
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

			// Fallback for logs without native reference rows.
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
	profile?: SyncProfileFn,
): AsyncGenerator<RawExchangeHeadsMessage | ExchangeHeadsMessage<any>, void, void> {
	let size = 0;
	let current: RawEntryWithRefs[] = [];
	const visitedHeads = new Set<string>();
	const headArray = Array.isArray(heads) ? heads : [...heads];
	// This path materializes entry block bytes as JS values (log.blocks reads)
	// and TS-serializes the message; the fused wasm send path replaces it.
	// The event makes the remaining JS-side outbound block copies countable.
	const emitJsBlockBytes = (heads: RawEntryWithRefs[], bytes: number) => {
		if (profile) {
			emitSyncProfileEvent(profile, {
				name: "sharedLog.rawSend.jsBlockBytes",
				component: "shared-log",
				entries: heads.length,
				bytes,
				messages: 1,
			});
		}
	};

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
			if (size > MAX_RAW_EXCHANGE_MESSAGE_SIZE) {
				emitJsBlockBytes(current, size);
				size = 0;
				yield new RawExchangeHeadsMessage({ heads: current });
				current = [];
			}
		}
	}
	if (current.length > 0) {
		emitJsBlockBytes(current, size);
		yield new RawExchangeHeadsMessage({ heads: current });
	}
};

export type RawExchangeHeadSendPlan = {
	hashes: string[];
	gidRefrences: string[][];
};

/**
 * The head/reference selection of {@link createRawExchangeHeadsMessages}
 * without resolving any block bytes: same visited-head deduplication, same
 * reference-gid collection, same batching of the native index lookups. Used
 * by the fused send path, where the block bytes stay in the native store and
 * the payload is serialized in wasm. Returns `undefined` when the log has no
 * native reference rows (callers fall back to the TS message path).
 */
export const collectRawExchangeHeadSendPlan = (
	log: Log<any>,
	heads: string[] | Set<string>,
): RawExchangeHeadSendPlan | undefined => {
	const headArray = Array.isArray(heads) ? heads : [...heads];
	const visitedHeads = new Set<string>();
	const hashes: string[] = [];
	const gidRefrences: string[][] = [];
	for (
		let offset = 0;
		offset < headArray.length;
		offset += EXCHANGE_HEADS_RESOLVE_BATCH_SIZE
	) {
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
			return undefined;
		}
		for (let i = 0; i < headBatch.length; i++) {
			const hash = headBatch[i]!;
			if (visitedHeads.has(hash)) {
				continue;
			}
			visitedHeads.add(hash);
			const nativeReferenceRows = nativeReferenceRowsByPosition[i];
			if (!nativeReferenceRows) {
				continue;
			}
			const refs: string[] = [];
			for (const [refHash, gid] of nativeReferenceRows) {
				if (visitedHeads.has(refHash)) {
					continue;
				}
				visitedHeads.add(refHash);
				refs.push(gid);
			}
			if (refs.length > 1000) {
				warn("Large refs count: ", refs.length);
			}
			hashes.push(hash);
			gidRefrences.push(refs);
		}
	}
	return { hashes, gidRefrences };
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
	options?: {
		nativeBackbone?: RawReceiveNativeBackbone;
		verifyNativeBackboneSignaturesDuringPrepare?: boolean;
		deferNativeBackboneSignatureVerificationUntilSelection?: boolean;
		deferNativeBackboneSignatureVerificationUntilCommit?: boolean;
		prepareNativeBackboneExpectedColumnsAndSelection?: (properties: {
			/** Lazy so the fused (stash-backed) path never builds JS block arrays. */
			blocks: () => Uint8Array[];
			hashes: string[];
			verifySignatures: boolean;
		}) =>
			| { columns: PreparedRawEntryV0FactsColumns }
			| undefined
			| Promise<{ columns: PreparedRawEntryV0FactsColumns } | undefined>;
		/**
		 * Stash-backed expected-columns prepare (blocks stay in wasm memory).
		 * Tried before the blocks-based nativeBackbone variants; undefined
		 * falls through to them.
		 */
		prepareNativeBackboneExpectedColumns?: (properties: {
			hashes: string[];
			verifySignatures: boolean;
		}) => PreparedRawEntryV0FactsColumns | undefined;
		tryPreparedRawReceiveFastDrop?: (properties: {
			heads: RawEntryWithRefs[];
			hashes: string[];
		}) => boolean | Promise<boolean>;
		selectPreparedRawReceiveHashes?: (properties: {
			heads: RawEntryWithRefs[];
			hashes: string[];
		}) =>
			| RawReceiveHashSelection
			| undefined
			| Promise<RawReceiveHashSelection | undefined>;
	},
): Promise<ExchangeHeadsMessage<any> | undefined> => {
	const hashes = new Array<string>(message.heads.length);
	let rawBytes = 0;
	for (let i = 0; i < message.heads.length; i++) {
		const head = message.heads[i]!;
		hashes[i] = head.hash;
		rawBytes += getRawExchangeHeadByteLength(head);
	}
	// Built lazily: the fused (stash-backed) prepare paths keep the entry
	// block bytes in wasm memory and never need a JS blocks array.
	let blocksValue: Uint8Array[] | undefined;
	const blocks = () =>
		(blocksValue ??= message.heads.map((head) => head.bytes));
	const nativePrepareStartedAt = syncProfileStart(profile);
	let preparedFacts: PreparedRawEntryV0Facts[] | undefined;
	let preparedColumns: PreparedRawEntryV0FactsColumns | undefined;
	let nativePrepareSource: "backbone-columns" | "backbone" | "log" | undefined;
	let hashesVerifiedByNative = false;
	const requestedVerifyDuringPrepare =
		options?.verifyNativeBackboneSignaturesDuringPrepare === true;
	const canDeferPreparedSelectionVerification =
		requestedVerifyDuringPrepare &&
		options?.deferNativeBackboneSignatureVerificationUntilSelection === true &&
		!!options?.nativeBackbone?.verifyPreparedRawReceiveEntries &&
		(!!options?.tryPreparedRawReceiveFastDrop ||
			!!options?.selectPreparedRawReceiveHashes);
	const verifySignaturesInPrepare =
		requestedVerifyDuringPrepare && !canDeferPreparedSelectionVerification;
	if (options?.nativeBackbone) {
		const profileNativeBackbone =
			!!profile &&
			!!options.nativeBackbone.setAppendProfileEnabled &&
			!!options.nativeBackbone.resetAppendProfile &&
			!!options.nativeBackbone.appendProfile;
		if (profileNativeBackbone) {
			options.nativeBackbone.resetAppendProfile?.();
			options.nativeBackbone.setAppendProfileEnabled?.(true);
		}
		try {
			const preparedColumnsAndSelection =
				await options.prepareNativeBackboneExpectedColumnsAndSelection?.({
					blocks,
					hashes,
					verifySignatures: verifySignaturesInPrepare,
				});
			preparedColumns = preparedColumnsAndSelection?.columns;
			preparedColumns ??= options.prepareNativeBackboneExpectedColumns?.({
				hashes,
				verifySignatures: verifySignaturesInPrepare,
			});
			preparedColumns ??=
				options.nativeBackbone.prepareRawReceiveExpectedColumnsBatch?.(
					blocks(),
					hashes,
					{ verifySignatures: verifySignaturesInPrepare },
				);
			hashesVerifiedByNative = !!preparedColumns;
			preparedColumns ??=
				options.nativeBackbone.prepareRawReceiveColumnsBatch?.(
					blocks(),
					hashes,
					{
						verifySignatures: verifySignaturesInPrepare,
					},
				);
			if (preparedColumns) {
				nativePrepareSource = "backbone-columns";
			} else {
				preparedFacts = options.nativeBackbone.prepareRawReceiveBatch(blocks());
				nativePrepareSource = "backbone";
			}
		} catch {
			preparedColumns = undefined;
			preparedFacts = undefined;
		} finally {
			if (profileNativeBackbone) {
				options.nativeBackbone.setAppendProfileEnabled?.(false);
				emitNativeBackboneRawPrepareProfile(
					profile,
					options.nativeBackbone.appendProfile?.(),
					message.heads.length,
					rawBytes,
				);
			}
		}
	}
	if (!preparedColumns && !preparedFacts) {
		preparedFacts = await prepareRawEntryV0Batch(blocks())
			.then((facts) => {
				nativePrepareSource = "log";
				return facts;
			})
			.catch(() => undefined);
	}
	if (preparedColumns || preparedFacts) {
		const clearPreparedRawHashes = (hashesToClear: Iterable<string>) => {
			if (nativePrepareSource === "backbone") {
				options?.nativeBackbone?.clearPreparedRawReceiveEntries?.(
					hashesToClear,
				);
				return;
			}
			if (nativePrepareSource === "backbone-columns") {
				options?.nativeBackbone?.clearPreparedRawReceiveEntries?.(
					hashesToClear,
				);
			}
		};
		const clearPreparedRaw = () => {
			clearPreparedRawHashes(
				nativePrepareSource === "backbone"
					? preparedRawFactsHashes(preparedFacts!)
					: hashes,
			);
		};
		emitSyncProfileDuration(profile, nativePrepareStartedAt, {
			name: "sharedLog.rawReceive.prepareFacts",
			component: "shared-log",
			entries: message.heads.length,
			bytes: rawBytes,
			messages: 1,
			details: {
				native: true,
				source: nativePrepareSource,
				verifySignatures: verifySignaturesInPrepare,
				deferredVerifySignatures: canDeferPreparedSelectionVerification,
				deferredVerifySignaturesUntilCommit:
					canDeferPreparedSelectionVerification &&
					options?.deferNativeBackboneSignatureVerificationUntilCommit ===
						true,
			},
		});
		try {
			if (
				preparedColumns &&
				preparedRawFactsCount(preparedColumns) !== message.heads.length
			) {
				throw new Error("Raw exchange head prepared column count mismatch");
			}
			if (preparedFacts && preparedFacts.length !== message.heads.length) {
				throw new Error("Raw exchange head prepared fact count mismatch");
			}
			if (!hashesVerifiedByNative) {
				const rowFacts = preparedFacts!;
				for (let i = 0; i < message.heads.length; i++) {
					const head = message.heads[i]!;
					const facts = preparedColumns ?? rowFacts[i]!;
					if (preparedRawCid(facts, i) !== head.hash) {
						throw new Error("Raw exchange head hash did not match bytes");
					}
				}
			}
			if (
				options?.tryPreparedRawReceiveFastDrop &&
				(await options.tryPreparedRawReceiveFastDrop({
					heads: message.heads,
					hashes,
				}))
			) {
				return undefined;
			}
			let selectedHeads = message.heads;
			let selectedHashes = hashes;
			let selectedIndexes: number[] | undefined;
			const selectedHashSelection =
				await options?.selectPreparedRawReceiveHashes?.({
					heads: message.heads,
					hashes,
				});
			if (selectedHashSelection) {
				const selectedHashSelectionObject =
					!isIterableRawReceiveHashSelection(selectedHashSelection)
						? selectedHashSelection
						: undefined;
				const selectedIndexIterable = selectedHashSelectionObject?.indexes;
				if (selectedIndexIterable) {
					const selectedFlags = new Uint8Array(hashes.length);
					selectedHeads = [];
					selectedHashes = [];
					selectedIndexes = [];
					for (const rawIndex of selectedIndexIterable) {
						if (
							!Number.isInteger(rawIndex) ||
							rawIndex < 0 ||
							rawIndex >= hashes.length
						) {
							throw new Error("Selected unknown raw receive index");
						}
						const index = rawIndex;
						if (selectedFlags[index]) {
							throw new Error("Selected duplicate raw receive index");
						}
						selectedFlags[index] = 1;
						selectedHeads.push(message.heads[index]!);
						selectedHashes.push(hashes[index]!);
						selectedIndexes.push(index);
					}
					let droppedHashes: string[] | undefined;
					if (selectedHashSelectionObject.droppedIndexes) {
						const droppedFlags = new Uint8Array(hashes.length);
						droppedHashes = [];
						for (const rawIndex of selectedHashSelectionObject.droppedIndexes) {
							if (
								!Number.isInteger(rawIndex) ||
								rawIndex < 0 ||
								rawIndex >= hashes.length
							) {
								throw new Error("Dropped unknown raw receive index");
							}
							const index = rawIndex;
							if (selectedFlags[index]) {
								throw new Error("Raw receive index selected and dropped");
							}
							if (droppedFlags[index]) {
								throw new Error("Dropped duplicate raw receive index");
							}
							droppedFlags[index] = 1;
							droppedHashes.push(hashes[index]!);
						}
						if (selectedHashes.length + droppedHashes.length !== hashes.length) {
							throw new Error("Raw receive selection did not cover every index");
						}
					}
					const expectedHashes = selectedHashSelectionObject.hashes
						? Array.from(selectedHashSelectionObject.hashes)
						: undefined;
					if (
						expectedHashes &&
						(expectedHashes.length !== selectedHashes.length ||
							expectedHashes.some(
								(hash, index) => hash !== selectedHashes[index],
							))
					) {
						throw new Error("Selected raw receive hashes did not match indexes");
					}
					if (!droppedHashes) {
						droppedHashes = [];
						for (let i = 0; i < hashes.length; i++) {
							if (!selectedFlags[i]) {
								droppedHashes.push(hashes[i]!);
							}
						}
					}
					if (droppedHashes.length > 0) {
						clearPreparedRawHashes(droppedHashes);
					}
					if (selectedHashes.length === 0) {
						return undefined;
					}
				} else {
					const selectedHashIterable =
						isIterableRawReceiveHashSelection(selectedHashSelection)
							? selectedHashSelection
							: selectedHashSelectionObject?.hashes;
					if (!selectedHashIterable) {
						throw new Error("Missing selected raw receive hashes");
					}
					const selectedHashSet = new Set(selectedHashIterable);
					const knownHashes = new Set(hashes);
					for (const hash of selectedHashSet) {
						if (!knownHashes.has(hash)) {
							throw new Error("Selected unknown raw receive hash");
						}
					}
					selectedHeads = [];
					selectedHashes = [];
					selectedIndexes = [];
					const droppedHashes: string[] = [];
					for (let i = 0; i < hashes.length; i++) {
						const hash = hashes[i]!;
						if (selectedHashSet.has(hash)) {
							selectedHeads.push(message.heads[i]!);
							selectedHashes.push(hash);
							selectedIndexes.push(i);
						} else {
							droppedHashes.push(hash);
						}
					}
					if (droppedHashes.length > 0) {
						clearPreparedRawHashes(droppedHashes);
					}
					if (selectedHashes.length === 0) {
						return undefined;
					}
				}
			}
			if (canDeferPreparedSelectionVerification) {
				if (
					options?.deferNativeBackboneSignatureVerificationUntilCommit === true
				) {
					emitSyncProfileDuration(profile, syncProfileStart(profile), {
						name: "sharedLog.rawReceive.deferVerifySelected",
						component: "shared-log",
						entries: selectedHashes.length,
						count: message.heads.length - selectedHashes.length,
						messages: 1,
					});
				} else {
					const verifyStartedAt = syncProfileStart(profile);
					const verified =
						options?.nativeBackbone?.verifyPreparedRawReceiveEntries?.(
							selectedHashes,
						);
					if (
						!verified ||
						verified.length !== selectedHashes.length ||
						verified.some((ok) => !ok)
					) {
						throw new Error("Raw exchange head signature verification failed");
					}
					if (preparedColumns) {
						for (
							let selectedIndex = 0;
							selectedIndex < selectedHashes.length;
							selectedIndex++
						) {
							preparedColumns[12][
								selectedIndexes?.[selectedIndex] ?? selectedIndex
							] = 1;
						}
					} else if (preparedFacts) {
						for (
							let selectedIndex = 0;
							selectedIndex < selectedHashes.length;
							selectedIndex++
						) {
							preparedFacts[
								selectedIndexes?.[selectedIndex] ?? selectedIndex
							]!.signatureVerified = true;
						}
					}
					emitSyncProfileDuration(profile, verifyStartedAt, {
						name: "sharedLog.rawReceive.verifySelected",
						component: "shared-log",
						entries: selectedHashes.length,
						count: message.heads.length - selectedHashes.length,
						messages: 1,
					});
				}
			}
			const headsToWrap = selectedHeads;
			const hashesToWrap = selectedHashes;
			const indexesToWrap = selectedIndexes;
			const wrapStartedAt = syncProfileStart(profile);
			// Lazy per-entry JS materialization (a change consumer reading
			// payload/signatures) is counted on the existing counter so the
			// fused no-consumer path stays assertable at zero.
			const onJsEntryDecode = profile
				? () =>
						emitSyncProfileEvent(profile, {
							name: "sharedLog.rawReceive.jsEntryDecode",
							component: "shared-log",
							entries: 1,
							messages: 0,
							details: { lazy: true },
						})
				: undefined;
			let materializedHeads: EntryWithRefs<any>[];
			try {
				const rowFacts = preparedFacts!;
				materializedHeads = headsToWrap.map((head, selectedIndex) => {
					const factsIndex = indexesToWrap?.[selectedIndex] ?? selectedIndex;
					const facts = preparedColumns ?? rowFacts[factsIndex]!;
					const preparedHead = new PreparedRawEntryWithRefs(
						head,
						facts,
						factsIndex,
						onJsEntryDecode,
					);
					preparedHead.initEntry({
						keychain: log.keychain,
						encoding: log.encoding,
					});
					return preparedHead as EntryWithRefs<any>;
				});
			} catch (error) {
				clearPreparedRaw();
				throw error;
			}
			const materialized = new ExchangeHeadsMessage({
				heads: materializedHeads,
				preparedHashes: hashesToWrap,
			});
			materialized.reserved = message.reserved;
			emitSyncProfileDuration(profile, wrapStartedAt, {
				name: "sharedLog.rawReceive.wrapPrepared",
				component: "shared-log",
				entries: headsToWrap.length,
				count: message.heads.length - headsToWrap.length,
				messages: 1,
			});
			return materialized;
		} catch (error) {
			clearPreparedRaw();
			throw error;
		}
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
	const calculatedHashes = await calculateRawCidV1Batch(blocks());
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
			const entry = materializeRawExchangeEntry({
				hash: head.hash,
				bytes: head.bytes,
				size: head.bytes.byteLength,
				keychain: log.keychain,
				encoding: log.encoding,
				gidRefrences: head.gidRefrences,
			});
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
