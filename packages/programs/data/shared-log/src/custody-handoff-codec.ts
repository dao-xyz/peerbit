import { deserialize, serialize } from "@dao-xyz/borsh";
import { cidifyString, stringifyCid } from "@peerbit/blocks-interface";
import {
	PreHash,
	PublicSignKey,
	SignatureWithKey,
	sha256Base64Sync,
	sha256Sync,
	toHexString,
	verify,
} from "@peerbit/crypto";
import { captureBoundedUint8Array } from "./bounded-bytes.js";
import { MAX_U32, MAX_U64, type NumberFromType } from "./integers.js";

type Resolution = "u32" | "u64";
type MaybePromise<T> = T | Promise<T>;

export const CUSTODY_HANDOFF_MANIFEST_FORMAT =
	"peerbit-shared-log-custody-handoff-manifest" as const;
export const CUSTODY_HANDOFF_RECEIPT_FORMAT =
	"peerbit-shared-log-custody-handoff-receipt" as const;
export const CUSTODY_HANDOFF_VERSION = 1 as const;

const MOVE_KEY_FORMAT = "peerbit-shared-log-custody-handoff-move-key" as const;
const PROFILE_DOMAIN =
	"peerbit/shared-log/custody-profile/exact-entry-closure-durable-pin/v1";
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const MAX_SAFE_U64 = BigInt(Number.MAX_SAFE_INTEGER);

/**
 * The V1 receipt attests to all four promises. The codec only binds and
 * authenticates this vocabulary; it cannot observe a block store, ledger, or
 * physical durability barrier and therefore creates no custody/prune authority.
 */
export const CUSTODY_HANDOFF_PROFILE_FLAGS = Object.freeze({
	exactEntryBlock: 1 << 0,
	completeReferencedBlocks: 1 << 1,
	requiredLedgerRows: 1 << 2,
	durableRetentionPin: 1 << 3,
});

export const CUSTODY_HANDOFF_PROFILE_MASK =
	CUSTODY_HANDOFF_PROFILE_FLAGS.exactEntryBlock |
	CUSTODY_HANDOFF_PROFILE_FLAGS.completeReferencedBlocks |
	CUSTODY_HANDOFF_PROFILE_FLAGS.requiredLedgerRows |
	CUSTODY_HANDOFF_PROFILE_FLAGS.durableRetentionPin;

export const CUSTODY_HANDOFF_PROFILE_ID = toHexString(
	sha256Sync(encoder.encode(PROFILE_DOMAIN)),
);

const profileIdBytes = sha256Sync(encoder.encode(PROFILE_DOMAIN));

export type CustodyHandoffCodecLimits = Readonly<{
	maxManifestBytes: number;
	maxReceiptBytes: number;
	maxIdentifierBytes: number;
	maxPublicKeyBytes: number;
	maxSignatureBytes: number;
}>;

/** Frozen hard ceilings; callers may only configure smaller positive values. */
export const MAX_CUSTODY_HANDOFF_CODEC_LIMITS: CustodyHandoffCodecLimits =
	Object.freeze({
		maxManifestBytes: 4 * 1024,
		maxReceiptBytes: 2 * 1024,
		maxIdentifierBytes: 512,
		maxPublicKeyBytes: 512,
		maxSignatureBytes: 512,
	});

export const DEFAULT_CUSTODY_HANDOFF_CODEC_LIMITS =
	MAX_CUSTODY_HANDOFF_CODEC_LIMITS;

export type CustodyHandoffSigner = (
	bytes: Uint8Array,
) => MaybePromise<SignatureWithKey>;

export type CustodyHandoffVisit<R extends Resolution = Resolution> = Readonly<{
	viewId: string;
	planDigest: string;
	installSequence: bigint;
	taskOrdinal: number;
	resolution: R;
	hashNumber: NumberFromType<R>;
}>;

export type CustodyHandoffManifestInput<R extends Resolution = Resolution> =
	Readonly<{
		logId: Uint8Array;
		/** Canonical Peerbit CID string. */
		entryHash: string;
		/** Exact encoded root-entry block length, represented as a safe positive u64. */
		entryByteLength: bigint;
		source: PublicSignKey;
		destination: PublicSignKey;
		visit: CustodyHandoffVisit<R>;
		ownerPlanId: string;
		/** Fresh random 32-byte value retained for an exact retry. */
		attemptGeneration: Uint8Array;
	}>;

export type CanonicalCustodyIdentity = Readonly<{
	/** SHA-256/base64 hash of the canonical public-key bytes. */
	hash: string;
	/** Lower-case hex of canonical PublicSignKey Borsh bytes. */
	publicKey: string;
}>;

export type CanonicalCustodyHandoffManifestFor<R extends Resolution> =
	Readonly<{
		format: typeof CUSTODY_HANDOFF_MANIFEST_FORMAT;
		version: typeof CUSTODY_HANDOFF_VERSION;
		/** Stable retry lookup; excludes attemptGeneration and the signature. */
		moveKey: string;
		/** Identity of the exact signed-body generation; excludes the signature. */
		handoffId: string;
		custodyProfileId: string;
		custodyProfileMask: number;
		logId: string;
		entryHash: string;
		entryByteLength: bigint;
		source: CanonicalCustodyIdentity;
		destination: CanonicalCustodyIdentity;
		visit: CustodyHandoffVisit<R>;
		ownerPlanId: string;
		attemptGeneration: string;
		bodyBytes: Uint8Array;
		bytes: Uint8Array;
	}>;

export type CanonicalCustodyHandoffManifest =
	| CanonicalCustodyHandoffManifestFor<"u32">
	| CanonicalCustodyHandoffManifestFor<"u64">;

export type CustodyHandoffReceiptInput = Readonly<{
	manifest: CanonicalCustodyHandoffManifest;
	/** Stable destination custody-store generation, fixed at 32 bytes. */
	custodyEpoch: Uint8Array;
	/** Positive monotone sequence of the exact durable retention pin. */
	pinSequence: bigint;
}>;

export type CanonicalCustodyHandoffReceipt = Readonly<{
	format: typeof CUSTODY_HANDOFF_RECEIPT_FORMAT;
	version: typeof CUSTODY_HANDOFF_VERSION;
	moveKey: string;
	handoffId: string;
	/** Identity of the receipt body; excludes the destination signature. */
	receiptId: string;
	custodyProfileId: string;
	custodyProfileMask: number;
	destination: CanonicalCustodyIdentity;
	custodyEpoch: string;
	pinSequence: bigint;
	bodyBytes: Uint8Array;
	bytes: Uint8Array;
}>;

type CanonicalKey = Readonly<{
	key: PublicSignKey;
	bytes: Uint8Array;
	hex: string;
	hash: string;
}>;

type NormalizedManifest<R extends Resolution = Resolution> = Readonly<{
	logId: Uint8Array;
	entryHash: string;
	entryByteLength: bigint;
	source: CanonicalKey;
	destination: CanonicalKey;
	visit: CustodyHandoffVisit<R>;
	ownerPlanId: Uint8Array;
	attemptGeneration: Uint8Array;
}>;

type DecodedEnvelope = Readonly<{
	bytes: Uint8Array;
	bodyBytes: Uint8Array;
	signature: Uint8Array;
}>;

const bytesEqual = (left: Uint8Array, right: Uint8Array) => {
	if (left.byteLength !== right.byteLength) return false;
	for (let index = 0; index < left.byteLength; index++) {
		if (left[index] !== right[index]) return false;
	}
	return true;
};

const own = (value: object, key: PropertyKey) =>
	Object.prototype.hasOwnProperty.call(value, key);

class BoundedWriter {
	private readonly value: Uint8Array;
	private offset = 0;

	constructor(
		maximumBytes: number,
		private readonly name: string,
	) {
		this.value = new Uint8Array(maximumBytes);
	}

	private reserve(length: number): number {
		if (
			!Number.isSafeInteger(length) ||
			length < 0 ||
			this.offset + length > this.value.byteLength
		) {
			throw new Error(`${this.name} exceeds its encoded byte limit`);
		}
		const start = this.offset;
		this.offset += length;
		return start;
	}

	u8(value: number): void {
		this.value[this.reserve(1)] = value;
	}

	u32(value: number): void {
		const start = this.reserve(4);
		new DataView(this.value.buffer).setUint32(start, value, true);
	}

	u64(value: bigint): void {
		const start = this.reserve(8);
		new DataView(this.value.buffer).setBigUint64(start, value, true);
	}

	raw(value: Uint8Array): void {
		this.value.set(value, this.reserve(value.byteLength));
	}

	bytes(value: Uint8Array): void {
		this.u32(value.byteLength);
		this.raw(value);
	}

	string(value: string): void {
		this.bytes(encoder.encode(value));
	}

	finish(): Uint8Array {
		return this.value.slice(0, this.offset);
	}
}

class BoundedReader {
	private offset = 0;

	constructor(
		private readonly value: Uint8Array,
		private readonly name: string,
	) {}

	private reserve(length: number): number {
		if (
			!Number.isSafeInteger(length) ||
			length < 0 ||
			this.offset + length > this.value.byteLength
		) {
			throw new Error(`Truncated ${this.name}`);
		}
		const start = this.offset;
		this.offset += length;
		return start;
	}

	u8(): number {
		return this.value[this.reserve(1)]!;
	}

	u32(): number {
		return new DataView(
			this.value.buffer,
			this.value.byteOffset + this.reserve(4),
			4,
		).getUint32(0, true);
	}

	u64(): bigint {
		return new DataView(
			this.value.buffer,
			this.value.byteOffset + this.reserve(8),
			8,
		).getBigUint64(0, true);
	}

	fixed(length: number): Uint8Array {
		const start = this.reserve(length);
		return this.value.slice(start, start + length);
	}

	bytes(maximumBytes: number, field: string): Uint8Array {
		const length = this.u32();
		if (length > maximumBytes) {
			throw new Error(`${field} exceeds its byte limit`);
		}
		return this.fixed(length);
	}

	string(maximumBytes: number, field: string): string {
		const bytes = this.bytes(maximumBytes, field);
		let value: string;
		try {
			value = decoder.decode(bytes);
		} catch (error) {
			throw new Error(`Invalid UTF-8 ${field}`, { cause: error });
		}
		if (!bytesEqual(encoder.encode(value), bytes)) {
			throw new Error(`Non-canonical UTF-8 ${field}`);
		}
		return value;
	}

	done(): void {
		if (this.offset !== this.value.byteLength) {
			throw new Error(`${this.name} has trailing bytes`);
		}
	}
}

const normalizeLimits = (
	input?: Partial<CustodyHandoffCodecLimits>,
): CustodyHandoffCodecLimits => {
	// Read only the fixed vocabulary, once per key. Enumerating/spreading an
	// untrusted options object would make limit validation itself unbounded.
	const limits: CustodyHandoffCodecLimits = {
		maxManifestBytes:
			input?.maxManifestBytes ??
			DEFAULT_CUSTODY_HANDOFF_CODEC_LIMITS.maxManifestBytes,
		maxReceiptBytes:
			input?.maxReceiptBytes ??
			DEFAULT_CUSTODY_HANDOFF_CODEC_LIMITS.maxReceiptBytes,
		maxIdentifierBytes:
			input?.maxIdentifierBytes ??
			DEFAULT_CUSTODY_HANDOFF_CODEC_LIMITS.maxIdentifierBytes,
		maxPublicKeyBytes:
			input?.maxPublicKeyBytes ??
			DEFAULT_CUSTODY_HANDOFF_CODEC_LIMITS.maxPublicKeyBytes,
		maxSignatureBytes:
			input?.maxSignatureBytes ??
			DEFAULT_CUSTODY_HANDOFF_CODEC_LIMITS.maxSignatureBytes,
	};
	for (const key of Object.keys(
		MAX_CUSTODY_HANDOFF_CODEC_LIMITS,
	) as (keyof CustodyHandoffCodecLimits)[]) {
		const value = limits[key];
		if (
			!Number.isSafeInteger(value) ||
			value <= 0 ||
			value > MAX_CUSTODY_HANDOFF_CODEC_LIMITS[key]
		) {
			throw new Error(`Invalid custody handoff codec limit: ${key}`);
		}
	}
	return Object.freeze(limits);
};

const captureBytes = (
	value: unknown,
	minimum: number,
	maximum: number,
	name: string,
): Uint8Array =>
	captureBoundedUint8Array(value, minimum, maximum, `custody handoff ${name}`);

const nonZeroGeneration = (value: Uint8Array, name: string): Uint8Array => {
	if (value.every((byte) => byte === 0)) {
		throw new Error(`Invalid custody handoff ${name}`);
	}
	return value;
};

const boundedString = (value: unknown, maximum: number, name: string) => {
	if (
		typeof value !== "string" ||
		value.length === 0 ||
		value.length > maximum
	) {
		throw new Error(`Invalid custody handoff ${name}`);
	}
	let bytes: Uint8Array;
	try {
		bytes = encoder.encode(value);
	} catch (error) {
		throw new Error(`Invalid custody handoff ${name}`, { cause: error });
	}
	if (bytes.byteLength > maximum || decoder.decode(bytes) !== value) {
		throw new Error(`Invalid custody handoff ${name}`);
	}
	return value;
};

const canonicalCid = (value: unknown, maximum: number) => {
	const bounded = boundedString(value, maximum, "entry CID");
	let canonical: string;
	try {
		canonical = stringifyCid(cidifyString(bounded));
	} catch (error) {
		throw new Error("Invalid custody handoff entry CID", { cause: error });
	}
	if (canonical !== bounded) {
		throw new Error("Custody handoff entry CID is not canonical");
	}
	return canonical;
};

const digestBytes = (value: unknown, name: string): Uint8Array => {
	if (
		typeof value !== "string" ||
		value.length !== 64 ||
		!/^[0-9a-f]{64}$/.test(value)
	) {
		throw new Error(`Invalid custody handoff ${name}`);
	}
	const bytes = new Uint8Array(32);
	for (let index = 0; index < bytes.length; index++) {
		bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
	}
	return bytes;
};

const positiveU64 = (
	value: unknown,
	name: string,
	maximum: bigint = MAX_U64,
): bigint => {
	if (
		typeof value !== "bigint" ||
		value <= 0n ||
		value > maximum ||
		value > MAX_U64
	) {
		throw new Error(`Invalid custody handoff ${name}`);
	}
	return value;
};

const taskOrdinal = (value: unknown): number => {
	if (
		typeof value !== "number" ||
		!Number.isSafeInteger(value) ||
		Object.is(value, -0) ||
		value < 0 ||
		value > MAX_U32
	) {
		throw new Error("Invalid custody handoff task ordinal");
	}
	return value;
};

const hashNumber = <R extends Resolution>(
	resolution: R,
	value: unknown,
): NumberFromType<R> => {
	if (resolution === "u32") {
		if (
			typeof value !== "number" ||
			!Number.isInteger(value) ||
			Object.is(value, -0) ||
			value < 0 ||
			value > MAX_U32
		) {
			throw new Error("Invalid custody handoff u32 hash number");
		}
		return value as NumberFromType<R>;
	}
	if (typeof value !== "bigint" || value < 0n || value > MAX_U64) {
		throw new Error("Invalid custody handoff u64 hash number");
	}
	return value as NumberFromType<R>;
};

const parseCanonicalKey = (
	bytesValue: Uint8Array,
	limits: CustodyHandoffCodecLimits,
	name: string,
): CanonicalKey => {
	const bytes = captureBytes(
		bytesValue,
		1,
		limits.maxPublicKeyBytes,
		`${name} public key`,
	);
	let key: PublicSignKey;
	try {
		key = deserialize(bytes, PublicSignKey);
	} catch (error) {
		throw new Error(`Invalid custody handoff ${name} public key`, {
			cause: error,
		});
	}
	if (!(key instanceof PublicSignKey)) {
		throw new Error(`Invalid custody handoff ${name} public key`);
	}
	const canonical = serialize(key);
	if (!bytesEqual(bytes, canonical)) {
		throw new Error(`Non-canonical custody handoff ${name} public key`);
	}
	const hash = sha256Base64Sync(bytes);
	if (key.hashcode() !== hash || !bytesEqual(key.bytes, bytes)) {
		throw new Error(`Invalid custody handoff ${name} public key hash`);
	}
	return Object.freeze({ key, bytes, hex: toHexString(bytes), hash });
};

const captureCanonicalKey = (
	value: unknown,
	limits: CustodyHandoffCodecLimits,
	name: string,
): CanonicalKey => {
	if (!(value instanceof PublicSignKey)) {
		throw new Error(`Invalid custody handoff ${name} public key`);
	}
	let bytes: Uint8Array;
	try {
		bytes = serialize(value);
	} catch (error) {
		throw new Error(`Invalid custody handoff ${name} public key`, {
			cause: error,
		});
	}
	const parsed = parseCanonicalKey(bytes, limits, name);
	if (
		!bytesEqual(value.bytes, parsed.bytes) ||
		value.hashcode() !== parsed.hash
	) {
		throw new Error(`Invalid custody handoff ${name} public key hash`);
	}
	return parsed;
};

const normalizeManifestInput = <R extends Resolution>(
	input: CustodyHandoffManifestInput<R>,
	limits: CustodyHandoffCodecLimits,
): NormalizedManifest<R> => {
	if (!input || typeof input !== "object") {
		throw new Error("Invalid custody handoff manifest input");
	}
	const logId = captureBytes(
		input.logId,
		1,
		limits.maxIdentifierBytes,
		"log id",
	);
	const source = captureCanonicalKey(input.source, limits, "source");
	const destination = captureCanonicalKey(
		input.destination,
		limits,
		"destination",
	);
	if (bytesEqual(source.bytes, destination.bytes)) {
		throw new Error("Custody handoff source and destination must be distinct");
	}
	if (!input.visit || typeof input.visit !== "object") {
		throw new Error("Invalid custody handoff visit");
	}
	const resolution = input.visit.resolution;
	if (resolution !== "u32" && resolution !== "u64") {
		throw new Error("Invalid custody handoff resolution");
	}
	const visit = Object.freeze({
		viewId: toHexString(digestBytes(input.visit.viewId, "view id")),
		planDigest: toHexString(digestBytes(input.visit.planDigest, "plan digest")),
		installSequence: positiveU64(
			input.visit.installSequence,
			"install sequence",
		),
		taskOrdinal: taskOrdinal(input.visit.taskOrdinal),
		resolution,
		hashNumber: hashNumber(resolution, input.visit.hashNumber),
	}) as CustodyHandoffVisit<R>;
	return Object.freeze({
		logId,
		entryHash: canonicalCid(input.entryHash, limits.maxIdentifierBytes),
		entryByteLength: positiveU64(
			input.entryByteLength,
			"entry byte length",
			MAX_SAFE_U64,
		),
		source,
		destination,
		visit,
		ownerPlanId: digestBytes(input.ownerPlanId, "owner plan id"),
		attemptGeneration: nonZeroGeneration(
			captureBytes(input.attemptGeneration, 32, 32, "attempt generation"),
			"attempt generation",
		),
	});
};

const writeManifestFields = (
	writer: BoundedWriter,
	value: NormalizedManifest,
) => {
	writer.raw(profileIdBytes);
	writer.u32(CUSTODY_HANDOFF_PROFILE_MASK);
	writer.bytes(value.logId);
	writer.string(value.entryHash);
	writer.u64(value.entryByteLength);
	writer.bytes(value.source.bytes);
	writer.bytes(value.destination.bytes);
	writer.raw(digestBytes(value.visit.viewId, "view id"));
	writer.raw(digestBytes(value.visit.planDigest, "plan digest"));
	writer.u64(value.visit.installSequence);
	writer.u32(value.visit.taskOrdinal);
	writer.u8(value.visit.resolution === "u32" ? 0 : 1);
	if (value.visit.resolution === "u32") {
		writer.u32(value.visit.hashNumber as number);
	} else {
		writer.u64(value.visit.hashNumber as bigint);
	}
	writer.raw(value.ownerPlanId);
};

const encodeMoveKeyBody = (
	value: NormalizedManifest,
	limits: CustodyHandoffCodecLimits,
) => {
	const writer = new BoundedWriter(
		limits.maxManifestBytes,
		"custody handoff move key",
	);
	writer.string(MOVE_KEY_FORMAT);
	writer.u32(CUSTODY_HANDOFF_VERSION);
	writeManifestFields(writer, value);
	return writer.finish();
};

const encodeManifestBody = (
	value: NormalizedManifest,
	limits: CustodyHandoffCodecLimits,
) => {
	const writer = new BoundedWriter(
		limits.maxManifestBytes,
		"custody handoff manifest body",
	);
	writer.string(CUSTODY_HANDOFF_MANIFEST_FORMAT);
	writer.u32(CUSTODY_HANDOFF_VERSION);
	writeManifestFields(writer, value);
	writer.raw(value.attemptGeneration);
	return writer.finish();
};

const encodeEnvelope = (
	bodyBytes: Uint8Array,
	signature: Uint8Array,
	maximumBytes: number,
	name: string,
) => {
	const writer = new BoundedWriter(maximumBytes, name);
	writer.bytes(bodyBytes);
	writer.bytes(signature);
	return writer.finish();
};

const decodeEnvelope = (
	input: unknown,
	maximumBytes: number,
	maximumSignatureBytes: number,
	name: string,
): DecodedEnvelope => {
	const bytes = captureBytes(input, 1, maximumBytes, name);
	const reader = new BoundedReader(bytes, name);
	const bodyBytes = reader.bytes(maximumBytes, `${name} body`);
	if (bodyBytes.byteLength === 0) {
		throw new Error(`Invalid ${name} body`);
	}
	const signature = reader.bytes(maximumSignatureBytes, `${name} signature`);
	if (signature.byteLength === 0) {
		throw new Error(`Invalid ${name} signature`);
	}
	reader.done();
	return Object.freeze({ bytes, bodyBytes, signature });
};

const readExpectedFormat = (
	reader: BoundedReader,
	expected: string,
	maximumBytes: number,
	name: string,
) => {
	if (reader.string(maximumBytes, `${name} format`) !== expected) {
		throw new Error(`Invalid ${name} format`);
	}
	if (reader.u32() !== CUSTODY_HANDOFF_VERSION) {
		throw new Error(`Unsupported ${name} version`);
	}
};

const readProfile = (reader: BoundedReader, name: string) => {
	const id = reader.fixed(32);
	const mask = reader.u32();
	if (
		!bytesEqual(id, profileIdBytes) ||
		mask !== CUSTODY_HANDOFF_PROFILE_MASK
	) {
		throw new Error(`Unsupported ${name} custody profile`);
	}
};

const readManifestFields = (
	reader: BoundedReader,
	limits: CustodyHandoffCodecLimits,
): Omit<NormalizedManifest, "attemptGeneration"> => {
	readProfile(reader, "custody handoff manifest");
	const logId = reader.bytes(limits.maxIdentifierBytes, "custody log id");
	if (logId.byteLength === 0) {
		throw new Error("Invalid custody handoff log id");
	}
	const entryHash = canonicalCid(
		reader.string(limits.maxIdentifierBytes, "custody entry CID"),
		limits.maxIdentifierBytes,
	);
	const entryByteLength = positiveU64(
		reader.u64(),
		"entry byte length",
		MAX_SAFE_U64,
	);
	const source = parseCanonicalKey(
		reader.bytes(limits.maxPublicKeyBytes, "custody source public key"),
		limits,
		"source",
	);
	const destination = parseCanonicalKey(
		reader.bytes(limits.maxPublicKeyBytes, "custody destination public key"),
		limits,
		"destination",
	);
	if (bytesEqual(source.bytes, destination.bytes)) {
		throw new Error("Custody handoff source and destination must be distinct");
	}
	const viewId = toHexString(reader.fixed(32));
	const planDigest = toHexString(reader.fixed(32));
	const installSequence = positiveU64(reader.u64(), "install sequence");
	const ordinal = taskOrdinal(reader.u32());
	const resolutionTag = reader.u8();
	if (resolutionTag !== 0 && resolutionTag !== 1) {
		throw new Error("Invalid custody handoff resolution");
	}
	const resolution = resolutionTag === 0 ? "u32" : "u64";
	const number =
		resolution === "u32"
			? hashNumber("u32", reader.u32())
			: hashNumber("u64", reader.u64());
	const ownerPlanId = reader.fixed(32);
	return Object.freeze({
		logId,
		entryHash,
		entryByteLength,
		source,
		destination,
		visit: Object.freeze({
			viewId,
			planDigest,
			installSequence,
			taskOrdinal: ordinal,
			resolution,
			hashNumber: number,
		}) as CustodyHandoffVisit,
		ownerPlanId,
	});
};

const parseManifestBody = (
	bodyBytes: Uint8Array,
	limits: CustodyHandoffCodecLimits,
): NormalizedManifest => {
	const reader = new BoundedReader(bodyBytes, "custody handoff manifest body");
	readExpectedFormat(
		reader,
		CUSTODY_HANDOFF_MANIFEST_FORMAT,
		limits.maxIdentifierBytes,
		"custody handoff manifest",
	);
	const fields = readManifestFields(reader, limits);
	const attemptGeneration = nonZeroGeneration(
		reader.fixed(32),
		"attempt generation",
	);
	reader.done();
	return Object.freeze({ ...fields, attemptGeneration });
};

const identity = (value: CanonicalKey): CanonicalCustodyIdentity =>
	Object.freeze({ hash: value.hash, publicKey: value.hex });

const manifestResult = <R extends Resolution>(
	value: NormalizedManifest<R>,
	bodyBytes: Uint8Array,
	bytes: Uint8Array,
	limits: CustodyHandoffCodecLimits,
): CanonicalCustodyHandoffManifestFor<R> => {
	const moveKey = toHexString(sha256Sync(encodeMoveKeyBody(value, limits)));
	return Object.freeze({
		format: CUSTODY_HANDOFF_MANIFEST_FORMAT,
		version: CUSTODY_HANDOFF_VERSION,
		moveKey,
		handoffId: toHexString(sha256Sync(bodyBytes)),
		custodyProfileId: CUSTODY_HANDOFF_PROFILE_ID,
		custodyProfileMask: CUSTODY_HANDOFF_PROFILE_MASK,
		logId: toHexString(value.logId),
		entryHash: value.entryHash,
		entryByteLength: value.entryByteLength,
		source: identity(value.source),
		destination: identity(value.destination),
		visit: Object.freeze({ ...value.visit }),
		ownerPlanId: toHexString(value.ownerPlanId),
		attemptGeneration: toHexString(value.attemptGeneration),
		bodyBytes: new Uint8Array(bodyBytes),
		bytes: new Uint8Array(bytes),
	});
};

const checkedSignature = async (
	signatureBytes: Uint8Array,
	key: CanonicalKey,
	bodyBytes: Uint8Array,
	name: string,
) => {
	const signature = new SignatureWithKey({
		signature: new Uint8Array(signatureBytes),
		publicKey: key.key,
		prehash: PreHash.SHA_256,
	});
	if (!(await verify(signature, bodyBytes))) {
		throw new Error(`Invalid ${name} signature`);
	}
};

const signBody = async (
	bodyBytes: Uint8Array,
	expected: CanonicalKey,
	signer: CustodyHandoffSigner,
	limits: CustodyHandoffCodecLimits,
	name: string,
) => {
	if (typeof signer !== "function") {
		throw new Error(`Invalid ${name} signer`);
	}
	let signed: SignatureWithKey;
	try {
		signed = await signer(new Uint8Array(bodyBytes));
	} catch (error) {
		throw new Error(`Failed to sign ${name}`, { cause: error });
	}
	if (!(signed instanceof SignatureWithKey)) {
		throw new Error(`Invalid ${name} signature`);
	}
	if (signed.prehash !== PreHash.SHA_256) {
		throw new Error(`${name} requires SHA-256 prehash`);
	}
	const signerKey = captureCanonicalKey(
		signed.publicKey,
		limits,
		`${name} signer`,
	);
	if (!bytesEqual(signerKey.bytes, expected.bytes)) {
		throw new Error(`${name} signer does not match its bound identity`);
	}
	const signatureBytes = captureBytes(
		signed.signature,
		1,
		limits.maxSignatureBytes,
		`${name} signature`,
	);
	await checkedSignature(signatureBytes, expected, bodyBytes, name);
	return signatureBytes;
};

/**
 * Create one independently verifiable, source-signed handoff manifest. The
 * resulting IDs identify a decision and attempt only; they prove neither
 * destination possession nor current placement and never authorize pruning.
 */
export const createCustodyHandoffManifestV1 = async <R extends Resolution>(
	input: CustodyHandoffManifestInput<R>,
	signer: CustodyHandoffSigner,
	options?: { limits?: Partial<CustodyHandoffCodecLimits> },
): Promise<CanonicalCustodyHandoffManifestFor<R>> => {
	const limits = normalizeLimits(options?.limits);
	const value = normalizeManifestInput(input, limits);
	const bodyBytes = encodeManifestBody(value, limits);
	const signature = await signBody(
		bodyBytes,
		value.source,
		signer,
		limits,
		"custody handoff manifest",
	);
	const bytes = encodeEnvelope(
		bodyBytes,
		signature,
		limits.maxManifestBytes,
		"custody handoff manifest",
	);
	return (await decodeCustodyHandoffManifestV1(bytes, {
		limits,
	})) as CanonicalCustodyHandoffManifestFor<R>;
};

/** Decode, canonicalize, re-hash, and verify one complete manifest envelope. */
export const decodeCustodyHandoffManifestV1 = async (
	input: Uint8Array,
	options?: { limits?: Partial<CustodyHandoffCodecLimits> },
): Promise<CanonicalCustodyHandoffManifest> => {
	const limits = normalizeLimits(options?.limits);
	const envelope = decodeEnvelope(
		input,
		limits.maxManifestBytes,
		limits.maxSignatureBytes,
		"custody handoff manifest",
	);
	const value = parseManifestBody(envelope.bodyBytes, limits);
	// Re-encoding rejects any future parser change that accidentally accepts an
	// alternate representation of the same semantic fields.
	if (!bytesEqual(encodeManifestBody(value, limits), envelope.bodyBytes)) {
		throw new Error("Custody handoff manifest body is not canonical");
	}
	await checkedSignature(
		envelope.signature,
		value.source,
		envelope.bodyBytes,
		"custody handoff manifest",
	);
	return value.visit.resolution === "u32"
		? manifestResult(
				value as NormalizedManifest<"u32">,
				envelope.bodyBytes,
				envelope.bytes,
				limits,
			)
		: manifestResult(
				value as NormalizedManifest<"u64">,
				envelope.bodyBytes,
				envelope.bytes,
				limits,
			);
};

const encodeReceiptBody = (
	manifest: CanonicalCustodyHandoffManifest,
	destinationKey: CanonicalKey,
	custodyEpoch: Uint8Array,
	pinSequence: bigint,
	limits: CustodyHandoffCodecLimits,
) => {
	const writer = new BoundedWriter(
		limits.maxReceiptBytes,
		"custody handoff receipt body",
	);
	writer.string(CUSTODY_HANDOFF_RECEIPT_FORMAT);
	writer.u32(CUSTODY_HANDOFF_VERSION);
	writer.raw(profileIdBytes);
	writer.u32(CUSTODY_HANDOFF_PROFILE_MASK);
	writer.raw(digestBytes(manifest.moveKey, "receipt move key"));
	writer.raw(digestBytes(manifest.handoffId, "receipt handoff id"));
	writer.bytes(destinationKey.bytes);
	writer.raw(custodyEpoch);
	writer.u64(pinSequence);
	return writer.finish();
};

const parseReceiptBody = (
	bodyBytes: Uint8Array,
	limits: CustodyHandoffCodecLimits,
) => {
	const reader = new BoundedReader(bodyBytes, "custody handoff receipt body");
	readExpectedFormat(
		reader,
		CUSTODY_HANDOFF_RECEIPT_FORMAT,
		limits.maxIdentifierBytes,
		"custody handoff receipt",
	);
	readProfile(reader, "custody handoff receipt");
	const moveKey = toHexString(reader.fixed(32));
	const handoffId = toHexString(reader.fixed(32));
	const destination = parseCanonicalKey(
		reader.bytes(limits.maxPublicKeyBytes, "custody receipt destination key"),
		limits,
		"receipt destination",
	);
	const custodyEpoch = nonZeroGeneration(reader.fixed(32), "custody epoch");
	const pinSequence = positiveU64(reader.u64(), "pin sequence");
	reader.done();
	return Object.freeze({
		moveKey,
		handoffId,
		destination,
		custodyEpoch,
		pinSequence,
	});
};

const decodeManifestAgain = async (
	manifest: unknown,
	limits: CustodyHandoffCodecLimits,
) => {
	if (!manifest || typeof manifest !== "object" || !own(manifest, "bytes")) {
		throw new Error("Invalid canonical custody handoff manifest");
	}
	// A getter/Proxy must not swap a small admitted buffer for a second, larger
	// value between validation and copying.
	const candidate = (manifest as { bytes?: unknown }).bytes;
	let bytes: Uint8Array;
	try {
		bytes = captureBoundedUint8Array(
			candidate,
			1,
			limits.maxManifestBytes,
			"canonical custody handoff manifest",
		);
	} catch {
		throw new Error("Invalid canonical custody handoff manifest");
	}
	return decodeCustodyHandoffManifestV1(bytes, { limits });
};

/**
 * Create the destination's signed receipt body. The caller must do this only
 * after the complete profile and exact epoch/sequence pin are physically
 * durable; this pure codec cannot inspect or manufacture that prerequisite.
 */
export const createCustodyHandoffReceiptV1 = async (
	input: CustodyHandoffReceiptInput,
	signer: CustodyHandoffSigner,
	options?: { limits?: Partial<CustodyHandoffCodecLimits> },
): Promise<CanonicalCustodyHandoffReceipt> => {
	const limits = normalizeLimits(options?.limits);
	if (!input || typeof input !== "object") {
		throw new Error("Invalid custody handoff receipt input");
	}
	const custodyEpoch = nonZeroGeneration(
		captureBytes(input.custodyEpoch, 32, 32, "custody epoch"),
		"custody epoch",
	);
	const pinSequence = positiveU64(input.pinSequence, "pin sequence");
	const manifest = await decodeManifestAgain(input.manifest, limits);
	const destinationRaw = hexBytes(
		manifest.destination.publicKey,
		limits.maxPublicKeyBytes,
		"destination public key",
	);
	const destination = parseCanonicalKey(destinationRaw, limits, "destination");
	const bodyBytes = encodeReceiptBody(
		manifest,
		destination,
		custodyEpoch,
		pinSequence,
		limits,
	);
	const signature = await signBody(
		bodyBytes,
		destination,
		signer,
		limits,
		"custody handoff receipt",
	);
	const bytes = encodeEnvelope(
		bodyBytes,
		signature,
		limits.maxReceiptBytes,
		"custody handoff receipt",
	);
	return decodeCustodyHandoffReceiptV1(bytes, manifest, { limits });
};

const hexBytes = (
	value: unknown,
	maximumBytes: number,
	name: string,
): Uint8Array => {
	if (
		typeof value !== "string" ||
		value.length === 0 ||
		value.length % 2 !== 0 ||
		value.length / 2 > maximumBytes ||
		!/^[0-9a-f]+$/.test(value)
	) {
		throw new Error(`Invalid custody handoff ${name}`);
	}
	const bytes = new Uint8Array(value.length / 2);
	for (let index = 0; index < bytes.length; index++) {
		bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
	}
	return bytes;
};

/** Decode and verify a receipt against one exact verified manifest. */
export const decodeCustodyHandoffReceiptV1 = async (
	input: Uint8Array,
	manifestInput: CanonicalCustodyHandoffManifest,
	options?: { limits?: Partial<CustodyHandoffCodecLimits> },
): Promise<CanonicalCustodyHandoffReceipt> => {
	const limits = normalizeLimits(options?.limits);
	const receiptBytes = captureBytes(
		input,
		1,
		limits.maxReceiptBytes,
		"custody handoff receipt",
	);
	const manifest = await decodeManifestAgain(manifestInput, limits);
	const envelope = decodeEnvelope(
		receiptBytes,
		limits.maxReceiptBytes,
		limits.maxSignatureBytes,
		"custody handoff receipt",
	);
	const value = parseReceiptBody(envelope.bodyBytes, limits);
	if (
		value.moveKey !== manifest.moveKey ||
		value.handoffId !== manifest.handoffId ||
		value.destination.hex !== manifest.destination.publicKey ||
		value.destination.hash !== manifest.destination.hash
	) {
		throw new Error("Custody handoff receipt does not match its manifest");
	}
	if (
		!bytesEqual(
			encodeReceiptBody(
				manifest,
				value.destination,
				value.custodyEpoch,
				value.pinSequence,
				limits,
			),
			envelope.bodyBytes,
		)
	) {
		throw new Error("Custody handoff receipt body is not canonical");
	}
	await checkedSignature(
		envelope.signature,
		value.destination,
		envelope.bodyBytes,
		"custody handoff receipt",
	);
	return Object.freeze({
		format: CUSTODY_HANDOFF_RECEIPT_FORMAT,
		version: CUSTODY_HANDOFF_VERSION,
		moveKey: value.moveKey,
		handoffId: value.handoffId,
		receiptId: toHexString(sha256Sync(envelope.bodyBytes)),
		custodyProfileId: CUSTODY_HANDOFF_PROFILE_ID,
		custodyProfileMask: CUSTODY_HANDOFF_PROFILE_MASK,
		destination: identity(value.destination),
		custodyEpoch: toHexString(value.custodyEpoch),
		pinSequence: value.pinSequence,
		bodyBytes: new Uint8Array(envelope.bodyBytes),
		bytes: new Uint8Array(envelope.bytes),
	});
};
