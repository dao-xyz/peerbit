import { deserialize, serialize } from "@dao-xyz/borsh";
import { DecryptedThing, type SignatureWithKey } from "@peerbit/crypto";
import { equals } from "uint8arrays";
import { NO_ENCODING } from "./encoding.js";
import { EntryV0, type Meta } from "./entry-v0.js";
import { Entry } from "./entry.js";

export type CanonicalPublicEntryV0ScanLimits = Readonly<{
	label: string;
	minimumSignatures: number;
	maximumSignatures: number;
	maximumDirectParents?: number;
	maximumMetadataBytes?: number;
}>;

export type CanonicalPublicEntryV0Scan = Readonly<{
	entry: EntryV0<Uint8Array>;
	meta: Meta;
	signatures: SignatureWithKey[];
	metaBytes: Uint8Array;
	payloadBytes: Uint8Array;
	signableBytes: Uint8Array;
	reservedBytes: Uint8Array;
	hasHash: boolean;
	directParentCount: number;
	signatureCount: number;
}>;

class BoundsReader {
	private offset = 0;
	private readonly view: DataView;

	constructor(
		private readonly bytes: Uint8Array,
		private readonly label: string,
	) {
		this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
	}

	get position(): number {
		return this.offset;
	}

	get remaining(): number {
		return this.bytes.byteLength - this.offset;
	}

	readU8(field: string): number {
		return this.readExact(1, field)[0]!;
	}

	readU32(field: string): number {
		this.requireRemaining(4, field);
		const value = this.view.getUint32(this.offset, true);
		this.offset += 4;
		return value;
	}

	readBytes(field: string): Uint8Array {
		return this.readExact(this.readU32(`${field} length`), field);
	}

	readExact(byteLength: number, field: string): Uint8Array {
		this.requireRemaining(byteLength, field);
		const start = this.offset;
		this.offset += byteLength;
		return this.bytes.subarray(start, this.offset);
	}

	expectU8(expected: number, field: string): void {
		if (this.readU8(field) !== expected) {
			throw new Error(`${this.label} EntryV0 has invalid ${field}`);
		}
	}

	expectDone(field: string): void {
		if (this.offset !== this.bytes.byteLength) {
			throw new Error(`${this.label} EntryV0 has trailing ${field} bytes`);
		}
	}

	private requireRemaining(byteLength: number, field: string): void {
		if (
			!Number.isSafeInteger(byteLength) ||
			byteLength < 0 ||
			byteLength > this.bytes.byteLength - this.offset
		) {
			throw new Error(`${this.label} EntryV0 has truncated ${field}`);
		}
	}
}

const readPublicWrapper = (
	reader: BoundsReader,
	label: string,
	field: string,
): Uint8Array => {
	if (
		reader.readU8(`${field} MaybeEncrypted variant`) !== 0 ||
		reader.readU8(`${field} DecryptedThing variant`) !== 0
	) {
		throw new Error(`${label} EntryV0 ${field} must be public`);
	}
	return reader.readBytes(field);
};

const scanMeta = (
	bytes: Uint8Array,
	limits: CanonicalPublicEntryV0ScanLimits,
): number => {
	const reader = new BoundsReader(bytes, limits.label);
	reader.expectU8(0, "metadata variant");
	reader.expectU8(0, "clock variant");
	reader.readBytes("clock id");
	reader.expectU8(0, "timestamp variant");
	reader.readExact(8, "timestamp wall time");
	reader.readExact(4, "timestamp logical time");
	reader.readBytes("gid");
	const parentCount = reader.readU32("direct-parent count");
	if (parentCount > Math.floor(reader.remaining / 4)) {
		throw new Error(
			`${limits.label} EntryV0 has impossible direct-parent count`,
		);
	}
	if (
		limits.maximumDirectParents !== undefined &&
		parentCount > limits.maximumDirectParents
	) {
		throw new Error(
			`${limits.label} EntryV0 may contain at most ${limits.maximumDirectParents} direct parents`,
		);
	}
	for (let index = 0; index < parentCount; index++) {
		reader.readBytes("direct parent");
	}
	reader.readU8("entry type");
	const metadataOption = reader.readU8("metadata data option");
	if (metadataOption === 1) {
		const metadata = reader.readBytes("metadata data");
		if (
			limits.maximumMetadataBytes !== undefined &&
			metadata.byteLength > limits.maximumMetadataBytes
		) {
			throw new Error(
				`${limits.label} EntryV0 metadata may contain at most ${limits.maximumMetadataBytes} bytes`,
			);
		}
	} else if (metadataOption !== 0) {
		throw new Error(`${limits.label} EntryV0 has invalid metadata data option`);
	}
	reader.expectDone("metadata");
	return parentCount;
};

const scanPayload = (bytes: Uint8Array, label: string): Uint8Array => {
	const reader = new BoundsReader(bytes, label);
	reader.expectU8(0, "payload variant");
	const data = reader.readBytes("payload data");
	reader.expectDone("payload");
	return data;
};

const scanSignature = (bytes: Uint8Array, label: string): void => {
	const reader = new BoundsReader(bytes, label);
	reader.expectU8(0, "signature variant");
	reader.readBytes("signature data");
	const keyVariant = reader.readU8("signature public-key variant");
	const keyLength = keyVariant === 0 ? 32 : keyVariant === 1 ? 33 : undefined;
	if (keyLength === undefined) {
		throw new Error(`${label} EntryV0 uses an unsupported signing key`);
	}
	reader.readExact(keyLength, "signature public key");
	reader.readU8("signature prehash");
	reader.expectDone("signature");
};

/**
 * Bounds-scans and canonically decodes one already-captured public EntryV0.
 * The caller must bound and capture the input bytes before scanning. Returned
 * byte slices alias that input; this function does not verify signatures.
 * Record meaning, signer authority, reserved bytes, hash option, and causal
 * semantics remain the responsibility of the caller's domain profile.
 */
export const scanCanonicalPublicEntryV0 = (
	entryBytes: Uint8Array,
	limits: CanonicalPublicEntryV0ScanLimits,
): CanonicalPublicEntryV0Scan => {
	if (
		!Number.isSafeInteger(limits.minimumSignatures) ||
		!Number.isSafeInteger(limits.maximumSignatures) ||
		limits.minimumSignatures < 1 ||
		limits.maximumSignatures < limits.minimumSignatures
	) {
		throw new Error("Invalid internal EntryV0 signature bounds");
	}
	const reader = new BoundsReader(entryBytes, limits.label);
	reader.expectU8(0, "entry variant");
	const metaBytes = readPublicWrapper(reader, limits.label, "metadata");
	const payloadContainerBytes = readPublicWrapper(
		reader,
		limits.label,
		"payload",
	);
	const reservedBytes = reader.readExact(4, "reserved bytes");
	const signablePrefixLength = reader.position;
	if (reader.readU8("signatures option") !== 1) {
		throw new Error(`${limits.label} EntryV0 must contain signatures`);
	}
	reader.expectU8(0, "signatures variant");
	const signatureCount = reader.readU32("signature count");
	if (
		signatureCount < limits.minimumSignatures ||
		signatureCount > limits.maximumSignatures ||
		signatureCount > Math.floor(reader.remaining / 6)
	) {
		const expected =
			limits.minimumSignatures === 1 && limits.maximumSignatures === 1
				? "exactly one signature"
				: limits.minimumSignatures === limits.maximumSignatures
					? `exactly ${limits.minimumSignatures}`
					: `${limits.minimumSignatures}-${limits.maximumSignatures} signatures`;
		throw new Error(`${limits.label} EntryV0 must contain ${expected}`);
	}
	for (let index = 0; index < signatureCount; index++) {
		scanSignature(
			readPublicWrapper(reader, limits.label, "signature"),
			limits.label,
		);
	}
	const hashOption = reader.readU8("hash option");
	if (hashOption === 1) {
		reader.readBytes("hash");
	} else if (hashOption !== 0) {
		throw new Error(`${limits.label} EntryV0 has invalid hash option`);
	}
	reader.expectDone("storage");

	const directParentCount = scanMeta(metaBytes, limits);
	const payloadBytes = scanPayload(payloadContainerBytes, limits.label);
	const signableBytes = new Uint8Array(signablePrefixLength + 2);
	signableBytes.set(entryBytes.subarray(0, signablePrefixLength));

	const entry = deserialize(entryBytes, Entry);
	if (!(entry instanceof EntryV0)) {
		throw new Error(`${limits.label} entry must use EntryV0`);
	}
	if (!equals(entryBytes, serialize(entry))) {
		throw new Error(`${limits.label} EntryV0 storage is not canonical`);
	}
	if (
		!(entry._meta instanceof DecryptedThing) ||
		!(entry._payload instanceof DecryptedThing)
	) {
		throw new Error(
			`${limits.label} EntryV0 metadata and payload must be public`,
		);
	}
	if (
		entry._signatures === undefined ||
		entry._signatures.signatures.length !== signatureCount ||
		entry._signatures.signatures.some(
			(signature) => !(signature instanceof DecryptedThing),
		)
	) {
		throw new Error(`${limits.label} EntryV0 signatures must be public`);
	}
	entry.init({ encoding: NO_ENCODING });
	const meta = entry.meta;
	if (!equals(metaBytes, serialize(meta))) {
		throw new Error(`${limits.label} EntryV0 nested encoding is not canonical`);
	}
	if (!equals(payloadBytes, entry.payload.data)) {
		throw new Error(`${limits.label} EntryV0 payload framing is inconsistent`);
	}
	const signatures = entry.signatures;
	if (signatures.length !== signatureCount) {
		throw new Error(
			`${limits.label} EntryV0 signature framing is inconsistent`,
		);
	}
	return {
		entry,
		meta,
		signatures,
		metaBytes,
		payloadBytes,
		signableBytes,
		reservedBytes,
		hasHash: hashOption === 1,
		directParentCount,
		signatureCount,
	};
};
