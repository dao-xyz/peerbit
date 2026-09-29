import { field, fixedArray, serialize, variant } from "@dao-xyz/borsh";
import { equals } from "uint8arrays";
import { Operation } from "./operation.js";

export const CHECKPOINT_MAX_KEY_BYTES = 1024;
export const CHECKPOINT_MAX_DOCUMENT_BYTES = 1024 * 1024;
export const CHECKPOINT_MAX_DOCUMENT_DEPTH = 32;
export const CHECKPOINT_MAX_DOCUMENT_FIELDS = 4096;
const MAX_DOCUMENT_NODES = 65_536;
// Operation discriminator (2), resource (32), epoch (8), checkpoint (32), kind
// (1), and the key/data length frames (4 each).
const ENVELOPE_OVERHEAD = 83;
export const CHECKPOINT_MAX_OPERATION_BYTES =
	ENVELOPE_OVERHEAD + CHECKPOINT_MAX_KEY_BYTES + CHECKPOINT_MAX_DOCUMENT_BYTES;

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype);
const byteLengthGetter = Object.getOwnPropertyDescriptor(
	typedArrayPrototype,
	"byteLength",
)!.get!;
const tagGetter = Object.getOwnPropertyDescriptor(
	typedArrayPrototype,
	Symbol.toStringTag,
)!.get!;
const setBytes = Uint8Array.prototype.set;

/** Bound and copy byte inputs without invoking caller-provided iterators. */
export const captureCheckpointBytes = (
	bytes: Uint8Array,
	maximumBytes: number,
): Uint8Array => {
	const length = byteLengthGetter.call(bytes);
	if (
		!Number.isSafeInteger(maximumBytes) ||
		maximumBytes < 0 ||
		tagGetter.call(bytes) !== "Uint8Array" ||
		length > maximumBytes
	) {
		throw new Error("Checkpoint byte capacity exceeded");
	}
	const copy = new Uint8Array(length);
	setBytes.call(copy, bytes);
	return copy;
};

const captureDigest = (bytes: Uint8Array): Uint8Array => {
	const digest = captureCheckpointBytes(bytes, 32);
	if (digest.byteLength !== 32) {
		throw new Error("Checkpoint digest must contain exactly 32 bytes");
	}
	return digest;
};

export const assertCheckpointKey = (key: string): void => {
	if (
		typeof key !== "string" ||
		key.length === 0 ||
		key.length > CHECKPOINT_MAX_KEY_BYTES
	) {
		throw new Error("Invalid checkpoint document key");
	}
	const bytes = encoder.encode(key);
	if (
		bytes.byteLength > CHECKPOINT_MAX_KEY_BYTES ||
		decoder.decode(bytes) !== key
	) {
		throw new Error("Invalid checkpoint document key encoding");
	}
};

export type CheckpointJSONValue =
	| null
	| boolean
	| number
	| string
	| CheckpointJSONValue[]
	| { [key: string]: CheckpointJSONValue };

export type CheckpointDocumentValue = {
	id: string;
	[key: string]: CheckpointJSONValue;
};

/** Produce one bounded, deterministic JSON representation without invoking getters. */
const canonicalDocument = (value: unknown): Uint8Array => {
	let nodes = 0;
	let fields = 0;
	let characters = 0;
	const active = new Set<object>();
	const add = (text: string): string => {
		characters += text.length;
		if (characters > CHECKPOINT_MAX_DOCUMENT_BYTES) {
			throw new Error("Checkpoint document byte capacity exceeded");
		}
		return text;
	};
	const node = () => {
		if (++nodes > MAX_DOCUMENT_NODES) {
			throw new Error("Checkpoint document node capacity exceeded");
		}
	};
	const encode = (current: unknown, depth: number): string => {
		node();
		if (current === null || typeof current === "boolean") {
			return add(JSON.stringify(current));
		}
		if (typeof current === "number" && Number.isFinite(current)) {
			return add(JSON.stringify(current));
		}
		if (typeof current === "string") {
			if (current.length > CHECKPOINT_MAX_DOCUMENT_BYTES) {
				throw new Error("Checkpoint document string capacity exceeded");
			}
			return add(JSON.stringify(current));
		}
		if (typeof current !== "object" || current === null) {
			throw new Error("Checkpoint document must contain only JSON values");
		}
		if (depth >= CHECKPOINT_MAX_DOCUMENT_DEPTH || active.has(current)) {
			throw new Error("Checkpoint document is too deep or cyclic");
		}
		const array = Array.isArray(current);
		const prototype = Object.getPrototypeOf(current);
		if (
			(array && prototype !== Array.prototype) ||
			(!array && prototype !== Object.prototype && prototype !== null)
		) {
			throw new Error("Checkpoint document requires plain objects");
		}
		const ownKeys = Reflect.ownKeys(current);
		if (ownKeys.some((key) => typeof key !== "string")) {
			throw new Error("Checkpoint document cannot contain symbol properties");
		}
		active.add(current);
		try {
			if (array) {
				if (
					current.length > MAX_DOCUMENT_NODES ||
					ownKeys.length !== current.length + 1
				) {
					throw new Error("Invalid checkpoint document array");
				}
				add("[]");
				const elements: string[] = [];
				for (let index = 0; index < current.length; index++) {
					const property = Object.getOwnPropertyDescriptor(current, index);
					if (!property?.enumerable || !("value" in property)) {
						throw new Error("Checkpoint arrays require ordinary elements");
					}
					if (index > 0) add(",");
					elements.push(encode(property.value, depth + 1));
				}
				return `[${elements.join(",")}]`;
			}
			fields += ownKeys.length;
			if (fields > CHECKPOINT_MAX_DOCUMENT_FIELDS) {
				throw new Error("Checkpoint document field capacity exceeded");
			}
			add("{}");
			const properties: string[] = [];
			for (const key of (ownKeys as string[]).sort()) {
				const property = Object.getOwnPropertyDescriptor(current, key)!;
				if (!property.enumerable || !("value" in property)) {
					throw new Error("Checkpoint objects require ordinary fields");
				}
				node();
				if (
					key.length > CHECKPOINT_MAX_KEY_BYTES ||
					encoder.encode(key).byteLength > CHECKPOINT_MAX_KEY_BYTES
				) {
					throw new Error("Checkpoint document field name is too long");
				}
				if (properties.length > 0) add(",");
				const name = add(JSON.stringify(key));
				add(":");
				properties.push(`${name}:${encode(property.value, depth + 1)}`);
			}
			return `{${properties.join(",")}}`;
		} finally {
			active.delete(current);
		}
	};
	const bytes = encoder.encode(encode(value, 0));
	if (bytes.byteLength > CHECKPOINT_MAX_DOCUMENT_BYTES) {
		throw new Error("Checkpoint document byte capacity exceeded");
	}
	return bytes;
};

/** Reject excessive structural work before invoking the generic JSON parser. */
export const boundCheckpointJSON = (bytes: Uint8Array): void => {
	let depth = 0;
	let nodes = 0;
	let fields = 0;
	let quoted = false;
	let escaped = false;
	let primitive = false;
	for (const byte of bytes) {
		if (quoted) {
			if (escaped) escaped = false;
			else if (byte === 92) escaped = true;
			else if (byte === 34) quoted = false;
			continue;
		}
		if (byte === 34) {
			quoted = true;
			primitive = false;
			nodes++;
		} else if (byte === 123 || byte === 91) {
			primitive = false;
			nodes++;
			if (++depth > CHECKPOINT_MAX_DOCUMENT_DEPTH) {
				throw new Error("Checkpoint document is too deep");
			}
		} else if (byte === 125 || byte === 93) {
			depth--;
			primitive = false;
		} else if (byte === 58) {
			primitive = false;
			if (++fields > CHECKPOINT_MAX_DOCUMENT_FIELDS) {
				throw new Error("Checkpoint document field capacity exceeded");
			}
		} else if (
			byte === 44 ||
			byte === 32 ||
			byte === 9 ||
			byte === 10 ||
			byte === 13
		) {
			primitive = false;
		} else if (!primitive) {
			primitive = true;
			nodes++;
		}
		if (nodes > MAX_DOCUMENT_NODES) {
			throw new Error("Checkpoint document node capacity exceeded");
		}
	}
};

const documentKey = (value: unknown): string => {
	if (!value || typeof value !== "object" || Array.isArray(value)) {
		throw new Error("Checkpoint document must be a plain object");
	}
	const property = Object.getOwnPropertyDescriptor(value, "id");
	if (!property?.enumerable || !("value" in property)) {
		throw new Error("Checkpoint document requires an ordinary id field");
	}
	assertCheckpointKey(property.value);
	return property.value;
};

/** Fixed identity-index value for this resource, independent of user schemas. */
@variant([1, 0])
export class CheckpointDocument {
	@field({ type: "string" })
	readonly id: string;

	@field({ type: Uint8Array })
	readonly value: Uint8Array;

	constructor(value: CheckpointDocumentValue) {
		this.value = canonicalDocument(value);
		// Read the key from the captured value, not from a second observation of
		// caller-owned properties that may have changed while being captured.
		this.id = documentKey(JSON.parse(decoder.decode(this.value)));
	}

	static from(value: CheckpointDocumentValue): CheckpointDocument {
		documentKey(value);
		return new CheckpointDocument(value);
	}

	static fromBytes(id: string, input: Uint8Array): CheckpointDocument {
		assertCheckpointKey(id);
		const bytes = captureCheckpointBytes(input, CHECKPOINT_MAX_DOCUMENT_BYTES);
		boundCheckpointJSON(bytes);
		const value: unknown = JSON.parse(decoder.decode(bytes));
		if (documentKey(value) !== id) {
			throw new Error("Checkpoint document key does not match the operation");
		}
		if (!equals(bytes, canonicalDocument(value))) {
			throw new Error("Noncanonical checkpoint document JSON");
		}
		// The bytes have already passed every constructor invariant. Avoid a
		// second serialization merely to construct the fixed index wrapper.
		return Object.create(CheckpointDocument.prototype, {
			id: { value: id, enumerable: true },
			value: { value: bytes, enumerable: true },
		}) as CheckpointDocument;
	}

	decode(): CheckpointDocumentValue {
		const document = CheckpointDocument.fromBytes(this.id, this.value);
		return JSON.parse(
			decoder.decode(document.value),
		) as CheckpointDocumentValue;
	}
}

/**
 * Signed payload of the checkpoint-backed Documents resource. Both puts and
 * tombstones are retained APPEND entries; this is deliberately not a legacy
 * PutOperation/DeleteOperation subtype. Decoding establishes canonical framing,
 * not signer authority, resource membership or admission into the current epoch.
 */
@variant(5)
export class CheckpointOperation extends Operation {
	@field({ type: fixedArray("u8", 32) })
	readonly resource: Uint8Array;

	@field({ type: "u64" })
	readonly epoch: bigint;

	@field({ type: fixedArray("u8", 32) })
	readonly checkpoint: Uint8Array;

	@field({ type: "u8" })
	readonly kind: 0 | 1;

	@field({ type: "string" })
	readonly key: string;

	@field({ type: Uint8Array })
	readonly data: Uint8Array;

	constructor(properties: {
		resource: Uint8Array;
		epoch: bigint;
		checkpoint: Uint8Array;
		kind: 0 | 1;
		key: string;
		data: Uint8Array;
	}) {
		super();
		this.resource = captureDigest(properties.resource);
		this.epoch = properties.epoch;
		this.checkpoint = captureDigest(properties.checkpoint);
		this.kind = properties.kind;
		this.key = properties.key;
		this.data = captureCheckpointBytes(
			properties.data,
			CHECKPOINT_MAX_DOCUMENT_BYTES,
		);
		if (
			typeof this.epoch !== "bigint" ||
			this.epoch < 0n ||
			this.epoch > 0xffffffffffffffffn
		) {
			throw new Error("Invalid checkpoint operation epoch");
		}
		assertCheckpointKey(this.key);
		if (
			(this.kind !== 0 && this.kind !== 1) ||
			(this.kind === 1 && this.data.byteLength !== 0)
		) {
			throw new Error("Invalid checkpoint operation kind or tombstone data");
		}
	}
}

/**
 * Parse only this bounded fixed profile. In particular, do not deserialize an
 * arbitrary Operation or application Borsh schema before framing is validated.
 * The result owns its buffers independently of the supplied payload.
 */
export const decodeCheckpointOperation = (
	input: Uint8Array,
): CheckpointOperation => {
	const bytes = captureCheckpointBytes(input, CHECKPOINT_MAX_OPERATION_BYTES);
	if (
		bytes.byteLength < ENVELOPE_OVERHEAD ||
		bytes[0] !== 0 ||
		bytes[1] !== 5
	) {
		throw new Error("Invalid checkpoint operation framing");
	}
	const frame = new DataView(bytes.buffer);
	const keyLength = frame.getUint32(75, true);
	if (
		keyLength === 0 ||
		keyLength > CHECKPOINT_MAX_KEY_BYTES ||
		ENVELOPE_OVERHEAD + keyLength > bytes.byteLength
	) {
		throw new Error("Invalid checkpoint operation key length");
	}
	const dataLength = frame.getUint32(79 + keyLength, true);
	if (
		dataLength > CHECKPOINT_MAX_DOCUMENT_BYTES ||
		ENVELOPE_OVERHEAD + keyLength + dataLength !== bytes.byteLength
	) {
		throw new Error("Invalid checkpoint operation data length");
	}
	const operation = new CheckpointOperation({
		resource: bytes.subarray(2, 34),
		epoch: frame.getBigUint64(34, true),
		checkpoint: bytes.subarray(42, 74),
		kind: bytes[74] as 0 | 1,
		key: decoder.decode(bytes.subarray(79, 79 + keyLength)),
		data: bytes.subarray(ENVELOPE_OVERHEAD + keyLength),
	});
	if (!equals(bytes, serialize(operation))) {
		throw new Error("Noncanonical checkpoint operation");
	}
	return operation;
};
