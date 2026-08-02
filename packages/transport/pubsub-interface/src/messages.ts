import {
	deserialize,
	field,
	option,
	serialize,
	variant,
	vec,
} from "@dao-xyz/borsh";
import { Uint8ArrayList } from "uint8arraylist";

export const TOPIC_ROOT_CANDIDATES_MAX = 64;
const TOPIC_ROOT_CANDIDATE_LENGTH = 44;
export const TOPIC_ROOT_CANDIDATES_MAX_FRAME_BYTES =
	1 + 4 + TOPIC_ROOT_CANDIDATES_MAX * (4 + TOPIC_ROOT_CANDIDATE_LENGTH);

// A SHA-256 digest in canonical, padded RFC 4648 Base64 is 44 ASCII bytes.
// The final data character has two zero padding bits, so only these Base64
// indices can precede the trailing `=`.
const CANONICAL_SHA256_BASE64 = /^[A-Za-z0-9+/]{42}[AEIMQUYcgkosw048]=$/;

export const isCanonicalTopicRootCandidate = (candidate: string): boolean =>
	typeof candidate === "string" && CANONICAL_SHA256_BASE64.test(candidate);

export const assertCanonicalTopicRootCandidates = (
	candidates: readonly string[],
): void => {
	if (candidates.length > TOPIC_ROOT_CANDIDATES_MAX) {
		throw new Error(
			`Topic-root candidate count exceeds ${TOPIC_ROOT_CANDIDATES_MAX}`,
		);
	}
	for (const candidate of candidates) {
		if (!isCanonicalTopicRootCandidate(candidate)) {
			throw new Error("Topic-root candidate is not a canonical SHA-256 hash");
		}
	}
};

const frameByteLength = (bytes: Uint8Array | Uint8ArrayList): number =>
	bytes.byteLength;

const frameByteAt = (
	bytes: Uint8Array | Uint8ArrayList,
	offset: number,
): number => (bytes instanceof Uint8Array ? bytes[offset]! : bytes.get(offset));

export const assertTopicRootCandidatesFrame = (
	bytes: Uint8Array | Uint8ArrayList,
): void => {
	const byteLength = frameByteLength(bytes);
	if (byteLength > TOPIC_ROOT_CANDIDATES_MAX_FRAME_BYTES) {
		throw new Error(
			`Topic-root candidates frame exceeds ${TOPIC_ROOT_CANDIDATES_MAX_FRAME_BYTES} bytes`,
		);
	}
	if (byteLength < 5) {
		throw new Error("Topic-root candidates frame is truncated");
	}
	if (frameByteAt(bytes, 0) !== 4) {
		throw new Error("Invalid topic-root candidates frame variant");
	}
	const count =
		frameByteAt(bytes, 1) |
		(frameByteAt(bytes, 2) << 8) |
		(frameByteAt(bytes, 3) << 16) |
		(frameByteAt(bytes, 4) << 24);
	if (count >>> 0 > TOPIC_ROOT_CANDIDATES_MAX) {
		throw new Error(
			`Topic-root candidate count exceeds ${TOPIC_ROOT_CANDIDATES_MAX}`,
		);
	}
};

export abstract class PubSubMessage {
	abstract bytes(): Uint8Array | Uint8ArrayList;
	static from(bytes: Uint8Array) {
		const first = bytes[0];
		if (first === 0) {
			return PubSubData.from(bytes);
		}
		if (first === 1) {
			return Subscribe.from(bytes);
		}
		if (first === 2) {
			return Unsubscribe.from(bytes);
		}

		if (first === 3) {
			return GetSubscribers.from(bytes);
		}

		if (first === 4) {
			return TopicRootCandidates.from(bytes);
		}

		if (first === 5) {
			return PeerUnavailable.from(bytes);
		}

		if (first === 6) {
			return TopicRootQuery.from(bytes);
		}

		if (first === 7) {
			return TopicRootQueryResponse.from(bytes);
		}

		throw new Error("Unsupported");
	}
}

@variant(0)
export class PubSubData extends PubSubMessage {
	@field({ type: vec("string") })
	topics: string[];

	@field({ type: "bool" })
	strict: boolean; // only deliver message to initial to receivers

	@field({ type: Uint8Array })
	data: Uint8Array;

	constructor(options: {
		topics: string[];
		data: Uint8Array | Uint8ArrayList;
		strict?: boolean;
	}) {
		super();
		this.data =
			options.data instanceof Uint8Array
				? options.data
				: options.data.subarray();
		this.topics = options.topics;
		this.strict = options.strict ?? false;
	}

	private _serialized!: Uint8ArrayList;

	bytes() {
		if (this._serialized) {
			return this._serialized;
		}

		return serialize(this);
	}
	static from(bytes: Uint8Array | Uint8ArrayList): PubSubData {
		const ret = deserialize(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			PubSubData,
		);
		if (bytes instanceof Uint8ArrayList) {
			ret._serialized = bytes;
		}
		return ret;
	}
}

@variant(1)
export class Subscribe extends PubSubMessage {
	@field({ type: vec("string") })
	topics: string[];

	@field({ type: "bool" })
	requestSubscribers: boolean;

	constructor(options: { topics: string[]; requestSubscribers: boolean }) {
		super();
		this.topics = options.topics;
		this.requestSubscribers = options.requestSubscribers;
	}

	private _serialized!: Uint8ArrayList;

	bytes() {
		if (this._serialized) {
			return this._serialized;
		}
		return serialize(this);
	}
	static from(bytes: Uint8Array | Uint8ArrayList): Subscribe {
		const ret = deserialize(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			Subscribe,
		);
		if (bytes instanceof Uint8ArrayList) {
			ret._serialized = bytes;
		}
		return ret;
	}
}

@variant(2)
export class Unsubscribe extends PubSubMessage {
	@field({ type: vec("string") })
	topics: string[];

	constructor(options: { topics: string[] }) {
		super();
		this.topics = options.topics;
	}

	private _serialized!: Uint8ArrayList;

	bytes() {
		if (this._serialized) {
			return this._serialized;
		}
		return serialize(this);
	}

	static from(bytes: Uint8Array | Uint8ArrayList): Unsubscribe {
		const ret = deserialize(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			Unsubscribe,
		);
		if (bytes instanceof Uint8ArrayList) {
			ret._serialized = bytes;
		}
		return ret;
	}
}

@variant(3)
export class GetSubscribers extends PubSubMessage {
	@field({ type: vec("string") })
	topics: string[];

	// add stop filter list to prvent this message from propgating to unecessary peers

	constructor(options: { topics: string[] }) {
		super();
		this.topics = options.topics;
	}

	_serialized!: Uint8ArrayList;

	bytes() {
		if (this._serialized) {
			return this._serialized;
		}
		return serialize(this);
	}

	static from(bytes: Uint8Array | Uint8ArrayList): GetSubscribers {
		const ret = deserialize(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			GetSubscribers,
		);
		if (bytes instanceof Uint8ArrayList) {
			ret._serialized = bytes;
		}
		return ret;
	}
}

// Internal control-plane message: used to converge deterministic topic-root
// candidate sets in small ad-hoc networks (when no explicit candidates/trackers
// are configured). This keeps shard-root resolution stable across partially
// connected topologies (e.g. star graphs).
@variant(4)
export class TopicRootCandidates extends PubSubMessage {
	@field({ type: vec("string") })
	candidates: string[];

	constructor(options: { candidates: string[] }) {
		super();
		this.candidates = options.candidates;
	}

	private _serialized!: Uint8ArrayList;

	bytes() {
		if (this._serialized) {
			return this._serialized;
		}
		return serialize(this);
	}

	static from(bytes: Uint8Array | Uint8ArrayList): TopicRootCandidates {
		assertTopicRootCandidatesFrame(bytes);
		const ret = deserialize(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			TopicRootCandidates,
		);
		assertCanonicalTopicRootCandidates(ret.candidates);
		if (bytes instanceof Uint8ArrayList) {
			ret._serialized = bytes;
		}
		return ret;
	}
}

@variant(5)
export class PeerUnavailable extends PubSubMessage {
	@field({ type: "string" })
	publicKeyHash: string;

	@field({ type: "u64" })
	session: bigint;

	@field({ type: "u64" })
	timestamp: bigint;

	@field({ type: vec("string") })
	topics: string[];

	constructor(options: {
		publicKeyHash: string;
		session: bigint;
		timestamp: bigint;
		topics: string[];
	}) {
		super();
		this.publicKeyHash = options.publicKeyHash;
		this.session = options.session;
		this.timestamp = options.timestamp;
		this.topics = options.topics;
	}

	private _serialized!: Uint8ArrayList;

	bytes() {
		if (this._serialized) {
			return this._serialized;
		}
		return serialize(this);
	}

	static from(bytes: Uint8Array | Uint8ArrayList): PeerUnavailable {
		const ret = deserialize(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			PeerUnavailable,
		);
		if (bytes instanceof Uint8ArrayList) {
			ret._serialized = bytes;
		}
		return ret;
	}
}

@variant(6)
export class TopicRootQuery extends PubSubMessage {
	@field({ type: "u32" })
	requestId: number;

	@field({ type: "string" })
	topic: string;

	constructor(options: { requestId: number; topic: string }) {
		super();
		this.requestId = options.requestId;
		this.topic = options.topic;
	}

	private _serialized!: Uint8ArrayList;

	bytes() {
		if (this._serialized) {
			return this._serialized;
		}
		return serialize(this);
	}

	static from(bytes: Uint8Array | Uint8ArrayList): TopicRootQuery {
		const ret = deserialize(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			TopicRootQuery,
		);
		if (bytes instanceof Uint8ArrayList) {
			ret._serialized = bytes;
		}
		return ret;
	}
}

@variant(7)
export class TopicRootQueryResponse extends PubSubMessage {
	@field({ type: "u32" })
	requestId: number;

	@field({ type: "string" })
	topic: string;

	@field({ type: option("string") })
	root?: string;

	constructor(options: { requestId: number; topic: string; root?: string }) {
		super();
		this.requestId = options.requestId;
		this.topic = options.topic;
		this.root = options.root;
	}

	private _serialized!: Uint8ArrayList;

	bytes() {
		if (this._serialized) {
			return this._serialized;
		}
		return serialize(this);
	}

	static from(bytes: Uint8Array | Uint8ArrayList): TopicRootQueryResponse {
		const ret = deserialize(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			TopicRootQueryResponse,
		);
		if (bytes instanceof Uint8ArrayList) {
			ret._serialized = bytes;
		}
		return ret;
	}
}
