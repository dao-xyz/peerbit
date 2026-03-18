import {
	deserialize,
	field,
	option,
	serialize,
	variant,
	vec,
} from "@dao-xyz/borsh";
import { Uint8ArrayList } from "uint8arraylist";

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
		const ret = deserialize(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			TopicRootCandidates,
		);
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
