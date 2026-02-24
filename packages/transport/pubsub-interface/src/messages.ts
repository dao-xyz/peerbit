import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
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
