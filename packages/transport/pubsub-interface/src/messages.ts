import { Uint8ArrayList } from "uint8arraylist";
import {
	field,
	vec,
	variant,
	serialize,
	deserialize,
	option
} from "@dao-xyz/borsh";
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
		throw new Error("Unsupported");
	}
}

export const toUint8Array = (arr: Uint8ArrayList | Uint8Array) =>
	arr instanceof Uint8ArrayList ? arr.subarray() : arr;

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

	_serialized: Uint8ArrayList;

	bytes() {
		if (this._serialized) {
			return this._serialized;
		}

		return serialize(this);
	}
	static from(bytes: Uint8Array | Uint8ArrayList): PubSubData {
		const ret = deserialize(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			PubSubData
		);
		if (bytes instanceof Uint8ArrayList) {
			ret._serialized = bytes;
		}
		return ret;
	}
}

@variant(0)
export class Subscription {
	@field({ type: "string" })
	topic: string;

	@field({ type: option(Uint8Array) })
	data?: Uint8Array; // if omitted, the subcription event is a no-op (will not replace anything)

	constructor(topic: string, data?: Uint8Array) {
		this.topic = topic;
		this.data = data;
	}
}

@variant(1)
export class Subscribe extends PubSubMessage {
	@field({ type: vec(Subscription) })
	subscriptions: Subscription[];

	constructor(options: { subscriptions: Subscription[] }) {
		super();
		this.subscriptions = options.subscriptions;
	}

	_serialized: Uint8ArrayList;

	bytes() {
		if (this._serialized) {
			return this._serialized;
		}
		return serialize(this);
	}
	static from(bytes: Uint8Array | Uint8ArrayList): Subscribe {
		const ret = deserialize(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			Subscribe
		);
		if (bytes instanceof Uint8ArrayList) {
			ret._serialized = bytes;
		}
		return ret;
	}

	get topics() {
		return this.subscriptions.map((x) => x.topic);
	}
}

@variant(0)
export class Unsubscription {
	@field({ type: "string" })
	topic: string;
	constructor(topic: string) {
		this.topic = topic;
	}
}

@variant(2)
export class Unsubscribe extends PubSubMessage {
	@field({ type: vec(Unsubscription) })
	unsubscriptions: Unsubscription[];

	constructor(options: { topics: string[] }) {
		super();
		this.unsubscriptions = options.topics.map((x) => new Unsubscription(x));
	}

	_serialized: Uint8ArrayList;

	bytes() {
		if (this._serialized) {
			return this._serialized;
		}
		return serialize(this);
	}

	static from(bytes: Uint8Array | Uint8ArrayList): Unsubscribe {
		const ret = deserialize(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			Unsubscribe
		);
		if (bytes instanceof Uint8ArrayList) {
			ret._serialized = bytes;
		}
		return ret;
	}

	get topics() {
		return this.unsubscriptions.map((x) => x.topic);
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

	_serialized: Uint8ArrayList;

	bytes() {
		if (this._serialized) {
			return this._serialized;
		}
		return serialize(this);
	}

	static from(bytes: Uint8Array | Uint8ArrayList): GetSubscribers {
		const ret = deserialize(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			GetSubscribers
		);
		if (bytes instanceof Uint8ArrayList) {
			ret._serialized = bytes;
		}
		return ret;
	}
}
