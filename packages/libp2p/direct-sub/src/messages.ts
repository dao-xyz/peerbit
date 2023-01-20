import { Uint8ArrayList } from "uint8arraylist";
import {
	field,
	vec,
	variant,
	serialize,
	deserialize,
} from "@dao-xyz/borsh";
export abstract class PubSubMessage {
	abstract serialize(): Uint8Array | Uint8ArrayList;
	static deserialize(bytes: Uint8ArrayList | Uint8Array) {
		const first = bytes instanceof Uint8Array ? bytes[0] : bytes.get(0);
		if (first === 0) {
			return PubSubData.deserialize(bytes);
		}
		if (first === 1) {
			return Subscribe.deserialize(bytes);
		}
		if (first === 2) {
			return Unsubscribe.deserialize(bytes);
		}

		if (first === 3) {
			return GetSubscribers.deserialize(bytes);
		}
		throw new Error("Unsupported");
	}
}

export const toUint8Array = (arr: Uint8ArrayList | Uint8Array) =>
	arr instanceof Uint8ArrayList ? arr.subarray() : arr;

@variant(0)
export class PubSubData extends PubSubMessage {
	@field({ type: vec('string') })
	topics: string[];

	@field({ type: Uint8Array })
	data: Uint8Array;

	constructor(options: {
		topics: string[];
		data: Uint8Array | Uint8ArrayList;
	}) {
		super();
		this.data =
			options.data instanceof Uint8Array
				? options.data
				: options.data.subarray();
		this.topics = options.topics;
	}

	_serialized: Uint8ArrayList;

	serialize() {
		if (this._serialized) {
			return this._serialized;
		}

		return serialize(this);
	}
	static deserialize(bytes: Uint8Array | Uint8ArrayList): PubSubData {
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

@variant(1)
export class Subscribe extends PubSubMessage {
	@field({ type: vec('string') })
	topics: string[];

	constructor(options: { topics: string[] }) {
		super();
		if (options.topics.length > 255) {
			throw new Error("Can at most subscribe to 255 topics at once");
		}
		this.topics = options.topics;
	}

	_serialized: Uint8ArrayList;

	serialize() {
		if (this._serialized) {
			return this._serialized;
		}
		return serialize(this);
	}
	static deserialize(bytes: Uint8Array | Uint8ArrayList): Subscribe {
		const ret = deserialize(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			Subscribe
		);
		if (bytes instanceof Uint8ArrayList) {
			ret._serialized = bytes;
		}
		return ret;
	}
}

@variant(2)
export class Unsubscribe extends PubSubMessage {
	@field({ type: vec('string') })
	topics: string[];

	constructor(options: { topics: string[] }) {
		super();
		this.topics = options.topics;
	}

	_serialized: Uint8ArrayList;

	serialize() {
		if (this._serialized) {
			return this._serialized;
		}
		return serialize(this);
	}

	static deserialize(bytes: Uint8Array | Uint8ArrayList): Unsubscribe {
		const ret = deserialize(
			bytes instanceof Uint8Array ? bytes : bytes.subarray(),
			Unsubscribe
		);
		if (bytes instanceof Uint8ArrayList) {
			ret._serialized = bytes;
		}
		return ret;
	}
}

@variant(3)
export class GetSubscribers extends PubSubMessage {
	@field({ type: vec('string') })
	topics: string[];

	// add stop filter list to prvent this message from propgating to unecessary peers

	constructor(options: { topics: string[] }) {
		super();
		this.topics = options.topics;
	}

	_serialized: Uint8ArrayList;

	serialize() {
		if (this._serialized) {
			return this._serialized;
		}
		return serialize(this);
	}

	static deserialize(bytes: Uint8Array | Uint8ArrayList): GetSubscribers {
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
