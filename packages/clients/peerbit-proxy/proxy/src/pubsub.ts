import { deserialize, field, option, variant, vec } from "@dao-xyz/borsh";
import { type PeerId } from "@libp2p/interface";
import { PublicSignKey, getPublicKeyFromPeerId } from "@peerbit/crypto";
import {
	DataEvent,
	type PubSubEvents,
	PublishEvent,
	type PublishOptions,
	SubscriptionEvent,
	UnsubcriptionEvent,
} from "@peerbit/pubsub-interface";
import {
	DeliveryMode,
	Message as StreamMessage,
} from "@peerbit/stream-interface";
import { Message } from "./message.js";

@variant(6)
export abstract class PubSubMessage extends Message {}

@variant(0)
export class REQ_GetSubscribers extends PubSubMessage {
	@field({ type: "string" })
	topic: string;

	constructor(topic: string) {
		super();
		this.topic = topic;
	}
}

@variant(1)
export class RESP_GetSubscribers extends PubSubMessage {
	@field({ type: option(vec(PublicSignKey)) })
	subscribers?: PublicSignKey[];

	constructor(subscribers?: PublicSignKey[]) {
		super();
		this.subscribers = subscribers;
	}
}

@variant(2)
export class REQ_RequestSubscribers extends PubSubMessage {
	@field({ type: "string" })
	topic: string;

	constructor(topic: string) {
		super();
		this.topic = topic;
	}
}

@variant(3)
export class RESP_RequestSubscribers extends PubSubMessage {}

@variant(4)
export class REQ_Publish extends PubSubMessage {
	@field({ type: Uint8Array })
	data: Uint8Array;

	@field({ type: option(Uint8Array) })
	id?: Uint8Array;

	@field({ type: option(vec("string")) })
	topics?: string[];

	@field({ type: option(DeliveryMode) })
	mode?: DeliveryMode;

	constructor(data: Uint8Array, options?: PublishOptions) {
		super();
		this.data = data;
		this.topics = options?.topics;
		this.mode = options?.mode;
		this.id = options?.id;
	}
}

@variant(5)
export class RESP_Publish extends PubSubMessage {
	constructor(messageId: Uint8Array) {
		super(messageId);
	}
}

@variant(6)
export class REQ_Subscribe extends PubSubMessage {
	@field({ type: "string" })
	topic: string;

	constructor(topic: string) {
		super();
		this.topic = topic;
	}
}

@variant(7)
export class RESP_Subscribe extends PubSubMessage {}

@variant(8)
export class REQ_Unsubscribe extends PubSubMessage {
	constructor(topic: string, options?: { force?: boolean }) {
		super();
		this.topic = topic;
		this.force = options?.force;
	}
	@field({ type: "string" })
	topic: string;

	@field({ type: option("bool") })
	force?: boolean;

	@field({ type: option(Uint8Array) })
	data?: Uint8Array;
}

@variant(9)
export class RESP_Unsubscribe extends PubSubMessage {
	@field({ type: "bool" })
	value: boolean;

	constructor(value: boolean) {
		super();
		this.value = value;
	}
}

@variant(10)
export class REQ_PubsubWaitFor extends PubSubMessage {
	@field({ type: "string" })
	hash: string;

	constructor(publicKey: PeerId | PublicSignKey | string) {
		super();
		this.hash =
			typeof publicKey === "string"
				? publicKey
				: publicKey instanceof PublicSignKey
					? publicKey.hashcode()
					: getPublicKeyFromPeerId(publicKey).hashcode();
	}
}

@variant(11)
export class RESP_PubsubWaitFor extends PubSubMessage {}

@variant(12)
export class REQ_AddEventListener extends PubSubMessage {
	@field({ type: "string" })
	type: keyof PubSubEvents;

	@field({ type: Uint8Array })
	emitMessageId: Uint8Array;

	constructor(type: keyof PubSubEvents, emitMessageId: Uint8Array) {
		super();
		this.type = type;
		this.emitMessageId = emitMessageId;
	}
}

@variant(13)
export class RESP_AddEventListener extends PubSubMessage {}

@variant(14)
export class REQ_RemoveEventListener extends PubSubMessage {
	@field({ type: "string" })
	type: keyof PubSubEvents;

	constructor(type: keyof PubSubEvents) {
		super();
		this.type = type;
	}
}

@variant(15)
export class RESP_RemoveEventListener extends PubSubMessage {}

@variant(16)
export class RESP_EmitEvent extends PubSubMessage {
	@field({ type: "string" })
	type: keyof PubSubEvents;

	@field({ type: Uint8Array })
	data: Uint8Array;

	constructor(type: keyof PubSubEvents, data: Uint8Array) {
		super();
		this.type = type;
		this.data = data;
	}
}

@variant(17)
export class REQ_DispatchEvent extends PubSubMessage {
	@field({ type: "string" })
	type: keyof PubSubEvents;

	@field({ type: Uint8Array })
	data: Uint8Array;

	constructor(type: keyof PubSubEvents, data: Uint8Array) {
		super();
		this.type = type;
		this.data = data;
	}
}

@variant(18)
export class RESP_DispatchEvent extends PubSubMessage {
	@field({ type: "bool" })
	value: boolean;
	constructor(value: boolean) {
		super();
		this.value = value;
	}
}

@variant(19)
export class REQ_GetPublicKey extends PubSubMessage {
	@field({ type: "string" })
	hash: string;

	constructor(hash: string) {
		super();
		this.hash = hash;
	}
}

@variant(20)
export class RESP_GetPublicKey extends PubSubMessage {
	@field({ type: option(PublicSignKey) })
	publicKey?: PublicSignKey;

	constructor(publicKey?: PublicSignKey) {
		super();
		this.publicKey = publicKey;
	}
}

export const createCustomEventFromType = (
	type: keyof PubSubEvents,
	data: Uint8Array,
) => {
	if (type === "data") {
		return new CustomEvent<DataEvent>("data", {
			detail: deserialize(data, DataEvent),
		});
	} else if (type === "publish") {
		return new CustomEvent<DataEvent>("publish", {
			detail: deserialize(data, PublishEvent),
		});
	} else if (type === "message") {
		return new CustomEvent<StreamMessage>("message", {
			detail: deserialize(data, StreamMessage),
		});
	} else if (type === "peer:session") {
		return new CustomEvent<PublicSignKey>("peer:session", {
			detail: deserialize(data, PublicSignKey),
		});
	} else if (type === "peer:reachable") {
		return new CustomEvent<PublicSignKey>("peer:reachable", {
			detail: deserialize(data, PublicSignKey),
		});
	} else if (type === "peer:unreachable") {
		return new CustomEvent<PublicSignKey>("peer:unreachable", {
			detail: deserialize(data, PublicSignKey),
		});
	} else if (type === "subscribe") {
		return new CustomEvent<SubscriptionEvent>("subscribe", {
			detail: deserialize(data, SubscriptionEvent),
		});
	} else if (type === "unsubscribe") {
		return new CustomEvent<UnsubcriptionEvent>("subscribe", {
			detail: deserialize(data, UnsubcriptionEvent),
		});
	} else throw new Error("Unsupported event type: " + String(type));
};
