import { field, vec } from "@dao-xyz/borsh";
import { type EventHandler } from "@libp2p/interface";
import { PublicSignKey } from "@peerbit/crypto";
import {
	DataMessage,
	DeliveryMode,
	type IdOptions,
	Message,
	type PeerEvents,
	type PriorityOptions,
	type PublicKeyFromHashResolver,
	type WaitForPeer,
	type WithExtraSigners,
} from "@peerbit/stream-interface";
import { PubSubData } from "./messages.js";

export class SubscriptionEvent {
	@field({ type: PublicSignKey })
	from: PublicSignKey;

	@field({ type: vec("string") })
	topics: string[];

	constructor(from: PublicSignKey, topics: string[]) {
		this.from = from;
		this.topics = topics;
	}
}

export class UnsubcriptionEvent {
	@field({ type: PublicSignKey })
	from: PublicSignKey;

	@field({ type: vec("string") })
	topics: string[];

	constructor(from: PublicSignKey, topics: string[]) {
		this.from = from;
		this.topics = topics;
	}
}

export class PublishEvent {
	@field({ type: PubSubData })
	data: PubSubData;

	@field({ type: DataMessage })
	message: DataMessage;

	client?: string;

	constructor(properties: {
		client?: string;
		data: PubSubData;
		message: DataMessage;
	}) {
		this.client = properties.client;
		this.data = properties.data;
		this.message = properties.message;
	}
}

export class DataEvent {
	@field({ type: PubSubData })
	data: PubSubData;

	@field({ type: DataMessage })
	message: DataMessage;

	constructor(properties: { data: PubSubData; message: DataMessage }) {
		this.data = properties.data;
		this.message = properties.message;
	}
}

export class SubscriptionData {
	@field({ type: PublicSignKey })
	publicKey: PublicSignKey;

	@field({ type: "u64" })
	session: bigint;

	@field({ type: "u64" })
	timestamp: bigint;

	constructor(properties: {
		publicKey: PublicSignKey;
		session: bigint;
		timestamp: bigint;
	}) {
		this.publicKey = properties.publicKey;
		this.session = properties.session;
		this.timestamp = properties.timestamp;
	}
}

export interface PubSubEvents extends PeerEvents {
	publish: CustomEvent<DataEvent>;
	data: CustomEvent<DataEvent>;
	subscribe: CustomEvent<SubscriptionEvent>;
	unsubscribe: CustomEvent<UnsubcriptionEvent>;
	message: CustomEvent<Message>;
}
export interface IEventEmitter<EventMap extends Record<string, any>> {
	addEventListener<K extends keyof EventMap>(
		type: K,
		listener: EventHandler<EventMap[K]> | null,
		options?: boolean | AddEventListenerOptions,
	): MaybePromise<void>;
	removeEventListener<K extends keyof EventMap>(
		type: K,
		listener?: EventHandler<EventMap[K]> | null,
		options?: boolean | EventListenerOptions,
	): MaybePromise<void>;
	dispatchEvent(event: Event): MaybePromise<boolean>;
}

type MaybePromise<T> = Promise<T> | T;
export type PublishOptions = (
	| {
			topics?: string[];
			mode?: DeliveryMode | undefined;
	  }
	| {
			topics: string[];
			mode?: DeliveryMode | undefined;
	  }
) & { client?: string } & PriorityOptions &
	IdOptions &
	WithExtraSigners & {
		signal?: AbortSignal;
	};

export interface PubSub
	extends IEventEmitter<PubSubEvents>,
		WaitForPeer,
		PublicKeyFromHashResolver {
	getSubscribers(topic: string): MaybePromise<PublicSignKey[] | undefined>;

	requestSubscribers(topic: string, from?: PublicSignKey): MaybePromise<void>;

	subscribe(topic: string): MaybePromise<void>;

	unsubscribe(
		topic: string,
		options?: {
			force?: boolean;
			data?: Uint8Array;
		},
	): MaybePromise<boolean>;

	publish(
		data: Uint8Array,
		options?: PublishOptions,
	): MaybePromise<Uint8Array | undefined>;
}

export * from "./messages.js";
