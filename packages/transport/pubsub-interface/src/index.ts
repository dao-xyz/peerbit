import { PublicSignKey } from "@peerbit/crypto";
import { PubSubData, Subscription } from "./messages.js";
import {
	Message,
	DataMessage,
	WaitForPeer,
	PeerEvents,
	DeliveryMode
} from "@peerbit/stream-interface";
import { EventHandler } from "@libp2p/interface/events";
import { PeerId as Libp2pPeerId } from "@libp2p/interface/peer-id";
import { field, option, vec } from "@dao-xyz/borsh";

export class SubscriptionEvent {
	@field({ type: PublicSignKey })
	from: PublicSignKey;

	@field({ type: vec(Subscription) })
	subscriptions: Subscription[];

	constructor(from: PublicSignKey, subscriptions: Subscription[]) {
		this.from = from;
		this.subscriptions = subscriptions;
	}
}

export class UnsubcriptionEvent {
	@field({ type: PublicSignKey })
	from: PublicSignKey;

	@field({ type: vec(Subscription) })
	unsubscriptions: Subscription[];

	constructor(from: PublicSignKey, unsubscriptions: Subscription[]) {
		this.from = from;
		this.unsubscriptions = unsubscriptions;
	}
}

export class DataEvent {
	@field({ type: PubSubData })
	data: PubSubData;

	@field({ type: DataMessage })
	message: DataMessage;
	constructor(data: PubSubData, message: DataMessage) {
		this.data = data;
		this.message = message;
	}
}

export class SubscriptionData {
	@field({ type: PublicSignKey })
	publicKey: PublicSignKey;

	@field({ type: "u64" })
	timestamp: bigint;

	constructor(properties: { publicKey: PublicSignKey; timestamp: bigint }) {
		this.publicKey = properties.publicKey;
		this.timestamp = properties.timestamp;
	}
}

export interface PubSubEvents extends PeerEvents {
	data: CustomEvent<DataEvent>;
	subscribe: CustomEvent<SubscriptionEvent>;
	unsubscribe: CustomEvent<UnsubcriptionEvent>;
	message: CustomEvent<Message>;
}
export interface IEventEmitter<EventMap extends Record<string, any>> {
	addEventListener<K extends keyof EventMap>(
		type: K,
		listener: EventHandler<EventMap[K]> | null,
		options?: boolean | AddEventListenerOptions
	): MaybePromise<void>;
	removeEventListener<K extends keyof EventMap>(
		type: K,
		listener?: EventHandler<EventMap[K]> | null,
		options?: boolean | EventListenerOptions
	): MaybePromise<void>;
	dispatchEvent(event: Event): MaybePromise<boolean>;
}

type MaybePromise<T> = Promise<T> | T;
export type PublishOptions =
	| {
			topics?: string[];
			to?: (string | PublicSignKey | Libp2pPeerId)[];
			strict?: false;
			mode?: DeliveryMode | undefined;
	  }
	| {
			topics: string[];
			to: (string | PublicSignKey | Libp2pPeerId)[];
			strict: true;
			mode?: DeliveryMode | undefined;
	  };

export interface PubSub extends IEventEmitter<PubSubEvents>, WaitForPeer {
	emitSelf: boolean;

	getSubscribers(topic: string): MaybePromise<PublicSignKey[] | undefined>;

	requestSubscribers(topic: string, from?: PublicSignKey): MaybePromise<void>;

	subscribe(topic: string): MaybePromise<void>;

	unsubscribe(
		topic: string,
		options?: {
			force?: boolean;
			data?: Uint8Array;
		}
	): MaybePromise<boolean>;

	publish(data: Uint8Array, options?: PublishOptions): MaybePromise<Uint8Array>;
}

export * from "./messages.js";
