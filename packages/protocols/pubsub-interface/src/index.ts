import { PublicSignKey, X25519Keypair, X25519PublicKey } from "@peerbit/crypto";
import { PubSubData, Subscription } from "./messages.js";
import {
	Message,
	DataMessage,
	WaitForPeer,
	PeerEvents,
} from "@peerbit/stream-interface";
import { EventHandler } from "@libp2p/interfaces/events";
import { PeerId as Libp2pPeerId } from "@libp2p/interface-peer-id";
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
	@field({ type: "u64" })
	timestamp: bigint;

	@field({
		type: option(Uint8Array),
	})
	data?: Uint8Array;

	constructor(properties: { timestamp: bigint; data?: Uint8Array }) {
		this.timestamp = properties.timestamp;
		this.data = properties.data;
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
export type PublishOptions = {
	topics?: string[];
	to?: (PublicSignKey | string)[];
	encrypt?: false;
	strict?: false;
}
	| {
		topics: string[];
		to: PublicSignKey[];
		keypair?: X25519Keypair,
		encrypt?: false;
		strict: true;
	} |
{
	topics: string[];
	to: (PublicSignKey | string)[];
	recievers?: X25519PublicKey[]
	keypair?: X25519Keypair,
	encrypt: true;
	strict: true;
}

export interface PubSub extends IEventEmitter<PubSubEvents>, WaitForPeer {
	emitSelf: boolean;

	getSubscribers(
		topic: string
	): MaybePromise<Map<string, SubscriptionData> | undefined>;

	requestSubscribers(topic: string, from?: PublicSignKey): Promise<void>;

	subscribe(
		topic: string,
		options?: {
			data?: Uint8Array;
		}
	): Promise<void>;

	unsubscribe(
		topic: string,
		options?: {
			force?: boolean;
			data?: Uint8Array;
		}
	): Promise<boolean>;

	publish(data: Uint8Array, options?: PublishOptions): Promise<Uint8Array>;
}

export * from "./messages.js";
