import { PublicSignKey } from "@peerbit/crypto";
import { PubSubData, Subscription } from "./messages";
import {
	Message,
	DataMessage,
	WaitForPeer,
	PeerEvents,
} from "@peerbit/stream-interface";
import { EventHandler } from "@libp2p/interfaces/events";
import { PeerId as Libp2pPeerId } from "@libp2p/interface-peer-id";

interface From {
	from: PublicSignKey;
}

export interface SubscriptionEvent extends From {
	subscriptions: Subscription[];
}

export interface UnsubcriptionEvent extends From {
	unsubscriptions: Subscription[];
}

export interface PubSubEvents extends PeerEvents {
	data: CustomEvent<{ data: PubSubData; message: DataMessage }>;
	subscribe: CustomEvent<SubscriptionEvent>;
	unsubscribe: CustomEvent<UnsubcriptionEvent>;
	message: CustomEvent<Message>;
}
export interface IEventEmitter<EventMap extends Record<string, any>> {
	addEventListener<K extends keyof EventMap>(
		type: K,
		listener: EventHandler<EventMap[K]> | null,
		options?: boolean | AddEventListenerOptions
	): void;
	removeEventListener<K extends keyof EventMap>(
		type: K,
		listener?: EventHandler<EventMap[K]> | null,
		options?: boolean | EventListenerOptions
	): void;
	dispatchEvent(event: Event): boolean;
}

export type PublishOptions =
	| {
			topics?: string[];
			to?: (string | PublicSignKey | Libp2pPeerId)[];
			strict?: false;
	  }
	| {
			topics: string[];
			to: (string | PublicSignKey | Libp2pPeerId)[];
			strict: true;
	  };

export interface PubSub extends IEventEmitter<PubSubEvents>, WaitForPeer {
	getSubscribers(topic: string):
		| Map<
				string,
				{
					timestamp: bigint;
					data?: Uint8Array;
				}
		  >
		| undefined;

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
			force: boolean;
			data: Uint8Array;
		}
	): Promise<boolean>;

	publish(data: Uint8Array, options?: PublishOptions): Promise<Uint8Array>;
}

export * from "./messages.js";
