import { v4 as uuid } from "uuid";
import { waitFor } from "@peerbit/time";

export interface Hello {
	type: "hello";
	from: string;
	to?: string;
	resp?: boolean;
}

export interface DataMessage {
	type: "data";
	from: string;
	to?: string;
	data: Uint8Array;
}
export interface Messages {
	hello: Hello;
	data: DataMessage;
}

export interface EventMessages {
	hello: Hello;
	data: DataMessage;
}

export type From = {
	publishMessage: (message: Hello | DataMessage) => void;
	id: string;
	parent?: boolean;
};
type MaybePromise<T> = T | Promise<T>;

export abstract class Node {
	id: string;
	out: Map<string, (message: Hello | DataMessage) => void>;
	parent?: string;

	constructor() {
		this.id = uuid();
		this.out = new Map();
	}

	abstract publishMessage(
		message: Hello | DataMessage,
		to?: string
	): MaybePromise<void>;
	abstract send(data: Uint8Array, to?: string): MaybePromise<void>;
	abstract subscribe<K extends keyof EventMessages>(
		type: K,
		fn: (message: EventMessages[K], from: From) => void
	): void;
	abstract unsubscribe<K extends keyof EventMessages>(
		type: K,
		fn: (
			message: EventMessages[K],
			from: From /* , from: ChannelFrom */
		) => void
	): void;

	get started(): boolean {
		return !!this._helloFn;
	}
	async connect(properties?: {
		to?: { id: string; parent?: boolean };
		waitForParent?: boolean;
	}) {
		if (!this.started) {
			await this.start();
		}

		if (properties?.to?.parent) {
			this.parent = properties.to.id;
		}

		this.publishMessage({
			type: "hello",
			from: this.id,
			to: properties?.to?.id,
		});

		try {
			if (properties?.to) await waitFor(() => this.out.has(properties!.to!.id));
			else {
				await waitFor(() => this.out.size > 0);
			}
		} catch (error) {
			if (properties!.to == null)
				throw new Error("Failed to connect to any remote");
			else {
				throw new Error("Failed to connect to: " + properties?.to?.id);
			}
		}
		if (properties?.waitForParent) {
			try {
				await waitFor(() => !!this.parent);
			} catch (error) {
				throw new Error("Parent was never found");
			}
		}
	}

	private _helloFn?: (hello: Hello, from: From) => void;
	start() {
		this._helloFn = (hello: Hello, from: From) => {
			if (hello.to && hello.to !== this.id) {
				return;
			}

			this.out.set(hello.from, from.publishMessage);
			if (from.parent) {
				this.parent = from.id;
			}

			if (!hello.resp) {
				from.publishMessage({
					type: "hello",
					from: this.id,
					to: hello.from,
					resp: true,
				});
			}
		};
		this.subscribe("hello", this._helloFn);
		return this;
	}

	stop() {
		this._helloFn && this.unsubscribe("hello", this._helloFn);
		this._helloFn = undefined;
	}
}

// say hello to sometone, they say hello back and we recieve the way wwe can send messages to them

export class MessageNode extends Node {
	constructor(
		readonly events: {
			dispatchEvent: (event: Hello | DataMessage) => void;
			addEventListener: <K extends keyof EventMessages>(
				type: K,
				fn: (message: EventMessages[K], from?: From) => void
			) => void;
		}
	) {
		super();
	}
	send(data: Uint8Array, to?: string | undefined) {
		if (to == null) {
			if (this.parent) {
				to = this.parent;
			} else if (this.out.size === 1) {
				to = this.out.keys().next().value;
			} else {
				throw new Error(
					"'to' is undefined there are more than one peer to send messages to"
				);
			}
		}

		const toStrict = to!; // TODO types

		const msg: DataMessage = {
			type: "data",
			data,
			from: this.id,
			to: typeof to === "string" ? to : toStrict,
		};

		const outFn = this.out.get(toStrict);
		if (outFn) {
			outFn(msg);
		} else {
			this.publishMessage(msg);
		}
	}

	publishMessage(message: Hello | DataMessage) {
		this.events.dispatchEvent(message);
	}

	subscribe<K extends keyof EventMessages>(
		type: K,
		fn: (message: EventMessages[K], from: From) => void
	) {
		this.events.addEventListener(type, (evt, from) => {
			if (evt.from === this.id) {
				return;
			}

			if (
				type === "data" &&
				evt.to != null &&
				evt.to !== this.id /* !this.out.has(evt.detail.from)) */
			) {
				return;
			}

			fn(evt, {
				id: evt.from,
				publishMessage:
					from?.publishMessage ||
					((message: Hello | DataMessage) => {
						this.events.dispatchEvent(message);
					}),
				parent: from?.parent,
			});
		});
	}
	unsubscribe<K extends keyof EventMessages>(
		type: K,
		fn: (
			message: EventMessages[K],
			from: From /* , from: ChannelFrom */
		) => void
	) {
		throw new Error("Not implemented");
	}
}
