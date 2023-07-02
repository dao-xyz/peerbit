import { PublicSignKey, getPublicKeyFromPeerId } from "@peerbit/crypto";
import { Constructor, getSchema, option, variant } from "@dao-xyz/borsh";
import { getValuesWithType } from "./utils.js";
import { serialize, deserialize } from "@dao-xyz/borsh";
import { CustomEvent, EventEmitter } from "@libp2p/interfaces/events";
import { Client } from "./node.js";
import { waitForAsync } from "@peerbit/time";
import { Blocks } from "@peerbit/blocks-interface";
import { PeerId as Libp2pPeerId } from "@libp2p/interface-peer-id";
import {
	SubscriptionEvent,
	UnsubcriptionEvent,
} from "@peerbit/pubsub-interface";
import { Address } from "./address.js";

export type { Address };

export interface Addressable {
	address?: Address | undefined;
}

const intersection = (
	a: Set<string> | undefined,
	b: Set<string> | IterableIterator<string>
) => {
	const newSet = new Set<string>();
	for (const el of b) {
		if (!a || a.has(el)) {
			newSet.add(el);
		}
	}
	return newSet;
};

export interface Saveable {
	save(
		store: Blocks,
		options?: {
			format?: string;
			timeout?: number;
		}
	): Promise<Address>;

	delete(): Promise<void>;
}

export type OpenProgram = (program: Program) => Promise<Program>;

export interface NetworkEvents {
	join: CustomEvent<PublicSignKey>;
	leave: CustomEvent<PublicSignKey>;
}

export interface LifeCycleEvents {
	drop: CustomEvent<Program>;
	open: CustomEvent<Program>;
	close: CustomEvent<Program>;
}

export interface ProgramEvents extends NetworkEvents, LifeCycleEvents {}

type EventOptions = {
	onBeforeOpen?: (program: AbstractProgram<any>) => Promise<void> | void;
	onOpen?: (program: AbstractProgram<any>) => Promise<void> | void;
	onDrop?: (program: AbstractProgram<any>) => Promise<void> | void;
	onClose?: (program: AbstractProgram<any>) => Promise<void> | void;
};

export type ProgramInitializationOptions<Args> = {
	// TODO
	// reset: boolean
	args?: Args;
	parent?: AbstractProgram;
} & EventOptions;

const getAllParentAddresses = (p: AbstractProgram): string[] => {
	return getAllParent(p, [])
		.filter((x) => x instanceof Program)
		.map((x) => (x as Program).address);
};

const getAllParent = (
	a: AbstractProgram,
	arr: AbstractProgram[] = [],
	includeThis = false
) => {
	includeThis && arr.push(a);
	if (a.parents) {
		for (const p of a.parents) {
			if (p) {
				getAllParent(p, arr, true);
			}
		}
	}
	return arr;
};

export type ProgramClient = Client<Program, AbstractProgram>;

@variant(0)
export abstract class AbstractProgram<
	Args = any,
	Events extends ProgramEvents = ProgramEvents
> {
	private _node: ProgramClient;
	private _allPrograms: AbstractProgram[] | undefined;

	private _events: EventEmitter<ProgramEvents>;
	private _closed: boolean;

	parents: (AbstractProgram | undefined)[];
	children: AbstractProgram[];

	addParent(program: AbstractProgram | undefined) {
		(this.parents || (this.parents = [])).push(program);
		if (program) {
			(program.children || (program.children = [])).push(this);
		}
	}

	get events(): EventEmitter<Events> {
		return this._events || (this._events = new EventEmitter());
	}

	get closed(): boolean {
		if (this._closed == null) {
			return true;
		}
		return this._closed;
	}
	set closed(closed: boolean) {
		this._closed = closed;
	}

	get node(): ProgramClient {
		return this._node;
	}

	set node(node: ProgramClient) {
		this._node = node;
	}

	private _eventOptions: EventOptions | undefined;

	async beforeOpen(
		node: ProgramClient,
		options?: ProgramInitializationOptions<Args>
	) {
		if (!this.closed) {
			this.addParent(options?.parent);
			return this;
		} else {
			this.addParent(options?.parent);
		}
		this._eventOptions = options;
		this.node = node;
		const nexts = this.programs;
		for (const next of nexts) {
			await next.beforeOpen(node, { ...options, parent: this });
		}

		this.node.services.pubsub.addEventListener(
			"subscribe",
			this._subscriptionEventListener ||
				(this._subscriptionEventListener = (s) =>
					!this.closed && this._emitJoinNetworkEvents(s.detail))
		);
		this.node.services.pubsub.addEventListener(
			"unsubscribe",
			this._unsubscriptionEventListener ||
				(this._unsubscriptionEventListener = (s) =>
					!this.closed && this._emitLeaveNetworkEvents(s.detail))
		);

		await this._eventOptions?.onBeforeOpen?.(this);
		return this;
	}

	async afterOpen() {
		this.emitEvent(new CustomEvent("open", { detail: this }), true);
		await this._eventOptions?.onOpen?.(this);
		this.closed = false;
		const nexts = this.programs;
		for (const next of nexts) {
			await next.afterOpen();
		}
		return this;
	}

	abstract open(args?: Args): Promise<void>;

	private _clear() {
		this._allPrograms = undefined;
	}

	private async _emitJoinNetworkEvents(s: SubscriptionEvent) {
		const allTopics = this.programs
			.map((x) => x.getTopics?.())
			.filter((x) => x)
			.flat() as string[];

		// if subscribing to all topics, emit "join" event
		for (const topic of allTopics) {
			if (
				!this.node.services.pubsub.getSubscribers(topic)?.has(s.from.hashcode())
			) {
				return;
			}
		}
		this.events.dispatchEvent(new CustomEvent("join", { detail: s.from }));
	}

	private async _emitLeaveNetworkEvents(s: UnsubcriptionEvent) {
		const allTopics = this.programs
			.map((x) => x.getTopics?.())
			.filter((x) => x)
			.flat() as string[];

		// if subscribing not subscribing to any topics, emit "leave" event
		for (const topic of allTopics) {
			if (
				this.node.services.pubsub.getSubscribers(topic)?.has(s.from.hashcode())
			) {
				return;
			}
		}
		this.events.dispatchEvent(new CustomEvent("leave", { detail: s.from }));
	}

	private _subscriptionEventListener: (
		e: CustomEvent<SubscriptionEvent>
	) => void;
	private _unsubscriptionEventListener: (
		e: CustomEvent<UnsubcriptionEvent>
	) => void;

	private async _end(type: "drop" | "close") {
		if (!this.closed) {
			this.emitEvent(new CustomEvent(type, { detail: this }), true);
			if (type === "close") {
				this._eventOptions?.onClose?.(this);
			} else if (type === "drop") {
				this._eventOptions?.onDrop?.(this);
			} else {
				throw new Error("Unsupported event type: " + type);
			}

			const promises: Promise<void | boolean>[] = [];

			if (this.children) {
				for (const program of this.children) {
					promises.push(program[type](this as AbstractProgram)); // TODO types
				}
				this.children = [];
			}
			await Promise.all(promises);

			this._clear();
			this.closed = true;
			return true;
		} else {
			this._clear();
			return true;
		}
	}

	async close(from?: AbstractProgram): Promise<boolean> {
		if (this.closed) {
			return true;
		}

		let parentIdx = -1;
		let close = true;
		if (this.parents) {
			parentIdx = this.parents.findIndex((x) => x == from);
			if (parentIdx !== -1) {
				if (this.parents.length === 1) {
					close = true;
				} else {
					this.parents.splice(parentIdx, 1);
					close = false;
				}
			} else if (from) {
				throw new Error("Could not find from in parents");
			}
		}

		const end = close && (await this._end("close"));
		if (end) {
			this.node?.services.pubsub.removeEventListener(
				"subscribe",
				this._subscriptionEventListener
			);
			this.node?.services.pubsub.removeEventListener(
				"unsubscribe",
				this._unsubscriptionEventListener
			);

			this._eventOptions = undefined;

			if (parentIdx !== -1) {
				this.parents.splice(parentIdx, 1); // We splice this here because this._end depends on this parent to exist
			}
		}

		return end;
	}

	async drop(): Promise<void> {
		await this._end("drop");
	}

	emitEvent(event: CustomEvent, parents = false) {
		this.events.dispatchEvent(event);
		if (parents) {
			if (this.parents) {
				for (const parent of this.parents) {
					parent?.emitEvent(event);
				}
			}
		}
	}

	/**
	 * Wait for another peer to be 'ready' to talk with you for this particular program
	 * @param other
	 */
	async waitFor(...other: (PublicSignKey | Libp2pPeerId)[]): Promise<void> {
		const expectedHashes = new Set(
			other.map((x) =>
				x instanceof PublicSignKey
					? x.hashcode()
					: getPublicKeyFromPeerId(x).hashcode()
			)
		);
		await waitForAsync(
			async () => {
				return (
					intersection(expectedHashes, await this.getReady()).size ===
					expectedHashes.size
				);
			},
			{ delayInterval: 200, timeout: 10 * 1000 }
		); // 200 ms delay since this is an expensive op. TODO, make event based instead
	}

	async getReady(): Promise<Set<string>> {
		// all peers that subscribe to all topics
		let ready: Set<string> | undefined = undefined; // the interesection of all ready
		for (const program of this.allPrograms) {
			if (program.getTopics) {
				const topics = program.getTopics();
				for (const topic of topics) {
					const subscribers = await this.node.services.pubsub.getSubscribers(
						topic
					);
					if (!subscribers) {
						throw new Error(
							"client is not subscriber to topic data, do not have any info about peer readiness"
						);
					}
					ready = intersection(ready, subscribers.keys());
				}
			}
		}
		if (ready == null) {
			throw new Error("Do not have any info about peer readiness");
		}
		return ready;
	}

	get allPrograms(): AbstractProgram[] {
		if (this._allPrograms) {
			return this._allPrograms;
		}
		const arr: AbstractProgram[] = this.programs;
		const nexts = this.programs;
		for (const next of nexts) {
			arr.push(...next.allPrograms);
		}
		this._allPrograms = arr;
		return this._allPrograms;
	}

	get programs(): AbstractProgram[] {
		return getValuesWithType(this, AbstractProgram);
	}

	clone(): this {
		return deserialize(serialize(this), this.constructor);
	}

	getTopics?(): string[];
}

export interface CanTrust {
	isTrusted(keyHash: string): Promise<boolean> | boolean;
}

@variant(0)
export abstract class Program<
		Args = any,
		Events extends ProgramEvents = ProgramEvents
	>
	extends AbstractProgram<Args, Events>
	implements Addressable, Saveable
{
	private _address?: Address;

	constructor() {
		super();
	}

	get address(): Address {
		if (!this._address) {
			throw new Error(
				"Address does not exist, please open or save this program once to obtain it"
			);
		}
		return this._address;
	}

	set address(address: Address) {
		this._address = address;
	}

	async beforeOpen(
		node: ProgramClient,
		options?: ProgramInitializationOptions<Args>
	) {
		// check that a  discriminator exist
		const schema = getSchema(this.constructor);
		if (!schema || typeof schema.variant !== "string") {
			throw new Error(
				`Expecting class to be decorated with a string variant. Example:\n\'import { variant } "@dao-xyz/borsh"\n@variant("example-db")\nclass ${this.constructor.name} { ...`
			);
		}

		await this.save(node.services.blocks);
		if (getAllParentAddresses(this as AbstractProgram).includes(this.address)) {
			throw new Error(
				"Subprogram has same address as some parent program. This is not currently supported"
			);
		}
		return super.beforeOpen(node, options);
	}

	static async open<T extends Program<Args>, Args = any>(
		this: Constructor<T>,
		address: Address,
		node: ProgramClient,
		options?: ProgramInitializationOptions<Args>
	): Promise<T> {
		const p = await Program.load<T>(address, node.services.blocks);

		if (!p) {
			throw new Error("Failed to load program");
		}
		await node.open(p, options);
		return p as T;
	}
	async save(store: Blocks = this.node.services.blocks): Promise<Address> {
		const existingAddress = this._address;
		const hash = await store.put(serialize(this));

		this._address = hash;
		if (!this.address) {
			throw new Error("Unexpected");
		}

		if (existingAddress && existingAddress !== this.address) {
			throw new Error(
				"Program properties has been changed after constructor so that the hash has changed. Make sure that the 'setup(...)' function does not modify any properties that are to be serialized"
			);
		}

		return this._address!;
	}

	async delete(): Promise<void> {
		if (this.address) {
			return this.node.services.blocks.rm(this.address);
		}
		// Not saved
	}

	static async load<P extends Program<any>>(
		address: Address,
		store: Blocks,
		options?: {
			timeout?: number;
		}
	): Promise<P | undefined> {
		const bytes = await store.get(address, options);
		if (!bytes) {
			return undefined;
		}
		const der = deserialize(bytes, Program);
		der.address = address;
		return der as P;
	}

	async drop(): Promise<void> {
		await super.drop();
		return this.delete();
	}
}

/**eve
 * Building block, but not something you use as a standalone
 */
@variant(1)
export abstract class ComposableProgram<
	Args = any,
	Events extends ProgramEvents = ProgramEvents
> extends AbstractProgram<Args, Events> {}
