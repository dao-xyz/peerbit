import { type Constructor, getSchema, variant } from "@dao-xyz/borsh";
import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	type PeerId as Libp2pPeerId,
	TypedEventEmitter,
	type TypedEventTarget,
} from "@libp2p/interface";
import { type Blocks, calculateRawCid } from "@peerbit/blocks-interface";
import { PublicSignKey, getPublicKeyFromPeerId } from "@peerbit/crypto";
import {
	SubscriptionEvent,
	UnsubcriptionEvent,
} from "@peerbit/pubsub-interface";
import { TimeoutError } from "@peerbit/time";
import { type Block } from "multiformats/block";
import { type Address } from "./address.js";
import { type Client } from "./client.js";
import {
	type EventOptions,
	Handler,
	type Manageable,
	type ProgramInitializationOptions,
	addParent,
} from "./handler.js";
import { getValuesWithType } from "./utils.js";

export class ClosedError extends Error {
	constructor(
		message: string = "Not open. Please invoke 'client.open(...)' before calling this method",
	) {
		super(message);
	}
}

export class MissingAddressError extends Error {
	constructor() {
		super(
			"Address does not exist, please open or save this program once to obtain it",
		);
	}
}

const intersection = (
	a: Map<string, PublicSignKey> | undefined,
	b: Map<string, PublicSignKey> | PublicSignKey[],
) => {
	const newSet = new Map<string, PublicSignKey>();

	if (Array.isArray(b)) {
		for (const el of b) {
			if (!a || a.has(el.hashcode())) {
				newSet.set(el.hashcode(), el);
			}
		}
	} else {
		for (const [key, el] of b) {
			if (!a || a.has(key)) {
				newSet.set(key, el);
			}
		}
	}
	return newSet;
};

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

const getAllParentAddresses = (p: Program): string[] => {
	return getAllParent(p, [])
		.filter((x) => x instanceof Program)
		.map((x) => (x as Program).address);
};

const getAllParent = (a: Program, arr: Program[] = [], includeThis = false) => {
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

export type ProgramClient = Client<Program>;
class ProgramHandler extends Handler<Program> {
	constructor(properties: { client: ProgramClient }) {
		super({
			identity: properties.client.identity,
			client: properties.client,
			shouldMonitor: (p) => p instanceof Program,
			load: Program.load,
		});
	}
}
export { ProgramHandler };

type ExtractArgs<T> = T extends Program<infer Args> ? Args : never;

@variant(0)
export abstract class Program<
	Args = any,
	Events extends ProgramEvents = ProgramEvents,
> implements Manageable<Args>
{
	private _node: ProgramClient;
	private _allPrograms: Program[] | undefined;

	private _events: TypedEventTarget<ProgramEvents>;
	private _closed: boolean;

	parents: (Program<any> | undefined)[];
	children: Program<Args>[];

	private _address?: Address;

	get address(): Address {
		if (!this._address) {
			throw new MissingAddressError();
		}
		return this._address;
	}

	set address(address: Address) {
		this._address = address;
	}

	get isRoot() {
		return this.parents == null || this.parents.filter((x) => !!x).length === 0;
	}

	get rootAddress(): Address {
		let root: Program = this;
		while (root.parents && root.parents.length > 0) {
			if (root.parents.length > 1) {
				throw new Error("Multiple parents not supported");
			}
			if (root.isRoot) {
				return root.address;
			}
			root = root.parents[0] as Program;
		}
		return root.address;
	}

	async calculateAddress(options?: {
		reset?: boolean;
	}): Promise<{ address: string; block?: Block<any, any, any, any> }> {
		if (this._address && !options?.reset) {
			return { address: this._address, block: undefined };
		}
		const out = await calculateRawCid(serialize(this));
		this._address = out.cid;
		return {
			address: out.cid,
			block: out.block,
		};
	}

	get events(): TypedEventTarget<Events> {
		return this._events || (this._events = new TypedEventEmitter<Events>());
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
		options?: ProgramInitializationOptions<Args, this>,
	) {
		// check that a  discriminator exist
		const schema = getSchema(this.constructor);
		if (!schema || typeof schema.variant !== "string") {
			throw new Error(
				`Expecting class to be decorated with a string variant. Example:\n'import { variant } "@dao-xyz/borsh"\n@variant("example-db")\nclass ${this.constructor.name} { ...`,
			);
		}

		// only store the root program, or programs that have been opened with a parent refernece ("loose programs")
		// TODO do we need addresses for subprograms? if so we also need to call this
		await this.calculateAddress();
		// await this.save(node.services.blocks, { skipOnAddress: true, save: () => !options?.parent || this.isRoot });

		// TODO is this check needed?
		if (getAllParentAddresses(this as Program).includes(this.address)) {
			throw new Error(
				"Subprogram has same address as some parent program. This is not currently supported",
			);
		}

		if (!this.closed) {
			addParent(this, options?.parent);
			return;
		} else {
			addParent(this, options?.parent);
		}

		this._eventOptions = options;
		this.node = node;
		const nexts = this.programs;
		for (const next of nexts) {
			await next.beforeOpen(node, { ...options, parent: this });
		}

		await this._eventOptions?.onBeforeOpen?.(this);
		this.closed = false;
	}

	async afterOpen() {
		if (!this.closed) {
			await this.node.services.pubsub.addEventListener(
				"subscribe",
				this._subscriptionEventListener ||
					(this._subscriptionEventListener = (s) =>
						!this.closed && this._emitJoinNetworkEvents(s.detail)),
			);
			await this.node.services.pubsub.addEventListener(
				"unsubscribe",
				this._unsubscriptionEventListener ||
					(this._unsubscriptionEventListener = (s) =>
						!this.closed && this._emitLeaveNetworkEvents(s.detail)),
			);

			this.emitEvent(new CustomEvent("open", { detail: this }), true);
			await this._eventOptions?.onOpen?.(this);
			const nexts = this.programs;
			for (const next of nexts) {
				await next.afterOpen();
			}
		}
	}

	abstract open(args?: Args): Promise<void>;

	private _clear() {
		this._allPrograms = undefined;
	}
	private _emittedEventsFor: Set<string> | undefined;
	private get emittedEventsFor(): Set<string> {
		return (this._emittedEventsFor = this._emittedEventsFor || new Set());
	}
	private async _emitJoinNetworkEvents(s: SubscriptionEvent) {
		if (this.emittedEventsFor.has(s.from.hashcode())) {
			return;
		}

		const allTopics = this.programs
			// TODO test this code path closed true/false
			.map((x) => x.closed === false && x.getTopics?.())
			.filter((x) => x)
			.flat() as string[];

		// if subscribing to all topics, emit "join" event
		for (const topic of allTopics) {
			if (
				!(await this.node.services.pubsub.getSubscribers(topic))?.find((x) =>
					s.from.equals(x),
				)
			) {
				return;
			}
		}
		if (this.emittedEventsFor.has(s.from.hashcode())) {
			return;
		}
		this.emittedEventsFor.add(s.from.hashcode());
		this.events.dispatchEvent(new CustomEvent("join", { detail: s.from }));
	}

	private async _emitLeaveNetworkEvents(s: UnsubcriptionEvent) {
		if (!this.emittedEventsFor.has(s.from.hashcode())) {
			return;
		}
		const allTopics = this.programs
			// TODO test this code path closed true/false
			.map((x) => x.closed === false && x.getTopics?.())
			.filter((x) => x)
			.flat() as string[];

		// if subscribing not subscribing to any topics, emit "leave" event
		for (const topic of allTopics) {
			if (
				(await this.node.services.pubsub.getSubscribers(topic))?.find((x) =>
					s.from.equals(x),
				)
			) {
				return;
			}
		}
		if (!this.emittedEventsFor.has(s.from.hashcode())) {
			return;
		}
		this.emittedEventsFor.delete(s.from.hashcode());
		this.events.dispatchEvent(new CustomEvent("leave", { detail: s.from }));
	}

	private _subscriptionEventListener: (
		e: CustomEvent<SubscriptionEvent>,
	) => void;
	private _unsubscriptionEventListener: (
		e: CustomEvent<UnsubcriptionEvent>,
	) => void;

	private async processEnd(type: "drop" | "close") {
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
					if (program.closed) {
						if (type === "close") {
							continue;
						}
					}
					promises.push(program[type](this as Program)); // TODO types
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

	private async end(type: "drop" | "close", from?: Program): Promise<boolean> {
		if (this.closed) {
			if (type === "drop") {
				throw new ClosedError("Program is closed, can not drop");
			}
			return true;
		}

		let parentIdx = -1;
		let close = true;
		if (this.parents) {
			parentIdx = this.parents.findIndex((x) => x === from);
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

		const end = close && (await this.processEnd(type));
		if (end) {
			this.node?.services.pubsub.removeEventListener(
				"subscribe",
				this._subscriptionEventListener,
			);
			this.node?.services.pubsub.removeEventListener(
				"unsubscribe",
				this._unsubscriptionEventListener,
			);

			this._eventOptions = undefined;

			if (parentIdx !== -1) {
				this.parents.splice(parentIdx, 1); // We splice this here because this._end depends on this parent to exist
			}
		}
		return end;
	}
	async close(from?: Program): Promise<boolean> {
		this._emittedEventsFor = undefined;
		return this.end("close", from);
	}

	async drop(from?: Program): Promise<boolean> {
		const dropped = await this.end("drop", from);
		if (dropped) {
			await this.delete();
		}
		return dropped;
	}

	emitEvent(event: CustomEvent, parents = false) {
		this.events.dispatchEvent(event);
		if (parents) {
			if (this.parents) {
				for (const parent of this.parents) {
					parent?.emitEvent(event, parents);
				}
			}
		}
	}

	/**
	 * Wait for another peer to be 'ready' to talk with you for this particular program
	 * @param other
	 * @throws TimeoutError if the timeout is reached
	 */
	async waitFor(
		other:
			| PublicSignKey
			| Libp2pPeerId
			| string
			| (PublicSignKey | string | Libp2pPeerId)[],
		options?: { signal?: AbortSignal; timeout?: number },
	): Promise<void> {
		const expectedHashes = new Set(
			(Array.isArray(other) ? other : [other])
				.map((x) =>
					typeof x === "string"
						? x
						: x instanceof PublicSignKey
							? x.hashcode()
							: getPublicKeyFromPeerId(x).hashcode(),
				)
				.filter((x) => x !== this.node.identity.publicKey.hashcode()),
		);

		// make sure nodes are reachable
		await Promise.all(
			[...expectedHashes].map((x) =>
				this.node.services.pubsub.waitFor(x, { signal: options?.signal }),
			),
		);

		// wait for subscribing to topics
		return new Promise<void>((resolve, reject) => {
			const timeout = setTimeout(
				() => {
					this.node.services.pubsub.removeEventListener("subscribe", listener);
					options?.signal?.removeEventListener("abort", abortListener);
					reject(new TimeoutError("Timeout waiting for replicating peer"));
				},
				options?.timeout || 10 * 1000,
			);

			const abortListener = (e: Event) => {
				this.node.services.pubsub.removeEventListener("subscribe", listener);
				clearTimeout(timeout);
				reject(new Error("Aborted"));
			};

			options?.signal?.addEventListener("abort", abortListener);

			const checkReady = async () => {
				let ready = true;
				const allReadyHashes = await this.getReady();
				for (const hash of expectedHashes) {
					if (!allReadyHashes.has(hash)) {
						ready = false;
						break;
					}
				}
				if (ready) {
					this.node.services.pubsub.removeEventListener("subscribe", listener);
					clearTimeout(timeout);
					options?.signal?.removeEventListener("abort", abortListener);
					resolve();
				}
			};
			const listener = () => {
				return checkReady();
			};
			this.node.services.pubsub.addEventListener("subscribe", listener);
			checkReady();
		});
	}

	async getReady(): Promise<Map<string, PublicSignKey>> {
		// all peers that subscribe to all topics
		let ready: Map<string, PublicSignKey> | undefined = undefined; // the interesection of all ready
		for (const program of this.allPrograms) {
			if (program.getTopics) {
				if (program.closed === false) {
					const topics = program.getTopics();
					for (const topic of topics) {
						const subscribers =
							await this.node.services.pubsub.getSubscribers(topic);
						if (!subscribers) {
							continue;
						}
						ready = intersection(ready, subscribers);
					}
				}
			}
		}
		if (ready == null) {
			return new Map();
		}
		return ready;
	}

	get allPrograms(): Program[] {
		if (this._allPrograms) {
			return this._allPrograms;
		}
		const arr: Program[] = this.programs;
		const nexts = this.programs;
		for (const next of nexts) {
			arr.push(...next.allPrograms);
		}
		this._allPrograms = arr;
		return this._allPrograms;
	}

	get programs(): Program[] {
		return getValuesWithType(this, Program);
	}

	clone(): this {
		return deserialize(serialize(this), this.constructor);
	}

	getTopics?(): string[];

	async save(
		store: Blocks = this.node.services.blocks,
		options?: {
			reset?: boolean;
			skipOnAddress?: boolean;
			save?: (address: string) => boolean | Promise<boolean>;
		},
	): Promise<Address> {
		const existingAddress = this._address;
		if (existingAddress) {
			if (options?.skipOnAddress) {
				return existingAddress;
			}
		}

		// always reset the address on save
		const toPut = await this.calculateAddress({ reset: true }); // this will also set the address (this.address)

		if (
			!options?.reset &&
			existingAddress &&
			existingAddress !== this.address
		) {
			this._address = existingAddress;
			throw new Error(
				"Program properties has been changed after constructor so that the hash has changed. Make sure that the 'setup(...)' function does not modify any properties that are to be serialized",
			);
		}

		if (
			(options?.save && !(await options.save(toPut.address))) ||
			(await store.has(toPut.address))
		) {
			return this.address!;
		}

		await store.put(
			toPut.block
				? { cid: toPut.address, block: toPut.block }
				: serialize(this),
		);

		if (options?.reset) {
			// delete the old address if it exists and different from the new one
			if (existingAddress && existingAddress !== this.address) {
				await store.rm(existingAddress);
			}
		}

		return this.address!;
	}

	async delete(): Promise<void> {
		if (this._address) {
			return this.node.services.blocks.rm(this.address);
		}
		// Not saved
	}

	static async load<P extends Program<any>>(
		address: Address,
		store: Blocks,
		options?: {
			timeout?: number;
		},
	): Promise<P | undefined> {
		const bytes = await store.get(address, {
			remote: {
				timeout: options?.timeout,
			},
		});
		if (!bytes) {
			return undefined;
		}
		const der = deserialize(bytes, Program);
		der.address = address;
		return der as P;
	}

	static async open<T extends Program<ExtractArgs<T>>>(
		this: Constructor<T>,
		address: string,
		node: ProgramClient,
		options?: ProgramInitializationOptions<ExtractArgs<T>, T>,
	): Promise<T> {
		const p = await Program.load<T>(address, node.services.blocks);

		if (!p) {
			throw new Error("Failed to load program");
		}
		await node.open<any>(p, options as any); // TODO fix types
		return p as T;
	}
}

export const getProgramFromVariants = <
	T extends Program,
>(): Constructor<T>[] => {
	const deps = (Program.prototype as any)[1000]; /// TODO improve BORSH lib to provide all necessary utility methods
	return (deps || []) as Constructor<T>[];
};

export const getProgramFromVariant = <T extends Program>(
	variant: string,
): Constructor<T> | undefined => {
	return getProgramFromVariants().filter(
		(x) => getSchema(x).variant === variant,
	)[0] as Constructor<T>;
};
