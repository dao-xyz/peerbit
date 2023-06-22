import { PublicSignKey, getPublicKeyFromPeerId } from "@peerbit/crypto";
import { Constructor, getSchema, variant } from "@dao-xyz/borsh";
import { getValuesWithType } from "./utils.js";
import { serialize, deserialize } from "@dao-xyz/borsh";
import { EventEmitter } from "@libp2p/interfaces/events";
import { Peerbit } from "@peerbit/interface";
import { waitForAsync } from "@peerbit/time";
import { Blocks } from "@peerbit/blocks-interface";
import { PeerId as Libp2pPeerId } from "@libp2p/interface-peer-id";

export type Address = string;

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

export type OpenProgram = (program: Program<any>) => Promise<Program<any>>;

export interface NetworkEvents {
	connect: CustomEvent<PublicSignKey>;
	disconnect: CustomEvent<PublicSignKey>;
}

export type ProgramInitializationOptions = {
	parent?: AbstractProgram;
	onClose?: () => Promise<void> | void;
	onDrop?: () => Promise<void> | void;
	onSave?: (address: Address) => Promise<void> | void;
	open?: OpenProgram;
};

@variant(0)
export abstract class AbstractProgram<
	Events extends Record<string, any> = NetworkEvents
> {
	private _node: Peerbit;
	private _onClose?: () => Promise<void> | void;
	private _onDrop?: () => Promise<void> | void;
	private _allPrograms: AbstractProgram[] | undefined;

	subOpen?: (program: Program) => Promise<Program>;
	programsOpened: Program[];

	_events: EventEmitter<Events>;

	_closed: boolean;

	get events(): EventEmitter<Events> {
		if (!this._events) {
			throw new Error("Program not setup");
		}
		return this._events;
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
	get node(): Peerbit {
		return this._node;
	}

	set node(node: Peerbit) {
		this._node = node;
	}

	async open(
		node: Peerbit,
		options?: ProgramInitializationOptions
	): Promise<this> {
		if (!this.closed) {
			throw new Error("Already open");
		}

		this.closed = false;

		this.node = node;

		this._onClose = options?.onClose;
		this._onDrop = options?.onDrop;
		/* this._getReady = options.getReady;

		this._libp2p.services.pubsub.addEventListener('subscribe', (s) => {
			s.detail.subscriptions.map(x => this._emitNetworkEvents(s.detail.from, x.data))
		})

		this._libp2p.services.pubsub.addEventListener('unsubscribe', (s) => {
			s.detail.unsubscriptions.map(x => this._emitNetworkEvents(s.detail.from, x.data))
		})
 */
		this._events = new EventEmitter();
		if (options?.open) {
			this.programsOpened = [];
			this.subOpen = async (program) => {
				if (!program.openedByPrograms) {
					program.openedByPrograms = [];
				}
				program.openedByPrograms.push(this);
				this.programsOpened.push(program);

				const opened = await options.open!(program);
				return opened;
			};
		}

		const nexts = this.programs;
		for (const next of nexts) {
			await next.open(node, {
				parent: this,
				onClose: options?.onClose,
				onDrop: options?.onDrop,
				onSave: options?.onSave,
				open: options?.open,
			});
		}

		return this;
	}

	private _clear() {
		this._allPrograms = undefined;
	}

	_prevReady: Map<string, Set<string> | undefined>;
	/* 	private _emitNetworkEvents = async (from: PublicSignKey, role?: Uint8Array) => {
			let key = toBase64(serialize(role));
			const current = await this.getReady(role)
			const past = this._prevReady.get(key);
	
			this._prevReady.set(key, current)
		}
	 */
	private async _end(
		type: "drop" | "close",
		onEvent?: () => void | Promise<void>
	) {
		if (!this.closed) {
			await onEvent?.();
			const promises: Promise<void | boolean>[] = [];

			for (const program of this.programs.values()) {
				promises.push(program[type]());
			}
			if (this.programsOpened) {
				for (const program of this.programsOpened) {
					promises.push(program[type](this as AbstractProgram)); // TODO types
				}
				this.programsOpened = [];
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

	async close(): Promise<boolean> {
		return this._end("close", this._onClose);
	}

	async drop(): Promise<void> {
		await this._end("drop", this._onDrop);
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

	getTopics?(): string[];
}

export interface CanTrust {
	isTrusted(keyHash: string): Promise<boolean> | boolean;
}

const SETUP_CALLS_PROPERTY = "__SETUP_CALLS";
const SETUP_INNER_FUNCTION_PROPRTY = "__SETUP_INNER_FUNCTION";

@variant(0)
export abstract class Program<
		Events extends Record<string, any> = NetworkEvents
	>
	extends AbstractProgram<Events>
	implements Addressable, Saveable
{
	private _address?: Address;

	openedByPrograms: (AbstractProgram | undefined)[];

	constructor() {
		super();
		this.setupSetupFunctionCounter();
	}

	get address(): Address | undefined {
		return this._address;
	}

	set address(address: Address) {
		this._address = address;
	}

	/**
	 * Will be called before program open(...)
	 * This function can be used to connect different modules
	 */

	// Programs must implement 0 arg
	abstract setup(): Promise<void> | void;

	// Can also have args
	abstract setup(args: any): Promise<void> | void;

	protected setupSetupFunctionCounter() {
		if (this[SETUP_CALLS_PROPERTY] == null) {
			this[SETUP_CALLS_PROPERTY] = 0;
			const setup = this.setup.bind(this);
			this.setup = (arg?: any) => {
				// external invocations
				this[SETUP_CALLS_PROPERTY]++;
				return setup(arg);
			};
			this[SETUP_INNER_FUNCTION_PROPRTY] = (arg?: any) => {
				this[SETUP_CALLS_PROPERTY]++;
				if (this[SETUP_CALLS_PROPERTY] > 1) {
					return; // don't call setup more than once
				}
				return setup(arg);
			};
		}
		for (const p of getValuesWithType(this, Program)) {
			p.setupSetupFunctionCounter();
		}
	}
	async open<T extends this & Program<any>>(
		this: T,
		peerbit: Peerbit,
		options?: ProgramInitializationOptions & {
			setup?: (program: T) => Promise<void> | void;
		}
	): Promise<this> {
		if (!this.closed) {
			throw new Error("Already open");
		}

		this.node = peerbit;
		this.setupSetupFunctionCounter();

		// check that a  discriminator exist
		const schema = getSchema(this.constructor);
		if (!schema || typeof schema.variant !== "string") {
			throw new Error(
				`Expecting class to be decorated with a string variant. Example:\n\'import { variant } "@dao-xyz/borsh"\n@variant("example-db")\nclass ${this.constructor.name} { ...`
			);
		}

		(this.openedByPrograms || (this.openedByPrograms = [])).push(undefined);

		// TODO, determine whether setup should be called before or after save
		const address = await this.save(peerbit.services.blocks);
		await options?.onSave?.(address);

		// call setup before open, because "setup" is rather something we do to make everything ready for start
		if (options?.setup) {
			await options.setup(this);
		} else {
			await this[SETUP_INNER_FUNCTION_PROPRTY]();
		}

		await super.open(peerbit, options);
		return this;
	}

	static async open<T extends Program<any>>(
		this: Constructor<T>,
		address: Address,
		peerbit: Peerbit,
		options?: ProgramInitializationOptions & {
			setup?: (program: T) => Promise<void> | void;
		}
	): Promise<T> {
		const p = await Program.load(address, peerbit.services.blocks);

		if (!p) {
			throw new Error("Failed to load program");
		}
		await options?.setup?.(p as T);
		return (p as T).open(peerbit, options); // TODO fix types
	}
	async save(store: Blocks = this.node.services.blocks): Promise<Address> {
		/* 	await this.initializeIds(); */

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
		der.setupSetupFunctionCounter();
		return der as P;
	}

	async close(from?: AbstractProgram): Promise<boolean> {
		if (from && this.openedByPrograms) {
			const ix = this.openedByPrograms.findIndex((x) => x == from);
			if (ix !== -1) {
				this.openedByPrograms.splice(ix, 1);
				if (this.openedByPrograms.length !== 0) {
					return false; // don't close, because someone else depends on this
				}
				// else close!
			} else {
				return false;
			}
		}
		return super.close();
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
	Events extends Record<string, any> = NetworkEvents
> extends AbstractProgram<Events> {}
