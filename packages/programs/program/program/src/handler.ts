import { type Blocks } from "@peerbit/blocks-interface";
import type { Identity } from "@peerbit/crypto";
import { logger as loggerFn } from "@peerbit/logger";
import PQueue from "p-queue";
import type { Address } from "./address.js";

export const logger = loggerFn("peerbit:program:handler");

type ProgramMergeStrategy = "replace" | "reject" | "reuse";
export type ExtractArgs<T> = T extends CanOpen<infer Args> ? Args : never;

export type EventOptions = {
	onBeforeOpen?: (program: Manageable<any>) => Promise<void> | void;
	onOpen?: (program: Manageable<any>) => Promise<void> | void;
	onDrop?: (program: Manageable<any>) => Promise<void> | void;
	onClose?: (program: Manageable<any>) => Promise<void> | void;
};

export type OpenOptions<T extends Manageable<ExtractArgs<T>>> = {
	timeout?: number;
	existing?: ProgramMergeStrategy;
	mode?: "auto" | "local" | "canonical";
} & ProgramInitializationOptions<ExtractArgs<T>, T>;

export type WithArgs<Args> = { args?: Args };
export type WithParent<T> = { parent?: T };
export type Closeable = { closed: boolean; close(): Promise<boolean> };
type WithNode = { node: { identity: Identity } };
export type Addressable = {
	address: Address;
};
export interface Saveable {
	save(
		store: Blocks,
		options?: {
			format?: string;
			timeout?: number;
			skipOnAddress?: boolean;
			condition?: (address: string) => boolean | Promise<boolean>;
		},
	): Promise<Address>;

	delete(): Promise<void>;
}
export type CanEmit = { emitEvent: (event: CustomEvent) => void };

export type CanOpen<Args> = {
	beforeOpen: (client: any, options?: EventOptions) => Promise<void>;
	open(args?: Args): Promise<void>;
	afterOpen: () => Promise<void>;
};

export type Manageable<Args> = Closeable &
	Addressable &
	CanOpen<Args> &
	Saveable &
	CanEmit & {
		children: Manageable<any>[];
		parents: (Manageable<any> | undefined)[];
	} & WithNode;

export type ProgramInitializationOptions<Args, T extends Manageable<Args>> = {
	// TODO
	// reset: boolean
} & WithArgs<Args> &
	WithParent<T> &
	EventOptions;

export const addParent = (child: Manageable<any>, parent?: Manageable<any>) => {
	if (child.parents && child.parents.includes(parent) && parent == null) {
		return; // prevent root parents to exist multiple times. This will allow use to close a program onces even if it is reused multiple times
	}

	(child.parents || (child.parents = [])).push(parent);
	if (parent) {
		(parent.children || (parent.children = [])).push(child);
	}
};

export class Handler<T extends Manageable<any>> {
	items: Map<string, T>;
	private _openQueue: Map<string, PQueue>;
	private _openingPromises: Map<string, Promise<T>>;

	constructor(
		readonly properties: {
			client: { services: { blocks: Blocks }; stop: () => Promise<void> };
			load: (
				address: Address,
				blocks: Blocks,
				options?: { timeout?: number },
			) => Promise<T | undefined>;
			shouldMonitor: (thing: any) => boolean;
			identity: Identity;
		},
	) {
		this._openQueue = new Map();
		this._openingPromises = new Map();
		this.items = new Map();
	}

	async stop() {
		await Promise.all(
			[...this._openQueue.values()].map((x) => {
				x.clear();
				return x.onIdle();
			}),
		);
		this._openQueue.clear();

		// Wait for any in-progress opens to complete before closing
		// This prevents race conditions where a program is being opened while we close
		if (this._openingPromises.size > 0) {
			await Promise.allSettled([...this._openingPromises.values()]);
		}
		this._openingPromises.clear();

		// Close all open databases
		await Promise.all(
			[...this.items.values()].map((program) => program.close()),
		);

		// Remove all databases from the state
		this.items = new Map();
	}

	private _onProgamClose(program: Manageable<any>) {
		this.items.delete(program.address!.toString());

		// TODO remove item from this._openQueue?
	}

	private async checkProcessExisting<S extends T>(
		address: Address,
		toOpen: Manageable<any>,
		mergeSrategy: ProgramMergeStrategy = "reject",
	): Promise<S | undefined> {
		const prev = this.items.get(address);
		if (mergeSrategy === "reject") {
			if (prev) {
				throw new Error(`Program at ${address} is already open`);
			}
		} else if (mergeSrategy === "replace") {
			if (prev && prev !== toOpen) {
				await prev.close(); // clouse previous
			}
		} else if (mergeSrategy === "reuse") {
			return prev as S;
		}
	}

	async open<S extends T>(
		storeOrAddress: S | Address | string,
		options: OpenOptions<S> = {},
	): Promise<S> {
		const fn = async (): Promise<S> => {
			// TODO add locks for store lifecycle, e.g. what happens if we try to open and close a store at the same time?
			let program = storeOrAddress as S;
			if (typeof storeOrAddress === "string") {
				try {
					if (this.items?.has(storeOrAddress.toString())) {
						const existing = await this.checkProcessExisting(
							storeOrAddress.toString(),
							program,
							options?.existing,
						);
						if (existing) {
							return existing as S;
						}
					} else {
						program = (await this.properties.load(
							storeOrAddress,
							this.properties.client.services.blocks,
							options,
						)) as S; // TODO fix typings

						if (!this.properties.shouldMonitor(program)) {
							if (!program) {
								throw new Error(
									"Failed to resolve program with address: " + storeOrAddress,
								);
							}
							throw new Error(
								`Failed to open program because program is of type ${program?.constructor.name} `,
							);
						}
					}
				} catch (error) {
					logger.error(
						"Failed to load store with address: " + storeOrAddress.toString(),
					);
					throw error;
				}
			} else {
				if (options.parent === program) {
					throw new Error("Parent program can not be equal to the program");
				}

				if (!program.closed) {
					const existing = this.items.get(program.address);
					if (existing === program) {
						addParent(existing, options.parent);
						return program;
					} else if (existing) {
						// we got existing, but it is not the same instance
						const existing = await this.checkProcessExisting(
							program.address,
							program,
							options?.existing,
						);

						if (existing) {
							return existing as S;
						}
					} else {
						if (
							!program.node.identity.publicKey.equals(
								this.properties.identity.publicKey,
							)
						) {
							throw new Error(
								`Program at ${program.address} is already opened with a different client`,
							);
						}

						// assume new instance was not added to monitored items, just add it
						// and return it as we would opened it normally

						await program.save(this.properties.client.services.blocks, {
							skipOnAddress: false,
							condition: (address) => {
								return !this.items.has(address.toString());
							},
						});
						this.items.set(program.address, program);
						addParent(program, options.parent);
						return program;
					}
				}
			}

			logger.trace(`Open database '${program.constructor.name}`);

			// todo make this nicer
			let address = await program.save(this.properties.client.services.blocks, {
				skipOnAddress: true,
				condition: (address) => {
					return !this.items.has(address.toString());
				},
			});

			const existing = await this.checkProcessExisting(
				address,
				program,
				options?.existing,
			);
			if (existing) {
				return existing as S;
			}

			await program.beforeOpen(this.properties.client, {
				onBeforeOpen: (p) => {
					if (this.properties.shouldMonitor(program)) {
						this.items.set(address, program);
					}
				},
				onClose: (p) => {
					if (this.properties.shouldMonitor(p)) {
						return this._onProgamClose(p as T); // TODO types
					}
				},
				onDrop: (p) => {
					if (this.properties.shouldMonitor(p)) {
						return this._onProgamClose(p);
					}
				},
				...options,
				// If the program opens more programs
				// reset: options.reset,
			});
			await program.open(options.args);
			await program.afterOpen();
			return program as S;
		};

		// Helper to resolve address from storeOrAddress
		const resolveAddress = async (): Promise<string> => {
			if (typeof storeOrAddress === "string") {
				return storeOrAddress;
			}
			if (!storeOrAddress.closed) {
				return storeOrAddress.address;
			}
			return storeOrAddress.save(this.properties.client.services.blocks, {
				skipOnAddress: true,
				condition: (addr) => !this.items.has(addr.toString()),
			});
		};

		const address = await resolveAddress();

		// For parent opens, check if already opened or in-progress and return early
		// This prevents race conditions while avoiding deadlocks
		if (options?.parent) {
			// Check if there's an open in progress FIRST - wait for it
			// This must be checked before items because beforeOpen() adds to items
			// before open() completes
			const existingPromise = this._openingPromises.get(address);
			if (existingPromise) {
				const result = await existingPromise;
				addParent(result, options.parent);
				return result as S;
			}

			// Check if already fully opened
			const existing = this.items.get(address);
			if (existing) {
				addParent(existing, options.parent);
				return existing as S;
			}

			// Track this open and bypass queue (parent already holds queue)
			const openPromise = fn();
			this._openingPromises.set(address, openPromise as Promise<T>);
			try {
				return await openPromise;
			} finally {
				this._openingPromises.delete(address);
			}
		}

		// Non-parent opens use queue for serialization
		let queue = this._openQueue.get(address);
		if (!queue) {
			queue = new PQueue({ concurrency: 1 });
			this._openQueue.set(address, queue);
		}
		return queue.add(fn) as any as S;
	}
}
