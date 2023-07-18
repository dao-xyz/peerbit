import { Blocks } from "@peerbit/blocks-interface";
import PQueue from "p-queue";
import { Address } from "./address.js";
import { logger as loggerFn } from "@peerbit/logger";

export const logger = loggerFn({ module: "program-handler" });

type ProgramMergeStrategy = "replace" | "reject" | "reuse";

export type EventOptions = {
	onBeforeOpen?: (program: Manageable<any>) => Promise<void> | void;
	onOpen?: (program: Manageable<any>) => Promise<void> | void;
	onDrop?: (program: Manageable<any>) => Promise<void> | void;
	onClose?: (program: Manageable<any>) => Promise<void> | void;
};

export type OpenOptions<Args, T extends Manageable<Args>> = {
	timeout?: number;
	existing?: ProgramMergeStrategy;
	/* 
    reset?: boolean; */
} & ProgramInitializationOptions<Args, T>;

export type WithArgs<Args> = { args?: Args };
export type WithParent<T> = { parent?: T };
export type Closeable = { closed: boolean; close(): Promise<boolean> };
export type Addressable = { address: Address };
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
	};

export type ProgramInitializationOptions<Args, T extends Manageable<Args>> = {
	// TODO
	// reset: boolean
} & WithArgs<Args> &
	WithParent<T> &
	EventOptions;

export class Handler<T extends Manageable<any>> {
	items: Map<string, Manageable<any>>;
	private _openQueue: PQueue;

	constructor(
		readonly properties: {
			client: { services: { blocks: Blocks }; stop: () => Promise<void> };
			load: (
				address: Address,
				blocks: Blocks,
				options?: { timeout?: number }
			) => Promise<T | undefined>;
			shouldMonitor: (thing: any) => boolean;
		}
	) {
		this._openQueue = new PQueue({ concurrency: 1 });
		this.items = new Map();
	}

	async stop() {
		this._openQueue.clear();
		await this._openQueue.onIdle();

		// Close all open databases
		await Promise.all(
			[...this.items.values()].map((program) => program.close())
		);

		// Remove all databases from the state
		this.items = new Map();
	}

	private _onProgamClose(program: Manageable<any>) {
		this.items.delete(program.address!.toString());
	}

	private async _onProgramOpen(
		program: Manageable<any>,
		mergeSrategy?: ProgramMergeStrategy
	) {
		const programAddress = program.address?.toString();
		if (!programAddress) {
			throw new Error("Missing program address");
		}
		if (this.items.has(programAddress)) {
			// second condition only makes this throw error if we are to add a new instance with the same address
			const existing = await this.checkProcessExisting(
				programAddress,
				program,
				mergeSrategy
			);
			if (!existing) {
				throw new Error("Unexpected");
			}
			this.items.set(programAddress, program);
		} else {
			this.items.set(programAddress, program);
		}
	}

	private async checkProcessExisting<S extends T>(
		address: Address,
		toOpen: Manageable<any>,
		mergeSrategy: ProgramMergeStrategy = "reject"
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

	async open<S extends T, Args = any>(
		storeOrAddress: S | Address | string,
		options: OpenOptions<Args, S> = {}
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
							options?.existing
						);
						if (existing) {
							return existing as S;
						}
					} else {
						program = (await this.properties.load(
							storeOrAddress,
							this.properties.client.services.blocks,
							options
						)) as S; // TODO fix typings
						if (!this.properties.shouldMonitor(program)) {
							throw new Error(
								`Failed to open program because program is of type ${program?.constructor.name} `
							);
						}
					}
				} catch (error) {
					logger.error(
						"Failed to load store with address: " + storeOrAddress.toString()
					);
					throw error;
				}
			} else if (!program.closed) {
				const existing = this.items.get(program.address);
				if (existing === program) {
					return program;
				} else if (existing) {
					const existing = await this.checkProcessExisting(
						program.address,
						program,
						options?.existing
					);

					if (existing) {
						return existing as S;
					}
				}
			}

			logger.debug(`Open database '${program.constructor.name}`);
			const address = await program.save(
				this.properties.client.services.blocks
			);
			const existing = await this.checkProcessExisting(
				address,
				program,
				options?.existing
			);
			if (existing) {
				return existing as S;
			}
			await program.beforeOpen(this.properties.client, {
				onBeforeOpen: async (p) => {
					if (
						this.properties.shouldMonitor(p) &&
						p.parents.length === 1 &&
						!p.parents[0]
					) {
						return this._onProgramOpen(p, options?.existing);
					}
				},
				onClose: (p) => {
					if (this.properties.shouldMonitor(p)) {
						return this._onProgamClose(p as Manageable<any>);
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

		// Prevent deadlocks when a program is opened by another program
		// TODO make proper deduplciation behaviour
		if (options?.parent) {
			return fn();
		}
		return this._openQueue.add(fn) as any as S; // TODO p-queue seem to return void type ;
	}
}
