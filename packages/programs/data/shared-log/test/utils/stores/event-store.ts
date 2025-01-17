import { field, variant } from "@dao-xyz/borsh";
import { PublicSignKey, randomBytes } from "@peerbit/crypto";
import type {
	CanAppend,
	Change,
	Encoding,
	EncryptionTemplateMaybeEncrypted,
	Entry,
	EntryType,
	ShallowEntry,
	TrimOptions,
} from "@peerbit/log";
import { Program } from "@peerbit/program";
import type { RequestContext } from "@peerbit/rpc";
import {
	AbsoluteReplicas,
	type ReplicationLimitsOptions,
	type ReplicationOptions,
	SharedLog,
} from "../../../src/index.js";
import type { TransportMessage } from "../../../src/message.js";
import type { EntryReplicated } from "../../../src/ranges.js";
import type {
	ReplicationDomain,
	ReplicationDomainConstructor,
} from "../../../src/replication-domain.js";
import type { SynchronizerConstructor } from "../../../src/sync/index.js";
import type { TestSetupConfig } from "../../utils.js";
import { JSON_ENCODING } from "./encoding.js";

// TODO: generalize the Iterator functions and spin to its own module
export interface Operation<T> {
	op: string;
	key?: string;
	value?: T;
}

export class EventIndex<T> {
	_log: SharedLog<Operation<T>, any>;
	constructor(log: SharedLog<Operation<T>, any>) {
		this._log = log;
	}

	async get(): Promise<any> {
		return this._log ? this._log.log.toArray() : [];
	}
}

export type Args<
	T,
	D extends ReplicationDomain<any, Operation<T>, R>,
	R extends "u32" | "u64" = D extends ReplicationDomain<any, T, infer I>
		? I
		: "u32",
> = {
	onChange?: (change: Change<Operation<T>>) => void;
	replicate?: ReplicationOptions<R>;
	trim?: TrimOptions;
	replicas?: ReplicationLimitsOptions;
	encoding?: Encoding<Operation<T>>;
	respondToIHaveTimeout?: number;
	timeUntilRoleMaturity?: number;
	waitForPruneDelay?: number;
	waitForReplicatorTimeout?: number;
	sync?: (
		entry: Entry<Operation<T>> | ShallowEntry | EntryReplicated<R>,
	) => boolean;
	canAppend?: CanAppend<Operation<T>>;
	canReplicate?: (publicKey: PublicSignKey) => Promise<boolean> | boolean;
	onMessage?: (msg: TransportMessage, context: RequestContext) => Promise<void>;
	compatibility?: number;
	setup?: TestSetupConfig<R>;
	domain?: ReplicationDomainConstructor<D>;
};
@variant("event_store")
export class EventStore<
	T,
	D extends ReplicationDomain<any, Operation<T>, R>,
	R extends "u32" | "u64" = D extends ReplicationDomain<any, T, infer I>
		? I
		: "u32",
> extends Program<Args<T, D, R>> {
	@field({ type: SharedLog })
	log: SharedLog<Operation<T>, D, R>;

	@field({ type: Uint8Array })
	id: Uint8Array;

	_index!: EventIndex<T>;
	_canAppend?: CanAppend<Operation<T>>;

	static staticArgs: Args<any, any, any> | undefined;

	constructor(properties?: { id: Uint8Array }) {
		super();
		this.id = properties?.id || randomBytes(32);
		this.log = new SharedLog({ id: this.id });
	}

	setCanAppend(canAppend: CanAppend<Operation<T>> | undefined) {
		this._canAppend = canAppend;
	}

	async open(properties?: Args<T, D, R>) {
		this._index = new EventIndex(this.log);

		if (properties?.onMessage) {
			this.log.onMessage = properties.onMessage;
		}

		if (properties?.domain && properties?.setup?.domain) {
			throw new Error("Cannot have both domain and setup.domain");
		}

		await this.log.open({
			compatibility: properties?.compatibility,
			onChange: properties?.onChange,
			canAppend: (entry) => {
				const a = this._canAppend ? this._canAppend(entry) : true;
				if (!a) {
					return false;
				}
				return properties?.canAppend ? properties.canAppend(entry) : true;
			},
			canReplicate: properties?.canReplicate,
			replicate: properties?.replicate,
			trim: properties?.trim,
			replicas: properties?.replicas,
			waitForReplicatorTimeout: properties?.waitForReplicatorTimeout,
			encoding: JSON_ENCODING,
			timeUntilRoleMaturity: properties?.timeUntilRoleMaturity ?? 3000,
			waitForPruneDelay: properties?.waitForPruneDelay ?? 300,
			sync: properties?.sync,
			respondToIHaveTimeout: properties?.respondToIHaveTimeout,
			distributionDebounceTime: 50, // to make tests fast
			domain: properties?.domain ?? properties?.setup?.domain,
			syncronizer: properties?.setup?.syncronizer as SynchronizerConstructor<R>,

			...(((this.constructor as typeof EventStore).staticArgs ?? {}) as any),
		});
	}

	add(
		data: T,
		options?: {
			pin?: boolean;
			receiver?: EncryptionTemplateMaybeEncrypted;
			meta?: {
				next?: Entry<any>[];
				gidSeed?: Uint8Array;
				type?: EntryType;
			};
			replicas?: AbsoluteReplicas;
			target?: "all" | "replicators";
			canAppend?: CanAppend<Operation<T>>;
			replicate?: boolean;
		},
	) {
		return this.log.append(
			{
				op: "ADD",
				value: data,
			},
			options,
		);
	}

	async get(hash: string) {
		return (await this.iterator({ gte: hash, limit: 1 })).collect()[0];
	}

	async iterator(options?: any) {
		const messages = await this._query(options);
		let currentIndex = 0;
		const iterator = {
			[Symbol.iterator]() {
				return this;
			},
			next() {
				let item: { value?: Entry<Operation<T>>; done: boolean } = {
					value: undefined,
					done: true,
				};
				if (currentIndex < messages.length) {
					item = { value: messages[currentIndex], done: false };
					currentIndex++;
				}
				return item;
			},
			collect: () => messages,
		};

		return iterator;
	}

	async _query(opts: any) {
		if (!opts) opts = {};

		const amount = opts.limit
			? opts.limit > -1
				? opts.limit
				: this.log.log.length
			: 1; // Return 1 if no limit is provided

		const events = (await this._index.get()).slice();
		let result: Entry<Operation<T>>[] = [];

		if (opts.gt || opts.gte) {
			// Greater than case
			result = this._read(
				events,
				opts.gt ? opts.gt : opts.gte,
				amount,
				!!opts.gte,
			);
		} else {
			// Lower than and lastN case, search latest first by reversing the sequence
			result = this._read(
				events.reverse(),
				opts.lt ? opts.lt : opts.lte,
				amount,
				opts.lte || !opts.lt,
			).reverse();
		}

		if (opts.reverse) {
			result.reverse();
		}

		return result;
	}

	_read(
		ops: Entry<Operation<T>>[],
		hash: string,
		amount: number,
		inclusive: boolean,
	) {
		// Find the index of the gt/lt hash, or start from the beginning of the array if not found
		const index = ops.map((e) => e.hash).indexOf(hash);
		let startIndex = Math.max(index, 0);
		// If gte/lte is set, we include the given hash, if not, start from the next element
		startIndex += inclusive ? 0 : 1;
		// Slice the array to its requested size
		const res = ops.slice(startIndex).slice(0, amount);
		return res;
	}
}
