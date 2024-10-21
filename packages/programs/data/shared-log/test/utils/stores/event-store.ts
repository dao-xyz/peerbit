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
import type { ReplicationDomainHash } from "../../../src/replication-domain-hash.js";
import type { ReplicationDomain } from "../../../src/replication-domain.js";
import { JSON_ENCODING } from "./encoding.js";

// TODO: generalize the Iterator functions and spin to its own module
export interface Operation<T> {
	op: string;
	key?: string;
	value?: T;
}

export class EventIndex<T> {
	_log: SharedLog<Operation<T>>;
	constructor(log: SharedLog<Operation<T>>) {
		this._log = log;
	}

	async get(): Promise<any> {
		return this._log ? this._log.log.toArray() : [];
	}
}

export type Args<T, D extends ReplicationDomain<any, Operation<T>>> = {
	onChange?: (change: Change<Operation<T>>) => void;
	replicate?: ReplicationOptions;
	trim?: TrimOptions;
	replicas?: ReplicationLimitsOptions;
	encoding?: Encoding<Operation<T>>;
	respondToIHaveTimeout?: number;
	timeUntilRoleMaturity?: number;
	waitForReplicatorTimeout?: number;
	sync?: (
		entry: Entry<Operation<T>> | ShallowEntry | EntryReplicated,
	) => boolean;
	canAppend?: CanAppend<Operation<T>>;
	canReplicate?: (publicKey: PublicSignKey) => Promise<boolean> | boolean;
	onMessage?: (msg: TransportMessage, context: RequestContext) => Promise<void>;
	compatibility?: number;
	domain?: D;
};
@variant("event_store")
export class EventStore<
	T,
	D extends ReplicationDomain<any, Operation<T>> = ReplicationDomainHash,
> extends Program<Args<T, D>> {
	@field({ type: SharedLog })
	log: SharedLog<Operation<T>, D>;

	@field({ type: Uint8Array })
	id: Uint8Array;

	_index!: EventIndex<T>;
	_canAppend?: CanAppend<Operation<T>>;

	constructor(properties?: { id: Uint8Array }) {
		super();
		this.id = properties?.id || randomBytes(32);
		this.log = new SharedLog({ id: this.id });
	}

	setCanAppend(canAppend: CanAppend<Operation<T>> | undefined) {
		this._canAppend = canAppend;
	}

	async open(properties?: Args<T, D>) {
		this._index = new EventIndex(this.log);

		if (properties?.onMessage) {
			this.log._onMessage = properties.onMessage;
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
			timeUntilRoleMaturity: properties?.timeUntilRoleMaturity ?? 1000,
			sync: properties?.sync,
			respondToIHaveTimeout: properties?.respondToIHaveTimeout,
			distributionDebounceTime: 50, // to make tests fast
			domain: properties?.domain,
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
