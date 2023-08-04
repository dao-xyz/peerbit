import { Encoding, TrimOptions } from "@peerbit/log";
import { Entry } from "@peerbit/log";
import { EncryptionTemplateMaybeEncrypted } from "@peerbit/log";
import { variant, field, option } from "@dao-xyz/borsh";
import { Program } from "@peerbit/program";
import { PublicSignKey, randomBytes } from "@peerbit/crypto";
import {
	AbsoluteReplicas,
	ReplicationLimitsOptions,
	SharedLog,
	SyncFilter,
} from "../../..";
import { Role } from "../../../role";
import { JSON_ENCODING } from "./encoding";

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

	async get() {
		return this._log ? this._log.log.values.toArray() : [];
	}
}

type Args<T> = {
	role?: Role;
	trim?: TrimOptions;
	replicas?: ReplicationLimitsOptions;
	sync?: SyncFilter;
	encoding?: Encoding<Operation<T>>;
	respondToIHaveTimeout?: number;
	canReplicate?: (publicKey: PublicSignKey) => Promise<boolean> | boolean;
};
@variant("event_store")
export class EventStore<T> extends Program<Args<T>> {
	@field({ type: SharedLog })
	log: SharedLog<Operation<T>>;

	@field({ type: Uint8Array })
	id: Uint8Array;

	_index: EventIndex<T>;

	constructor(properties?: { id: Uint8Array }) {
		super();
		this.id = properties?.id || randomBytes(32);
		this.log = new SharedLog();
	}

	async open(properties?: Args<T>) {
		this._index = new EventIndex(this.log);
		await this.log.open({
			onChange: () => undefined,
			canAppend: () => Promise.resolve(true),
			canReplicate: properties?.canReplicate,
			role: properties?.role,
			trim: properties?.trim,
			replicas: properties?.replicas,
			sync: properties?.sync,
			encoding: properties?.encoding || JSON_ENCODING,
			respondToIHaveTimeout: properties?.respondToIHaveTimeout,
		});
	}

	add(
		data: T,
		options?: {
			pin?: boolean;
			receiver?: EncryptionTemplateMaybeEncrypted;
			meta?: {
				next?: Entry<any>[];
			};
			replicas?: AbsoluteReplicas;
		}
	) {
		return this.log.append(
			{
				op: "ADD",
				value: data,
			},
			options
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
				!!opts.gte
			);
		} else {
			// Lower than and lastN case, search latest first by reversing the sequence
			result = this._read(
				events.reverse(),
				opts.lt ? opts.lt : opts.lte,
				amount,
				opts.lte || !opts.lt
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
		inclusive: boolean
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
