import { JSON_ENCODING } from "@dao-xyz/peerbit-log";
import { Entry } from "@dao-xyz/peerbit-log";
import { Store } from "@dao-xyz/peerbit-store";
import { EncryptionTemplateMaybeEncrypted } from "@dao-xyz/peerbit-log";
import { variant, field } from "@dao-xyz/borsh";
import { Program } from "@dao-xyz/peerbit-program";

// TODO: generalize the Iterator functions and spin to its own module
export interface Operation<T> {
	op: string;
	key?: string;
	value?: T;
}

const encoding = JSON_ENCODING;

export class EventIndex<T> {
	_store: Store<Operation<T>>;
	constructor(store: Store<Operation<T>>) {
		this._store = store;
	}

	async get() {
		return this._store ? this._store.oplog.values.toArray() : [];
	}
}

@variant("eventstore")
export class EventStore<T> extends Program {
	_index: EventIndex<T>;

	@field({ type: Store })
	store: Store<Operation<T>>;

	constructor(properties?: { id?: string }) {
		super(properties);
		this.store = new Store();
	}

	async setup() {
		this._index = new EventIndex(this.store);
		this.store.setup({
			onUpdate: () => undefined,
			encoding,
			canAppend: () => Promise.resolve(true),
		});
	}

	add(
		data: T,
		options?: {
			pin?: boolean;
			reciever?: EncryptionTemplateMaybeEncrypted;
			nexts?: Entry<any>[];
		}
	) {
		return this.store.addOperation(
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
				: this.store.oplog.length
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
