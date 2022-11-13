import { JSON_ENCODING, Log } from "@dao-xyz/ipfs-log";
import { Entry } from "@dao-xyz/ipfs-log";
import { Store } from "@dao-xyz/peerbit-store";
import { EncryptionTemplateMaybeEncrypted } from "@dao-xyz/ipfs-log";
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
    _index: Log<Operation<T>>;
    constructor() {
        this._index = null as any;
    }

    get() {
        return this._index ? this._index.values : [];
    }

    async updateIndex(
        oplog: Log<Operation<T>>,
        entries?: Entry<Operation<T>>[] | undefined
    ) {
        this._index = oplog;
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
        this._index = new EventIndex();
    }

    async setup() {
        this.store.setup({
            onUpdate: this._index.updateIndex.bind(this._index),
            encoding,
            canAppend: () => Promise.resolve(true),
        });
    }

    add(
        data: T,
        options?: {
            onProgressCallback?: (any: any) => void;
            pin?: boolean;
            reciever?: EncryptionTemplateMaybeEncrypted;
            nexts?: Entry<any>[];
        }
    ) {
        return this.store._addOperation(
            {
                op: "ADD",
                value: data,
            },
            { ...options }
        );
    }

    get(hash: string) {
        return this.iterator({ gte: hash, limit: 1 }).collect()[0];
    }

    iterator(options?: any) {
        const messages = this._query(options);
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

    _query(opts: any) {
        if (!opts) opts = {};

        const amount = opts.limit
            ? opts.limit > -1
                ? opts.limit
                : this._index.get().length
            : 1; // Return 1 if no limit is provided
        const events = this._index.get().slice();
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
