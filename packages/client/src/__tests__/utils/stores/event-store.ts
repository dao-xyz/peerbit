import { Identity, JSON_ENCODING, Log } from "@dao-xyz/ipfs-log";
import { Entry } from "@dao-xyz/ipfs-log";
import { Address, IInitializationOptions, load } from "@dao-xyz/peerbit-dstore";
import { Store } from "@dao-xyz/peerbit-dstore"
import { EncryptionTemplateMaybeEncrypted } from '@dao-xyz/ipfs-log';
import { variant } from '@dao-xyz/borsh';
import { EncodingType } from "@dao-xyz/peerbit-dstore";
import { TestStore } from "./test-store";

// TODO: generalize the Iterator functions and spin to its own module
export interface Operation<T> {
    op: string
    key?: string
    value?: T
}
export class EventIndex<T> {
    _index: Log<Operation<T>>;
    constructor() {
        this._index = null as any;
    }

    get() {
        return this._index ? this._index.values : []
    }

    async updateIndex(oplog: Log<Operation<T>>, entries?: Entry<Operation<T>>[] | undefined) {
        this._index = oplog
    }
}

@variant(0)
export class EventStore<T> extends TestStore<Operation<T>> {

    _index: EventIndex<T>;

    constructor(properties: {
        name?: string
    }) {
        super({ ...properties, encoding: EncodingType.JSON })
        this._index = new EventIndex();
    }

    async init(ipfs: any, identity: Identity, options: IInitializationOptions<Operation<T>>) {
        return super.init(ipfs, identity, { ...options, onUpdate: this._index.updateIndex.bind(this._index) })
    }

    add(data: any, options?: {
        onProgressCallback?: (any: any) => void;
        pin?: boolean;
        reciever?: EncryptionTemplateMaybeEncrypted,
        nexts?: Entry<any>[]
    }) {
        return this._addOperation({
            op: 'ADD',
            value: data
        }, options)
    }

    get(hash: string) {
        return this.iterator({ gte: hash, limit: 1 }).collect()[0]
    }

    iterator(options?: any) {
        const messages = this._query(options)
        let currentIndex = 0
        let iterator = {
            [Symbol.iterator]() {
                return this
            },
            next() {
                let item: { value?: Entry<Operation<T>>, done: boolean } = { value: undefined, done: true }
                if (currentIndex < messages.length) {
                    item = { value: messages[currentIndex], done: false }
                    currentIndex++
                }
                return item
            },
            collect: () => messages
        }

        return iterator
    }

    _query(opts: any) {
        if (!opts) opts = {}

        const amount = opts.limit ? (opts.limit > -1 ? opts.limit : this._index.get().length) : 1 // Return 1 if no limit is provided
        const events = this._index.get().slice()
        let result: Entry<Operation<T>>[] = []

        if (opts.gt || opts.gte) {
            // Greater than case
            result = this._read(events, opts.gt ? opts.gt : opts.gte, amount, !!opts.gte)
        } else {
            // Lower than and lastN case, search latest first by reversing the sequence
            result = this._read(events.reverse(), opts.lt ? opts.lt : opts.lte, amount, opts.lte || !opts.lt).reverse()
        }

        if (opts.reverse) {
            result.reverse()
        }

        return result
    }

    _read(ops: Entry<Operation<T>>[], hash: string, amount: number, inclusive: boolean) {
        // Find the index of the gt/lt hash, or start from the beginning of the array if not found
        const index = ops.map((e) => e.hash).indexOf(hash)
        let startIndex = Math.max(index, 0)
        // If gte/lte is set, we include the given hash, if not, start from the next element
        startIndex += inclusive ? 0 : 1
        // Slice the array to its requested size
        const res = ops.slice(startIndex).slice(0, amount)
        return res
    }

    static async load<T>(ipfs: any, address: Address, options?: {
        timeout?: number;
    }): Promise<EventStore<T>> {
        const instance = await load<EventStore<T>>(ipfs, address, EventStore, options)
        if (instance instanceof EventStore === false) {
            throw new Error("Unexpected")
        };
        return instance as any as EventStore<T>
    }

}
