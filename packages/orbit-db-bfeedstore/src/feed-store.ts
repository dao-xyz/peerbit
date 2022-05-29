import Store from 'orbit-db-store'
import { FeedIndex } from './feed-index'
import { IPFS as IPFSInstance } from 'ipfs';
import { Identity } from 'orbit-db-identity-provider';
import { Constructor, serialize } from '@dao-xyz/borsh';
import bs58 from 'bs58';

export const BINARY_FEED_STORE_TYPE = 'bfeedstore';
const defaultOptions = (options: IStoreOptions): any => {
  if (!options.Index) Object.assign(options, { Index: FeedIndex })
  return options;
}
export class BinaryFeedStore<T> extends Store<T, FeedIndex<T>> {

  _type: string = undefined;
  constructor(ipfs: IPFSInstance, id: Identity, dbname: string, options: IStoreOptions & { indexBy?: string, clazz: Constructor<T> }) {
    super(ipfs, id, dbname, defaultOptions(options))
    this._type = BINARY_FEED_STORE_TYPE;
    this._index.init(this.options.clazz);

  }

  remove(hash: string, options = {}) {
    return this.del(hash, options)
  }


  add(data: T, options = {}) {
    return this._addOperation({
      op: 'ADD',
      key: null,
      value: bs58.encode(serialize(data)),
    }, options)
  }

  get(hash: string): T {
    return this.iterator({ gte: hash, limit: 1 }).collect()[0]
  }

  iterator(options) {
    const messages = this._query(options)
    let currentIndex = 0
    let iterator = {
      [Symbol.iterator]() {
        return this
      },
      next() {
        let item = { value: null, done: true }
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

  _query(opts) {
    if (!opts) opts = {}

    const amount = opts.limit ? (opts.limit > -1 ? opts.limit : this._index.get().length) : 1 // Return 1 if no limit is provided
    const events = this._index.get().slice()
    let result = []

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

  _read(ops, hash, amount, inclusive) {
    // Find the index of the gt/lt hash, or start from the beginning of the array if not found
    const index = ops.map((e) => e.hash).indexOf(hash)
    let startIndex = Math.max(index, 0)
    // If gte/lte is set, we include the given hash, if not, start from the next element
    startIndex += inclusive ? 0 : 1
    // Slice the array to its requested size
    const res = ops.slice(startIndex).slice(0, amount)
    return res
  }

  del(hash: string, options = {}) {
    const operation = {
      op: 'DEL',
      key: null,
      valuse: hash
    }
    return this._addOperation(operation, options)
  }
}

