import Store from 'orbit-db-store'
import { FeedIndex } from './feed-index'
import { IPFS as IPFSInstance } from 'ipfs';
import { Identity } from 'orbit-db-identity-provider';
import { Constructor } from '@dao-xyz/borsh';
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

  del(hash: string, options = {}) {
    const operation = {
      op: 'DEL',
      key: null,
      value: hash
    }
    return this._addOperation(operation, options)
  }
}

