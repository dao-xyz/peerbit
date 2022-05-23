import { Constructor, serialize } from '@dao-xyz/borsh';
import Store from 'orbit-db-store';
import { KeyValueIndex } from './key-value-index';
import bs58 from 'bs58';

export const BINARY_KEYVALUE_STORE_TYPE = 'bkvstore';

const defaultOptions = (options: IStoreOptions): any => {
  if (!options.Index) Object.assign(options, { Index: KeyValueIndex })
  return options;
}

export class BinaryKeyValueStore<T> extends Store {

  _type: string;

  constructor(ipfs, id, dbname, options: IStoreOptions & { clazz: Constructor<T> }) {
    super(ipfs, id, dbname, defaultOptions(options))
    this._type = BINARY_KEYVALUE_STORE_TYPE;
    (this._index as KeyValueIndex<T>).init(this.options.clazz);
  }

  get all(): T[] {
    return this._index._index
  }

  get(key: string): T {
    return this._index.get(key)
  }

  set(key: string, data: T, options = {}) {
    return this.put(key, data, options)
  }

  put(key: string, data: T, options = {}) {
    return this._addOperation({
      op: 'PUT',
      key: key,
      value: bs58.encode(serialize(data))
    }, options)
  }

  del(key: string, options = {}) {
    return this._addOperation({
      op: 'DEL',
      key: key,
      value: null
    }, options)
  }
}

