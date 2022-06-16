import { Constructor, field, serialize, variant } from '@dao-xyz/borsh';
import Store from 'orbit-db-store';
import { KeyValueIndex } from './key-value-index';
import bs58 from 'bs58';
import OrbitDB from 'orbit-db';
import { StoreOptions, IQueryStoreOptions } from '@dao-xyz/orbit-db-bstores'
export const BINARY_KEYVALUE_STORE_TYPE = 'bkv_store';

const defaultOptions = (options: IStoreOptions): any => {
  if (!options.Index) Object.assign(options, { Index: KeyValueIndex })
  return options;
}


@variant([0, 1])
export class BinaryKeyValueStoreOptions<T> extends StoreOptions<BinaryKeyValueStore<T>> {


  @field({ type: 'String' })
  objectType: string;

  constructor(opts: {
    objectType: string;

  }) {
    super();
    if (opts) {
      Object.assign(this, opts);
    }
  }
  async newStore(address: string, orbitDB: OrbitDB, typeMap: { [key: string]: Constructor<any> }, options: IQueryStoreOptions): Promise<BinaryKeyValueStore<T>> {
    let clazz = typeMap[this.objectType];
    if (!clazz) {
      throw new Error(`Undefined type: ${this.objectType}`);
    }

    return orbitDB.open<BinaryKeyValueStore<T>>(address, { ...options, ...{ clazz, create: true, type: BINARY_KEYVALUE_STORE_TYPE } } as any)
  }

  get identifier(): string {
    return BINARY_KEYVALUE_STORE_TYPE
  }

}

export class BinaryKeyValueStore<T> extends Store<T, KeyValueIndex<T>> {

  _type: string;

  constructor(ipfs, id, dbname, options: IStoreOptions & { clazz: Constructor<T> }) {
    super(ipfs, id, dbname, defaultOptions(options))
    this._type = BINARY_KEYVALUE_STORE_TYPE;
    this._index.init(this.options.clazz);
  }

  /*  get all(): T[] {
     return this._index._index
   } */

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

  public get size(): number {
    return Object.keys(this._index._index).length
  }
}

OrbitDB.addDatabaseType(BINARY_KEYVALUE_STORE_TYPE, BinaryKeyValueStore as any)
