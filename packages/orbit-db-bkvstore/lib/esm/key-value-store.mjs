import { serialize } from '@dao-xyz/borsh';
import Store from 'orbit-db-store';
import { KeyValueIndex } from './key-value-index.mjs';
import bs58 from 'bs58';
export const BINARY_KEYVALUE_STORE_TYPE = 'bkvstore';
const defaultOptions = (options) => {
    if (!options.Index)
        Object.assign(options, { Index: KeyValueIndex });
    return options;
};
export class BinaryKeyValueStore extends Store {
    constructor(ipfs, id, dbname, options) {
        super(ipfs, id, dbname, defaultOptions(options));
        this._type = BINARY_KEYVALUE_STORE_TYPE;
        this._index.init(this.options.clazz);
    }
    /*  get all(): T[] {
       return this._index._index
     } */
    get(key) {
        return this._index.get(key);
    }
    set(key, data, options = {}) {
        return this.put(key, data, options);
    }
    put(key, data, options = {}) {
        return this._addOperation({
            op: 'PUT',
            key: key,
            value: bs58.encode(serialize(data))
        }, options);
    }
    del(key, options = {}) {
        return this._addOperation({
            op: 'DEL',
            key: key,
            value: null
        }, options);
    }
}
//# sourceMappingURL=key-value-store.js.map