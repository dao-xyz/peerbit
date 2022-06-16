var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
import { field, serialize, variant } from '@dao-xyz/borsh';
import Store from 'orbit-db-store';
import { KeyValueIndex } from './key-value-index.mjs';
import bs58 from 'bs58';
import OrbitDB from 'orbit-db';
import { StoreOptions } from '@dao-xyz/orbit-db-bstores';
export const BINARY_KEYVALUE_STORE_TYPE = 'bkv_store';
const defaultOptions = (options) => {
    if (!options.Index)
        Object.assign(options, { Index: KeyValueIndex });
    return options;
};
let BinaryKeyValueStoreOptions = class BinaryKeyValueStoreOptions extends StoreOptions {
    constructor(opts) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }
    async newStore(address, orbitDB, typeMap, options) {
        let clazz = typeMap[this.objectType];
        if (!clazz) {
            throw new Error(`Undefined type: ${this.objectType}`);
        }
        return orbitDB.open(address, { ...options, ...{ clazz, create: true, type: BINARY_KEYVALUE_STORE_TYPE } });
    }
    get identifier() {
        return BINARY_KEYVALUE_STORE_TYPE;
    }
};
__decorate([
    field({ type: 'String' }),
    __metadata("design:type", String)
], BinaryKeyValueStoreOptions.prototype, "objectType", void 0);
BinaryKeyValueStoreOptions = __decorate([
    variant([0, 1]),
    __metadata("design:paramtypes", [Object])
], BinaryKeyValueStoreOptions);
export { BinaryKeyValueStoreOptions };
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
    get size() {
        return Object.keys(this._index._index).length;
    }
}
OrbitDB.addDatabaseType(BINARY_KEYVALUE_STORE_TYPE, BinaryKeyValueStore);
//# sourceMappingURL=key-value-store.js.map