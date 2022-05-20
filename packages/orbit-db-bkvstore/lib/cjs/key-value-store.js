"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.BinaryKeyValueStore = exports.BINARY_KEYVALUE_STORE_TYPE = void 0;
const borsh_1 = require("@dao-xyz/borsh");
const orbit_db_store_1 = __importDefault(require("orbit-db-store"));
const key_value_index_1 = require("./key-value-index");
const bs58_1 = __importDefault(require("bs58"));
exports.BINARY_KEYVALUE_STORE_TYPE = 'bkvstore';
const defaultOptions = (options) => {
    if (!options.Index)
        Object.assign(options, { Index: key_value_index_1.KeyValueIndex });
    return options;
};
class BinaryKeyValueStore extends orbit_db_store_1.default {
    constructor(ipfs, id, dbname, options) {
        super(ipfs, id, dbname, defaultOptions(options));
        this._type = exports.BINARY_KEYVALUE_STORE_TYPE;
        this._index.init(this.options.clazz);
    }
    get all() {
        return this._index._index;
    }
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
            value: bs58_1.default.encode((0, borsh_1.serialize)(data))
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
exports.BinaryKeyValueStore = BinaryKeyValueStore;
//# sourceMappingURL=key-value-store.js.map