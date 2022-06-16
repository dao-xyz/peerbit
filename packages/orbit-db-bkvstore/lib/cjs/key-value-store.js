"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.BinaryKeyValueStore = exports.BinaryKeyValueStoreOptions = exports.BINARY_KEYVALUE_STORE_TYPE = void 0;
const borsh_1 = require("@dao-xyz/borsh");
const orbit_db_store_1 = __importDefault(require("orbit-db-store"));
const key_value_index_1 = require("./key-value-index");
const bs58_1 = __importDefault(require("bs58"));
const orbit_db_1 = __importDefault(require("orbit-db"));
const orbit_db_bstores_1 = require("@dao-xyz/orbit-db-bstores");
exports.BINARY_KEYVALUE_STORE_TYPE = 'bkv_store';
const defaultOptions = (options) => {
    if (!options.Index)
        Object.assign(options, { Index: key_value_index_1.KeyValueIndex });
    return options;
};
let BinaryKeyValueStoreOptions = class BinaryKeyValueStoreOptions extends orbit_db_bstores_1.StoreOptions {
    constructor(opts) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }
    newStore(address, orbitDB, typeMap, options) {
        return __awaiter(this, void 0, void 0, function* () {
            let clazz = typeMap[this.objectType];
            if (!clazz) {
                throw new Error(`Undefined type: ${this.objectType}`);
            }
            return orbitDB.open(address, Object.assign(Object.assign({}, options), { clazz, create: true, type: exports.BINARY_KEYVALUE_STORE_TYPE }));
        });
    }
    get identifier() {
        return exports.BINARY_KEYVALUE_STORE_TYPE;
    }
};
__decorate([
    (0, borsh_1.field)({ type: 'String' }),
    __metadata("design:type", String)
], BinaryKeyValueStoreOptions.prototype, "objectType", void 0);
BinaryKeyValueStoreOptions = __decorate([
    (0, borsh_1.variant)([0, 1]),
    __metadata("design:paramtypes", [Object])
], BinaryKeyValueStoreOptions);
exports.BinaryKeyValueStoreOptions = BinaryKeyValueStoreOptions;
class BinaryKeyValueStore extends orbit_db_store_1.default {
    constructor(ipfs, id, dbname, options) {
        super(ipfs, id, dbname, defaultOptions(options));
        this._type = exports.BINARY_KEYVALUE_STORE_TYPE;
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
orbit_db_1.default.addDatabaseType(exports.BINARY_KEYVALUE_STORE_TYPE, BinaryKeyValueStore);
//# sourceMappingURL=key-value-store.js.map