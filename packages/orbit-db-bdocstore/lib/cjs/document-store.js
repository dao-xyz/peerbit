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
exports.BinaryDocumentStore = exports.BinaryDocumentStoreOptions = exports.BINARY_DOCUMENT_STORE_TYPE = void 0;
const document_index_1 = require("./document-index");
const p_map_1 = __importDefault(require("p-map"));
const borsh_1 = require("@dao-xyz/borsh");
const utils_1 = require("./utils");
const bquery_1 = require("@dao-xyz/bquery");
const orbit_db_query_store_1 = require("@dao-xyz/orbit-db-query-store");
const orbit_db_bstores_1 = require("@dao-xyz/orbit-db-bstores");
const orbit_db_1 = require("@dao-xyz/orbit-db");
const replaceAll = (str, search, replacement) => str.toString().split(search).join(replacement);
exports.BINARY_DOCUMENT_STORE_TYPE = 'bdoc_store';
let BinaryDocumentStoreOptions = class BinaryDocumentStoreOptions extends orbit_db_bstores_1.BStoreOptions {
    constructor(opts) {
        super();
        if (opts) {
            Object.assign(this, opts);
        }
    }
    newStore(address, orbitDB, options) {
        return __awaiter(this, void 0, void 0, function* () {
            let clazz = options.typeMap[this.objectType];
            if (!clazz) {
                throw new Error(`Undefined type: ${this.objectType}`);
            }
            return orbitDB.open(address, Object.assign(Object.assign({}, options), {
                clazz, create: true, type: exports.BINARY_DOCUMENT_STORE_TYPE, indexBy: this.indexBy
            }));
        });
    }
    get identifier() {
        return exports.BINARY_DOCUMENT_STORE_TYPE;
    }
};
__decorate([
    (0, borsh_1.field)({ type: 'String' }),
    __metadata("design:type", String)
], BinaryDocumentStoreOptions.prototype, "indexBy", void 0);
__decorate([
    (0, borsh_1.field)({ type: 'String' }),
    __metadata("design:type", String)
], BinaryDocumentStoreOptions.prototype, "objectType", void 0);
BinaryDocumentStoreOptions = __decorate([
    (0, borsh_1.variant)([0, 0]),
    __metadata("design:paramtypes", [Object])
], BinaryDocumentStoreOptions);
exports.BinaryDocumentStoreOptions = BinaryDocumentStoreOptions;
const defaultOptions = (options) => {
    if (!options["indexBy"])
        Object.assign(options, { indexBy: '_id' });
    if (!options.Index)
        Object.assign(options, { Index: document_index_1.DocumentIndex });
    if (!options.io) {
        options.io = {
            decoder: (bytes) => (0, borsh_1.deserialize)(Buffer.from(bytes), document_index_1.Operation),
            encoder: (data) => (0, borsh_1.serialize)(data)
        };
    }
    return options;
};
class BinaryDocumentStore extends orbit_db_query_store_1.QueryStore {
    constructor(ipfs, id, dbname, options) {
        super(ipfs, id, dbname, defaultOptions(options));
        this._type = undefined;
        this._type = exports.BINARY_DOCUMENT_STORE_TYPE;
        this._index.init(this.options.clazz);
    }
    get index() {
        return this._index;
    }
    get(key, caseSensitive = false) {
        key = key.toString();
        const terms = key.split(' ');
        key = terms.length > 1 ? replaceAll(key, '.', ' ').toLowerCase() : key.toLowerCase();
        const search = (e) => {
            if (terms.length > 1) {
                return replaceAll(e, '.', ' ').toLowerCase().indexOf(key) !== -1;
            }
            return e.toLowerCase().indexOf(key) !== -1;
        };
        const mapper = e => this._index.get(e);
        const filter = e => caseSensitive
            ? e.indexOf(key) !== -1
            : search(e);
        return Object.keys(this._index._index)
            .filter(filter)
            .map(mapper);
    }
    load(amount, opts) {
        const _super = Object.create(null, {
            load: { get: () => super.load }
        });
        return __awaiter(this, void 0, void 0, function* () {
            yield _super.load.call(this, amount, opts);
        });
    }
    close() {
        const _super = Object.create(null, {
            close: { get: () => super.close }
        });
        return __awaiter(this, void 0, void 0, function* () {
            yield _super.close.call(this);
        });
    }
    queryDocuments(mapper) {
        // Whether we return the full operation data or just the db value
        return Object.keys(this.index._index)
            .map((e) => this.index.get(e))
            .filter((doc) => mapper(doc));
    }
    queryHandler(query) {
        const documentQuery = query.type;
        let filters = documentQuery.queries.filter(q => q instanceof bquery_1.FieldQuery);
        let results = this.queryDocuments(doc => (filters === null || filters === void 0 ? void 0 : filters.length) > 0 ? filters.map(f => {
            if (f instanceof bquery_1.FieldQuery) {
                return f.apply(doc.value);
            }
            else {
                throw new Error("Unsupported query type");
            }
        }).reduce((prev, current) => prev && current) : true).map(x => x.value);
        if (documentQuery.sort) {
            const resolveField = (obj) => {
                let v = obj;
                for (let i = 0; i < documentQuery.sort.fieldPath.length; i++) {
                    v = v[documentQuery.sort.fieldPath[i]];
                }
                return v;
            };
            let direction = 1;
            if (documentQuery.sort.direction == bquery_1.SortDirection.Descending) {
                direction = -1;
            }
            results.sort((a, b) => {
                const af = resolveField(a);
                const bf = resolveField(b);
                if (af < bf) {
                    return -direction;
                }
                else if (af > bf) {
                    return direction;
                }
                return 0;
            });
        }
        if (documentQuery.offset) {
            results = results.slice(documentQuery.offset.toNumber());
        }
        if (documentQuery.size) {
            results = results.slice(0, documentQuery.size.toNumber());
        }
        return Promise.resolve(results.map(r => new bquery_1.ResultWithSource({
            source: r
        })));
    }
    batchPut(docs, onProgressCallback) {
        const mapper = (doc, idx) => {
            return this._addOperationBatch({
                op: 'PUT',
                key: (0, utils_1.asString)(doc[this.options.indexBy]),
                value: doc
            }, true, idx === docs.length - 1, onProgressCallback);
        };
        return (0, p_map_1.default)(docs, mapper, { concurrency: 1 })
            .then(() => this.saveSnapshot());
    }
    put(doc, options = {}) {
        if (!doc[this.options.indexBy]) {
            throw new Error(`The provided document doesn't contain field '${this.options.indexBy}'`);
        }
        return this._addOperation(new document_index_1.PutOperation({
            key: (0, utils_1.asString)(doc[this.options.indexBy]),
            value: (0, borsh_1.serialize)(doc),
        }), options);
    }
    putAll(docs, options = {}) {
        if (!(Array.isArray(docs))) {
            docs = [docs];
        }
        if (!(docs.every(d => d[this.options.indexBy]))) {
            throw new Error(`The provided document doesn't contain field '${this.options.indexBy}'`);
        }
        return this._addOperation(new document_index_1.PutAllOperation({
            docs: docs.map((value) => new document_index_1.PutOperation({
                key: (0, utils_1.asString)(value[this.options.indexBy]),
                value: (0, borsh_1.serialize)(value)
            }))
        }), options);
    }
    del(key, options = {}) {
        if (!this._index.get(key)) {
            throw new Error(`No entry with key '${key}' in the database`);
        }
        return this._addOperation(new document_index_1.DeleteOperation({
            key: (0, utils_1.asString)(key)
        }), options);
    }
    get size() {
        return Object.keys(this.index._index).length;
    }
}
exports.BinaryDocumentStore = BinaryDocumentStore;
orbit_db_1.OrbitDB.addDatabaseType(exports.BINARY_DOCUMENT_STORE_TYPE, BinaryDocumentStore);
//# sourceMappingURL=document-store.js.map