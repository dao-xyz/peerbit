"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.BinaryDocumentStore = exports.BINARY_DOCUMENT_STORE_TYPE = void 0;
const orbit_db_store_1 = __importDefault(require("orbit-db-store"));
const document_index_1 = require("./document-index");
const p_map_1 = __importDefault(require("p-map"));
const borsh_1 = require("@dao-xyz/borsh");
const bs58_1 = __importDefault(require("bs58"));
const replaceAll = (str, search, replacement) => str.toString().split(search).join(replacement);
exports.BINARY_DOCUMENT_STORE_TYPE = 'bdocstore';
const defaultOptions = (options) => {
    if (!options["indexBy"])
        Object.assign(options, { indexBy: '_id' });
    if (!options.Index)
        Object.assign(options, { Index: document_index_1.DocumentIndex });
    return options;
};
class BinaryDocumentStore extends orbit_db_store_1.default {
    constructor(ipfs, id, dbname, options) {
        super(ipfs, id, dbname, defaultOptions(options));
        this._type = undefined;
        this._type = exports.BINARY_DOCUMENT_STORE_TYPE;
        this._index.init(this.options.clazz);
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
    query(mapper, options = {}) {
        // Whether we return the full operation data or just the db value
        const fullOp = options["fullOp"] || false;
        return Object.keys(this._index._index)
            .map((e) => this._index.get(e, fullOp))
            .filter(mapper);
    }
    batchPut(docs, onProgressCallback) {
        const mapper = (doc, idx) => {
            return this._addOperationBatch({
                op: 'PUT',
                key: doc[this.options.indexBy],
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
        return this._addOperation({
            op: 'PUT',
            key: doc[this.options.indexBy],
            value: bs58_1.default.encode((0, borsh_1.serialize)(doc)),
        }, options);
    }
    putAll(docs, options = {}) {
        if (!(Array.isArray(docs))) {
            docs = [docs];
        }
        if (!(docs.every(d => d[this.options.indexBy]))) {
            throw new Error(`The provided document doesn't contain field '${this.options.indexBy}'`);
        }
        return this._addOperation({
            op: 'PUTALL',
            docs: docs.map((value) => ({
                key: value[this.options.indexBy],
                value: bs58_1.default.encode((0, borsh_1.serialize)(value))
            }))
        }, options);
    }
    del(key, options = {}) {
        if (!this._index.get(key)) {
            throw new Error(`No entry with key '${key}' in the database`);
        }
        return this._addOperation({
            op: 'DEL',
            key: key,
            value: null
        }, options);
    }
}
exports.BinaryDocumentStore = BinaryDocumentStore;
//# sourceMappingURL=document-store.js.map