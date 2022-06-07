"use strict";
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
exports.BinaryDocumentStore = exports.BINARY_DOCUMENT_STORE_TYPE = void 0;
const orbit_db_store_1 = __importDefault(require("orbit-db-store"));
const document_index_1 = require("./document-index");
const p_map_1 = __importDefault(require("p-map"));
const borsh_1 = require("@dao-xyz/borsh");
const bs58_1 = __importDefault(require("bs58"));
const utils_1 = require("./utils");
const query_1 = require("./query");
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
        this._subscribed = false;
        this._type = exports.BINARY_DOCUMENT_STORE_TYPE;
        this._index.init(this.options.clazz);
        ipfs.dag;
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
    query(mapper, options = {}) {
        // Whether we return the full operation data or just the db value
        const fullOp = options.fullOp || false;
        const getValue = fullOp ? (value) => value.payload.value : (value) => value;
        return Object.keys(this._index._index)
            .map((e) => this._index.get(e, fullOp))
            .filter((doc) => mapper(getValue(doc)));
    }
    queryAny(query, clazz, responseHandler, maxAggregationTime = 30 * 1000) {
        return __awaiter(this, void 0, void 0, function* () {
            // send query and wait for replies in a generator like behaviour
            let responseTopic = query.getResponseTopic(this.queryTopic);
            yield this._ipfs.pubsub.subscribe(responseTopic, (msg) => {
                const encoded = (0, borsh_1.deserialize)(Buffer.from(msg.data), query_1.EncodedQueryResponse);
                let result = query_1.QueryResponse.from(encoded, clazz);
                responseHandler(result);
            });
            yield this._ipfs.pubsub.publish(this.queryTopic, (0, borsh_1.serialize)(query));
        });
    }
    load(amount, opts) {
        const _super = Object.create(null, {
            load: { get: () => super.load }
        });
        return __awaiter(this, void 0, void 0, function* () {
            yield _super.load.call(this, amount, opts);
            yield this._subscribeToQueries();
        });
    }
    close() {
        const _super = Object.create(null, {
            close: { get: () => super.close }
        });
        return __awaiter(this, void 0, void 0, function* () {
            yield this._ipfs.pubsub.unsubscribe(this.queryTopic);
            this._subscribed = false;
            yield _super.close.call(this);
        });
    }
    _subscribeToQueries() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this._subscribed) {
                return;
            }
            yield this._ipfs.pubsub.subscribe(this.queryTopic, (msg) => __awaiter(this, void 0, void 0, function* () {
                try {
                    let query = (0, borsh_1.deserialize)(Buffer.from(msg.data), query_1.QueryRequestV0);
                    let filters = query.queries;
                    let results = this.query(doc => filters.map(f => {
                        if (f instanceof query_1.Query) {
                            return f.apply(doc);
                        }
                        else {
                            return f(doc);
                        }
                    }).reduce((prev, current) => prev && current));
                    if (query.sort) {
                        const resolveField = (obj) => {
                            let v = obj;
                            for (let i = 0; i < query.sort.fieldPath.length; i++) {
                                v = v[query.sort.fieldPath[i]];
                            }
                            return v;
                        };
                        let direction = 1;
                        if (query.sort.direction == query_1.SortDirection.Descending) {
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
                    if (query.offset) {
                        results = results.slice(query.offset.toNumber());
                    }
                    if (query.size) {
                        results = results.slice(0, query.size.toNumber());
                    }
                    let response = new query_1.EncodedQueryResponse({
                        results: results.map(r => bs58_1.default.encode((0, borsh_1.serialize)(r)))
                    });
                    let bytes = (0, borsh_1.serialize)(response);
                    yield this._ipfs.pubsub.publish(query.getResponseTopic(this.queryTopic), bytes);
                }
                catch (error) {
                    console.error(error);
                }
            }));
            this._subscribed = true;
        });
    }
    get queryTopic() {
        return this.address + '/query';
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
        return this._addOperation({
            op: 'PUT',
            key: (0, utils_1.asString)(doc[this.options.indexBy]),
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
                key: (0, utils_1.asString)(value[this.options.indexBy]),
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
            key: (0, utils_1.asString)(key),
            value: null
        }, options);
    }
}
exports.BinaryDocumentStore = BinaryDocumentStore;
//# sourceMappingURL=document-store.js.map