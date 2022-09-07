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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.BinaryDocumentStore = void 0;
const document_index_1 = require("./document-index");
const p_map_1 = __importDefault(require("p-map"));
const borsh_1 = require("@dao-xyz/borsh");
const utils_1 = require("./utils");
const query_protocol_1 = require("@dao-xyz/query-protocol");
const io_utils_1 = require("@dao-xyz/io-utils");
const orbit_db_query_store_1 = require("@dao-xyz/orbit-db-query-store");
const replaceAll = (str, search, replacement) => str.toString().split(search).join(replacement);
/*
export const BINARY_DOCUMENT_STORE_TYPE = 'bdoc_store';

export type DocumentStoreOptions<T> = IQueryStoreOptions<Operation> & { indexBy?: string, clazz: Constructor<T> };
export type IBinaryDocumentStoreOptions<T> = IQueryStoreOptions<Operation> & { indexBy?: string, clazz: Constructor<T> };

@variant([0, 0])
export class BinaryDocumentStoreOptions<T extends BinaryPayload> extends BStoreOptions<BinaryDocumentStore<T>> {

  @field({ type: 'string' })
  indexBy: string;

  @field({ type: 'string' })
  objectType: string;

  constructor(opts: {
    indexBy: string;
    objectType: string;

  }) {
    super();
    if (opts) {
      Object.assign(this, opts);
    }
  }
  async newStore(address: string, orbitDB: OrbitDB, options: IBinaryDocumentStoreOptions<T>): Promise<BinaryDocumentStore<T>> {
    let clazz = options.typeMap[this.objectType];
    if (!clazz) {
      throw new Error(`Undefined type: ${this.objectType}`);
    }
    return orbitDB.open(address, {
      ...options, ...{
        clazz, create: true, indexBy: this.indexBy
      }
    } as DocumentStoreOptions<T>)
  }


  get identifier(): string {
    return BINARY_DOCUMENT_STORE_TYPE
  }
} */
const defaultOptions = (options) => {
    /*   if (!options["indexBy"]) Object.assign(options, { indexBy: '_id' }) */
    if (!options.encoding) {
        options.encoding = {
            decoder: (bytes) => (0, borsh_1.deserialize)(Buffer.from(bytes), document_index_1.Operation),
            encoder: (data) => (0, borsh_1.serialize)(data)
        };
    }
    return options;
};
class BinaryDocumentStore extends orbit_db_query_store_1.QueryStore {
    constructor(properties) {
        super(properties);
        if (properties) {
            this.indexBy = properties.indexBy;
            this.objectType = properties.objectType;
        }
        this._index = new document_index_1.DocumentIndex();
    }
    async init(ipfs, identity, options) {
        this._index.init(options.clazz);
        await super.init(ipfs, identity, { ...defaultOptions(options), onUpdate: this._index.updateIndex.bind(this._index) });
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
    queryDocuments(filter) {
        // Whether we return the full operation data or just the db value
        return Object.keys(this._index._index)
            .map((e) => this._index.get(e))
            .filter((doc) => filter(doc));
    }
    queryHandler(query) {
        const documentQuery = query.type;
        let filters = documentQuery.queries.filter(q => q instanceof query_protocol_1.FieldQuery);
        let results = this.queryDocuments(doc => filters?.length > 0 ? filters.map(f => {
            if (f instanceof query_protocol_1.FieldQuery) {
                const fv = doc.value[f.key];
                if (f instanceof query_protocol_1.FieldStringMatchQuery) {
                    if (typeof fv !== 'string')
                        return false;
                    return fv.toLowerCase().indexOf(f.value.toLowerCase()) !== -1;
                }
                if (f instanceof query_protocol_1.FieldByteMatchQuery) {
                    if (!Array.isArray(fv))
                        return false;
                    return (0, io_utils_1.arraysEqual)(fv, f.value);
                }
                if (f instanceof query_protocol_1.FieldBigIntCompareQuery) {
                    let value = fv;
                    if (typeof value !== 'bigint' && typeof value !== 'number') {
                        return false;
                    }
                    switch (f.compare) {
                        case query_protocol_1.Compare.Equal:
                            return value == f.value; // == because with want bigint == number at some cases
                        case query_protocol_1.Compare.Greater:
                            return value > f.value;
                        case query_protocol_1.Compare.GreaterOrEqual:
                            return value >= f.value;
                        case query_protocol_1.Compare.Less:
                            return value < f.value;
                        case query_protocol_1.Compare.LessOrEqual:
                            return value <= f.value;
                        default:
                            console.warn("Unexpected compare");
                            return false;
                    }
                }
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
            if (documentQuery.sort.direction == query_protocol_1.SortDirection.Descending) {
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
        // TODO check conversions
        if (documentQuery.offset) {
            results = results.slice(Number(documentQuery.offset));
        }
        if (documentQuery.size) {
            results = results.slice(0, Number(documentQuery.size));
        }
        return Promise.resolve(results.map(r => new query_protocol_1.ResultWithSource({
            source: r
        })));
    }
    batchPut(docs, onProgressCallback) {
        const mapper = (doc, idx) => {
            return this._addOperationBatch({
                op: 'PUT',
                key: (0, utils_1.asString)(doc[this.indexBy]),
                value: doc
            }, true, idx === docs.length - 1, onProgressCallback);
        };
        return (0, p_map_1.default)(docs, mapper, { concurrency: 1 })
            .then(() => this.saveSnapshot());
    }
    put(doc, options = {}) {
        if (!doc[this.indexBy]) {
            throw new Error(`The provided document doesn't contain field '${this.indexBy}'`);
        }
        const ser = (0, borsh_1.serialize)(doc);
        return this._addOperation(new document_index_1.PutOperation({
            key: (0, utils_1.asString)(doc[this.indexBy]),
            value: ser,
        }), options);
    }
    putAll(docs, options = {}) {
        if (!(Array.isArray(docs))) {
            docs = [docs];
        }
        if (!(docs.every(d => d[this.indexBy]))) {
            throw new Error(`The provided document doesn't contain field '${this.indexBy}'`);
        }
        return this._addOperation(new document_index_1.PutAllOperation({
            docs: docs.map((value) => new document_index_1.PutOperation({
                key: (0, utils_1.asString)(value[this.indexBy]),
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
        return Object.keys(this._index).length;
    }
    clone(newName) {
        return new BinaryDocumentStore({
            accessController: this.access.clone(newName),
            indexBy: this.indexBy,
            objectType: this.objectType,
            name: newName,
            queryRegion: this.queryRegion
        });
    }
}
__decorate([
    (0, borsh_1.field)({ type: 'string' }),
    __metadata("design:type", String)
], BinaryDocumentStore.prototype, "indexBy", void 0);
__decorate([
    (0, borsh_1.field)({ type: 'string' }),
    __metadata("design:type", String)
], BinaryDocumentStore.prototype, "objectType", void 0);
exports.BinaryDocumentStore = BinaryDocumentStore;
//# sourceMappingURL=document-store.js.map