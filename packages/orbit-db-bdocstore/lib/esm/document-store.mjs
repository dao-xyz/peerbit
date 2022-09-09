var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
import { DeleteOperation, DocumentIndex, Operation, PutAllOperation, PutOperation } from './document-index.mjs';
import pMap from 'p-map';
import { deserialize, field, serialize } from '@dao-xyz/borsh';
import { asString } from './utils.mjs';
import { FieldQuery, FieldStringMatchQuery, ResultWithSource, SortDirection, FieldByteMatchQuery, FieldBigIntCompareQuery, Compare } from '@dao-xyz/query-protocol';
import { arraysEqual } from '@dao-xyz/io-utils';
import { Store, load } from '@dao-xyz/orbit-db-store';
import { QueryStore } from '@dao-xyz/orbit-db-query-store';
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
        clazz,  indexBy: this.indexBy
      }
    } as DocumentStoreOptions<T>)
  }


  get identifier(): string {
    return BINARY_DOCUMENT_STORE_TYPE
  }
} */
/* export interface Typed {
  addTypes: (typeMap: { [name: string]: Constructor<any> }) => void
}
 */
const defaultOptions = (options) => {
    if (!options.encoding) {
        options.encoding = {
            decoder: (bytes) => deserialize(Buffer.from(bytes), Operation),
            encoder: (data) => serialize(data)
        };
    }
    return options;
};
export class BinaryDocumentStore extends QueryStore /*  implements Typed */ {
    constructor(properties) {
        super(properties);
        if (properties) {
            this.indexBy = properties.indexBy;
            this.objectType = properties.objectType;
        }
        this._index = new DocumentIndex();
    }
    /*  addTypes(_typeMap: { [name: string]: Constructor<any>; }) {
       throw new Error("Not implemented");
     } */
    async init(ipfs, identity, options) {
        if (!this._clazz) {
            if (!options.typeMap)
                throw new Error("Class not set, " + this.objectType);
            else {
                const clazz = options.typeMap[this.objectType];
                if (!clazz) {
                    throw new Error("Class not set in typemap, " + this.objectType);
                }
                this._clazz = clazz;
            }
        }
        this._index.init(this._clazz);
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
        let filters = documentQuery.queries.filter(q => q instanceof FieldQuery);
        let results = this.queryDocuments(doc => filters?.length > 0 ? filters.map(f => {
            if (f instanceof FieldQuery) {
                const fv = doc.value[f.key];
                if (f instanceof FieldStringMatchQuery) {
                    if (typeof fv !== 'string')
                        return false;
                    return fv.toLowerCase().indexOf(f.value.toLowerCase()) !== -1;
                }
                if (f instanceof FieldByteMatchQuery) {
                    if (!Array.isArray(fv))
                        return false;
                    return arraysEqual(fv, f.value);
                }
                if (f instanceof FieldBigIntCompareQuery) {
                    let value = fv;
                    if (typeof value !== 'bigint' && typeof value !== 'number') {
                        return false;
                    }
                    switch (f.compare) {
                        case Compare.Equal:
                            return value == f.value; // == because with want bigint == number at some cases
                        case Compare.Greater:
                            return value > f.value;
                        case Compare.GreaterOrEqual:
                            return value >= f.value;
                        case Compare.Less:
                            return value < f.value;
                        case Compare.LessOrEqual:
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
            if (documentQuery.sort.direction == SortDirection.Descending) {
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
        return Promise.resolve(results.map(r => new ResultWithSource({
            source: r
        })));
    }
    batchPut(docs, onProgressCallback) {
        const mapper = (doc, idx) => {
            return this._addOperationBatch({
                op: 'PUT',
                key: asString(doc[this.indexBy]),
                value: doc
            }, true, idx === docs.length - 1, onProgressCallback);
        };
        return pMap(docs, mapper, { concurrency: 1 })
            .then(() => this.saveSnapshot());
    }
    put(doc, options = {}) {
        if (!doc[this.indexBy]) {
            throw new Error(`The provided document doesn't contain field '${this.indexBy}'`);
        }
        const ser = serialize(doc);
        return this._addOperation(new PutOperation({
            key: asString(doc[this.indexBy]),
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
        return this._addOperation(new PutAllOperation({
            docs: docs.map((value) => new PutOperation({
                key: asString(value[this.indexBy]),
                value: serialize(value)
            }))
        }), options);
    }
    del(key, options = {}) {
        if (!this._index.get(key)) {
            throw new Error(`No entry with key '${key}' in the database`);
        }
        return this._addOperation(new DeleteOperation({
            key: asString(key)
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
    static async load(ipfs, address, options) {
        const instance = await load(ipfs, address, Store, options);
        if (instance instanceof BinaryDocumentStore === false) {
            throw new Error("Unexpected");
        }
        ;
        return instance;
    }
}
__decorate([
    field({ type: 'string' }),
    __metadata("design:type", String)
], BinaryDocumentStore.prototype, "indexBy", void 0);
__decorate([
    field({ type: 'string' }),
    __metadata("design:type", String)
], BinaryDocumentStore.prototype, "objectType", void 0);
//# sourceMappingURL=document-store.js.map