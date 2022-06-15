import { DocumentIndex } from './document-index.mjs';
import pMap from 'p-map';
import { serialize } from '@dao-xyz/borsh';
import bs58 from 'bs58';
import { asString } from './utils.mjs';
import { FieldQuery, ResultWithSource, SortDirection } from '@dao-xyz/bquery';
import { QueryStore } from '@dao-xyz/orbit-db-query-store';
const replaceAll = (str, search, replacement) => str.toString().split(search).join(replacement);
export const BINARY_DOCUMENT_STORE_TYPE = 'bdocstore';
const defaultOptions = (options) => {
    if (!options["indexBy"])
        Object.assign(options, { indexBy: '_id' });
    if (!options.Index)
        Object.assign(options, { Index: DocumentIndex });
    return options;
};
export class BinaryDocumentStore extends QueryStore {
    constructor(ipfs, id, dbname, options) {
        super(ipfs, id, dbname, defaultOptions(options));
        this._type = undefined;
        this._subscribed = false;
        this.subscribeToQueries = false;
        this._type = BINARY_DOCUMENT_STORE_TYPE;
        this._index.init(this.options.clazz);
        this.subscribeToQueries = options.subscribeToQueries;
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
    async load(amount, opts) {
        await super.load(amount, opts);
    }
    async close() {
        await super.close();
    }
    queryDocuments(mapper, options = {}) {
        // Whether we return the full operation data or just the db value
        const fullOp = options.fullOp || false;
        const getValue = fullOp ? (value) => value.payload.value : (value) => value;
        return Object.keys(this.index._index)
            .map((e) => this.index.get(e, fullOp))
            .filter((doc) => mapper(getValue(doc)));
    }
    queryHandler(query) {
        const documentQuery = query.type;
        let filters = documentQuery.queries;
        let results = this.queryDocuments(doc => (filters === null || filters === void 0 ? void 0 : filters.length) > 0 ? filters.map(f => {
            if (f instanceof FieldQuery) {
                return f.apply(doc);
            }
            else {
                return f(doc);
            }
        }).reduce((prev, current) => prev && current) : true);
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
        if (documentQuery.offset) {
            results = results.slice(documentQuery.offset.toNumber());
        }
        if (documentQuery.size) {
            results = results.slice(0, documentQuery.size.toNumber());
        }
        return Promise.resolve(results.map(r => new ResultWithSource({
            source: r
        })));
    }
    batchPut(docs, onProgressCallback) {
        const mapper = (doc, idx) => {
            return this._addOperationBatch({
                op: 'PUT',
                key: asString(doc[this.options.indexBy]),
                value: doc
            }, true, idx === docs.length - 1, onProgressCallback);
        };
        return pMap(docs, mapper, { concurrency: 1 })
            .then(() => this.saveSnapshot());
    }
    put(doc, options = {}) {
        if (!doc[this.options.indexBy]) {
            throw new Error(`The provided document doesn't contain field '${this.options.indexBy}'`);
        }
        return this._addOperation({
            op: 'PUT',
            key: asString(doc[this.options.indexBy]),
            value: bs58.encode(serialize(doc)),
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
                key: asString(value[this.options.indexBy]),
                value: bs58.encode(serialize(value))
            }))
        }, options);
    }
    del(key, options = {}) {
        if (!this._index.get(key)) {
            throw new Error(`No entry with key '${key}' in the database`);
        }
        return this._addOperation({
            op: 'DEL',
            key: asString(key),
            value: null
        }, options);
    }
}
//# sourceMappingURL=document-store.js.map