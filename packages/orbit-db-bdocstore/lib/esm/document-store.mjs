import Store from 'orbit-db-store';
import { DocumentIndex } from './document-index.mjs';
import pMap from 'p-map';
import { serialize } from '@dao-xyz/borsh';
import bs58 from 'bs58';
import { asString } from './utils.mjs';
const replaceAll = (str, search, replacement) => str.toString().split(search).join(replacement);
export const BINARY_DOCUMENT_STORE_TYPE = 'bdocstore';
const defaultOptions = (options) => {
    if (!options["indexBy"])
        Object.assign(options, { indexBy: '_id' });
    if (!options.Index)
        Object.assign(options, { Index: DocumentIndex });
    return options;
};
export class BinaryDocumentStore extends Store {
    constructor(ipfs, id, dbname, options) {
        super(ipfs, id, dbname, defaultOptions(options));
        this._type = undefined;
        this._type = BINARY_DOCUMENT_STORE_TYPE;
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
    query(mapper, options = {}) {
        // Whether we return the full operation data or just the db value
        const fullOp = options.fullOp || false;
        const getValue = fullOp ? (value) => value.payload.value : (value) => value;
        return Object.keys(this._index._index)
            .map((e) => this._index.get(e, fullOp))
            .filter((doc) => mapper(getValue(doc)));
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