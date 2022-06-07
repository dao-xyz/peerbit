import Store from 'orbit-db-store';
import { DocumentIndex } from './document-index.mjs';
import pMap from 'p-map';
import { deserialize, serialize } from '@dao-xyz/borsh';
import bs58 from 'bs58';
import { asString } from './utils.mjs';
import { EncodedQueryResponse, Query, QueryRequestV0, QueryResponse, SortDirection } from './query.mjs';
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
        this._subscribed = false;
        this._type = BINARY_DOCUMENT_STORE_TYPE;
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
    async queryAny(query, clazz, responseHandler, maxAggregationTime = 30 * 1000) {
        // send query and wait for replies in a generator like behaviour
        let responseTopic = query.getResponseTopic(this.queryTopic);
        await this._ipfs.pubsub.subscribe(responseTopic, (msg) => {
            const encoded = deserialize(Buffer.from(msg.data), EncodedQueryResponse);
            let result = QueryResponse.from(encoded, clazz);
            responseHandler(result);
        });
        await this._ipfs.pubsub.publish(this.queryTopic, serialize(query));
    }
    async load(amount, opts) {
        await super.load(amount, opts);
        await this._subscribeToQueries();
    }
    async close() {
        await this._ipfs.pubsub.unsubscribe(this.queryTopic);
        this._subscribed = false;
        await super.close();
    }
    async _subscribeToQueries() {
        if (this._subscribed) {
            return;
        }
        await this._ipfs.pubsub.subscribe(this.queryTopic, async (msg) => {
            try {
                let query = deserialize(Buffer.from(msg.data), QueryRequestV0);
                let filters = query.queries;
                let results = this.query(doc => filters.map(f => {
                    if (f instanceof Query) {
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
                    if (query.sort.direction == SortDirection.Descending) {
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
                let response = new EncodedQueryResponse({
                    results: results.map(r => bs58.encode(serialize(r)))
                });
                let bytes = serialize(response);
                await this._ipfs.pubsub.publish(query.getResponseTopic(this.queryTopic), bytes);
            }
            catch (error) {
                console.error(error);
            }
        });
        this._subscribed = true;
    }
    get queryTopic() {
        return this.address + '/query';
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