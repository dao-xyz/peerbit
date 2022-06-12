import Store from 'orbit-db-store'
import { DocumentIndex } from './document-index'
import pMap from 'p-map'
import { Identity } from 'orbit-db-identity-provider';
import { Constructor, deserialize, serialize } from '@dao-xyz/borsh';
import bs58 from 'bs58';
import { asString } from './utils';
import { Message } from 'ipfs-core-types/types/src/pubsub'
import { EncodedQueryResponse, Query, QueryRequestV0, QueryResponse, SortDirection } from './query';
import { IPFS as IPFSInstance } from "ipfs-core-types";

const replaceAll = (str, search, replacement) => str.toString().split(search).join(replacement)

export const BINARY_DOCUMENT_STORE_TYPE = 'bdocstore';
export type DocumentStoreOptions<T> = IStoreOptions & { indexBy?: string, clazz: Constructor<T>, subscribeToQueries: boolean };
const defaultOptions = (options: IStoreOptions): IStoreOptions => {
  if (!options["indexBy"]) Object.assign(options, { indexBy: '_id' })
  if (!options.Index) Object.assign(options, { Index: DocumentIndex })
  return options;
}
export class BinaryDocumentStore<T> extends Store<T, DocumentIndex<T>> {

  _type: string = undefined;
  _subscribed: boolean = false
  subscribeToQueries = false;
  constructor(ipfs: IPFSInstance, id: Identity, dbname: string, options: DocumentStoreOptions<T>) {
    super(ipfs, id, dbname, defaultOptions(options))
    this._type = BINARY_DOCUMENT_STORE_TYPE;
    this._index.init(this.options.clazz);
    this.subscribeToQueries = options.subscribeToQueries;
    ipfs.dag
  }
  public get index(): DocumentIndex<T> {
    return this._index;
  }

  public get(key: any, caseSensitive = false): T[] {
    key = key.toString()
    const terms = key.split(' ')
    key = terms.length > 1 ? replaceAll(key, '.', ' ').toLowerCase() : key.toLowerCase()

    const search = (e) => {
      if (terms.length > 1) {
        return replaceAll(e, '.', ' ').toLowerCase().indexOf(key) !== -1
      }
      return e.toLowerCase().indexOf(key) !== -1
    }
    const mapper = e => this._index.get(e) as T
    const filter = e => caseSensitive
      ? e.indexOf(key) !== -1
      : search(e)

    return Object.keys(this._index._index)
      .filter(filter)
      .map(mapper)
  }

  public query(mapper: ((doc: T) => boolean), options: { fullOp?: boolean } = {}): T[] | { payload: Payload<T> }[] {
    // Whether we return the full operation data or just the db value
    const fullOp = options.fullOp || false
    const getValue: (value: T | { payload: Payload<T> }) => T = fullOp ? (value: { payload: Payload<T> }) => value.payload.value : (value: T) => value
    return Object.keys(this._index._index)
      .map((e) => this._index.get(e, fullOp))
      .filter((doc) => mapper(getValue(doc))) as T[] | { payload: Payload<T> }[]
  }


  async queryAny(query: QueryRequestV0, clazz: Constructor<T>, responseHandler: (response: QueryResponse<T>) => void, maxAggregationTime: number = 30 * 1000) {
    // send query and wait for replies in a generator like behaviour
    let responseTopic = query.getResponseTopic(this.queryTopic);
    await this._ipfs.pubsub.subscribe(responseTopic, (msg: Message) => {
      const encoded = deserialize(Buffer.from(msg.data), EncodedQueryResponse);
      let result = QueryResponse.from(encoded, clazz);
      responseHandler(result);
    })
    await this._ipfs.pubsub.publish(this.queryTopic, serialize(query));
  }

  public async load(amount?: number, opts?: {}): Promise<void> {
    await super.load(amount, opts);
    if (this.subscribeToQueries) {
      await this._subscribeToQueries();
    }
  }

  public async close(): Promise<void> {
    await this._ipfs.pubsub.unsubscribe(this.queryTopic);
    this._subscribed = false;
    await super.close();
  }

  async _subscribeToQueries(): Promise<void> {
    if (this._subscribed) {
      return
    }

    await this._ipfs.pubsub.subscribe(this.queryTopic, async (msg: Message) => {
      try {
        let query = deserialize(Buffer.from(msg.data), QueryRequestV0);
        let filters: (Query | ((v: any) => boolean))[] = query.queries;
        let results = this.query(
          doc =>
            filters?.length > 0 ? filters.map(f => {
              if (f instanceof Query) {
                return f.apply(doc)
              }
              else {
                return (f as ((v: any) => boolean))(doc)
              }
            }).reduce((prev, current) => prev && current) : true
        )

        if (query.sort) {
          const resolveField = (obj: any) => {
            let v = obj;
            for (let i = 0; i < query.sort.fieldPath.length; i++) {
              v = v[query.sort.fieldPath[i]]
            }
            return v
          }
          let direction = 1;
          if (query.sort.direction == SortDirection.Descending) {
            direction = -1;
          }
          results.sort((a, b) => {
            const af = resolveField(a)
            const bf = resolveField(b)
            if (af < bf) {
              return -direction;
            }
            else if (af > bf) {
              return direction;
            }
            return 0;
          })
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
        await this._ipfs.pubsub.publish(
          query.getResponseTopic(this.queryTopic),
          bytes
        )
      } catch (error) {
        console.error(error)
      }
    })
    this._subscribed = true;
  }

  get queryTopic() {
    if (!this.address) {
      throw new Error("Not initialized");
    }

    return this.address + '/query';
  }


  public batchPut(docs: T[], onProgressCallback) {
    const mapper = (doc, idx) => {
      return this._addOperationBatch(
        {
          op: 'PUT',
          key: asString(doc[this.options.indexBy]),
          value: doc
        },
        true,
        idx === docs.length - 1,
        onProgressCallback
      )
    }

    return pMap(docs, mapper, { concurrency: 1 })
      .then(() => this.saveSnapshot())
  }

  public put(doc: T, options = {}) {
    if (!doc[this.options.indexBy]) { throw new Error(`The provided document doesn't contain field '${this.options.indexBy}'`) }
    return this._addOperation({
      op: 'PUT',
      key: asString(doc[this.options.indexBy]),
      value: bs58.encode(serialize(doc)),
    }, options)
  }

  public putAll(docs: T[], options = {}) {
    if (!(Array.isArray(docs))) {
      docs = [docs]
    }
    if (!(docs.every(d => d[this.options.indexBy]))) { throw new Error(`The provided document doesn't contain field '${this.options.indexBy}'`) }
    return this._addOperation({
      op: 'PUTALL',
      docs: docs.map((value) => ({
        key: asString(value[this.options.indexBy]),
        value: bs58.encode(serialize(value))
      }))
    }, options)
  }

  del(key, options = {}) {
    if (!this._index.get(key)) { throw new Error(`No entry with key '${key}' in the database`) }

    return this._addOperation({
      op: 'DEL',
      key: asString(key),
      value: null
    }, options)
  }
}


