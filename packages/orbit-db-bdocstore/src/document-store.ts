import { DocumentIndex, LogEntry } from './document-index'
import pMap from 'p-map'
import { Identity } from 'orbit-db-identity-provider';
import { Constructor, field, serialize, variant } from '@dao-xyz/borsh';
import bs58 from 'bs58';
import { asString } from './utils';
import { DocumentQueryRequest, FieldQuery, Query, QueryRequestV0, Result, ResultWithSource, SortDirection } from '@dao-xyz/bquery';
import { IPFS as IPFSInstance } from "ipfs-core-types";
import { QueryStore } from '@dao-xyz/orbit-db-query-store';
import { IQueryStoreOptions } from '@dao-xyz/orbit-db-query-store'
import { BStoreOptions } from '@dao-xyz/orbit-db-bstores'
import { BPayload } from '@dao-xyz/bgenerics';
import OrbitDB from 'orbit-db';

const replaceAll = (str, search, replacement) => str.toString().split(search).join(replacement)

export const BINARY_DOCUMENT_STORE_TYPE = 'bdoc_store';
export type DocumentStoreOptions<T> = IQueryStoreOptions<T, DocumentIndex<T>> & { indexBy?: string, clazz: Constructor<T> };

export type IBinaryDocumentStoreOptions<T> = IQueryStoreOptions<T, DocumentIndex<T>> & { indexBy?: string, clazz: Constructor<T> };

@variant([0, 0])
export class BinaryDocumentStoreOptions<T extends BPayload> extends BStoreOptions<BinaryDocumentStore<T>> {

  @field({ type: 'String' })
  indexBy: string;

  @field({ type: 'String' })
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
  async newStore(address: string, orbitDB: OrbitDB, typeMap: { [key: string]: Constructor<any> }, options: IBinaryDocumentStoreOptions<T>): Promise<BinaryDocumentStore<T>> {
    let clazz = typeMap[this.objectType];
    if (!clazz) {
      throw new Error(`Undefined type: ${this.objectType}`);
    }
    return orbitDB.open<BinaryDocumentStore<T>>(address, { ...options, ...{ clazz, create: true, type: BINARY_DOCUMENT_STORE_TYPE, indexBy: this.indexBy } } as DocumentStoreOptions<T>)
  }


  get identifier(): string {
    return BINARY_DOCUMENT_STORE_TYPE
  }
}

const defaultOptions = <T>(options: IBinaryDocumentStoreOptions<T>): IBinaryDocumentStoreOptions<T> => {
  if (!options["indexBy"]) Object.assign(options, { indexBy: '_id' })
  if (!options.Index) Object.assign(options, { Index: DocumentIndex })
  return options
}

export class BinaryDocumentStore<T extends BPayload> extends QueryStore<T, DocumentIndex<T>, IBinaryDocumentStoreOptions<T>> {

  _type: string = undefined;
  constructor(ipfs: IPFSInstance, id: Identity, dbname: string, options: IBinaryDocumentStoreOptions<T>) {
    super(ipfs, id, dbname, defaultOptions(options))
    this._type = BINARY_DOCUMENT_STORE_TYPE;
    this._index.init(this.options.clazz);
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



  public async load(amount?: number, opts?: {}): Promise<void> {
    await super.load(amount, opts);
  }

  public async close(): Promise<void> {
    await super.close();
  }



  queryDocuments(mapper: ((doc: T) => boolean), options: { fullOp?: boolean } = {}): T[] | LogEntry<T>[] {
    // Whether we return the full operation data or just the db value
    const fullOp = options.fullOp || false
    const getValue: (value: T | LogEntry<T>) => T = fullOp ? (value: LogEntry<T>) => value.payload.value : (value: T) => value
    return Object.keys(this.index._index)
      .map((e) => this.index.get(e, fullOp))
      .filter((doc) => mapper(getValue(doc))) as T[] | LogEntry<T>[]
  }

  queryHandler(query: QueryRequestV0): Promise<Result[]> {
    const documentQuery = query.type as DocumentQueryRequest;

    let filters: FieldQuery[] = documentQuery.queries.filter(q => q instanceof FieldQuery) as any;
    let results = this.queryDocuments(
      doc =>
        filters?.length > 0 ? filters.map(f => {
          if (f instanceof FieldQuery) {
            return f.apply(doc)
          }
          else {
            throw new Error("Unsupported query type")
          }
        }).reduce((prev, current) => prev && current) : true
    ) as T[];

    if (documentQuery.sort) {
      const resolveField = (obj: any) => {
        let v = obj;
        for (let i = 0; i < documentQuery.sort.fieldPath.length; i++) {
          v = v[documentQuery.sort.fieldPath[i]]
        }
        return v
      }
      let direction = 1;
      if (documentQuery.sort.direction == SortDirection.Descending) {
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

  public get size(): number {
    return Object.keys(this.index._index).length
  }
}

OrbitDB.addDatabaseType(BINARY_DOCUMENT_STORE_TYPE, BinaryDocumentStore as any)



