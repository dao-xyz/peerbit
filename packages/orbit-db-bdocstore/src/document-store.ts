import Store from 'orbit-db-store'
import { DocumentIndex } from './document-index'
import pMap from 'p-map'
import { IPFS as IPFSInstance } from 'ipfs';
import { Identity } from 'orbit-db-identity-provider';
import { Constructor, serialize } from '@dao-xyz/borsh';
import bs58 from 'bs58';
import { asString } from './utils';
const replaceAll = (str, search, replacement) => str.toString().split(search).join(replacement)

export const BINARY_DOCUMENT_STORE_TYPE = 'bdocstore';
const defaultOptions = (options: IStoreOptions): any => {
  if (!options["indexBy"]) Object.assign(options, { indexBy: '_id' })
  if (!options.Index) Object.assign(options, { Index: DocumentIndex })
  return options;
}
export class BinaryDocumentStore<T> extends Store<T, DocumentIndex<T>> {

  _type: string = undefined;
  constructor(ipfs: IPFSInstance, id: Identity, dbname: string, options: IStoreOptions & { indexBy?: string, clazz: Constructor<T> }) {
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

  public query(mapper: ((doc: T) => boolean), options: { fullOp?: boolean } = {}): T[] | { payload: Payload<T> }[] {
    // Whether we return the full operation data or just the db value
    const fullOp = options.fullOp || false
    const getValue: (value: T | { payload: Payload<T> }) => T = fullOp ? (value: { payload: Payload<T> }) => value.payload.value : (value: T) => value
    return Object.keys(this._index._index)
      .map((e) => this._index.get(e, fullOp))
      .filter((doc) => mapper(getValue(doc))) as T[] | { payload: Payload<T> }[]
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


