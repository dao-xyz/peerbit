import Store from 'orbit-db-store'
import { DocumentIndex } from './document-index'
import pMap from 'p-map'
import Readable from 'readable-stream'
import { IPFS as IPFSInstance } from 'ipfs';
import { Identity } from 'orbit-db-identity-provider';
import { Constructor, serialize } from '@dao-xyz/borsh';
import bs58 from 'bs58';
const replaceAll = (str, search, replacement) => str.toString().split(search).join(replacement)

export const BINARY_DOCUMENT_STORE_TYPE = 'bdocstore';
const defaultOptions = (options: IStoreOptions): any => {
  if (!options["indexBy"]) Object.assign(options, { indexBy: '_id' })
  if (!options.Index) Object.assign(options, { Index: DocumentIndex })
  return options;
}
export class BinaryDocumentStore<T> extends Store {

  _type: string = undefined;
  constructor(ipfs: IPFSInstance, id: Identity, dbname: string, options: IStoreOptions & { indexBy?: string, clazz: Constructor<T> }) {
    super(ipfs, id, dbname, defaultOptions(options))
    this._type = BINARY_DOCUMENT_STORE_TYPE;
    (this._index as DocumentIndex<T>).init(this.options.clazz);

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
    const mapper = e => this._index.get(e)
    const filter = e => caseSensitive
      ? e.indexOf(key) !== -1
      : search(e)

    return Object.keys(this._index._index)
      .filter(filter)
      .map(mapper)
  }

  public query(mapper: ((doc: T) => boolean), options = {}): T[] {
    // Whether we return the full operation data or just the db value
    const fullOp = options["fullOp"] || false

    return Object.keys(this._index._index)
      .map((e) => this._index.get(e, fullOp))
      .filter(mapper)
  }

  public batchPut(docs: T[], onProgressCallback) {
    const mapper = (doc, idx) => {
      return this._addOperationBatch(
        {
          op: 'PUT',
          key: doc[this.options.indexBy],
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
      key: doc[this.options.indexBy],
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
        key: value[this.options.indexBy],
        value: bs58.encode(serialize(value))
      }))
    }, options)
  }

  del(key, options = {}) {
    if (!this._index.get(key)) { throw new Error(`No entry with key '${key}' in the database`) }

    return this._addOperation({
      op: 'DEL',
      key: key,
      value: null
    }, options)
  }
}

