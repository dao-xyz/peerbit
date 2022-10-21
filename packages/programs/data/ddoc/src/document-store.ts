import { DeleteOperation, DocumentIndex, IndexedValue, Operation, PutAllOperation, PutOperation } from './document-index'
import { Constructor, field, serialize, variant } from '@dao-xyz/borsh';
import { asString } from './utils.js';
import { DocumentQueryRequest, FieldQuery, FieldStringMatchQuery, Result, ResultWithSource, SortDirection, FieldByteMatchQuery, FieldBigIntCompareQuery, Compare, Query, MemoryCompareQuery, DSearchInitializationOptions, QueryType } from '@dao-xyz/peerbit-dsearch';
import { BinaryPayload } from '@dao-xyz/peerbit-bpayload';
import { arraysEqual } from '@dao-xyz/peerbit-borsh-utils';
import { Store, IInitializationOptions } from '@dao-xyz/peerbit-dstore';
import { DSearch } from '@dao-xyz/peerbit-dsearch';
import { BORSH_ENCODING, CanAppend, Identity, Payload } from '@dao-xyz/ipfs-log';
import { IPFS } from 'ipfs-core-types';
import { SignatureWithKey } from '@dao-xyz/peerbit-crypto';
import { Program, ProgramInitializationOptions, RootProgram } from '@dao-xyz/peerbit-program';

const replaceAll = (str: string, search: any, replacement: any) => str.toString().split(search).join(replacement)

const encoding = BORSH_ENCODING(Operation);

@variant([0, 6])
export class DDocs<T extends BinaryPayload> extends Program {

  @field({ type: Store })
  store: Store<Operation<T>>

  @field({ type: 'string' })
  indexBy: string;

  @field({ type: DSearch })
  search: DSearch<Operation<T>>


  _clazz?: Constructor<T>;

  _index: DocumentIndex<T>;


  constructor(properties: {
    name?: string,
    indexBy: string,
    search: DSearch<Operation<T>>
  }) {
    super(properties)
    if (properties) {
      this.store = new Store(properties);
      this.indexBy = properties.indexBy;
      this.search = properties.search;
    }
    this._index = new DocumentIndex();
  }


  async setup(options: { type: Constructor<T>, canRead?(key: SignatureWithKey): Promise<boolean>, canAppend?: CanAppend<Operation<T>> }) {

    this._clazz = options.type;
    this._index.init(this._clazz);
    this.store.onUpdate = this._index.updateIndex.bind(this._index);
    if (options.canAppend) {
      this.store.canAppend = options.canAppend
    }
    await this.search.setup({ context: { address: () => this.address }, canRead: options.canRead, queryHandler: this.queryHandler.bind(this) });
    return this;
  }

  /* get encoding(): Encoding<Operation<T>> {
    return;
  } */

  public get(key: any, caseSensitive = false): IndexedValue<T>[] {
    key = key.toString()
    const terms = key.split(' ')
    key = terms.length > 1 ? replaceAll(key, '.', ' ').toLowerCase() : key.toLowerCase()

    const search = (e: string) => {
      if (terms.length > 1) {
        return replaceAll(e, '.', ' ').toLowerCase().indexOf(key) !== -1
      }
      return e.toLowerCase().indexOf(key) !== -1
    }
    const mapper = (e: string) => this._index.get(e)
    const filter = (e: string) => caseSensitive
      ? e.indexOf(key) !== -1
      : search(e)

    const keys = Object.keys(this._index._index);
    return keys.filter(filter)
      .map(mapper)
  }

  _queryDocuments(filter: ((doc: IndexedValue<T>) => boolean)): IndexedValue<T>[] {
    // Whether we return the full operation data or just the db value
    return Object.keys(this._index._index)
      .map((e) => this._index.get(e))
      .filter((doc) => filter(doc))
  }

  queryHandler(query: QueryType): Promise<Result[]> {
    if (query instanceof DocumentQueryRequest) {
      let queries: Query[] = query.queries
      let results = this._queryDocuments(
        doc =>
          queries?.length > 0 ? queries.map(f => {
            if (f instanceof FieldQuery) {
              let fv: any = doc.value;
              for (let i = 0; i < f.key.length; i++) {
                fv = fv[f.key[i]];
              }

              if (f instanceof FieldStringMatchQuery) {
                if (typeof fv !== 'string')
                  return false;
                return fv.toLowerCase().indexOf(f.value.toLowerCase()) !== -1;
              }
              if (f instanceof FieldByteMatchQuery) {
                if (!Array.isArray(fv))
                  return false;
                return arraysEqual(fv, f.value)
              }
              if (f instanceof FieldBigIntCompareQuery) {
                let value: bigint | number = fv;

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
              return false
            }
            else if (f instanceof MemoryCompareQuery) {
              const payload = doc.entry._payload.decrypted.getValue(Payload);
              const operation = payload.getValue(encoding);
              if (operation instanceof PutOperation) {
                const bytes = operation.data;
                for (const compare of f.compares) {
                  const offsetn = Number(compare.offset); // TODO type check

                  for (let b = 0; b < compare.bytes.length; b++) {
                    if (bytes[offsetn + b] !== compare.bytes[b]) {
                      return false;
                    }
                  }
                }
              }
              else {
                // TODO add implementations for PutAll
                return false;
              }
              return true;
            }
            else {
              throw new Error("Unsupported query type")
            }
          }).reduce((prev, current) => prev && current) : true
      ).map(x => x.value);

      if (query.sort) {
        const sort = query.sort;
        const resolveField = (obj: T) => {
          let v = obj;
          for (let i = 0; i < sort.key.length; i++) {
            v = (v as any)[sort.key[i]]
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
      // TODO check conversions
      if (query.offset) {
        results = results.slice(Number(query.offset));
      }

      if (query.size) {
        results = results.slice(0, Number(query.size));
      }
      return Promise.resolve(results.map(r => new ResultWithSource({
        source: r
      })));
    }


    // TODO diagnostics for other query types
    return Promise.resolve([]);

  }


  /* TODO  
   public batchPut(docs: T[]) {
      const mapper = (doc, idx) => {
        return this._addOperationBatch(
          {
            op: 'PUT',
            key: asString(doc[this.indexBy]),
            value: doc
          },
          true,
          idx === docs.length - 1
        )
      }
  
      return pMap(docs, mapper, { concurrency: 1 })
        .then(() => this.saveSnapshot())
    } */

  public put(doc: T, options = {}) {
    if (!(doc as any)[this.indexBy]) { throw new Error(`The provided document doesn't contain field '${this.indexBy}'`) }
    const ser = serialize(doc);
    return this.store._addOperation(
      new PutOperation(
        {
          key: asString((doc as any)[this.indexBy]),
          data: ser,
          value: doc

        })
      , { nexts: [], ...options, encoding })
  }

  public putAll(docs: T[], options = {}) {
    if (!(Array.isArray(docs))) {
      docs = [docs]
    }
    if (!(docs.every(d => (d as any)[this.indexBy]))) { throw new Error(`The provided document doesn't contain field '${this.indexBy}'`) }
    return this.store._addOperation(new PutAllOperation({
      docs: docs.map((value) => new PutOperation({
        key: asString((value as any)[this.indexBy]),
        data: serialize(value),
        value
      }))
    }), { nexts: [], ...options, encoding })
  }

  del(key: string, options = {}) {
    const existing = this._index.get(key);
    if (!existing) { throw new Error(`No entry with key '${key}' in the database`) }

    return this.store._addOperation(new DeleteOperation({
      key: asString(key)
    }), { nexts: [existing.entry], ...options, encoding })
  }



  public get size(): number {
    return Object.keys(this._index).length
  }
  /*   clone(newName: string): DDocs<T> {
      return new DDocs<T>({
        accessController: this.accessController.clone(newName),
        indexBy: this.indexBy,
        objectType: this.objectType,
        name: newName,
        queryRegion: this.queryRegion
      })
    }
   */
}




