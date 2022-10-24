import { DeleteOperation, DocumentIndex, IndexedValue, Operation, PutOperation, encoding } from './document-index'
import { Constructor, field, serialize, variant } from '@dao-xyz/borsh';
import { asString } from './utils.js';
import { DocumentQueryRequest, FieldQuery, FieldStringMatchQuery, Result, ResultWithSource, SortDirection, FieldByteMatchQuery, FieldBigIntCompareQuery, Compare, Query, MemoryCompareQuery, DSearchInitializationOptions, QueryType } from '@dao-xyz/peerbit-dsearch';
import { BinaryPayload } from '@dao-xyz/peerbit-bpayload';
import { arraysEqual } from '@dao-xyz/peerbit-borsh-utils';
import { Store } from '@dao-xyz/peerbit-store';
import { DSearch } from '@dao-xyz/peerbit-dsearch';
import { BORSH_ENCODING, CanAppend, Encoding, EncryptionTemplateMaybeEncrypted, Entry, Payload } from '@dao-xyz/ipfs-log';
import { CanOpenSubPrograms, ComposableProgram, Program, ProgramOwner } from '@dao-xyz/peerbit-program';
import { CanRead } from '@dao-xyz/peerbit-dquery';

const replaceAll = (str: string, search: any, replacement: any) => str.toString().split(search).join(replacement)
export class OperationError extends Error {
  constructor(message?: string) {
    super(message);
  }
}
@variant([0, 6])
export class DDocs<T extends BinaryPayload> extends ComposableProgram {

  @field({ type: Store })
  store: Store<Operation<T>>

  @field({ type: 'string' })
  indexBy: string;

  @field({ type: DSearch })
  search: DSearch<Operation<T>>

  @field({ type: 'bool' })
  canEdit: boolean; // "Can I overwrite a document?"

  _clazz?: Constructor<T>;

  _index: DocumentIndex<T>;
  _valueEncoding: Encoding<T>

  _optionCanAppend?: CanAppend<Operation<T>>

  constructor(properties: {
    name?: string,
    indexBy: string,
    search: DSearch<Operation<T>>
    canEdit?: boolean
  }) {
    super(properties)
    if (properties) {
      this.store = new Store(properties);
      this.indexBy = properties.indexBy;
      this.search = properties.search;
      this.canEdit = properties.canEdit || false
    }
    this._index = new DocumentIndex();
  }


  async setup(options: { type: Constructor<T>, canRead?: CanRead, canAppend?: CanAppend<Operation<T>> }) {

    this._clazz = options.type;
    this._valueEncoding = BORSH_ENCODING(this._clazz);
    this._index.init(this._clazz);
    this.store.setup({ encoding: BORSH_ENCODING(Operation), canAppend: this.canAppend.bind(this), onUpdate: this._index.updateIndex.bind(this._index) })
    if (options.canAppend) {
      this._optionCanAppend = options.canAppend
    }

    await this.search.setup({ context: { address: () => this.parentProgram.address }, canRead: options.canRead, queryHandler: this.queryHandler.bind(this) });
  }
  async canAppend(entry: Entry<Operation<T>>): Promise<boolean> {
    const l0 = await this._canAppend(entry);
    if (!l0) {
      return false;
    }

    if (this._optionCanAppend && !await this._optionCanAppend(entry)) {
      return false;
    }
    return true;
  }

  async _canAppend(entry: Entry<Operation<T>>): Promise<boolean> {

    const operation = await entry.getPayloadValue();

    const pointsToHistory = (history: Entry<Operation<T>>) => {
      // make sure nexts only points to this document at some point in history
      let current = history;
      const next = entry.next[0];
      while (current?.hash && next !== current?.hash && current.next.length > 0) {
        current = this.store.oplog.get(current.next[0])
      }
      if (current?.hash === next) {
        return true; // Ok, we are pointing this new edit to some exising point in time of the old document
      }
      return false;
    }

    if (operation instanceof PutOperation) {
      // check nexts
      const putOperation = operation as PutOperation<T>

      const key = (putOperation.getValue(this._valueEncoding))[this.indexBy];
      if (!key) {
        throw new Error("Unexpected")
      }
      const existingDocument = this._index.get(key)
      if (!!existingDocument) {
        if (!this.canEdit) {
          //Key already exist and this instance DDocs can note overrite/edit'
          return false
        }

        if (entry.next.length !== 1) {
          return false;
        }

        return pointsToHistory(existingDocument.entry)
      }
      else {
        if (entry.next.length !== 0) {
          return false;
        }
      }
    }
    else if (operation instanceof DeleteOperation) {
      if (entry.next.length !== 1) {
        return false;
      }
      const existingDocument = this._index.get(operation.key)
      if (!existingDocument) { // already deleted
        return false;
      }
      return pointsToHistory(existingDocument.entry) // references the existing document
    }
    return true;
  }


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
              const operation = payload.getValue(this.store.encoding);
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


  public put(doc: T, options?: {
    skipCanAppendCheck?: boolean;
    onProgressCallback?: (any: any) => void;
    pin?: boolean;
    reciever?: EncryptionTemplateMaybeEncrypted;
  }) {
    if (doc instanceof Program) {
      if (!(this.parentProgram as any as CanOpenSubPrograms).canOpen) {
        throw new Error("Class " + this.parentProgram.constructor.name + " needs to implement CanOpenSubPrograms for this DDocs store to progams")
      }
      doc.programOwner = new ProgramOwner({
        address: this.parentProgram.address
      })
    }


    const key = (doc as any)[this.indexBy];
    if (!key) { throw new Error(`The provided document doesn't contain field '${this.indexBy}'`) }
    const ser = serialize(doc);

    const existingDocument = this._index.get(key)

    return this.store._addOperation(
      new PutOperation(
        {
          key: asString((doc as any)[this.indexBy]),
          data: ser,
          value: doc

        })
      , { nexts: existingDocument ? [existingDocument.entry] : [], ...options })
  }

  /* public putAll(docs: T[], options = {}) {
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
    }), { nexts: [], ...options })
  } */

  del(key: string, options?: {
    skipCanAppendCheck?: boolean;
    onProgressCallback?: (any: any) => void;
    pin?: boolean;
    reciever?: EncryptionTemplateMaybeEncrypted;
  }) {
    const existing = this._index.get(key);
    if (!existing) { throw new Error(`No entry with key '${key}' in the database`) }

    return this.store._addOperation(new DeleteOperation({
      key: asString(key)
    }), { nexts: [existing.entry], ...options })
  }



  public get size(): number {
    return Object.keys(this._index).length
  }

}




