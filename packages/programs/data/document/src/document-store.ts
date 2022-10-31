import { DeleteOperation, DocumentIndex, Operation, PutOperation } from './document-index'
import { Constructor, field, serialize, variant } from '@dao-xyz/borsh';
import { asString } from './utils.js';
import { BinaryPayload } from '@dao-xyz/peerbit-bpayload';
import { Store } from '@dao-xyz/peerbit-store';
import { BORSH_ENCODING, CanAppend, Encoding, EncryptionTemplateMaybeEncrypted, Entry } from '@dao-xyz/ipfs-log';
import { CanOpenSubPrograms, ComposableProgram, Program, ProgramOwner } from '@dao-xyz/peerbit-program';
import { CanRead } from '@dao-xyz/peerbit-query';
import { LogIndex } from '@dao-xyz/peerbit-logindex'

import { AccessError } from '@dao-xyz/peerbit-crypto';

import pino from 'pino'
const logger = pino().child({ module: 'document-store' });

export class OperationError extends Error {
  constructor(message?: string) {
    super(message);
  }
}
@variant([0, 8])
export class Documents<T extends BinaryPayload> extends ComposableProgram {

  @field({ type: Store })
  store: Store<Operation<T>>

  @field({ type: 'bool' })
  canEdit: boolean; // "Can I overwrite a document?"

  @field({ type: DocumentIndex })
  _index: DocumentIndex<T>;

  @field({ type: LogIndex })
  _logIndex: LogIndex

  _clazz?: Constructor<T>;

  _valueEncoding: Encoding<T>

  _optionCanAppend?: CanAppend<Operation<T>>

  constructor(properties: {
    id?: string,
    canEdit?: boolean,
    index: DocumentIndex<T>,
    logIndex?: LogIndex
  }) {
    super(properties)
    if (properties) {
      this.store = new Store(properties);
      this.canEdit = properties.canEdit || false
      this._index = properties.index;
      this._logIndex = properties.logIndex || new LogIndex({ id: properties.id });
    }
  }


  get logIndex(): LogIndex {
    return this._logIndex;
  }
  get index(): DocumentIndex<T> {
    return this._index;
  }
  async setup(options: { type: Constructor<T>, canRead?: CanRead, canAppend?: CanAppend<Operation<T>> }) {

    this._clazz = options.type;
    this._valueEncoding = BORSH_ENCODING(this._clazz);
    if (options.canAppend) {
      this._optionCanAppend = options.canAppend
    }
    await this.store.setup({ encoding: BORSH_ENCODING(Operation), canAppend: this.canAppend.bind(this), onUpdate: this._index.updateIndex.bind(this._index) })
    await this._logIndex.setup({ store: this.store, canRead: options.canRead || (() => Promise.resolve(true)) })
    await this._index.setup({ type: this._clazz, canRead: options.canRead || (() => Promise.resolve(true)) })


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

    try {

      const operation = await entry.getPayloadValue();
      if (operation instanceof PutOperation) {
        // check nexts
        const putOperation = operation as PutOperation<T>

        const key = (putOperation.getValue(this._valueEncoding))[this._index.indexBy];
        if (!key) {
          throw new Error("Expecting document to contained index field")
        }
        const existingDocument = this._index.get(key)
        if (!!existingDocument) {
          if (!this.canEdit) {
            //Key already exist and this instance Documents can note overrite/edit'
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
        } 2
        const existingDocument = this._index.get(operation.key)
        if (!existingDocument) { // already deleted
          return false;
        }
        return pointsToHistory(existingDocument.entry) // references the existing document
      }
    } catch (error) {
      if (error instanceof AccessError) {
        return false; // we cant index because we can not decrypt
      }
      throw error;
    }
    return true;
  }



  public put(doc: T, options?: {
    skipCanAppendCheck?: boolean;
    onProgressCallback?: (any: any) => void;
    pin?: boolean;
    reciever?: EncryptionTemplateMaybeEncrypted;
  }) {
    if (doc instanceof Program) {
      if (!(this.parentProgram as any as CanOpenSubPrograms).canOpen) {
        throw new Error("Class " + this.parentProgram.constructor.name + " needs to implement CanOpenSubPrograms for this Documents store to progams")
      }
      doc.programOwner = new ProgramOwner({
        address: this.parentProgram.address
      })
    }

    const key = (doc as any)[this._index.indexBy];
    if (!key) { throw new Error(`The provided document doesn't contain field '${this._index.indexBy}'`) }
    const ser = serialize(doc);
    const existingDocument = this._index.get(key)

    return this.store._addOperation(
      new PutOperation(
        {
          key: asString((doc as any)[this._index.indexBy]),
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

}




