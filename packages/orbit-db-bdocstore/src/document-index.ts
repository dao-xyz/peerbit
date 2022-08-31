import { Constructor, deserialize, field, variant, vec } from "@dao-xyz/borsh";
import { Identity } from "@dao-xyz/orbit-db-identity-provider";
import { asString, Hashable } from "./utils";
import { Entry } from "@dao-xyz/ipfs-log-entry";
import { Log } from "@dao-xyz/ipfs-log";
import { U8IntArraySerializer } from "@dao-xyz/io-utils";

@variant(0)
export class Operation { }

@variant(0)
export class PutOperation extends Operation {

  @field({ type: 'string' })
  key: string

  @field(U8IntArraySerializer)
  value: Uint8Array

  constructor(props?: {
    key: string,
    value: Uint8Array
  }) {
    super();
    if (props) {
      this.key = props.key;
      this.value = props.value;
    }
  }

}

@variant(1)
export class PutAllOperation extends Operation {

  @field({ type: vec(PutOperation) })
  docs: PutOperation[]

  constructor(props?: {
    docs: PutOperation[]
  }) {
    super();
    if (props) {
      this.docs = props.docs;
    }
  }
}

@variant(2)
export class DeleteOperation extends Operation {

  @field({ type: 'string' })
  key: string

  constructor(props?: {
    key: string
  }) {
    super();
    if (props) {
      this.key = props.key;
    }
  }
}


export interface IndexedValue<T> {
  key: string,
  value: T, // decrypted, decoded
  entry: Entry<Operation>
}



export class DocumentIndex<T> {
  _index: { [key: string]: IndexedValue<T> };
  clazz: Constructor<T>

  constructor() {
    this._index = {}
  }

  init(clazz: Constructor<T>) {
    this.clazz = clazz;
  }

  get(key: Hashable): IndexedValue<T> {
    let stringKey = asString(key);
    return this._index[stringKey]
  }

  async updateIndex(oplog: Log<IndexedValue<T>>) {
    if (!this.clazz) {
      throw new Error("Not initialized");
    }
    const reducer = (handled, item: Entry<Operation>, idx) => {
      let payload = item.payload.value;
      if (payload instanceof PutAllOperation) {
        for (const doc of payload.docs) {
          if (doc && handled[doc.key] !== true) {
            handled[doc.key] = true
            this._index[doc.key] = {
              key: asString(doc.key),
              value: this.deserializeOrPass(doc.value),
              entry: item
            }
          }
        }
      }
      else if (payload instanceof PutOperation) {
        const key = payload.key;
        if (handled[key] !== true) {
          handled[key] = true
          this._index[key] = this.deserializeOrItem(item, payload)
        }
      }
      else if (payload instanceof DeleteOperation) {
        const key = payload.key;
        if (handled[key] !== true) {
          handled[key] = true
          delete this._index[key]

        }
      }
      else {
        // Unknown operation
      }
      return handled
    }

    try {
      oplog.values
        .slice()
        .reverse()
        .reduce(reducer, {})
    } catch (error) {
      console.error(JSON.stringify(error))
      throw error;
    }
  }

  deserializeOrPass(value: Uint8Array | T): T {
    return value instanceof Uint8Array ? deserialize(Buffer.isBuffer(value) ? value : Buffer.from(value), this.clazz) : value
  }

  deserializeOrItem(entry: Entry<Operation>, operation: PutOperation): IndexedValue<T> {
    /* if (typeof item.payload.value !== 'string')
      return item as LogEntry<T> */
    const item: IndexedValue<T> = {
      entry,
      key: operation.key,
      value: this.deserializeOrPass(operation.value)
    }
    return item;
    /* const newItem = { ...item, payload: { ...item.payload } };
    newItem.payload.value = this.deserializeOrPass(newItem.payload.value)
    return newItem as LogEntry<T>; */
  }

}



