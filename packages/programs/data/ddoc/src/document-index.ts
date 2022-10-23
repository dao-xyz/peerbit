import { Constructor, deserialize, field, variant, vec } from "@dao-xyz/borsh";
import { asString, Hashable } from "./utils";
import { BORSH_ENCODING, Encoding, Entry } from "@dao-xyz/ipfs-log";
import { Log } from "@dao-xyz/ipfs-log";
import { UInt8ArraySerializer } from "@dao-xyz/peerbit-borsh-utils";

@variant(0)
export class Operation<T> { }

@variant(0)
export class PutOperation<T> extends Operation<T> {

  @field({ type: 'string' })
  key: string

  @field(UInt8ArraySerializer)
  data: Uint8Array

  _value?: T

  constructor(props?: {
    key: string,
    data: Uint8Array,
    value?: T
  }) {
    super();
    if (props) {
      this.key = props.key;
      this.data = props.data;
      this._value = props.value;
    }
  }

  get value(): T | undefined {
    if (!this._value) {
      throw new Error("Unexpected")
    }
    return this._value;
  }

}

@variant(1)
export class PutAllOperation<T> extends Operation<T> {

  @field({ type: vec(PutOperation) })
  docs: PutOperation<T>[]

  constructor(props?: {
    docs: PutOperation<T>[]
  }) {
    super();
    if (props) {
      this.docs = props.docs;
    }
  }
}

@variant(2)
export class DeleteOperation extends Operation<any> {

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
  entry: Entry<Operation<T>>
}



export class DocumentIndex<T> {
  _index: { [key: string]: IndexedValue<T> };
  clazz: Constructor<T>
  _encoding: Encoding<Operation<T>>

  constructor() {
    this._index = {}
  }

  init(clazz: Constructor<T>) {
    this.clazz = clazz;
    this._encoding = BORSH_ENCODING(Operation)
  }

  get(key: Hashable): IndexedValue<T> {
    let stringKey = asString(key);
    return this._index[stringKey]
  }

  async updateIndex(oplog: Log<Operation<T>>) {
    if (!this.clazz) {
      throw new Error("Not initialized");
    }
    const reducer = (handled: { [key: string]: boolean }, item: Entry<Operation<T>>) => {
      let payload = item.payload.getValue(this._encoding);
      if (payload instanceof PutAllOperation) {
        for (const doc of payload.docs) {
          if (doc && handled[doc.key] !== true) {
            handled[doc.key] = true
            this._index[doc.key] = {
              key: asString(doc.key),
              value: this.deserializeOrPass(doc),
              entry: item
            }
          }
        }
      }
      else if (payload instanceof PutOperation) {
        const key = payload.key;
        if (handled[key] !== true) {
          handled[key] = true
          this._index[key] = {
            entry: item,
            key: payload.key,
            value: this.deserializeOrPass(payload)
          }
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

  deserializeOrPass(value: PutOperation<T>): T {
    if (value._value) {
      return value._value;
    }
    else {
      value._value = deserialize(value.data, this.clazz);
      return value._value;
    }
  }

  deserializeOrItem(entry: Entry<Operation<T>>, operation: PutOperation<T>): IndexedValue<T> {
    const item: IndexedValue<T> = {
      entry,
      key: operation.key,
      value: this.deserializeOrPass(operation)
    }
    return item;

  }
}
