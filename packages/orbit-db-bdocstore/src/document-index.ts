import { Constructor, deserialize } from "@dao-xyz/borsh";
import { Payload } from "./payload";
import bs58 from 'bs58';

export class DocumentIndex<T> {
  _index: { [key: string]: { payload: Payload } };
  clazz: Constructor<T>
  constructor() {
    this._index = {}
  }
  init(clazz: Constructor<T>) {
    this.clazz = clazz;
  }

  get(key, fullOp = false): { payload: Payload } {
    return fullOp
      ? this._index[key]
      : this._index[key] ? this._index[key].payload.value : null
  }

  updateIndex(oplog, onProgressCallback) {
    if (!this.clazz) {
      throw new Error("Not initialized");
    }
    const reducer = (handled, item, idx) => {
      if (item.payload.op === 'PUTALL' && item.payload.docs[Symbol.iterator]) {
        for (const doc of item.payload.docs) {
          if (doc && handled[doc.key] !== true) {
            handled[doc.key] = true
            this._index[doc.key] = {
              payload: {
                op: 'PUT',
                key: doc.key,
                value: this.deserializeOrPass(doc.value)
              }
            }
          }
        }
      } else if (handled[item.payload.key] !== true) {
        handled[item.payload.key] = true
        if (item.payload.op === 'PUT') {
          item.payload.value = this.deserializeOrPass(item.payload.value)
          this._index[item.payload.key] = item
        } else if (item.payload.op === 'DEL') {
          delete this._index[item.payload.key]
        }
      }
      if (onProgressCallback) onProgressCallback(item, idx)
      return handled
    }

    try {
      oplog.values
        .slice()
        .reverse()
        .reduce(reducer, {})
    } catch (error) {
      console.error(JSON.stringify(error))
    }
  }
  deserializeOrPass(value: string | T): T {
    return typeof value === 'string' ? deserialize(bs58.decode(value), this.clazz) : value
  }

}


