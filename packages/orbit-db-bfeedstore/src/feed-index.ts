import { Constructor, deserialize } from "@dao-xyz/borsh";
import bs58 from 'bs58';

export class FeedIndex<T> {
  _index: { [key: string]: LogEntry<T> };
  clazz: Constructor<T>
  constructor() {
    this._index = {}
  }
  init(clazz: Constructor<T>) {
    this.clazz = clazz;
  }

  get(key?: any, fullOp?: boolean): T | LogEntry<T>[] {
    if (key) {
      return
    }
    return Object.keys(this._index).map((f) => this._index[f])
  }

  updateIndex(oplog, onProgressCallback) {
    if (!this.clazz) {
      throw new Error("Not initialized");
    }

    this._index = {}
    oplog.values.reduce((handled, item) => {
      if (!handled.includes(item.hash)) {
        handled.push(item.hash)
        if (item.payload.op === 'ADD') {
          item.payload.value = this.deserializeOrPass(item.payload.value)
          this._index[item.hash] = item
        } else if (item.payload.op === 'DEL') {
          delete this._index[item.payload.value]
        }
      }
      return handled
    }, [])
  }

  deserializeOrPass(value: string | T): T {
    return typeof value === 'string' ? deserialize(bs58.decode(value), this.clazz) : value
  }

}


