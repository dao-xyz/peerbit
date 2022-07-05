import { Constructor, deserialize } from "@dao-xyz/borsh";
import bs58 from 'bs58';


interface Payload<T> {
  op?: string;
  key?: string;
  value: T
}

interface LogEntry<T> {
  payload: Payload<T>
}
export class FeedIndex<T> {
  _index: { [key: string]: LogEntry<T> };
  clazz: Constructor<T>
  constructor() {
    this._index = {}
  }
  init(clazz: Constructor<T>) {
    this.clazz = clazz;
  }

  get(key?: any, fullOp?: boolean): (T | LogEntry<T>)[] {
    if (key) {
      return
    }
    return Object.keys(this._index).map((f) => this._index[f])
  }

  async updateIndex(oplog) {
    if (!this.clazz) {
      throw new Error("Not initialized");
    }

    this._index = {}
    oplog.values.reduce((handled, item) => {
      if (!handled.includes(item.hash)) {
        handled.push(item.hash)
        if (item.payload.op === 'ADD') {
          item = this.deserializeOrItem(item)
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
  deserializeOrItem(item: LogEntry<T | string>): LogEntry<T> {
    if (typeof item.payload.value !== 'string')
      return item as LogEntry<T>

    const newItem = { ...item, payload: { ...item.payload } };
    newItem.payload.value = this.deserializeOrPass(newItem.payload.value)
    return newItem as LogEntry<T>;
  }

}


