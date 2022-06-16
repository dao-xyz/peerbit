import { Constructor, deserialize } from "@dao-xyz/borsh";
import bs58 from 'bs58';
export class KeyValueIndex<T> {
  _index: { [key: string]: T };
  clazz: Constructor<T>
  constructor() {
    this._index = {}
  }
  init(clazz: Constructor<T>) {
    this.clazz = clazz;
  }

  get(key): T {
    return this._index[key]
  }

  updateIndex(oplog) {
    if (!this.clazz) {
      throw new Error("Not initialized");
    }

    const values = oplog.values
    const handled = {}

    for (let i = values.length - 1; i >= 0; i--) {
      const item = values[i]
      if (handled[item.payload.key]) {
        continue
      }
      handled[item.payload.key] = true
      if (item.payload.op === 'PUT') {
        let buffer = bs58.decode(item.payload.value);
        this._index[item.payload.key] = deserialize(buffer, this.clazz)
        continue
      }
      if (item.payload.op === 'DEL') {
        delete this._index[item.payload.key]
        continue
      }
    }
  }
  get size(): number {
    return Object.keys(this._index._index).length
  }
}

