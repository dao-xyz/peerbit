import { Constructor, deserialize } from "@dao-xyz/borsh";
import bs58 from 'bs58';
import { IdentityAsJson } from "@dao-xyz/orbit-db-identity-provider";
import { asString, ToStringable } from "./utils";

export interface LogEntry<T> { identity: IdentityAsJson, payload: Payload<T> }
export interface Payload<T> {
  op?: string;
  key?: string;
  value: T
}

export class DocumentIndex<T> {
  _index: { [key: string]: LogEntry<T> };
  clazz: Constructor<T>

  constructor() {
    this._index = {}
  }

  init(clazz: Constructor<T>) {
    this.clazz = clazz;
  }

  get(key: ToStringable, fullOp = false): (LogEntry<T> | T) {
    let stringKey = asString(key);
    return fullOp
      ? this._index[stringKey]
      : this._index[stringKey] ? this._index[stringKey].payload.value : null
  }

  async updateIndex(oplog) {
    if (!this.clazz) {
      throw new Error("Not initialized");
    }
    const reducer = (handled, item, idx) => {
      let key = asString(item.payload.key);
      if (item.payload.op === 'PUTALL' && item.payload.docs[Symbol.iterator]) {
        for (const doc of item.payload.docs) {
          if (doc && handled[doc.key] !== true) {
            handled[doc.key] = true
            this._index[doc.key] = {
              payload: {
                op: 'PUT',
                key: asString(doc.key),
                value: this.deserializeOrPass(doc.value)
              },
              identity: item.identity
            }
          }
        }
      } else if (handled[key] !== true) {
        handled[key] = true
        if (item.payload.op === 'PUT') {
          this._index[key] = this.deserializeOrItem(item)
        } else if (item.payload.op === 'DEL') {
          delete this._index[key]
        }
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



