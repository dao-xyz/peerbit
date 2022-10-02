import { Entry } from "@dao-xyz/ipfs-log-entry"

export class EntryIndex<T> {
  _cache: { [key: string]: Entry<T> }
  constructor(entries = {}) {
    this._cache = entries
  }

  set(k: string, v: Entry<T>) {

    this._cache[k] = v
  }

  get(k: string): Entry<T> {
    return this._cache[k]
  }

  delete(k: string) {
    return delete this._cache[k]
  }

  add(newItems: { [key: string]: Entry<T> }) {
    this._cache = Object.assign(this._cache, newItems)
  }

  get length(): number {
    return Object.values(this._cache).length
  }
}

