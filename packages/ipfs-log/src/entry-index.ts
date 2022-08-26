import { Entry } from "@dao-xyz/ipfs-log-entry"

export class EntryIndex<T> {
  _cache: any
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

  add(newItems) {
    this._cache = Object.assign(this._cache, newItems)
  }

  get length() {
    return Object.values(this._cache).length
  }
}

