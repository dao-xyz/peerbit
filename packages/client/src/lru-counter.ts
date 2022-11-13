import LRU from "lru-cache";
export class LRUCounter {
  _lru: LRU<string, number>;
  constructor(lru: LRU<string, number>) {
    this._lru = lru;
  }

  _increment: Promise<number>;
  async increment(key: string) {
    await this._increment;
    return (this._increment = new Promise((resolve, rj) => {
      let value = this._lru.get(key);
      if (!value) {
        value = 0;
      }
      value++;
      this._lru.set(key, value);
      resolve(value);
    }));
  }

  get(key: string) {
    return this._lru.get(key);
  }

  async clear(key?: string) {
    if (key) {
      await this._increment;
      this._lru.delete(key);
    } else {
      this._lru.clear();
    }
  }
}
