import { Query, Key, Pair, KeyQuery } from 'interface-datastore'
import { BaseDatastore, Errors } from 'datastore-core'
import filter from 'it-filter'
import map from 'it-map'
import take from 'it-take'
import sort from 'it-sort'
import { Level } from 'level'
import { OpenOptions, DatabaseOptions } from 'level'
import { AbstractBatchOperation, AbstractLevel } from "abstract-level";
import { waitFor } from '@dao-xyz/peerbit-time';


export type LevelBatchOptions = {
  interval?: number;
  onError?: (error: any) => void;
};
export type LevelOptions = DatabaseOptions<string, Uint8Array> & OpenOptions & { batch?: LevelBatchOptions | boolean };

/**
 * A datastore backed by leveldb
 */
export class LazyLevelDatastore extends BaseDatastore {
  db: AbstractLevel<any, any, any>;
  opts: DatabaseOptions<string, Uint8Array> & OpenOptions;

  _batchOptions?: { interval: number; onError?: (e: any) => void };
  _interval: any;
  _txQueue?: AbstractBatchOperation<
    AbstractLevel<any, string, Uint8Array>,
    string,
    Uint8Array
  >[];
  _tempStore?: Map<string, Uint8Array>;
  _txPromise?: Promise<any>;

  constructor(path: string | AbstractLevel<any, string, Uint8Array>, opts: LevelOptions = { batch: { interval: 300 } }) {
    super()

    /** @type {LevelDb} */
    this.db = typeof path === 'string'
      ? new Level<string, Uint8Array>(path, {
        ...opts,
        keyEncoding: 'utf8',
        valueEncoding: 'view'
      })
      : path

    /** @type {import('level').OpenOptions} */
    this.opts = {
      createIfMissing: true,
      compression: false, // same default as go
      ...opts
    }
    if (this._batchOptions) {
      this._txQueue = [];
      this._tempStore = new Map();

      this._interval = setInterval(() => {
        if (this.db.status === "open" && this._txQueue && this._txQueue.length > 0) {
          try {
            const arr = this._txQueue.splice(
              0,
              this._txQueue.length
            );
            if (arr?.length > 0) {
              this._txPromise = (
                this._txPromise
                  ? this._txPromise
                  : Promise.resolve()
              ).finally(() => {
                return this.db.batch(arr).then(() => {
                  arr.forEach((v) => {
                    if (v.type === "put") {
                      this._tempStore!.delete(v.key);
                    } else if (v.type === "del") {
                      this._tempStore!.delete(v.key);
                    }
                  });
                });
              });
            }
          } catch (error) {
            this._batchOptions?.onError &&
              this._batchOptions.onError(error);
          }
        }
      }, this._batchOptions.interval);
    }

  }

  async open() {
    try {
      await this.db.open(this.opts)
    } catch (err: any) {
      throw Errors.dbOpenFailedError(err)
    }
  }


  async put(key: Key, value: Uint8Array) {
    const keyStr = key.toString();
    try {
      if (this._batchOptions) {
        this._tempStore!.set(keyStr, value);
        this._txQueue!.push({
          type: "put",
          key: keyStr,
          value: value
        });
      }
      else {
        await this.db.put(keyStr, value)
      }
    } catch (err: any) {
      throw Errors.dbWriteFailedError(err)
    }
  }

  /**
   * @param {Key} key
   * @returns {Promise<Uint8Array>}
   */
  async get(key: Key) {
    let data
    try {
      const keyStr = key.toString();
      data = (this._tempStore && this._tempStore.get(keyStr)) || await this.db.get(keyStr, { valueEncoding: 'view' })
    } catch (err: any) {
      if (err.notFound) throw Errors.notFoundError(err)
      throw Errors.dbWriteFailedError(err)
    }
    return data
  }

  /**
   * @param {Key} key
   * @returns {Promise<boolean>}
   */
  async has(key: Key) {
    try {
      await this.db.get(key.toString())
    } catch (err: any) {
      if (err.notFound) return false
      throw err
    }
    return true
  }

  /**
   * @param {Key} key
   * @returns {Promise<void>}
   */
  async delete(key: Key) {
    try {
      const keyStr = key.toString();
      if (this._batchOptions) {
        this._tempStore!.delete(keyStr);
        this._txQueue!.push({ type: "del", key: keyStr });
      }
      else {
        await this.db.del(key.toString())
      }
    } catch (err: any) {
      throw Errors.dbDeleteFailedError(err)
    }
  }

  async close() {
    if (this._batchOptions) {
      await waitFor(() => this._txQueue!.length === 0)
      clearInterval(this._interval);
      this._interval = undefined;
      this._tempStore!.clear();
    }
    return this.db && this.db.close()
  }

  /**
   * @returns {Batch}
   */
  batch() {
    /** @type {Array<{ type: 'put', key: string, value: Uint8Array; } | { type: 'del', key: string }>} */
    const ops: { type: any, key: string, value?: Uint8Array }[] = []
    return {
      put: (key: Key, value: Uint8Array) => {
        ops.push({
          type: 'put',
          key: key.toString(),
          value: value
        })
      },
      delete: (key: Key) => {
        ops.push({
          type: 'del',
          key: key.toString()
        })
      },
      commit: () => {
        return this.db.batch(ops)
      }
    }
  }


  query(q: Query) {
    let it = this._query({
      values: true,
      prefix: q.prefix
    })

    if (Array.isArray(q.filters)) {
      it = q.filters.reduce((it, f) => filter(it, f), it)
    }

    if (Array.isArray(q.orders)) {
      it = q.orders.reduce((it, f) => sort(it, f), it)
    }

    const { offset, limit } = q
    if (offset) {
      let i = 0
      it = filter(it, () => i++ >= offset)
    }

    if (limit) {
      it = take(it, limit)
    }

    return it
  }


  queryKeys(q: KeyQuery) {
    let it = map(this._query({
      values: false,
      prefix: q.prefix
    }), ({ key }) => key)

    if (Array.isArray(q.filters)) {
      it = q.filters.reduce((it, f) => filter(it, f), it)
    }

    if (Array.isArray(q.orders)) {
      it = q.orders.reduce((it, f) => sort(it, f), it)
    }

    const { offset, limit } = q
    if (offset) {
      let i = 0
      it = filter(it, () => i++ >= offset)
    }

    if (limit) {
      it = take(it, limit)
    }

    return it
  }

  /**
   * @param {object} opts
   * @param {boolean} opts.values
   * @param {string} [opts.prefix]
   * @returns {AsyncIterable<Pair>}
   */
  _query(opts: any): AsyncIterable<Pair> {
    /** @type {import('level').IteratorOptions<string, Uint8Array>} */
    const iteratorOpts: { keys: boolean, keyEncoding: string, values: any, gte?: string, lt?: string } = {
      keys: true,
      keyEncoding: 'buffer',
      values: opts.values
    }

    // Let the db do the prefix matching
    if (opts.prefix != null) {
      const prefix = opts.prefix.toString()
      // Match keys greater than or equal to `prefix` and
      iteratorOpts.gte = prefix
      // less than `prefix` + \xFF (hex escape sequence)
      iteratorOpts.lt = prefix + '\xFF'
    }

    const iterator = this.db.iterator(iteratorOpts)

    if (iterator[Symbol.asyncIterator]) {
      return levelIteratorToIterator(iterator)
    }

    // @ts-expect-error support older level
    if (iterator.next != null && iterator.end != null) {
      // @ts-expect-error support older level
      return oldLevelIteratorToIterator(iterator)
    }

    throw new Error('Level returned incompatible iterator')
  }
}

/**
 * @param {import('level').Iterator<LevelDb, string, Uint8Array>} li - Level iterator
 * @returns {AsyncIterable<Pair>}
 */
async function* levelIteratorToIterator(li: any) {
  for await (const [key, value] of li) {
    yield { key: new Key(key, false), value }
  }

  await li.close()
}

/**
 * @typedef {object} LevelIterator
 * @property {(cb: (err: Error, key: string | Uint8Array | null, value: any)=> void)=>void} next
 * @property {(cb: (err: Error) => void) => void } end
 */

/**
 * @param {LevelIterator} li - Level iterator
 * @returns {AsyncIterable<Pair>}
 */
function oldLevelIteratorToIterator(li: any) {
  return {
    [Symbol.asyncIterator]() {
      return {
        next: () => new Promise((resolve, reject) => {
          li.next((err: any, key: string, value: Uint8Array) => {
            if (err) return reject(err)
            if (key == null) {
              return li.end((err: any) => {
                if (err) return reject(err)
                resolve({ done: true, value: undefined })
              })
            }
            resolve({ done: false, value: { key: new Key(key, false), value } })
          })
        }),
        return: () => new Promise((resolve, reject) => {
          li.end((err: any) => {
            if (err) return reject(err)
            resolve({ done: true, value: undefined })
          })
        })
      }
    }
  }
}