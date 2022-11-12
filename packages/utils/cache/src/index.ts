import { serialize, deserialize, Constructor } from '@dao-xyz/borsh';
import { AbstractLevel } from 'abstract-level';
import pino from 'pino'

const logger = pino().child({ module: 'cache' });

export default class Cache<T> {

  _store: AbstractLevel<any, any, any>;
  constructor(store: AbstractLevel<any, any, any>) {
    this._store = store
  }

  get status() { return this._store.status }

  async close() {
    if (!this._store) return Promise.reject(new Error('No cache store found to close'))
    if (this.status === 'open') {
      await this._store.close()
      return Promise.resolve()
    }
  }

  async open() {
    if (!this._store) return Promise.reject(new Error('No cache store found to open'))
    if (this.status !== 'open') {
      await this._store.open()
      return Promise.resolve()
    }
  }

  async get<T>(key: string): Promise<T | undefined> {
    return new Promise((resolve, reject) => {
      this._store.get(key, (err, value) => {
        if (err) {
          // Ignore error if key was not found
          if (err["status"] !== 404) {
            return reject(err)
          }
          resolve(undefined)
        }
        resolve(value ? JSON.parse(value) : null)
      })
    })
  }

  // Set value in the cache and return the new value
  set(key: string, value: T) {
    return new Promise((resolve, reject) => {
      this._store.put(key, JSON.stringify(value), (err) => {
        if (err) {
          return reject(err)

        }
        logger.debug(`cache: Set ${key} to ${JSON.stringify(value)}`)
        resolve(true)
      })
    })
  }

  async getBinary<B extends T>(key: string, clazz: Constructor<B>): Promise<B | undefined> {
    return new Promise((resolve, reject) => {
      this._store.get(key, { valueEncoding: 'view' }, (err: any, value: string | undefined) => {
        if (err) {
          if (err["status"] !== 404) {
            return reject(err)
          }
        }
        if (!value) {
          resolve(undefined)
          return;
        }
        const buffer = Buffer.isBuffer(value) ? value : Buffer.from(value);
        const der = value ? deserialize(buffer, clazz) : undefined
        resolve(der)
      })
    })
  }

  setBinary<B extends T>(key: string, value: B) {
    const bytes = serialize(value);
    this._store.put(key, bytes, {
      valueEncoding: 'view'
    })
  }

  // Remove a value and key from the cache
  async del(key: string) {
    return new Promise((resolve, reject) => {
      this._store.del(key, (err) => {
        if (err) {
          // Ignore error if key was not found
          if (err.toString().indexOf('NotFoundError: Key not found in database') === -1 &&
            err.toString().indexOf('NotFound') === -1) {
            return reject(err)
          }
        }
        resolve(true)
      })
    })
  }
}

