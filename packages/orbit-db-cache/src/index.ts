import Logger from 'logplease'
const logger = Logger.create('cache', { color: Logger.Colors.Magenta })
Logger.setLogLevel('ERROR')
import { serialize, deserialize, Constructor } from '@dao-xyz/borsh';
import bs58 from 'bs58';

export default class Cache {
  _store: any;
  constructor(store) {
    this._store = store
  }

  get status() { return this._store.db.status }

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

  async get(key) {
    return new Promise((resolve, reject) => {
      this._store.get(key, (err, value) => {
        if (err) {
          // Ignore error if key was not found
          if (err.toString().indexOf('NotFoundError: Key not found in database') === -1 &&
            err.toString().indexOf('NotFound') === -1) {
            return reject(err)
          }
        }
        resolve(value ? JSON.parse(value) : null)
      })
    })
  }

  // Set value in the cache and return the new value
  set(key, value) {
    return new Promise((resolve, reject) => {
      this._store.put(key, JSON.stringify(value), (err) => {
        if (err) {
          // Ignore error if key was not found
          if (err.toString().indexOf('NotFoundError: Key not found in database') === -1 &&
            err.toString().indexOf('NotFound') === -1) {
            return reject(err)
          }
        }
        logger.debug(`cache: Set ${key} to ${JSON.stringify(value)}`)
        resolve(true)
      })
    })
  }

  async getBinary<T>(key: string, clazz: Constructor<T>): Promise<T | null> {
    return new Promise((resolve, reject) => {
      this._store.get(key, (err, value: string) => {
        if (err) {
          // Ignore error if key was not found
          if (err.toString().indexOf('NotFoundError: Key not found in database') === -1 &&
            err.toString().indexOf('NotFound') === -1) {
            return reject(err)
          }
        }
        resolve(value ? deserialize(bs58.decode(value), clazz) : null)
      })
    })
  }

  setBinary(key: string, value: any) {
    return new Promise((resolve, reject) => {
      const encoded = bs58.encode(serialize(value));
      this._store.put(key, encoded, (err) => {
        if (err) {
          // Ignore error if key was not found
          if (err.toString().indexOf('NotFoundError: Key not found in database') === -1 &&
            err.toString().indexOf('NotFound') === -1) {
            return reject(err)
          }
        }
        logger.debug(`cache: SetBinary ${key} to ${encoded}`)
        resolve(true)
      })
    })
  }

  load() { } // noop
  destroy() { } // noop

  // Remove a value and key from the cache
  async del(key) {
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

