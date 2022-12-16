import { serialize, deserialize, Constructor } from "@dao-xyz/borsh";
import { AbstractLevel } from "abstract-level";
import { logger } from "@dao-xyz/peerbit-logger";
const log = logger({ module: "cache" });

export default class Cache<T> {
    _store: AbstractLevel<any, any, any>;
    constructor(store: AbstractLevel<any, any, any>) {
        this._store = store;
    }

    get status() {
        return this._store.status;
    }

    async close() {
        if (!this._store)
            return Promise.reject(new Error("No cache store found to close"));
        if (this.status === "open") {
            await this._store.close();
            return Promise.resolve();
        }
    }

    async open() {
        if (!this._store)
            return Promise.reject(new Error("No cache store found to open"));
        if (this.status !== "open") {
            await this._store.open();
            return Promise.resolve();
        }
    }

    async get<T>(key: string): Promise<T | undefined> {
        return new Promise((resolve, reject) => {
            this._store.get(key, (err, value) => {
                if (err) {
                    // Ignore error if key was not found
                    if (err["status"] !== 404) {
                        return reject(err);
                    }
                    resolve(undefined);
                }
                resolve(value ? JSON.parse(value) : null);
            });
        });
    }

    // Set value in the cache and return the new value
    set(key: string, value: T) {
        return new Promise((resolve, reject) => {
            this._store.put(key, JSON.stringify(value), (err) => {
                if (err) {
                    return reject(err);
                }
                log.debug(`cache: Set ${key} to ${JSON.stringify(value)}`);
                resolve(true);
            });
        });
    }

    async getBinary<B extends T>(
        key: string,
        clazz: Constructor<B>
    ): Promise<B | undefined> {
        return new Promise((resolve, reject) => {
            this._store.get(
                key,
                { valueEncoding: "view" },
                (err: any, value: Uint8Array | undefined) => {
                    if (err) {
                        if (err["status"] !== 404) {
                            return reject(err);
                        }
                    }
                    if (!value) {
                        resolve(undefined);
                        return;
                    }
                    try {
                        const der = value
                            ? deserialize(value, clazz)
                            : undefined;
                        resolve(der);
                    } catch (error) {
                        reject(error);
                    }
                }
            );
        });
    }

    async getBinaryPrefix<B extends T>(
        prefix: string,
        clazz: Constructor<B>
    ): Promise<B[]> {
        const iterator = this._store.iterator<any, Uint8Array>({
            gte: prefix,
            lte: prefix + "\xFF",
            valueEncoding: "view",
        });
        const ret: B[] = [];
        for await (const [_key, value] of iterator) {
            ret.push(deserialize(value, clazz));
        }

        return ret;
    }

    async deleteByPrefix(prefix: string): Promise<void[]> {
        const iterator = this._store.iterator<any, Uint8Array>({
            gte: prefix,
            lte: prefix + "\xFF",
            valueEncoding: "view",
        });
        const promises: Promise<any>[] = [];
        for await (const [key, _value] of iterator) {
            promises.push(this.del(key));
        }
        return Promise.all(promises);
    }

    setBinary<B extends T>(key: string, value: B | Uint8Array) {
        const bytes = value instanceof Uint8Array ? value : serialize(value);
        this._store.put(key, bytes, {
            valueEncoding: "view",
        });
    }

    // Remove a value and key from the cache
    async del(key: string) {
        return new Promise((resolve, reject) => {
            this._store.del(key, (err) => {
                if (err) {
                    // Ignore error if key was not found
                    if (
                        err
                            .toString()
                            .indexOf(
                                "NotFoundError: Key not found in database"
                            ) === -1 &&
                        err.toString().indexOf("NotFound") === -1
                    ) {
                        return reject(err);
                    }
                }
                resolve(true);
            });
        });
    }
}
