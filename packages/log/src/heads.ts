import { Entry } from "./entry.js";
import { ISortFunction } from "./log-sorting.js";

export class HeadsIndex<T> {
    _index: Map<string, Entry<T>> = new Map();
    _headsCache?: Entry<T>[];
    _sortFn: ISortFunction;

    constructor(properties: {
        sortFn: ISortFunction;
        entries: { [key: string]: Entry<any> };
    }) {
        this._sortFn = properties.sortFn;
        this.reset(properties.entries);
    }

    reset(entries: { [key: string]: Entry<any> } | Entry<T>[]) {
        this._index.clear();
        (Array.isArray(entries) ? entries : Object.values(entries)).forEach(
            (entry) => {
                if (!entry.hash) {
                    throw new Error("Unexpected");
                }
                this._index.set(entry.hash, entry);
            }
        );
        this._headsCache = undefined;
    }

    get(hash: string) {
        return this._index.get(hash);
    }

    del(hash: string) {
        const _deleted = this._index.delete(hash);
        this._headsCache = undefined; // TODO do smarter things here, only remove the element needed (?)
    }

    /**
     * Returns an array of heads.
     * @returns {Array<Entry<T>>}
     */
    get heads(): Entry<T>[] {
        if (this._headsCache != undefined) {
            return [...this._headsCache];
        }
        this._headsCache = [...this._index.values()]
            .sort(this._sortFn)
            .reverse();
        return this.heads;
    }
}
