import { Entry } from "./entry.js";
import { ISortFunction } from "./log-sorting.js";

export class HeadsIndex<T> {
    _index: Map<string, Entry<T>> = new Map();
    //  _headsCache?: Entry<T>[];
    _sortFn: ISortFunction;

    _gids: Map<string, number>;

    constructor(properties: {
        sortFn: ISortFunction;
        entries: { [key: string]: Entry<any> };
    }) {
        this._gids = new Map();
        this._sortFn = properties.sortFn;
        this.reset(properties.entries);
    }

    get index(): Map<string, Entry<T>> {
        return this._index;
    }

    get gids(): Map<string, number> {
        return this._gids;
    }

    reset(entries: { [key: string]: Entry<any> } | Entry<T>[]) {
        this._index.clear();
        this._gids = new Map();
        (Array.isArray(entries) ? entries : Object.values(entries)).forEach(
            (entry) => {
                this.put(entry);
            }
        );
        //    this._headsCache = undefined;
    }

    get(hash: string) {
        return this._index.get(hash);
    }

    put(entry: Entry<any>) {
        if (!entry.hash) {
            throw new Error("Missing hash");
        }
        this._index.set(entry.hash, entry);
        if (!this._gids.has(entry.gid)) {
            this._gids.set(entry.gid, 1);
        } else {
            this._gids.set(entry.gid, this._gids.get(entry.gid)! + 1);
        }
    }

    del(entry: Entry<any>): { removed: boolean; lastWithGid: boolean } {
        const wasHead = this._index.delete(entry.hash);
        if (!wasHead) {
            return {
                lastWithGid: false,
                removed: false,
            };
        }
        const newValue = this._gids.get(entry.gid)! - 1;
        const lastWithGid = newValue <= 0;
        if (newValue <= 0) {
            this._gids.delete(entry.gid);
        } else {
            this._gids.set(entry.gid, newValue);
        }
        if (!entry.hash) {
            throw new Error("Missing hash");
        }
        return {
            removed: wasHead,
            lastWithGid: lastWithGid,
        };
        //     this._headsCache = undefined; // TODO do smarter things here, only remove the element needed (?)
    }

    /**
     * Returns an array of heads.
     * Dont use this anywhere where performance matters
     * @returns {Array<Entry<T>>}
     */
    get array(): Entry<T>[] {
        return [...this._index.values()].sort(this._sortFn).reverse();
    }
}
