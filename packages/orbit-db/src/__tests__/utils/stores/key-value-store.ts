import { Log } from "@dao-xyz/ipfs-log";
import { JSON_ENCODER } from "@dao-xyz/orbit-db-store";
import { Store } from "@dao-xyz/orbit-db-store"
import { OrbitDB } from "../../../orbit-db";


export const KEY_VALUE_STORE_TYPE = 'keyvalue';

export class KeyValueIndex {
    _index: any
    constructor() {
        this._index = {}
    }

    get(key: string) {
        return this._index[key]
    }

    updateIndex(oplog: Log<any>) {
        const values = oplog.payloadsDecoded
        const handled = {}
        for (let i = values.length - 1; i >= 0; i--) {
            const item = values[i]
            if (handled[item.payload.key]) {
                continue
            }
            handled[item.payload.key] = true
            if (item.payload.op === 'PUT') {
                this._index[item.payload.key] = item.payload.value
                continue
            }
            if (item.payload.op === 'DEL') {
                delete this._index[item.payload.key]
                continue
            }
        }
    }
}


export class KeyValueStore extends Store<any, any, any> {
    _type: string;
    _index: any;
    constructor(ipfs, id, dbname, options: any) {
        let opts = Object.assign({}, { Index: KeyValueIndex })
        if (options.io === undefined) Object.assign(options, { io: JSON_ENCODER })
        Object.assign(opts, options)
        super(ipfs, id, dbname, opts)
        this._type = KEY_VALUE_STORE_TYPE
    }

    get all() {
        return this._index._index
    }

    get(key) {
        return this._index.get(key)
    }

    set(key, data, options = {}) {
        return this.put(key, data, options)
    }

    put(key, data, options = {}) {
        return this._addOperation({
            op: 'PUT',
            key: key,
            value: data
        }, options)
    }

    del(key, options = {}) {
        return this._addOperation({
            op: 'DEL',
            key: key,
            value: null
        }, options)
    }
}

OrbitDB.addDatabaseType(KEY_VALUE_STORE_TYPE, KeyValueStore)
