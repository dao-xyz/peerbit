import { Log } from "@dao-xyz/ipfs-log";
import { JSON_ENCODER } from "@dao-xyz/orbit-db-store";
import { Store } from "@dao-xyz/orbit-db-store"
import { OrbitDB } from "../../../orbit-db";
import { X25519PublicKey } from 'sodium-plus';

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
        const values = oplog.values
        const handled = {}
        for (let i = values.length - 1; i >= 0; i--) {
            const item = values[i]
            if (handled[item.payload.value.key]) {
                continue
            }
            handled[item.payload.value.key] = true
            if (item.payload.value.op === 'PUT') {
                this._index[item.payload.value.key] = item.payload.value.value
                continue
            }
            if (item.payload.value.op === 'DEL') {
                delete this._index[item.payload.value.key]
                continue
            }
        }
    }
}


export class KeyValueStore extends Store<any, any, any, any> {
    _type: string;
    _index: any;
    constructor(ipfs, id, dbname, options: any) {
        let opts = Object.assign({}, { Index: KeyValueIndex })
        if (options.encoding === undefined) Object.assign(options, { encoding: JSON_ENCODER })
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

    set(key, data, options?: {
        onProgressCallback?: (any: any) => void;
        pin?: boolean;
        reciever?: X25519PublicKey;
    }) {
        return this.put(key, data, options)
    }

    put(key, data, options?: {
        onProgressCallback?: (any: any) => void;
        pin?: boolean;
        reciever?: X25519PublicKey;
    }) {
        return this._addOperation({
            op: 'PUT',
            key: key,
            value: data
        }, options)
    }

    del(key, options?: {
        onProgressCallback?: (any: any) => void;
        pin?: boolean;
        reciever?: X25519PublicKey;
    }) {
        return this._addOperation({
            op: 'DEL',
            key: key,
            value: null
        }, options)
    }
}

OrbitDB.addDatabaseType(KEY_VALUE_STORE_TYPE, KeyValueStore)
