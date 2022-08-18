import { Log } from "@dao-xyz/ipfs-log";
import { OrbitDB } from "../../../orbit-db";
import { EventStore } from "./event-store"
import { X25519PublicKey } from 'sodium-plus';

export const FEED_STORE_TYPE = 'feed';
class FeedIndex {
    _index: any;
    constructor() {
        this._index = {}
    }

    get() {
        return Object.keys(this._index).map((f) => this._index[f])
    }

    updateIndex(oplog: Log<any>) {
        this._index = {}
        oplog.values.reduce((handled, item) => {
            if (!handled.includes(item.hash)) {
                handled.push(item.hash)
                if (item.payload.value.op === 'ADD') {
                    this._index[item.hash] = item
                } else if (item.payload.value.op === 'DEL') {
                    delete this._index[item.payload.value.value]
                }
            }
            return handled
        }, [])
    }
}
export class FeedStore<T> extends EventStore<T> {
    constructor(ipfs, id, dbname, options) {
        if (!options) options = {}
        if (!options.Index) Object.assign(options, { Index: FeedIndex })
        super(ipfs, id, dbname, options)
        this._type = FEED_STORE_TYPE
    }

    remove(hash, options?: {
        onProgressCallback?: (any: any) => void;
        pin?: boolean;
        reciever?: X25519PublicKey;
    }) {
        return this.del(hash, options)
    }

    del(hash, options?: {
        onProgressCallback?: (any: any) => void;
        pin?: boolean;
        reciever?: X25519PublicKey;
    }) {
        const operation = {
            op: 'DEL',
            key: null,
            value: hash
        }
        return this._addOperation(operation, options)
    }
}

OrbitDB.addDatabaseType(FEED_STORE_TYPE, FeedStore)
