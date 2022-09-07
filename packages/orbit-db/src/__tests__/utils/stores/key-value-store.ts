import { Log } from "@dao-xyz/ipfs-log";
import { JSON_ENCODER } from "@dao-xyz/orbit-db-store";
import { Store } from "@dao-xyz/orbit-db-store"
import { EncryptionTemplateMaybeEncrypted } from '@dao-xyz/ipfs-log-entry';
import { AccessController } from "@dao-xyz/orbit-db-store";
import { Operation } from "./event-store";
import { variant } from '@dao-xyz/borsh';


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


@variant(2)
export class KeyValueStore<T> extends Store<Operation<T>> {
    _type: string;
    _index: KeyValueIndex;
    constructor(properties: {
        name: string;
        accessController: AccessController<Operation<T>>;
    }) {
        super(properties)
        this._index = new KeyValueIndex();
    }
    async init(ipfs, identity, options): Promise<void> {
        let opts = Object.assign({}, { Index: KeyValueIndex })
        if (options.encoding === undefined) Object.assign(options, { encoding: JSON_ENCODER })
        Object.assign(opts, options)
        super.init(ipfs, identity, { ...options, onUpdate: this._index.updateIndex.bind(this._index) })
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
        reciever?: EncryptionTemplateMaybeEncrypted;
    }) {
        return this.put(key, data, options)
    }

    put(key, data, options?: {
        onProgressCallback?: (any: any) => void;
        pin?: boolean;
        reciever?: EncryptionTemplateMaybeEncrypted;
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
        reciever?: EncryptionTemplateMaybeEncrypted;
    }) {
        return this._addOperation({
            op: 'DEL',
            key: key,
            value: null
        }, options)
    }
}

