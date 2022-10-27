import { JSON_ENCODING, Log } from "@dao-xyz/ipfs-log";
import { Store } from "@dao-xyz/peerbit-store"
import { EncryptionTemplateMaybeEncrypted } from '@dao-xyz/ipfs-log';
import { variant, field } from '@dao-xyz/borsh';
import { Program } from "@dao-xyz/peerbit-program";
import { Operation } from "@dao-xyz/peerbit-document";


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
        const handled: { [key: string]: boolean } = {}
        for (let i = values.length - 1; i >= 0; i--) {
            const item = values[i]
            if (handled[item.payload.getValue().key]) {
                continue
            }
            handled[item.payload.getValue().key] = true
            if (item.payload.getValue().op === 'PUT') {
                this._index[item.payload.getValue().key] = item.payload.getValue().value
                continue
            }
            if (item.payload.getValue().op === 'DEL') {
                delete this._index[item.payload.getValue().key]
                continue
            }
        }
    }
}

const encoding = JSON_ENCODING;
@variant([0, 253])
export class KeyValueStore<T> extends Program {
    _index: KeyValueIndex;

    @field({ type: Store })
    store: Store<Operation<T>>

    constructor(properties: {
        id: string
    }) {
        super(properties);
        this.store = new Store({ ...properties })
        this._index = new KeyValueIndex();
    }
    async setup() {
        this.store.setup({
            onUpdate: this._index.updateIndex.bind(this._index),
            encoding,
            canAppend: () => Promise.resolve(true)
        })
    }

    get all() {
        return this._index._index
    }

    get(key: string) {
        return this._index.get(key)
    }

    set(key: string, data: any, options?: {
        onProgressCallback?: (any: any) => void;
        pin?: boolean;
        reciever?: EncryptionTemplateMaybeEncrypted;
    }) {
        return this.put(key, data, options)
    }

    put(key: string, data: any, options?: {
        onProgressCallback?: (any: any) => void;
        pin?: boolean;
        reciever?: EncryptionTemplateMaybeEncrypted;
    }) {
        return this.store._addOperation({
            op: 'PUT',
            key: key,
            value: data
        }, { ...options })
    }

    del(key: string, options?: {
        onProgressCallback?: (any: any) => void;
        pin?: boolean;
        reciever?: EncryptionTemplateMaybeEncrypted;
    }) {
        return this.store._addOperation({
            op: 'DEL',
            key: key,
            value: undefined
        }, { ...options })
    }
}

