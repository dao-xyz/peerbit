import { Identity, Log } from "@dao-xyz/ipfs-log";
import { Address, IInitializationOptions, load } from "@dao-xyz/peerbit-dstore";
import { Store } from "@dao-xyz/peerbit-dstore"
import { EncryptionTemplateMaybeEncrypted } from '@dao-xyz/ipfs-log';
import { variant, field } from '@dao-xyz/borsh';
import { IPFS } from "ipfs-core-types";
import { EncodingType } from "@dao-xyz/peerbit-dstore";
import { Program } from "@dao-xyz/peerbit-program";
import { Operation } from "@dao-xyz/peerbit-ddoc";


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


@variant([0, 253])
export class KeyValueStore<T> extends Program {
    _type: string;
    _index: KeyValueIndex;

    @field({ type: Store })
    store: Store<Operation<T>>

    constructor(properties: {
        name: string
    }) {
        super(properties);
        if (properties) {
            this.store = new Store({ ...properties, encoding: EncodingType.JSON })
            this._index = new KeyValueIndex();
        }
    }
    async init(ipfs: IPFS, identity: Identity, options: IInitializationOptions<Operation<T>>): Promise<this> {
        let opts = Object.assign({}, { Index: KeyValueIndex })
        Object.assign(opts, options)
        return super.init(ipfs, identity, { ...options, onUpdate: this._index.updateIndex.bind(this._index) })
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
        }, options)
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
        }, options)
    }
}

