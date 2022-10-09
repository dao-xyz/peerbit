import { Identity, Log } from "@dao-xyz/ipfs-log";
import { Address, IInitializationOptions, load, StoreLike } from "@dao-xyz/orbit-db-store";
import { Store } from "@dao-xyz/orbit-db-store"
import { EncryptionTemplateMaybeEncrypted } from '@dao-xyz/ipfs-log';
import { AccessController } from "@dao-xyz/orbit-db-store";
import { Operation } from "./event-store";
import { variant } from '@dao-xyz/borsh';
import { IPFS } from "ipfs-core-types";
import { EncodingType } from "@dao-xyz/orbit-db-store";


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


@variant(2)
export class KeyValueStore<T> extends Store<Operation<T>> {
    _type: string;
    _index: KeyValueIndex;
    constructor(properties: {
        name: string;
        accessController: AccessController<Operation<T>>;
    }) {
        super({ ...properties, encoding: EncodingType.JSON })
        this._index = new KeyValueIndex();
    }
    async init(ipfs: IPFS, identity: Identity, options: IInitializationOptions<Operation<T>>): Promise<StoreLike<Operation<T>>> {
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
        return this._addOperation({
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
        return this._addOperation({
            op: 'DEL',
            key: key,
            value: undefined
        }, options)
    }

    static async load<T>(ipfs: any, address: Address, options?: {
        timeout?: number;
    }): Promise<KeyValueStore<T>> {
        const instance = await load(ipfs, address, Store, options)
        if (instance instanceof KeyValueStore === false) {
            throw new Error("Unexpected")
        };
        return instance as KeyValueStore<T>;
    }
}

