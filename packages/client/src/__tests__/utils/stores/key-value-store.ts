import { JSON_ENCODING } from "@dao-xyz/peerbit-log";
import { Store } from "@dao-xyz/peerbit-store";
import { EncryptionTemplateMaybeEncrypted } from "@dao-xyz/peerbit-log";
import { variant, field } from "@dao-xyz/borsh";
import { Program } from "@dao-xyz/peerbit-program";
import { Operation } from "@dao-xyz/peerbit-document";

export class KeyValueIndex {
    _index: any;
    _store: Store<any>;
    constructor() {
        this._index = {};
    }

    get(key: string) {
        return this._index[key];
    }

    setup(store: Store<any>) {
        this._store = store;
    }

    updateIndex() {
        const values = this._store.oplog.values;
        const handled: { [key: string]: boolean } = {};
        for (let i = values.length - 1; i >= 0; i--) {
            const item = values[i];
            if (handled[item.payload.getValue().key]) {
                continue;
            }
            handled[item.payload.getValue().key] = true;
            if (item.payload.getValue().op === "PUT") {
                this._index[item.payload.getValue().key] =
                    item.payload.getValue().value;
                continue;
            }
            if (item.payload.getValue().op === "DEL") {
                delete this._index[item.payload.getValue().key];
                continue;
            }
        }
    }
}

const encoding = JSON_ENCODING;

@variant("kvstore")
export class KeyBlocks<T> extends Program {
    _index: KeyValueIndex;

    @field({ type: Store })
    store: Store<Operation<T>>;

    constructor(properties: { id: string }) {
        super(properties);
        this.store = new Store();
    }
    async setup() {
        this._index = new KeyValueIndex();

        this.store.setup({
            onUpdate: this._index.updateIndex.bind(this._index),
            encoding,
            canAppend: () => Promise.resolve(true),
        });
    }

    get all() {
        return this._index._index;
    }

    get(key: string) {
        return this._index.get(key);
    }

    set(
        key: string,
        data: any,
        options?: {
            pin?: boolean;
            reciever?: EncryptionTemplateMaybeEncrypted;
        }
    ) {
        return this.put(key, data, options);
    }

    put(
        key: string,
        data: any,
        options?: {
            pin?: boolean;
            reciever?: EncryptionTemplateMaybeEncrypted;
        }
    ) {
        return this.store.addOperation(
            {
                op: "PUT",
                key: key,
                value: data,
            },
            { ...options }
        );
    }

    del(
        key: string,
        options?: {
            pin?: boolean;
            reciever?: EncryptionTemplateMaybeEncrypted;
        }
    ) {
        return this.store.addOperation(
            {
                op: "DEL",
                key: key,
                value: undefined,
            },
            { ...options }
        );
    }
}
