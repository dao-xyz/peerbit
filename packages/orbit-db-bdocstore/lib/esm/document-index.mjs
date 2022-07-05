import { deserialize } from "@dao-xyz/borsh";
import bs58 from 'bs58';
import { asString } from './utils.mjs';
export class DocumentIndex {
    constructor() {
        this._index = {};
    }
    init(clazz) {
        this.clazz = clazz;
    }
    get(key, fullOp = false) {
        let stringKey = asString(key);
        return fullOp
            ? this._index[stringKey]
            : this._index[stringKey] ? this._index[stringKey].payload.value : null;
    }
    async updateIndex(oplog) {
        if (!this.clazz) {
            throw new Error("Not initialized");
        }
        const reducer = (handled, item, idx) => {
            let key = asString(item.payload.key);
            if (item.payload.op === 'PUTALL' && item.payload.docs[Symbol.iterator]) {
                for (const doc of item.payload.docs) {
                    if (doc && handled[doc.key] !== true) {
                        handled[doc.key] = true;
                        this._index[doc.key] = {
                            payload: {
                                op: 'PUT',
                                key: asString(doc.key),
                                value: this.deserializeOrPass(doc.value)
                            },
                            identity: item.identity
                        };
                    }
                }
            }
            else if (handled[key] !== true) {
                handled[key] = true;
                if (item.payload.op === 'PUT') {
                    this._index[key] = this.deserializeOrItem(item);
                }
                else if (item.payload.op === 'DEL') {
                    delete this._index[key];
                }
            }
            return handled;
        };
        try {
            oplog.values
                .slice()
                .reverse()
                .reduce(reducer, {});
        }
        catch (error) {
            console.error(JSON.stringify(error));
            throw error;
        }
    }
    deserializeOrPass(value) {
        return typeof value === 'string' ? deserialize(bs58.decode(value), this.clazz) : value;
    }
    deserializeOrItem(item) {
        if (typeof item.payload.value !== 'string')
            return item;
        const newItem = { ...item, payload: { ...item.payload } };
        newItem.payload.value = this.deserializeOrPass(newItem.payload.value);
        return newItem;
    }
}
//# sourceMappingURL=document-index.js.map