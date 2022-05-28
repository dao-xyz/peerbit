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
    updateIndex(oplog, onProgressCallback) {
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
                            }
                        };
                    }
                }
            }
            else if (handled[key] !== true) {
                handled[key] = true;
                if (item.payload.op === 'PUT') {
                    item.payload.value = this.deserializeOrPass(item.payload.value);
                    this._index[key] = item;
                }
                else if (item.payload.op === 'DEL') {
                    delete this._index[key];
                }
            }
            if (onProgressCallback)
                onProgressCallback(item, idx);
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
        }
    }
    deserializeOrPass(value) {
        return typeof value === 'string' ? deserialize(bs58.decode(value), this.clazz) : value;
    }
}
//# sourceMappingURL=document-index.js.map