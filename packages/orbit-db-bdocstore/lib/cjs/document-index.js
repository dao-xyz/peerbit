"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DocumentIndex = void 0;
const borsh_1 = require("@dao-xyz/borsh");
const bs58_1 = __importDefault(require("bs58"));
class DocumentIndex {
    constructor() {
        this._index = {};
    }
    init(clazz) {
        this.clazz = clazz;
    }
    get(key, fullOp = false) {
        return fullOp
            ? this._index[key]
            : this._index[key] ? this._index[key].payload.value : null;
    }
    updateIndex(oplog, onProgressCallback) {
        if (!this.clazz) {
            throw new Error("Not initialized");
        }
        const reducer = (handled, item, idx) => {
            if (item.payload.op === 'PUTALL' && item.payload.docs[Symbol.iterator]) {
                for (const doc of item.payload.docs) {
                    if (doc && handled[doc.key] !== true) {
                        handled[doc.key] = true;
                        this._index[doc.key] = {
                            payload: {
                                op: 'PUT',
                                key: doc.key,
                                value: this.deserializeOrPass(doc.value)
                            }
                        };
                    }
                }
            }
            else if (handled[item.payload.key] !== true) {
                handled[item.payload.key] = true;
                if (item.payload.op === 'PUT') {
                    item.payload.value = this.deserializeOrPass(item.payload.value);
                    this._index[item.payload.key] = item;
                }
                else if (item.payload.op === 'DEL') {
                    delete this._index[item.payload.key];
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
        return typeof value === 'string' ? (0, borsh_1.deserialize)(bs58_1.default.decode(value), this.clazz) : value;
    }
}
exports.DocumentIndex = DocumentIndex;
//# sourceMappingURL=document-index.js.map