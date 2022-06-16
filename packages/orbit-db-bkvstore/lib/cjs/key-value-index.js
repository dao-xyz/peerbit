"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.KeyValueIndex = void 0;
const borsh_1 = require("@dao-xyz/borsh");
const bs58_1 = __importDefault(require("bs58"));
class KeyValueIndex {
    constructor() {
        this._index = {};
    }
    init(clazz) {
        this.clazz = clazz;
    }
    get(key) {
        return this._index[key];
    }
    updateIndex(oplog) {
        if (!this.clazz) {
            throw new Error("Not initialized");
        }
        const values = oplog.values;
        const handled = {};
        for (let i = values.length - 1; i >= 0; i--) {
            const item = values[i];
            if (handled[item.payload.key]) {
                continue;
            }
            handled[item.payload.key] = true;
            if (item.payload.op === 'PUT') {
                let buffer = bs58_1.default.decode(item.payload.value);
                this._index[item.payload.key] = (0, borsh_1.deserialize)(buffer, this.clazz);
                continue;
            }
            if (item.payload.op === 'DEL') {
                delete this._index[item.payload.key];
                continue;
            }
        }
    }
    get size() {
        return Object.keys(this._index._index).length;
    }
}
exports.KeyValueIndex = KeyValueIndex;
//# sourceMappingURL=key-value-index.js.map