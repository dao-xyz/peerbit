import { deserialize } from "@dao-xyz/borsh";
import bs58 from 'bs58';
export class KeyValueIndex {
    constructor() {
        this._index = {};
    }
    init(clazz) {
        this.clazz = clazz;
    }
    get(key) {
        return this._index[key];
    }
    async updateIndex(oplog) {
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
                let buffer = bs58.decode(item.payload.value);
                this._index[item.payload.key] = deserialize(buffer, this.clazz);
                continue;
            }
            if (item.payload.op === 'DEL') {
                delete this._index[item.payload.key];
                continue;
            }
        }
    }
}
//# sourceMappingURL=key-value-index.js.map