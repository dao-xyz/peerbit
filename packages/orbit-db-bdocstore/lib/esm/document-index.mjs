var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
import { deserialize, field, variant, vec } from "@dao-xyz/borsh";
import { asString } from './utils.mjs';
import { U8IntArraySerializer } from "@dao-xyz/io-utils";
let Operation = class Operation {
};
Operation = __decorate([
    variant(0)
], Operation);
export { Operation };
let PutOperation = class PutOperation extends Operation {
    constructor(props) {
        super();
        if (props) {
            this.key = props.key;
            this.data = props.data;
            this._value = props.value;
        }
    }
    get value() {
        if (!this._value) {
            throw new Error("Unexpected");
        }
        return this._value;
    }
};
__decorate([
    field({ type: 'string' }),
    __metadata("design:type", String)
], PutOperation.prototype, "key", void 0);
__decorate([
    field(U8IntArraySerializer),
    __metadata("design:type", Uint8Array)
], PutOperation.prototype, "data", void 0);
PutOperation = __decorate([
    variant(0),
    __metadata("design:paramtypes", [Object])
], PutOperation);
export { PutOperation };
let PutAllOperation = class PutAllOperation extends Operation {
    constructor(props) {
        super();
        if (props) {
            this.docs = props.docs;
        }
    }
};
__decorate([
    field({ type: vec(PutOperation) }),
    __metadata("design:type", Array)
], PutAllOperation.prototype, "docs", void 0);
PutAllOperation = __decorate([
    variant(1),
    __metadata("design:paramtypes", [Object])
], PutAllOperation);
export { PutAllOperation };
let DeleteOperation = class DeleteOperation extends Operation {
    constructor(props) {
        super();
        if (props) {
            this.key = props.key;
        }
    }
};
__decorate([
    field({ type: 'string' }),
    __metadata("design:type", String)
], DeleteOperation.prototype, "key", void 0);
DeleteOperation = __decorate([
    variant(2),
    __metadata("design:paramtypes", [Object])
], DeleteOperation);
export { DeleteOperation };
export class DocumentIndex {
    constructor() {
        this._index = {};
    }
    init(clazz) {
        this.clazz = clazz;
    }
    get(key) {
        let stringKey = asString(key);
        return this._index[stringKey];
    }
    async updateIndex(oplog) {
        if (!this.clazz) {
            throw new Error("Not initialized");
        }
        const reducer = (handled, item, idx) => {
            let payload = item.payload.value;
            if (payload instanceof PutAllOperation) {
                for (const doc of payload.docs) {
                    if (doc && handled[doc.key] !== true) {
                        handled[doc.key] = true;
                        this._index[doc.key] = {
                            key: asString(doc.key),
                            value: this.deserializeOrPass(doc),
                            entry: item
                        };
                    }
                }
            }
            else if (payload instanceof PutOperation) {
                const key = payload.key;
                if (handled[key] !== true) {
                    handled[key] = true;
                    this._index[key] = {
                        entry: item,
                        key: payload.key,
                        value: this.deserializeOrPass(payload)
                    };
                }
            }
            else if (payload instanceof DeleteOperation) {
                const key = payload.key;
                if (handled[key] !== true) {
                    handled[key] = true;
                    delete this._index[key];
                }
            }
            else {
                // Unknown operation
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
        if (value._value) {
            return value._value;
        }
        else {
            value._value = deserialize(Buffer.from(value.data), this.clazz);
            return value._value;
        }
    }
    deserializeOrItem(entry, operation) {
        /* if (typeof item.payload.value !== 'string')
          return item as LogEntry<T> */
        const item = {
            entry,
            key: operation.key,
            value: this.deserializeOrPass(operation)
        };
        return item;
        /* const newItem = { ...item, payload: { ...item.payload } };
        newItem.payload.value = this.deserializeOrPass(newItem.payload.value)
        return newItem as LogEntry<T>; */
    }
}
//# sourceMappingURL=document-index.js.map