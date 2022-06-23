"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DocumentIndex = void 0;
const borsh_1 = require("@dao-xyz/borsh");
const bs58_1 = __importDefault(require("bs58"));
const utils_1 = require("./utils");
class DocumentIndex {
    constructor() {
        this._index = {};
    }
    init(clazz) {
        this.clazz = clazz;
    }
    get(key, fullOp = false) {
        let stringKey = (0, utils_1.asString)(key);
        return fullOp
            ? this._index[stringKey]
            : this._index[stringKey] ? this._index[stringKey].payload.value : null;
    }
    updateIndex(oplog) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.clazz) {
                throw new Error("Not initialized");
            }
            const reducer = (handled, item, idx) => {
                let key = (0, utils_1.asString)(item.payload.key);
                if (item.payload.op === 'PUTALL' && item.payload.docs[Symbol.iterator]) {
                    for (const doc of item.payload.docs) {
                        if (doc && handled[doc.key] !== true) {
                            handled[doc.key] = true;
                            this._index[doc.key] = {
                                payload: {
                                    op: 'PUT',
                                    key: (0, utils_1.asString)(doc.key),
                                    value: this.deserializeOrPass(doc.value)
                                }
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
        });
    }
    deserializeOrPass(value) {
        return typeof value === 'string' ? (0, borsh_1.deserialize)(bs58_1.default.decode(value), this.clazz) : value;
    }
    deserializeOrItem(item) {
        if (typeof item.payload.value !== 'string')
            return item;
        const newItem = Object.assign(Object.assign({}, item), { payload: Object.assign({}, item.payload) });
        newItem.payload.value = this.deserializeOrPass(newItem.payload.value);
        return newItem;
    }
}
exports.DocumentIndex = DocumentIndex;
//# sourceMappingURL=document-index.js.map