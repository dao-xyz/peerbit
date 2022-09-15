import { Log } from "@dao-xyz/ipfs-log";
import { EventStore, Operation } from "./event-store"
import { EncryptionTemplateMaybeEncrypted } from '@dao-xyz/ipfs-log-entry';
import { AccessController } from "@dao-xyz/orbit-db-store";
import { variant } from '@dao-xyz/borsh';

/* class FeedIndex {
    _index: any;
    constructor() {
        this._index = {}
    }

    get() {
        return Object.keys(this._index).map((f) => this._index[f])
    }

    updateIndex(oplog: Log<any>) {
        this._index = {}
        oplog.values.reduce((handled, item) => {
            if (!handled.includes(item.hash)) {
                handled.push(item.hash)
                if (item.payload.value.op === 'ADD') {
                    this._index[item.hash] = item
                } else if (item.payload.value.op === 'DEL') {
                    delete this._index[item.payload.value.value]
                }
            }
            return handled
        }, [])
    }
}
 *//* 
@variant(1)
export class FeedStore<T> extends EventStore<T> {
constructor(properties: {
   name: string;
   accessController: AccessController<Operation<T>>;
}) {
   super(properties)
}
async init(ipfs, address, identity, options) {
   if (!options) options = {}
   super.init(ipfs, address, identity, { ...options, onUpdate: this._index.updateIndex.bind(this._index) })
}

remove(hash, options?: {
   onProgressCallback?: (any: any) => void;
   pin?: boolean;
   reciever?: EncryptionTemplateMaybeEncrypted;
}) {
   return this.del(hash, options)
}

del(hash, options?: {
   onProgressCallback?: (any: any) => void;
   pin?: boolean;
   reciever?: EncryptionTemplateMaybeEncrypted;
}) {
   const operation = {
       op: 'DEL',
       key: null,
       value: hash
   }
   return this._addOperation(operation, options)
}
}

*/