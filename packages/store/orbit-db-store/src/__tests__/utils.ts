import { MaybeEncrypted } from "@dao-xyz/encryption-utils"
import { Entry, Payload } from "@dao-xyz/ipfs-log-entry"
import { variant } from '@dao-xyz/borsh';
import { Log } from "@dao-xyz/ipfs-log";
import { AccessController } from "../access-controller";
import { Ed25519PublicKeyData } from '@dao-xyz/identity';

@variant([0, 254])
export class SimpleAccessController<T> extends AccessController<T>
{
    async canAppend(payload: MaybeEncrypted<Payload<T>>, key: MaybeEncrypted<Ed25519PublicKeyData>) {
        return true;
    }
}

export const defaultAccessController = () => {
    return
}


export class SimpleIndex<T> {


    _index: Entry<T>[];
    id?: any;
    constructor(id?: string) {
        this.id = id
        this._index = []
    }

    async updateIndex(oplog: Log<T>, entries?: Entry<T>[]) {
        this._index = oplog.values
    }
}
