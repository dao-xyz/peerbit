import { Change, Entry } from "@dao-xyz/peerbit-log";
import { Log } from "@dao-xyz/peerbit-log";

export class SimpleIndex<T> {
    _index: Entry<T>[];
    id?: any;
    constructor(id?: string) {
        this.id = id;
        this._index = [];
    }

    async updateIndex(oplog: Log<T>, change: Change<T>) {
        this._index = oplog.values;
    }
}
