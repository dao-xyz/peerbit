import { Entry } from "@dao-xyz/ipfs-log";
import { Log } from "@dao-xyz/ipfs-log";

export class SimpleIndex<T> {
  _index: Entry<T>[];
  id?: any;
  constructor(id?: string) {
    this.id = id;
    this._index = [];
  }

  async updateIndex(oplog: Log<T>, entries?: Entry<T>[]) {
    this._index = oplog.values;
  }
}
