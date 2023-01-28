import { Change, Entry } from "@dao-xyz/peerbit-log";
import { Store } from "../store";

export class SimpleIndex<T> {
	_index: Entry<T>[];
	_store: Store<T>;
	constructor(store: Store<T>) {
		if (!store) {
			throw new Error("Unexpected");
		}
		this._index = [];
		this._store = store;
	}

	async updateIndex(change: Change<T>) {
		this._index = this._store.oplog.values;
	}
}
