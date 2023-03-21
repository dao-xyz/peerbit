import { Entry } from "./entry.js";

export class HeadsIndex<T> {
	private _index: Set<string> = new Set();
	private _gids: Map<string, number>;

	constructor(properties: { entries?: Entry<T>[] }) {
		this._gids = new Map();
		this.reset(properties.entries);
	}

	get index() {
		return this._index;
	}

	get gids(): Map<string, number> {
		return this._gids;
	}

	reset(entries?: Entry<T>[]) {
		this._index.clear();
		this._gids = new Map();
		entries?.forEach((v) => {
			this.put(v);
		});
	}

	has(cid: string) {
		return this._index.has(cid);
	}

	put(entry: Entry<any>) {
		if (!entry.hash) {
			throw new Error("Missing hash");
		}

		if (this._index.has(entry.hash)) {
			return;
		}

		this._index.add(entry.hash);
		if (!this._gids.has(entry.gid)) {
			this._gids.set(entry.gid, 1);
		} else {
			this._gids.set(entry.gid, this._gids.get(entry.gid)! + 1);
		}
	}

	del(entry: { hash: string; gid: string }): {
		removed: boolean;
		lastWithGid: boolean;
	} {
		const wasHead = this._index.delete(entry.hash);
		if (!wasHead) {
			return {
				lastWithGid: false,
				removed: false,
			};
		}
		const newValue = this._gids.get(entry.gid)! - 1;
		const lastWithGid = newValue <= 0;
		if (newValue <= 0) {
			this._gids.delete(entry.gid);
		} else {
			this._gids.set(entry.gid, newValue);
		}
		if (!entry.hash) {
			throw new Error("Missing hash");
		}
		return {
			removed: wasHead,
			lastWithGid: lastWithGid,
		};
		//     this._headsCache = undefined; // TODO do smarter things here, only remove the element needed (?)
	}
}
