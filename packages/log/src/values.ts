import { Entry } from "./entry";
import { ISortFunction } from "./log-sorting";
import yallist from "yallist";
import { EntryIndex } from "./entry-index";

type Storage<T> = (
	hash: string
) => Promise<Entry<T> | undefined> | Entry<T> | undefined;

interface Value {
	hash: string;
	gid: string;
	byteLength: number;
}

export type EntryNode = yallist.Node<Value>;

export class Values<T> {
	/**
	 * Keep track of sorted elements in descending sort order (i.e. newest elements)
	 */
	private _values: yallist<Value>;
	private _sortFn: ISortFunction;
	private _byteLength: number;
	private _entryIndex: EntryIndex<T>;

	constructor(
		entryIndex: EntryIndex<T>,
		sortFn: ISortFunction,
		entries: Entry<T>[] = []
	) {
		this._values = yallist.create(
			entries
				.slice()
				.sort(sortFn)
				.reverse()
				.map((x) => {
					if (!x.hash) throw new Error("Unexpected");
					return {
						hash: x.hash,
						byteLength: x._payload.byteLength,
						gid: x.gid,
					};
				})
		);
		this._byteLength = 0;
		entries.forEach((entry) => {
			this._byteLength += entry._payload.byteLength;
		});
		this._sortFn = sortFn;
		this._entryIndex = entryIndex;
	}

	toArray(): Promise<Entry<T>[]> {
		return Promise.all(
			this._values.toArrayReverse().map((x) => this._entryIndex.get(x.hash))
		).then((arr) => arr.filter((x) => !!x)) as Promise<Entry<T>[]>; // we do reverse because we assume the log is only meaningful if we read it from start to end
	}

	get head() {
		return this._values.head;
	}
	get tail() {
		return this._values.tail;
	}
	get length() {
		return this._values.length;
	}

	private _putPromise: Map<string, Promise<any>> = new Map();
	async put(value: Entry<T>) {
		let promise = this._putPromise.get(value.hash);
		if (promise) {
			return promise;
		}
		promise = this._put(value).then((v) => {
			this._putPromise.delete(value.hash);
			return v;
		});
		this._putPromise.set(value.hash, promise);
		return promise;
	}
	async _put(value: Entry<T>) {
		// assume we want to insert at head (or somehere close)
		let walker = this._values.head;
		let last: EntryNode | undefined = undefined;
		while (walker) {
			const walkerValue = await this.getEntry(walker);
			if (!walkerValue) {
				throw new Error("Missing walker value");
			}
			if (walkerValue.hash === value.hash) {
				return; // already exist!
			}

			if (this._sortFn(walkerValue, value) < 0) {
				break;
			}
			last = walker;
			walker = walker.next;
			continue;
		}

		this._byteLength += value._payload.byteLength;
		if (!value.hash) {
			throw new Error("Unexpected");
		}

		_insertAfter(this._values, last, {
			byteLength: value._payload.byteLength,
			gid: value.gid,
			hash: value.hash,
		});
	}

	async delete(value: Entry<T> | string) {
		const hash = typeof value === "string" ? value : value.hash;
		// Assume we want to delete at tail (or somwhere close)

		let walker = this._values.tail;
		while (walker) {
			const walkerValue = await this.getEntry(walker);

			if (!walkerValue) {
				throw new Error("Missing walker value");
			}

			if (walkerValue.hash === hash) {
				this._values.removeNode(walker);
				this._byteLength -= walkerValue._payload.byteLength;
				return;
			}
			walker = walker.prev; // prev will be undefined if you do removeNode(walker)
		}
		throw new Error("Failed to delete, entry does not exist");
	}

	deleteNode(node: EntryNode) {
		this._values.removeNode(node);
		this._byteLength -= node.value.byteLength;
		return;
	}

	pop() {
		const value = this._values.pop();
		if (value) {
			this._byteLength -= value.byteLength;
		}
		return value;
	}

	get byteLength() {
		return this._byteLength;
	}

	async getEntry(node: EntryNode) {
		return this._entryIndex.get(node.value.hash);
	}
}

function _insertAfter(
	self: yallist<any>,
	node: EntryNode | undefined,
	value: Value
) {
	const inserted = !node
		? new yallist.Node(
				value,
				null as any,
				self.head as EntryNode | undefined,
				self
		  )
		: new yallist.Node(value, node, node.next as EntryNode | undefined, self);

	// is tail
	if (inserted.next === null) {
		self.tail = inserted;
	}

	// is head
	if (inserted.prev === null) {
		self.head = inserted;
	}

	self.length++;
	return inserted;
}
