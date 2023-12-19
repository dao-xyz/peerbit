import { Entry, ShallowEntry } from "./entry";
import { ISortFunction } from "./log-sorting";
import yallist from "yallist";
import { EntryIndex } from "./entry-index";

export type EntryNode = yallist.Node<string>;

export class Values<T> {
	/**
	 * Keep track of sorted elements in descending sort order (i.e. newest elements)
	 */
	private _values: yallist<string>;
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
					return x.hash; /* {
						hash: x.hash,
						byteLength: x._payload.byteLength,
						meta: {
							gids: x.gids,
							gid: x.gid,
							data: x.meta.data,
						},
					}; */
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
			this._values.toArrayReverse().map((x) => this._entryIndex.get(x))
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

	get entryIndex(): EntryIndex<T> {
		return this._entryIndex;
	}
	put(value: Entry<T>) {
		return this._put(value);
	}
	_put(value: Entry<T>) {
		// assume we want to insert at head (or somehere close)
		let walker = this._values.head;
		let last: EntryNode | undefined = undefined;
		while (walker) {
			const walkerValue = this._entryIndex.getShallow(walker.value);
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

		_insertAfter(this._values, last, value.hash);
	}

	async delete(value: Entry<T> | string) {
		const hash = typeof value === "string" ? value : value.hash;
		// Assume we want to delete at tail (or somwhere close)

		let walker = this._values.tail;
		while (walker) {
			const walkerValue = this._entryIndex.getShallow(walker.value);

			if (!walkerValue) {
				throw new Error("Missing walker value");
			}

			if (walkerValue.hash === hash) {
				this._values.removeNode(walker);
				this._byteLength -= walkerValue.payloadByteLength;
				return;
			}
			walker = walker.prev; // prev will be undefined if you do removeNode(walker)
		}
		throw new Error(
			"Failed to delete, entry does not exist" +
				" ??? " +
				this.length +
				" ??? " +
				hash
		);
	}

	deleteNode(node: EntryNode) {
		this._values.removeNode(node);
		this._byteLength -= this._entryIndex.getShallow(
			node.value
		)!.payloadByteLength;
		return;
	}

	pop() {
		const value = this._values.pop();
		if (value) {
			this._byteLength -= this._entryIndex.getShallow(value)!.payloadByteLength;
		}
		return value;
	}

	get byteLength() {
		return this._byteLength;
	}
}

function _insertAfter(
	self: yallist<any>,
	node: EntryNode | undefined,
	value: string
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
