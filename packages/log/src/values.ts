import { Entry } from "./entry";
import { ISortFunction } from "./log-sorting";
import yallist from "yallist";

export class Values<T> {
    /**
     * Keep track of sorted elements in descending sort order (i.e. newest elements)
     */
    private _values: yallist<Entry<T>>;
    _sortFn: ISortFunction;
    _byteLength: number;

    constructor(sortFn: ISortFunction, entries: Entry<T>[] = []) {
        this._values = yallist.create(entries.slice().sort(sortFn).reverse());
        this._byteLength = 0;
        entries.forEach((entry) => {
            this._byteLength += entry._payload.byteLength;
        });
        this._sortFn = sortFn;
    }

    toArray(): Entry<T>[] {
        return this._values.toArrayReverse(); // we do reverse because we assume the log is only meaningful if we read it from start to end
    }

    get head() {
        return this._values.head;
    }
    get tail() {
        return this._values.tail;
    }

    put(value: Entry<T>) {
        // assume we want to insert at head (or somehere close)
        let walker = this._values.head;
        let last: yallist.Node<Entry<T>> | undefined = undefined;
        while (walker) {
            if (this._sortFn(walker.value, value) < 0) {
                break;
            }
            last = walker;
            walker = walker.next;
            continue;
        }

        this._byteLength += value._payload.byteLength;
        _insertAfter(this._values, last, value);
    }

    delete(value: Entry<T> | string) {
        const hash = typeof value === "string" ? value : value.hash;
        // Assume we want to delete at tail (or somwhere close)
        let walker = this._values.tail;
        while (walker) {
            if (walker.value.hash === hash) {
                this._values.removeNode(walker);
                this._byteLength -= walker.value._payload.byteLength;
                return;
            }
            walker = walker.prev;
        }

        throw new Error("Failed to delete, entry does not exist");
    }

    pop() {
        const value = this._values.pop();
        if (value) {
            this._byteLength -= value._payload.byteLength;
        }
        return value;
    }

    getLowerIndex(value: Entry<T>) {
        let cmp = this._values.head;
        while (cmp) {
            if (this._sortFn(cmp.value, value) > 0) {
                cmp = cmp.prev;
                continue;
            }
        }
    }

    get byteLength() {
        return this._byteLength;
    }
}

function _insertAfter(self, node, value) {
    const inserted = !node
        ? new yallist.Node(value, null as any, self.head, self)
        : new yallist.Node(value, node, node.next, self);

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
