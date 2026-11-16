/** Entries a journal keeps for its slowest reader before that reader must rebase. */
export const CHANGE_JOURNAL_CAPACITY = 16_384;

export interface ChangeJournalReader {
	/** Absolute position of the first entry this reader has not read. */
	readonly position: number;
	/** The journal epoch `position` belongs to. */
	readonly epoch: number;
}

/** What one `pull` saw. Pass it to `advance` once the reader acted on it. */
export interface ChangeJournalPull {
	readonly epoch: number;
	/** Absolute position after the last entry the pull covered. */
	readonly end: number;
	/**
	 * True when the journal cannot give this reader a complete window: the
	 * reader is behind the oldest kept entry, was registered in an earlier
	 * epoch, or is not registered. The pull then visits no entry, and the
	 * reader must rebuild its state from the tables before it advances.
	 */
	readonly rebase: boolean;
}

class Reader implements ChangeJournalReader {
	constructor(
		public position: number,
		public epoch: number,
	) {}
}

/**
 * The root keys that committed writes of one `SQLiteIndex` changed, in commit
 * order: one `(root table ordinal, primary key)` entry per value written or
 * deleted. The key is the value SQLite stores and returns for the primary key
 * column (a u64 key in its shifted form).
 *
 * An iterator registers a reader. Inside each of its database admissions it
 * pulls the entries after its position, and it advances only after all of
 * that admission's statements succeeded, so a failed call consumes nothing.
 *
 * - **Only while read.** With no reader registered, `record` returns at once
 *   and the journal holds no entry.
 * - **Committed changes only.** A write stages its keys and `commit` publishes
 *   them when its transaction commits. `rollback` discards the keys staged
 *   since a savepoint that rolled back. If a rollback fails, the staged keys
 *   are still published: an extra entry costs a reader one re-check, while a
 *   missing one would hide a change.
 * - **Trimmed and bounded.** Entries every reader has read are dropped. At most
 *   `capacity` entries are kept: past that, the oldest are dropped, and a
 *   reader that had not read them is told to rebase.
 * - **Epochs.** `beginEpoch` drops every entry and tells every registered
 *   reader to rebase. The engine calls it when its tables close (stop, drop),
 *   since they may be replaced before they open again.
 *
 * Positions are absolute and never decrease, so a reader never reads an entry
 * twice. Writes through another `SQLiteIndex` instance or raw SQL are not
 * recorded, as `mutationVersion` does not count them either.
 */
export class ChangeJournal {
	/** Most entries kept at once. Tests lower it to force an overflow. */
	capacity: number;

	private _epoch = 0;
	/** Absolute position of the oldest kept entry. */
	private _start = 0;
	private readonly readers = new Set<Reader>();
	/** Kept entries, oldest first from index `head`. */
	private tables: number[] = [];
	private keys: unknown[] = [];
	private head = 0;
	/** Keys of the write in progress, published by `commit`. */
	private stagedTables: number[] = [];
	private stagedKeys: unknown[] = [];

	constructor(capacity: number = CHANGE_JOURNAL_CAPACITY) {
		this.capacity = capacity;
	}

	/** Whether writes are recorded, that is whether any reader is registered. */
	get recording(): boolean {
		return this.readers.size > 0;
	}

	get epoch(): number {
		return this._epoch;
	}

	/** Absolute position of the oldest kept entry. */
	get start(): number {
		return this._start;
	}

	/** Absolute position after the newest published entry. */
	get end(): number {
		return this._start + this.keys.length - this.head;
	}

	/** Published entries currently kept. */
	get size(): number {
		return this.keys.length - this.head;
	}

	/** Keys recorded by the write in progress and not yet published. */
	get staged(): number {
		return this.stagedKeys.length;
	}

	get readerCount(): number {
		return this.readers.size;
	}

	/**
	 * Registers a reader at the current end. Its window covers every write that
	 * commits after this call, so take the reader's first view of the tables
	 * after registering it, in the same or a later admission.
	 */
	register(): ChangeJournalReader {
		const reader = new Reader(this.end, this._epoch);
		this.readers.add(reader);
		return reader;
	}

	unregister(reader: ChangeJournalReader): void {
		const known = reader as Reader;
		if (!this.readers.delete(known)) {
			return;
		}
		if (this.readers.size === 0) {
			this.clear();
		} else if (this.mayHoldFloor(known)) {
			this.trim();
		}
	}

	/** Stages a key written by the current write. A no-op without readers. */
	record(table: number, key: unknown): void {
		if (this.readers.size === 0 || table < 0 || key == null) {
			return;
		}
		this.stagedTables.push(table);
		// Keep a copy: a caller may reuse the buffer of a byte key. (A Node
		// Buffer's slice() would share its memory.)
		this.stagedKeys.push(key instanceof Uint8Array ? new Uint8Array(key) : key);
	}

	/** Marks the staged keys a savepoint opened now would roll back to. */
	savepoint(): number {
		return this.stagedKeys.length;
	}

	/** Discards the keys staged since `savepoint`: their writes rolled back. */
	rollback(savepoint: number): void {
		if (this.stagedKeys.length > savepoint) {
			this.stagedTables.length = savepoint;
			this.stagedKeys.length = savepoint;
		}
	}

	/** Publishes the staged keys: their writes committed. */
	commit(): void {
		const count = this.stagedKeys.length;
		if (count === 0) {
			return;
		}
		if (this.readers.size > 0) {
			for (let i = 0; i < count; i++) {
				this.tables.push(this.stagedTables[i]);
				this.keys.push(this.stagedKeys[i]);
			}
		}
		this.stagedTables.length = 0;
		this.stagedKeys.length = 0;
		const overflow = this.size - this.capacity;
		if (overflow > 0) {
			this.drop(overflow);
		}
	}

	/**
	 * Visits the entries `reader` has not read, oldest first, without moving
	 * it. When the result asks for a rebase, no entry is visited.
	 */
	pull(
		reader: ChangeJournalReader,
		visit?: (table: number, key: unknown) => void,
	): ChangeJournalPull {
		// Readers run in their own admission, after every write before them
		// committed or rolled back. Keys still staged belong to a write that did
		// neither: publish them rather than risk hiding a committed change.
		this.commit();
		const known = reader as Reader;
		const end = this.end;
		if (
			!this.readers.has(known) ||
			known.epoch !== this._epoch ||
			known.position < this._start
		) {
			return { epoch: this._epoch, end, rebase: true };
		}
		if (visit) {
			for (
				let i = this.head + (known.position - this._start);
				i < this.keys.length;
				i++
			) {
				visit(this.tables[i], this.keys[i]);
			}
		}
		return { epoch: this._epoch, end, rebase: false };
	}

	/**
	 * Moves `reader` past the entries of `pull`. Call it once the reader acted
	 * on them, or rebuilt its state after a rebase, in the same admission as
	 * the pull. A pull from an earlier epoch moves nothing, and a reader never
	 * moves back.
	 */
	advance(reader: ChangeJournalReader, pull: ChangeJournalPull): void {
		const known = reader as Reader;
		if (!this.readers.has(known) || pull.epoch !== this._epoch) {
			return;
		}
		if (known.epoch === pull.epoch && known.position >= pull.end) {
			return;
		}
		const mayHoldFloor = this.mayHoldFloor(known);
		known.position = pull.end;
		known.epoch = pull.epoch;
		if (mayHoldFloor) {
			this.trim();
		}
	}

	/**
	 * Drops every entry and staged key, and tells every registered reader to
	 * rebase: the tables may have been replaced. Readers stay registered.
	 */
	beginEpoch(): void {
		this._epoch++;
		this.clear();
	}

	/**
	 * Whether moving `reader` can let `trim` drop entries: it may be the reader
	 * at `start`, or it is behind `start` or from an earlier epoch. Skipping the
	 * others keeps `advance` O(1) for every reader but the slowest. After an
	 * overflow no current reader may be at `start`; the floor then rises when a
	 * reader behind it moves or unregisters, and `capacity` bounds the entries
	 * kept until then.
	 */
	private mayHoldFloor(reader: Reader): boolean {
		return reader.epoch !== this._epoch || reader.position <= this._start;
	}

	/** Drops the entries every current reader has read. */
	private trim(): void {
		let floor = this.end;
		for (const reader of this.readers) {
			if (
				reader.epoch === this._epoch &&
				reader.position >= this._start &&
				reader.position < floor
			) {
				floor = reader.position;
			}
		}
		const count = floor - this._start;
		if (count > 0) {
			this.drop(count);
		}
	}

	/** Drops the oldest `count` kept entries. */
	private drop(count: number): void {
		const head = this.head + count;
		if (head >= this.keys.length) {
			// Every reader caught up: reuse the arrays.
			this._start = this.end;
			this.tables.length = 0;
			this.keys.length = 0;
			this.head = 0;
			return;
		}
		for (let i = this.head; i < head; i++) {
			this.keys[i] = undefined;
		}
		this.head = head;
		this._start += count;
		if (head >= 1024 && head * 2 >= this.keys.length) {
			this.tables.splice(0, head);
			this.keys.splice(0, head);
			this.head = 0;
		}
	}

	/** Drops everything, releasing the arrays' memory. */
	private clear(): void {
		this._start = this.end;
		this.tables = [];
		this.keys = [];
		this.head = 0;
		this.stagedTables = [];
		this.stagedKeys = [];
	}
}
