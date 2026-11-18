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
 * - **Trimmed and bounded.** Entries no reader can still read are dropped:
 *   those before the lowest reader, and every entry while all readers must
 *   rebase (a commit then keeps none of its keys). At most `capacity` entries
 *   are kept: past that, the oldest are dropped, and a reader that had not
 *   read them is told to rebase.
 * - **Epochs.** `beginEpoch` drops every entry and tells every registered
 *   reader to rebase. The engine calls it when its tables close (stop, drop),
 *   since they may be replaced before they open again. A change `record`
 *   cannot name (no root table, or no key) begins an epoch too: no entry can
 *   stand for it, and dropping it would hide it.
 *
 * Positions are absolute and never decrease, so a reader never reads an entry
 * twice. Readers are counted per position and never scanned: whatever the
 * number of readers, a call costs O(1) plus O(1) per entry it publishes,
 * visits or drops. Writes through another `SQLiteIndex` instance or raw SQL
 * are not recorded, as `mutationVersion` does not count them either.
 */
export class ChangeJournal {
	/** Most entries kept at once. Tests lower it to force an overflow. */
	capacity: number;

	/**
	 * Whether writes are recorded, that is whether any reader is registered.
	 * Set only by `register` and `unregister`. A field, not a getter: a put
	 * reads it for every value it writes, reader or not.
	 */
	recording = false;

	private _epoch = 0;
	/** Absolute position of the oldest kept entry. */
	private _start = 0;
	private readonly readers = new Set<Reader>();
	/**
	 * How many readers of the current epoch sit at each position. Positions
	 * before `start` belong to readers that must rebase; the lowest one from
	 * `start` on is the floor that `trim` drops entries up to.
	 */
	private readonly readersAt = new Map<number, number>();
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
		this.recording = true;
		this.count(reader.position, 1);
		return reader;
	}

	unregister(reader: ChangeJournalReader): void {
		const known = reader as Reader;
		if (!this.readers.delete(known)) {
			return;
		}
		if (this.readers.size === 0) {
			this.recording = false;
			this.clear();
		} else if (known.epoch === this._epoch) {
			this.count(known.position, -1);
			this.trim();
		}
	}

	/**
	 * Stages a key written by the current write. A no-op without readers. A
	 * change it cannot name (a negative table ordinal, or no key) begins an
	 * epoch instead, so that every reader rebases rather than miss it.
	 */
	record(table: number, key: unknown): void {
		if (!this.recording) {
			return;
		}
		if (table < 0 || key == null) {
			this.invalidate();
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
		if (this.size === 0 && !this.readersAt.has(this._start)) {
			// No reader can read these. `trim` keeps entries only while a reader
			// sits at `start`, so every reader sits before it and must rebase.
			// Move past them without keeping them: `trim` would drop them all.
			if (this.readers.size > 0) {
				this._start += count;
			}
		} else {
			for (let i = 0; i < count; i++) {
				this.tables.push(this.stagedTables[i]);
				this.keys.push(this.stagedKeys[i]);
			}
			const overflow = this.size - this.capacity;
			if (overflow > 0) {
				// The readers that had not read these must rebase.
				this.drop(overflow);
			}
			// A no-op while a reader sits at `start`. After an overflow it drops
			// the entries before the lowest reader left, and it drops every entry
			// when all readers must rebase.
			this.trim();
		}
		this.stagedTables.length = 0;
		this.stagedKeys.length = 0;
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
	 * moves back. The last reader to leave `start` trims the entries no reader
	 * needs any more.
	 */
	advance(reader: ChangeJournalReader, pull: ChangeJournalPull): void {
		const known = reader as Reader;
		if (!this.readers.has(known) || pull.epoch !== this._epoch) {
			return;
		}
		if (known.epoch === pull.epoch && known.position >= pull.end) {
			return;
		}
		const counted = known.epoch === this._epoch;
		if (counted) {
			this.count(known.position, -1);
		}
		known.position = pull.end;
		known.epoch = pull.epoch;
		this.count(known.position, 1);
		if (counted) {
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
	 * Tells every registered reader to rebase and drops the published entries.
	 * The staged keys stay: the write in progress publishes or discards them as
	 * usual, for the readers registered after this.
	 */
	private invalidate(): void {
		this._epoch++;
		this.dropPublished();
	}

	/** Adds `delta` to the readers counted at `position`. */
	private count(position: number, delta: 1 | -1): void {
		const count = (this.readersAt.get(position) ?? 0) + delta;
		if (count > 0) {
			this.readersAt.set(position, count);
		} else {
			this.readersAt.delete(position);
		}
	}

	/**
	 * Drops the entries before the lowest reader that can still read: no reader
	 * needs them. While one sits at `start` this is one lookup; otherwise each
	 * step drops one entry, so its cost is paid for by the entries it drops.
	 */
	private trim(): void {
		const end = this.end;
		let floor = this._start;
		if (this.readersAt.size === 0) {
			floor = end;
		} else {
			while (floor < end && !this.readersAt.has(floor)) {
				floor++;
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
			// Nothing is left: reuse the arrays.
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

	/**
	 * Drops every published entry and every reader count, releasing the arrays'
	 * memory. Callers leave no reader counted: none is registered, or the epoch
	 * just changed.
	 */
	private dropPublished(): void {
		this._start = this.end;
		this.tables = [];
		this.keys = [];
		this.head = 0;
		this.readersAt.clear();
	}

	/** Drops everything, releasing the arrays' memory. */
	private clear(): void {
		this.dropPublished();
		this.stagedTables = [];
		this.stagedKeys = [];
	}
}
