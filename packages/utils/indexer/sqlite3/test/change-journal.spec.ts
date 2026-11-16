import { field, option, variant, vec } from "@dao-xyz/borsh";
import {
	type Index,
	StringMatch,
	getMatcher,
	id,
	toId,
} from "@peerbit/indexer-interface";
import { expect } from "chai";
import sinon from "sinon";
import type { SQLiteIndex, SQLiteIndices } from "../src/engine.js";
import type { Database } from "../src/types.js";
import { setup } from "./utils.js";

// The journal is internal: reach it through the index. Typed structurally, so
// this spec also compiles against an engine without a journal.
type Reader = { readonly position: number; readonly epoch: number };
type Pull = { readonly epoch: number; readonly end: number; rebase: boolean };
type Journal = {
	capacity: number;
	readonly recording: boolean;
	readonly epoch: number;
	readonly start: number;
	readonly end: number;
	readonly size: number;
	readonly staged: number;
	readonly readerCount: number;
	register(): Reader;
	unregister(reader: Reader): void;
	record(table: number, key: unknown): void;
	pull(reader: Reader, visit?: (table: number, key: unknown) => void): Pull;
	advance(reader: Reader, pull: Pull): void;
};
type Entry = [table: number, key: unknown];

const journalOf = (index: Index<any, any>): Journal => {
	const journal = (index as unknown as { changeJournal?: Journal })
		.changeJournal;
	if (!journal) {
		throw new Error("The index has no change journal");
	}
	return journal;
};

/** The reader's unread entries, without moving it. */
const peek = (journal: Journal, reader: Reader) => {
	const entries: Entry[] = [];
	const pull = journal.pull(reader, (table, key) => entries.push([table, key]));
	return { pull, entries };
};

/** Reads the reader's window and moves it past, as an iterator would. */
const consume = (journal: Journal, reader: Reader) => {
	const { pull, entries } = peek(journal, reader);
	journal.advance(reader, pull);
	return { rebase: pull.rebase, entries };
};

const keysOf = (entries: Entry[]) => entries.map(([, key]) => key);

@variant("sqlite_change_journal_document")
class JournalDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	label: string;

	@field({ type: vec("string") })
	tags: string[];

	@field({ type: vec("u32") })
	values: number[];

	constructor(properties: {
		id: string;
		label?: string;
		tags?: string[];
		values?: number[];
	}) {
		this.id = properties.id;
		this.label = properties.label ?? "";
		this.tags = properties.tags ?? [];
		this.values = properties.values ?? [];
	}
}

abstract class Variant {
	abstract id: string;
}

@variant("sqlite_change_journal_first")
class FirstVariant extends Variant {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	group = "all";

	@field({ type: "string" })
	first = "yes";

	constructor(id: string) {
		super();
		this.id = id;
	}
}

@variant("sqlite_change_journal_second")
class SecondVariant extends Variant {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	group = "all";

	@field({ type: "string" })
	second = "yes";

	constructor(id: string) {
		super();
		this.id = id;
	}
}

@variant("sqlite_change_journal_u64")
class U64Row {
	@id({ type: "u64" })
	id: bigint;

	constructor(id: bigint) {
		this.id = id;
	}
}

@variant("sqlite_change_journal_bytes")
class BytesRow {
	@id({ type: Uint8Array })
	id: Uint8Array;

	constructor(id: Uint8Array) {
		this.id = id;
	}
}

@variant("sqlite_change_journal_assigned_id")
class AssignedIdRow {
	@id({ type: option("u64") })
	id?: bigint;

	@field({ type: "string" })
	label = "row";
}

const doc = (
	id: string,
	properties?: { label?: string; tags?: string[]; values?: number[] },
) => new JournalDocument({ id, ...properties });

const docs = (prefix: string, count: number) =>
	Array.from({ length: count }, (_, i) =>
		doc(`${prefix}-${String(i).padStart(3, "0")}`),
	);

const failingNestedField = (value: JournalDocument, failure: Error) => {
	// Read after the root row is written, while the nested rows are written.
	Object.defineProperty(value, "values", {
		get: () => {
			throw failure;
		},
	});
	return value;
};

const expectRejected = async (promise: Promise<unknown>) => {
	try {
		await promise;
	} catch (error) {
		return error;
	}
	throw new Error("Expected promise to reject");
};

const setupDocuments = async () => {
	const { indices, store } = await setup<JournalDocument>({
		schema: JournalDocument,
	});
	return {
		store,
		journal: journalOf(store),
		database: (indices as SQLiteIndices).properties.db,
	};
};

/** Runs `onExec` before every statement `database.exec` runs. */
const hookExec = (
	database: Database,
	onExec: (sql: string) => void,
): (() => void) => {
	const originalExec = database.exec;
	database.exec = (sql: string) => {
		onExec(sql);
		return originalExec.call(database, sql);
	};
	return () => {
		database.exec = originalExec;
	};
};

describe("SQLite change journal", () => {
	it("records nothing while no reader is registered", async () => {
		const { store, journal } = await setupDocuments();
		const record = sinon.spy(journal, "record");
		try {
			await store.put(doc("put", { tags: ["a"], values: [1] }));
			await store.putBatch!(docs("batch", 70));
			await store.withOrderedWriteSession!(async (session) => {
				await session.put(doc("ordered-0", { label: "drop" }));
				await session.put(doc("ordered-1"));
			});
			await store.del({
				query: [new StringMatch({ key: "label", value: "drop" })],
			});
			await store.del({ query: [getMatcher(["id"], "put")] });
			// Iterators do not read the journal yet, so they register no reader.
			const iterator = store.iterate({});
			expect(await iterator.next(2)).to.have.length(2);
			await store.put(doc("between-pages"));
			expect(await iterator.pending()).to.equal(70);
			expect(await iterator.all()).to.have.length(70);
			await iterator.close();

			expect(record.callCount).to.equal(0);
			expect({
				readers: journal.readerCount,
				recording: journal.recording,
				end: journal.end,
				size: journal.size,
				staged: journal.staged,
			}).to.deep.equal({
				readers: 0,
				recording: false,
				end: 0,
				size: 0,
				staged: 0,
			});

			const reader = journal.register();
			await store.put(doc("after-register"));
			expect(record.callCount).to.equal(1);
			expect(consume(journal, reader).entries).to.deep.equal([
				[0, "after-register"],
			]);
		} finally {
			record.restore();
		}
	});

	it("records the root key of each put when its savepoint commits", async () => {
		const { store, journal, database } = await setupDocuments();
		const reader = journal.register();
		const atRelease: { end: number; staged: number }[] = [];
		const restore = hookExec(database, (sql) => {
			if (sql.startsWith("RELEASE SAVEPOINT peerbit_put_batch_")) {
				atRelease.push({ end: journal.end, staged: journal.staged });
			}
		});
		try {
			await store.put(doc("a", { tags: ["x", "y"], values: [1, 2] }));
			await store.put(doc("b"));
			await store.put(doc("a", { label: "replaced" }));
			// replace: false over an existing id falls back to a replacement
			await store.put(doc("b", { label: "kept" }), undefined, {
				replace: false,
			});
		} finally {
			restore();
		}
		// Staged while the savepoint is open, published once it is released.
		expect(atRelease).to.deep.equal([
			{ end: 0, staged: 1 },
			{ end: 1, staged: 1 },
			{ end: 2, staged: 1 },
			{ end: 3, staged: 1 },
		]);
		expect(consume(journal, reader)).to.deep.equal({
			rebase: false,
			entries: [
				[0, "a"],
				[0, "b"],
				[0, "a"],
				[0, "b"],
			],
		});
	});

	it("records every value of a putBatch across chunk boundaries, one commit per chunk", async () => {
		const { store, journal, database } = await setupDocuments();
		const reader = journal.register();
		const values = docs("batch", 130);
		const endAtRelease: number[] = [];
		const restore = hookExec(database, (sql) => {
			if (sql.startsWith("RELEASE SAVEPOINT peerbit_put_batch_")) {
				endAtRelease.push(journal.end);
			}
		});
		try {
			await store.putBatch!(values);
		} finally {
			restore();
		}
		expect(endAtRelease).to.deep.equal([0, 64, 128]);
		expect(journal.end).to.equal(130);
		expect(keysOf(consume(journal, reader).entries)).to.deep.equal(
			values.map((value) => value.id),
		);
	});

	it("drops the keys of a putBatch chunk that rolls back and keeps earlier chunks", async () => {
		const { store, journal } = await setupDocuments();
		const reader = journal.register();
		const firstChunk = docs("first", 64);
		const invalid = { id: "invalid" } as unknown as JournalDocument;

		await expectRejected(
			Promise.resolve(
				store.putBatch!([...firstChunk, doc("rolled-back"), invalid]),
			),
		);
		expect(await store.get(toId("rolled-back"))).to.equal(undefined);
		expect(await store.getSize()).to.equal(64);
		expect(journal.staged).to.equal(0);
		expect(keysOf(consume(journal, reader).entries)).to.deep.equal(
			firstChunk.map((value) => value.id),
		);
	});

	it("keeps no key of a put that rolls back, whether a nested row or its release fails", async () => {
		const { store, journal, database } = await setupDocuments();
		const reader = journal.register();
		await store.put(doc("previous", { tags: ["kept"] }));
		expect(consume(journal, reader).entries).to.deep.equal([[0, "previous"]]);

		const nestedFailure = new Error("nested row failed");
		expect(
			await expectRejected(
				Promise.resolve(
					store.put(
						failingNestedField(
							doc("previous", { tags: ["new"] }),
							nestedFailure,
						),
					),
				),
			),
		).to.equal(nestedFailure);
		expect((await store.get(toId("previous")))?.value.tags).to.deep.equal([
			"kept",
		]);

		const releaseFailure = new Error("release failed");
		let failRelease = true;
		const restore = hookExec(database, (sql) => {
			if (
				failRelease &&
				sql.startsWith("RELEASE SAVEPOINT peerbit_put_batch_")
			) {
				failRelease = false;
				throw releaseFailure;
			}
		});
		try {
			expect(
				await expectRejected(Promise.resolve(store.put(doc("released")))),
			).to.equal(releaseFailure);
		} finally {
			restore();
		}
		expect(await store.get(toId("released"))).to.equal(undefined);
		expect({
			staged: journal.staged,
			...consume(journal, reader),
		}).to.deep.equal({ staged: 0, rebase: false, entries: [] });

		await store.put(doc("later"));
		expect(consume(journal, reader).entries).to.deep.equal([[0, "later"]]);
	});

	it("records an ordered session's rows when each chunk commits at a flush", async () => {
		const { store, journal } = await setupDocuments();
		const reader = journal.register();
		const values = docs("ordered", 66);
		const seen: { end: number; staged: number }[] = [];
		await store.withOrderedWriteSession!(async (session) => {
			await session.put(values[0]);
			seen.push({ end: journal.end, staged: journal.staged });
			for (const value of values.slice(1, 65)) {
				await session.put(value);
			}
			// The 64th put flushed the first chunk; the 65th is staged.
			seen.push({ end: journal.end, staged: journal.staged });
			await session.flush();
			seen.push({ end: journal.end, staged: journal.staged });
			await session.put(values[65]);
			seen.push({ end: journal.end, staged: journal.staged });
		});
		expect(seen).to.deep.equal([
			{ end: 0, staged: 1 },
			{ end: 64, staged: 1 },
			{ end: 65, staged: 0 },
			{ end: 65, staged: 1 },
		]);
		expect(journal.end).to.equal(66);
		expect(keysOf(consume(journal, reader).entries)).to.deep.equal(
			values.map((value) => value.id),
		);
	});

	it("records an ordered session's committed prefix but not its failed row", async () => {
		const { store, journal } = await setupDocuments();
		const reader = journal.register();
		const failure = new Error("nested row failed");
		const error = await expectRejected(
			Promise.resolve(
				store.withOrderedWriteSession!(async (session) => {
					await session.put(doc("first"));
					await session.put(failingNestedField(doc("failed"), failure));
				}),
			),
		);
		expect(error).to.equal(failure);
		expect(await store.get(toId("first"))).not.to.equal(undefined);
		expect(await store.get(toId("failed"))).to.equal(undefined);
		expect(consume(journal, reader).entries).to.deep.equal([[0, "first"]]);
	});

	it("drops the key of an ordered row whose release fails and rolls back", async () => {
		const { store, journal, database } = await setupDocuments();
		const reader = journal.register();
		const failure = new Error("row release failed");
		let rowReleases = 0;
		const restore = hookExec(database, (sql) => {
			if (
				sql.startsWith("RELEASE SAVEPOINT peerbit_ordered_row_") &&
				++rowReleases === 2
			) {
				throw failure;
			}
		});
		let error: unknown;
		try {
			error = await expectRejected(
				Promise.resolve(
					store.withOrderedWriteSession!(async (session) => {
						await session.put(doc("first"));
						await session.put(doc("second"));
					}),
				),
			);
		} finally {
			restore();
		}
		expect(error).to.equal(failure);
		expect(await store.get(toId("first"))).not.to.equal(undefined);
		expect(await store.get(toId("second"))).to.equal(undefined);
		expect(journal.staged).to.equal(0);
		expect(consume(journal, reader).entries).to.deep.equal([[0, "first"]]);
	});

	it("records the root keys a delete by query removes", async () => {
		const { store, journal } = await setupDocuments();
		await store.putBatch!([
			doc("a", { label: "x", tags: ["child"] }),
			doc("b", { label: "y" }),
			doc("c", { label: "x" }),
			doc("d", { label: "x", values: [1] }),
		]);
		const reader = journal.register();

		const deleted = await store.del({
			query: [new StringMatch({ key: "label", value: "x" })],
		});
		expect(deleted.map((key) => key.primitive).sort()).to.deep.equal([
			"a",
			"c",
			"d",
		]);
		// Published as the DELETE commits, before any reader pulls.
		expect({ end: journal.end, staged: journal.staged }).to.deep.equal({
			end: 3,
			staged: 0,
		});
		const { entries } = consume(journal, reader);
		expect(
			entries.map(([table, key]) => `${table}:${key}`).sort(),
		).to.deep.equal(["0:a", "0:c", "0:d"]);

		// A delete that removes nothing records nothing.
		await store.del({
			query: [new StringMatch({ key: "label", value: "missing" })],
		});
		expect(journal.end).to.equal(3);
	});

	it("records the root key a delete by id removes", async () => {
		const { store, journal } = await setupDocuments();
		await store.put(doc("a"));
		await store.put(doc("b", { tags: ["child"] }));
		const reader = journal.register();

		await store.del({ query: [getMatcher(["id"], "b")] });
		expect(consume(journal, reader).entries).to.deep.equal([[0, "b"]]);
		await store.del({ query: [getMatcher(["id"], "b")] });
		expect(consume(journal, reader).entries).to.deep.equal([]);
	});

	it("records each key with its root table in a polymorphic schema", async () => {
		const { store } = await setup<Variant>({ schema: Variant });
		const journal = journalOf(store);
		const rootTables = (store as unknown as SQLiteIndex<Variant>).rootTables;
		expect(rootTables).to.have.length(2);
		const rootOf = (name: string) =>
			rootTables.findIndex((table) =>
				table.fields.some((field) => field.name === name),
			);
		const first = rootOf("first");
		const second = rootOf("second");
		expect([first, second].sort()).to.deep.equal([0, 1]);
		const reader = journal.register();

		await store.put(new FirstVariant("a"));
		await store.put(new SecondVariant("b"));
		await store.putBatch!([new SecondVariant("c"), new FirstVariant("d")]);
		expect(consume(journal, reader).entries).to.deep.equal([
			[first, "a"],
			[second, "b"],
			[second, "c"],
			[first, "d"],
		]);

		await store.del({
			query: [new StringMatch({ key: "group", value: "all" })],
		});
		const { entries } = consume(journal, reader);
		expect(
			entries.map(([table, key]) => `${table}:${key}`).sort(),
		).to.deep.equal([`${first}:a`, `${first}:d`, `${second}:b`, `${second}:c`]);
	});

	it("records keys in the form SQLite stores them, alike for puts and deletes", async () => {
		const u64 = await setup<U64Row>({ schema: U64Row });
		const u64Journal = journalOf(u64.store);
		const u64Reader = u64Journal.register();
		const max = 2n ** 64n - 1n;
		await u64.store.put(new U64Row(max));
		await u64.store.del({ query: [getMatcher(["id"], max)] });
		const u64Keys = keysOf(consume(u64Journal, u64Reader).entries);
		// u64 keys are stored shifted into the i64 range.
		expect(u64Keys.map((key) => BigInt(key as bigint))).to.deep.equal([
			max - 2n ** 63n,
			max - 2n ** 63n,
		]);

		const bytes = await setup<BytesRow>({ schema: BytesRow });
		const bytesJournal = journalOf(bytes.store);
		const bytesReader = bytesJournal.register();
		const key = new Uint8Array([1, 2, 3]);
		await bytes.store.put(new BytesRow(key));
		// A caller reusing the id buffer does not change the recorded key.
		key[0] = 9;
		await bytes.store.del({
			query: [getMatcher(["id"], new Uint8Array([1, 2, 3]))],
		});
		const bytesKeys = keysOf(consume(bytesJournal, bytesReader).entries);
		expect(bytesKeys.map((value) => [...(value as Uint8Array)])).to.deep.equal([
			[1, 2, 3],
			[1, 2, 3],
		]);

		// A Node Buffer's slice() shares memory, so the copy must not use it.
		if (typeof Buffer !== "undefined") {
			const buffer = Buffer.from([4, 5, 6]);
			await bytes.store.put(new BytesRow(buffer));
			buffer[0] = 9;
			const [recorded] = keysOf(consume(bytesJournal, bytesReader).entries);
			expect([...(recorded as Uint8Array)]).to.deep.equal([4, 5, 6]);
		}
	});

	it("records the key SQLite assigns to a root row put without an id", async () => {
		const { store } = await setup<AssignedIdRow>({ schema: AssignedIdRow });
		const journal = journalOf(store);
		const reader = journal.register();
		await store.put(new AssignedIdRow());
		await store.put(new AssignedIdRow());
		const put = keysOf(consume(journal, reader).entries);
		expect(put.map((key) => BigInt(key as bigint))).to.deep.equal([1n, 2n]);
		await store.del({
			query: [new StringMatch({ key: "label", value: "row" })],
		});
		const deleted = keysOf(consume(journal, reader).entries);
		expect(deleted.map((key) => BigInt(key as bigint)).sort()).to.deep.equal([
			1n,
			2n,
		]);
	});

	it("trims the entries every reader has read, down to the lowest reader", async () => {
		const { store, journal } = await setupDocuments();
		const slow = journal.register();
		const fast = journal.register();
		await store.put(doc("a"));
		await store.put(doc("b"));
		expect(keysOf(consume(journal, slow).entries)).to.deep.equal(["a", "b"]);
		// The fast reader has not read anything yet.
		expect({ start: journal.start, size: journal.size }).to.deep.equal({
			start: 0,
			size: 2,
		});

		await store.put(doc("c"));
		await store.put(doc("d"));
		expect(keysOf(consume(journal, fast).entries)).to.deep.equal([
			"a",
			"b",
			"c",
			"d",
		]);
		// Down to the slow reader, which has read up to "b".
		expect({ start: journal.start, size: journal.size }).to.deep.equal({
			start: 2,
			size: 2,
		});
		expect(keysOf(consume(journal, slow).entries)).to.deep.equal(["c", "d"]);
		expect({ start: journal.start, size: journal.size }).to.deep.equal({
			start: 4,
			size: 0,
		});

		// Unregistering the lowest reader trims to the next one.
		await store.put(doc("e"));
		consume(journal, fast);
		expect(journal.size).to.equal(1);
		journal.unregister(slow);
		expect({ start: journal.start, size: journal.size }).to.deep.equal({
			start: 5,
			size: 0,
		});

		// With no reader left, writes are not recorded.
		journal.unregister(fast);
		expect(journal.recording).to.equal(false);
		await store.put(doc("f"));
		expect({ end: journal.end, size: journal.size }).to.deep.equal({
			end: 5,
			size: 0,
		});
	});

	it("tells a reader behind the trimmed start to rebase, then resumes it", async () => {
		const { store, journal } = await setupDocuments();
		journal.capacity = 4;
		const idle = journal.register();
		for (const value of docs("k", 4)) {
			await store.put(value);
		}
		const active = journal.register();
		await store.put(doc("k-004"));
		// The fifth entry pushed the first out: the idle reader lost it.
		expect({ start: journal.start, end: journal.end }).to.deep.equal({
			start: 1,
			end: 5,
		});
		const behind = peek(journal, idle);
		expect(behind.pull.rebase).to.equal(true);
		expect(behind.entries).to.deep.equal([]);
		// A reader whose window is complete reads normally.
		const earlier = journal.pull(active);
		expect(consume(journal, active)).to.deep.equal({
			rebase: false,
			entries: [[0, "k-004"]],
		});

		// After rebuilding from the tables, the reader advances to the pull's end.
		journal.advance(idle, behind.pull);
		expect(idle.position).to.equal(5);
		await store.put(doc("k-005"));
		expect(consume(journal, idle)).to.deep.equal({
			rebase: false,
			entries: [[0, "k-005"]],
		});

		// Advancing with an earlier pull never moves a reader back.
		consume(journal, active);
		expect(active.position).to.equal(6);
		expect(earlier.end).to.equal(5);
		journal.advance(active, earlier);
		expect(active.position).to.equal(6);

		// An unregistered reader is told to rebase, and advancing does not
		// register it again.
		journal.unregister(idle);
		const unknown = journal.pull(idle);
		expect(unknown.rebase).to.equal(true);
		journal.advance(idle, unknown);
		expect(journal.pull(idle).rebase).to.equal(true);
		expect(journal.readerCount).to.equal(1);
	});

	it("keeps at most its capacity and sends only the readers that lost entries to rebase", async () => {
		const { store, journal } = await setupDocuments();
		journal.capacity = 10;
		const early = journal.register();
		await store.putBatch!(docs("first", 6));
		const late = journal.register();
		const second = docs("second", 6);
		await store.putBatch!(second);
		expect({
			start: journal.start,
			end: journal.end,
			size: journal.size,
		}).to.deep.equal({ start: 2, end: 12, size: 10 });
		expect(peek(journal, early)).to.deep.equal({
			pull: { epoch: journal.epoch, end: 12, rebase: true },
			entries: [],
		});
		expect(keysOf(consume(journal, late).entries)).to.deep.equal(
			second.map((value) => value.id),
		);

		// One chunk larger than the capacity overflows in a single commit.
		await store.putBatch!(docs("third", 25));
		expect({
			start: journal.start,
			end: journal.end,
			size: journal.size,
		}).to.deep.equal({ start: 27, end: 37, size: 10 });
		expect(peek(journal, late).pull.rebase).to.equal(true);
	});

	it("begins an epoch across stop() and start(), sending earlier readers to rebase", async () => {
		const { store, journal } = await setupDocuments();
		const before = journal.register();
		await store.put(doc("a"));
		const epoch = journal.epoch;
		const beforeStop = journal.pull(before);
		// stop() ends the epoch as the tables close, and drops every entry.
		await store.stop();
		expect(journal.epoch).to.be.greaterThan(epoch);
		expect(journal.size).to.equal(0);
		await store.start();
		const restarted = journal.epoch;
		expect(restarted).to.be.greaterThan(epoch);

		await store.put(doc("b"));
		// A pull from the earlier epoch cannot move the reader into this one.
		journal.advance(before, beforeStop);
		const stale = peek(journal, before);
		expect(stale.pull.rebase).to.equal(true);
		expect(stale.entries).to.deep.equal([]);
		// It rebuilds from the tables in the same admission, then advances, and
		// continues in the new epoch like a reader registered in it.
		journal.advance(before, stale.pull);
		expect(before.epoch).to.equal(restarted);
		// A pull from the earlier epoch cannot move it back there.
		journal.advance(before, beforeStop);
		expect(before.epoch).to.equal(restarted);
		const after = journal.register();
		await store.put(doc("c"));
		expect(consume(journal, before).entries).to.deep.equal([[0, "c"]]);
		expect(consume(journal, after).entries).to.deep.equal([[0, "c"]]);

		// drop() ends the epoch as well.
		await store.drop();
		await store.start();
		expect(journal.epoch).to.be.greaterThan(restarted);
		await store.put(doc("d"));
		expect(peek(journal, before).pull.rebase).to.equal(true);
		expect(peek(journal, after).pull.rebase).to.equal(true);
	});

	it("delivers keys staged by a write that neither committed nor rolled back", async () => {
		const { store, journal } = await setupDocuments();
		const reader = journal.register();
		// A write path that records without publishing: the next pull treats its
		// keys as committed, since a missing entry could hide a change.
		journal.record(0, "unpublished");
		expect({ staged: journal.staged, end: journal.end }).to.deep.equal({
			staged: 1,
			end: 0,
		});
		expect(consume(journal, reader).entries).to.deep.equal([
			[0, "unpublished"],
		]);
		await store.put(doc("a"));
		expect(consume(journal, reader).entries).to.deep.equal([[0, "a"]]);
	});

	it("leaves key scans to their own invalidation: they neither register nor read the journal", async () => {
		const { store, journal } = await setupDocuments();
		await store.put(doc("a"));
		await store.put(doc("b"));
		const scan = store.scanKeyPrimitives!({ pageSize: 1 });
		expect(await scan.next()).to.deep.equal({ status: "more", keys: ["a"] });
		expect(journal.readerCount).to.equal(0);
		await store.put(doc("c"));
		expect(await scan.next()).to.deep.equal({
			status: "invalidated",
			keys: [],
		});
		expect(journal.end).to.equal(0);

		const reader = journal.register();
		const complete = store.scanKeyPrimitives!({ pageSize: 10 });
		expect(await complete.next()).to.deep.equal({
			status: "complete",
			keys: ["a", "b", "c"],
		});
		await store.del({ query: [getMatcher(["id"], "a")] });
		expect(consume(journal, reader).entries).to.deep.equal([[0, "a"]]);
		expect(journal.readerCount).to.equal(1);
	});
});
