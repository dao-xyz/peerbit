import { field, variant } from "@dao-xyz/borsh";
import { type IndexIterator, Sort, id, toId } from "@peerbit/indexer-interface";
import { expect } from "chai";
import sinon from "sinon";
import { SQLiteIndex } from "../src/engine.js";
import { createDatabase } from "../src/index.js";
import type { Database, Statement } from "../src/types.js";

@variant("sqlite_iterator_close_inflight")
class Row {
	@id({ type: "u32" })
	id: number;
	@field({ type: "u32" })
	value: number;
	constructor(id: number, value = 0) {
		this.id = id;
		this.value = value;
	}
}

const gate = () => {
	let resolve!: () => void;
	const promise = new Promise<void>((done) => {
		resolve = done;
	});
	return { promise, resolve };
};

describe("SQLite in-flight iterator close", () => {
	let db: Database;
	let index: SQLiteIndex<Row>;
	let iterators: IndexIterator<Row, undefined>[];
	let releases: (() => void)[];
	let pending: Promise<unknown>[];
	let selects: number;
	let holdSelect: (() => Promise<void>) | undefined;

	beforeEach(async () => {
		iterators = [];
		releases = [];
		pending = [];
		selects = 0;
		holdSelect = undefined;
		db = await createDatabase();
		await db.open();
		index = new SQLiteIndex<Row>({ scope: [], db, schema: Row }).init({
			schema: Row,
		});
		await index.start();
		await index.putBatch(Array.from({ length: 1152 }, (_, id) => new Row(id)));
		const prepare = db.prepare.bind(db);
		const wrapped = new Set<Statement>();
		sinon.stub(db, "prepare").callsFake(async (sql, id) => {
			const statement = await prepare(sql, id);
			if (!wrapped.has(statement)) {
				wrapped.add(statement);
				const all = statement.all.bind(statement);
				sinon.stub(statement, "all").callsFake(async (...args) => {
					// Paged queries can begin with a WITH clause.
					const isSelect = /\bselect\b/i.test(sql);
					if (isSelect) selects++;
					const result = await all(...args);
					if (isSelect) await holdSelect?.();
					return result;
				});
				if (/\bcount\s*\(/i.test(sql)) {
					const get = statement.get.bind(statement);
					sinon.stub(statement, "get").callsFake(async (...args) => {
						selects++;
						const result = await get(...args);
						await holdSelect?.();
						return result;
					});
				}
			}
			return statement;
		});
	});

	afterEach(async () => {
		try {
			for (const release of releases) release();
			for (const iterator of iterators) await iterator.close();
			await Promise.all(pending);
		} finally {
			sinon.restore();
			try {
				await index?.stop();
			} finally {
				await db?.close();
			}
		}
	});

	const iterate = () => {
		const iterator = index.iterate<undefined>({
			sort: [new Sort({ key: "id" })],
		});
		iterators.push(iterator);
		return iterator;
	};
	const track = <T>(operation: Promise<T>): Promise<T> => {
		pending.push(
			operation.then(
				() => undefined,
				() => undefined,
			),
		);
		return operation;
	};
	const holdPage = (pageNumber: number) => {
		const entered = gate(),
			release = gate();
		releases.push(release.resolve);
		let pages = 0;
		// Preserve real SQLite and admission. Hold one completed SQL page before
		// delivering it, without depending on the number of admission windows.
		holdSelect = async () => {
			if (++pages === pageNumber) {
				entered.resolve();
				await release.promise;
			}
		};
		return { entered: entered.promise, release: release.resolve };
	};
	const waitEntered = async (
		entered: Promise<void>,
		operation: Promise<unknown>,
	) => {
		await Promise.race([
			entered,
			operation.then(() => {
				throw new Error(
					"operation completed before the controlled page boundary",
				);
			}),
		]);
	};
	const assertTerminal = async (
		iterator: IndexIterator<Row, undefined>,
		selectCount: number,
	) => {
		expect(await iterator.next(1)).to.deep.equal([]);
		expect(await iterator.all()).to.deep.equal([]);
		expect(await iterator.pending()).to.equal(0);
		expect(iterator.done()).to.equal(true);
		expect(index.cursor.size).to.equal(0);
		expect(selects).to.equal(selectCount);
		await index.put(new Row(2000, 7));
		expect((await index.get(toId(2000)))?.value.value).to.equal(7);
	};

	for (const mode of ["next", "pending", "all"] as const) {
		it(`keeps mutation ${mode} terminal after closing between internal pages`, async () => {
			const iterator = iterate();
			expect(
				(await iterator.next(64)).map((row) => row.value.id),
			).to.deep.equal(Array.from({ length: 64 }, (_, id) => id));
			await index.put(new Row(1151, 1));
			const held = holdPage(mode === "all" ? 3 : 2);
			const operation = track(
				Promise.resolve(
					mode === "next"
						? iterator.next(200)
						: mode === "all"
							? iterator.all()
							: iterator.pending(),
				),
			);
			await waitEntered(held.entered, operation);
			await iterator.close();
			const atClose = selects;
			expect(atClose).to.be.greaterThan(0);
			held.release();
			expect(await operation).to.deep.equal(mode === "pending" ? 0 : []);
			await assertTerminal(iterator, atClose);
		});
	}

	for (const mode of ["next", "all"] as const) {
		it(`discards a fresh ${mode} page closed before delivery`, async () => {
			const iterator = iterate();
			const held = holdPage(1);
			const operation = track(
				Promise.resolve(mode === "next" ? iterator.next(10) : iterator.all()),
			);
			await waitEntered(held.entered, operation);
			await iterator.close();
			const atClose = selects;
			expect(atClose).to.be.greaterThan(0);
			held.release();
			expect(await operation).to.deep.equal([]);
			await assertTerminal(iterator, atClose);
		});
	}

	it("does not execute a page closed while queued for database admission", async () => {
		const iterator = iterate();
		const acquire = index["acquireDatabaseBarrier"].bind(index);
		const release = await acquire();
		releases.push(release);
		const entered = gate();
		sinon.stub(index as any, "acquireDatabaseBarrier").callsFake(() => {
			entered.resolve();
			return acquire();
		});
		const operation = track(Promise.resolve(iterator.next(10)));
		await waitEntered(entered.promise, operation);
		expect(selects).to.equal(0);
		await iterator.close();
		release();
		expect(await operation).to.deep.equal([]);
		await assertTerminal(iterator, 0);
	});

	it("discards a pending count closed before delivery", async () => {
		const iterator = iterate();
		await iterator.next(64);
		const entered = gate(),
			release = gate();
		releases.push(release.resolve);
		holdSelect = async () => {
			entered.resolve();
			await release.promise;
		};
		const operation = track(Promise.resolve(iterator.pending()));
		await waitEntered(entered.promise, operation);
		await iterator.close();
		const atClose = selects;
		release.resolve();
		expect(await operation).to.equal(0);
		await assertTerminal(iterator, atClose);
	});

	it("discards an admitted SQL read closed before its result resolves", async () => {
		const iterator = iterate();
		const entered = gate(),
			release = gate();
		releases.push(release.resolve);
		holdSelect = async () => {
			entered.resolve();
			await release.promise;
		};
		const operation = track(Promise.resolve(iterator.next(10)));
		await waitEntered(entered.promise, operation);
		expect(selects).to.equal(1);
		await iterator.close();
		release.resolve();
		expect(await operation).to.deep.equal([]);
		await assertTerminal(iterator, 1);
	});

	it("preserves exact rows and pending counts for an open mutation iterator", async () => {
		const iterator = iterate();
		const first = await iterator.next(64);
		expect(await iterator.pending()).to.equal(1088);
		await index.put(new Row(1151, 7));
		const second = await iterator.next(200);
		expect(await iterator.pending()).to.equal(888);
		const remaining = await iterator.all();
		expect(
			[...first, ...second, ...remaining].map((row) => row.value.id),
		).to.deep.equal(Array.from({ length: 1152 }, (_, id) => id));
		expect(remaining.at(-1)?.value.value).to.equal(7);
		expect(await iterator.pending()).to.equal(0);
		expect(iterator.done()).to.equal(true);
	});

	it("preserves a real SQLite error while the iterator remains open", async () => {
		const iterator = iterate();
		await iterator.next(1);
		// Destroy only this test's in-memory table to provoke a genuine driver
		// error rather than replacing the iterator's implementation with a throw.
		await db.exec(`DROP TABLE ${index.rootTables[0]!.name}`);
		const error = await track(Promise.resolve(iterator.next(1))).then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(error).to.be.instanceOf(Error);
		// The browser worker forwards the SQLite message, not its native code.
		expect((error as Error).message).to.include("no such table");
		expect(index.closed).to.equal(false);
	});
});
