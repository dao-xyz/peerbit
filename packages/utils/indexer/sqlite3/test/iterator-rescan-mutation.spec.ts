import { field, variant } from "@dao-xyz/borsh";
import {
	Compare,
	type IndexIterator,
	IntegerCompare,
	Sort,
	id,
	toId,
} from "@peerbit/indexer-interface";
import { expect } from "chai";
import sinon from "sinon";
import { SQLiteIndex } from "../src/engine.js";
import { createDatabase } from "../src/index.js";
import type { Database, Statement } from "../src/types.js";

@variant("sqlite_iterator_rescan_mutation")
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
	const promise = new Promise<void>((done) => (resolve = done));
	return { promise, resolve };
};
const ids = (start: number, end: number) =>
	Array.from({ length: end - start }, (_, i) => i + start);
const deleteRow = (id: number) => ({
	query: new IntegerCompare({ key: "id", compare: Compare.Equal, value: id }),
});

describe("SQLite mutation rescan admission", () => {
	let db: Database;
	let index: SQLiteIndex<Row>;
	let iterators: IndexIterator<Row, undefined>[];
	let releases: (() => void)[];
	let pending: Promise<unknown>[];
	let afterPage: (() => Promise<void>) | undefined;
	let pageReads: number;

	beforeEach(async () => {
		iterators = [];
		releases = [];
		pending = [];
		afterPage = undefined;
		pageReads = 0;
		db = await createDatabase();
		await db.open();
		index = new SQLiteIndex<Row>({ scope: [], db, schema: Row }).init({
			schema: Row,
		});
		await index.start();
		await index.putBatch(ids(0, 1152).map((id) => new Row(id)));
		// Plan DELETE before holding a read, so its observed admission is the
		// actual delete operation, not lazy planner DDL queued before it.
		expect(await index.del(deleteRow(5000))).to.deep.equal([]);
		const prepare = db.prepare.bind(db);
		const wrapped = new Set<Statement>();
		sinon.stub(db, "prepare").callsFake(async (sql, id) => {
			const statement = await prepare(sql, id);
			if (/limit \? offset \?/i.test(sql) && !wrapped.has(statement)) {
				wrapped.add(statement);
				const all = statement.all.bind(statement);
				sinon.stub(statement, "all").callsFake(async (...args) => {
					const result = await all(...args);
					pageReads++;
					await afterPage?.();
					return result;
				});
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
	const track = <T>(operation: Promise<T>): Promise<T> => {
		pending.push(
			operation.then(
				() => undefined,
				() => undefined,
			),
		);
		return operation;
	};
	const iterate = () => {
		const iterator = index.iterate<undefined>({
			sort: [new Sort({ key: "id" })],
		});
		iterators.push(iterator);
		return iterator;
	};
	const primed = async () => {
		const iterator = iterate();
		expect((await iterator.next(64)).map((row) => row.value.id)).to.deep.equal(
			ids(0, 64),
		);
		await index.put(new Row(1151, 1));
		return iterator;
	};
	const holdPage = (pageNumber = 1, error?: Error) => {
		const entered = gate(),
			release = gate();
		releases.push(release.resolve);
		let pages = 0;
		afterPage = async () => {
			if (++pages !== pageNumber) return;
			afterPage = undefined;
			entered.resolve();
			await release.promise;
			if (error) throw error;
		};
		return { entered: entered.promise, release: release.resolve };
	};
	const observeAdmission = (attempts = 1) => {
		const entered = gate();
		const acquire = index["acquireDatabaseBarrier"].bind(index);
		let observed = 0;
		sinon.stub(index as any, "acquireDatabaseBarrier").callsFake(() => {
			const admitted = acquire();
			if (++observed === attempts) entered.resolve();
			return admitted;
		});
		return entered.promise;
	};
	const enteredBeforeCompletion = async (
		entered: Promise<void>,
		operation: Promise<unknown>,
	) => {
		await Promise.race([
			entered,
			operation.then(() => {
				throw new Error("operation completed before controlled boundary");
			}),
		]);
	};

	it("does not omit an unseen row when deletion queues during a rescan page", async () => {
		const iterator = await primed();
		const held = holdPage();
		const scan = track(Promise.resolve(iterator.next(Infinity)));
		await enteredBeforeCompletion(held.entered, scan);
		const admission = observeAdmission();
		let deleted = false;
		const deletion = track(
			index.del(deleteRow(0)).then((result) => {
				deleted = true;
				return result;
			}),
		);
		await enteredBeforeCompletion(admission, deletion);
		expect(deleted).to.equal(false);
		held.release();
		const rows = await scan;
		expect((await deletion).map((id) => id.primitive)).to.deep.equal([0]);
		const actual = rows.map((row) => row.value.id);
		expect(
			ids(64, 1152).filter((id) => !actual.includes(id)),
			"missing IDs",
		).to.deep.equal([]);
		expect(new Set(actual).size).to.equal(actual.length);
		expect(actual).to.deep.equal(ids(64, 1152));
		expect(iterator.done()).to.equal(true);
		expect(await iterator.pending()).to.equal(0);
		expect(await index.get(toId(0))).to.equal(undefined);
	});

	it("counts one coherent unseen set while deletion queues during pending", async () => {
		const iterator = await primed();
		const held = holdPage();
		const count = track(Promise.resolve(iterator.pending()));
		await enteredBeforeCompletion(held.entered, count);
		const admission = observeAdmission();
		const deletion = track(index.del(deleteRow(0)));
		await enteredBeforeCompletion(admission, deletion);
		held.release();
		expect(await count).to.equal(1088);
		expect((await deletion).map((id) => id.primitive)).to.deep.equal([0]);
		expect(
			(await iterator.next(Infinity)).map((row) => row.value.id),
		).to.deep.equal(ids(64, 1152));
		expect(iterator.done()).to.equal(true);
		expect(await iterator.pending()).to.equal(0);
	});

	for (const mode of ["next", "pending"] as const) {
		it(`chooses ${mode} mutation handling after an earlier queued delete commits`, async () => {
			const iterator = iterate();
			await iterator.next(64);
			const acquire = index["acquireDatabaseBarrier"].bind(index);
			const release = await acquire();
			releases.push(release);
			const first = gate(),
				second = gate();
			let attempts = 0;
			sinon.stub(index as any, "acquireDatabaseBarrier").callsFake(() => {
				const admitted = acquire();
				if (++attempts === 1) first.resolve();
				if (attempts === 2) second.resolve();
				return admitted;
			});
			const deletion = track(index.del(deleteRow(0)));
			await enteredBeforeCompletion(first.promise, deletion);
			const operation = track(
				Promise.resolve(
					mode === "next" ? iterator.next(64) : iterator.pending(),
				),
			);
			await enteredBeforeCompletion(second.promise, operation);
			release();
			const result = await operation;
			await deletion;
			if (mode === "next") {
				expect(
					(result as Awaited<ReturnType<typeof iterator.next>>).map(
						(row) => row.value.id,
					),
				).to.deep.equal(ids(64, 128));
			} else {
				expect(result).to.equal(1088);
			}
		});
	}

	it("finishes a two-page rescan with repeated same-row writes queued", async () => {
		const iterator = await primed();
		const held = holdPage();
		const before = pageReads;
		const scan = track(Promise.resolve(iterator.next(Infinity)));
		await enteredBeforeCompletion(held.entered, scan);
		const admission = observeAdmission(8);
		const writes = track(
			Promise.all(ids(2, 10).map((value) => index.put(new Row(1151, value)))),
		);
		await enteredBeforeCompletion(admission, writes);
		held.release();
		const rows = await scan;
		await writes;
		expect(rows.map((row) => row.value.id)).to.deep.equal(ids(64, 1152));
		expect(pageReads - before).to.equal(2);
		expect((await index.get(toId(1151)))?.value.value).to.equal(9);
		expect(await iterator.pending()).to.equal(0);
	});

	it("keeps completed mutations visible between public calls", async () => {
		const iterator = iterate();
		await iterator.next(64);
		await index.del(deleteRow(0));
		await index.put(new Row(1151, 7));
		await index.put(new Row(1152, 8));
		const rows = await iterator.next(Infinity);
		expect(rows.map((row) => row.value.id)).to.deep.equal(ids(64, 1153));
		expect(rows.slice(-2).map((row) => row.value.value)).to.deep.equal([7, 8]);
		expect(await iterator.pending()).to.equal(0);
		expect(iterator.done()).to.equal(true);
	});

	it("preserves result IDs when a mutation rescan projects out the primary field", async () => {
		const iterator = index.iterate(
			{ sort: [new Sort({ key: "id" })] },
			{ shape: { value: true } },
		);
		try {
			const first = await iterator.next(64);
			expect(first.map((row) => row.id.primitive)).to.deep.equal(ids(0, 64));
			for (const row of first) expect(row.value).not.to.have.property("id");
			await index.put(new Row(1151, 7));
			expect(await iterator.pending()).to.equal(1088);
			const remaining = await iterator.next(Infinity);
			expect(remaining.map((row) => row.id.primitive)).to.deep.equal(
				ids(64, 1152),
			);
			for (const row of remaining) expect(row.value).not.to.have.property("id");
			expect(remaining.at(-1)?.value.value).to.equal(7);
			expect(await iterator.pending()).to.equal(0);
			expect(iterator.done()).to.equal(true);
		} finally {
			await iterator.close();
		}
	});

	it("releases queued writes without consuming unseen IDs after a later-page error", async () => {
		const iterator = await primed();
		const sentinel = new Error("later page read failed");
		const held = holdPage(2, sentinel);
		const scan = track(Promise.resolve(iterator.next(Infinity)));
		await enteredBeforeCompletion(held.entered, scan);
		const admission = observeAdmission();
		const write = track(index.put(new Row(2000, 7)));
		await enteredBeforeCompletion(admission, write);
		held.release();
		const error = await scan.then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(error).to.equal(sentinel);
		await write;
		expect((await index.get(toId(2000)))?.value.value).to.equal(7);
		const retried = (await iterator.next(Infinity)).map((row) => row.value.id);
		expect(retried).to.deep.equal([...ids(64, 1152), 2000]);
		await iterator.close();
		expect(await iterator.pending()).to.equal(0);
		expect(iterator.done()).to.equal(true);
	});

	for (const marked of [64, 1024]) {
		it(`applies explicit mark ${marked} received during a later page`, async () => {
			const iterator = await primed();
			const held = holdPage(2);
			const scan = track(Promise.resolve(iterator.next(Infinity)));
			await enteredBeforeCompletion(held.entered, scan);
			await iterator.markYielded!([toId(marked)]);
			held.release();
			expect((await scan).map((row) => row.value.id)).to.deep.equal(
				ids(64, 1152).filter((id) => id !== marked),
			);
			expect(await iterator.pending()).to.equal(0);
		});
	}
});
