import { BinaryWriter, field, serializer, variant } from "@dao-xyz/borsh";
import {
	Compare,
	type IndexIterator,
	IntegerCompare,
	Sort,
	id,
	toId,
} from "@peerbit/indexer-interface";
import { expect } from "chai";
import { type RustIndex, type RustIndices, create } from "../src/index.js";

@variant("rust_iterator_rescan_mutation")
class Row {
	static cloneError: Error | undefined;
	@id({ type: "u32" })
	id: number;
	@field({ type: "u32" })
	value: number;
	constructor(id: number, value = 0) {
		this.id = id;
		this.value = value;
	}
	@serializer()
	serializeValue(writer: BinaryWriter): void {
		if (Row.cloneError) throw Row.cloneError;
		writer.u32(this.id);
		writer.u32(this.value);
	}
}

const ids = (start: number, end: number) =>
	Array.from({ length: end - start }, (_, i) => i + start);
const deleteRow = (id: number) => ({
	query: new IntegerCompare({ key: "id", compare: Compare.Equal, value: id }),
});

describe("Rust mutation rescan coherence", () => {
	let indices: RustIndices;
	let index: RustIndex<Row>;
	let iterators: IndexIterator<Row, undefined>[];
	let work: Promise<unknown>[];
	let restoreRead: (() => void) | undefined;

	beforeEach(async () => {
		iterators = [];
		Row.cloneError = undefined;
		work = [];
		restoreRead = undefined;
		indices = create();
		await indices.start();
		index = await indices.init({ schema: Row });
		expect(index["nativeBackboneDocumentIndex"]).to.equal(undefined);
		expect(index["native"]?.query_page).to.be.a("function");
		await index.putBatch(ids(0, 1152).map((id) => new Row(id)));
	});
	afterEach(async () => {
		try {
			for (const iterator of iterators) await iterator.close();
			await Promise.allSettled(work);
		} finally {
			Row.cloneError = undefined;
			restoreRead?.();
			await indices?.drop();
		}
	});
	const primed = async () => {
		const iterator = index.iterate<undefined>({
			sort: [new Sort({ key: "id" })],
		});
		iterators.push(iterator);
		expect(
			(await iterator.next(64)).map((row) => row.id.primitive),
		).to.deep.equal(ids(0, 64));
		await index.put(new Row(1151, 1));
		return iterator;
	};
	const observePages = (afterPage: (ids: number[], page: number) => void) => {
		const original = index["getNativeCandidatesForPlan"];
		let pages = 0;
		index["getNativeCandidatesForPlan"] = function (plan) {
			const rows = original.call(this, plan);
			if (plan.limit != null) {
				afterPage(
					rows.map((row) => row.id.primitive as number),
					++pages,
				);
			}
			return rows;
		};
		restoreRead = () => (index["getNativeCandidatesForPlan"] = original);
		return () => pages;
	};

	for (const mode of ["next", "pending"] as const) {
		it(`keeps ${mode} coherent when a deletion is queued after a real native page`, async () => {
			const iterator = await primed();
			let deletion: ReturnType<typeof index.del> | undefined;
			let firstPage: number[] | undefined;
			let queued = false;
			const pages = observePages((pageIds, page) => {
				if (page !== 1) return;
				firstPage = pageIds;
				queued = true;
				queueMicrotask(() => {
					deletion = index.del(deleteRow(0));
					work.push(deletion);
				});
			});
			const result =
				mode === "next"
					? await iterator.next(Infinity)
					: await iterator.pending();
			expect(queued).to.equal(true);
			expect(pages()).to.be.greaterThan(1);
			expect(firstPage).to.deep.equal(ids(0, mode === "next" ? 1024 : 128));
			expect(deletion).not.to.equal(undefined);
			expect((await deletion!).map((id) => id.primitive)).to.deep.equal([0]);
			expect(await index.get(toId(0))).to.equal(undefined);
			if (mode === "next") {
				const actual = (
					result as Awaited<ReturnType<typeof iterator.next>>
				).map((row) => row.id.primitive);
				expect(new Set(actual).size).to.equal(actual.length);
				expect(actual).to.deep.equal(ids(64, 1152));
				expect(iterator.done()).to.equal(true);
				expect(await iterator.pending()).to.equal(0);
			} else {
				expect(result).to.equal(1088);
				await index.put(new Row(1152, 7));
				expect(await iterator.pending()).to.equal(1089);
				expect(
					(await iterator.next(Infinity)).map((row) => row.id.primitive),
				).to.deep.equal(ids(64, 1153));
				expect(await iterator.pending()).to.equal(0);
			}
		});
	}

	it("does not consume unseen IDs when a later real native page throws", async () => {
		const iterator = await primed();
		const sentinel = new Error("later native page delivery failed");
		let injected = false;
		const pages = observePages((_ids, page) => {
			if (page === 2) {
				injected = true;
				throw sentinel;
			}
		});
		const error = await Promise.resolve(iterator.next(Infinity)).then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(injected).to.equal(true);
		expect(pages()).to.equal(2);
		expect(error).to.equal(sentinel);
		const retried = (await iterator.next(Infinity)).map(
			(row) => row.id.primitive,
		);
		expect(retried).to.deep.equal(ids(64, 1152));
		expect(await iterator.pending()).to.equal(0);
		expect(iterator.done()).to.equal(true);
	});

	it("keeps committed writes visible between public iterator calls", async () => {
		const iterator = await primed();
		await index.del(deleteRow(0));
		await index.put(new Row(1151, 7));
		await index.put(new Row(1152, 8));
		expect(await iterator.pending()).to.equal(1089);
		const rows = await iterator.next(Infinity);
		expect(rows.map((row) => row.id.primitive)).to.deep.equal(ids(64, 1153));
		expect(rows.slice(-2).map((row) => row.value.value)).to.deep.equal([7, 8]);
		expect(await iterator.pending()).to.equal(0);
		expect(iterator.done()).to.equal(true);
	});

	it("does not advance an ordinary iterator when real result cloning throws", async () => {
		const iterator = index.iterate<undefined>({
			sort: [new Sort({ key: "id" })],
		});
		iterators.push(iterator);
		expect(
			(await iterator.next(64)).map((row) => row.id.primitive),
		).to.deep.equal(ids(0, 64));
		const sentinel = new Error("result serialization failed");
		Row.cloneError = sentinel;
		const error = await Promise.resolve(iterator.next(1)).then(
			() => undefined,
			(error: unknown) => error,
		);
		Row.cloneError = undefined;
		expect(error).to.equal(sentinel);
		expect(
			(await iterator.next(1)).map((row) => row.id.primitive),
		).to.deep.equal([64]);
		expect(await iterator.pending()).to.equal(1087);
	});
});
