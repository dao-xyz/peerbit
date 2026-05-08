import { expect } from "chai";
import { type NativeLogEntry, createLogGraphIndex } from "../src/index.js";

const APPEND = 0;
const CUT = 1;

const entry = (
	hash: string,
	gid: string,
	next: string[] = [],
	wallTime = 1n,
	type = APPEND,
): NativeLogEntry => ({
	hash,
	gid,
	next,
	type,
	head: true,
	payloadSize: 1,
	clock: { timestamp: { wallTime, logical: 0 } },
});

describe("native log graph index", () => {
	it("tracks heads and next adjacency", async () => {
		const index = await createLogGraphIndex();
		index.put(entry("a", "g", [], 1n));
		expect(index.heads()).to.deep.equal(["a"]);

		index.put(entry("b", "g", ["a"], 2n));
		expect(index.heads()).to.deep.equal(["b"]);
		expect(index.children("a")).to.deep.equal(["b"]);
		expect(index.countHasNext("a")).to.equal(1);

		index.put(entry("c", "g", ["a"], 3n));
		expect(index.heads()).to.deep.equal(["b", "c"]);
		expect(index.countHasNext("a")).to.equal(2);

		expect(index.delete("b")).to.equal(true);
		expect(index.heads()).to.deep.equal(["c"]);
		expect(index.countHasNext("a")).to.equal(1);

		expect(index.delete("c")).to.equal(true);
		expect(index.heads()).to.deep.equal(["a"]);
		expect(index.countHasNext("a")).to.equal(0);
	});

	it("filters heads by gid and clock order", async () => {
		const index = await createLogGraphIndex();
		index.put(entry("b", "one", [], 2n));
		index.put(entry("a", "one", [], 1n));
		index.put(entry("c", "two", [], 3n));

		expect(index.heads()).to.deep.equal(["a", "b", "c"]);
		expect(index.heads("one")).to.deep.equal(["a", "b"]);
		expect(index.heads("two")).to.deep.equal(["c"]);
	});

	it("returns sortable head metadata for append planning", async () => {
		const index = await createLogGraphIndex();
		index.put(entry("b", "one", [], 2n));
		index.put(entry("a", "one", [], 1n));
		index.put(entry("c", "two", [], 3n));

		expect(index.headEntries("one")).to.deep.equal([
			{
				hash: "a",
				meta: {
					gid: "one",
					clock: { timestamp: { wallTime: 1n, logical: 0 } },
				},
			},
			{
				hash: "b",
				meta: {
					gid: "one",
					clock: { timestamp: { wallTime: 2n, logical: 0 } },
				},
			},
		]);

		expect(index.joinHeadEntries("one")).to.deep.equal([
			{
				hash: "a",
				meta: {
					gid: "one",
					type: APPEND,
					next: [],
					clock: { timestamp: { wallTime: 1n, logical: 0 } },
				},
			},
			{
				hash: "b",
				meta: {
					gid: "one",
					type: APPEND,
					next: [],
					clock: { timestamp: { wallTime: 2n, logical: 0 } },
				},
			},
		]);
	});

	it("returns shaped head metadata", async () => {
		const index = await createLogGraphIndex();
		index.put({
			...entry("a", "one", [], 1n),
			data: new Uint8Array([7, 8, 9]),
		});

		const heads = index.headDataEntries("one");
		expect(heads).to.have.length(1);
		expect(heads[0]!.hash).equal("a");
		expect([...(heads[0]!.meta.data ?? [])]).to.deep.equal([7, 8, 9]);
	});

	it("does not demote nexts for cut entries", async () => {
		const index = await createLogGraphIndex();
		index.put(entry("a", "g", [], 1n));
		index.put(entry("cut", "g", ["a"], 2n, CUT));

		expect(index.heads()).to.deep.equal(["a", "cut"]);
		expect(index.countHasNext("a")).to.equal(1);

		expect(index.delete("cut")).to.equal(true);
		expect(index.heads()).to.deep.equal(["a"]);
	});

	it("reports shadowed gids for cross-gid nexts", async () => {
		const index = await createLogGraphIndex();
		index.put(entry("a", "old", [], 1n));

		expect(index.shadowedGids("new", ["a"], "b")).to.deep.equal(["old"]);

		index.put(entry("c", "other", ["a"], 2n));
		expect(index.shadowedGids("new", ["a"], "b")).to.deep.equal([]);
	});

	it("batches membership checks", async () => {
		const index = await createLogGraphIndex();
		index.put(entry("a", "g", [], 1n));
		index.put(entry("c", "g", [], 3n));

		expect([...index.hasMany(["missing", "a", "c"])]).to.deep.equal(["a", "c"]);
	});

	it("sums payload sizes", async () => {
		const index = await createLogGraphIndex();
		index.put({ ...entry("a", "g", [], 1n), payloadSize: 7 });
		index.put({ ...entry("b", "g", [], 2n), payloadSize: 9 });

		expect(index.payloadSizeSum()).to.equal(16);

		index.delete("a");
		expect(index.payloadSizeSum()).to.equal(9);
	});

	it("returns child join entries for cut recursion", async () => {
		const index = await createLogGraphIndex();
		index.put(entry("a", "g", [], 1n));
		index.put(entry("b", "g", ["a"], 2n));
		index.put(entry("cut", "g", ["a"], 3n, CUT));

		expect(
			index.childJoinEntries("a").map((entry) => [entry.hash, entry.meta.type]),
		).to.deep.equal([
			["b", APPEND],
			["cut", CUT],
		]);
	});

	it("plans recursive cut deletes", async () => {
		const index = await createLogGraphIndex();
		index.put(entry("root", "g", [], 1n));
		index.put(entry("child", "g", ["root"], 2n));
		index.put(entry("cut", "g", ["child"], 3n, CUT));

		expect(index.planDeleteRecursively(["cut"], true)).to.deep.equal([
			"child",
			"root",
		]);

		const branched = await createLogGraphIndex();
		branched.put(entry("root", "g", [], 1n));
		branched.put(entry("child", "g", ["root"], 2n));
		branched.put(entry("sibling", "g", ["root"], 3n));
		branched.put(entry("cut", "g", ["child"], 4n, CUT));

		expect(branched.planDeleteRecursively(["cut"], true)).to.deep.equal([
			"child",
		]);
	});

	it("plans joins with missing parents", async () => {
		const index = await createLogGraphIndex();
		index.put(entry("a", "g", [], 1n));

		expect(index.planJoin("b", ["a", "missing"], APPEND)).to.deep.equal({
			skip: false,
			missingParents: ["missing"],
			cutChecked: false,
			coveredByCut: false,
		});
		expect(index.planJoin("a", [], APPEND)).to.deep.equal({
			skip: true,
			missingParents: [],
			cutChecked: false,
			coveredByCut: false,
		});
		expect(index.planJoin("a", [], APPEND, true)).to.deep.equal({
			skip: false,
			missingParents: [],
			cutChecked: false,
			coveredByCut: false,
		});
		expect(index.planJoin("cut", ["missing"], CUT)).to.deep.equal({
			skip: false,
			missingParents: [],
			cutChecked: false,
			coveredByCut: false,
		});
	});

	it("plans cut-covered joins", async () => {
		const index = await createLogGraphIndex();
		index.put(entry("cut", "g", ["old"], 2n, CUT));

		expect(
			index.planJoin("old", ["missing"], APPEND, false, {
				gid: "g",
				wallTime: 1n,
				logical: 0,
			}),
		).to.deep.equal({
			skip: false,
			missingParents: [],
			cutChecked: true,
			coveredByCut: true,
		});
		expect(
			index.planJoin("new", ["missing"], APPEND, false, {
				gid: "g",
				wallTime: 3n,
				logical: 0,
			}),
		).to.deep.equal({
			skip: false,
			missingParents: ["missing"],
			cutChecked: true,
			coveredByCut: false,
		});
	});
});
