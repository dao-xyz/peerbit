import { expect } from "chai";
import {
	type NativeLogEntry,
	calculateRawCidV1,
	createLogGraphIndex,
	createNativeLogBlockStore,
	encodeEntryV0Signable,
	encodeEntryV0SignableBatch,
	encodeEntryV0Storage,
	encodeEntryV0StorageBatchWithCids,
	encodeEntryV0StorageWithCid,
	prepareEntryV0PlainChain,
	signEd25519,
} from "../src/index.js";

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

const absoluteReplicaData = (value: number) =>
	new Uint8Array([
		0,
		value & 0xff,
		(value >>> 8) & 0xff,
		(value >>> 16) & 0xff,
		(value >>> 24) & 0xff,
	]);

const bytes = (length: number, offset = 0) =>
	Uint8Array.from({ length }, (_, index) => (index + offset) & 0xff);

const fromHex = (hex: string) =>
	Uint8Array.from(
		hex.match(/.{2}/g)?.map((byte) => Number.parseInt(byte, 16)) ?? [],
	);

const TS_BORSH_ENTRY_V0_FIXTURE = {
	withMeta: {
		signable:
			"0000005e0000000000210000000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20210015cd5b070000000007000000050000006769642d6102000000060000006e6578742d61060000006e6578742d62000103000000090807000009000000000400000001020304000000000000",
		storage:
			"0000005e0000000000210000000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20210015cd5b070000000007000000050000006769642d6102000000060000006e6578742d61060000006e6578742d62000103000000090807000009000000000400000001020304000000000100010000000000670000000040000000606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f00404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f0000",
		cid: "zb2rhXpjPn9fDgku56mickZTNbZDfiWmZRy5WDnHjAeLB8Yqa",
	},
	noMeta: {
		signable:
			"000000490000000000210000000b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b00b168de3a00000000000000000b0000006769642d6e6f2d6d65746100000000000000000a00000000050000000504030201000000000000",
	},
};

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

	it("tracks heads and next adjacency in native batches", async () => {
		const index = await createLogGraphIndex();
		index.putBatch([
			entry("a", "g", [], 1n),
			entry("b", "g", ["a"], 2n),
			entry("c", "g", ["b"], 3n),
		]);

		expect(index.heads()).to.deep.equal(["c"]);
		expect(index.children("a")).to.deep.equal(["b"]);
		expect(index.children("b")).to.deep.equal(["c"]);
		expect(index.payloadSizeSum()).to.equal(3);
	});

	it("tracks heads and next adjacency in native append chains", async () => {
		const index = await createLogGraphIndex();
		index.put(entry("root", "g", [], 1n));
		index.putAppendChain([
			entry("a", "g", ["root"], 2n),
			entry("b", "g", ["a"], 3n),
			entry("c", "g", ["b"], 4n),
		]);

		expect(index.heads()).to.deep.equal(["c"]);
		expect(index.children("root")).to.deep.equal(["a"]);
		expect(index.children("a")).to.deep.equal(["b"]);
		expect(index.children("b")).to.deep.equal(["c"]);
		expect(index.payloadSizeSum()).to.equal(4);
	});

	it("filters heads by gid and clock order", async () => {
		const index = await createLogGraphIndex();
		index.put(entry("b", "one", [], 2n));
		index.put(entry("a", "one", [], 1n));
		index.put(entry("c", "two", [], 3n));

		expect(index.heads()).to.deep.equal(["a", "b", "c"]);
		expect(index.heads("one")).to.deep.equal(["a", "b"]);
		expect(index.heads("two")).to.deep.equal(["c"]);
		expect(index.hasHead()).equal(true);
		expect(index.hasHead("one")).equal(true);
		expect(index.hasHead("two")).equal(true);
		expect(index.hasHead("missing")).equal(false);
		expect(index.hasAnyHead(["missing", "two"])).equal(true);
		expect(index.hasAnyHead(["missing"])).equal(false);
		expect(
			index.hasAnyHeadBatch([["missing", "two"], ["missing"], []]),
		).to.deep.equal([true, false, false]);
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
			data: absoluteReplicaData(9),
		});

		const heads = index.headDataEntries("one");
		expect(heads).to.have.length(1);
		expect(heads[0]!.hash).equal("a");
		expect([...(heads[0]!.meta.data ?? [])]).to.deep.equal([0, 9, 0, 0, 0]);
	});

	it("computes max u32 from shaped head metadata", async () => {
		const index = await createLogGraphIndex();
		index.put({
			...entry("a", "one", [], 1n),
			data: absoluteReplicaData(2),
		});
		index.put({
			...entry("b", "one", [], 2n),
			data: absoluteReplicaData(5),
		});
		index.put({
			...entry("c", "two", [], 3n),
			data: absoluteReplicaData(9),
		});

		expect(index.maxHeadDataU32("one")).equal(5);
		expect(index.maxHeadDataU32("two")).equal(9);
		expect(index.maxHeadDataU32("missing")).equal(undefined);
		expect(index.maxHeadDataU32Batch(["one", "two", "missing"])).to.deep.equal([
			5,
			9,
			undefined,
		]);
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

	it("plans unique reference gids for exchange heads", async () => {
		const index = await createLogGraphIndex();
		index.put(entry("root", "root-gid", [], 1n));
		index.put(entry("same-gid-parent", "root-gid", [], 2n));
		index.put(entry("side", "side-gid", [], 3n));
		index.put(entry("branch", "branch-gid", ["root", "side"], 4n));
		index.put(entry("head", "head-gid", ["branch", "same-gid-parent"], 5n));

		expect(index.uniqueReferenceGids("head")).to.deep.equal([
			"branch-gid",
			"root-gid",
			"side-gid",
		]);
		expect(index.uniqueReferenceGidRowsBatch(["head", "missing"])).to.deep.equal([
			[
				["branch", "branch-gid"],
				["same-gid-parent", "root-gid"],
				["side", "side-gid"],
			],
			undefined,
		]);
		expect(index.uniqueReferenceGids("missing")).to.equal(undefined);

		index.put(entry("cut", "cut-gid", ["head"], 6n, CUT));
		expect(index.uniqueReferenceGids("cut")).to.deep.equal([]);

		index.put(entry("incomplete", "incomplete-gid", ["not-indexed"], 7n));
		expect(index.uniqueReferenceGids("incomplete")).to.equal(undefined);
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

describe("native EntryV0 encoding", () => {
	it("signs Ed25519 bytes with the expected RFC 8032 test vector", async () => {
		expect([
			...(await signEd25519({
				privateKey: fromHex(
					"9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
				),
				publicKey: fromHex(
					"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
				),
				data: new Uint8Array(),
			})),
		]).to.deep.equal([
			...fromHex(
				"e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b",
			),
		]);
	});

	it("matches TS/Borsh signable, storage, and raw CID bytes", async () => {
		const clockId = bytes(33, 1);
		const publicKeyBytes = bytes(32, 64);
		const signatureBytes = bytes(64, 96);
		const metaData = new Uint8Array([9, 8, 7]);
		const payloadData = new Uint8Array([1, 2, 3, 4]);
		const wallTime = 123456789n;
		const logical = 7;
		const gid = "gid-a";
		const next = ["next-a", "next-b"];

		const nativeSignable = await encodeEntryV0Signable({
			clockId,
			wallTime,
			logical,
			gid,
			next,
			type: APPEND,
			metaData,
			payloadData,
		});

		expect([...nativeSignable]).to.deep.equal([
			...fromHex(TS_BORSH_ENTRY_V0_FIXTURE.withMeta.signable),
		]);

		const nativeStorage = await encodeEntryV0Storage({
			clockId,
			wallTime,
			logical,
			gid,
			next,
			type: APPEND,
			metaData,
			payloadData,
			signature: signatureBytes,
			signaturePublicKey: publicKeyBytes,
			prehash: 0,
		});

		expect([...nativeStorage]).to.deep.equal([
			...fromHex(TS_BORSH_ENTRY_V0_FIXTURE.withMeta.storage),
		]);
		expect(await calculateRawCidV1(nativeStorage)).to.equal(
			TS_BORSH_ENTRY_V0_FIXTURE.withMeta.cid,
		);
		expect(
			await encodeEntryV0StorageWithCid({
				clockId,
				wallTime,
				logical,
				gid,
				next,
				type: APPEND,
				metaData,
				payloadData,
				signature: signatureBytes,
				signaturePublicKey: publicKeyBytes,
				prehash: 0,
			}),
		).to.deep.equal({
			bytes: nativeStorage,
			cid: TS_BORSH_ENTRY_V0_FIXTURE.withMeta.cid,
		});
	});

	it("matches TS/Borsh encoding without optional entry metadata", async () => {
		const clockId = bytes(33, 11);
		const payloadData = new Uint8Array([5, 4, 3, 2, 1]);
		const wallTime = 987654321n;
		const gid = "gid-no-meta";

		expect([
			...(await encodeEntryV0Signable({
				clockId,
				wallTime,
				gid,
				payloadData,
			})),
		]).to.deep.equal([...fromHex(TS_BORSH_ENTRY_V0_FIXTURE.noMeta.signable)]);
	});

	it("batches independent TS/Borsh-compatible encodes", async () => {
		const withMeta = {
			clockId: bytes(33, 1),
			wallTime: 123456789n,
			logical: 7,
			gid: "gid-a",
			next: ["next-a", "next-b"],
			type: APPEND,
			metaData: new Uint8Array([9, 8, 7]),
			payloadData: new Uint8Array([1, 2, 3, 4]),
		};
		const noMeta = {
			clockId: bytes(33, 11),
			wallTime: 987654321n,
			gid: "gid-no-meta",
			payloadData: new Uint8Array([5, 4, 3, 2, 1]),
		};

		const signables = await encodeEntryV0SignableBatch([withMeta, noMeta]);
		expect(signables.map((bytes) => [...bytes])).to.deep.equal([
			[...fromHex(TS_BORSH_ENTRY_V0_FIXTURE.withMeta.signable)],
			[...fromHex(TS_BORSH_ENTRY_V0_FIXTURE.noMeta.signable)],
		]);

		const storage = await encodeEntryV0StorageBatchWithCids([
			{
				...withMeta,
				signature: bytes(64, 96),
				signaturePublicKey: bytes(32, 64),
				prehash: 0,
			},
		]);
		expect(storage).to.have.length(1);
		expect([...storage[0]!.bytes]).to.deep.equal([
			...fromHex(TS_BORSH_ENTRY_V0_FIXTURE.withMeta.storage),
		]);
		expect(storage[0]!.cid).to.equal(TS_BORSH_ENTRY_V0_FIXTURE.withMeta.cid);
	});

	it("prepares a hash-linked plain entry chain natively", async () => {
		const privateKey = fromHex(
			"9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
		);
		const publicKey = fromHex(
			"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
		);
		const chain = await prepareEntryV0PlainChain({
			clockId: publicKey,
			privateKey,
			publicKey,
			wallTimes: [11n, 12n, 13n],
			gid: "chain-gid",
			initialNext: ["root"],
			payloadDatas: [
				new Uint8Array([1]),
				new Uint8Array([2]),
				new Uint8Array([3]),
			],
		});

		expect(chain).to.have.length(3);
		expect(chain[0]!.next).to.deep.equal(["root"]);
		expect(chain[1]!.next).to.deep.equal([chain[0]!.cid]);
		expect(chain[2]!.next).to.deep.equal([chain[1]!.cid]);
		for (const prepared of chain) {
			expect(prepared.cid).to.equal(await calculateRawCidV1(prepared.bytes));
			expect(prepared.signature).to.have.length(64);
			expect(prepared.metaBytes.byteLength).greaterThan(0);
			expect(prepared.payloadBytes.byteLength).greaterThan(0);
			expect(prepared.signatureBytes.byteLength).greaterThan(0);
		}
	});

	it("commits prepared plain entry blocks and graph rows natively", async () => {
		const privateKey = fromHex(
			"9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
		);
		const publicKey = fromHex(
			"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
		);
		const index = await createLogGraphIndex();
		const blockStore = await createNativeLogBlockStore();
		index.put(entry("root", "chain-gid", [], 10n));

		const chain = await index.prepareEntryV0PlainChainCommit(
			{
				clockId: publicKey,
				privateKey,
				publicKey,
				wallTimes: [11n, 12n, 13n],
				gid: "chain-gid",
				initialNext: ["root"],
				payloadDatas: [
					new Uint8Array([1]),
					new Uint8Array([2]),
					new Uint8Array([3]),
				],
			},
			blockStore,
		);

		expect(chain).to.have.length(3);
		expect(index.heads()).to.deep.equal([chain![2]!.cid]);
		expect(index.children("root")).to.deep.equal([chain![0]!.cid]);
		for (const prepared of chain!) {
			expect(prepared.bytes).equal(undefined);
			const stored = await blockStore.get(prepared.cid);
			expect(stored?.byteLength).equal(prepared.byteLength);
			expect(await calculateRawCidV1(stored!)).equal(prepared.cid);
		}
		expect(await blockStore.size()).to.equal(
			chain!.reduce((sum, prepared) => sum + prepared.byteLength, 0),
		);
	});
});
