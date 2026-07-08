import { field, variant } from "@dao-xyz/borsh";
import {
	type Indices,
	Sort,
	SortDirection,
	getIdProperty,
	id,
} from "@peerbit/indexer-interface";
import { expect } from "chai";

// Cross-backend SORT TIE-BREAK parity: @peerbit/indexer-simple (HashmapIndex) and
// @peerbit/indexer-sqlite3 must break ties on equal sort keys IDENTICALLY, in the
// document primary-key id's natural typed order:
//   - string ids    -> UTF-8 byte order   (sqlite TEXT / BINARY collation)
//   - integer ids    -> NUMERIC order       (sqlite INTEGER: 2 < 10, not "10" < "2")
//   - Uint8Array ids -> raw-byte memcmp     (sqlite BLOB: 0x00 < 0xff, not base64)
// and REVERSE that id order when the (single) sort field is DESC.
//
// Every document shares sortKey=1 so the whole result set is one tie group, which
// forces the id tie-break for every row. Simple historically ordered ties by
// INSERTION order (direction-independent), matching neither sqlite nor a stable
// content order; this suite locks simple == sqlite.

@variant("tie_str")
class StringIdDoc {
	@id({ type: "string" })
	id: string;

	@field({ type: "u32" })
	sortKey: number;

	constructor(id: string) {
		this.id = id;
		this.sortKey = 1;
	}
}

@variant("tie_bigint")
class BigIntIdDoc {
	@id({ type: "u64" })
	id: bigint;

	@field({ type: "u32" })
	sortKey: number;

	constructor(id: bigint) {
		this.id = id;
		this.sortKey = 1;
	}
}

@variant("tie_bytes")
class BytesIdDoc {
	@id({ type: Uint8Array })
	id: Uint8Array;

	@field({ type: "u32" })
	sortKey: number;

	constructor(id: Uint8Array) {
		this.id = id;
		this.sortKey = 1;
	}
}

const fmt = (value: string | number | bigint | Uint8Array): string =>
	value instanceof Uint8Array
		? Array.from(value)
				.map((x) => x.toString(16).padStart(2, "0"))
				.join("")
		: String(value);

export const tieParityTests = (
	createIndicies: (directory?: string) => Indices | Promise<Indices>,
) => {
	return describe("sort tie-break parity", () => {
		let indices: Indices | undefined;

		afterEach(async () => {
			await indices?.stop?.();
			await indices?.drop?.();
			indices = undefined;
		});

		const collect = async (schema: any, docs: any[]) => {
			indices = await createIndicies();
			await indices.start();
			const store = await indices.init({
				schema,
				indexBy: getIdProperty(schema) || ["id"],
			});
			for (const doc of docs) {
				await store.put(doc);
			}
			const order = async (direction: SortDirection) => {
				const results = await store
					.iterate({
						query: [],
						sort: [new Sort({ key: "sortKey", direction })],
					})
					.all();
				return results.map((r) => fmt(r.value.id));
			};
			const asc = await order(SortDirection.ASC);
			const desc = await order(SortDirection.DESC);
			await store.stop?.();
			await indices.stop?.();
			await indices.drop?.();
			indices = undefined;
			return { asc, desc };
		};

		it("orders string-id ties by UTF-8 bytes, both directions", async () => {
			const docs = ["c", "a", "b", "aa", "A", "Z"].map((s) => new StringIdDoc(s));
			const { asc, desc } = await collect(StringIdDoc, docs);
			// UTF-8 byte order: uppercase (A, Z) before lowercase (a, aa, b, c).
			expect(asc).to.deep.equal(["A", "Z", "a", "aa", "b", "c"]);
			expect(desc).to.deep.equal(["c", "b", "aa", "a", "Z", "A"]);
		});

		it("orders bigint-id ties numerically, both directions", async () => {
			const docs = [10n, 2n, 21n, 9n, 100n, 300n].map((n) => new BigIntIdDoc(n));
			const { asc, desc } = await collect(BigIntIdDoc, docs);
			expect(asc).to.deep.equal(["2", "9", "10", "21", "100", "300"]);
			expect(desc).to.deep.equal(["300", "100", "21", "10", "9", "2"]);
		});

		it("orders Uint8Array-id ties by raw bytes, both directions", async () => {
			const docs = [[0xff], [0x00], [0x0a], [0x3e], [0xf8], [0xfb]].map(
				(b) => new BytesIdDoc(new Uint8Array(b)),
			);
			const { asc, desc } = await collect(BytesIdDoc, docs);
			// Raw-byte memcmp: 0x00 < 0x0a < 0x3e < 0xf8 < 0xfb < 0xff.
			expect(asc).to.deep.equal(["00", "0a", "3e", "f8", "fb", "ff"]);
			expect(desc).to.deep.equal(["ff", "fb", "f8", "3e", "0a", "00"]);
		});
	});
};
