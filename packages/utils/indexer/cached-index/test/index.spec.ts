/* eslint-env mocha */
import { field } from "@dao-xyz/borsh";
import { id } from "@peerbit/indexer-interface";
import type { IterateOptions } from "@peerbit/indexer-interface";
import { HashmapIndex } from "@peerbit/indexer-simple";
import { waitForResolved } from "@peerbit/time";
import { delay } from "@peerbit/time";
import { expect } from "chai";
import type { QueryCacheOptions } from "../src/cache.js";
import { CachedIndex } from "../src/index.js";

/* ---------------------------------------------------------------- model */

class Document {
	@id({ type: "string" }) id!: string;
	@field({ type: "string" }) content!: string;
	constructor(p: { id: string; content: string }) {
		Object.assign(this, p);
	}
}

/* ---------------------------------------------------------------- helpers */

const makeDb = async (
	size = 200,
	cacheOpt: QueryCacheOptions = {
		strategy: "auto",
		maxSize: 50,
		maxTotalSize: 150,
		prefetchThreshold: 2,
	},
) => {
	const raw = new HashmapIndex<Document>();
	await raw.init({ schema: Document, indexBy: ["id"] });
	for (let i = 0; i < size; i++) {
		await raw.put(new Document({ id: i.toString(), content: "#" + i }));
	}
	return new CachedIndex(raw, cacheOpt);
};

const collect = async (it: any, limit = 1_000) => {
	const out: Document[] = [];
	while (!it.done() && out.length < limit) {
		out.push(...(await it.next(100)).map((r: any) => r.value));
	}
	await it.close();
	return out;
};

/** wait until `warmKeys.length >= n` or 500 ms */
const waitForWarmEntries = async (db: CachedIndex<any>, n: number) => {
	const deadline = Date.now() + 500;
	while (Date.now() < deadline) {
		if (db.iteratorCache!._debugStats.activeQueries.length >= n) return;
		await new Promise((r) => setTimeout(r, 10));
	}
};

/* ---------------------------------------------------------------- tests */

describe("CachedIndex iterator cache", () => {
	it("switches seamlessly from prefetched to underlying iterator", async () => {
		const size = 180;
		const db = await makeDb(size, {
			prefetchThreshold: 1,
			keepAlive: 1e4,
			maxSize: 60,
			maxTotalSize: 1e4,
			strategy: "auto",
		});
		let it = db.iterate();
		await it.close();

		await waitForWarmEntries(db, 1); // warm up

		it = db.iterate();
		const b1 = await it.next(50);
		const b2 = await it.next(160);
		expect(b1.length + b2.length).to.equal(size);
	});

	it("does not prefetch until query used N times", async () => {
		let N = 3;
		const db = await makeDb(10, {
			strategy: "auto",
			maxSize: 50,
			maxTotalSize: 150,
			prefetchThreshold: N,
			keepAlive: 3e3, // timeout for prefetch threshold hit
		});
		const opts: IterateOptions = { query: { id: "5" } };

		for (let i = 0; i < N; i++) {
			await collect(db.iterate(opts));
			if (i < N - 1) {
				expect(db.iteratorCache._debugStats.activeQueries.length).to.equal(0);
			} else {
				await waitForWarmEntries(db, 1);
				expect(
					db.iteratorCache._debugStats.activeQueries.length,
				).to.be.greaterThan(0);
			}
		}
	});

	it("eventually prunes queries that does not lead to prefetch", async () => {
		let prefetchThreshold = 2;
		let keepAlive = 3e3;
		const db = await makeDb(200, {
			strategy: "auto",
			maxSize: 50,
			maxTotalSize: 150,
			prefetchThreshold,
			keepAlive, // timeout for prefetch threshold hit
		});

		/* helper: fetch *one* row then close */
		const touch = async (opts: IterateOptions) => {
			const it = db.iterate(opts);
			await it.next(1);
			await it.close();
		};

		await touch({ query: { id: "1" } }); // 1st hit (new id)
		await touch({ query: { id: "2" } }); // 1st hit (new id)
		await touch({ query: { id: "3" } }); // 1st hit (new id)
		await touch({ query: { id: "3" } }); // 2st hit (prefetchThreshold)

		await waitForResolved(() =>
			expect(db.iteratorCache!._debugStats.cachedQueries).to.equal(3),
		);

		await delay(keepAlive + 100); // wait for prefetch threshold to be hit
		await db.iteratorCache?.pruneStale();

		expect(db.iteratorCache!._debugStats.cachedQueries).to.equal(1);

		/* → inside eviction test (final assertions) */
		const stats = db.iteratorCache!._debugStats;
		expect(stats.activeQueries).to.have.length.at.most(3);
		expect(stats.prefetchedRows).to.be.at.most(150);
	});

	it("does not prunes queries that are active", async () => {
		let prefetchThreshold = 1;
		let keepAlive = 3e3;
		const db = await makeDb(200, {
			strategy: "auto",
			maxSize: 50,
			maxTotalSize: 150,
			prefetchThreshold,
			keepAlive, // timeout for prefetch threshold hit
		});

		const touch = async (opts: IterateOptions) => {
			const it = db.iterate(opts);
			await it.next(1);
			await it.close();
		};

		await touch({ query: { id: "1" } }); // 1st hit (new id)
		await touch({ query: { id: "2" } }); // 1st hit (new id)
		await delay(keepAlive + 100); // wait for prefetch threshold to be hit
		await db.iteratorCache?.pruneStale();
		expect(db.iteratorCache!._debugStats.cachedQueries).to.equal(2);
		const stats = db.iteratorCache!._debugStats;
		expect(stats.activeQueries).to.have.length(2);
	});

	it("evicts least-popular queries when maxTotalSize exceeded", async () => {
		const db = await makeDb(200, {
			maxTotalSize: 3,
			maxSize: 1,
			strategy: "auto",
			prefetchThreshold: 2,
			keepAlive: 1e4,
		});

		/* helper: fetch *one* row then close */
		const touch = async (opts: IterateOptions) => {
			const it = db.iterate(opts);
			await it.next(1);
			await it.close();
		};

		// hit three distinct queries twice → triggers warm-up
		for (let q = 0; q < 3; q++) {
			await touch({ query: { id: q.toString() } }); // 1st hit
			await touch({ query: { id: q.toString() } }); // 2nd hit → warm
		}

		await waitForWarmEntries(db, 3); // make sure they’re warm
		expect(db.iteratorCache!._debugStats.activeQueries).to.have.lengthOf(3);

		// a 4th warm query forces eviction (maxTotalSize = 3 → only 1 * 3 fit)
		await touch({ query: { id: "150" } });
		await touch({ query: { id: "150" } });
		await waitForWarmEntries(db, 3);

		const stats = db.iteratorCache!._debugStats;
		expect(stats.activeQueries).to.to.have.length(3); // still three warmers
		expect(stats.queryIsActive({ query: { id: "150" } })).to.be.true;
		expect(stats.queryIsActive({ query: { id: "0" } })).to.be.false; // evicted
	});

	it("re-calculates cache after put / del", async () => {
		const db = await makeDb();
		const all = () => collect(db.iterate({}));

		expect((await all()).some((d) => d.id === "999")).to.be.false;

		await db.put(new Document({ id: "999", content: "hello" }));
		expect((await all()).some((d) => d.id === "999")).to.be.true;

		await db.del({ query: { id: "999" } });
		expect((await all()).some((d) => d.id === "999")).to.be.false;
	});

	it("enforces maxSize and maxTotalSize", async () => {
		const db = await makeDb(200, {
			strategy: "auto",
			prefetchThreshold: 1,
			maxSize: 30,
			maxTotalSize: 90,
		});

		for (let q = 0; q < 4; q++) {
			await collect(db.iterate({ query: { id: q.toString() } }));
		}
		await waitForWarmEntries(db, 3); // at most 3 warmers fit
		for (const key of db.iteratorCache._debugStats.activeQueries) {
			expect(db.iteratorCache._debugStats.getCached(key)?.cached.size).to.eq(
				30,
			);
		}

		expect(db.iteratorCache._debugStats.activeQueries).to.to.have.length(3); /// 30 * 3 = 90
	});
});
