import { field } from "@dao-xyz/borsh";
import { ready } from "@peerbit/crypto";
import {
	type Index,
	type IndexKeyScan,
	type IndexKeyScanOptions,
	type IterateOptions,
	NotStartedError,
	type Shape,
	StringMatch,
	id,
} from "@peerbit/indexer-interface";
import { HashmapIndex } from "@peerbit/indexer-simple";
import { expect } from "chai";
import { CachedIndex } from "../src/index.js";

class KeyScanDocument {
	@id({ type: "string" })
	id: string;

	@field({ type: "string" })
	value: string;

	constructor(id: string, value = id) {
		this.id = id;
		this.value = value;
	}
}

const cacheOptions = {
	strategy: "auto" as const,
	maxSize: 4,
	maxTotalSize: 8,
	prefetchThreshold: 1,
};

const makeOrigin = async () => {
	const origin = new HashmapIndex<KeyScanDocument>();
	await origin.init({ schema: KeyScanDocument });
	await origin.start();
	await origin.put(new KeyScanDocument("a"));
	await origin.put(new KeyScanDocument("b"));
	return origin;
};

// Await the actual bounded fixture warm-up, not a polling/timing assumption.
const finishWarmup = async (cached: CachedIndex<KeyScanDocument>) => {
	const cache = cached.iteratorCache as unknown as {
		pendingJob: Map<string, Promise<void>>;
	};
	await Promise.all([...cache.pendingJob.values()]);
};

const cleanup = async (cached: CachedIndex<KeyScanDocument>) => {
	try {
		await finishWarmup(cached);
	} finally {
		await cached.stop();
	}
};

describe("CachedIndex key primitive scan forwarding", () => {
	before(async () => {
		await ready;
	});

	it("keeps the capability absent for an origin that does not implement it", async () => {
		const raw = await makeOrigin();
		// Deliberately use a plain facade: subclassing Simple would inherit its
		// new optional capability and would not exercise an unsupported origin.
		const origin: Index<KeyScanDocument> = {
			init: raw.init.bind(raw),
			start: raw.start.bind(raw),
			stop: raw.stop.bind(raw),
			drop: raw.drop.bind(raw),
			get: raw.get.bind(raw),
			put: raw.put.bind(raw),
			del: raw.del.bind(raw),
			sum: raw.sum.bind(raw),
			count: raw.count.bind(raw),
			getSize: raw.getSize.bind(raw),
			persisted: raw.persisted.bind(raw),
			iterate: () => {
				throw new Error("unsupported inventory must not fall back to iterate");
			},
		};
		const cached = new CachedIndex(origin, cacheOptions);
		try {
			expect(Object.hasOwn(origin, "scanKeyPrimitives")).to.equal(false);
			expect(origin.scanKeyPrimitives).to.equal(undefined);
			expect(cached.scanKeyPrimitives).to.equal(undefined);
			expect(cached.iteratorCache._debugStats.cachedQueries).to.equal(0);
		} finally {
			await cleanup(cached);
		}
	});

	it("forwards the exact options and cursor with origin binding and no query cache work", async () => {
		const origin = await makeOrigin();
		const originalScan = origin.scanKeyPrimitives;
		let receivedOptions: IndexKeyScanOptions | undefined;
		let originalCursor: IndexKeyScan | undefined;
		origin.scanKeyPrimitives = function (options) {
			expect(this).to.equal(origin);
			receivedOptions = options;
			originalCursor = originalScan.call(this, options);
			return originalCursor;
		};
		origin.iterate = () => {
			throw new Error("inventory must not create a query iterator");
		};
		const cached = new CachedIndex(origin, cacheOptions);
		cached.iteratorCache.acquire = () => {
			throw new Error("inventory must not acquire the query cache");
		};
		cached.iteratorCache.refresh = async () => {
			throw new Error("inventory must not refresh the query cache");
		};
		const controller = new AbortController();
		const options = { pageSize: 1, signal: controller.signal };
		try {
			const detached = cached.scanKeyPrimitives!;
			const scan = detached(options);
			expect(receivedOptions).to.equal(options);
			expect(scan).to.equal(originalCursor);
			expect(scan).not.to.be.instanceOf(Promise);
			expect(await scan.next()).to.deep.equal({ status: "more", keys: ["a"] });
			controller.abort();
			expect(await scan.next()).to.deep.equal({ status: "aborted", keys: [] });
			expect(cached.iteratorCache._debugStats.cachedQueries).to.equal(0);
		} finally {
			await originalCursor?.close();
			await cleanup(cached);
		}
	});

	it("returns fresh unfiltered membership despite an already warm filtered query", async () => {
		const origin = await makeOrigin();
		const originalIterate = origin.iterate.bind(origin);
		let queryCalls = 0;
		origin.iterate = <S extends Shape | undefined = undefined>(
			request?: IterateOptions,
			options?: { shape?: S; reference?: boolean },
		) => {
			queryCalls++;
			return originalIterate<S>(request, options);
		};
		const cached = new CachedIndex(origin, cacheOptions);
		let scan: IndexKeyScan | undefined;
		const query = { query: new StringMatch({ key: "id", value: "a" }) };
		try {
			const iterator = cached.iterate(query);
			await iterator.close();
			await finishWarmup(cached);
			expect(cached.iteratorCache._debugStats.queryIsActive(query)).to.equal(
				true,
			);
			expect(cached.iteratorCache._debugStats.prefetchedRows).to.equal(1);
			// A direct origin write intentionally does not refresh the query cache.
			await origin.put(new KeyScanDocument("c"));
			const callsBeforeScan = queryCalls;
			scan = cached.scanKeyPrimitives!({ pageSize: 2 });
			expect(await scan.next()).to.deep.equal({
				status: "more",
				keys: ["a", "b"],
			});
			expect(await scan.next()).to.deep.equal({
				status: "complete",
				keys: ["c"],
			});
			expect(queryCalls).to.equal(callsBeforeScan);
			expect(cached.iteratorCache._debugStats.queryIsActive(query)).to.equal(
				true,
			);
			expect(cached.iteratorCache._debugStats.prefetchedRows).to.equal(1);
		} finally {
			await scan?.close();
			await cleanup(cached);
		}
	});

	for (const through of ["origin", "wrapper"] as const) {
		it(`retains invalidation for replacement writes through the ${through}`, async () => {
			const origin = await makeOrigin();
			const cached = new CachedIndex(origin, cacheOptions);
			const scan = cached.scanKeyPrimitives!({ pageSize: 1 });
			try {
				expect(await scan.next()).to.deep.equal({
					status: "more",
					keys: ["a"],
				});
				await (through === "origin" ? origin : cached).put(
					new KeyScanDocument("b", "replacement"),
				);
				expect(await scan.next()).to.deep.equal({
					status: "invalidated",
					keys: [],
				});
				expect(await scan.next()).to.deep.equal({
					status: "invalidated",
					keys: [],
				});
			} finally {
				await scan.close();
				await cleanup(cached);
			}
		});
	}

	it("preserves synchronous origin lifecycle checks and terminal cursor ownership", async () => {
		const origin = await makeOrigin();
		const cached = new CachedIndex(origin, cacheOptions);
		const detached = cached.scanKeyPrimitives!;
		const oldScan = detached({ pageSize: 1 });
		let freshScan: IndexKeyScan | undefined;
		try {
			await origin.stop();
			expect(() => detached({ pageSize: 1 })).to.throw(NotStartedError);
			expect(await oldScan.next()).to.deep.equal({
				status: "closed",
				keys: [],
			});
			await cached.start();
			expect(await oldScan.next()).to.deep.equal({
				status: "closed",
				keys: [],
			});
			freshScan = detached({ pageSize: 2 });
			expect(freshScan).not.to.be.instanceOf(Promise);
			expect(await freshScan.next()).to.deep.equal({
				status: "complete",
				keys: ["a", "b"],
			});
			expect(() => detached({ pageSize: 0 })).to.throw(RangeError);
		} finally {
			await oldScan.close();
			await freshScan?.close();
			await cleanup(cached);
		}
	});
});
