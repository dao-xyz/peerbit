import { expect } from "chai";
import sinon from "sinon";
import {
	BoundedEagerBlockCache,
	normalizeEagerBlocksOptions,
} from "../src/eager-cache.js";

describe("bounded eager-block cache", () => {
	it("keeps byte and entry accounting exact across eviction and replacement", () => {
		const cache = new BoundedEagerBlockCache({
			maxEntries: 2,
			maxBytes: 6,
			ttlMs: 10_000,
		});

		expect(cache.add("a", new Uint8Array(4))).to.equal(true);
		expect(cache.add("b", new Uint8Array(3))).to.equal(true);
		expect(cache.get("a")).to.equal(undefined);
		expect(cache.stats()).to.include({ entries: 1, bytes: 3, evictions: 1 });

		expect(cache.add("b", new Uint8Array(5))).to.equal(true);
		expect(cache.stats()).to.include({ entries: 1, bytes: 5 });
		cache.del("b");
		expect(cache.stats()).to.include({ entries: 0, bytes: 0 });

		// The historical lazy-delete cache drifted after this cycle and could
		// bypass its capacity. A fresh insert must still have exact accounting.
		expect(cache.add("b", new Uint8Array(6))).to.equal(true);
		cache.del("b");
		expect(cache.add("c", new Uint8Array(6))).to.equal(true);
		expect(cache.stats()).to.include({ entries: 1, bytes: 6 });
		cache.clear();
	});

	it("releases expired buffers without requiring a later cache access", () => {
		const clock = sinon.useFakeTimers({ now: 1_000 });
		try {
			const cache = new BoundedEagerBlockCache({
				maxEntries: 2,
				maxBytes: 8,
				ttlMs: 100,
			});
			cache.add("a", new Uint8Array(4));
			clock.tick(99);
			expect(cache.stats().entries).to.equal(1);
			clock.tick(1);
			expect(cache.stats()).to.include({
				entries: 0,
				bytes: 0,
				expirations: 1,
			});
		} finally {
			clock.restore();
		}
	});

	it("copies aliased views and bounds zero-byte duplicate entries", () => {
		const cache = new BoundedEagerBlockCache({
			maxEntries: 2,
			maxBytes: 2,
			ttlMs: 10_000,
		});
		const backing = new Uint8Array(1024);
		backing.set([1, 2], 100);
		expect(cache.add("aliased", backing.subarray(100, 102))).to.equal(true);
		const retained = cache.get("aliased")!;
		expect(retained).to.deep.equal(new Uint8Array([1, 2]));
		expect(retained.buffer.byteLength).to.equal(2);

		expect(cache.add("zero", new Uint8Array())).to.equal(true);
		expect(cache.add("zero", new Uint8Array())).to.equal(true);
		expect(cache.stats()).to.include({ entries: 2, bytes: 2 });
		expect(cache.add("zero-2", new Uint8Array())).to.equal(true);
		expect(cache.stats()).to.include({ entries: 2, bytes: 0 });
		expect(cache.get("aliased")).to.equal(undefined);
		cache.clear();
	});

	it("copies length-tracking views over resizable buffers", function () {
		type ResizableBuffer = ArrayBuffer & {
			readonly resizable: boolean;
			resize(byteLength: number): void;
		};
		let backing: ResizableBuffer;
		try {
			const Constructor = ArrayBuffer as unknown as new (
				byteLength: number,
				options: { maxByteLength: number },
			) => ResizableBuffer;
			backing = new Constructor(2, { maxByteLength: 8 });
		} catch {
			this.skip();
			return;
		}
		if (backing.resizable !== true || typeof backing.resize !== "function") {
			this.skip();
			return;
		}

		const source = new Uint8Array(backing);
		source.set([1, 2]);
		const cache = new BoundedEagerBlockCache({
			maxEntries: 1,
			maxBytes: 2,
			ttlMs: 10_000,
		});
		expect(cache.add("resizable", source)).to.equal(true);
		backing.resize(8);

		const retained = cache.get("resizable")!;
		expect(retained).to.deep.equal(new Uint8Array([1, 2]));
		expect(retained.buffer.byteLength).to.equal(2);
		expect(cache.stats()).to.include({ entries: 1, bytes: 2 });
		cache.clear();
	});

	it("keeps the legacy true/cacheSize configuration compatible and validates bounds", () => {
		expect(normalizeEagerBlocksOptions(true).maxEntries).to.equal(1_000);
		expect(normalizeEagerBlocksOptions({ cacheSize: 7 }).maxEntries).to.equal(
			7,
		);
		expect(() => normalizeEagerBlocksOptions({ maxBytes: 0 })).to.throw(
			RangeError,
		);
		expect(() =>
			normalizeEagerBlocksOptions({ cacheSize: 0x1_0000_0000 }),
		).to.throw(RangeError);
		expect(() => normalizeEagerBlocksOptions({ ttlMs: 0x8000_0000 })).to.throw(
			RangeError,
		);
		expect(
			() =>
				new BoundedEagerBlockCache({
					maxEntries: 0,
					maxBytes: 1,
					ttlMs: 1,
				}),
		).to.throw(RangeError);
	});
});
