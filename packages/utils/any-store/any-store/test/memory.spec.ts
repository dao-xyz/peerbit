import { expect } from "chai";
import { MemoryStore } from "../src/memory.js";

const isNode = typeof process !== "undefined" && !!process.versions?.node;

describe("MemoryStore", () => {
	it("keeps exact size through puts, replacements, and deletes", () => {
		const store = new MemoryStore();

		expect(store.size()).to.equal(0);
		store.put("a", new Uint8Array(3));
		store.put("b", new Uint8Array(5));
		expect(store.size()).to.equal(8);

		store.put("a", new Uint8Array(11));
		expect(store.size()).to.equal(16);
		store.put("b", new Uint8Array(1));
		expect(store.size()).to.equal(12);
		store.put("a", new Uint8Array(0));
		expect(store.size()).to.equal(1);

		store.del("missing");
		expect(store.size()).to.equal(1);
		store.del("a");
		expect(store.size()).to.equal(1);
		store.del("b");
		expect(store.size()).to.equal(0);
		store.del("b");
		expect(store.size()).to.equal(0);
	});

	it("preserves size when the same instance closes and reopens", () => {
		const store = new MemoryStore();
		store.put("a", new Uint8Array(3));
		store.put("b", new Uint8Array(5));

		store.open();
		store.close();
		expect(store.size()).to.equal(8);
		store.open();
		expect(store.size()).to.equal(8);
	});

	it("tracks each level independently and clears descendants", () => {
		const root = new MemoryStore();
		const child = root.sublevel("child");
		const grandchild = child.sublevel("grandchild");

		root.put("root", new Uint8Array(2));
		child.put("child", new Uint8Array(3));
		grandchild.put("grandchild", new Uint8Array(5));

		expect(root.size()).to.equal(2);
		expect(child.size()).to.equal(3);
		expect(grandchild.size()).to.equal(5);

		child.clear();
		expect(root.size()).to.equal(2);
		expect(child.size()).to.equal(0);
		expect(grandchild.size()).to.equal(0);

		child.put("child", new Uint8Array(7));
		grandchild.put("grandchild", new Uint8Array(11));
		root.clear();
		expect(root.size()).to.equal(0);
		expect(child.size()).to.equal(0);
		expect(grandchild.size()).to.equal(0);
	});

	it("answers repeated size queries without iterating stored values", () => {
		const store = new MemoryStore();
		for (let i = 0; i < 256; i++) {
			store.put(String(i), new Uint8Array(i));
		}
		const expected = (255 * 256) / 2;
		const entries = (store as unknown as { store: Map<string, Uint8Array> })
			.store;
		Object.defineProperty(entries, Symbol.iterator, {
			value: () => {
				throw new Error("size must not iterate entries");
			},
		});

		for (let i = 0; i < 1_024; i++) {
			expect(store.size()).to.equal(expected);
		}
	});

	it("does not mutate when reading an incoming byte length throws", () => {
		const store = new MemoryStore();
		const original = new Uint8Array(3);
		store.put("existing", original);
		const hostile = new Uint8Array(5);
		Object.defineProperty(hostile, "byteLength", {
			get: () => {
				throw new Error("hostile byteLength");
			},
		});

		expect(() => store.put("existing", hostile)).to.throw("hostile byteLength");
		expect(store.get("existing")).to.equal(original);
		expect(store.size()).to.equal(3);

		expect(() => store.put("new", hostile)).to.throw("hostile byteLength");
		expect(store.get("new")).to.be.undefined;
		expect(store.size()).to.equal(3);
	});

	it("rejects invalid byte lengths before mutating", () => {
		const store = new MemoryStore();
		const original = new Uint8Array(3);
		store.put("existing", original);

		for (const byteLength of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
			const hostile = new Uint8Array(5);
			Object.defineProperty(hostile, "byteLength", { value: byteLength });
			expect(() => store.put("existing", hostile)).to.throw(RangeError);
			expect(store.get("existing")).to.equal(original);
			expect(store.size()).to.equal(3);
		}
	});

	it("rejects aggregate overflow and deletes back to exact zero", () => {
		const store = new MemoryStore();
		const maximum = new Uint8Array(0);
		Object.defineProperty(maximum, "byteLength", {
			value: Number.MAX_SAFE_INTEGER,
		});
		store.put("maximum", maximum);
		expect(store.size()).to.equal(Number.MAX_SAFE_INTEGER);

		const extra = new Uint8Array(1);
		expect(() => store.put("extra", extra)).to.throw(
			RangeError,
			"aggregate size",
		);
		expect(store.get("extra")).to.be.undefined;
		expect(store.get("maximum")).to.equal(maximum);
		expect(store.size()).to.equal(Number.MAX_SAFE_INTEGER);

		store.del("maximum");
		expect(store.size()).to.equal(0);
	});

	it("keeps the prior value and size when a replacement would overflow", () => {
		const store = new MemoryStore();
		const large = new Uint8Array(0);
		Object.defineProperty(large, "byteLength", {
			value: Number.MAX_SAFE_INTEGER - 2,
		});
		const original = new Uint8Array(2);
		store.put("large", large);
		store.put("replace", original);
		expect(store.size()).to.equal(Number.MAX_SAFE_INTEGER);

		const replacement = new Uint8Array(3);
		expect(() => store.put("replace", replacement)).to.throw(
			RangeError,
			"aggregate size",
		);
		expect(store.get("replace")).to.equal(original);
		expect(store.get("large")).to.equal(large);
		expect(store.size()).to.equal(Number.MAX_SAFE_INTEGER);

		store.del("replace");
		expect(store.size()).to.equal(Number.MAX_SAFE_INTEGER - 2);
		store.del("large");
		expect(store.size()).to.equal(0);
	});

	if (isNode) {
		it("uses credited length after a backing buffer is detached", function () {
			if (typeof structuredClone !== "function") {
				this.skip();
			}
			const store = new MemoryStore();
			const buffer = new ArrayBuffer(8);
			const value = new Uint8Array(buffer);
			store.put("value", value);

			structuredClone(buffer, { transfer: [buffer] });
			expect(value.byteLength).to.equal(0);
			expect(store.size()).to.equal(8);

			store.del("value");
			expect(store.size()).to.equal(0);
		});

		it("uses credited length after a backing buffer is resized", function () {
			type ResizableBuffer = ArrayBuffer & {
				resize: (byteLength: number) => void;
			};
			const ResizableArrayBuffer = ArrayBuffer as typeof ArrayBuffer & {
				new (
					byteLength: number,
					options: { maxByteLength: number },
				): ResizableBuffer;
			};
			if (
				typeof (ArrayBuffer.prototype as Partial<ResizableBuffer>).resize !==
				"function"
			) {
				this.skip();
			}
			const store = new MemoryStore();
			const buffer = new ResizableArrayBuffer(4, { maxByteLength: 16 });
			const value = new Uint8Array(buffer);
			store.put("value", value);

			buffer.resize(12);
			expect(value.byteLength).to.equal(12);
			expect(store.size()).to.equal(4);

			store.put("value", value);
			expect(store.size()).to.equal(12);
			store.put("value", new Uint8Array(3));
			expect(store.size()).to.equal(3);
			store.del("value");
			expect(store.size()).to.equal(0);
		});
	}
});
