import { expect } from "chai";
import { captureBoundedUint8Array } from "../src/bounded-bytes.js";

describe("bounded byte capture", () => {
	it("uses typed-array intrinsics instead of shadowed length or iteration", () => {
		let shadowReads = 0;
		const oversized = new Uint8Array(128);
		Object.defineProperty(oversized, "byteLength", {
			get: () => {
				shadowReads++;
				return 1;
			},
		});
		expect(() =>
			captureBoundedUint8Array(oversized, 1, 16, "test bytes"),
		).to.throw("Invalid test bytes");
		expect(shadowReads).to.equal(0);

		let propertyReads = 0;
		let prototypeReads = 0;
		const proxied = new Proxy(new Uint8Array(128), {
			getPrototypeOf() {
				prototypeReads++;
				throw new Error("prototype user hook");
			},
			get(target, property, receiver) {
				propertyReads++;
				if (property === Symbol.iterator || property === "byteLength") {
					throw new Error("unbounded user hook");
				}
				return Reflect.get(target, property, receiver);
			},
		});
		expect(() =>
			captureBoundedUint8Array(proxied, 1, 16, "test bytes"),
		).to.throw("Invalid test bytes");
		expect(propertyReads).to.equal(0);
		expect(prototypeReads).to.equal(0);
	});

	it("rejects other internal typed-array kinds with forged prototypes", () => {
		for (const value of [
			new Uint16Array([1, 2]),
			new Int8Array([1, 2]),
			new Uint8ClampedArray([1, 2]),
		]) {
			Object.setPrototypeOf(value, Uint8Array.prototype);
			expect(value).to.be.instanceOf(Uint8Array);
			expect(() =>
				captureBoundedUint8Array(value, 1, 16, "test bytes"),
			).to.throw("Invalid test bytes");
		}
	});

	it("accepts buffers as real Uint8Arrays and returns an isolated exact copy", () => {
		const source = Buffer.from([1, 2, 3]);
		const captured = captureBoundedUint8Array(source, 1, 3, "test bytes");
		expect([...captured]).to.deep.equal([1, 2, 3]);
		source[0] = 9;
		expect([...captured]).to.deep.equal([1, 2, 3]);
	});
});
