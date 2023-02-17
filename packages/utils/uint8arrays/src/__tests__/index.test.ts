import { compare, startsWith } from "../index.js";

it("compare", () => {
	const a = new Uint8Array([1]);
	const b = new Uint8Array([1, 2]);
	const c = new Uint8Array([2, 2]);
	const d = new Uint8Array([3]);
	const e = new Uint8Array([1, 1, 1]);
	const arrays = [a, b, c, d, e];
	const expectedSorted = [a, e, b, c, d];
	arrays.sort(compare);
	expect(arrays).toEqual(expectedSorted);
});

it("startWith", () => {
	const a = new Uint8Array([1]);
	const b = new Uint8Array([1, 2, 3]);
	const c = new Uint8Array([2, 1, 3]);

	const empty = new Uint8Array();
	expect(startsWith(b, a)).toBeTrue();
	expect(startsWith(a, b)).toBeFalse();
	expect(startsWith(a, a)).toBeTrue();
	expect(startsWith(a, empty)).toBeTrue();
	expect(startsWith(b, empty)).toBeTrue();
	expect(startsWith(empty, a)).toBeFalse();
	expect(startsWith(a, c)).toBeFalse();
	expect(startsWith(c, a)).toBeFalse();
});
