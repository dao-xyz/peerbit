import { arraysCompare } from "../index.js";

it("array compare", () => {
    const a = new Uint8Array([1]);
    const b = new Uint8Array([1, 2]);
    const c = new Uint8Array([2, 2]);
    const d = new Uint8Array([3]);
    const e = new Uint8Array([1, 1, 1]);
    const arrays = [a, b, c, d, e];
    const expectedSorted = [a, e, b, c, d];
    arrays.sort(arraysCompare);
    expect(arrays).toEqual(expectedSorted);
});
