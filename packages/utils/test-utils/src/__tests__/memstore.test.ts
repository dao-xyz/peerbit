import { MemStore } from "../index.js";

describe("MemStore", function () {
  it("puts and gets values", async () => {
    const memStore = new MemStore();

    const value1 = "abc123";
    const value2 = {
      heads: ["QmZcxMx76VaDyFi4hCvJ9Cg8odLJM5PQeg4bgVzCyN6xbr"],
      nexts: [""],
    };
    const cid1 = await memStore.put(value1);
    const cid2 = await memStore.put(value2);

    expect(await memStore.get(cid1)).toEqual({ value: value1 });
    expect(await memStore.get(cid2)).toEqual({ value: value2 });
  });
});
