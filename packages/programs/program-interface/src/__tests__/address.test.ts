import assert from "assert";
import { Address } from "../index.js";

describe("Parse Address", () => {
  it("throws an error if address is empty", () => {
    let err;
    try {
      const result = Address.parse("");
    } catch (e: any) {
      err = e.toString();
    }
    expect(err).toEqual("Error: Not a valid Peerbit address: ");
  });

  it("parse address successfully", () => {
    const address =
      "/peerbit/zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13";
    const result = Address.parse(address);

    const isInstanceOf = result instanceof Address;
    expect(isInstanceOf).toEqual(true);

    expect(result.cid).toEqual(
      "zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13"
    );

    assert.equal(result.toString().indexOf("/peerbit"), 0);
    assert.equal(result.toString().indexOf("zd"), 9);
  });

  it("parse address with backslashes (win32) successfully", () => {
    const address = "\\peerbit\\Qmdgwt7w4uBsw8LXduzCd18zfGXeTmBsiR8edQ1hSfzcJC";
    const result = Address.parse(address);

    const isInstanceOf = result instanceof Address;
    expect(isInstanceOf).toEqual(true);

    expect(result.cid).toEqual(
      "Qmdgwt7w4uBsw8LXduzCd18zfGXeTmBsiR8edQ1hSfzcJC"
    );

    assert.equal(result.toString().indexOf("/peerbit"), 0);
    assert.equal(result.toString().indexOf("Qm"), 9);
  });

  it("parse address with type and index correctly", () => {
    const address =
      "/peerbit/zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13/123";
    const result = Address.parse(address);

    const isInstanceOf = result instanceof Address;
    expect(isInstanceOf).toEqual(true);

    expect(result.cid).toEqual(
      "zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13"
    );

    assert.equal(result.toString().indexOf("/peerbit"), 0);
    assert.equal(result.toString().indexOf("zd"), 9);
    assert.equal(result.toString().indexOf("123"), 59);
  });
});

describe("isValid Address", () => {
  it("returns false for empty string", () => {
    const result = Address.isValid("");
    expect(result).toEqual(false);
  });

  it("validate address successfully", () => {
    const address =
      "/peerbit/zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13";
    const result = Address.isValid(address);

    expect(result).toEqual(true);
  });

  it("handle missing peerbit prefix", () => {
    const address = "zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13";
    const result = Address.isValid(address);

    expect(result).toEqual(true);
  });

  it("handle missing db address name", () => {
    const address =
      "/peerbit/zdpuAuK3BHpS7NvMBivynypqciYCuy2UW77XYBPUYRnLjnw13";
    const result = Address.isValid(address);

    expect(result).toEqual(true);
  });

  it("handle invalid multihash", () => {
    const address = "/peerbit/Qmdgwt7w4uBsw8LXduzCd18zfGXeTmBsiR8edQ1hSfzc";
    const result = Address.isValid(address);

    expect(result).toEqual(false);
  });

  it("validate address with backslashes (win32) successfully", () => {
    const address = "\\peerbit\\Qmdgwt7w4uBsw8LXduzCd18zfGXeTmBsiR8edQ1hSfzcJC";
    const result = Address.isValid(address);

    expect(result).toEqual(true);
  });
});
