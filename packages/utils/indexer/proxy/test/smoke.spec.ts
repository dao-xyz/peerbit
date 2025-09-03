import { expect } from "chai";
import { IndicesRPCContract } from "../src/indices.rpc.js";

describe("proxy package compiles", () => {
  it("exports IndicesRPCContract", () => {
    expect(IndicesRPCContract).to.be.a("function");
  });
});