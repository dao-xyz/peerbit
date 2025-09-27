import { expect } from "chai";
import { TEST_EXPORTS } from "../src/index.js";

describe("assets toggle", () => {
	it("exports defaultAssetSources for composing assets externally", () => {
		expect(TEST_EXPORTS.defaultAssetSources).to.be.an("array");
		expect(TEST_EXPORTS.defaultAssetSources.length).greaterThan(0);
	});
});
