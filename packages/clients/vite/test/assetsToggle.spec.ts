import { expect } from "chai";
import { __test__ } from "../src/index.js";

describe("assets toggle", () => {
	it("exports defaultAssetSources for composing assets externally", () => {
		expect(__test__.defaultAssetSources).to.be.an("array");
		expect(__test__.defaultAssetSources.length).greaterThan(0);
	});
});
