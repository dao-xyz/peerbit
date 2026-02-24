import { expect } from "chai";
import { TopicRootDirectory } from "../src/index.js";

describe("topic-root-directory", () => {
	it("resolves explicit roots before defaults", async () => {
		const directory = new TopicRootDirectory({
			defaultCandidates: ["peer-b", "peer-a"],
		});
		directory.setRoot("orders", "peer-x");

		expect(await directory.resolveRoot("orders")).to.equal("peer-x");
	});

	it("picks deterministic roots from default candidates", async () => {
		const directory = new TopicRootDirectory({
			defaultCandidates: ["peer-c", "peer-a", "peer-b", "peer-a"],
		});

		const first = await directory.resolveRoot("topic-1");
		const second = await directory.resolveRoot("topic-1");
		const third = await directory.resolveRoot("topic-2");

		expect(first).to.exist;
		expect(second).to.equal(first);
		expect(["peer-a", "peer-b", "peer-c"]).to.include(first!);
		expect(["peer-a", "peer-b", "peer-c"]).to.include(third!);
	});

	it("uses resolver before candidate hashing", async () => {
		const directory = new TopicRootDirectory({
			defaultCandidates: ["peer-a", "peer-b"],
			resolver: (topic) => (topic === "rpc" ? "peer-z" : undefined),
		});

		expect(await directory.resolveRoot("rpc")).to.equal("peer-z");
		expect(await directory.resolveRoot("other")).to.not.equal(undefined);
	});
});
