import { expect } from "chai";
import { logger } from "../src/index.js";

describe("logger", () => {
	it("re-exports libp2p logger api", () => {
		const log = logger("peerbit:test");
		expect(log).to.be.a("function");
		expect(log.error).to.be.a("function");
		expect(log.trace).to.be.a("function");
		expect(log.newScope("child")).to.be.a("function");
	});
});
