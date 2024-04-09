import { logger } from "../src/index.js";
import { expect } from "chai";

describe("logger", () => {
	let reset: string | undefined;
	beforeEach(() => {
		reset = process.env.LOG_LEVEL;
	});

	afterEach(() => {
		if (reset) {
			process.env.LOG_LEVEL = reset;
		} else {
			delete process.env.LOG_LEVEL;
		}
	});
	it("can get log level", async () => {
		process.env.LOG_LEVEL = "fatal";
		expect(logger().level).equal("fatal");
	});

	it("can handle undefined level", async () => {
		expect(logger().level).equal("info");
	});
});
