import { logger } from "../index.js";

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
		expect(logger().level).toEqual("fatal");
	});

	it("can handle undefined level", async () => {
		expect(logger().level).toEqual("info");
	});
});
