import { delay } from "../index.js";
describe("delay", () => {
	it("delay", async () => {
		let startTime = +new Date();
		await delay(1000);
		expect(+new Date() - startTime).toBeLessThan(1200);
	});

	it("stop early", async () => {
		let startTime = +new Date();
		await delay(5000, {
			stopperCallback: (stop) => {
				setTimeout(() => {
					stop();
				}, 1000);
			},
		});
		expect(+new Date() - startTime).toBeLessThan(1200);
	});
});

describe("waitFor", () => {
	it("waitFor", async () => {
		const startTime = +new Date();
		await delay(1000);
		expect(+new Date() - startTime).toBeLessThan(1200);
	});
	it("stop early", async () => {
		const startTime = +new Date();
		await delay(5000, {
			stopperCallback: (stop) => {
				setTimeout(() => {
					stop();
				}, 1000);
			},
		});
		expect(+new Date() - startTime).toBeLessThan(1200);
	});
});
