import { AbortError, delay, waitFor } from "../index.js";
describe("delay", () => {
	it("delay", async () => {
		let startTime = +new Date();
		await delay(1000);
		expect(+new Date() - startTime).toBeLessThan(1500);
	});

	it("stop early", async () => {
		let startTime = +new Date();
		await expect(
			delay(5000, {
				signal: AbortSignal.timeout(1000)
			})
		).rejects.toThrow(AbortError);
		expect(+new Date() - startTime).toBeLessThan(1500);
	});
});

describe("waitFor", () => {
	it("waitFor", async () => {
		const startTime = +new Date();
		let done = false;
		setTimeout(() => {
			done = true;
		}, 1000);
		await waitFor(() => {
			return done;
		});
		expect(+new Date() - startTime).toBeLessThan(1400);
	});
	it("stop early", async () => {
		const startTime = +new Date();
		let done = false;
		setTimeout(() => {
			done = true;
		}, 1000);
		await expect(
			waitFor(
				() => {
					return done;
				},
				{ signal: AbortSignal.timeout(1000) }
			)
		).rejects.toThrow(AbortError);
		expect(+new Date() - startTime).toBeLessThan(1400);
	});
});

describe("waitForResolved", () => {
	it("waitFor", async () => {
		const startTime = +new Date();
		let done = false;
		setTimeout(() => {
			done = true;
		}, 1000);
		await waitFor(() => {
			return done;
		});
		expect(+new Date() - startTime).toBeLessThan(1400);
	});
	it("stop early", async () => {
		const startTime = +new Date();
		let done = false;
		setTimeout(() => {
			done = true;
		}, 1000);
		await expect(
			waitFor(
				() => {
					return done;
				},
				{ signal: AbortSignal.timeout(1000) }
			)
		).rejects.toThrow(AbortError);
		expect(+new Date() - startTime).toBeLessThan(1400);
	});
});
