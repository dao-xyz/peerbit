import { AbortError, delay, waitFor } from "../src/index.js";
import { expect, use } from "chai";
import chaiAsPromised from 'chai-as-promised';
use(chaiAsPromised);

describe("delay", () => {
	it("delay", async () => {
		let startTime = +new Date();
		await delay(1000);
		expect(+new Date() - startTime).lessThan(1500);
	});

	it("stop early", async () => {
		let startTime = +new Date();
		await expect(
			delay(5000, {
				signal: AbortSignal.timeout(1000)
			})
		).rejectedWith(AbortError);
		expect(+new Date() - startTime).lessThan(1500);
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
		expect(+new Date() - startTime).lessThan(1500);
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
		).rejectedWith(AbortError);
		expect(+new Date() - startTime).lessThan(1500);
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
		expect(+new Date() - startTime).lessThan(1500);
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
		).rejectedWith(AbortError);
		expect(+new Date() - startTime).lessThan(1500);
	});
});
