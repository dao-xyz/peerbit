import { expect, use } from "chai";
import chaiAsPromised from "chai-as-promised";
import { AbortError, delay, waitFor, waitForResolved } from "../src/index.js";

use(chaiAsPromised);

describe("delay", () => {
	it("delay", async () => {
		const startTime = Number(new Date());
		await delay(1000);
		expect(Number(new Date()) - startTime).lessThan(1500);
	});

	it("stop early", async () => {
		const startTime = Number(new Date());
		await expect(
			delay(5000, {
				signal: AbortSignal.timeout(1000),
			}),
		).rejectedWith(AbortError);
		expect(Number(new Date()) - startTime).lessThan(1500);
	});
});

describe("waitFor", () => {
	it("waitFor", async () => {
		const startTime = Number(new Date());
		let done = false;
		setTimeout(() => {
			done = true;
		}, 1000);
		await waitFor(() => {
			return done;
		});
		expect(Number(new Date()) - startTime).lessThan(1500);
	});
	it("stop early", async () => {
		const startTime = Number(new Date());
		let done = false;
		setTimeout(() => {
			done = true;
		}, 1000);
		await expect(
			waitFor(
				() => {
					return done;
				},
				{ signal: AbortSignal.timeout(1000) },
			),
		).rejectedWith(AbortError);
		expect(Number(new Date()) - startTime).lessThan(1500);
	});

	it("aborted before start", async () => {
		const startTime = Number(new Date());
		const controller = new AbortController();
		controller.abort();
		await expect(
			waitFor(
				() => {
					return true;
				},
				{ signal: controller.signal },
			),
		).rejectedWith(AbortError);
		expect(Number(new Date()) - startTime).lessThan(100);
	});
});

describe("waitForResolved", () => {
	it("waitFor", async () => {
		const startTime = Number(new Date());
		let done = false;
		setTimeout(() => {
			done = true;
		}, 1000);
		await waitForResolved(() => {
			return expect(done).to.be.true;
		});
		expect(Number(new Date()) - startTime).lessThan(1500);
	});
	it("stop early", async () => {
		const startTime = Number(new Date());
		let done = false;
		setTimeout(() => {
			done = true;
		}, 1000);
		await expect(
			waitForResolved(
				() => {
					expect(done).to.be.true;
				},
				{ signal: AbortSignal.timeout(1000) },
			),
		).rejectedWith(AbortError);
		expect(Number(new Date()) - startTime).lessThan(1500);
	});

	it("abort before start", async () => {
		const startTime = Number(new Date());
		const controller = new AbortController();
		controller.abort();
		await expect(
			waitForResolved(() => expect(false).to.be.true, {
				signal: controller.signal,
			}),
		).rejectedWith(AbortError);
		expect(Number(new Date()) - startTime).lessThan(100);
	});
});
