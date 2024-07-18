import { expect, use } from "chai";
import chaiAsPromised from "chai-as-promised";
import { AbortError, delay, waitFor } from "../src/index.js";

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
});

describe("waitForResolved", () => {
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
});
