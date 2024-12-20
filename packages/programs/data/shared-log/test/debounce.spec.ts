import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import {
	debounceFixedInterval,
	debouncedAccumulatorMap,
} from "../src/debounce.js";

describe("debounceFixedInterval", () => {
	const checkInterval = (current: number, last: number, interval: number) => {
		if (+current - +last < interval) {
			throw new Error("Invoking too quickly");
		}
	};
	const setupFn = (wait?: number | undefined) => {
		let invoked = 0;
		let last = 0;
		const interval = 10;
		let timings: number[] = [];
		let slowFn = wait
			? async () => {
					await delay(wait);
					let current = +new Date();
					checkInterval(current, last, interval);
					last = current;
					invoked++;
					timings.push(current);
					return;
				}
			: () => {
					let current = +new Date();
					checkInterval(current, last, interval);
					last = current;
					invoked++;
					timings.push(current);
					return;
				};

		return { fn: slowFn, timings, invoked: () => invoked };
	};
	it("will wait for asynchronus function to finish before calling again", async () => {
		const fn = setupFn(1000);
		let debounced = debounceFixedInterval(fn.fn, 10);
		let start = Date.now();
		debounced();
		await delay(300); // we do this to make sure the debounced function is called
		debounced();
		await waitForResolved(() => expect(fn.invoked()).to.eq(2));
		let end = Date.now();
		expect(end - start).to.be.greaterThan(2000);
	});

	it("will invoke on fixed interval", async () => {
		const fn = setupFn(100);
		const interval = 100;
		let errors: any[] = [];
		let debounced = debounceFixedInterval(fn.fn, interval, {
			onError: (error) => errors.push(error),
		});
		let count = 10;

		for (let i = 0; i < count; i++) {
			debounced();
			debounced();
			debounced();
			debounced();
			debounced();
			await delay(95);
		}
		await waitForResolved(() => expect(fn.invoked()).to.greaterThanOrEqual(5));
		expect(errors).to.deep.eq([]);
	});
});

describe("debounceAcculmulatorMap", () => {
	it("will wait for asynchronus function to finish before calling again", async () => {
		let invokedSize: number[] = [];
		const debounce = debouncedAccumulatorMap(async (map) => {
			await delay(1000);
			invokedSize.push(map.size);
		}, 100);

		debounce.add({ key: "1", value: 1 });
		await delay(200);
		debounce.add({ key: "2", value: 2 });
		debounce.add({ key: "3", value: 3 });

		await waitForResolved(() => expect(invokedSize).to.deep.eq([1, 2]));
	});

	it("will aggregate while waiting for asynchronus function to finish", async () => {
		let invokedSize: number[] = [];
		const debounce = debouncedAccumulatorMap(async (map) => {
			await delay(1000);
			invokedSize.push(map.size);
		}, 100);

		debounce.add({ key: "1", value: 1 });
		await delay(200);
		debounce.add({ key: "2", value: 2 });
		await delay(200);
		debounce.add({ key: "3", value: 3 });

		await waitForResolved(() => expect(invokedSize).to.deep.eq([1, 2]));

		debounce.add({ key: "4", value: 4 });

		await waitForResolved(() => expect(invokedSize).to.deep.eq([1, 2, 1]));
	});
});
