import { expect } from "chai";
import {
	debounceAccumulator,
	debounceFixedInterval,
	delay,
	waitForResolved,
} from "../src";

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
		debounced.call();
		await delay(300); // we do this to make sure the debounced function is called
		debounced.call();
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
			debounced.call();
			debounced.call();
			debounced.call();
			debounced.call();
			debounced.call();
			await delay(95);
		}
		await waitForResolved(() => expect(fn.invoked()).to.greaterThanOrEqual(5));
		expect(errors).to.deep.eq([]);
	});
});

describe("debounceAccumulator", () => {
	it("will wait on add (leading behavior)", async () => {
		let out: number[][] = [];
		const delayTime = 1000;
		const debounced = debounceAccumulator<number, number, number[]>(
			(values: number[]) => {
				out.push(values);
			},
			() => {
				let values: number[] = [];
				return {
					delete: (value: number) => {
						values = values.filter((v) => v !== value);
					},
					add: (value: number) => {
						values.push(value);
					},
					size: () => values.length,
					value: values,
					has: (key: number) => values.includes(key),
				};
			},
			delayTime,
			//  { leading: true } // Leading default enabled
		);

		const t0 = Date.now();
		await debounced.add(1);
		const t1 = Date.now();
		// With leading enabled, the first call resolves quickly.
		expect(t1 - t0).to.be.lessThan(delayTime);

		await Promise.all([debounced.add(2), debounced.add(3), debounced.add(4)]);
		const t2 = Date.now();
		const allowedError = 100;
		// The subsequent group call should resolve after roughly delayTime.
		expect(t2 - t1).to.be.closeTo(delayTime, allowedError);
		expect(out).to.deep.eq([[1], [2, 3, 4]]);
	});

	it("will wait on add (unleading/trailing behavior)", async () => {
		let out: number[][] = [];
		const delayTime = 1000;
		const debounced = debounceAccumulator<number, number, number[]>(
			(values: number[]) => {
				out.push(values);
			},
			() => {
				let values: number[] = [];
				return {
					delete: (value: number) => {
						values = values.filter((v) => v !== value);
					},
					add: (value: number) => {
						values.push(value);
					},
					size: () => values.length,
					value: values,
					has: (key: number) => values.includes(key),
				};
			},
			delayTime,
			{ leading: false }, // Leading disabled, so all calls are trailing.
		);

		const t0 = Date.now();
		await debounced.add(1);
		const t1 = Date.now();
		// With leading disabled, the first call should resolve only after the delay.
		expect(t1 - t0).to.be.closeTo(delayTime, 100);

		const tGroupStart = Date.now();
		await Promise.all([debounced.add(2), debounced.add(3), debounced.add(4)]);
		const t2 = Date.now();
		// Subsequent calls within the same batch should also resolve after delayTime.
		expect(t2 - tGroupStart).to.be.closeTo(delayTime, 100);
		expect(out).to.deep.eq([[1], [2, 3, 4]]);
	});
});
