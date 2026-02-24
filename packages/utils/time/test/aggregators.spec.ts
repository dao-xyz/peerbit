/* eslint-disable @typescript-eslint/explicit-function-return-type, no-implicit-coercion, @stylistic/indent, no-useless-return, prefer-const, @stylistic/space-before-function-paren, curly */
import { expect } from "chai";
import {
	debounceAccumulator,
	debounceFixedInterval,
	delay,
	waitForResolved,
} from "../src/index.js";

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
			onError: (error: any) => errors.push(error),
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

	it("flush() runs pending work immediately when idle (trailing mode)", async () => {
		let invoked = 0;
		const calledAt: number[] = [];

		const d = debounceFixedInterval(
			async () => {
				invoked++;
				calledAt.push(Date.now());
			},
			500,
			{ leading: false },
		);

		const start = Date.now();
		d.call(); // schedules for ~500ms later
		await delay(50);
		const flushed = d.flush(); // force immediate
		await flushed;

		expect(invoked).to.eq(1);
		// Should not have waited the full delay
		expect(calledAt[0] - start).to.be.lessThan(300);
	});

	it("flush() during a running invocation triggers the next run immediately (no extra delay)", async () => {
		const times: number[] = [];
		const fn = async () => {
			times.push(Date.now());
			await delay(200);
		};

		const d = debounceFixedInterval(fn, 300, { leading: true });

		// 1st run starts immediately
		d.call();
		await delay(50);
		// queue another call while running
		d.call();
		// force the 'trailing' run to fire right after the first completes
		const flushed = d.flush();

		await flushed;

		expect(times.length).to.eq(2);
		const gap = times[1] - (times[0] + 200); // ~ time between end of first and start of second
		// Should be close to immediate (allow a little scheduler jitter)
		expect(gap).to.be.lessThan(50);
	});

	it("close() cancels pending work", async () => {
		let invoked = 0;
		const d = debounceFixedInterval(
			() => {
				invoked++;
			},
			300,
			{ leading: false },
		);

		d.call(); // would schedule for ~300ms later
		d.close(); // cancel it

		await delay(400);
		expect(invoked).to.eq(0);
	});

	it("preserves latest args and call-site `this`", async () => {
		const seen: Array<{ selfHasValue: number | undefined; arg: string }> = [];

		function target(this: any, arg: string) {
			seen.push({ selfHasValue: this?.value, arg });
		}

		const d = debounceFixedInterval(target, 60, { leading: false });
		// set a property on the debouncer object; calling d.call() makes `this===d` inside target
		(d as any).value = 123;

		d.call("a");
		await delay(10);
		d.call("b"); // should overwrite args to 'b'

		await waitForResolved(() => expect(seen.length).to.eq(1));
		expect(seen[0].arg).to.eq("b");
		expect(seen[0].selfHasValue).to.eq(123);
	});

	it("onError is called and does not break subsequent calls", async () => {
		const errors: Error[] = [];
		let okCount = 0;

		const d = debounceFixedInterval(
			async (mode: "fail" | "ok") => {
				if (mode === "fail") throw new Error("boom");
				okCount++;
			},
			50,
			{ leading: false, onError: (e) => errors.push(e) },
		);

		d.call("fail" as any);
		await waitForResolved(() => expect(errors.length).to.eq(1));

		d.call("ok" as any);
		await waitForResolved(() => expect(okCount).to.eq(1));
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
			// The subsequent group call should resolve after roughly delayTime. Under
			// workspace-wide test load, timers can fire late; assert "not early" with
			// a generous upper bound to avoid flakes while still catching regressions.
			expect(t2 - t1).to.be.greaterThan(delayTime - 100);
			expect(t2 - t1).to.be.lessThan(delayTime + 2500);
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
			// In CI and during workspace-wide test runs, the event loop can be delayed by
			// unrelated CPU-heavy tasks. Assert "not early" and keep a generous upper bound
			// to avoid flakes while still catching egregious regressions.
			expect(t1 - t0).to.be.greaterThan(delayTime - 100);
			expect(t1 - t0).to.be.lessThan(delayTime + 2500);

			const tGroupStart = Date.now();
			await Promise.all([debounced.add(2), debounced.add(3), debounced.add(4)]);
			const t2 = Date.now();
			// Subsequent calls within the same batch should also resolve after delayTime.
			expect(t2 - tGroupStart).to.be.greaterThan(delayTime - 100);
			expect(t2 - tGroupStart).to.be.lessThan(delayTime + 2500);
			expect(out).to.deep.eq([[1], [2, 3, 4]]);
		});

	const makeAcc = (delayTime: number, opts?: { leading?: boolean }) => {
		let out: number[][] = [];
		const debounced = debounceAccumulator<number, number, number[]>(
			(values: number[]) => {
				out.push(values);
			},
			() => {
				let values: number[] = [];
				return {
					delete: (value: number) => {
						const i = values.indexOf(value);
						if (i !== -1) values.splice(i, 1); // mutate in place
					},
					add: (value: number) => {
						values.push(value);
					},
					size: () => values.length,
					value: values, // same object identity throughout the batch
					has: (key: number) => values.includes(key),
				};
			},
			delayTime,
			opts,
		);
		return { debounced, out };
	};

	it("flush() sends the current batch immediately (trailing mode)", async () => {
		const { debounced, out } = makeAcc(500, { leading: false });

		const start = Date.now();
		debounced.add(1);
		debounced.add(2);
		await delay(50);

		await (debounced as any).flush(); // immediate send
		expect(out).to.deep.eq([[1, 2]]);
		expect(Date.now() - start).to.be.lessThan(300);
	});

	it("invoke() sends immediately and resets accumulator", async () => {
		const { debounced, out } = makeAcc(1000, { leading: false });

		// Don't await these: keep them in the same pending batch
		debounced.add(1);
		debounced.add(2);

		await debounced.invoke(); // immediate send, cancels pending timer
		expect(out).to.deep.eq([[1, 2]]);
		expect(debounced.size()).to.eq(0);

		// Next batch is fresh
		debounced.add(3);
		await debounced.invoke();
		expect(out).to.deep.eq([[1, 2], [3]]);
	});

	it("delete() and has() affect the current batch only", async () => {
		const { debounced, out } = makeAcc(80, { leading: false });

		// Do not await here; we want to inspect the live accumulator
		debounced.add(1);
		debounced.add(2);

		expect(debounced.has(2)).to.eq(true);

		debounced.delete(2);
		expect(debounced.has(2)).to.eq(false);

		// Now flush this batch
		await (debounced as any).flush?.();
		expect(out).to.deep.eq([[1]]);

		// Next batch is independent
		debounced.add(2);
		await (debounced as any).flush?.();
		expect(out).to.deep.eq([[1], [2]]);
	});

	it("add() promises resolve when the batch actually runs (with flush)", async () => {
		const { debounced, out } = makeAcc(1000, { leading: false });

		const p1 = debounced.add(10);
		const p2 = debounced.add(11);

		// Neither promise should be resolved until we flush/run
		let settled = 0;
		p1.then(() => settled++);
		p2.then(() => settled++);
		await delay(100);
		expect(settled).to.eq(0);

		await (debounced as any).flush();
		expect(settled).to.eq(2);
		expect(out).to.deep.eq([[10, 11]]);
	});

	it("close() cancels pending batch (no send)", async () => {
		const { debounced, out } = makeAcc(300, { leading: false });

		debounced.add(1);
		debounced.add(2);
		debounced.close();

		await delay(400);
		expect(out).to.deep.eq([]);
		expect(debounced.size()).to.eq(0); // now true: accumulator recreated on close()
	});
});
