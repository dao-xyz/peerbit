/**
The MIT License (MIT)

Copyright (c) 2021 Martin Heidegger
Copyright (c) 2022 dao.xyz

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

 */
import { deserialize, serialize } from "@dao-xyz/borsh";
import { expect } from "chai";
import {
	ClockOffsetError,
	ForwardJumpError,
	HLC,
	Timestamp,
	WallTimeOverflowError,
} from "../src/clock.js";

describe("hlc", () => {
	// TODO rm shim
	const t = {
		equals: (a: any, b: any) => expect(a).equal(b),
		deepEquals: (a: any, b: any) => expect(a).to.deep.equal(b),
		// This asserted nothing until 2026-08-14. `catch (error)` shadowed the
		// expected-error parameter and the body compared the caught error to
		// ITSELF, which is true for any value; and when the call did not throw
		// at all, the `expect(result).to.throw()` that was supposed to catch
		// that raised an AssertionError into the very same catch, where the
		// self-comparison swallowed it. It was also `async` and never awaited,
		// so even a genuine rejection landed after the test had passed.
		throws: (call: () => unknown, expected: Error) => {
			let caught: unknown;
			try {
				call();
			} catch (error) {
				caught = error;
			}
			expect(caught, "the call was expected to throw").to.be.instanceOf(
				expected.constructor as new (...args: any[]) => Error,
			);
			// The error classes encode their operands in the message, so this
			// pins the values and not merely the type.
			expect((caught as Error).message).to.equal(expected.message);
		},
		ok: (x: any) => expect(x).to.equal(true),
		fail: (message: string) => expect.fail(message),
	};

	it(".now() returns a new timestamp", () => {
		const clock = new HLC();
		t.equals(clock.maxOffset, 0n);
		t.equals(clock.toleratedForwardClockJump, 0n);
		t.equals(clock.wallTimeUpperBound, 0n);
		const time = clock.now();
		t.equals(time.logical, 0);
		t.ok(time instanceof Timestamp);
		const time2 = clock.now();
		t.equals(time2.compare(time), 1);
	});
	it(".update() can override the internal clock", () => {
		const clock = new HLC();
		const time = clock.now();
		time.wallTime += BigInt(1e9); // Stepping 1s into the future
		clock.update(time);
		const time2 = clock.now();
		t.equals(time2.wallTime, time.wallTime);
	});
	it("repeat clocks on the same walltime increment logical parts", () => {
		const clock = new HLC();
		const time = clock.now();
		time.wallTime += BigInt(1e9); // Stepping 1s into the future
		clock.update(time);
		const time2 = clock.now();
		const time3 = clock.now();
		t.equals(time2.wallTime, time3.wallTime);
		t.equals(time2.logical, 2);
		t.equals(time3.logical, 3);
	});
	it(".nowBatch() returns ordered timestamps from one walltime sample", () => {
		let samples = 0;
		const wallTimes = [10n, 11n, 11n];
		const clock = new HLC({
			wallTime: () => wallTimes[Math.min(samples++, wallTimes.length - 1)]!,
		});

		const timestamps = clock.nowBatch(3);

		t.equals(samples, 2); // constructor + batch
		t.deepEquals(timestamps, [
			new Timestamp({ wallTime: 11n, logical: 0 }),
			new Timestamp({ wallTime: 11n, logical: 1 }),
			new Timestamp({ wallTime: 11n, logical: 2 }),
		]);
		t.deepEquals(
			clock.now(),
			new Timestamp({ wallTime: 11n, logical: 3 }),
		);
	});
	it("Timestamp comparison", () => {
		const a = new Timestamp({ wallTime: 0n, logical: 0 });
		const b = new Timestamp({ wallTime: 0n, logical: 1 });
		const c = new Timestamp({ wallTime: 1n, logical: 1 });
		t.equals(a.compare(a), 0);
		t.equals(a.compare(b), -1);
		t.equals(a.compare(c), -1);
		t.equals(b.compare(a), 1);
		t.equals(b.compare(b), 0);
		t.equals(b.compare(c), -1);
		t.equals(c.compare(a), 1);
		t.equals(c.compare(b), 1);
		t.equals(c.compare(c), 0);
		t.equals(Timestamp.bigger(a, a), a);
		t.equals(Timestamp.bigger(a, b), b);
		t.equals(Timestamp.bigger(a, c), c);
		t.equals(Timestamp.bigger(b, a), b);
		t.equals(Timestamp.bigger(b, b), b);
		t.equals(Timestamp.bigger(b, c), c);
		t.equals(Timestamp.bigger(c, a), c);
		t.equals(Timestamp.bigger(c, b), c);
		t.equals(Timestamp.bigger(c, c), c);
	});

	it("restoring from a past timestamp", () => {
		const clockOlder = new HLC({
			wallTime: () => 0n,
			last: new Timestamp({ wallTime: 1n }),
		});
		t.equals(clockOlder.last.wallTime, 1n);
		const clockNewer = new HLC({
			wallTime: () => 2n,
			last: new Timestamp({ wallTime: 1n }),
		});
		t.equals(clockNewer.last.wallTime, 2n);
	});
	it("updating with newer logical", () => {
		const clock = new HLC({
			wallTime: () => 0n,
			last: new Timestamp({ wallTime: 1n, logical: 2 }),
		});
		clock.update(new Timestamp({ wallTime: 1n, logical: 5 }));
		t.equals(clock.last.wallTime, 1n);
		t.equals(clock.last.logical, 6);
	});
	it("updating with older logical", () => {
		const clock = new HLC({
			wallTime: () => 0n,
			last: new Timestamp({ wallTime: 1n, logical: 5 }),
		});
		clock.update(new Timestamp({ wallTime: 1n, logical: 2 }));
		t.equals(clock.last.wallTime, 1n);
		t.equals(clock.last.logical, 6);
	});

	it("forward clock jump error", () => {
		let myTime = 1n;
		const wallTime = () => myTime;
		const clockNoError = new HLC({ wallTime });
		const clockError = new HLC({
			wallTime,
			toleratedForwardClockJump: 10n,
		});
		t.equals(clockError.toleratedForwardClockJump, 10n);
		myTime = 2n;
		t.deepEquals(clockError.now(), clockNoError.now());
		myTime = 20n;
		t.equals(
			clockNoError.now().compare(new Timestamp({ wallTime: 20n, logical: 0 })),
			0,
		);
		t.throws(() => clockError.now(), new ForwardJumpError(18n, 10n));
	});
	it("maxOffset error", () => {
		const wallTime = () => 0n;
		const clockNoError = new HLC({ wallTime });
		const clockError = new HLC({ wallTime, maxOffset: 10n });
		t.equals(clockError.maxOffset, 10n);
		const jumpStamp = new Timestamp({ wallTime: 20n });
		clockNoError.update(jumpStamp);
		t.deepEquals(
			clockNoError.now(),
			new Timestamp({
				wallTime: 20n,
				logical: 2,
			}),
		);
		t.throws(
			() => clockError.update(jumpStamp),
			new ClockOffsetError(20n, 10n),
		);
	});
	it("wall overflow error", () => {
		t.throws(
			() => {
				new HLC({ wallTime: () => 18446744073709551615n + 1n }).now();
			},
			new WallTimeOverflowError(18446744073709551616n, 18446744073709551615n),
		);
		t.throws(
			() => {
				new HLC({
					wallTime: () => 2n,
					wallTimeUpperBound: 1n,
				}).now();
			},
			new WallTimeOverflowError(2n, 1n),
		);
	});
	it("logical overflow leads to physical increase", () => {
		const clock = new HLC({
			wallTime: () => 0n,
			last: new Timestamp({ wallTime: 0n, logical: 0xffffffff - 1 }),
		});
		t.deepEquals(
			clock.now(),
			new Timestamp({
				wallTime: 0n,
				logical: 0xffffffff,
			}),
		);
		t.deepEquals(
			clock.now(),
			new Timestamp({
				wallTime: 1n,
				logical: 0,
			}),
		);
	});
	it("example: usage", () => {
		const clock = new HLC({
			maxOffset: 0n, // [default=0] Maximum time in nanosecons that another timestamp may exceed the wall-clock before an error is thrown.
			toleratedForwardClockJump: 0n, // [default=0] Maximum time in nanoseconds that the wall-clock may exceed the previous timestamp before an error is thrown. Setting it 0 will disable it.
			wallTimeUpperBound: 0n, // [default=0] will throw an error if the wallTime exceeds this value. Setting it to 0 will limit it to the uint64 max-value.
			last: undefined, // [default=undefined] The last known timestamp to start off, useful for restoring a clock's state
		});

		const timestamp = clock.now();

		// Makes sure that the next timestamp is bigger than the other timestamp
		clock.update(new Timestamp({ wallTime: 1n }));

		// Turn the clock into an Uint8Array
		const bytes = serialize(timestamp);

		expect(deserialize(bytes, Timestamp).compare(timestamp)).equal(0);
	});
	it("example: clock drift", () => {
		try {
			const clock = new HLC({
				maxOffset: BigInt(60 * 1e9) /* 1 minute in nanoseconds */,
			});
			const timestamp = clock.now();
			clock.update(
				new Timestamp({
					wallTime: timestamp.wallTime + BigInt(120 * 1e9),
				}),
			);
			t.fail("error should have thrown");
		} catch (error: any) {
			if (error instanceof ClockOffsetError === false) {
				throw error;
			}
			// Was `deepEquals(error, new ClockOffsetError(error.offset,
			// error.maxOffset))` — the expectation was built out of the actual
			// error's own fields, so it could only fail if the constructor
			// mangled them. Assert the scenario instead: the configured limit,
			// and that the rejected offset genuinely exceeded it.
			expect(error.maxOffset).to.equal(BigInt(60 * 1e9));
			expect(error.offset > error.maxOffset).to.equal(true);
		}
	});
	it("example-2: clock drift", () => {
		// Hoisted out of the try so the catch below can assert against it.
		const wallTimeUpperBound =
			BigInt(new Date("2022-01-01T00:00:00.000Z").getTime()) * BigInt(1e6);
		try {
			const clock = new HLC({
				wallTime: () => wallTimeUpperBound + 1n, // Faking a wallTime that is beyond the max we allow
				wallTimeUpperBound,
			});
			clock.now();
			t.fail("error should have thrown");
		} catch (error: any) {
			if (error instanceof WallTimeOverflowError === false) {
				throw error;
			}
			// Same self-comparison as above, but this scenario is fully
			// deterministic, so both operands can be pinned exactly.
			expect(error.maxTime).to.equal(wallTimeUpperBound);
			expect(error.time).to.equal(wallTimeUpperBound + 1n);
		}
	});
	it("example-3: clock drift", async () => {
		const clock = new HLC({
			toleratedForwardClockJump: BigInt(1e6) /* 1 ms in nanoseconds */,
		});
		// Until 2026-08-14 this was a SYNCHRONOUS test whose only assertions
		// lived inside a setTimeout: mocha passed it the moment the callback
		// was scheduled, so the drift detection was never actually checked, and
		// a failure would have surfaced 10ms later as an unhandled error
		// attributed to whichever test was running by then.
		await new Promise((resolve) => setTimeout(resolve, 10));
		let caught: unknown;
		try {
			clock.now();
		} catch (error) {
			caught = error;
		}
		expect(caught, "the clock jump should have been rejected").to.be.instanceOf(
			ForwardJumpError,
		);
		const error = caught as ForwardJumpError;
		expect(error.tolerance).to.equal(BigInt(1e6));
		expect(error.timejump > error.tolerance).to.equal(true);
	});
	it("example: drift monitoring", () => {
		class CockroachHLC extends HLC {
			monotonicityErrorCount: number;
			constructor(opts: any) {
				super(opts);
				this.monotonicityErrorCount = 0;
			}

			validateOffset(offset: any) {
				super.validateOffset(offset);
				if (this.maxOffset > 10n && offset > this.maxOffset / 10n) {
					this.monotonicityErrorCount += 1;
				}
			}
		}

		const clock = new CockroachHLC({
			wallTime: () => 10n,
			maxOffset: 20,
		});
		clock.update(new Timestamp({ wallTime: 13n }));
		t.equals(clock.monotonicityErrorCount, 1);
	});
});
