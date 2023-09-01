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
import {
	ClockOffsetError,
	ForwardJumpError,
	HLC,
	Timestamp,
	WallTimeOverflowError
} from "../clock.js";

describe("hlc", () => {
	/// To lazy rn to rewrite test for jest so I will just make an adapter
	const t = {
		equals: (a: any, b: any) => expect(a).toEqual(b),
		deepEquals: (a: any, b: any) => expect(a).toEqual(b),
		throws: (a: any, error: any) => expect(a).toThrow(error),
		ok: (x: any) => x === true,
		fail: (x: any) => fail(x)
	};

	test(".now() returns a new timestamp", () => {
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
	test(".update() can override the internal clock", () => {
		const clock = new HLC();
		const time = clock.now();
		time.wallTime += BigInt(1e9); // Stepping 1s into the future
		clock.update(time);
		const time2 = clock.now();
		t.equals(time2.wallTime, time.wallTime);
	});
	test("repeat clocks on the same walltime increment logical parts", () => {
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
	test("Timestamp comparison", () => {
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

	test("restoring from a past timestamp", () => {
		const clockOlder = new HLC({
			wallTime: () => 0n,
			last: new Timestamp({ wallTime: 1n })
		});
		t.equals(clockOlder.last.wallTime, 1n);
		const clockNewer = new HLC({
			wallTime: () => 2n,
			last: new Timestamp({ wallTime: 1n })
		});
		t.equals(clockNewer.last.wallTime, 2n);
	});
	test("updating with newer logical", () => {
		const clock = new HLC({
			wallTime: () => 0n,
			last: new Timestamp({ wallTime: 1n, logical: 2 })
		});
		clock.update(new Timestamp({ wallTime: 1n, logical: 5 }));
		t.equals(clock.last.wallTime, 1n);
		t.equals(clock.last.logical, 6);
	});
	test("updating with older logical", () => {
		const clock = new HLC({
			wallTime: () => 0n,
			last: new Timestamp({ wallTime: 1n, logical: 5 })
		});
		clock.update(new Timestamp({ wallTime: 1n, logical: 2 }));
		t.equals(clock.last.wallTime, 1n);
		t.equals(clock.last.logical, 6);
	});

	test("forward clock jump error", () => {
		let myTime = 1n;
		const wallTime = () => myTime;
		const clockNoError = new HLC({ wallTime });
		const clockError = new HLC({
			wallTime,
			toleratedForwardClockJump: 10n
		});
		t.equals(clockError.toleratedForwardClockJump, 10n);
		myTime = 2n;
		t.deepEquals(clockError.now(), clockNoError.now());
		myTime = 20n;
		t.equals(
			clockNoError.now().compare(new Timestamp({ wallTime: 20n, logical: 0 })),
			0
		);
		t.throws(() => clockError.now(), new ForwardJumpError(18n, 10n));
	});
	test("maxOffset error", () => {
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
				logical: 2
			})
		);
		t.throws(
			() => clockError.update(jumpStamp),
			new ClockOffsetError(20n, 10n)
		);
	});
	test("wall overflow error", () => {
		t.throws(
			() => {
				new HLC({ wallTime: () => 18446744073709551615n + 1n }).now();
			},
			new WallTimeOverflowError(18446744073709551616n, 18446744073709551615n)
		);
		t.throws(
			() => {
				new HLC({
					wallTime: () => 2n,
					wallTimeUpperBound: 1n
				}).now();
			},
			new WallTimeOverflowError(2n, 1n)
		);
	});
	test("logical overflow leads to physical increase", () => {
		const clock = new HLC({
			wallTime: () => 0n,
			last: new Timestamp({ wallTime: 0n, logical: 0xffffffff - 1 })
		});
		t.deepEquals(
			clock.now(),
			new Timestamp({
				wallTime: 0n,
				logical: 0xffffffff
			})
		);
		t.deepEquals(
			clock.now(),
			new Timestamp({
				wallTime: 1n,
				logical: 0
			})
		);
	});
	test("example: usage", () => {
		const clock = new HLC({
			maxOffset: 0n, // [default=0] Maximum time in nanosecons that another timestamp may exceed the wall-clock before an error is thrown.
			toleratedForwardClockJump: 0n, // [default=0] Maximum time in nanoseconds that the wall-clock may exceed the previous timestamp before an error is thrown. Setting it 0 will disable it.
			wallTimeUpperBound: 0n, // [default=0] will throw an error if the wallTime exceeds this value. Setting it to 0 will limit it to the uint64 max-value.
			last: undefined // [default=undefined] The last known timestamp to start off, useful for restoring a clock's state
		});

		const timestamp = clock.now();

		// Makes sure that the next timestamp is bigger than the other timestamp
		clock.update(new Timestamp({ wallTime: 1n }));

		// Turn the clock into an Uint8Array
		const bytes = serialize(timestamp);

		expect(deserialize(bytes, Timestamp).compare(timestamp)).toEqual(0);
	});
	test("example: clock drift", () => {
		try {
			const clock = new HLC({
				maxOffset: BigInt(60 * 1e9) /* 1 minute in nanoseconds */
			});
			const timestamp = clock.now();
			clock.update(
				new Timestamp({
					wallTime: timestamp.wallTime + BigInt(120 * 1e9)
				})
			);
			t.fail("error should have thrown");
		} catch (error: any) {
			if (error instanceof ClockOffsetError === false) {
				throw error;
			}
			t.deepEquals(error, new ClockOffsetError(error.offset, error.maxOffset));
		}
	});
	test("example: clock drift", () => {
		try {
			const wallTimeUpperBound =
				BigInt(new Date("2022-01-01T00:00:00.000Z").getTime()) * BigInt(1e6);
			const clock = new HLC({
				wallTime: () => wallTimeUpperBound + 1n, // Faking a wallTime that is beyond the max we allow
				wallTimeUpperBound
			});
			clock.now();
			t.fail("error should have thrown");
		} catch (error: any) {
			if (error instanceof WallTimeOverflowError === false) {
				throw error;
			}
			t.deepEquals(error, new WallTimeOverflowError(error.time, error.maxTime));
		}
	});
	test("example: clock drift", () => {
		const clock = new HLC({
			toleratedForwardClockJump: BigInt(1e6) /* 1 ms in nanoseconds */
		});
		setTimeout(() => {
			try {
				clock.now();
				t.fail("error should have thrown");
			} catch (error: any) {
				if (error instanceof ForwardJumpError === false) {
					throw error;
				}
				t.deepEquals(
					error,
					new ForwardJumpError(error.timejump, error.tolerance)
				);
			}
		}, 10); // we didn't update the clock in 10 seconds
	});
	test("example: drift monitoring", () => {
		class CockroachHLC extends HLC {
			monotonicityErrorCount: number;
			constructor(opts) {
				super(opts);
				this.monotonicityErrorCount = 0;
			}

			validateOffset(offset) {
				super.validateOffset(offset);
				if (this.maxOffset > 10n && offset > this.maxOffset / 10n) {
					this.monotonicityErrorCount += 1;
				}
			}
		}

		const clock = new CockroachHLC({
			wallTime: () => 10n,
			maxOffset: 20
		});
		clock.update(new Timestamp({ wallTime: 13n }));
		t.equals(clock.monotonicityErrorCount, 1);
	});
});
