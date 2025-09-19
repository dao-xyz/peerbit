import { delay } from "@peerbit/time";
import { expect } from "chai";
import all from "it-all";
import { pipe } from "it-pipe";
import pDefer from "p-defer";
import { Uint8ArrayList } from "uint8arraylist";
import { pushableLanes } from "../src/pushable-lanes.js";

describe("it-pushable", () => {
	it("should push input slowly", async () => {
		const source = pushableLanes();
		const input = [1, 2, 3].map((x) => new Uint8Array(x));
		for (let i = 0; i < input.length; i++) {
			setTimeout(() => source.push(input[i]), i * 10);
		}
		setTimeout(() => source.end(), input.length * 10);
		const output = await pipe(source, async (source) => all(source));
		expect(output).to.deep.equal(input);
	});

	it("should buffer input", async () => {
		const source = pushableLanes();
		const input = [1, 2, 3].map((x) => new Uint8Array(x));
		input.forEach((v) => source.push(v));
		setTimeout(() => source.end());
		const output = await pipe(source, async (source) => all(source));
		expect(output).to.deep.equal(input);
	});

	it("should allow end before start", async () => {
		const source = pushableLanes();
		const input = [1, 2, 3].map((x) => new Uint8Array(x));
		input.forEach((v) => source.push(v));
		source.end();
		const output = await pipe(source, async (source) => all(source));
		expect(output).to.deep.equal(input);
	});

	it("should end with error immediately", async () => {
		const source = pushableLanes();
		const input = [1, 2, 3].map((x) => new Uint8Array(x));
		input.forEach((v) => source.push(v));
		source.end(new Error("boom"));

		await expect(
			pipe(source, async (source) => all(source)),
		).to.be.rejected.and.eventually.have.property("message", "boom");
	});

	it("should end with error in the middle", async () => {
		const source = pushableLanes();
		const input = [new Uint8Array([1]), new Error("boom"), new Uint8Array([3])];
		for (let i = 0; i < input.length; i++) {
			setTimeout(() => {
				if (input[i] instanceof Error) {
					source.end(input[i] as Error);
				} else {
					source.push(input[i] as any); // TODO types
				}
			}, i * 10);
		}
		setTimeout(() => source.end(), input.length * 10);

		await expect(
			pipe(source, async (source) => all(source)),
		).to.be.rejected.and.eventually.have.property("message", "boom");
	});

	it("should allow end without push", async () => {
		const source = pushableLanes();
		const input: any[] = [];
		source.end();
		const output = await pipe(source, async (source) => all(source));
		expect(output).to.deep.equal(input);
	});

	it("should allow next after end", async () => {
		const source = pushableLanes();
		const input = [new Uint8Array(1)];
		source.push(input[0]);
		let next = await source.next();
		expect(next.done).to.be.false;
		expect(next.value).equal(input[0]);
		source.end();
		next = await source.next();
		expect(next.done).to.be.true;
		next = await source.next();
		expect(next.done).to.be.true;
	});

	it("should call onEnd", (done) => {
		const source = pushableLanes({
			onEnd: () => {
				done();
			},
		});
		const input = [1, 2, 3].map((x) => new Uint8Array(x));
		for (let i = 0; i < input.length; i++) {
			setTimeout(() => source.push(input[i]), i * 10);
		}
		setTimeout(() => source.end(), input.length * 10);
		void pipe(source, async (source) => all(source));
	});

	it("should call onEnd after onEmpty", async () => {
		const ended = pDefer();
		const source = pushableLanes({
			onEnd: () => {
				ended.resolve();
			},
		});
		source.push(new Uint8Array(1));
		source.push(new Uint8Array(2));
		source.push(new Uint8Array(3));
		source.end();

		await source.onEmpty();
		await ended.promise;
	});

	it("should call onEnd if passed in options object", (done) => {
		const source = pushableLanes({
			onEnd: () => {
				done();
			},
		});
		const input = [1, 2, 3].map((x) => new Uint8Array(x));
		for (let i = 0; i < input.length; i++) {
			setTimeout(() => source.push(input[i]), i * 10);
		}
		setTimeout(() => source.end(), input.length * 10);
		void pipe(source, async (source) => all(source));
	});

	it("should call onEnd even if not piped", (done) => {
		const source = pushableLanes({
			onEnd: () => {
				done();
			},
		});
		source.end();
	});

	it("should call onEnd with error", (done) => {
		const source = pushableLanes({
			onEnd: (err) => {
				expect(err).to.have.property("message", "boom");
				done();
			},
		});
		setTimeout(() => source.end(new Error("boom")), 10);
		void pipe(source, async (source) => all(source)).catch(() => {});
	});

	it("should call onEnd on return before end", (done) => {
		const input = [1, 2, 3, 4, 5].map((x) => new Uint8Array(x));
		const max = 2;
		const output: Uint8Array[] = [];

		const source = pushableLanes({
			onEnd: () => {
				expect(output).to.deep.equal(input.slice(0, max));
				done();
			},
		});

		input.forEach((v, i) => setTimeout(() => source.push(v), i * 10));
		setTimeout(() => source.end(), input.length * 10);

		void (async () => {
			let i = 0;
			for await (const value of source) {
				output.push(value);
				i++;
				if (i === max) break;
			}
		})();
	});

	it("should call onEnd by calling return", (done) => {
		const input = [1, 2, 3, 4, 5].map((x) => new Uint8Array(x));
		const max = 2;
		const output: Uint8Array[] = [];

		const source = pushableLanes({
			onEnd: () => {
				expect(output).to.deep.equal(input.slice(0, max));
				done();
			},
		});

		let index = 0;
		input.forEach((v, i) => {
			setTimeout(() => {
				source.push(input[index]);
				index++;
			}, i * 10);
		});
		setTimeout(() => source.end(), input.length * 10);

		void (async () => {
			let i = 0;
			while (i !== max) {
				i++;
				const { value } = await source.next();

				if (value instanceof Uint8Array) {
					output.push(value);
				}
			}
			await source.return();
		})();
	});

	it("should call onEnd once", (done) => {
		const input = [1, 2, 3, 4, 5].map((x) => new Uint8Array(x));

		let count = 0;
		const source = pushableLanes({
			onEnd: () => {
				count++;
				expect(count).equal(1);
				setTimeout(() => {
					done();
				}, 50);
			},
		});

		input.forEach((v, i) => setTimeout(() => source.push(v), i * 10));

		void (async () => {
			await source.next();
			await source.return();
			await source.next();
		})();
	});

	it("should call onEnd by calling throw", (done) => {
		const input = [1, 2, 3, 4, 5].map((x) => new Uint8Array(x));
		const max = 2;
		const output: Uint8Array[] = [];

		const source = pushableLanes({
			onEnd: (err) => {
				expect(err).to.have.property("message", "boom");
				expect(output).to.deep.equal(input.slice(0, max));
				done();
			},
		});

		input.forEach((v, i) => setTimeout(() => source.push(v), i * 10));
		setTimeout(() => source.end(), input.length * 10);

		void (async () => {
			let i = 0;
			while (i !== max) {
				i++;
				const { value } = await source.next();

				if (value instanceof Uint8Array) {
					output.push(value);
				}
			}
			await source.throw(new Error("boom"));
		})();
	});

	it("should support readableLength for objects", async () => {
		const source = pushableLanes();

		expect(source).to.have.property("readableLength", 0);

		source.push(new Uint8Array([1]));

		expect(source).to.have.property("readableLength", 1);

		source.push(new Uint8Array([1]));

		expect(source).to.have.property("readableLength", 2);

		await source.next();
		expect(source).to.have.property("readableLength", 1);

		await source.next();
		expect(source).to.have.property("readableLength", 0);
	});

	it("should support readableLength for bytes", async () => {
		const source = pushableLanes();

		expect(source).to.have.property("readableLength", 0);

		source.push(Uint8Array.from([1, 2]));
		expect(source).to.have.property("readableLength", 2);

		source.push(Uint8Array.from([3, 4, 5]));
		expect(source).to.have.property("readableLength", 5);

		await source.next();
		expect(source).to.have.property("readableLength", 3);

		await source.next();
		expect(source).to.have.property("readableLength", 0);
	});

	it("should support readableLength for Uint8ArrayLists", async () => {
		const source = pushableLanes<Uint8ArrayList>();

		expect(source).to.have.property("readableLength", 0);

		source.push(new Uint8ArrayList(Uint8Array.from([1, 2])));
		expect(source).to.have.property("readableLength", 2);

		source.push(new Uint8ArrayList(Uint8Array.from([3, 4, 5])));
		expect(source).to.have.property("readableLength", 5);

		await source.next();
		expect(source).to.have.property("readableLength", 3);

		await source.next();
		expect(source).to.have.property("readableLength", 0);
	});

	it("should support readableLength for mixed Uint8ArrayLists and Uint8Arrays", async () => {
		const source = pushableLanes<Uint8ArrayList | Uint8Array>();

		expect(source).to.have.property("readableLength", 0);

		source.push(new Uint8ArrayList(Uint8Array.from([1, 2])));
		expect(source).to.have.property("readableLength", 2);

		source.push(Uint8Array.from([3, 4, 5]));
		expect(source).to.have.property("readableLength", 5);

		await source.next();
		expect(source).to.have.property("readableLength", 3);

		await source.next();
		expect(source).to.have.property("readableLength", 0);
	});

	it("should return from onEmpty when the pushable becomes empty", async () => {
		const source = pushableLanes();

		source.push(new Uint8Array([1]));

		let resolved = false;
		const onEmptyPromise = source.onEmpty().then(() => {
			resolved = true;
		});

		expect(resolved).to.be.false;

		source.push(new Uint8Array([2]));
		expect(resolved).to.be.false;

		await source.next();
		expect(resolved).to.be.false;

		await source.next();
		await onEmptyPromise;
		expect(resolved).to.be.true;
	});

	it("should reject from onEmpty when the passed abort signal is aborted", async () => {
		const source = pushableLanes();

		source.push(new Uint8Array([1]));

		const controller = new AbortController();
		const p = source.onEmpty({ signal: controller.signal });

		source.push(new Uint8Array([2]));

		controller.abort();

		await expect(p).to.be.rejected.eventually.have.property(
			"code",
			"ABORT_ERR",
		);
	});

	describe("lanes", () => {
		it("drains lower lanes before upper", async () => {
			const source = pushableLanes({ lanes: 3 });
			source.push(new Uint8Array([2]), 1);
			source.push(new Uint8Array([3]), 1);
			source.push(new Uint8Array([4]), 2);
			source.push(new Uint8Array([1]), 0);
			expect(source.readableLength).equal(4);
			setTimeout(() => source.end());
			const output = await pipe(source, async (source) => all(source));
			expect(output).to.deep.equal([
				new Uint8Array([1]),
				new Uint8Array([2]),
				new Uint8Array([3]),
				new Uint8Array([4]),
			]);
			expect(source.readableLength).equal(0);
		});

		it("can get size after end", async () => {
			const source = pushableLanes({ lanes: 3 });
			source.end(new Error("Test error"));
			expect(source.readableLength).equal(0); // no actual data in the error
			expect(source.getReadableLength(2)).equal(0); // no actual data in the error
			await expect(source.next()).rejectedWith("Test error");
			expect(source.readableLength).equal(0);
			expect(source.getReadableLength(2)).equal(0);
		});
	});

	it("does not starve any lane under sustained lane-0 pressure (2 lanes, WRR default)", async () => {
		const p = pushableLanes<Uint8Array>({ lanes: 2 }); // fairness defaults to 'wrr'
		const seen: number[] = [];

		(async () => {
			for await (const v of p) seen.push(v[0]);
		})();

		// Start hammering lane 0
		const feeder = setInterval(() => {
			for (let i = 0; i < 16; i++) p.push(new Uint8Array([0]), 0);
		}, 0);

		// Inject lane-1 signal after flood begins
		setTimeout(() => p.push(new Uint8Array([1]), 1), 20);

		await delay(250);
		clearInterval(feeder);

		expect(seen).to.include(1);
	});

	it("respects weights roughly (lane 0 ~4x lane 1)", async () => {
		const p = pushableLanes<Uint8Array>({
			lanes: 2,
			fairness: "wrr",
			weights: [4, 1],
		});
		const seen: number[] = [];

		(async () => {
			for await (const v of p) {
				seen.push(v[0]);
				// tiny delay -> ensures backlog/competition between lanes
				await new Promise((resolve) => setTimeout(resolve, 1));
			}
		})();

		const feed0 = setInterval(() => p.push(new Uint8Array([0]), 0), 0);
		const feed1 = setInterval(() => p.push(new Uint8Array([1]), 1), 0);

		await delay(200);
		clearInterval(feed0);
		clearInterval(feed1);
		p.end();

		const zeros = seen.filter((x) => x === 0).length;
		const ones = seen.filter((x) => x === 1).length;
		expect(zeros).to.be.greaterThan(ones * 2.2); // 4:1 target with tolerance
	});

	it("distributes roughly as [8,4,2,1] when lanes=4 and fairness is default", async () => {
		const L = 4;
		const p = pushableLanes<Uint8Array>({ lanes: L }); // defaults: fairness='wrr', bias=2, weights auto
		const counts = Array<number>(L).fill(0);

		// consumer: small processing delay to create contention
		const consumer = (async () => {
			for await (const v of p) {
				const lane = v[0]; // we encode lane index in first byte
				counts[lane] += 1;
				await delay(1); // keep a backlog so WRR can arbitrate
			}
		})();

		// producers: equal push rate on all lanes (bursting to deepen the backlog)
		const feeders = Array.from({ length: L }, (_, i) =>
			setInterval(() => {
				for (let k = 0; k < 6; k++) p.push(new Uint8Array([i]), i);
			}, 0),
		);

		// run for a short while
		await delay(300);

		// stop producers and end stream
		feeders.forEach(clearInterval);
		p.end();
		await consumer;

		// sanity: we must have seen items from all lanes
		counts.forEach((c, i) =>
			expect(c, `lane ${i} had zero`).to.be.greaterThan(0),
		);

		// default bias=2 ⇒ expected weights [8,4,2,1]
		const expected = [8, 4, 2, 1];
		const expectedFrac = expected.map(
			(w) => w / expected.reduce((a, b) => a + b, 0),
		);

		const total = counts.reduce((a, b) => a + b, 0);
		const actualFrac = counts.map((c) => c / total);

		// 1) monotonicity (lane0 > lane1 > lane2 > lane3)
		for (let i = 0; i < L - 1; i++) {
			expect(actualFrac[i], `lane ${i} vs ${i + 1}`).to.be.greaterThan(
				actualFrac[i + 1],
			);
		}

		// 2) rough proportionality to [8,4,2,1]
		// allow generous tolerance because of timer jitter & scheduling
		const ABS_TOL = 0.2; // ±20 percentage points
		for (let i = 0; i < L; i++) {
			const diff = Math.abs(actualFrac[i] - expectedFrac[i]);
			expect(diff, `lane ${i} fraction off too far`).to.be.lessThan(ABS_TOL);
		}

		// 3) adjacent ratios are meaningfully separated (extra guard)
		// expected adjacent ratios: 8/4=2, 4/2=2, 2/1=2 → require at least ~1.5x
		for (let i = 0; i < L - 1; i++) {
			const ratio = counts[i] / counts[i + 1];
			expect(ratio, `adjacent ratio lane${i}/lane${i + 1}`).to.be.greaterThan(
				1.5,
			);
		}
	});

	it("scales to many lanes and remains starvation-free", async () => {
		const L = 5;
		const p = pushableLanes<Uint8Array>({ lanes: L, fairness: "wrr", bias: 2 });
		const seen = new Set<number>();

		(async () => {
			for await (const v of p) seen.add(v[0]);
		})();

		// hammer lane 0, trickle others once
		const feeder = setInterval(() => {
			for (let i = 0; i < 8; i++) p.push(new Uint8Array([0]), 0);
		}, 0);
		for (let i = 1; i < L; i++)
			setTimeout(() => p.push(new Uint8Array([i]), i), 10 * i);

		await delay(400);
		clearInterval(feeder);
		p.end();

		for (let i = 1; i < L; i++) {
			expect(seen.has(i), `lane ${i} was starved`).to.equal(true);
		}
	});

	it("drains single active lane at full speed (no fairness penalty)", async () => {
		const p = pushableLanes<Uint8Array>({ lanes: 3, fairness: "wrr" });
		const out: number[] = [];

		(async () => {
			for await (const v of p) out.push(v[0]);
		})();

		for (let i = 0; i < 100; i++) p.push(new Uint8Array([2]), 2);
		await delay(50);
		p.end();

		expect(out.filter((x) => x === 2).length).to.be.greaterThan(90);
	});

	it("strict priority starves lane 1 when lane 0 never empties (slow consumer)", async () => {
		const p = pushableLanes<Uint8Array>({ lanes: 2, fairness: "priority" });
		const seen: number[] = [];

		// 1) Put the lane-1 sentinel in BEFORE the consumer starts.
		p.push(new Uint8Array([1]), 1);

		// 2) Prefill lane 0 so it’s non-empty from the first read.
		for (let i = 0; i < 5000; i++) p.push(new Uint8Array([0]), 0);

		// 3) Slow consumer (process ~1 item per few ms)
		let running = true;
		const consumer = (async () => {
			for await (const v of p) {
				seen.push(v[0]);
				await delay(2); // slow down processing
				if (!running) break; // allow exit
			}
		})();

		// Let it run a bit
		await delay(250);
		running = false;
		p.end();
		await consumer;

		// With strict priority and lane 0 never empty, lane 1 should be starved
		expect(seen.includes(1)).to.equal(false);

		// Optional sanity: lane 0 should still have been non-empty the whole time
		// (You can also instrument getReadableLength(0) over time if you want)
	});
});
