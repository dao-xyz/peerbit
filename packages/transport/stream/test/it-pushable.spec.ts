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
});
