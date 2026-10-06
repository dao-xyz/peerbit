/* eslint-env mocha */
// Node-only: uses fs-based wasm init
import { expect } from "chai";
import { DecoderWrapper, EncoderWrapper, ready } from "../dist/index.js";

describe("riblt", () => {
	before(async () => {
		await ready;
	});

	it("diff", async () => {
		const aliceSymbols = [1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n, 9n, 10n].map(
			(n) => n,
		);
		const bobSymbols = [1n, 3n, 4n, 5n, 6n, 7n, 8n, 9n, 10n, 11n].map((n) => n);

		const encoder = new EncoderWrapper();
		aliceSymbols.forEach((sym) => encoder.add_symbol(sym));

		const decoder = new DecoderWrapper();
		bobSymbols.forEach((sym) => decoder.add_symbol(sym));

		let cost = 0;
		let once = false;
		while (!decoder.decoded() || !once) {
			once = true;
			const codedSymbol = encoder.produce_next_coded_symbol();
			decoder.add_coded_symbol(codedSymbol);
			decoder.try_decode();
			cost += 1;
		}

		const remoteSymbols = decoder.get_remote_symbols();
		const localSymbols = decoder.get_local_symbols();

		expect(remoteSymbols.length).to.equal(1);
		expect(remoteSymbols[0]).to.equal(2n);

		expect(localSymbols.length).to.equal(1);
		expect(localSymbols[0]).to.equal(11n);
		expect(cost).to.equal(2);
	});

	it("diff (add_symbols)", async () => {
		const aliceSymbols = [1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n, 9n, 10n];
		const bobSymbols = [1n, 3n, 4n, 5n, 6n, 7n, 8n, 9n, 10n, 11n];

		const encoder = new EncoderWrapper();
		encoder.add_symbols(BigUint64Array.from(aliceSymbols));

		const decoder = new DecoderWrapper();
		bobSymbols.forEach((sym) => decoder.add_symbol(sym));

		let cost = 0;
		let once = false;
		while (!decoder.decoded() || !once) {
			once = true;
			const codedSymbol = encoder.produce_next_coded_symbol();
			decoder.add_coded_symbol(codedSymbol);
			decoder.try_decode();
			cost += 1;
		}

		const remoteSymbols = decoder.get_remote_symbols();
		const localSymbols = decoder.get_local_symbols();

		expect(remoteSymbols.length).to.equal(1);
		expect(remoteSymbols[0]).to.equal(2n);

		expect(localSymbols.length).to.equal(1);
		expect(localSymbols[0]).to.equal(11n);
		expect(cost).to.equal(2);
	});

	it("diff (batched coded symbols)", async () => {
		const aliceSymbols = [1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n, 9n, 10n];
		const bobSymbols = [1n, 3n, 4n, 5n, 6n, 7n, 8n, 9n, 10n, 11n];

		const encoder = new EncoderWrapper();
		encoder.add_symbols(BigUint64Array.from(aliceSymbols));

		const decoder = new DecoderWrapper();
		decoder.add_symbols(BigUint64Array.from(bobSymbols));

		const codedSymbols = encoder.produce_next_coded_symbols(2);
		expect(codedSymbols).to.be.instanceOf(BigUint64Array);
		expect(codedSymbols.length).to.equal(6);
		expect(decoder.add_coded_symbols_and_try_decode(codedSymbols)).to.equal(
			true,
		);

		const remoteSymbols = decoder.get_remote_symbol_values();
		const localSymbols = decoder.get_local_symbol_values();

		expect(remoteSymbols).to.be.instanceOf(BigUint64Array);
		expect(localSymbols).to.be.instanceOf(BigUint64Array);
		expect(Array.from(remoteSymbols)).to.deep.equal([2n]);
		expect(Array.from(localSymbols)).to.deep.equal([11n]);
	});

	it("prepares an encoder from unsorted symbols", async () => {
		const symbols = [10n, 1n, 3n, 8n];
		const encoder = new EncoderWrapper();
		const range = encoder.add_symbols_sorted_and_find_range(
			BigUint64Array.from(symbols),
			11n,
		);

		expect(range).to.be.instanceOf(BigUint64Array);
		expect(Array.from(range)).to.deep.equal([8n, 4n]);

		const decoder = new DecoderWrapper();
		decoder.add_symbols(BigUint64Array.from(symbols));
		expect(
			decoder.add_coded_symbols_and_try_decode(
				encoder.produce_next_coded_symbols(1),
			),
		).to.equal(true);
		expect(Array.from(decoder.get_remote_symbol_values())).to.deep.equal([]);
		expect(Array.from(decoder.get_local_symbol_values())).to.deep.equal([]);

		const single = new EncoderWrapper();
		expect(
			Array.from(
				single.add_symbols_sorted_and_find_range(
					BigUint64Array.from([11n]),
					11n,
				),
			),
		).to.deep.equal([11n, 0n]);
	});

	it("prepares an encoder and produces coded symbols", async () => {
		const symbols = [10n, 1n, 3n, 8n];
		const encoder = new EncoderWrapper();
		const prepared = encoder.add_symbols_sorted_find_range_and_produce(
			BigUint64Array.from(symbols),
			11n,
			1,
		);

		expect(prepared).to.be.instanceOf(BigUint64Array);
		expect(prepared.length).to.equal(5);
		expect(Array.from(prepared.subarray(0, 2))).to.deep.equal([8n, 4n]);

		const decoder = new DecoderWrapper();
		decoder.add_symbols(BigUint64Array.from(symbols));
		expect(
			decoder.add_coded_symbols_and_try_decode(prepared.subarray(2)),
		).to.equal(true);
		expect(Array.from(decoder.get_remote_symbol_values())).to.deep.equal([]);
		expect(Array.from(decoder.get_local_symbol_values())).to.deep.equal([]);
	});

	describe("prepared circular ranges", () => {
		const maxU64 = (1n << 64n) - 1n;
		const cases = [
			{
				name: "standard range",
				symbols: [6n, 2n, 4n],
				max: 15n,
				range: [2n, 7n],
				receiver: [0n, 1n, 2n, 3n, 4n, 5n, 6n, 7n, 15n],
			},
			{
				name: "wrapped range",
				symbols: [14n, 1n, 2n],
				max: 15n,
				range: [14n, 3n],
				receiver: [0n, 1n, 2n, 3n, 10n, 14n, 15n],
			},
			{
				name: "maximum u64 endpoint",
				symbols: [maxU64, maxU64 - 2n],
				max: maxU64,
				range: [maxU64 - 2n, 0n],
				receiver: [0n, maxU64 - 3n, maxU64 - 2n, maxU64 - 1n, maxU64],
			},
			{
				name: "single symbol",
				symbols: [7n],
				max: 15n,
				range: [7n, 8n],
				receiver: [6n, 7n, 8n],
			},
			{
				name: "single maximum symbol",
				symbols: [maxU64],
				max: maxU64,
				range: [maxU64, 0n],
				receiver: [0n, maxU64 - 1n, maxU64],
			},
			{
				name: "full tiny ring",
				symbols: [3n, 0n, 2n, 1n],
				max: 3n,
				range: [1n, 1n],
				receiver: [0n, 1n, 2n, 3n],
			},
			{
				name: "wraparound gap breaks the apparent tie",
				symbols: [4n, 0n],
				max: 8n,
				range: [0n, 5n],
				receiver: [0n, 1n, 3n, 4n, 5n, 8n],
			},
		];

		for (const produce of [false, true]) {
			for (const fixture of cases) {
				it(`${produce ? "fused" : "range-only"}: ${fixture.name}`, () => {
					const encoder = new EncoderWrapper();
					const decoder = new DecoderWrapper();
					try {
						const prepared = produce
							? encoder.add_symbols_sorted_find_range_and_produce(
									BigUint64Array.from(fixture.symbols),
									fixture.max,
									1,
								)
							: encoder.add_symbols_sorted_and_find_range(
									BigUint64Array.from(fixture.symbols),
									fixture.max,
								);
						const [start, end] = prepared;
						// Circular ranges include start and exclude end. Equal endpoints
						// describe the full ring for a nonempty prepared set.
						const selected = fixture.receiver.filter((symbol) =>
							start === end
								? true
								: start < end
									? symbol >= start && symbol < end
									: symbol >= start || symbol < end,
						);
						decoder.add_symbols(BigUint64Array.from(selected));
						let decoded = decoder.add_coded_symbols_and_try_decode(
							produce
								? prepared.subarray(2)
								: encoder.produce_next_coded_symbols(1),
						);
						for (let count = 1; !decoded && count < 128; count++) {
							decoded = decoder.add_coded_symbols_and_try_decode(
								encoder.produce_next_coded_symbols(1),
							);
						}
						expect(decoded, "bounded decode must complete").to.equal(true);
						expect(
							Array.from(decoder.get_remote_symbol_values()),
						).to.deep.equal([]);
						expect(
							Array.from(decoder.get_local_symbol_values()),
						).to.have.members(
							selected.filter((symbol) => !fixture.symbols.includes(symbol)),
						);
						expect([start, end]).to.deep.equal(fixture.range);
					} finally {
						decoder.free();
						encoder.free();
					}
				});
			}
		}
	});

	it("no diff", async () => {
		const aliceSymbols = [1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n, 9n, 10n];
		const bobSymbols = [1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n, 9n, 10n];

		const encoder = new EncoderWrapper();
		aliceSymbols.forEach((sym) => encoder.add_symbol(sym));

		const decoder = new DecoderWrapper();
		bobSymbols.forEach((sym) => decoder.add_symbol(sym));

		let cost = 0;
		let once = false;
		while (!decoder.decoded() || !once) {
			once = true;
			const codedSymbol = encoder.produce_next_coded_symbol();
			decoder.add_coded_symbol(codedSymbol);
			decoder.try_decode();
			cost += 1;
		}

		const remoteSymbols = decoder.get_remote_symbols();
		const localSymbols = decoder.get_local_symbols();

		expect(remoteSymbols.length).to.equal(0);
		expect(localSymbols.length).to.equal(0);
		expect(cost).to.equal(1);
	});
});
