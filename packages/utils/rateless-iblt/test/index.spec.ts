import { expect } from "chai";
import { DecoderWrapper, EncoderWrapper } from "../dist/index.js";

describe("riblt", () => {
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
