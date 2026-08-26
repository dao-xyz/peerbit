import { Cache } from "@peerbit/cache";
import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";
import sinon from "sinon";
import { createSyncronizer } from "../src/sync/factory.js";
import { StartSync } from "../src/sync/rateless-iblt.js";

describe("sync factory", () => {
	it("preserves unlimited range resolution for legacy custom synchronizers", async () => {
		const legacyRange = sinon.stub().returns([1n, 2n]);
		const legacyTypedRange = sinon.stub().returns(undefined);
		const limitedRange = sinon
			.stub()
			.throws(new Error("limited API must not serve a legacy resolver call"));
		const limitedTypedRange = sinon
			.stub()
			.throws(new Error("limited API must not serve a legacy resolver call"));
		let components: any;
		class LegacyCustomSyncronizer {
			constructor(received: any) {
				components = received;
			}
		}

		createSyncronizer<"u64">({
			coordinateToHash: new Cache<string>({ max: 10 }),
			entryIndex: {} as any,
			getNativeState: () =>
				({
					getEntryHashNumbersInRange: legacyRange,
					getEntryHashNumbersInRangeU64: legacyTypedRange,
					getEntryHashNumbersInRangeLimited: limitedRange,
					getEntryHashNumbersInRangeU64Limited: limitedTypedRange,
				}) as any,
			isEntryRecentlyKnownByPeer: () => false,
			log: {} as any,
			numbers: { maxValue: 2n ** 64n - 1n } as any,
			peerSupportsRawExchangeHeads: () => false,
			rangeIndex: {} as any,
			resolution: "u64",
			rpc: {} as any,
			sendRawExchangeHeads: sinon.stub().resolves(0),
			syncronizer: LegacyCustomSyncronizer as any,
			warn: sinon.stub(),
		});

		const range = {
			start1: 0n,
			end1: 10n,
			start2: 0n,
			end2: 0n,
		};
		const resolved = await components.resolveHashNumbersInRange(range);

		expect(Array.from(resolved)).to.deep.equal([1n, 2n]);
		expect(legacyTypedRange.calledOnceWithExactly(range)).to.equal(true);
		expect(legacyRange.calledOnceWithExactly(range)).to.equal(true);
		expect(limitedTypedRange.called).to.equal(false);
		expect(limitedRange.called).to.equal(false);
	});

	it("pages the index instead of calling an old unlimited native range API", async () => {
		const peer = (await Ed25519Keypair.create()).publicKey;
		const legacyRange = sinon
			.stub()
			.throws(new Error("legacy native range API must not be called"));
		const legacyTypedRange = sinon
			.stub()
			.throws(new Error("legacy typed native range API must not be called"));
		const next = sinon.stub().resolves([
			{
				value: {
					hash: "head-a",
					hashNumber: 1n,
				},
			},
		]);
		const close = sinon.stub().resolves();
		const iterate = sinon.stub().returns({
			all: sinon.stub().throws(new Error("all() must not be used")),
			close,
			done: sinon.stub().returns(true),
			next,
		});
		const sync = createSyncronizer<"u64">({
			coordinateToHash: new Cache<string>({ max: 10 }),
			entryIndex: { iterate } as any,
			getNativeState: () =>
				({
					getEntryHashNumbersInRange: legacyRange,
					getEntryHashNumbersInRangeU64: legacyTypedRange,
				}) as any,
			isEntryRecentlyKnownByPeer: () => false,
			log: {} as any,
			numbers: { maxValue: 2n ** 64n - 1n } as any,
			peerSupportsRawExchangeHeads: () => false,
			rangeIndex: {} as any,
			resolution: "u64",
			rpc: { send: sinon.stub().resolves() } as any,
			sendRawExchangeHeads: sinon.stub().resolves(0),
			sync: { maxRatelessReceiveRangeEntries: 2 },
			warn: sinon.stub(),
		});

		try {
			expect(
				await sync.onMessage(
					new StartSync({ from: 0n, to: 10n, symbols: [] }),
					{ from: peer } as any,
				),
			).to.equal(true);

			expect(legacyRange.called).to.equal(false);
			expect(legacyTypedRange.called).to.equal(false);
			expect(iterate.calledOnce).to.equal(true);
			expect(iterate.firstCall.args[0].consistency).to.equal("weak");
			expect(next.calledOnceWithExactly(3)).to.equal(true);
			expect(close.calledOnce).to.equal(true);
		} finally {
			await sync.close();
		}
	});
});
