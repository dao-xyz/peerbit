import { Cache } from "@peerbit/cache";
import { Ed25519Keypair, type PublicSignKey } from "@peerbit/crypto";
import { EncoderWrapper } from "@peerbit/riblt";
import { expect } from "chai";
import sinon from "sinon";
import {
	RatelessIBLTSynchronizer,
	RequestAll,
	StartSync,
} from "../src/sync/rateless-iblt.js";

describe("rateless-iblt-syncronizer cache", () => {
	let peer: PublicSignKey;

	before(async () => {
		peer = (await Ed25519Keypair.create()).publicKey;
	});

	it("reuses cached local range encoder across StartSync", async () => {
		const next = sinon.stub().resolves([
			{
				value: {
					hash: "h0",
					hashNumber: 1n,
				},
			},
		]);
		const close = sinon.stub().resolves();
		const all = sinon.stub().throws(new Error("all() must not be used"));
		const done = sinon.stub().returns(true);
		const iterate = sinon.stub().returns({ next, close, all, done });

		const send = sinon.stub().resolves();
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: { send } as any,
			rangeIndex: {} as any,
			entryIndex: { iterate } as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
		});

		const context = { from: peer } as any;
		const createStartSync = () =>
			new StartSync({ from: 0n, to: 10n, symbols: [] });

		expect(await sync.onMessage(createStartSync(), context)).to.equal(true);
		expect(await sync.onMessage(createStartSync(), context)).to.equal(true);

		expect(iterate.callCount).to.equal(1);
		expect(next.calledOnceWithExactly(1_024)).to.equal(true);
		expect(close.calledOnce).to.equal(true);
		expect(all.called).to.equal(false);

		await sync.close();
	});

	it("builds local range encoder from native hash-number resolver", async () => {
		const iterate = sinon.stub().throws(new Error("entry index should not be used"));
		const resolvedHashNumbers = new BigUint64Array([1n, 2n]);
		const resolveHashNumbersInRange = sinon.stub().returns(resolvedHashNumbers);
		const send = sinon.stub().resolves();
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: { send } as any,
			rangeIndex: {} as any,
			entryIndex: { iterate } as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
			resolveHashNumbersInRange,
			sync: { maxRatelessReceiveRangeEntries: 2 },
		});

		expect(
			await sync.onMessage(new StartSync({ from: 0n, to: 10n, symbols: [] }), {
				from: peer,
			} as any),
		).to.equal(true);

		expect(iterate.called).to.equal(false);
		expect(resolveHashNumbersInRange.calledOnce).to.equal(true);
		expect(resolveHashNumbersInRange.firstCall.args[0]).to.deep.equal({
			start1: 0n,
			end1: 10n,
			start2: 0n,
			end2: 0n,
			limit: 3,
		});

		await sync.close();
	});

	it("falls back to simple sync when the native range exceeds the cap", async () => {
		const free = sinon.spy(EncoderWrapper.prototype, "free");
		const send = sinon.stub().resolves();
		const resolveHashNumbersInRange = sinon
			.stub()
			.returns(new BigUint64Array([1n, 2n, 3n]));
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: { send } as any,
			rangeIndex: {} as any,
			entryIndex: {
				iterate: sinon
					.stub()
					.throws(new Error("entry index should not be used")),
			} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
			resolveHashNumbersInRange,
			sync: { maxRatelessReceiveRangeEntries: 2 },
		});

		try {
			expect(
				await sync.onMessage(
					new StartSync({ from: 0n, to: 10n, symbols: [] }),
					{ from: peer } as any,
				),
			).to.equal(true);

			expect(resolveHashNumbersInRange.firstCall.args[0].limit).to.equal(3);
			expect(free.calledOnce).to.equal(true);
			expect(send.calledOnce).to.equal(true);
			expect(send.firstCall.args[0]).to.be.instanceOf(RequestAll);
			expect((sync as any).localRangeEncoderCache.size).to.equal(0);
		} finally {
			await sync.close();
			free.restore();
		}
	});

	it("bounds index iteration and falls back without calling all()", async () => {
		const next = sinon.stub().resolves(
			[1n, 2n, 3n].map((hashNumber) => ({
				value: { hash: `h${hashNumber}`, hashNumber },
			})),
		);
		const close = sinon.stub().resolves();
		const all = sinon.stub().throws(new Error("all() must not be used"));
		const done = sinon.stub().returns(true);
		const send = sinon.stub().resolves();
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: { send } as any,
			rangeIndex: {} as any,
			entryIndex: {
				iterate: sinon.stub().returns({ next, close, all, done }),
			} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
			sync: { maxRatelessReceiveRangeEntries: 2 },
		});

		try {
			await sync.onMessage(new StartSync({ from: 0n, to: 10n, symbols: [] }), {
				from: peer,
			} as any);

			expect(next.calledOnceWithExactly(3)).to.equal(true);
			expect(close.calledOnce).to.equal(true);
			expect(all.called).to.equal(false);
			expect(send.calledOnce).to.equal(true);
			expect(send.firstCall.args[0]).to.be.instanceOf(RequestAll);
		} finally {
			await sync.close();
		}
	});

	it("continues through short index pages up to the overflow sentinel", async () => {
		const next = sinon.stub();
		next.onCall(0).resolves([{ value: { hash: "h1", hashNumber: 1n } }]);
		next.onCall(1).resolves([{ value: { hash: "h2", hashNumber: 2n } }]);
		next
			.onCall(2)
			.resolves([
				{ value: { hash: "h3", hashNumber: 3n } },
				{ value: { hash: "h4", hashNumber: 4n } },
			]);
		const close = sinon.stub().resolves();
		const all = sinon.stub().throws(new Error("all() must not be used"));
		const done = sinon.stub().returns(false);
		const send = sinon.stub().resolves();
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: { send } as any,
			rangeIndex: {} as any,
			entryIndex: {
				iterate: sinon.stub().returns({ next, close, all, done }),
			} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
			sync: { maxRatelessReceiveRangeEntries: 3 },
		});

		try {
			await sync.onMessage(new StartSync({ from: 0n, to: 10n, symbols: [] }), {
				from: peer,
			} as any);

			expect(next.callCount).to.equal(3);
			expect(next.getCall(0).args).to.deep.equal([4]);
			expect(next.getCall(1).args).to.deep.equal([3]);
			expect(next.getCall(2).args).to.deep.equal([2]);
			expect(close.calledOnce).to.equal(true);
			expect(all.called).to.equal(false);
			expect(send.firstCall.args[0]).to.be.instanceOf(RequestAll);
		} finally {
			await sync.close();
		}
	});

	it("frees an empty native range encoder before requesting all", async () => {
		const free = sinon.spy(EncoderWrapper.prototype, "free");
		const send = sinon.stub().resolves();
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: { send } as any,
			rangeIndex: {} as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
			resolveHashNumbersInRange: async () => [],
		});

		try {
			await sync.onMessage(new StartSync({ from: 0n, to: 10n, symbols: [] }), {
				from: peer,
			} as any);

			expect(free.calledOnce).to.equal(true);
			expect(send.calledOnce).to.equal(true);
		} finally {
			await sync.close();
			free.restore();
		}
	});

	it("frees the native range encoder when resolution rejects", async () => {
		const free = sinon.spy(EncoderWrapper.prototype, "free");
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			rangeIndex: {} as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
			resolveHashNumbersInRange: async () => {
				throw new Error("native resolver failed");
			},
		});

		try {
			await expect(
				sync.onMessage(new StartSync({ from: 0n, to: 10n, symbols: [] }), {
					from: peer,
				} as any),
			).to.be.rejectedWith("native resolver failed");
			expect(free.calledOnce).to.equal(true);
		} finally {
			await sync.close();
			free.restore();
		}
	});

	it("does not cache a decoder initializer released after close and reopen", async () => {
		let release!: (values: bigint[]) => void;
		const resolved = new Promise<bigint[]>((resolve) => {
			release = resolve;
		});
		let markStarted!: () => void;
		const started = new Promise<void>((resolve) => {
			markStarted = resolve;
		});
		const free = sinon.spy(EncoderWrapper.prototype, "free");
		const send = sinon.stub().resolves();
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: { send } as any,
			rangeIndex: {} as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
			resolveHashNumbersInRange: async () => {
				markStarted();
				return resolved;
			},
		});

		try {
			const handling = sync.onMessage(
				new StartSync({ from: 0n, to: 10n, symbols: [] }),
				{ from: peer } as any,
			);
			await started;
			await sync.close();
			await sync.open();
			release([1n]);
			expect(await handling).to.equal(true);

			expect(free.calledOnce).to.equal(true);
			expect((sync as any).localRangeEncoderCache.size).to.equal(0);
			expect(send.called).to.equal(false);
		} finally {
			release([]);
			await sync.close();
			free.restore();
		}
	});

	it("does not free the cached encoder when decoder conversion throws", async () => {
		const conversionError = new Error("decoder conversion failed");
		// `to_decoder` borrows (&self) and clones internally, so the cached
		// encoder must survive a throw and must not be copied first. `clone`
		// is spied purely so reintroducing the redundant copy fails here.
		const encoder = {
			to_decoder: sinon.stub().throws(conversionError),
			clone: sinon.spy(),
			free: sinon.spy(),
		};
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			rangeIndex: {} as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
		});
		const ranges = {
			start1: 0n,
			end1: 10n,
			start2: 0n,
			end2: 0n,
		};
		(sync as any).localRangeEncoderCache.set(
			(sync as any).localRangeEncoderCacheKey(ranges),
			{ encoder, version: 0, lastUsed: 0 },
		);

		try {
			await expect(
				(sync as any).getLocalDecoderForRange(ranges),
			).to.be.rejectedWith(conversionError.message);

			expect(encoder.to_decoder.calledOnce).to.equal(true);
			expect(encoder.clone.called).to.equal(false);
			expect(encoder.free.called).to.equal(false);
			await sync.close();
			expect(encoder.free.calledOnce).to.equal(true);
		} finally {
			await sync.close();
		}
	});

	it("frees a produced cached decoder when local-decoder profiling throws", async () => {
		const profileError = new Error("local decoder profile failed");
		const decoder = { free: sinon.spy() };
		const encoder = {
			to_decoder: sinon.stub().returns(decoder),
			clone: sinon.spy(),
			free: sinon.spy(),
		};
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: { send: sinon.stub().resolves() } as any,
			rangeIndex: {} as any,
			entryIndex: {} as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
			sync: {
				profile: (event) => {
					if (event.name === "rateless.localDecoder") {
						throw profileError;
					}
				},
			},
		});
		const ranges = {
			start1: 0n,
			end1: 10n,
			start2: 0n,
			end2: 0n,
		};
		(sync as any).localRangeEncoderCache.set(
			(sync as any).localRangeEncoderCacheKey(ranges),
			{ encoder, version: 0, lastUsed: 0 },
		);

		try {
			await expect(
				(sync as any).getLocalDecoderForRange(ranges),
			).to.be.rejectedWith(profileError.message);

			expect(encoder.to_decoder.calledOnce).to.equal(true);
			expect(encoder.clone.called).to.equal(false);
			expect(decoder.free.calledOnce).to.equal(true);
			expect(encoder.free.called).to.equal(false);
			await sync.close();
			expect(decoder.free.calledOnce).to.equal(true);
			expect(encoder.free.calledOnce).to.equal(true);
		} finally {
			await sync.close();
		}
	});

	it("invalidates cached range encoder on entry removal", async () => {
		const all = sinon.stub().throws(new Error("all() must not be used"));
		const close = sinon.stub().resolves();
		const next = sinon.stub().resolves([
			{
				value: {
					hash: "h0",
					hashNumber: 1n,
				},
			},
		]);
		const iterate = sinon.stub().callsFake(() => ({
			all,
			close,
			next,
			done: sinon.stub().returns(true),
		}));

		const send = sinon.stub().resolves();
		const sync = new RatelessIBLTSynchronizer<"u64">({
			rpc: { send } as any,
			rangeIndex: {} as any,
			entryIndex: { iterate } as any,
			log: {} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			numbers: { maxValue: 2n ** 64n - 1n } as any,
		});

		const context = { from: peer } as any;
		const createStartSync = () =>
			new StartSync({ from: 0n, to: 10n, symbols: [] });

		await sync.onMessage(createStartSync(), context);
		sync.onEntryRemoved("h0");
		await sync.onMessage(createStartSync(), context);

		expect(iterate.callCount).to.equal(2);
		expect(next.callCount).to.equal(2);
		expect(close.callCount).to.equal(2);
		expect(all.called).to.equal(false);

		await sync.close();
	});
});
