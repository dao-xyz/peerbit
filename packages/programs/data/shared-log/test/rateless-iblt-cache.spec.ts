import { Cache } from "@peerbit/cache";
import { Ed25519Keypair, type PublicSignKey } from "@peerbit/crypto";
import { EncoderWrapper } from "@peerbit/riblt";
import { expect } from "chai";
import sinon from "sinon";
import {
	RatelessIBLTSynchronizer,
	StartSync,
} from "../src/sync/rateless-iblt.js";

describe("rateless-iblt-syncronizer cache", () => {
	let peer: PublicSignKey;

	before(async () => {
		peer = (await Ed25519Keypair.create()).publicKey;
	});

	it("reuses cached local range encoder across StartSync", async () => {
		const iterate = sinon.stub().returns({
			all: async () => [
				{
					value: {
						hash: "h0",
						hashNumber: 1n,
					},
				},
			],
		});

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
		});

		await sync.close();
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

	it("frees a cached encoder clone once when decoder conversion throws", async () => {
		const conversionError = new Error("decoder conversion failed");
		const clone = {
			to_decoder: sinon.stub().throws(conversionError),
			free: sinon.spy(),
		};
		const encoder = {
			clone: sinon.stub().returns(clone),
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

			expect(encoder.clone.calledOnce).to.equal(true);
			expect(clone.to_decoder.calledOnce).to.equal(true);
			expect(clone.free.calledOnce).to.equal(true);
			expect(encoder.free.called).to.equal(false);
			await sync.close();
			expect(clone.free.calledOnce).to.equal(true);
			expect(encoder.free.calledOnce).to.equal(true);
		} finally {
			await sync.close();
		}
	});

	it("frees a produced cached decoder when local-decoder profiling throws", async () => {
		const profileError = new Error("local decoder profile failed");
		const decoder = { free: sinon.spy() };
		const clone = {
			to_decoder: sinon.stub().returns(decoder),
			free: sinon.spy(),
		};
		const encoder = {
			clone: sinon.stub().returns(clone),
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

			expect(encoder.clone.calledOnce).to.equal(true);
			expect(clone.to_decoder.calledOnce).to.equal(true);
			expect(clone.free.calledOnce).to.equal(true);
			expect(decoder.free.calledOnce).to.equal(true);
			expect(encoder.free.called).to.equal(false);
			await sync.close();
			expect(clone.free.calledOnce).to.equal(true);
			expect(decoder.free.calledOnce).to.equal(true);
			expect(encoder.free.calledOnce).to.equal(true);
		} finally {
			await sync.close();
		}
	});

	it("invalidates cached range encoder on entry removal", async () => {
		const iterate = sinon.stub().returns({
			all: async () => [
				{
					value: {
						hash: "h0",
						hashNumber: 1n,
					},
				},
			],
		});

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

		await sync.close();
	});
});
