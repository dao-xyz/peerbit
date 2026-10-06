import { field } from "@dao-xyz/borsh";
import { Cache } from "@peerbit/cache";
import { Ed25519Keypair, type PublicSignKey } from "@peerbit/crypto";
import { id } from "@peerbit/indexer-interface";
import { HashmapIndex } from "@peerbit/indexer-simple";
import { EncoderWrapper, ready } from "@peerbit/riblt";
import { createSharedLogState } from "@peerbit/shared-log-rust";
import { expect } from "chai";
import sinon from "sinon";
import type {
	HashSymbolRangeResolver,
	SyncProfileEvent,
} from "../src/sync/index.js";
import {
	RatelessIBLTSynchronizer,
	RequestAll,
	StartSync,
} from "../src/sync/rateless-iblt.js";

const MAX = 2n ** 64n - 1n;

class RangeEntry {
	@id({ type: "string" })
	hash: string;

	@field({ type: "u64" })
	hashNumber: bigint;

	constructor(hashNumber: bigint) {
		this.hash = `hash-${hashNumber}`;
		this.hashNumber = hashNumber;
	}
}

const vectors = [
	{
		name: "ordinary exclusive end",
		max: 11n,
		remote: [1n, 3n, 5n],
		extra: [0n, 2n, 6n, 11n],
		range: [1n, 6n],
		selected: 4,
	},
	{
		name: "wrapped exclusive end",
		max: 11n,
		remote: [8n, 10n, 1n, 3n],
		extra: [0n, 2n, 4n, 9n, 11n],
		range: [8n, 4n],
		selected: 8,
	},
	{
		name: "inclusive maximum in the ring gap",
		max: 12n,
		remote: [0n, 4n, 8n],
		extra: [2n, 9n, 12n],
		range: [0n, 9n],
		selected: 4,
	},
	{
		name: "single zero",
		max: MAX,
		remote: [0n],
		extra: [1n, MAX],
		range: [0n, 1n],
		selected: 1,
	},
	{
		name: "single interior coordinate",
		max: MAX,
		remote: [7n],
		extra: [6n, 8n, MAX],
		range: [7n, 8n],
		selected: 1,
	},
	{
		name: "single maximum coordinate",
		max: MAX,
		remote: [MAX],
		extra: [0n, MAX - 1n],
		range: [MAX, 0n],
		selected: 1,
	},
	{
		name: "end wraps to zero",
		max: MAX,
		remote: [MAX - 2n, MAX],
		extra: [0n, MAX - 1n],
		range: [MAX - 2n, 0n],
		selected: 3,
	},
	{
		name: "wrapped range includes maximum",
		max: MAX,
		remote: [MAX - 2n, 0n, 2n],
		extra: [MAX, 1n, 3n],
		range: [MAX - 2n, 3n],
		selected: 5,
	},
	{
		name: "full small ring",
		max: 3n,
		remote: [0n, 1n, 2n, 3n],
		extra: [],
		range: [1n, 1n],
		selected: 4,
	},
] as const;

type ReceiverMode = "index" | "native";

const createReceiver = async (
	mode: ReceiverMode,
	values: readonly bigint[],
	max: bigint,
	cap = 32,
) => {
	const index = new HashmapIndex<RangeEntry>();
	await index.init({ schema: RangeEntry });
	await index.start();
	await index.putBatch(values.map((value) => new RangeEntry(value)));
	const native =
		mode === "native" ? await createSharedLogState("u64") : undefined;
	for (const value of values) {
		native?.putEntryCoordinates(
			`hash-${value}`,
			`gid-${value}`,
			[value],
			false,
			1,
			value,
		);
	}
	const resolve = sinon.spy((range: Parameters<HashSymbolRangeResolver>[0]) => {
		if (range.limit == null)
			throw new Error("Expected a bounded native range query");
		const result = native!.getEntryHashNumbersInRangeU64Limited({
			...range,
			limit: range.limit,
		});
		expect(result).to.be.instanceOf(BigUint64Array);
		return result;
	});
	const iterate = sinon.spy(index, "iterate");
	const events: SyncProfileEvent[] = [];
	const send = sinon.stub().resolves();
	const sync = new RatelessIBLTSynchronizer<"u64">({
		rpc: { send } as any,
		rangeIndex: {} as any,
		entryIndex: index as any,
		log: {} as any,
		coordinateToHash: new Cache<string>({ max: 100 }),
		numbers: { maxValue: max } as any,
		resolveHashNumbersInRange: native ? resolve : undefined,
		sync: {
			maxRatelessReceiveRangeEntries: cap,
			profile: (event) => events.push(event),
		},
	});
	// Observe the raw decoded R-only set, before Simple sync's presence filter.
	const queue = sinon.stub(sync.simple, "queueSync").resolves();
	return {
		index,
		native,
		sync,
		queue,
		send,
		events,
		iterate,
		resolve,
		queryCount: () => (native ? resolve.callCount : iterate.callCount),
		close: async () => {
			await sync.close();
			native?.clear();
			await index.stop();
		},
	};
};

describe("rateless-iblt-syncronizer range endpoints", () => {
	let peer: PublicSignKey;
	before(async () => {
		await ready;
		peer = (await Ed25519Keypair.create()).publicKey;
	});

	for (const senderMode of [
		"native-combined",
		"native-range",
		"javascript",
	] as const) {
		for (const receiverMode of ["index", "native"] as const) {
			for (const vector of vectors) {
				it(`${senderMode}/${receiverMode}: no false R-only for ${vector.name}`, async () => {
					const sandbox = sinon.createSandbox();
					const combined = "add_symbols_sorted_find_range_and_produce";
					const rangeOnly = "add_symbols_sorted_and_find_range";
					if (senderMode !== "native-combined") {
						sandbox.stub(EncoderWrapper.prototype, combined).value(undefined);
					}
					if (senderMode === "javascript") {
						sandbox.stub(EncoderWrapper.prototype, rangeOnly).value(undefined);
					}
					const prepare = sandbox.spy(
						EncoderWrapper.prototype,
						senderMode === "native-combined"
							? combined
							: senderMode === "native-range"
								? rangeOnly
								: "add_symbols",
					);
					const sent: unknown[] = [];
					const sender = new RatelessIBLTSynchronizer<"u64">({
						rpc: {
							send: async (message: unknown) => {
								sent.push(message);
							},
						} as any,
						rangeIndex: {} as any,
						entryIndex: {} as any,
						log: {} as any,
						coordinateToHash: new Cache<string>({ max: 1000 }),
						numbers: { maxValue: vector.max } as any,
					});
					let receiver: Awaited<ReturnType<typeof createReceiver>> | undefined;
					try {
						receiver = await createReceiver(
							receiverMode,
							[...vector.remote, ...vector.extra],
							vector.max,
						);
						// Cross the existing dispatch threshold without padding the IBLT set:
						// the remaining entries use its existing Simple boundary prelude.
						const entries = new Map<string, any>();
						for (let i = 0; i < 334; i++) {
							const hash = `dispatch-${i}`;
							entries.set(hash, {
								hash,
								hashNumber: vector.remote[i] ?? 0n,
								assignedToRangeBoundary: i >= vector.remote.length,
							});
						}
						await sender.onMaybeMissingEntries({
							entries,
							targets: [peer.hashcode()],
						});
						expect(prepare.calledOnce).to.equal(true);
						const starts = sent.filter(
							(message): message is StartSync => message instanceof StartSync,
						);
						expect(starts).to.have.length(1);
						const message = starts[0];
						expect([message.start, message.end]).to.deep.equal(vector.range);
						await receiver.sync.onMessage(message, { from: peer } as any);
						expect(receiver.queue.calledOnce).to.equal(true);
						expect(Array.from(receiver.queue.firstCall.args[0])).to.deep.equal(
							[],
						);
						expect(
							receiver.events
								.filter((event) => event.name === "rateless.rangeQuery")
								.map((event) => event.entries),
						).to.deep.equal([vector.selected]);
						expect(
							receiver.send
								.getCalls()
								.some((call) => call.args[0] instanceof RequestAll),
						).to.equal(false);
						expect(receiver.queryCount()).to.equal(1);
						expect(receiver.iterate.callCount).to.equal(
							receiverMode === "index" ? 1 : 0,
						);
					} finally {
						try {
							await sender.close();
						} finally {
							try {
								await receiver?.close();
							} finally {
								sandbox.restore();
							}
						}
					}
				});
			}
		}
	}

	for (const mode of ["index", "native"] as const) {
		for (const shape of ["wrapped", "full"] as const) {
			it(`${mode}: bounds and invalidates cached ${shape} ranges including MAX`, async () => {
				const receiver = await createReceiver(
					mode,
					[MAX - 2n, MAX, 0n],
					MAX,
					3,
				);
				const encoder = new EncoderWrapper();
				encoder.add_symbols(BigUint64Array.from([MAX - 2n, 0n]));
				const symbols = encoder.produce_next_coded_symbols(64);
				const message = () =>
					new StartSync({
						from: shape === "full" ? 0n : MAX - 2n,
						to: shape === "full" ? 0n : 1n,
						symbols,
					});
				try {
					for (let i = 0; i < 2; i++) {
						await receiver.sync.onMessage(message(), { from: peer } as any);
					}
					expect(receiver.queue.callCount).to.equal(2);
					for (const call of receiver.queue.getCalls()) {
						expect(Array.from(call.args[0])).to.deep.equal([]);
					}
					expect(receiver.queryCount()).to.equal(1);
					expect(
						receiver.events
							.filter((event) => event.name === "rateless.rangeQuery")
							.map((event) => event.entries),
					).to.deep.equal([3]);
					await receiver.index.put(new RangeEntry(MAX - 1n));
					receiver.native?.putEntryCoordinates(
						`hash-${MAX - 1n}`,
						"gid-extra",
						[MAX - 1n],
						false,
						1,
						MAX - 1n,
					);
					receiver.sync.onEntryAddedHash(`hash-${MAX - 1n}`);
					await receiver.sync.onMessage(message(), { from: peer } as any);
					expect(receiver.queryCount()).to.equal(2);
					expect(receiver.queue.callCount).to.equal(2);
					expect(
						receiver.send
							.getCalls()
							.filter((call) => call.args[0] instanceof RequestAll),
					).to.have.length(1);
					if (mode === "native") {
						expect(receiver.resolve.lastCall.args[0].limit).to.equal(4);
					}
				} finally {
					encoder.free();
					await receiver.close();
				}
			});
		}
	}
});
