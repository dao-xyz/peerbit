import { Cache } from "@peerbit/cache";
import { Ed25519Keypair } from "@peerbit/crypto";
import { CONVERGENCE_MESSAGE_PRIORITY } from "@peerbit/stream-interface";
import { expect } from "chai";
import sinon from "sinon";
import { RawExchangeHeadsMessage } from "../src/exchange-heads.js";
import {
	RequestMaybeSync,
	RequestMaybeSyncCoordinateCapabilities,
	RequestMaybeSyncCoordinate,
	ResponseMaybeSync,
	SYNC_MESSAGE_PRIORITY,
	ResponseMaybeSyncCapabilities,
	SimpleSyncronizer,
} from "../src/sync/simple.js";

describe("sync-chunking", () => {
	let peerA: Awaited<ReturnType<typeof Ed25519Keypair.create>>["publicKey"];

	before(async () => {
		peerA = (await Ed25519Keypair.create()).publicKey;
	});

	it("uses the convergence transport priority for sync messages", () => {
		expect(SYNC_MESSAGE_PRIORITY).to.equal(CONVERGENCE_MESSAGE_PRIORITY);
	});

	it("chunks hash maybe-sync messages", async () => {
		const send = sinon.stub().resolves();
		const rpc = { send } as any;
		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: {} as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				maxSimpleHashesPerMessage: 2,
			},
		});

		const entries = new Map<string, any>();
		for (let i = 0; i < 5; i++) {
			entries.set(`h${i}`, { hash: `h${i}` });
		}

		await sync.onMaybeMissingEntries({
			entries: entries as any,
			targets: ["p"],
		});

		expect(send.callCount).to.equal(3);
		const sentHashes = send.getCalls().map((call) => {
			const message = call.args[0];
			expect(call.args[1].priority).to.equal(SYNC_MESSAGE_PRIORITY);
			expect(message).to.be.instanceOf(RequestMaybeSync);
			return (message as RequestMaybeSync).hashes;
		});
		expect(sentHashes.flat()).to.deep.equal(["h0", "h1", "h2", "h3", "h4"]);
		expect(sentHashes.map((x) => x.length)).to.deep.equal([2, 2, 1]);
	});

	it("chunks coordinate maybe-sync requests", async () => {
		const send = sinon.stub().resolves();
		const rpc = { send } as any;
		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: { count: async () => 0 } as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				maxSimpleCoordinatesPerMessage: 2,
			},
		});

		await sync.queueSync(
			[1n, 2n, 3n, 4n, 5n],
			{
				hashcode: () => "peer-a",
				equals: () => false,
			} as any,
			{ skipCheck: true },
		);

		expect(send.callCount).to.equal(3);
		const sentCoordinates = send.getCalls().map((call) => {
			const message = call.args[0];
			expect(call.args[1].priority).to.equal(SYNC_MESSAGE_PRIORITY);
			expect(message).to.be.instanceOf(RequestMaybeSyncCoordinate);
			return (message as RequestMaybeSyncCoordinate).hashNumbers;
		});
		expect(sentCoordinates.flat()).to.deep.equal([1n, 2n, 3n, 4n, 5n]);
		expect(sentCoordinates.map((x) => x.length)).to.deep.equal([2, 2, 1]);
	});

	it("uses native resolver for coordinate queue preflight", async () => {
		const send = sinon.stub().resolves();
		const count = sinon.stub().throws(new Error("entry index should not be used"));
		const resolveHashesForSymbols = sinon
			.stub()
			.returns(new Map([[42n, ["head-a"]]]));
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: { count } as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			resolveHashesForSymbols,
		});

		await sync.queueSync(
			[42n, 7n],
			{
				hashcode: () => "peer-a",
				equals: () => false,
			} as any,
		);

		expect(count.called).to.equal(false);
		expect(resolveHashesForSymbols.firstCall.args[0]).to.deep.equal([42n, 7n]);
		expect(send.callCount).to.equal(1);
		expect(send.firstCall.args[0]).to.be.instanceOf(RequestMaybeSyncCoordinate);
		expect(send.firstCall.args[0].hashNumbers).to.deep.equal([7n]);
	});

	it("uses native coordinate symbol resolver before index lookup", async () => {
		const send = sinon.stub().resolves();
		const iterate = sinon.stub().throws(new Error("entry index should not be used"));
		const rpc = { send } as any;
		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: { iterate } as any,
			log: {
				get: async (hash: string) => ({
					hash,
					size: 1,
					meta: { gid: `gid-${hash}` },
				}),
				entryIndex: { getUniqueReferenceGids: () => [] },
			} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			resolveHashesForSymbols: (symbols) => {
				expect(symbols).to.deep.equal([42n]);
				return new Map([[42n, ["head-a"]]]);
			},
		});

		await sync.onMessage(
			new RequestMaybeSyncCoordinate({ hashNumbers: [42n] }),
			{ from: peerA } as any,
		);

		expect(iterate.called).to.equal(false);
		expect(send.callCount).to.equal(1);
		expect(send.firstCall.args[0].heads.map((x: any) => x.entry.hash)).to.deep.equal([
			"head-a",
		]);
	});

	it("uses native flat coordinate symbol resolver for response lookup", async () => {
		const send = sinon.stub().resolves();
		const iterate = sinon.stub().throws(new Error("entry index should not be used"));
		const resolveHashesForSymbols = sinon
			.stub()
			.throws(new Error("map resolver should not be used"));
		const resolveHashListForSymbols = sinon.stub().returns(["head-a"]);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: { iterate } as any,
			log: {
				get: async (hash: string) => ({
					hash,
					size: 1,
					meta: { gid: `gid-${hash}` },
				}),
				entryIndex: { getUniqueReferenceGids: () => [] },
			} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			resolveHashesForSymbols,
			resolveHashListForSymbols,
		});

		await sync.onMessage(
			new RequestMaybeSyncCoordinate({ hashNumbers: [42n] }),
			{ from: peerA } as any,
		);

		expect(iterate.called).to.equal(false);
		expect(resolveHashListForSymbols.calledOnce).to.equal(true);
		expect(resolveHashListForSymbols.firstCall.args[0]).to.deep.equal([42n]);
		expect(resolveHashesForSymbols.called).to.equal(false);
		expect(send.callCount).to.equal(1);
		expect(send.firstCall.args[0].heads.map((x: any) => x.entry.hash)).to.deep.equal([
			"head-a",
		]);
	});

	it("splits mixed hash and coordinate maybe-sync batches by type", async () => {
		const send = sinon.stub().resolves();
		const rpc = { send } as any;
		const sync = new SimpleSyncronizer<"u64">({
			rpc,
			entryIndex: { count: async () => 0 } as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				maxSimpleHashesPerMessage: 8,
				maxSimpleCoordinatesPerMessage: 8,
			},
		});

		await (sync as any).requestSync(["h1", 2n, "h2", 4n], ["peer-a"]);

		expect(send.callCount).to.equal(2);

		const sentHashMessages = send
			.getCalls()
			.filter((call) => {
				expect(call.args[1].priority).to.equal(SYNC_MESSAGE_PRIORITY);
				return true;
			})
			.map((call) => call.args[0])
			.filter((message) => message instanceof ResponseMaybeSync);
		expect(sentHashMessages).to.have.length(1);
		expect((sentHashMessages[0] as ResponseMaybeSync).hashes).to.deep.equal([
			"h1",
			"h2",
		]);

		const sentCoordinateMessages = send
			.getCalls()
			.map((call) => call.args[0])
			.filter((message) => message instanceof RequestMaybeSyncCoordinate);
		expect(sentCoordinateMessages).to.have.length(1);
		expect(
			(sentCoordinateMessages[0] as RequestMaybeSyncCoordinate).hashNumbers,
		).to.deep.equal([2n, 4n]);
	});

	it("advertises raw exchange-head support when enabled", async () => {
		const send = sinon.stub().resolves();
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: { count: async () => 0 } as any,
			log: { has: async () => false } as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
			sync: {
				rawExchangeHeads: true,
				maxSimpleHashesPerMessage: 8,
				maxSimpleCoordinatesPerMessage: 8,
			},
		});

		await (sync as any).requestSync(["h1", 2n], ["peer-a"]);

		const messages = send.getCalls().map((call) => call.args[0]);
		const hashMessage = messages.find(
			(message) => message instanceof ResponseMaybeSyncCapabilities,
		) as ResponseMaybeSyncCapabilities | undefined;
		const coordinateMessage = messages.find(
			(message) => message instanceof RequestMaybeSyncCoordinateCapabilities,
		) as RequestMaybeSyncCoordinateCapabilities | undefined;
		expect(hashMessage?.hashes).to.deep.equal(["h1"]);
		expect(coordinateMessage?.hashNumbers).to.deep.equal([2n]);
	});

	it("responds with raw exchange heads only to capable requests", async () => {
		const send = sinon.stub().resolves();
		const get = sinon.stub().throws(new Error("full entry get should not be used"));
		const getMany = sinon.stub().resolves([new Uint8Array([1, 2, 3])]);
		const sync = new SimpleSyncronizer<"u64">({
			rpc: { send } as any,
			entryIndex: {} as any,
			log: {
				get,
				blocks: { getMany },
				entryIndex: {
					getUniqueReferenceGidRowsFlatBatch: sinon.stub().returns([]),
				},
			} as any,
			coordinateToHash: new Cache<string>({ max: 10 }),
		});

		await sync.onMessage(
			new ResponseMaybeSyncCapabilities({ hashes: ["head-a"] }),
			{ from: peerA } as any,
		);

		expect(send.callCount).to.equal(1);
		expect(send.firstCall.args[0]).to.be.instanceOf(RawExchangeHeadsMessage);
		expect(send.firstCall.args[0].heads.map((head: any) => head.hash)).to.deep.equal(
			["head-a"],
		);
		expect(get.called).to.equal(false);
		expect(getMany.calledOnceWithExactly(["head-a"])).to.equal(true);
	});
});
