import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	BlockMessage,
	BlockRequest,
	BlockResponse,
	DirectBlock,
	type DirectBlockComponents,
} from "@peerbit/blocks";
import { TestSession } from "@peerbit/libp2p-test-utils";
import {
	NativeLogBlockStore,
	createNativeLogBlockStore,
} from "@peerbit/log-rust";
import {
	Routes,
	type RustBlockExchange,
	type RustCoreStream,
	waitForNeighbour,
} from "@peerbit/stream";
import { expect } from "chai";
import sinon from "sinon";
import { createRustCoreStream } from "../src/index.js";

type Session = TestSession<{ blocks: DirectBlock }>;

const store = (session: Session, index: number) =>
	session.peers[index].services.blocks;

/**
 * Wrap the native log block store in the `AnyStore` shape DirectBlock's
 * `localStore` option expects, instrumented to prove the serve path: `gets`
 * records every JS materialization of stored bytes, `payloadCalls` every
 * wasm-serialized response.
 */
const instrumentedNativeStore = (native: NativeLogBlockStore) => {
	const gets: string[] = [];
	let payloadCalls = 0;
	const wrapped = {
		status: () => native.status(),
		open: () => native.open(),
		close: () => native.close(),
		get: (key: string) => {
			gets.push(key);
			return native.get(key);
		},
		put: (key: string, value: Uint8Array): void => {
			void native.put(key, value);
		},
		del: (key: string) => native.del(key),
		sublevel: () => native.sublevel(),
		iterator: () => native.iterator(),
		clear: () => native.clear(),
		size: () => native.size(),
		persisted: () => native.persisted(),
		getBlockResponsePayload: (cid: string) => {
			payloadCalls += 1;
			return native.getBlockResponsePayload(cid);
		},
	};
	return { wrapped, gets, payloadCalls: () => payloadCalls };
};

describe("direct-block rust-core", () => {
	let core: RustCoreStream;
	let exchange: RustBlockExchange;

	before(async () => {
		core = await createRustCoreStream();
		exchange = core.blockExchange!;
	});

	describe("codec parity", () => {
		const cids = [
			"zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J",
			"",
			"åäö-块",
		];

		it("encodes block requests byte-identically to borsh", () => {
			for (const cid of cids) {
				expect(exchange.encodeBlockRequest(cid)).to.deep.equal(
					serialize(new BlockRequest(cid)),
				);
			}
		});

		it("encodes block responses byte-identically to borsh", () => {
			for (const cid of cids) {
				for (const bytes of [new Uint8Array(), new Uint8Array([5, 4, 3])]) {
					expect(exchange.encodeBlockResponse(cid, bytes)).to.deep.equal(
						serialize(new BlockResponse(cid, bytes)),
					);
				}
			}
		});

		it("decodes borsh-serialized block messages", () => {
			const request = exchange.decodeBlockMessage(
				serialize(new BlockRequest("cid-a")),
			);
			expect(request).to.deep.equal({ type: "request", cid: "cid-a" });

			const payload = new Uint8Array([9, 8, 7, 6]);
			const response = exchange.decodeBlockMessage(
				serialize(new BlockResponse("cid-b", payload)),
			);
			expect(response.type).to.equal("response");
			expect(response.cid).to.equal("cid-b");
			expect(
				new Uint8Array((response as { bytes: Uint8Array }).bytes),
			).to.deep.equal(payload);

			// and the TS decoder accepts the native encoding
			const tsDecoded = deserialize(
				exchange.encodeBlockResponse("cid-b", payload),
				BlockMessage,
			) as BlockResponse;
			expect(tsDecoded.cid).to.equal("cid-b");
			expect(new Uint8Array(tsDecoded.bytes)).to.deep.equal(payload);
		});

		it("rejects malformed frames like the TS decoder", () => {
			const malformed = [
				new Uint8Array(),
				new Uint8Array([2, 0, 0, 0, 0]),
				serialize(new BlockRequest("cid")).slice(0, 3),
				new Uint8Array([...serialize(new BlockRequest("cid")), 0]),
			];
			for (const frame of malformed) {
				expect(() => deserialize(frame, BlockMessage)).to.throw();
				expect(() => exchange.decodeBlockMessage(frame)).to.throw();
			}
		});
	});

	describe("eager-cache contract", () => {
		it("enforces byte/entry/ttl bounds with exact delete accounting", () => {
			const clock = sinon.useFakeTimers({ now: 1_000 });
			try {
				const cache = exchange.createEagerCache({
					maxEntries: 2,
					maxBytes: 6,
					ttlMs: 100,
				});
				expect(cache.add("a", new Uint8Array(4))).to.equal(true);
				expect(cache.add("b", new Uint8Array(3))).to.equal(true);
				expect(cache.get("a")).to.equal(undefined);
				expect(cache.stats()).to.include({
					entries: 1,
					bytes: 3,
					evictions: 1,
				});

				cache.del("b");
				expect(cache.stats()).to.include({ entries: 0, bytes: 0 });
				expect(cache.add("b", new Uint8Array(5))).to.equal(true);
				cache.del("b");
				expect(cache.add("c", new Uint8Array(6))).to.equal(true);
				expect(cache.add("oversized", new Uint8Array(7))).to.equal(false);
				expect(cache.stats()).to.include({ entries: 1, bytes: 6 });

				clock.tick(100);
				expect(cache.stats()).to.include({
					entries: 0,
					bytes: 0,
					expirations: 1,
				});
			} finally {
				clock.restore();
			}
		});

		it("copies aliased views and entry-bounds zero-byte replacements", () => {
			const cache = exchange.createEagerCache({
				maxEntries: 2,
				maxBytes: 2,
				ttlMs: 10_000,
			});
			const backing = new Uint8Array(1024);
			backing.set([1, 2], 100);
			expect(cache.add("aliased", backing.subarray(100, 102))).to.equal(true);
			const retained = cache.get("aliased")!;
			expect(retained).to.deep.equal(new Uint8Array([1, 2]));
			expect(retained.buffer.byteLength).to.equal(2);

			expect(cache.add("zero", new Uint8Array())).to.equal(true);
			expect(cache.add("zero", new Uint8Array())).to.equal(true);
			expect(cache.stats()).to.include({ entries: 2, bytes: 2 });
			expect(cache.add("zero-2", new Uint8Array())).to.equal(true);
			expect(cache.stats()).to.include({ entries: 2, bytes: 0 });
			expect(cache.get("aliased")).to.equal(undefined);
			cache.clear();

			expect(() =>
				exchange.createEagerCache({
					maxEntries: 1,
					maxBytes: 1,
					ttlMs: 0x8000_0000,
				}),
			).to.throw(RangeError);
		});

		it("copies length-tracking views over resizable buffers", function () {
			type ResizableBuffer = ArrayBuffer & {
				readonly resizable: boolean;
				resize(byteLength: number): void;
			};
			let backing: ResizableBuffer;
			try {
				const Constructor = ArrayBuffer as unknown as new (
					byteLength: number,
					options: { maxByteLength: number },
				) => ResizableBuffer;
				backing = new Constructor(2, { maxByteLength: 8 });
			} catch {
				this.skip();
				return;
			}
			if (backing.resizable !== true || typeof backing.resize !== "function") {
				this.skip();
				return;
			}

			const source = new Uint8Array(backing);
			source.set([1, 2]);
			const cache = exchange.createEagerCache({
				maxEntries: 1,
				maxBytes: 2,
				ttlMs: 10_000,
			});
			expect(cache.add("resizable", source)).to.equal(true);
			backing.resize(8);

			const retained = cache.get("resizable")!;
			expect(retained).to.deep.equal(new Uint8Array([1, 2]));
			expect(retained.buffer.byteLength).to.equal(2);
			expect(cache.stats()).to.include({ entries: 1, bytes: 2 });
			cache.clear();
		});
	});

	describe("mixed topology", () => {
		let session: Session;

		afterEach(async () => {
			await session?.stop();
		});

		it("exchanges blocks between rust-core and default peers over a default relay", async () => {
			// rust-core peer — default relay — default peer
			session = await TestSession.disconnected(3, [
				{
					services: {
						blocks: (c: DirectBlockComponents) =>
							new DirectBlock(c, { rustCore: core }),
					},
				},
				{
					services: {
						blocks: (c: DirectBlockComponents) =>
							new DirectBlock(c, { rustCore: false }),
					},
				},
				{
					services: {
						blocks: (c: DirectBlockComponents) =>
							new DirectBlock(c, { rustCore: false }),
					},
				},
			]);
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);
			await waitForNeighbour(store(session, 0), store(session, 1));
			await waitForNeighbour(store(session, 1), store(session, 2));

			// prove the modes actually differ: the rust-core peer runs the native
			// routing table and the native provider cache
			expect(store(session, 0).routes).to.not.be.instanceOf(Routes);
			expect(store(session, 1).routes).to.be.instanceOf(Routes);
			expect((store(session, 0) as any).remoteBlocks._rustProviderCache).to
				.exist;
			expect((store(session, 2) as any).remoteBlocks._rustProviderCache).to.be
				.undefined;

			const data = new Uint8Array([5, 4, 3]);

			// default peer serves the rust-core requester across the relay
			const cid = await store(session, 2).put(data);
			const read = await store(session, 0).get(cid, {
				remote: {
					timeout: 5000,
					from: [store(session, 2).publicKeyHash],
				},
			});
			expect(new Uint8Array(read!)).to.deep.equal(data);

			// the response taught the native provider cache; re-read without `from`
			const reread = await store(session, 0).get(cid, {
				remote: { timeout: 5000 },
			});
			expect(new Uint8Array(reread!)).to.deep.equal(data);

			// rust-core peer serves a default requester across the relay
			const data2 = new Uint8Array([7, 7, 7, 7]);
			const cid2 = await store(session, 0).put(data2);
			const read2 = await store(session, 2).get(cid2, {
				remote: {
					timeout: 5000,
					from: [store(session, 0).publicKeyHash],
				},
			});
			expect(new Uint8Array(read2!)).to.deep.equal(data2);
		});
	});

	describe("native-store-served responses", () => {
		let session: Session;

		afterEach(async () => {
			await session?.stop();
		});

		it("serializes responses in wasm without surfacing block bytes to JS", async () => {
			const native = await createNativeLogBlockStore();
			const instrumented = instrumentedNativeStore(native);
			session = await TestSession.connected(2, [
				{
					services: {
						blocks: (c: DirectBlockComponents) =>
							new DirectBlock(c, {
								rustCore: core,
								localStore: instrumented.wrapped as any,
							}),
					},
				},
				{
					services: {
						blocks: (c: DirectBlockComponents) =>
							new DirectBlock(c, { rustCore: false }),
					},
				},
			]);
			await waitForNeighbour(store(session, 0), store(session, 1));

			const data = new Uint8Array([5, 4, 3]);
			const cid = await store(session, 0).put(data);
			expect(cid).to.equal("zb2rhbnwihVzMMEGAPf9EwTZBsQz9fszCnM4Y8mJmBFgiyN7J");

			// the wasm-serialized payload is the exact borsh BlockResponse
			expect(native.getBlockResponsePayload(cid)).to.deep.equal(
				serialize(new BlockResponse(cid, data)),
			);
			expect(native.getBlockResponsePayload("zb2unknown")).to.be.undefined;

			const read = await store(session, 1).get(cid, {
				remote: {
					timeout: 5000,
					from: [store(session, 0).publicKeyHash],
				},
			});
			expect(new Uint8Array(read!)).to.deep.equal(data);

			// the serve path produced the payload natively and never read the
			// block bytes into JS
			expect(instrumented.payloadCalls()).to.be.greaterThan(0);
			expect(instrumented.gets).to.deep.equal([]);
		});

		it("falls back to the JS path for blocks missing from the native store", async () => {
			const native = await createNativeLogBlockStore();
			const instrumented = instrumentedNativeStore(native);
			session = await TestSession.connected(2, [
				{
					services: {
						blocks: (c: DirectBlockComponents) =>
							new DirectBlock(c, {
								rustCore: core,
								localStore: instrumented.wrapped as any,
							}),
					},
				},
				{
					services: {
						blocks: (c: DirectBlockComponents) =>
							new DirectBlock(c, { rustCore: false }),
					},
				},
			]);
			await waitForNeighbour(store(session, 0), store(session, 1));

			const missing = await store(session, 1).get(
				"zb3we1BmfxpFg6bCXmrsuEo8JuQrGEf7RyFBdRxEHLuqc4CSr",
				{
					remote: {
						timeout: 1000,
						from: [store(session, 0).publicKeyHash],
					},
				},
			);
			expect(missing).to.be.undefined;
			// the native payload path was consulted but had nothing to serve,
			// so the regular localStore lookup ran
			expect(instrumented.payloadCalls()).to.be.greaterThan(0);
			expect(instrumented.gets.length).to.be.greaterThan(0);
		});
	});
});
