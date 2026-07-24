import { deserialize, serialize } from "@dao-xyz/borsh";
import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { Log } from "@peerbit/log";
import { expect } from "chai";
import sinon from "sinon";
import {
	CheckedPruneRequest,
	MAX_RAW_EXCHANGE_MESSAGE_SIZE,
	RawExchangeHeadsMessage,
	RequestIPruneV2,
	ResponseIPruneV2,
	createExchangeHeadsMessages,
	createRawExchangeHeadsMessages,
	materializeRawExchangeHeadsMessage,
} from "../src/exchange-heads.js";
import { TransportMessage } from "../src/message.js";

const LARGE_HEAD_PAYLOAD_BYTES = MAX_RAW_EXCHANGE_MESSAGE_SIZE + 1;
const PAYLOAD_RESOLVE_BATCH_SIZE = 16;

const appendLargeIndependentHeads = async (
	log: Log<Uint8Array>,
	count: number,
) => {
	const heads: string[] = [];
	for (let i = 0; i < count; i++) {
		const { entry } = await log.append(
			new Uint8Array(LARGE_HEAD_PAYLOAD_BYTES),
			{
				meta: { next: [], gidSeed: new Uint8Array([i]) },
			},
		);
		heads.push(entry.hash);
	}
	return heads;
};

const createMissingBlockHash = async (log: Log<Uint8Array>) => {
	const hash = await log.blocks.put(new Uint8Array([0xde, 0xad, 0xbe, 0xef]));
	await log.blocks.rm(hash);
	return hash;
};

describe("exchange heads", () => {
	let store: AnyBlockStore;
	let signKey: Ed25519Keypair;

	before(async () => {
		store = new AnyBlockStore();
		signKey = await Ed25519Keypair.create();
		await store.start();
	});

	after(async () => {
		await store.stop();
	});

	it("keeps the checked-prune correlation wire format stable", () => {
		const requestId = Uint8Array.from({ length: 32 }, (_, index) => index);
		const requestIdHex =
			"000102030405060708090a0b0c0d0e0f" + "101112131415161718191a1b1c1d1e1f";
		for (const [message, variant] of [
			[
				new RequestIPruneV2({
					requests: [{ hash: "h", requestId }],
				}),
				"0b",
			],
			[
				new ResponseIPruneV2({
					requests: [{ hash: "h", requestId }],
				}),
				"0c",
			],
		] as const) {
			const bytes = serialize(message);
			expect(
				Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(
					"",
				),
			).to.equal(`0000${variant}01000000000100000068${requestIdHex}`);
			const roundTrip = deserialize(bytes, TransportMessage) as
				| RequestIPruneV2
				| ResponseIPruneV2;
			expect(roundTrip).to.be.instanceOf(message.constructor);
			expect(roundTrip.requests).to.have.length(1);
			expect(roundTrip.requests[0]).to.be.instanceOf(CheckedPruneRequest);
			expect(roundTrip.requests[0]!.hash).to.equal("h");
			expect([...roundTrip.requests[0]!.requestId]).to.deep.equal([
				...requestId,
			]);
		}
	});

	it("rejects checked-prune correlation ids that are not 32 bytes", () => {
		for (const create of [
			(requestId: Uint8Array) =>
				new RequestIPruneV2({ requests: [{ hash: "h", requestId }] }),
			(requestId: Uint8Array) =>
				new ResponseIPruneV2({ requests: [{ hash: "h", requestId }] }),
		]) {
			for (const length of [0, 31, 33]) {
				expect(() => serialize(create(new Uint8Array(length)))).to.throw(
					/Expected: 32/,
				);
			}
		}
	});

	it("uses native graph reference gids for single-head messages", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			nativeGraph: true,
		});

		const { entry: left } = await log.append(new Uint8Array([1]), {
			meta: { next: [], gidSeed: new Uint8Array([1]) },
		});
		const { entry: right } = await log.append(new Uint8Array([2]), {
			meta: { next: [], gidSeed: new Uint8Array([2]) },
		});
		const { entry: head } = await log.append(new Uint8Array([3]), {
			meta: { next: [left, right] },
		});
		const expectedReferenceGids = [left, right]
			.filter((entry) => entry.meta.gid !== head.meta.gid)
			.map((entry) => entry.meta.gid);

		const nativeGraph = log.entryIndex.properties.nativeGraph!.graph;
		const uniqueReferenceGidsSpy = sinon.spy(
			nativeGraph,
			"uniqueReferenceGids",
		);
		const getShallowSpy = sinon.spy(log.entryIndex, "getShallow");
		try {
			const messages = [];
			for await (const message of createExchangeHeadsMessages(log, [head])) {
				messages.push(message);
			}

			expect(messages).to.have.length(1);
			expect(messages[0]!.heads).to.have.length(1);
			expect(messages[0]!.heads[0]!.entry.hash).equal(head.hash);
			expect(messages[0]!.heads[0]!.gidRefrences).to.deep.equal(
				expectedReferenceGids,
			);
			expect(uniqueReferenceGidsSpy.calledOnceWithExactly(head.hash)).to.be
				.true;
			expect(getShallowSpy.callCount).equal(0);
		} finally {
			getShallowSpy.restore();
			uniqueReferenceGidsSpy.restore();
			await log.close();
		}
	});

	it("does not materialize fallback reference payloads while a message is suspended", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			nativeGraph: false,
		});
		expect(log.entryIndex.properties.nativeGraph).to.equal(undefined);

		const roots = [];
		for (const seed of [1, 2, 3]) {
			const { entry } = await log.append(new Uint8Array(128 * 1024), {
				meta: { next: [], gidSeed: new Uint8Array([seed]) },
			});
			roots.push(entry);
		}
		roots.sort((left, right) => left.meta.gid.localeCompare(right.meta.gid));
		const headGroup = roots[0]!;
		const branchGroup = roots[1]!;
		const deepGroup = roots[2]!;
		const { entry: branch } = await log.append(new Uint8Array(128 * 1024), {
			meta: { next: [branchGroup, deepGroup] },
		});
		const { entry: head } = await log.append(new Uint8Array(128 * 1024), {
			meta: { next: [branch, headGroup] },
		});
		const expectedReferenceGids = [branch, deepGroup]
			.filter((entry) => entry.meta.gid !== head.meta.gid)
			.map((entry) => entry.meta.gid);
		expect(expectedReferenceGids).to.not.be.empty;

		const getSpy = sinon.spy(log.entryIndex, "get");
		const getManySpy = sinon.spy(log.entryIndex, "getMany");
		const generator = createExchangeHeadsMessages(log, [head]);
		try {
			const first = await generator.next();

			expect(first.done).to.equal(false);
			expect(first.value!.heads).to.have.length(1);
			expect(first.value!.heads[0]!.entry.hash).to.equal(head.hash);
			expect(first.value!.heads[0]!.gidRefrences).to.deep.equal(
				expectedReferenceGids,
			);
			expect(getSpy.callCount).to.equal(0);
			expect(getManySpy.callCount).to.equal(0);
		} finally {
			await generator.return(undefined);
			getManySpy.restore();
			getSpy.restore();
			await log.close();
		}
	});

	it("resolves multiple hash heads in one entry-index batch", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			nativeGraph: true,
		});

		const { entry: left } = await log.append(new Uint8Array([1]), {
			meta: { next: [], gidSeed: new Uint8Array([1]) },
		});
		const { entry: right } = await log.append(new Uint8Array([2]), {
			meta: { next: [], gidSeed: new Uint8Array([2]) },
		});

		const getManySpy = sinon.spy(log.entryIndex, "getMany");
		const getSpy = sinon.spy(log, "get");
		try {
			const messages = [];
			for await (const message of createExchangeHeadsMessages(log, [
				left.hash,
				right.hash,
			])) {
				messages.push(message);
			}

			expect(messages).to.have.length(1);
			expect(messages[0]!.heads.map((head) => head.entry.hash)).to.deep.equal([
				left.hash,
				right.hash,
			]);
			expect(getManySpy.calledOnce).to.equal(true);
			expect(getManySpy.firstCall.args[0]).to.deep.equal([
				left.hash,
				right.hash,
			]);
			expect(getSpy.callCount).to.equal(0);
		} finally {
			getSpy.restore();
			getManySpy.restore();
			await log.close();
		}
	});

	it("deduplicates hash heads before full entry resolution", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			nativeGraph: true,
		});

		const { entry: left } = await log.append(new Uint8Array([1]), {
			meta: { next: [], gidSeed: new Uint8Array([1]) },
		});
		const { entry: right } = await log.append(new Uint8Array([2]), {
			meta: { next: [], gidSeed: new Uint8Array([2]) },
		});

		const getManySpy = sinon.spy(log.entryIndex, "getMany");
		try {
			const messages = [];
			for await (const message of createExchangeHeadsMessages(log, [
				left.hash,
				left.hash,
				right.hash,
			])) {
				messages.push(message);
			}

			expect(messages).to.have.length(1);
			expect(messages[0]!.heads.map((head) => head.entry.hash)).to.deep.equal([
				left.hash,
				right.hash,
			]);
			expect(getManySpy.calledOnce).to.equal(true);
			expect(getManySpy.firstCall.args[0]).to.deep.equal([
				left.hash,
				right.hash,
			]);
		} finally {
			getManySpy.restore();
			await log.close();
		}
	});

	it("creates raw hash heads without full entry resolution", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			nativeGraph: true,
		});

		const { entry: left } = await log.append(new Uint8Array([1]), {
			meta: { next: [], gidSeed: new Uint8Array([1]) },
		});
		const { entry: right } = await log.append(new Uint8Array([2]), {
			meta: { next: [], gidSeed: new Uint8Array([2]) },
		});

		const getManySpy = sinon.spy(log.entryIndex, "getMany");
		const getSpy = sinon.spy(log, "get");
		try {
			const messages = [];
			for await (const message of createRawExchangeHeadsMessages(log, [
				left.hash,
				right.hash,
			])) {
				messages.push(message);
			}

			expect(messages).to.have.length(1);
			expect(messages[0]).to.be.instanceOf(RawExchangeHeadsMessage);
			const raw = messages[0] as RawExchangeHeadsMessage;
			expect(raw.heads.map((head) => head.hash)).to.deep.equal([
				left.hash,
				right.hash,
			]);
			expect(raw.heads.every((head) => head.bytes.byteLength > 0)).to.equal(
				true,
			);
			expect(getManySpy.callCount).to.equal(0);
			expect(getSpy.callCount).to.equal(0);

			const roundTrip = deserialize(serialize(raw), TransportMessage);
			expect(roundTrip).to.be.instanceOf(RawExchangeHeadsMessage);

			const materialized = materializeRawExchangeHeadsMessage(
				roundTrip as RawExchangeHeadsMessage,
				log,
			);
			expect(materialized.heads.map((head) => head.entry.hash)).to.deep.equal([
				left.hash,
				right.hash,
			]);
		} finally {
			getSpy.restore();
			getManySpy.restore();
			await log.close();
		}
	});

	it("bounds full-entry lookahead while a large response is suspended", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			nativeGraph: true,
		});

		const heads = await appendLargeIndependentHeads(log, 18);
		const missing = await createMissingBlockHash(log);
		const requested = [...heads.slice(0, 17), missing, heads[17]!];
		const getManySpy = sinon.spy(log.entryIndex, "getMany");
		const generator = createExchangeHeadsMessages(log, requested);
		try {
			const first = await generator.next();
			expect(first.done).to.equal(false);
			expect(first.value!.heads.map((head) => head.entry.hash)).to.deep.equal([
				heads[0],
			]);
			expect(getManySpy.callCount).to.equal(1);
			expect(getManySpy.firstCall.args[0]).to.have.length(
				PAYLOAD_RESOLVE_BATCH_SIZE,
			);

			const received = first.value!.heads.map((head) => head.entry.hash);
			for await (const message of generator) {
				received.push(...message.heads.map((head) => head.entry.hash));
			}

			expect(received).to.deep.equal(heads);
			expect(getManySpy.callCount).to.equal(2);
			expect(getManySpy.secondCall.args[0]).to.deep.equal(requested.slice(16));
			expect(
				getManySpy
					.getCalls()
					.every((call) => call.args[0].length <= PAYLOAD_RESOLVE_BATCH_SIZE),
			).to.equal(true);
		} finally {
			await generator.return(undefined);
			getManySpy.restore();
			await log.close();
		}
	});

	it("bounds raw-block lookahead while a large response is suspended", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			nativeGraph: true,
		});

		const heads = await appendLargeIndependentHeads(log, 18);
		const getManySpy = sinon.spy(log.blocks, "getMany");
		const generator = createRawExchangeHeadsMessages(log, heads);
		try {
			const first = await generator.next();
			expect(first.done).to.equal(false);
			expect(first.value).to.be.instanceOf(RawExchangeHeadsMessage);
			expect(
				(first.value as RawExchangeHeadsMessage).heads.map((head) => head.hash),
			).to.deep.equal(heads.slice(0, 1));
			expect(getManySpy.callCount).to.equal(1);
			expect(getManySpy.firstCall.args[0]).to.have.length(
				PAYLOAD_RESOLVE_BATCH_SIZE,
			);

			const received = (first.value as RawExchangeHeadsMessage).heads.map(
				(head) => head.hash,
			);
			for await (const message of generator) {
				received.push(
					...(message instanceof RawExchangeHeadsMessage
						? message.heads.map((head) => head.hash)
						: message.heads.map((head) => head.entry.hash)),
				);
			}

			expect(received).to.deep.equal(heads);
			expect(getManySpy.callCount).to.equal(2);
			expect(getManySpy.secondCall.args[0]).to.deep.equal(heads.slice(16));
			expect(
				getManySpy
					.getCalls()
					.every((call) => call.args[0].length <= PAYLOAD_RESOLVE_BATCH_SIZE),
			).to.equal(true);
		} finally {
			await generator.return(undefined);
			getManySpy.restore();
			await log.close();
		}
	});

	it("preserves order when a later raw batch falls back", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			nativeGraph: true,
		});

		const heads: string[] = [];
		for (let i = 0; i < 17; i++) {
			const { entry } = await log.append(new Uint8Array([i]), {
				meta: { next: [], gidSeed: new Uint8Array([i]) },
			});
			heads.push(entry.hash);
		}
		await log.blocks.rm(heads[16]!);

		try {
			const messages = [];
			const profileEvents: Array<{
				name: string;
				entries?: number;
				messages?: number;
			}> = [];
			for await (const message of createRawExchangeHeadsMessages(
				log,
				heads,
				(event) => profileEvents.push(event),
			)) {
				messages.push(message);
			}
			expect(messages).to.have.length(2);
			expect(messages[0]).to.be.instanceOf(RawExchangeHeadsMessage);
			expect(messages[1]).to.not.be.instanceOf(RawExchangeHeadsMessage);
			expect(
				messages.flatMap((message) =>
					message instanceof RawExchangeHeadsMessage
						? message.heads.map((head) => head.hash)
						: message.heads.map((head) => head.entry.hash),
				),
			).to.deep.equal(heads);
			const rawProfileEvents = profileEvents.filter(
				(event) => event.name === "sharedLog.rawSend.jsBlockBytes",
			);
			expect(rawProfileEvents).to.have.length(1);
			expect(rawProfileEvents[0]!.entries).to.equal(16);
			expect(rawProfileEvents[0]!.messages).to.equal(1);
		} finally {
			await log.close();
		}
	});

	it("does not repeat native reference gids in one-head fallback", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			nativeGraph: true,
		});

		const roots = [];
		for (const seed of [21, 22, 23]) {
			const { entry } = await log.append(new Uint8Array([seed]), {
				meta: { next: [], gidSeed: new Uint8Array([seed]) },
			});
			roots.push(entry);
		}
		roots.sort((left, right) =>
			left.meta.gid < right.meta.gid
				? -1
				: left.meta.gid > right.meta.gid
					? 1
					: 0,
		);
		const { entry: rawHead } = await log.append(new Uint8Array([24]), {
			meta: { next: [roots[0]!, roots[2]!] },
		});
		const { entry: fallbackHead } = await log.append(new Uint8Array([25]), {
			meta: { next: [roots[1]!, roots[2]!] },
		});
		const sharedReference = roots[2]!;
		const firstBatch = [rawHead.hash];
		for (let i = 0; i < 15; i++) {
			const { entry } = await log.append(new Uint8Array([i + 30]), {
				meta: { next: [], gidSeed: new Uint8Array([i + 30]) },
			});
			firstBatch.push(entry.hash);
		}
		expect((await log.get(fallbackHead.hash))?.hash).to.equal(
			fallbackHead.hash,
		);
		await log.blocks.rm(fallbackHead.hash);

		try {
			const sent: Array<{ hash: string; gidRefrences: string[] }> = [];
			for await (const message of createRawExchangeHeadsMessages(log, [
				...firstBatch,
				fallbackHead.hash,
			])) {
				sent.push(
					...(message instanceof RawExchangeHeadsMessage
						? message.heads.map(({ hash, gidRefrences }) => ({
								hash,
								gidRefrences,
							}))
						: message.heads.map(({ entry, gidRefrences }) => ({
								hash: entry.hash,
								gidRefrences,
							}))),
				);
			}

			expect(sent.map(({ hash }) => hash)).to.deep.equal([
				...firstBatch,
				fallbackHead.hash,
			]);
			expect(
				sent
					.flatMap(({ gidRefrences }) => gidRefrences)
					.filter((gid) => gid === sharedReference.meta.gid),
			).to.have.length(1);
			expect(
				sent.find(({ hash }) => hash === rawHead.hash)!.gidRefrences,
			).to.include(sharedReference.meta.gid);
			expect(
				sent.find(({ hash }) => hash === fallbackHead.hash)!.gidRefrences,
			).to.not.include(sharedReference.meta.gid);
		} finally {
			await log.close();
		}
	});

	it("deduplicates heads and references across raw fallback batches", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			nativeGraph: false,
		});

		const { entry: left } = await log.append(new Uint8Array([1]), {
			meta: { next: [], gidSeed: new Uint8Array([1]) },
		});
		const { entry: right } = await log.append(new Uint8Array([2]), {
			meta: { next: [], gidSeed: new Uint8Array([2]) },
		});
		const { entry: head } = await log.append(new Uint8Array([3]), {
			meta: { next: [left, right] },
		});
		const reference = [left, right].find(
			(entry) => entry.meta.gid !== head.meta.gid,
		)!;
		const firstBatch = [head.hash];
		for (let i = 0; i < 15; i++) {
			const { entry } = await log.append(new Uint8Array([i + 4]), {
				meta: { next: [], gidSeed: new Uint8Array([i + 4]) },
			});
			firstBatch.push(entry.hash);
		}

		try {
			const sent: Array<{ hash: string; gidRefrences: string[] }> = [];
			for await (const message of createRawExchangeHeadsMessages(log, [
				...firstBatch,
				reference.hash,
				head.hash,
			])) {
				sent.push(
					...(message instanceof RawExchangeHeadsMessage
						? message.heads.map(({ hash, gidRefrences }) => ({
								hash,
								gidRefrences,
							}))
						: message.heads.map(({ entry, gidRefrences }) => ({
								hash: entry.hash,
								gidRefrences,
							}))),
				);
			}
			expect(sent.map(({ hash }) => hash)).to.deep.equal(firstBatch);
			expect(
				sent.find(({ hash }) => hash === head.hash)!.gidRefrences,
			).to.include(reference.meta.gid);
		} finally {
			await log.close();
		}
	});

	it("uses native graph reference gids for multi-head messages", async () => {
		const log = new Log<Uint8Array>();
		await log.open(store, signKey, {
			appendDurability: "strict",
			nativeGraph: true,
		});

		const { entry: leftRoot } = await log.append(new Uint8Array([1]), {
			meta: { next: [], gidSeed: new Uint8Array([1]) },
		});
		const { entry: leftSide } = await log.append(new Uint8Array([2]), {
			meta: { next: [], gidSeed: new Uint8Array([2]) },
		});
		const { entry: rightRoot } = await log.append(new Uint8Array([3]), {
			meta: { next: [], gidSeed: new Uint8Array([3]) },
		});
		const { entry: rightSide } = await log.append(new Uint8Array([4]), {
			meta: { next: [], gidSeed: new Uint8Array([4]) },
		});
		const { entry: leftHead } = await log.append(new Uint8Array([5]), {
			meta: { next: [leftRoot, leftSide] },
		});
		const { entry: rightHead } = await log.append(new Uint8Array([6]), {
			meta: { next: [rightRoot, rightSide] },
		});

		const nativeGraph = log.entryIndex.properties.nativeGraph!.graph;
		const expectedLeftReferences =
			nativeGraph.uniqueReferenceGids(leftHead.hash) ?? [];
		const expectedRightReferences =
			nativeGraph.uniqueReferenceGids(rightHead.hash) ?? [];
		expect(expectedLeftReferences).to.not.be.empty;
		expect(expectedRightReferences).to.not.be.empty;
		const uniqueReferenceGidRowsFlatBatchSpy = sinon.spy(
			nativeGraph,
			"uniqueReferenceGidRowsFlatBatch",
		);
		const getShallowSpy = sinon.spy(log.entryIndex, "getShallow");
		try {
			const messages = [];
			for await (const message of createExchangeHeadsMessages(log, [
				leftHead,
				rightHead,
			])) {
				messages.push(message);
			}

			expect(messages).to.have.length(1);
			expect(messages[0]!.heads).to.have.length(2);
			expect(messages[0]!.heads.map((head) => head.entry.hash)).to.deep.equal([
				leftHead.hash,
				rightHead.hash,
			]);
			expect(messages[0]!.heads[0]!.gidRefrences).to.deep.equal(
				expectedLeftReferences,
			);
			expect(messages[0]!.heads[1]!.gidRefrences).to.deep.equal(
				expectedRightReferences,
			);
			expect(
				uniqueReferenceGidRowsFlatBatchSpy.calledOnceWithExactly([
					leftHead.hash,
					rightHead.hash,
				]),
			).to.equal(true);
			expect(getShallowSpy.callCount).equal(0);
		} finally {
			getShallowSpy.restore();
			uniqueReferenceGidRowsFlatBatchSpy.restore();
			await log.close();
		}
	});
});
