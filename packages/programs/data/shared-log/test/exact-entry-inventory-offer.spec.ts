import { serialize } from "@dao-xyz/borsh";
import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { type Entry, Log } from "@peerbit/log";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import {
	ExchangeHeadsMessage,
	createExactExchangeHeadsMessages,
} from "../src/exchange-heads.js";

describe("entry inventory exact entry offers", () => {
	let blocks: AnyBlockStore;
	let identity: Ed25519Keypair;
	let sandbox: sinon.SinonSandbox;
	let logs: Log<Uint8Array>[];
	let iterators: AsyncGenerator<ExchangeHeadsMessage<any>>[];
	let releases: (() => void)[];
	let pending: Promise<unknown>[];

	beforeEach(async () => {
		sandbox = sinon.createSandbox();
		logs = [];
		iterators = [];
		releases = [];
		pending = [];
		blocks = new AnyBlockStore();
		identity = await Ed25519Keypair.create();
		await blocks.start();
	});

	afterEach(async () => {
		for (const release of releases) release();
		await Promise.allSettled(pending);
		try {
			await Promise.all(
				iterators.map((iterator) => iterator.return(undefined)),
			);
		} finally {
			sandbox.restore();
			try {
				await Promise.all(logs.map((log) => log.close()));
			} finally {
				await blocks.stop();
			}
		}
	});

	const openLog = async (nativeGraph = false) => {
		const log = new Log<Uint8Array>();
		logs.push(log);
		await log.open(blocks, identity, {
			appendDurability: "strict",
			nativeGraph,
		});
		return log;
	};
	const offer = (
		log: Log<Uint8Array>,
		hashes: string[],
		current = () => true,
	) => {
		const iterator = createExactExchangeHeadsMessages(log, hashes, current);
		iterators.push(iterator);
		return iterator;
	};
	const independentEntries = async (
		log: Log<Uint8Array>,
		count: number,
		size = 1,
	) => {
		const entries: Entry<Uint8Array>[] = [];
		for (let index = 0; index < count; index++) {
			entries.push(
				(
					await log.append(new Uint8Array(size), {
						meta: { next: [], gidSeed: new Uint8Array([index]) },
					})
				).entry,
			);
		}
		return entries;
	};

	for (const nativeGraph of [false, true]) {
		it(`keeps the signed mixed-GID entry without traversing reference hints (${nativeGraph ? "native" : "JS"})`, async () => {
			const log = await openLog(nativeGraph);
			const parents = await independentEntries(log, 3);
			const { entry } = await log.append(new Uint8Array([9]), {
				meta: { next: parents },
			});
			expect(new Set(parents.map((parent) => parent.meta.gid)).size).to.equal(
				3,
			);
			const signedBytes = serialize(entry);
			const traversal = [
				sandbox
					.stub(log.entryIndex, "getShallow")
					.throws(new Error("unexpected parent traversal")),
				sandbox
					.stub(log.entryIndex, "getUniqueReferenceGids")
					.throws(new Error("unexpected native hints")),
				sandbox
					.stub(log.entryIndex, "getUniqueReferenceGidRowsBatch")
					.throws(new Error("unexpected native hint rows")),
				sandbox
					.stub(log.entryIndex, "getUniqueReferenceGidRowsFlatBatch")
					.throws(new Error("unexpected native flat hints")),
			];
			const get = sandbox.spy(log, "get");
			const iterator = offer(log, [entry.hash]);
			const result = await iterator.next();
			expect(result.done).to.equal(false);
			const message = result.value as ExchangeHeadsMessage<Uint8Array>;
			expect(message.heads).to.have.length(1);
			const offered = message.heads[0];
			expect(offered.entry.hash).to.equal(entry.hash);
			expect(serialize(offered.entry)).to.deep.equal(signedBytes);
			expect(offered.entry.meta.next).to.deep.equal(entry.meta.next);
			expect(await offered.entry.verifySignatures()).to.equal(true);
			expect(offered.gidRefrences).to.deep.equal([]);
			expect(get.calledOnceWithExactly(entry.hash, { remote: false })).to.equal(
				true,
			);
			for (const stub of traversal) expect(stub.notCalled).to.equal(true);
			expect((await iterator.next()).done).to.equal(true);
		});
	}

	it("retains at most the message batch and one locally resolved lookahead entry", async () => {
		const log = await openLog();
		const entries = await independentEntries(log, 5, 60_000);
		const get = sandbox.spy(log, "get");
		const iterator = offer(
			log,
			entries.map((entry) => entry.hash),
		);
		const received: string[] = [];
		for (let index = 0; index < entries.length; index++) {
			const result = await iterator.next();
			expect(result.done).to.equal(false);
			const message = result.value as ExchangeHeadsMessage<Uint8Array>;
			expect(message.heads).to.have.length(1);
			received.push(message.heads[0].entry.hash);
			expect(get.callCount).to.equal(Math.min(index + 2, entries.length));
		}
		expect(received).to.deep.equal(entries.map((entry) => entry.hash));
		expect((await iterator.next()).done).to.equal(true);
	});

	it("does not perform a local read when ownership is already stale", async () => {
		const log = await openLog();
		const [entry] = await independentEntries(log, 1);
		const get = sandbox.spy(log, "get");
		expect((await offer(log, [entry.hash], () => false).next()).done).to.equal(
			true,
		);
		expect(get.notCalled).to.equal(true);
	});

	it("drops an entry when ownership changes during its awaited local read", async () => {
		const log = await openLog();
		const [entry] = await independentEntries(log, 1);
		const blocked = pDefer<void>();
		releases.push(() => blocked.resolve());
		let current = true;
		const original = log.get.bind(log);
		const get = sandbox.stub(log, "get").callsFake(async (hash, options) => {
			await blocked.promise;
			return original(hash, options);
		});
		const result = offer(log, [entry.hash], () => current).next();
		void result.catch(() => {});
		pending.push(result);
		expect(get.calledOnce).to.equal(true);
		current = false;
		blocked.resolve();
		expect((await result).done).to.equal(true);
	});

	it("does not publish its lookahead or read again after ownership changes at a yield", async () => {
		const log = await openLog();
		const entries = await independentEntries(log, 3, 60_000);
		let current = true;
		const get = sandbox.spy(log, "get");
		const iterator = offer(
			log,
			entries.map((entry) => entry.hash),
			() => current,
		);
		expect((await iterator.next()).done).to.equal(false);
		expect(get.callCount).to.equal(2);
		current = false;
		expect((await iterator.next()).done).to.equal(true);
		expect(get.callCount).to.equal(2);
	});

	it("skips an absent local block without changing the requested order", async () => {
		const log = await openLog();
		const entries = await independentEntries(log, 2);
		const missing = await blocks.put(new Uint8Array([91, 92]));
		await blocks.rm(missing);
		const get = sandbox.spy(log, "get");
		const hashes = [entries[1].hash, missing, entries[0].hash];
		const iterator = offer(log, hashes);
		const result = await iterator.next();
		const message = result.value as ExchangeHeadsMessage<Uint8Array>;
		expect(message.heads.map((head) => head.entry.hash)).to.deep.equal([
			entries[1].hash,
			entries[0].hash,
		]);
		expect(get.getCalls().map((call) => call.args)).to.deep.equal(
			hashes.map((hash) => [hash, { remote: false }]),
		);
		expect((await iterator.next()).done).to.equal(true);
	});

	it("leaves normal receiver authorization in control of the signed entry", async () => {
		const source = await openLog();
		const [entry] = await independentEntries(source, 1);
		const result = await offer(source, [entry.hash]).next();
		const message = result.value as ExchangeHeadsMessage<Uint8Array>;
		const failure = new Error("receiver authorization denied");
		const canAppend = sandbox.stub().rejects(failure);
		const destination = new Log<Uint8Array>();
		logs.push(destination);
		await destination.open(blocks, identity, { nativeGraph: false, canAppend });
		const error = await destination
			.join(message.heads.map((head) => head.entry))
			.then(
				() => undefined,
				(error: unknown) => error,
			);
		expect(error).to.equal(failure);
		expect(canAppend.called).to.equal(true);
		expect(await destination.has(entry.hash)).to.equal(false);
	});
});
