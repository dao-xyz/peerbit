import { TestSession } from "@peerbit/test-utils";
import { AbortError, TimeoutError } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { Documents } from "../src/index.js";
import { Document, TestStore } from "./data.js";

describe("public iterator cover readiness", function () {
	this.timeout(30_000);
	let session: TestSession;
	let store: TestStore;
	let clock: sinon.SinonFakeTimers;
	let getCover: sinon.SinonStub;
	let query: sinon.SinonStub;
	let iterators: { close(): Promise<void> }[];
	let releases: (() => void)[];
	let pending: Promise<unknown>[];

	before(async () => {
		session = await TestSession.connected(1);
	});

	beforeEach(async () => {
		iterators = [];
		releases = [];
		pending = [];
		store = await session.peers[0].open(
			new TestStore({ docs: new Documents<Document>() }),
			{ args: { replicate: false, timeUntilRoleMaturity: 0 } },
		);
		// Only cover snapshots and the first-query boundary are controlled. The
		// public iterator and its lifecycle are real; no transport claim is made.
		getCover = sinon.stub(store.docs.log, "getCover").resolves([]);
		query = sinon.stub(store.docs.index as any, "queryCommence").resolves([]);
		clock = sinon.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
	});

	afterEach(async () => {
		try {
			for (const release of releases) release();
			await Promise.all(iterators.map((iterator) => iterator.close()));
			await clock.tickAsync(100);
			await Promise.all(pending);
			// Give unowned warmup rejections a real event-loop turn. Do not attach
			// handlers to internal promises or suppress the strict runner's errors.
			await new Promise<void>((resolve) => setImmediate(resolve));
		} finally {
			clock?.restore();
			sinon.restore();
			await store?.close();
		}
	});

	after(async () => {
		await session?.stop();
	});

	const gate = () => {
		const deferred = pDefer<string[]>();
		releases.push(() => deferred.resolve([]));
		return deferred;
	};
	const iterate = () => {
		const iterator = store.docs.index.iterate(
			{},
			{
				local: false,
				remote: {
					wait: { behavior: "block", timeout: 100, onTimeout: "error" },
				},
			},
		);
		iterators.push(iterator);
		return iterator;
	};
	const observe = <T>(operation: Promise<T>) => {
		const outcome = operation.then(
			(value) => ({ value, error: undefined }),
			(error: unknown) => ({ value: undefined, error }),
		);
		pending.push(outcome);
		return outcome;
	};

	it("starts the first query after an event received during its initial cover read", async () => {
		const held = gate();
		getCover.onFirstCall().returns(held.promise);
		getCover.onSecondCall().resolves(["remote"]);
		const iterator = iterate();
		const first = observe(iterator.next(1));
		expect(getCover.callCount).to.equal(1);
		expect(query.called).to.equal(false);
		store.docs.log.events.dispatchEvent(
			new CustomEvent("replicator:join", {
				detail: { publicKey: session.peers[0].identity.publicKey },
			}),
		);
		held.resolve([]);
		await clock.tickAsync(0);
		expect(getCover.callCount).to.equal(2);
		expect(query.callCount).to.equal(1);
		expect(await first).to.deep.equal({ value: [], error: undefined });
	});

	it("can close before next without an unhandled eager warmup rejection", async () => {
		const iterator = iterate();
		await clock.tickAsync(0);
		await iterator.close();
		await new Promise<void>((resolve) => setImmediate(resolve));
		expect(iterator.done()).to.equal(true);
		expect(query.called).to.equal(false);
	});

	it("retains a strict timeout for a delayed first next without an unhandled rejection", async () => {
		const iterator = iterate();
		await clock.tickAsync(100);
		await new Promise<void>((resolve) => setImmediate(resolve));
		expect(query.called).to.equal(false);
		const result = await observe(iterator.next(1));
		expect(result.error).to.be.instanceOf(TimeoutError);
		expect(query.called).to.equal(false);
	});

	it("cancels a pending first next while its initial cover read is held", async () => {
		const held = gate();
		getCover.returns(held.promise);
		const iterator = iterate();
		let settled = false;
		const first = observe(iterator.next(1)).then((result) => {
			settled = true;
			return result;
		});
		await iterator.close();
		await clock.tickAsync(0);
		expect(settled).to.equal(true);
		expect((await first).error).to.be.instanceOf(AbortError);
		expect(query.called).to.equal(false);
		held.resolve(["remote"]);
		await clock.tickAsync(0);
		expect(query.called).to.equal(false);
		expect(iterator.done()).to.equal(true);
	});
});
