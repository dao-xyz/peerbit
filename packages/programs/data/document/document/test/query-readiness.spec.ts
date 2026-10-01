import { AbortError, TimeoutError } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { DocumentIndex } from "../src/search.js";

describe("query cover readiness", () => {
	let clock: sinon.SinonFakeTimers;
	let events: EventTarget;
	let controller: AbortController;
	let getCover: sinon.SinonStub;
	let releases: (() => void)[];
	let pending: Promise<unknown>[];
	let added: sinon.SinonSpy;
	let removed: sinon.SinonSpy;
	let abortAdded: sinon.SinonSpy;
	let abortRemoved: sinon.SinonSpy;

	beforeEach(() => {
		clock = sinon.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
		events = new EventTarget();
		controller = new AbortController();
		getCover = sinon.stub().resolves(["self"]);
		releases = [];
		pending = [];
		added = sinon.spy(events, "addEventListener");
		removed = sinon.spy(events, "removeEventListener");
		abortAdded = sinon.spy(controller.signal, "addEventListener");
		abortRemoved = sinon.spy(controller.signal, "removeEventListener");
	});

	afterEach(async () => {
		try {
			for (const release of releases) release();
			controller.abort();
			await clock.tickAsync(1_000);
			await Promise.all(pending);
		} finally {
			sinon.restore();
			clock.restore();
		}
	});

	const gate = () => {
		const deferred = pDefer<string[]>();
		releases.push(() => deferred.resolve(["self"]));
		return deferred;
	};
	const start = (
		options: { onTimeout?: "error" | "proceed"; timeout?: number } = {
			onTimeout: "error",
		},
	) => {
		let result: "ready" | Error | undefined;
		// Exercise the actual waiter with controlled cover snapshots and events.
		const context = {
			node: { identity: { publicKey: { hashcode: () => "self" } } },
			_log: { getCover, events },
		} as unknown as DocumentIndex<any, any, any>;
		const operation = DocumentIndex.prototype["waitForCoverReady"].call(
			context,
			{ settle: "any", timeout: 100, signal: controller.signal, ...options },
		);
		pending.push(
			operation.then(
				() => {
					result = "ready";
				},
				(error) => {
					result = error;
				},
			),
		);
		return () => result;
	};
	const assertClean = () => {
		for (const call of added.getCalls()) {
			expect(removed.calledWith(call.args[0], call.args[1])).to.equal(true);
		}
		for (const call of abortAdded.getCalls()) {
			expect(abortRemoved.calledWith(call.args[0], call.args[1])).to.equal(
				true,
			);
		}
		expect(clock.countTimers()).to.equal(0);
		const calls = getCover.callCount;
		events.dispatchEvent(new Event("replication:change"));
		expect(getCover.callCount).to.equal(calls);
	};

	it("accepts an existing non-self candidate and cleans up", async () => {
		getCover.resolves(["self", "remote"]);
		const result = start();
		await clock.tickAsync(0);
		expect(result()).to.equal("ready");
		assertClean();
	});

	it("does not lose a readiness event during the initial check", async () => {
		const held = gate();
		getCover.onFirstCall().returns(held.promise);
		getCover.onSecondCall().resolves(["remote"]);
		const result = start();
		events.dispatchEvent(new Event("replicator:join"));
		held.resolve(["self"]);
		await clock.tickAsync(0);
		expect(result()).to.equal("ready");
		expect(getCover.callCount).to.equal(2);
		assertClean();
	});

	it("coalesces readiness events during a later check without parallel checks", async () => {
		const held = gate();
		getCover.onSecondCall().returns(held.promise);
		getCover.onThirdCall().resolves(["remote"]);
		const result = start();
		await clock.tickAsync(0);
		events.dispatchEvent(new Event("replication:change"));
		expect(getCover.callCount).to.equal(2);
		for (let i = 0; i < 5; i++) {
			events.dispatchEvent(new Event("replicator:mature"));
		}
		expect(getCover.callCount).to.equal(2);
		held.resolve(["self"]);
		await clock.tickAsync(0);
		expect(result()).to.equal("ready");
		expect(getCover.callCount).to.equal(3);
		assertClean();
	});

	it("rejects an already-aborted signal without waiting for cover", async () => {
		getCover.returns(gate().promise);
		controller.abort();
		const result = start();
		await clock.tickAsync(0);
		expect(result()).to.be.instanceOf(AbortError);
		expect(getCover.callCount).to.equal(0);
		assertClean();
	});

	it("cancels during the initial check and ignores late readiness", async () => {
		const held = gate();
		getCover.returns(held.promise);
		const result = start();
		controller.abort();
		await clock.tickAsync(0);
		expect(result()).to.be.instanceOf(AbortError);
		assertClean();
		held.resolve(["remote"]);
		await clock.tickAsync(0);
		expect(result()).to.be.instanceOf(AbortError);
		assertClean();
	});

	it("does not recheck a queued event after cancellation", async () => {
		const held = gate();
		getCover.onSecondCall().returns(held.promise);
		const result = start();
		await clock.tickAsync(0);
		events.dispatchEvent(new Event("replication:change"));
		events.dispatchEvent(new Event("replicator:mature"));
		controller.abort();
		await clock.tickAsync(0);
		expect(result()).to.be.instanceOf(AbortError);
		assertClean();
		held.resolve(["self"]);
		await clock.tickAsync(0);
		expect(getCover.callCount).to.equal(2);
		assertClean();
	});

	it("keeps a zero timeout untimed until cancellation", async () => {
		const result = start({ timeout: 0, onTimeout: "error" });
		await clock.tickAsync(1_000);
		expect(result()).to.equal(undefined);
		expect(clock.countTimers()).to.equal(0);
		controller.abort();
		await clock.tickAsync(0);
		expect(result()).to.be.instanceOf(AbortError);
		assertClean();
	});

	it("keeps the configured timeout bounded while a cover check is pending", async () => {
		const held = gate();
		getCover.returns(held.promise);
		const result = start();
		await clock.tickAsync(100);
		expect(result()).to.be.instanceOf(TimeoutError);
		assertClean();
		held.reject(new Error("late cover failure"));
		await clock.tickAsync(0);
		expect(result()).to.be.instanceOf(TimeoutError);
		assertClean();
	});

	it("rejects self-only coverage when timeout policy is error", async () => {
		const result = start();
		await clock.tickAsync(100);
		expect(result()).to.be.instanceOf(TimeoutError);
		assertClean();
	});

	it("preserves the default proceed-on-timeout policy", async () => {
		const result = start({});
		await clock.tickAsync(100);
		expect(result()).to.equal("ready");
		assertClean();
	});

	it("proceeds on timeout even while the initial cover check is pending", async () => {
		const held = gate();
		getCover.returns(held.promise);
		const result = start({});
		await clock.tickAsync(100);
		expect(result()).to.equal("ready");
		assertClean();
		held.reject(new Error("late cover failure"));
		await clock.tickAsync(0);
		expect(result()).to.equal("ready");
		assertClean();
	});

	it("propagates a cover failure and cleans up", async () => {
		const failure = new Error("cover failed");
		getCover.rejects(failure);
		const result = start();
		await clock.tickAsync(0);
		expect(result()).to.equal(failure);
		assertClean();
	});
});
