import { AbortError, TimeoutError } from "@peerbit/time";
import { expect } from "chai";
import { AsyncLocalStorage, AsyncResource } from "node:async_hooks";
import pDefer from "p-defer";
import sinon from "sinon";
import { DocumentIndex } from "../src/search.js";

describe("query cover readiness", () => {
	// The fake clock replaces the process-wide setTimeout, so work that other
	// spec files in this mocha process leave behind (a peer that was never
	// stopped, or libp2p work that finishes after a peer stops) can also put
	// timers on it. Only the waiter's own timers are counted: the waiter starts
	// in this async context, the events and aborts that reach its listeners are
	// dispatched in it, and the callbacks of its timers run in it.
	const waiterContext = new AsyncLocalStorage<true>();
	const inWaiterContext = <T>(fn: () => T): T => waiterContext.run(true, fn);
	let pendingWaiterTimers: Set<unknown>;
	let unwrapTimers: (() => void) | undefined;
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
		const fakeSetTimeout = globalThis.setTimeout;
		const fakeClearTimeout = globalThis.clearTimeout;
		unwrapTimers = () => {
			globalThis.setTimeout = fakeSetTimeout;
			globalThis.clearTimeout = fakeClearTimeout;
		};
		pendingWaiterTimers = new Set();
		globalThis.setTimeout = ((
			callback: (...args: unknown[]) => void,
			ms?: number,
			...args: unknown[]
		) => {
			if (!waiterContext.getStore()) {
				return fakeSetTimeout(callback, ms, ...args);
			}
			const timer = fakeSetTimeout(
				AsyncResource.bind((...callbackArgs: unknown[]) => {
					pendingWaiterTimers.delete(timer);
					callback(...callbackArgs);
				}),
				ms,
				...args,
			);
			pendingWaiterTimers.add(timer);
			return timer;
		}) as typeof setTimeout;
		globalThis.clearTimeout = ((timer) => {
			pendingWaiterTimers.delete(timer);
			fakeClearTimeout(timer);
		}) as typeof clearTimeout;
		events = new EventTarget();
		const dispatchEvent = events.dispatchEvent.bind(events);
		events.dispatchEvent = (event) =>
			inWaiterContext(() => dispatchEvent(event));
		controller = new AbortController();
		const abort = controller.abort.bind(controller);
		controller.abort = (reason) => inWaiterContext(() => abort(reason));
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
			// Put the clock's own functions back before sinon.restore() uninstalls
			// the clock: some fake-timers versions look at the installed function
			// to decide how to restore a global, and would delete setTimeout
			// instead of restoring it if they found a wrapper there
			unwrapTimers?.();
			unwrapTimers = undefined;
			sinon.restore();
			clock.restore();
		}
	});

	after(() => {
		// Stop async context tracking for the spec files that run after this one
		waiterContext.disable();
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
		const operation = inWaiterContext(() =>
			DocumentIndex.prototype["waitForCoverReady"].call(context, {
				settle: "any",
				timeout: 100,
				signal: controller.signal,
				...options,
			}),
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
		expect(pendingWaiterTimers.size).to.equal(0);
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
		expect(pendingWaiterTimers.size).to.equal(0);
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
