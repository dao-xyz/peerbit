import { expect } from "chai";
import pDefer, { type DeferredPromise } from "p-defer";
import sinon from "sinon";
import { EntryInventoryRecovery } from "../src/sync/entry-inventory-recovery.js";

type Session = { generation: number };
type Result = "more" | "complete" | "retry";
type Pass = {
	peer: string;
	session: Session;
	signal: AbortSignal;
	close: sinon.SinonSpy;
};
type Call = {
	peer: string;
	session: Session;
	signal: AbortSignal;
	pass: Pass;
	release: DeferredPromise<Result>;
};

describe("entry inventory page recovery scheduler", () => {
	let clock: sinon.SinonFakeTimers;
	let owner: AbortController;
	let current: Map<string, Session>;
	let calls: Call[];
	let passes: Pass[];
	let physical: Set<Call>;
	let maximumPhysical: number;
	let errors: unknown[];
	let recovery: EntryInventoryRecovery<Session>;
	const session = (peer: string, generation = 1) => {
		const next = { generation };
		current.set(peer, next);
		return next;
	};
	const names = () =>
		calls.map((call) => `${call.peer}:${call.session.generation}`);
	const flush = () => clock.tickAsync(0);

	beforeEach(() => {
		clock = sinon.useFakeTimers({
			now: 0,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		owner = new AbortController();
		current = new Map();
		calls = [];
		passes = [];
		physical = new Set();
		maximumPhysical = 0;
		errors = [];
		recovery = new EntryInventoryRecovery({
			signal: owner.signal,
			isCurrent: (peer, captured) => current.get(peer) === captured,
			create: (peer, captured, signal) => {
				const pass = { peer, session: captured, signal, close: sinon.spy() };
				passes.push(pass);
				return {
					next: () => {
						const call: Call = {
							peer,
							session: captured,
							signal,
							pass,
							release: pDefer<Result>(),
						};
						calls.push(call);
						physical.add(call);
						maximumPhysical = Math.max(maximumPhysical, physical.size);
						// Ignore abort deliberately: a page keeps its slot until its
						// physical operation settles, including after replacement.
						return call.release.promise.finally(() => {
							physical.delete(call);
						});
					},
					close: pass.close,
				};
			},
			onError: (error) => errors.push(error),
		});
	});
	afterEach(async () => {
		try {
			recovery.close();
			for (const call of calls) call.release.resolve("complete");
			await flush();
			expect(physical.size).to.equal(0);
			expect(maximumPhysical).to.be.at.most(2);
			for (const pass of passes) expect(pass.close.calledOnce).to.equal(true);
			expect(clock.countTimers()).to.equal(0);
		} finally {
			clock.restore();
			sinon.restore();
		}
	});

	it("coalesces repeated active and queued same-session wakes into one pass", async () => {
		const peers = ["a", "b", "c"].map((peer) => [peer, session(peer)] as const);
		for (let round = 0; round < 10; round++)
			for (const [peer, captured] of peers) recovery.wake(peer, captured);
		expect(names()).to.deep.equal(["a:1", "b:1"]);
		expect(physical.size).to.equal(2);
		calls[0].release.resolve("complete");
		await flush();
		expect(names()).to.deep.equal(["a:1", "b:1", "c:1"]);
		calls[1].release.resolve("complete");
		calls[2].release.resolve("complete");
		await clock.tickAsync(5_000);
		expect(names()).to.deep.equal(["a:1", "b:1", "c:1"]);
		expect(errors).to.deep.equal([]);
	});

	it("keeps aborted replacement and forgotten jobs charged until physical settlement", async () => {
		const oldA = session("a");
		recovery.wake("a", oldA);
		recovery.wake("b", session("b"));
		const replacement = session("a", 2);
		for (let i = 0; i < 10; i++) recovery.wake("a", replacement);
		recovery.wake("c", session("c"));
		current.delete("b");
		recovery.forget("b");
		expect(calls[0].signal.aborted && calls[1].signal.aborted).to.equal(true);
		await clock.tickAsync(10_000);
		expect(names()).to.deep.equal(["a:1", "b:1"]);
		expect(physical.size).to.equal(2);

		calls[1].release.resolve("retry");
		await flush();
		// A's replacement cannot overlap the old A; another healthy peer can run.
		expect(names()).to.deep.equal(["a:1", "b:1", "c:1"]);
		calls[0].release.resolve("retry");
		await flush();
		expect(names()).to.deep.equal(["a:1", "b:1", "c:1", "a:2"]);
		expect(calls[3].session).to.equal(replacement);
		expect(calls[3].signal.aborted).to.equal(false);
		expect(maximumPhysical).to.equal(2);
	});

	it("drops obsolete queued sessions and ignores stale wake callbacks", async () => {
		recovery.wake("a", session("a"));
		recovery.wake("b", session("b"));
		const staleC = session("c");
		recovery.wake("c", staleC);
		const replacement = session("c", 2);
		recovery.wake("c", staleC);
		calls[0].release.resolve("complete");
		await flush();
		expect(names()).to.deep.equal(["a:1", "b:1"]);
		recovery.wake("c", replacement);
		expect(names()).to.deep.equal(["a:1", "b:1", "c:2"]);
	});

	it("coalesces ownership restarts without replacing physically active slots", async () => {
		const a = session("a"),
			b = session("b");
		recovery.wake("a", a);
		recovery.wake("b", b);
		recovery.restartActive();
		recovery.restartActive();
		expect(calls.every((call) => call.signal.aborted)).to.equal(true);
		await flush();
		expect(calls).to.have.length(2);
		const replacement = session("a", 2);
		recovery.wake("a", replacement);
		recovery.restartActive(); // Must not replace the queued newer session.
		calls[0].release.resolve("complete");
		await flush();
		expect(names()).to.deep.equal(["a:1", "b:1", "a:2"]);
		expect(calls[2].signal.aborted).to.equal(false);
		calls[1].release.resolve("complete");
		await flush();
		expect(names()).to.deep.equal(["a:1", "b:1", "a:2", "b:1"]);
		expect(calls[3].session).to.equal(b);
		expect(calls[3].signal).not.to.equal(calls[1].signal);
		expect(calls[3].signal.aborted).to.equal(false);
	});

	it("sends a retrying peer to the back so healthy queued peers progress", async () => {
		for (const peer of ["retry", "blocker", "healthy-1", "healthy-2"])
			recovery.wake(peer, session(peer));
		calls[0].release.resolve("retry");
		await flush();
		expect(names()).to.deep.equal(["retry:1", "blocker:1", "healthy-1:1"]);
		calls[2].release.resolve("complete");
		await flush();
		expect(names()).to.deep.equal([
			"retry:1",
			"blocker:1",
			"healthy-1:1",
			"healthy-2:1",
		]);
		calls[3].release.resolve("complete");
		await flush();
		expect(clock.countTimers()).to.equal(1);
		await clock.tickAsync(999);
		expect(calls).to.have.length(4);
		await clock.tickAsync(1);
		expect(names().at(-1)).to.equal("retry:1");
		expect(calls).to.have.length(5);
	});

	it("gives healthy peers their first page before two long incomplete histories continue", async () => {
		for (const peer of [
			"lost-replies",
			"rejected-entries",
			"healthy-1",
			"healthy-2",
		])
			recovery.wake(peer, session(peer));
		expect(names()).to.deep.equal(["lost-replies:1", "rejected-entries:1"]);
		// Each slow pass has arbitrarily many remaining pages. Its page deadline
		// may finish without presence proof, but continuation is a new FIFO turn.
		calls[0].release.resolve("more");
		calls[1].release.resolve("more");
		await flush();
		expect(names()).to.deep.equal([
			"lost-replies:1",
			"rejected-entries:1",
			"healthy-1:1",
			"healthy-2:1",
		]);
		expect(passes[0].close.notCalled && passes[1].close.notCalled).to.equal(
			true,
		);
		calls[2].release.resolve("complete");
		calls[3].release.resolve("complete");
		await flush();
		expect(names()).to.deep.equal([
			"lost-replies:1",
			"rejected-entries:1",
			"healthy-1:1",
			"healthy-2:1",
			"lost-replies:1",
			"rejected-entries:1",
		]);
		expect(calls[4].pass).to.equal(calls[0].pass);
		expect(calls[5].pass).to.equal(calls[1].pass);
		expect(passes).to.have.length(4); // Cursors resume; no prefix rescans.
		expect(physical.size).to.equal(2);
	});

	it("services timers and newly ready peers between immediately resolved pages", async () => {
		recovery.close();
		let pages = 0;
		let pagesAtTimer: number | undefined;
		let pagesAtHealthyAdmission: number | undefined;
		const closed: string[] = [];
		recovery = new EntryInventoryRecovery<Session>({
			signal: owner.signal,
			isCurrent: (peer, captured) => current.get(peer) === captured,
			create: (peer) => ({
				next: async () => {
					if (peer === "healthy") {
						pagesAtHealthyAdmission = pages;
						return "complete";
					}
					// Model pages with no eligible hashes: no network or storage timer
					// yields the event loop on behalf of the scheduler.
					pages++;
					return pages < 100 ? "more" : "complete";
				},
				close: () => {
					closed.push(peer);
				},
			}),
			onError: (error) => errors.push(error),
		});
		recovery.wake("immediate", session("immediate"));
		const observer = setTimeout(() => {
			pagesAtTimer = pages;
			recovery.wake("healthy", session("healthy"));
		}, 0);
		try {
			await clock.runAllAsync();
			expect(pagesAtTimer).to.equal(1);
			expect(pagesAtHealthyAdmission).to.be.at.most(2);
			expect(pages).to.equal(100);
			expect(closed).to.deep.equal(["healthy", "immediate"]);
			expect(clock.countTimers()).to.equal(0);
			expect(errors).to.deep.equal([]);
		} finally {
			clearTimeout(observer);
		}
	});

	it("clears a scheduled page continuation when its owner aborts", async () => {
		recovery.close();
		const next = sinon.stub().resolves("more");
		const close = sinon.spy();
		recovery = new EntryInventoryRecovery<Session>({
			signal: owner.signal,
			isCurrent: (peer, captured) => current.get(peer) === captured,
			create: () => ({ next, close }),
			onError: (error) => errors.push(error),
		});
		recovery.wake("a", session("a"));
		await Promise.resolve(); // Finish the immediate page, without running timers.
		expect(next.calledOnce).to.equal(true);
		expect(clock.countTimers()).to.equal(1);
		owner.abort();
		expect(clock.countTimers()).to.equal(0);
		await clock.runAllAsync();
		expect(next.calledOnce && close.calledOnce).to.equal(true);
	});

	it("closes a parked cursor on replacement without disturbing physically active pages", async () => {
		for (const peer of ["a", "b", "c"]) recovery.wake(peer, session(peer));
		calls[0].release.resolve("more");
		await flush(); // B and C execute; A's cursor is parked.
		const oldA = passes[0];
		const replacement = session("a", 2);
		recovery.wake("a", replacement);
		expect(oldA.signal.aborted && oldA.close.calledOnce).to.equal(true);
		expect(physical.size).to.equal(2);
		expect(passes).to.have.length(3);
		calls[1].release.resolve("complete");
		await flush();
		expect(names()).to.deep.equal(["a:1", "b:1", "c:1", "a:2"]);
		expect(calls[3].pass).not.to.equal(oldA);
		expect(calls[3].session).to.equal(replacement);
	});

	it("invalidates parked and active ownership snapshots while retaining physical charges", async () => {
		for (const peer of ["a", "b", "c"]) recovery.wake(peer, session(peer));
		calls[0].release.resolve("more");
		await flush();
		const oldA = passes[0];
		recovery.restartActive();
		expect(oldA.signal.aborted && oldA.close.calledOnce).to.equal(true);
		expect(calls[1].signal.aborted && calls[2].signal.aborted).to.equal(true);
		expect(passes[1].close.notCalled && passes[2].close.notCalled).to.equal(
			true,
		);
		expect(physical.size).to.equal(2);
		calls[2].release.resolve("more");
		await flush();
		expect(names()).to.deep.equal(["a:1", "b:1", "c:1", "a:1"]);
		expect(calls[3].pass).not.to.equal(oldA);
		expect(calls[3].signal.aborted).to.equal(false);
		expect(passes[2].close.calledOnce).to.equal(true);
		calls[1].release.resolve("more");
		await flush();
		expect(names().at(-1)).to.equal("b:1");
		expect(calls[4].pass).not.to.equal(passes[1]);
	});

	it("closes parked cursors immediately but waits for active physical work on shutdown", async () => {
		for (const peer of ["a", "b", "c"]) recovery.wake(peer, session(peer));
		calls[0].release.resolve("more");
		await flush();
		recovery.close();
		expect(passes[0].close.calledOnce).to.equal(true);
		expect(passes[1].close.notCalled && passes[2].close.notCalled).to.equal(
			true,
		);
		expect(passes.every((pass) => pass.signal.aborted)).to.equal(true);
		expect(physical.size).to.equal(2);
		calls[1].release.resolve("more");
		calls[2].release.resolve("more");
		await flush();
		expect(calls).to.have.length(3);
		expect(physical.size).to.equal(0);
		expect(passes.every((pass) => pass.close.calledOnce)).to.equal(true);
	});

	it("preserves queued retry backoff when the same session becomes writable again", async () => {
		const captured = session("a");
		recovery.wake("a", captured);
		calls[0].release.resolve("retry");
		await flush();
		await clock.tickAsync(500);
		for (let index = 0; index < 10; index++) recovery.wake("a", captured);
		expect(calls).to.have.length(1);
		await clock.tickAsync(499);
		expect(calls).to.have.length(1);
		await clock.tickAsync(1);
		expect(calls).to.have.length(2);
		expect(calls[1].pass).not.to.equal(calls[0].pass);
	});

	it("retains an active slot through cursor cleanup and owns cleanup rejection", async () => {
		recovery.close();
		const first = pDefer<Result>();
		const second = pDefer<Result>();
		const cleanup = pDefer<void>();
		const created: string[] = [];
		const close = sinon.spy(() => cleanup.promise);
		recovery = new EntryInventoryRecovery<Session>({
			signal: owner.signal,
			isCurrent: (peer, captured) => current.get(peer) === captured,
			create: (peer) => {
				created.push(peer);
				return {
					next: () =>
						peer === "a"
							? first.promise
							: peer === "b"
								? second.promise
								: Promise.resolve("complete" as const),
					close: peer === "a" ? close : () => {},
				};
			},
			onError: (error) => errors.push(error),
		});
		try {
			for (const peer of ["a", "b", "c"]) recovery.wake(peer, session(peer));
			first.resolve("complete");
			await flush();
			expect(close.calledOnce).to.equal(true);
			expect(created).to.deep.equal(["a", "b"]);
			const failure = new Error("cursor cleanup failed");
			cleanup.reject(failure);
			await flush();
			expect(errors).to.deep.equal([failure]);
			expect(created).to.deep.equal(["a", "b", "c"]);
		} finally {
			first.resolve("complete");
			second.resolve("complete");
			cleanup.resolve();
			await flush();
		}
	});

	it("owns run rejections and ignores late rejection of obsolete work", async () => {
		recovery.wake("a", session("a"));
		const failure = new Error("bounded pass failed");
		calls[0].release.reject(failure);
		await flush();
		expect(errors).to.deep.equal([failure]);
		expect(clock.countTimers()).to.equal(1);
		await clock.tickAsync(1_000);
		expect(calls).to.have.length(2);
		const replacement = session("a", 2);
		recovery.wake("a", replacement);
		calls[1].release.reject(new Error("obsolete physical failure"));
		await flush();
		expect(errors).to.deep.equal([failure]);
		expect(names()).to.deep.equal(["a:1", "a:1", "a:2"]);
	});

	it("does not strand a slot or leak rejection when error reporting throws", async () => {
		const failure = new Error("pass failure");
		recovery.close();
		const onError = sinon.spy((_error: unknown) => {
			throw new Error("diagnostic failure");
		});
		const next = sinon.stub().rejects(failure);
		recovery = new EntryInventoryRecovery<Session>({
			signal: owner.signal,
			isCurrent: (peer, captured) => current.get(peer) === captured,
			create: () => ({ next, close: () => {} }),
			onError,
		});
		recovery.wake("a", session("a"));
		await flush();
		expect(onError.calledOnceWithExactly(failure)).to.equal(true);
		expect(clock.countTimers()).to.equal(1);
		await clock.tickAsync(1_000);
		expect(next.callCount).to.equal(2);
	});

	for (const stop of ["close", "abort"] as const) {
		it(`${stop} clears retry timers and fences late physical completion`, async () => {
			recovery.wake("retry", session("retry"));
			recovery.wake("stuck", session("stuck"));
			calls[0].release.resolve("retry");
			await flush();
			expect(clock.countTimers()).to.equal(1);
			if (stop === "close") recovery.close();
			else owner.abort();
			expect(calls[1].signal.aborted).to.equal(true);
			expect(clock.countTimers()).to.equal(0);
			recovery.wake("late", session("late"));
			calls[1].release.reject(new Error("late stopped operation"));
			await clock.tickAsync(10_000);
			expect(names()).to.deep.equal(["retry:1", "stuck:1"]);
			expect(errors).to.deep.equal([]);
			expect(physical.size).to.equal(0);
		});
	}

	it("does not admit work when its owner is already aborted", () => {
		recovery.close();
		owner.abort();
		const create = sinon.spy(() => ({
			next: async () => "complete" as const,
			close: () => {},
		}));
		recovery = new EntryInventoryRecovery<Session>({
			signal: owner.signal,
			isCurrent: () => true,
			create,
			onError: (error) => errors.push(error),
		});
		recovery.wake("a", session("a"));
		expect(create.notCalled).to.equal(true);
		expect(clock.countTimers()).to.equal(0);
	});
});
