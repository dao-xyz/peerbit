/* 
ISC License (ISC)
Copyright (c) 2016, Wes Cruver <chieffancypants@gmail.com>

Permission to use, copy, modify, and/or distribute this software for any purpose
with or without fee is hereby granted, provided that the above copyright notice
and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF
THIS SOFTWARE.
*/
import { expect } from "chai";
import nodelocalstorage from "node-localstorage";
import sinon from "sinon";
import {
	afterAll,
	afterEach,
	beforeAll,
	beforeEach,
	describe,
	it,
} from "vitest";
import { FastMutex } from "../src/lockstorage.ts";

describe("FastMutex", () => {
	let sandbox: sinon.SinonSandbox;
	let localStorage: nodelocalstorage.LocalStorage;

	beforeAll(() => {
		const LocalStorage = nodelocalstorage!.LocalStorage;
		localStorage = new LocalStorage("./tmp/FastMutex");
		localStorage.clear();
		globalThis.localStorage = localStorage as any;
	});

	afterAll(() => {
		localStorage.clear();
	});

	beforeEach(() => {
		sandbox = sinon.createSandbox();
		localStorage.clear();
	});
	afterEach(() => {
		sandbox.restore();
		localStorage.clear();
		expect(localStorage.length).to.eq(0);
	});

	it("should immediately establish a lock when there is no contention", async () => {
		const fm1 = new FastMutex({ localStorage: localStorage });

		expect(fm1.isLocked("clientId")).to.be.false;
		await fm1.lock("clientId");
		expect(fm1.isLocked("clientId")).to.be.true;
	});

	it("When another client has a lock (Y is not 0), it should restart to acquire a lock at a later time", function () {
		const fm1 = new FastMutex({
			xPrefix: "xPrefix_",
			yPrefix: "yPrefix_",
			localStorage: localStorage,
		});

		const key = "clientId";
		fm1.setItem(`yPrefix_${key}`, "someOtherMutexId");

		setTimeout(() => {
			localStorage.removeItem(`yPrefix_${key}`);
		}, 20);

		return fm1.lock(key).then(() => {
			expect(fm1.getLockedInfo(key)).to.exist;
		});
	});

	it("when contending for a lock and ultimately losing, it should restart", () => {
		const key = "somekey";
		const fm = new FastMutex({
			localStorage: localStorage,
			clientId: "uniqueId",
		});
		const stub = sandbox.stub(fm, "getItem");

		// Set up scenario for lock contention where we lost Y
		stub.onCall(0).returns(undefined); // getItem Y
		stub.onCall(1).returns("lockcontention"); // getItem X
		stub.onCall(2).returns("youLostTheLock"); // getItem Y

		// fastmutex should have restarted, so let's free up the lock:
		stub.onCall(3).returns(undefined);
		stub.onCall(4).returns("uniqueId");

		return fm.lock(key).then((stats) => {
			expect(stats.restartCount).to.eq(1);
			expect(stats.locksLost).to.eq(1);
			expect(stats.contentionCount).to.eq(1);
		});
	});

	it("When contending for a lock and ultimately winning, it should not restart", () => {
		const key = "somekey";
		const fm = new FastMutex({
			localStorage: localStorage,
			clientId: "uniqueId",
		});
		const stub = sandbox.stub(fm, "getItem");

		// Set up scenario for lock contention where we lost Y
		stub.onCall(0).returns(undefined); // getItem Y
		stub.onCall(1).returns("lockContention");
		stub.onCall(2).returns("uniqueId");

		const spy = sandbox.spy(fm, "lock");

		return fm.lock(key).then((stats) => {
			expect(stats.restartCount).to.eq(0);
			expect(stats.locksLost).to.eq(0);
			expect(stats.contentionCount).to.eq(1);
			expect(spy.callCount).to.eq(1);
		});
	});

	// This is just to ensure that the internals of FastMutex have prefixes on the
	// X and Y locks such that two different FM clients can acquire locks on
	// different keys concurrently without clashing.
	it("should not clash with other fastMutex locks", async () => {
		const yPrefix = "yLock";
		const xPrefix = "xLock";
		const opts = { localStorage, yPrefix, xPrefix };

		const fm1 = new FastMutex(opts);
		const fm2 = new FastMutex(opts);

		let lock1Acquired = false;
		let lock2Acquired = false;

		/* eslint-disable jest/valid-expect-in-promise */
		const lock1Promise = fm1.lock("lock1").then((stats) => {
			lock1Acquired = true;
			expect(localStorage.getItem(yPrefix + "lock1")).to.exist;
			return stats;
		});

		/* eslint-disable jest/valid-expect-in-promise */
		const lock2Promise = fm2.lock("lock2").then((stats) => {
			lock2Acquired = true;
			expect(localStorage.getItem(yPrefix + "lock2")).to.exist;
			return stats;
		});

		await Promise.all([lock1Promise, lock2Promise]).then(() => {
			expect(lock1Acquired).to.be.true;
			expect(lock2Acquired).to.be.true;
		});
	});

	it("release() should remove the y lock in localStorage", () => {
		const key = "somekey";
		const fm1 = new FastMutex({
			localStorage: localStorage,
			clientId: "releaseTestId",
			yPrefix: "yLock",
		});
		return fm1
			.lock(key)
			.then(() => {
				expect(fm1.getItem("yLock" + key)).to.eq("releaseTestId");
				return fm1.release(key);
			})
			.then(() => {
				expect(fm1.getItem("yLock" + key)).to.be.undefined;
			});
	});

	// this is essentially just a better way to test that two locks cannot get
	// an exclusive lock until the other releases.  It's a bit more accurate
	// than the test above ("release should remove the y lock in localstorage")
	it("two clients should never get locks at the same time", function () {
		const fm1 = new FastMutex({ localStorage: localStorage });
		const fm2 = new FastMutex({ localStorage: localStorage });
		let fm1LockReleased = false;
		const lockHoldTime = 10;

		return fm1
			.lock("clientId")
			.then(() => {
				// before the lock is released, try to establish another lock:
				var lock2Promise = fm2.lock("clientId");
				expect(fm1LockReleased).to.be.false;

				// in a few milliseconds, release the lock
				setTimeout(() => {
					fm1.release("clientId");
					fm1LockReleased = true;
				}, lockHoldTime);

				return lock2Promise;
			})
			.then((lock2) => {
				// this will only execute once the other lock was released
				expect(fm1LockReleased).to.be.true;
			});
	});

	it("should throw if lock is never acquired after set time period", async () => {
		const holderTimeout = 2_000;
		const waiterTimeout = 100;
		const fm1 = new FastMutex({
			localStorage: localStorage,
			timeout: holderTimeout,
		});
		const fm2 = new FastMutex({
			localStorage: localStorage,
			timeout: waiterTimeout,
		});
		await fm1.lock("timeoutTest", () => true);
		const start = Date.now();
		let threw = false;
		try {
			await fm2.lock("timeoutTest");
		} catch (e) {
			threw = true;
		} finally {
			fm1.release("timeoutTest");
		}
		const elapsed = Date.now() - start;
		expect(threw).to.be.true;
		expect(elapsed).to.be.greaterThan(0);
	});

	it("should ignore expired locks", async () => {
		const fm1 = new FastMutex({
			localStorage: localStorage,
			timeout: 5000,
			yPrefix: "yLock",
			clientId: "timeoutClient",
		});
		const expiredRecord = {
			expiresAt: new Date().getTime() - 5000,
			value: "oldclient",
		};

		localStorage.setItem("yLocktimeoutTest", JSON.stringify(expiredRecord));
		expect(JSON.parse(localStorage.getItem("yLocktimeoutTest")!).value).to.eq(
			"oldclient",
		);

		await fm1.lock("timeoutTest"); // should not throw
	});

	it("preserves the legacy grace period after the stored expiry", () => {
		const clock = sandbox.useFakeTimers({ now: 10_000 });
		const fm = new FastMutex({ localStorage, timeout: 50 });
		fm.setItem("lease", "owner");

		clock.tick(50);
		expect(fm.getItem("lease")).to.eq("owner");
		clock.tick(49);
		expect(fm.getItem("lease")).to.eq("owner");
		clock.tick(1);
		expect(fm.getItem("lease")).to.be.undefined;
	});

	it("does not let an old owner release a successor lock", async () => {
		const key = "owner-safe-release";
		const oldOwner = new FastMutex({
			localStorage,
			clientId: "old-owner",
		});
		const successor = new FastMutex({
			localStorage,
			clientId: "successor",
		});

		await oldOwner.lock(key, () => true);
		successor.pin(key);
		oldOwner.release(key);

		expect(successor.getLockedOwners(key)).to.deep.equal(["successor"]);
		successor.release(key);
	});

	it("stops an old keepalive before it can overwrite a successor", async () => {
		const clock = sandbox.useFakeTimers({ now: 20_000 });
		const key = "owner-safe-renewal";
		const oldOwner = new FastMutex({
			localStorage,
			clientId: "old-owner",
			timeout: 50,
		});
		const successor = new FastMutex({
			localStorage,
			clientId: "successor",
			timeout: 50,
		});

		await oldOwner.lock(key, () => true);
		successor.pin(key);
		clock.tick(50);

		expect(successor.getLockedOwners(key)).to.deep.equal(["successor"]);
		expect(oldOwner.intervals.size).to.eq(0);
		successor.release(key);
	});

	it("cleans a timed-out contender without deleting the holder", async () => {
		const key = "timed-out-contender";
		const holder = new FastMutex({
			localStorage,
			clientId: "holder",
			timeout: 500,
		});
		const contender = new FastMutex({
			localStorage,
			clientId: "contender",
			timeout: 40,
		});

		await holder.lock(key, () => true);
		let rejected = false;
		try {
			await contender.lock(key, () => true);
		} catch {
			rejected = true;
		}
		expect(rejected).to.be.true;
		expect(contender.intervals.size).to.eq(0);
		expect(holder.getLockedOwners(key)).to.deep.equal(["holder"]);
		holder.release(key);
	});

	it("does not keep a Node process alive for a keep-alive lock", () => {
		const fm = new FastMutex({ localStorage, timeout: 1_000 });
		fm.setItem("node-unref", "owner", () => true);

		const interval = fm.intervals.get("node-unref") as
			| { hasRef?: () => boolean }
			| undefined;
		expect(interval).to.not.equal(undefined);
		expect(interval?.hasRef?.()).to.equal(false);

		fm.releaseIfOwnedBy("node-unref", () => true);
	});

	it("can reacquire after the keep-alive callback releases the lock", async () => {
		const clock = sandbox.useFakeTimers({ now: 30_000 });
		const fm1 = new FastMutex({ localStorage: localStorage, timeout: 50 });
		let keepLock = true;
		let keepLockFn = () => keepLock;
		await fm1.lock("resetStats", keepLockFn);
		expect(fm1.isLocked("resetStats")).to.be.true;
		keepLock = false;
		clock.tick(50);
		expect(fm1.getLockedOwners("resetStats")).to.deep.equal([]);
		expect(fm1.intervals.size).to.eq(0);
		try {
			const stats = await fm1.lock("resetStats");
			expect(stats).to.deep.equal({
				restartCount: 0,
				contentionCount: 0,
				locksLost: 0,
			});
		} finally {
			fm1.release("resetStats");
		}
	});

	it("can release and reacquire without stale ownership", async () => {
		// This test covers marker cleanup, not wall-clock expiry. Keep time fixed so
		// synchronous disk-backed localStorage calls cannot consume the deliberately
		// short lease while a coverage runner is under load.
		sandbox.useFakeTimers({ now: 50_000 });
		const fm1 = new FastMutex({ localStorage: localStorage, timeout: 50 });
		try {
			expect(await fm1.lock("x")).to.deep.equal({
				restartCount: 0,
				contentionCount: 0,
				locksLost: 0,
			});
			fm1.release("x");
			expect(fm1.isLocked("x")).to.be.false;
			expect(await fm1.lock("x")).to.deep.equal({
				restartCount: 0,
				contentionCount: 0,
				locksLost: 0,
			});
		} finally {
			fm1.release("x");
		}
	});

	it("can reacquire after the lock and its legacy grace period expire", async () => {
		const clock = sandbox.useFakeTimers({ now: 40_000 });
		const fm1 = new FastMutex({ localStorage: localStorage, timeout: 50 });

		await fm1.lock("resetStats");
		clock.tick(99);
		expect(fm1.getLockedOwners("resetStats")).to.deep.equal([fm1.clientId]);
		clock.tick(1);
		expect(fm1.getLockedOwners("resetStats")).to.deep.equal([]);
		try {
			const stats = await fm1.lock("resetStats");
			expect(stats).to.deep.equal({
				restartCount: 0,
				contentionCount: 0,
				locksLost: 0,
			});
		} finally {
			fm1.release("resetStats");
		}
	});
});
