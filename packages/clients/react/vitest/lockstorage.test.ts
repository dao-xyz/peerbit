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

import { FastMutex } from "../src/lockstorage.js";
import sinon from "sinon";
import nodelocalstorage from "node-localstorage";
import { expect } from "chai";
import { delay } from "@peerbit/time";
import {
    beforeAll,
    afterAll,
    beforeEach,
    afterEach,
    describe,
    it,
} from "vitest";

describe("FastMutex", () => {
    let sandbox: sinon.SinonSandbox;

    beforeAll(() => {
        var LocalStorage = nodelocalstorage!.LocalStorage;
        var localStorage = new LocalStorage("./tmp/FastMutex");
        localStorage.clear();
        globalThis.localStorage = localStorage;
    });

    afterAll(() => {
        globalThis.localStorage.clear();
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
        const stats = await fm1.lock("clientId");
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
        const fm1 = new FastMutex({ localStorage: localStorage, timeout: 50 });
        const fm2 = new FastMutex({ localStorage: localStorage, timeout: 50 });
        await fm1.lock("timeoutTest");
        const start = Date.now();
        let threw = false;
        try {
            await fm2.lock("timeoutTest");
        } catch (e) {
            threw = true;
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
        expect(
            JSON.parse(localStorage.getItem("yLocktimeoutTest")!).value
        ).to.eq("oldclient");

        await fm1.lock("timeoutTest"); // should not throw
    });

    it("should reset the client stats after lock is released", async () => {
        // without resetting the stats, the acquireStart will always be set, and
        // after `timeout` ms, will be unable to acquire a lock anymore
        const fm1 = new FastMutex({ localStorage: localStorage, timeout: 50 });
        let keepLock = true;
        let keepLockFn = () => keepLock;
        await fm1.lock("resetStats", keepLockFn);
        expect(fm1.isLocked("resetStats")).to.be.true;
        keepLock = false;
        await delay(100); // await timeout
        expect(fm1.isLocked("resetStats")).to.be.false;
        const p = fm1.lock("resetStats").then(() => fm1.release("resetStats"));

        await p; // should not throw
    });

    it("can keep lock with callback function", async () => {
        const fm1 = new FastMutex({ localStorage: localStorage, timeout: 50 });
        await fm1.lock("x");
        await fm1.release("x");
        expect(fm1.isLocked("x")).to.be.false;
        await fm1.lock("x").then(() => fm1.release("x"));
    });

    it("should reset the client stats if the lock has expired", async () => {
        // in the event a lock cannot be acquired within `timeout`, acquireStart
        // will never be reset, and a subsequent call (after the `timeout`) would
        // immediately fail
        const fm1 = new FastMutex({ localStorage: localStorage, timeout: 50 });

        await fm1.lock("resetStats");

        // try to acquire a lock after `timeout`:
        await delay(75); // a small buffer over timeout to avoid flakiness

        await fm1.lock("resetStats"); // should not throw
    });
});
