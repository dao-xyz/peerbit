import nodelocalstorage from "node-localstorage";
import { expect } from "vitest";
import { afterEach, beforeAll, beforeEach, describe, it } from "vitest";
import { FastMutex } from "../src/lockstorage.ts";

describe("FastMutex singleton semantics", () => {
	let localStorage: any;

	beforeAll(() => {
		const LocalStorage = nodelocalstorage!.LocalStorage;
		localStorage = new LocalStorage("./tmp/FastMutex-singleton");
		globalThis.localStorage = localStorage as any;
	});

	beforeEach(() => {
		localStorage.clear();
	});

	afterEach(() => {
		localStorage.clear();
		expect(localStorage.length).to.eq(0);
	});

	it("allows same-session to reacquire the singleton lock when replaceIfSameClient is true", async () => {
		const key = "localId-singleton";

		const fm1 = new FastMutex({
			localStorage,
			clientId: "session-1",
			timeout: 200,
		});

		// First acquire and keep the lock alive
		await fm1.lock(key, () => true);
		expect(fm1.isLocked(key)).to.be.true;

		// Reacquire from the same client with replaceIfSameClient
		const start = Date.now();
		await fm1.lock(key, () => true, { replaceIfSameClient: true });
		const elapsed = Date.now() - start;

		// Should not time out and should be very fast
		expect(elapsed).to.be.lessThan(100);
		expect(fm1.isLocked(key)).to.be.true;

		fm1.release(key);
		expect(fm1.isLocked(key)).to.be.false;
	});

	it("blocks a different session while held, then allows after release", async () => {
		const key = "localId-singleton";

		const fm1 = new FastMutex({
			localStorage,
			clientId: "session-1",
			timeout: 500,
		});
		const fm2 = new FastMutex({
			localStorage,
			clientId: "session-2",
			timeout: 100, // short timeout to trigger failure while held
		});

		await fm1.lock(key, () => true);
		expect(fm1.isLocked(key)).to.be.true;

		// Attempt to acquire from a different client should fail within timeout
		let failed = false;
		try {
			await fm2.lock(key, () => true);
		} catch (e) {
			failed = true;
		}
		expect(failed).to.be.true;

		// After release, the second client should be able to acquire
		fm1.release(key);
		expect(fm1.isLocked(key)).to.be.false;

		await fm2.lock(key, () => true);
		expect(fm2.isLocked(key)).to.be.true;
		fm2.release(key);
		expect(fm2.isLocked(key)).to.be.false;
	});
});
