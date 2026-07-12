import { delay } from "@peerbit/time";
import { expect } from "chai";
import { default as sodium } from "libsodium-wrappers";
import nodelocalstorage from "node-localstorage";
import { v4 as uuid } from "uuid";
import { afterAll, beforeAll, describe, it } from "vitest";
import { FastMutex } from "../src/lockstorage.ts";
import {
	createWebLockClientId,
	getAllKeyPairs,
	getFreeKeypair,
	getFreeKeypairWithWebLock,
	releaseKey,
} from "../src/utils.ts";

type TestLockCallback<T> = (lock: Lock | null) => T | PromiseLike<T>;

class InMemoryLockManager {
	private held = new Map<string, symbol>();
	private waiters = new Map<string, Set<() => void>>();

	private wake(name: string) {
		for (const resolve of this.waiters.get(name) ?? []) {
			resolve();
		}
		this.waiters.delete(name);
	}

	async request<T>(
		name: string,
		optionsOrCallback: LockOptions | TestLockCallback<T>,
		maybeCallback?: TestLockCallback<T>,
	): Promise<T> {
		const options =
			typeof optionsOrCallback === "function" ? {} : optionsOrCallback;
		const callback =
			typeof optionsOrCallback === "function"
				? optionsOrCallback
				: maybeCallback!;
		if (options.ifAvailable && options.signal) {
			throw new TypeError(
				"The 'signal' and 'ifAvailable' options cannot be used together.",
			);
		}
		if (this.held.has(name) && options.ifAvailable) {
			return callback(null);
		}
		while (this.held.has(name)) {
			await new Promise<void>((resolve) => {
				const waiters = this.waiters.get(name) ?? new Set();
				waiters.add(resolve);
				this.waiters.set(name, waiters);
			});
		}

		const token = Symbol(name);
		this.held.set(name, token);
		try {
			return await callback({ name, mode: "exclusive" } as Lock);
		} finally {
			if (this.held.get(name) === token) {
				this.held.delete(name);
				this.wake(name);
			}
		}
	}

	crashAll() {
		for (const name of this.held.keys()) {
			this.held.delete(name);
			this.wake(name);
		}
	}
}

describe("getKeypair", () => {
	let localStorage: nodelocalstorage.LocalStorage;

	beforeAll(async () => {
		await sodium.ready;

		const LocalStorage = nodelocalstorage.LocalStorage;
		localStorage = new LocalStorage("./tmp/getKeypair");
		globalThis.localStorage = localStorage as any;
	});

	afterAll(() => {
		localStorage.clear();
	});

	it("can aquire multiple keypairs", async () => {
		let timeout = 1000;
		let mutex = new FastMutex({ localStorage, timeout });
		let lock = true;
		const lockCondition = () => lock;
		let id = uuid();
		const { key: keypair, path: path1 } = await getFreeKeypair(
			id,
			mutex,
			lockCondition,
		);
		const { key: keypair2, path: path2 } = await getFreeKeypair(id, mutex);
		expect(keypair!.equals(keypair2!)).to.be.false;
		expect(path1).not.to.eq(path2);
		lock = false;
		await delay(timeout);
		const { path: path3, key: keypair3 } = await getFreeKeypair(id, mutex);
		expect(path3).to.eq(path1);
		expect(keypair3.equals(keypair)).to.be.true;

		const allKeypair = await getAllKeyPairs(id);
		expect(allKeypair.map((x) => x.publicKey.hashcode())).to.deep.eq([
			keypair3.publicKey.hashcode(),
			keypair2.publicKey.hashcode(),
		]);
	});

	it("can release if same id", async () => {
		let timeout = 1000;
		let mutex = new FastMutex({ localStorage, timeout });
		let lock = true;
		const lockCondition = () => lock;
		let id = uuid();
		const { key: keypair, path: path1 } = await getFreeKeypair(
			id,
			mutex,
			lockCondition,
			{ releaseLockIfSameId: true },
		);
		const { key: keypair2, path: path2 } = await getFreeKeypair(
			id,
			mutex,
			undefined,
			{ releaseLockIfSameId: true },
		);
		expect(keypair!.equals(keypair2!)).to.be.true;
		expect(path1).to.eq(path2);
		const allKeypair = await getAllKeyPairs(id);
		expect(allKeypair).to.have.length(1);
	});

	it("can force release the first lock after storage reset", async () => {
		const id = uuid();
		const oldMutex = new FastMutex({
			localStorage,
			clientId: "old-owner",
			timeout: 1000,
		});
		const currentMutex = new FastMutex({
			localStorage,
			clientId: "current-owner",
			timeout: 1000,
		});
		const old = await getFreeKeypair(id, oldMutex, () => true);

		const current = await getFreeKeypair(
			id,
			currentMutex,
			() => true,
			{ releaseFirstLock: true },
		);

		expect(current.path).to.eq(old.path);
		expect(current.key.equals(old.key)).to.be.true;
		oldMutex.release(old.path);
		expect(currentMutex.getLockedOwners(current.path)).to.deep.equal([
			"current-owner",
		]);
		currentMutex.release(current.path);
	});

	it("releases manually", async () => {
		let timeout = 1000;
		let mutex = new FastMutex({ localStorage, timeout });
		const id = uuid();

		const { path: path1 } = await getFreeKeypair(id, mutex);

		const { path: path2 } = await getFreeKeypair(id, mutex);

		expect(path1).not.to.eq(path2);
		releaseKey(path1, mutex);
		expect(mutex.getLockedInfo(path1)).to.be.undefined;
		const { path: path3 } = await getFreeKeypair(id, mutex);

		expect(path1).to.eq(path3); // we can now acquire key at path1 again, since we released it
	});

	it("keeps concurrent Web Lock tabs on distinct identities", async () => {
		const id = uuid();
		const manager = new InMemoryLockManager();
		const firstMutex = new FastMutex({
			localStorage,
			clientId: createWebLockClientId(),
			timeout: 1000,
		});
		const secondMutex = new FastMutex({
			localStorage,
			clientId: createWebLockClientId(),
			timeout: 1000,
		});

		const first = await getFreeKeypairWithWebLock(
			id,
			firstMutex,
			manager as unknown as LockManager,
		);
		const second = await getFreeKeypairWithWebLock(
			id,
			secondMutex,
			manager as unknown as LockManager,
		);

		expect(first.index).to.eq(0);
		expect(second.index).to.eq(1);
		expect(first.key.equals(second.key)).to.be.false;
		second.release();
		first.release();
	});

	it("uses an abort signal without forwarding it to Web Lock probes", async () => {
		const id = uuid();
		const manager = new InMemoryLockManager();
		const controller = new AbortController();
		const mutex = new FastMutex({
			localStorage,
			clientId: createWebLockClientId(),
			timeout: 1000,
		});

		const current = await getFreeKeypairWithWebLock(
			id,
			mutex,
			manager as unknown as LockManager,
			undefined,
			controller.signal,
		);

		expect(current.index).to.eq(0);
		expect(mutex.getLockedOwners(current.path)).to.deep.equal([mutex.clientId]);
		current.release();
	});

	it("does not overwrite a successor marker when cancellation races local locking", async () => {
		const id = uuid();
		const manager = new InMemoryLockManager();
		const controller = new AbortController();
		const firstMutex = new FastMutex({
			localStorage,
			clientId: createWebLockClientId(),
			timeout: 1000,
		});
		const replacementMutex = new FastMutex({
			localStorage,
			clientId: createWebLockClientId(),
			timeout: 1000,
		});
		let replacement:
			| Awaited<ReturnType<typeof getFreeKeypairWithWebLock>>
			| undefined;
		const originalLock = firstMutex.lock.bind(firstMutex);
		firstMutex.lock = async (key, keepLocked, options) => {
			const result = await originalLock(key, keepLocked, options);
			if (key === `${id}/0`) {
				controller.abort(new DOMException("Cancelled", "AbortError"));
				replacement = await getFreeKeypairWithWebLock(
					id,
					replacementMutex,
					manager as unknown as LockManager,
				);
			}
			return result;
		};

		let rejected = false;
		try {
			await getFreeKeypairWithWebLock(
				id,
				firstMutex,
				manager as unknown as LockManager,
				undefined,
				controller.signal,
			);
		} catch (error) {
			rejected = (error as DOMException).name === "AbortError";
		}

		expect(rejected).to.be.true;
		expect(replacement).not.to.be.undefined;
		expect(replacement!.index).to.eq(0);
		expect(replacementMutex.getLockedOwners(replacement!.path)).to.deep.equal([
			replacementMutex.clientId,
		]);
		replacement!.release();
	});

	it("releases both lock layers when pinning the advisory marker fails", async () => {
		const id = uuid();
		const manager = new InMemoryLockManager();
		const failedMutex = new FastMutex({
			localStorage,
			clientId: createWebLockClientId(),
			timeout: 1000,
		});
		failedMutex.pin = () => {
			throw new Error("storage write failed");
		};

		let rejected = false;
		try {
			await getFreeKeypairWithWebLock(
				id,
				failedMutex,
				manager as unknown as LockManager,
			);
		} catch {
			rejected = true;
		}
		expect(rejected).to.be.true;
		expect(failedMutex.getLockedOwners(`${id}/0`)).to.deep.equal([]);

		const replacementMutex = new FastMutex({
			localStorage,
			clientId: createWebLockClientId(),
			timeout: 1000,
		});
		const replacement = await getFreeKeypairWithWebLock(
			id,
			replacementMutex,
			manager as unknown as LockManager,
		);
		expect(replacement.index).to.eq(0);
		replacement.release();
	});

	it("honors an active legacy owner when allocating with Web Locks", async () => {
		const id = uuid();
		const manager = new InMemoryLockManager();
		const legacyMutex = new FastMutex({
			localStorage,
			clientId: "legacy-owner",
			timeout: 1000,
		});
		const legacy = await getFreeKeypair(id, legacyMutex, () => true);
		const webMutex = new FastMutex({
			localStorage,
			clientId: createWebLockClientId(),
			timeout: 1000,
		});

		const current = await getFreeKeypairWithWebLock(
			id,
			webMutex,
			manager as unknown as LockManager,
		);
		expect(legacy.index).to.eq(0);
		expect(current.index).to.eq(1);
		expect(current.key.equals(legacy.key)).to.be.false;

		current.release();
		legacyMutex.release(legacy.path);
	});

	it("publishes a pinned advisory marker visible to legacy tabs", async () => {
		const id = uuid();
		const manager = new InMemoryLockManager();
		const webMutex = new FastMutex({
			localStorage,
			clientId: createWebLockClientId(),
			timeout: 50,
		});
		const current = await getFreeKeypairWithWebLock(
			id,
			webMutex,
			manager as unknown as LockManager,
		);
		const xRecord = JSON.parse(
			localStorage.getItem(`_MUTEX_LOCK_X_${current.path}`)!,
		);
		expect(xRecord.expiresAt).to.eq(Number.MAX_SAFE_INTEGER);

		const legacyMutex = new FastMutex({
			localStorage,
			clientId: "later-legacy-owner",
			timeout: 50,
		});
		const legacy = await getFreeKeypair(id, legacyMutex, () => true);
		expect(legacy.index).to.eq(1);

		legacyMutex.release(legacy.path);
		current.release();
	});

	it("immediately reclaims a crashed Web Lock identity", async () => {
		const id = uuid();
		const manager = new InMemoryLockManager();
		const crashedMutex = new FastMutex({
			localStorage,
			clientId: createWebLockClientId(),
			timeout: 1000,
		});
		const crashed = await getFreeKeypairWithWebLock(
			id,
			crashedMutex,
			manager as unknown as LockManager,
		);

		manager.crashAll();
		const replacementMutex = new FastMutex({
			localStorage,
			clientId: createWebLockClientId(),
			timeout: 1000,
		});
		const replacement = await getFreeKeypairWithWebLock(
			id,
			replacementMutex,
			manager as unknown as LockManager,
		);

		expect(replacement.index).to.eq(0);
		expect(replacement.key.equals(crashed.key)).to.be.true;
		crashed.release();
		expect(replacementMutex.getLockedOwners(replacement.path)).to.deep.equal([
			replacementMutex.clientId,
		]);
		replacement.release();
	});
});
