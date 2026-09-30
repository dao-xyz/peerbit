import { AnyBlockStore } from "@peerbit/blocks";
import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";
import sinon from "sinon";
import { createEntry } from "../src/entry-create.js";
import type { Entry } from "../src/entry.js";
import { Log } from "../src/log.js";

const deferred = <T = void>() => {
	let resolve!: (value: T | PromiseLike<T>) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<T>((yes, no) => {
		resolve = yes;
		reject = no;
	});
	return { promise, resolve, reject };
};

const turn = () => new Promise<void>((resolve) => setImmediate(resolve));
const outcome = (promise: Promise<unknown>) =>
	promise.then(
		() => ({ error: undefined as unknown }),
		(error: unknown) => ({ error }),
	);

describe("join caller cancellation", () => {
	let store: AnyBlockStore;
	let sourceStore: AnyBlockStore;
	let source: Log<Uint8Array>;
	let target: Log<Uint8Array>;
	let root: Entry<Uint8Array>;
	let middle: Entry<Uint8Array>;
	let head: Entry<Uint8Array>;
	const cleanup: Array<() => Promise<unknown> | void> = [];

	beforeEach(async () => {
		store = new AnyBlockStore();
		sourceStore = new AnyBlockStore();
		await store.start();
		await sourceStore.start();
		source = new Log();
		target = new Log();
		await source.open(sourceStore, await Ed25519Keypair.create());
		await target.open(store, await Ed25519Keypair.create());
		root = (await source.append(new Uint8Array([1]), { meta: { next: [] } }))
			.entry;
		middle = (
			await source.append(new Uint8Array([2]), { meta: { next: [root] } })
		).entry;
		head = (
			await source.append(new Uint8Array([3]), { meta: { next: [middle] } })
		).entry;
	});

	afterEach(async () => {
		for (const finish of cleanup.splice(0)) await finish();
		sinon.restore();
		await target.close();
		await source.close();
		await store.stop();
		await sourceStore.stop();
	});

	const join = (entries: Entry<Uint8Array>[], signal?: AbortSignal) =>
		target.join(entries, { signal });

	const prepareBatch = async () => {
		const identity = target.identity;
		await target.close();
		await target.open(store, identity, { nativeGraph: true });
		const root = await createEntry({
			store: sourceStore,
			identity: source.identity,
			data: new Uint8Array([4]),
			meta: { next: [] },
			deferStore: true,
		});
		const middle = await createEntry({
			store: sourceStore,
			identity: source.identity,
			data: new Uint8Array([5]),
			meta: { next: [] },
			deferStore: true,
		});
		return [root, middle];
	};

	const holdParent = (hash: string, cooperative = false) => {
		const entered = deferred<AbortSignal>();
		const release = deferred();
		const original = store.get.bind(store);
		let calls = 0;
		const stub = sinon.stub(store, "get").callsFake(async (cid, options) => {
			if (!options?.remote) return original(cid, options);
			if (cid !== hash) return sourceStore.get(cid);
			calls++;
			const signal = (options.remote as { signal: AbortSignal }).signal;
			entered.resolve(signal);
			if (cooperative) {
				await new Promise<void>((resolve, reject) => {
					const abort = () => reject(signal.reason);
					signal.addEventListener("abort", abort, { once: true });
					if (signal.aborted) abort();
					release.promise.then(resolve, reject).finally(() => {
						signal.removeEventListener("abort", abort);
					});
				});
			} else {
				await release.promise;
			}
			return sourceStore.get(cid);
		});
		cleanup.push(() => release.resolve());
		return { entered: entered.promise, release, stub, calls: () => calls };
	};

	it("forwards caller cancellation to a parent fetch and physically drains it", async () => {
		const gate = holdParent(root.hash);
		const controller = new AbortController();
		const reason = new Error("receive generation retired");
		let settled = false;
		const joining = outcome(join([middle], controller.signal)).then((value) => {
			settled = true;
			return value;
		});
		cleanup.push(() => joining);
		const signal = await gate.entered;
		controller.abort(reason);
		await turn();
		expect(signal.aborted).to.equal(true);
		expect(settled).to.equal(false);
		gate.release.resolve();
		expect((await joining).error).to.equal(reason);
		expect(await target.has(middle.hash)).to.equal(false);
	});

	it("does not admit a pre-aborted caller", async () => {
		const controller = new AbortController();
		const reason = new Error("already cancelled");
		controller.abort(reason);
		const plan = sinon.spy(target.entryIndex, "planJoin");
		expect((await outcome(join([root], controller.signal))).error).to.equal(
			reason,
		);
		expect(plan.callCount).to.equal(0);
		expect(await target.has(root.hash)).to.equal(false);
	});

	it("cancels a cooperative fetch promptly with the exact caller reason", async () => {
		const gate = holdParent(root.hash, true);
		const controller = new AbortController();
		const reason = new Error("receive cancelled while fetching");
		const joining = outcome(join([middle], controller.signal));
		cleanup.push(() => joining);
		await gate.entered;
		controller.abort(reason);
		expect((await joining).error).to.equal(reason);
		expect(await target.has(middle.hash)).to.equal(false);
	});

	it("still observes lower-log close while a caller signal remains active", async () => {
		const gate = holdParent(root.hash, true);
		const controller = new AbortController();
		const joining = outcome(join([middle], controller.signal));
		cleanup.push(() => joining);
		const signal = await gate.entered;
		await target.close();
		expect(signal.aborted).to.equal(true);
		expect(controller.signal.aborted).to.equal(false);
		expect((await joining).error).to.equal(signal.reason);
	});

	it("preserves a real fetch failure racing caller cancellation", async () => {
		const gate = holdParent(root.hash);
		const controller = new AbortController();
		const joining = outcome(join([middle], controller.signal));
		cleanup.push(() => joining);
		await gate.entered;
		controller.abort(new Error("caller left"));
		const failure = new Error("storage I/O failed");
		gate.release.reject(failure);
		expect((await joining).error).to.equal(failure);
	});

	it("drains an admitted mutation callback and keeps its committed prefix", async () => {
		const entered = deferred();
		const release = deferred();
		cleanup.push(() => release.resolve());
		const controller = new AbortController();
		const reason = new Error("cancel after commit");
		let settled = false;
		let callbackFinished = false;
		const joining = outcome(
			target.join([root, middle], {
				signal: controller.signal,
				onChange: async () => {
					entered.resolve();
					await release.promise;
					callbackFinished = true;
				},
			}),
		).then((value) => {
			settled = true;
			return value;
		});
		cleanup.push(() => joining);
		await entered.promise;
		controller.abort(reason);
		await turn();
		expect(settled).to.equal(false);
		expect(callbackFinished).to.equal(false);
		release.resolve();
		expect((await joining).error).to.equal(reason);
		expect(callbackFinished).to.equal(true);
		expect(await target.has(root.hash)).to.equal(true);
		expect(await target.has(middle.hash)).to.equal(false);
	});

	it("does not commit descendants after cancelling a deep missing ancestor", async () => {
		const gate = holdParent(root.hash);
		const controller = new AbortController();
		const reason = new Error("deep receive cancelled");
		const joining = outcome(join([head], controller.signal));
		cleanup.push(() => joining);
		await gate.entered;
		controller.abort(reason);
		gate.release.resolve();
		expect((await joining).error).to.equal(reason);
		expect(await target.has(root.hash)).to.equal(false);
		expect(await target.has(middle.hash)).to.equal(false);
		expect(await target.has(head.hash)).to.equal(false);
	});

	it("does not admit a prepared batch after cancelled planning drains", async () => {
		const [root, middle] = await prepareBatch();
		const entered = deferred();
		const release = deferred();
		cleanup.push(() => release.resolve());
		const original = target.entryIndex.planJoinBatch.bind(target.entryIndex);
		sinon
			.stub(target.entryIndex, "planJoinBatch")
			.callsFake(async (...args) => {
				const plan = await original(...args);
				entered.resolve();
				await release.promise;
				return plan;
			});
		const put = sinon.spy(target.entryIndex, "putAppendBatch");
		const controller = new AbortController();
		const reason = new Error("cancel during batch preparation");
		const joining = outcome(
			target.join([root, middle], {
				signal: controller.signal,
				__peerbitBatchIndependent: true,
			} as any),
		);
		cleanup.push(() => joining);
		await entered.promise;
		controller.abort(reason);
		release.resolve();
		expect((await joining).error).to.equal(reason);
		expect(put.callCount).to.equal(0);
		expect(target.length).to.equal(0);
	});

	it("drains an admitted prepared batch and its callback before rejecting", async () => {
		const [root, middle] = await prepareBatch();
		const entered = deferred();
		const release = deferred();
		cleanup.push(() => release.resolve());
		const put = sinon.spy(target.entryIndex, "putAppendBatch");
		const controller = new AbortController();
		const reason = new Error("cancel after batch commit");
		let settled = false;
		const joining = outcome(
			target.join([root, middle], {
				signal: controller.signal,
				__peerbitBatchIndependent: true,
				onChange: async () => {
					entered.resolve();
					await release.promise;
				},
			} as any),
		).then((value) => {
			settled = true;
			return value;
		});
		cleanup.push(() => joining);
		await entered.promise;
		expect(put.callCount).to.equal(1);
		controller.abort(reason);
		await turn();
		expect(settled).to.equal(false);
		release.resolve();
		expect((await joining).error).to.equal(reason);
		expect(await target.has(root.hash)).to.equal(true);
		expect(await target.has(middle.hash)).to.equal(true);
	});

	for (const recursive of [false, true]) {
		it(`cancels a borrowed ${recursive ? "ancestor" : "head"} join wait without cancelling its live owner`, async () => {
			const gate = holdParent(root.hash);
			const owner = outcome(join([middle]));
			cleanup.push(() => owner);
			const signal = await gate.entered;
			const controller = new AbortController();
			const reason = new Error("only the follower left");
			const follower = outcome(
				join([recursive ? head : middle], controller.signal),
			);
			cleanup.push(() => follower);
			await turn();
			controller.abort(reason);
			await turn();
			expect(signal.aborted).to.equal(false);
			let followerResult: Awaited<typeof follower> | undefined;
			void follower.then((value) => (followerResult = value));
			await turn();
			expect(followerResult?.error).to.equal(reason);
			gate.release.resolve();
			expect((await owner).error).to.equal(undefined);
			expect(await target.has(middle.hash)).to.equal(true);
		});

		it(`lets a live ${recursive ? "ancestor" : "head"} follower retry after its cancelled owner's physical join drains`, async () => {
			const gate = holdParent(root.hash);
			const controller = new AbortController();
			const reason = new Error("first receive left");
			const owner = outcome(join([middle], controller.signal));
			cleanup.push(() => owner);
			await gate.entered;
			const follower = outcome(join([recursive ? head : middle]));
			cleanup.push(() => follower);
			await turn();
			controller.abort(reason);
			await turn();
			expect(gate.calls()).to.equal(1);
			gate.release.resolve();
			expect((await owner).error).to.equal(reason);
			expect((await follower).error).to.equal(undefined);
			expect(await target.has(middle.hash)).to.equal(true);
			if (recursive) expect(await target.has(head.hash)).to.equal(true);
			expect(gate.calls()).to.equal(2);
		});
	}
});
