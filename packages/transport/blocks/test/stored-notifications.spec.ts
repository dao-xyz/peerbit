import { createStore } from "@peerbit/any-store";
import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";
import pDefer from "p-defer";
import { AnyBlockStore } from "../src/any-blockstore.js";
import {
	RemoteBlocks,
	type StoredNotificationHookOptions,
} from "../src/remote.js";

// Once a hook call has started, a write that does not wait for it finishes
// within microtasks. setImmediate runs after the microtask queue is empty, so
// it orders "the write resolved" against "the hook is still pending" without
// a timer.
const drainMicrotasks = () =>
	new Promise<void>((resolve) => setImmediate(resolve));

type Hooks = {
	onPut?: (
		cid: string,
		options?: StoredNotificationHookOptions,
	) => Promise<void> | void;
	onPutMany?: (
		cids: string[],
		options?: StoredNotificationHookOptions,
	) => Promise<void> | void;
};

describe("@peerbit/blocks — stored-block notifications", () => {
	// The documented number of CIDs that may wait for a hook call.
	const MAX_PENDING = 4096;
	let remotes: RemoteBlocks[] = [];
	// Hooks below that ignore their signal are released here, so stop() can
	// settle them after a failed assertion too.
	let cleanups: Array<() => void> = [];

	const createRemote = async (hooks: Hooks) => {
		const key = await Ed25519Keypair.create();
		const remote = new RemoteBlocks({
			local: new AnyBlockStore(createStore()),
			publicKey: key.publicKey,
			publish: async () => undefined,
			waitFor: async () => [],
			...hooks,
		});
		remotes.push(remote);
		await remote.start();
		return remote;
	};

	const held = () => {
		const release = pDefer<void>();
		cleanups.push(release.resolve);
		return release;
	};

	afterEach(async () => {
		for (const cleanup of cleanups) cleanup();
		cleanups = [];
		for (const remote of remotes.reverse()) {
			await remote.stop().catch((): void => undefined);
		}
		remotes = [];
	});

	it("resolves every write while its announcement never settles", async () => {
		const writes: Array<[string, (remote: RemoteBlocks) => Promise<unknown>]> =
			[
				["put", (remote) => remote.put(new Uint8Array([1]))],
				[
					"putKnown",
					(remote) => remote.putKnown("known-single", new Uint8Array([2])),
				],
				[
					"putMany",
					(remote) =>
						remote.putMany([new Uint8Array([3]), new Uint8Array([4])]),
				],
				[
					"putKnownMany",
					(remote) =>
						remote.putKnownMany([
							["known-a", new Uint8Array([5])],
							["known-b", new Uint8Array([6])],
						]),
				],
			];
		for (const [name, write] of writes) {
			const started = pDefer<void>();
			const never = held();
			const announce = () => {
				started.resolve();
				return never.promise;
			};
			const remote = await createRemote({
				onPut: announce,
				onPutMany: announce,
			});

			let resolved = false;
			const writing = write(remote).then(() => {
				resolved = true;
			});
			await started.promise;
			await drainMicrotasks();
			expect(resolved, `${name} resolved before its announcement`).equal(true);
			await writing;
		}
	});

	it("announces a block after its write resolves", async () => {
		const started = pDefer<void>();
		const release = held();
		const announced: string[] = [];
		let settledAnnouncements = 0;
		const remote = await createRemote({
			onPut: async (cid) => {
				announced.push(cid);
				started.resolve();
				await release.promise;
				settledAnnouncements += 1;
			},
		});

		let resolved = false;
		const writing = remote.put(new Uint8Array([7])).then((cid) => {
			resolved = true;
			return cid;
		});
		await started.promise;
		await drainMicrotasks();
		expect(resolved).equal(true);
		const cid = await writing;
		expect(announced).deep.equal([cid]);

		let notified = false;
		const notifying = remote.waitForStoredNotifications().then(() => {
			notified = true;
		});
		await drainMicrotasks();
		expect(notified).equal(false);
		expect(settledAnnouncements).equal(0);

		release.resolve();
		await notifying;
		expect(settledAnnouncements).equal(1);
	});

	it("batches writes that arrive while an aggregate announcement runs", async () => {
		const started = pDefer<void>();
		const release = held();
		const batches: string[][] = [];
		const remote = await createRemote({
			onPutMany: async (cids) => {
				batches.push(cids);
				started.resolve();
				await release.promise;
			},
		});

		let resolved = false;
		const first = remote.put(new Uint8Array([11])).then((cid) => {
			resolved = true;
			return cid;
		});
		await started.promise;
		await drainMicrotasks();
		expect(resolved).equal(true);
		const later = [
			await remote.put(new Uint8Array([12])),
			await remote.put(new Uint8Array([13])),
		];
		// One aggregate call runs at a time; the later CIDs wait for the next.
		expect(batches).deep.equal([[await first]]);

		release.resolve();
		await remote.waitForStoredNotifications();
		expect(batches).deep.equal([[await first], later]);
	});

	it("aborts a pending announcement when the store stops", async () => {
		const started = pDefer<AbortSignal | undefined>();
		const cleanup = held();
		const remote = await createRemote({
			onPut: (_cid, options) => {
				started.resolve(options?.signal);
				// Like a bootstrap dial that never answers: it ends only when aborted.
				return new Promise<void>((resolve) => {
					options?.signal?.addEventListener("abort", () => resolve(), {
						once: true,
					});
					void cleanup.promise.then(resolve);
				});
			},
		});

		let resolved = false;
		const writing = remote.put(new Uint8Array([8])).then(() => {
			resolved = true;
		});
		const signal = await started.promise;
		await drainMicrotasks();
		expect(resolved).equal(true);
		expect(signal, "the hook receives the store's lifecycle signal").to.exist;
		expect(signal!.aborted).equal(false);

		await remote.stop();
		expect(signal!.aborted).equal(true);
		expect(remote.status).equal("closed");
		await writing;
	});

	it("discards waiting announcements and aborts running ones on stop", async () => {
		const signals: AbortSignal[] = [];
		const eightStarted = pDefer<void>();
		const cleanup = held();
		let calls = 0;
		const remote = await createRemote({
			onPut: (_cid, options) => {
				calls += 1;
				if (options?.signal) signals.push(options.signal);
				if (calls === 8) eightStarted.resolve();
				return new Promise<void>((resolve) => {
					options?.signal?.addEventListener("abort", () => resolve(), {
						once: true,
					});
					void cleanup.promise.then(resolve);
				});
			},
		});

		remote.notifyStoredManyDeferred(
			Array.from({ length: 32 }, (_, index) => `queued-${index}`),
		);
		await eightStarted.promise;
		await drainMicrotasks();
		expect(calls).equal(8);
		expect(signals).to.have.length(8);

		await remote.stop();
		expect(signals.every((signal) => signal.aborted)).equal(true);
		expect(calls).equal(8);
		expect(remote.getStoredNotificationTelemetry()).to.deep.include({
			pending: 0,
			running: 0,
			abandoned: 24,
		});
		remote.notifyStoredDeferred("after-stop");
		expect(calls).equal(8);
	});

	it("coalesces a CID while its announcement waits or runs", async () => {
		const started = pDefer<void>();
		const release = held();
		const announced: string[] = [];
		const remote = await createRemote({
			onPut: async (cid) => {
				announced.push(cid);
				started.resolve();
				await release.promise;
			},
		});
		const bytes = new Uint8Array([9]);

		let resolved = false;
		const first = remote.put(bytes).then((cid) => {
			resolved = true;
			return cid;
		});
		await started.promise;
		await drainMicrotasks();
		expect(resolved).equal(true);
		const cid = await first;

		expect(await remote.put(bytes)).equal(cid);
		remote.notifyStoredManyDeferred([cid, "other", "other"]);
		await drainMicrotasks();
		expect(announced).deep.equal([cid, "other"]);
		expect(remote.getStoredNotificationTelemetry()).to.deep.include({
			admitted: 2,
			coalesced: 3,
		});

		release.resolve();
		await remote.waitForStoredNotifications();
		expect(announced).deep.equal([cid, "other"]);

		// Coalescing lasts only while a CID waits or runs.
		await remote.put(bytes);
		await remote.waitForStoredNotifications();
		expect(announced).deep.equal([cid, "other", cid]);
	});

	it("keeps at most the pending bound and drops the excess", async () => {
		let calls = 0;
		const remote = await createRemote({
			onPut: () => {
				calls += 1;
			},
		});
		const cids = Array.from(
			{ length: MAX_PENDING + 904 },
			(_, index) => `overflow-${index}`,
		);

		remote.notifyStoredManyDeferred(cids);
		// These hooks settle at once, so the admitted CIDs run within microtasks
		// and stop() has nothing left to discard.
		await drainMicrotasks();
		await remote.stop();
		expect(calls).equal(MAX_PENDING);
		expect(remote.getStoredNotificationTelemetry()).to.deep.include({
			pending: 0,
			running: 0,
			admitted: MAX_PENDING,
			dropped: 904,
			abandoned: 0,
		});
	});

	it("admits again once a full queue drains", async () => {
		let calls = 0;
		const remote = await createRemote({
			onPut: () => {
				calls += 1;
			},
		});

		remote.notifyStoredManyDeferred(
			Array.from({ length: MAX_PENDING + 1 }, (_, index) => `refill-${index}`),
		);
		await drainMicrotasks();
		remote.notifyStoredDeferred("after-overflow");
		await remote.waitForStoredNotifications();
		expect(calls).equal(MAX_PENDING + 1);
		expect(remote.getStoredNotificationTelemetry()).to.deep.include({
			admitted: MAX_PENDING + 1,
			dropped: 1,
		});
	});

	it("stays bounded while announcements are stuck", async () => {
		const never = held();
		const remote = await createRemote({ onPut: () => never.promise });
		const { maxPending, concurrency } =
			remote.getStoredNotificationTelemetry().limits;
		expect(maxPending).equal(MAX_PENDING);
		expect(concurrency).equal(8);

		remote.notifyStoredManyDeferred(
			Array.from({ length: maxPending + 904 }, (_, index) => `stuck-${index}`),
		);
		await drainMicrotasks();
		expect(remote.getStoredNotificationTelemetry()).to.deep.include({
			running: concurrency,
			pending: maxPending - concurrency,
			admitted: maxPending,
			dropped: 904,
		});

		// Writes still resolve, and their CIDs take the free slots or are dropped.
		await remote.putKnownMany(
			Array.from(
				{ length: concurrency + 1 },
				(_, index) => [`late-${index}`, new Uint8Array([index])] as const,
			),
		);
		expect(remote.getStoredNotificationTelemetry()).to.deep.include({
			pending: maxPending,
			dropped: 905,
		});
	});
});
