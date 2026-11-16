import { createStore } from "@peerbit/any-store";
import { Ed25519Keypair } from "@peerbit/crypto";
import { disable, enable } from "@peerbit/logger";
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

	it("announces every block of one write larger than the pending bound", async () => {
		// Distinct blocks, so every CID needs its own announcement.
		const blocks = Array.from({ length: MAX_PENDING + 904 }, (_, index) => {
			const bytes = new Uint8Array(4);
			new DataView(bytes.buffer).setUint32(0, index);
			return bytes;
		});
		for (const aggregate of [true, false]) {
			const announced = new Set<string>();
			const hookCalls: number[] = [];
			const remote = await createRemote({
				onPut: (cid) => {
					hookCalls.push(1);
					announced.add(cid);
				},
				...(aggregate
					? {
							onPutMany: (cids: string[]) => {
								hookCalls.push(cids.length);
								for (const cid of cids) announced.add(cid);
							},
						}
					: {}),
			});

			const cids = await remote.putMany(blocks);
			await remote.waitForStoredNotifications();
			expect(new Set(cids).size).equal(blocks.length);
			expect(
				cids.filter((cid) => !announced.has(cid)),
				aggregate ? "aggregate hook" : "per-CID hook",
			).deep.equal([]);
			// One aggregate call, or one call per CID.
			expect(hookCalls).deep.equal(
				aggregate ? [blocks.length] : blocks.map(() => 1),
			);
			expect(remote.getStoredNotificationTelemetry()).to.deep.include({
				pending: 0,
				running: 0,
				admitted: blocks.length,
				dropped: 0,
			});
			await remote.stop();
		}
	});

	it("drops whole writes at the pending bound and admits again once it drains", async () => {
		const release = held();
		const announced: string[] = [];
		const remote = await createRemote({
			onPut: async (cid) => {
				announced.push(cid);
				await release.promise;
			},
		});
		const { maxPending, concurrency } =
			remote.getStoredNotificationTelemetry().limits;

		// Arrives below the bound, so it is admitted whole although it fills the
		// queue past it.
		const first = Array.from(
			{ length: maxPending + concurrency + 1 },
			(_, index) => `bound-${index}`,
		);
		remote.notifyStoredManyDeferred(first);
		await drainMicrotasks();
		expect(remote.getStoredNotificationTelemetry()).to.deep.include({
			running: concurrency,
			pending: maxPending + 1,
			admitted: first.length,
			dropped: 0,
		});

		// Arrives at the bound: dropped whole, not cut off part-way.
		await remote.putKnownMany([
			["late-a", new Uint8Array([1])],
			["late-b", new Uint8Array([2])],
		]);
		remote.notifyStoredDeferred("late-c");
		expect(remote.getStoredNotificationTelemetry()).to.deep.include({
			pending: maxPending + 1,
			admitted: first.length,
			dropped: 3,
		});

		release.resolve();
		await remote.waitForStoredNotifications();
		remote.notifyStoredDeferred("after-drain");
		await remote.waitForStoredNotifications();
		expect(announced).deep.equal([...first, "after-drain"]);
		expect(remote.getStoredNotificationTelemetry()).to.deep.include({
			pending: 0,
			running: 0,
			admitted: first.length + 1,
			dropped: 3,
		});
	});

	it("logs the first overflow of each store generation in the warn scope", async () => {
		const namespace = "peerbit:transport:blocks:stored-notifications";
		const lines: string[] = [];
		const write = process.stderr.write;
		process.stderr.write = function (
			this: unknown,
			chunk: unknown,
			...rest: unknown[]
		) {
			if (String(chunk).includes("notification queue is full")) {
				lines.push(String(chunk));
				return true;
			}
			return (write as (...args: unknown[]) => boolean).call(
				process.stderr,
				chunk,
				...rest,
			);
		} as typeof process.stderr.write;
		enable(`${namespace}*`);
		try {
			let gate = held();
			const remote = await createRemote({ onPut: () => gate.promise });
			const { maxPending, concurrency } =
				remote.getStoredNotificationTelemetry().limits;
			// Fills the queue, then drops two later writes while it is full.
			const overflow = async (prefix: string) => {
				remote.notifyStoredManyDeferred(
					Array.from(
						{ length: maxPending + concurrency },
						(_, index) => `${prefix}-${index}`,
					),
				);
				await drainMicrotasks();
				remote.notifyStoredDeferred(`${prefix}-late-1`);
				remote.notifyStoredDeferred(`${prefix}-late-2`);
			};
			const drain = async () => {
				gate.resolve();
				await remote.waitForStoredNotifications();
				gate = held();
			};
			const scopes = () =>
				lines.map((line) =>
					line.includes(`${namespace}:warn`) ? "warn" : "debug",
				);

			await overflow("first");
			expect(scopes(), "one line per overflow").deep.equal(["warn"]);
			await drain();
			await overflow("again");
			expect(scopes()).deep.equal(["warn", "debug"]);
			await drain();

			// A restarted store is a new generation.
			await remote.stop();
			await remote.start();
			await overflow("restarted");
			expect(scopes()).deep.equal(["warn", "debug", "warn"]);
			expect(remote.getStoredNotificationTelemetry()).to.deep.include({
				dropped: 2,
			});
		} finally {
			disable();
			if (process.env.DEBUG) enable(process.env.DEBUG);
			process.stderr.write = write;
		}
	});

	it("stays bounded while announcements are stuck", async () => {
		const never = held();
		const remote = await createRemote({ onPut: () => never.promise });
		const { maxPending, concurrency } =
			remote.getStoredNotificationTelemetry().limits;
		expect(maxPending).equal(MAX_PENDING);
		expect(concurrency).equal(8);

		const largest = maxPending + 904;
		remote.notifyStoredManyDeferred(
			Array.from({ length: largest }, (_, index) => `stuck-${index}`),
		);
		await drainMicrotasks();
		expect(remote.getStoredNotificationTelemetry()).to.deep.include({
			running: concurrency,
			pending: largest - concurrency,
			admitted: largest,
			dropped: 0,
		});

		// Writes still resolve, and while the bound is reached their CIDs are
		// dropped, so no more than `maxPending - 1` plus one write ever wait.
		for (let round = 0; round < 3; round++) {
			await remote.putKnownMany(
				Array.from(
					{ length: concurrency + 1 },
					(_, index) =>
						[`late-${round}-${index}`, new Uint8Array([index])] as const,
				),
			);
		}
		expect(remote.getStoredNotificationTelemetry()).to.deep.include({
			running: concurrency,
			pending: largest - concurrency,
			dropped: 3 * (concurrency + 1),
		});
	});

	it("tells the aggregate hook whether its CIDs include a multi-block write", async () => {
		const started = pDefer<void>();
		const release = held();
		const calls: Array<{ cids: string[]; batch?: boolean }> = [];
		const remote = await createRemote({
			onPut: (cid) => {
				calls.push({ cids: [cid] });
			},
			onPutMany: async (cids, options) => {
				calls.push({ cids, batch: options?.batch });
				if (calls.length === 1) {
					started.resolve();
					await release.promise;
				}
			},
		});

		// A write of several blocks on an idle store.
		const batch = await remote.putMany([
			new Uint8Array([21]),
			new Uint8Array([22]),
		]);
		await started.promise;
		// Writes of one block each that queue behind the running call reach the
		// aggregate hook together, but they are not a batch.
		const singles = [
			await remote.put(new Uint8Array([23])),
			await remote.putKnown("single-known", new Uint8Array([24])),
		];
		remote.notifyStoredDeferred("single-deferred");
		release.resolve();
		await remote.waitForStoredNotifications();

		expect(calls).deep.equal([
			{ cids: batch, batch: true },
			{ cids: [...singles, "single-deferred"], batch: false },
		]);
	});

	it("counts a write that repeats waiting blocks as a batch", async () => {
		const started = pDefer<void>();
		const release = held();
		const calls: Array<{ cids: string[]; batch?: boolean }> = [];
		const remote = await createRemote({
			onPut: async (cid) => {
				calls.push({ cids: [cid] });
				started.resolve();
				await release.promise;
			},
			onPutMany: (cids, options) => {
				calls.push({ cids, batch: options?.batch });
			},
		});

		const running = await remote.put(new Uint8Array([31]));
		await started.promise;
		const waiting = [
			await remote.put(new Uint8Array([32])),
			await remote.put(new Uint8Array([33])),
		];
		// One write stores both waiting blocks again; its CIDs coalesce into the
		// waiting ones, and the call that takes them counts as a batch.
		expect(
			await remote.putMany([new Uint8Array([32]), new Uint8Array([33])]),
		).deep.equal(waiting);
		release.resolve();
		await remote.waitForStoredNotifications();

		expect(calls).deep.equal([
			{ cids: [running] },
			{ cids: waiting, batch: true },
		]);
		expect(remote.getStoredNotificationTelemetry()).to.deep.include({
			admitted: 3,
			coalesced: 2,
		});
	});

	it("keeps a batch whose other blocks are running a batch for the aggregate hook", async () => {
		const started = pDefer<void>();
		const release = held();
		const calls: Array<{ cids: string[]; batch?: boolean }> = [];
		const remote = await createRemote({
			onPut: async (cid) => {
				calls.push({ cids: [cid] });
				started.resolve();
				await release.promise;
			},
			onPutMany: (cids, options) => {
				calls.push({ cids, batch: options?.batch });
			},
		});

		const running = await remote.put(new Uint8Array([41]));
		await started.promise;
		// One write stores the running block again with a new one. Only the new
		// block is left to announce, and the aggregate hook still learns that it
		// came from a batch rather than from a write of one block.
		const [repeated, added] = await remote.putMany([
			new Uint8Array([41]),
			new Uint8Array([42]),
		]);
		expect(repeated).equal(running);
		release.resolve();
		await remote.waitForStoredNotifications();

		expect(calls).deep.equal([
			{ cids: [running] },
			{ cids: [added], batch: true },
		]);
		expect(remote.getStoredNotificationTelemetry()).to.deep.include({
			admitted: 2,
			coalesced: 1,
		});
	});
});
