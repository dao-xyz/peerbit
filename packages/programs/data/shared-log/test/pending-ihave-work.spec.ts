import { randomBytes } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { RequestIPruneV2 } from "../src/exchange-heads.js";
import { EventStore } from "./utils/stores/event-store.js";

describe("pending IHave callback work", () => {
	it("bounds active callbacks, coalesces duplicate notifications, and retries overflow", async () => {
		const session = await TestSession.disconnected(1);
		try {
			const store = await session.peers[0].open(new EventStore<string, any>(), {
				args: { replicate: false },
			});
			const entries: Array<Awaited<ReturnType<typeof store.add>>["entry"]> = [];
			for (let index = 0; index < 5; index++) {
				entries.push((await store.add(`pending-ihave-${index}`)).entry);
			}

			const log = store.log as any;
			const synchronizerEntryAdded = sinon
				.stub(log.syncronizer, "onEntryAdded")
				.callsFake(() => {});
			const releases = entries.map(() => pDefer<void>());
			let callbacksStarted = 0;
			const peer = "pending-ihave-requester";

			try {
				for (let index = 0; index < entries.length; index++) {
					const entry = entries[index]!;
					let pending: any;
					pending = {
						requesting: new Set<string>(),
						requestIdsByPeer: new Map(),
						resetTimeout: () => {},
						clear: () => {},
						callback: async () => {
							callbacksStarted++;
							await releases[index]!.promise;
						},
					};
					expect(log.addPendingIHaveRequester(pending, peer)).to.be.true;
					log._pendingIHave.set(entry.hash, pending);
				}

				for (const entry of entries.slice(0, 4)) {
					log.onEntryAdded(entry);
				}
				await waitForResolved(() => expect(callbacksStarted).to.equal(4));
				expect(log._pendingIHaveActiveCallbacks).to.equal(4);
				expect(log._pendingIHaveCallbacks.size).to.equal(4);

				// Re-observing an admitted hash must not start the same callback twice.
				log.onEntryAdded(entries[0]);
				// A fifth hash is retained instead of joining an unbounded work queue.
				log.onEntryAdded(entries[4]);
				await new Promise<void>((resolve) => setTimeout(resolve, 0));
				expect(callbacksStarted).to.equal(4);
				expect(log._pendingIHave.get(entries[4].hash).callbackActive).to.not.be
					.true;

				releases[0]!.resolve();
				await waitForResolved(() =>
					expect(log._pendingIHaveActiveCallbacks).to.equal(3),
				);

				// A later notification can retry the retained overflow item.
				log.onEntryAdded(entries[4]);
				await waitForResolved(() => expect(callbacksStarted).to.equal(5));
				expect(log._pendingIHaveActiveCallbacks).to.equal(4);

				for (const release of releases) {
					release.resolve();
				}
				await waitForResolved(() =>
					expect(log._pendingIHaveCallbacks.size).to.equal(0),
				);
				expect(log._pendingIHaveActiveCallbacks).to.equal(0);
				expect(log._pendingIHave.size).to.equal(0);
				expect(log._pendingIHaveCountByPeer.size).to.equal(0);
				expect(log._pendingIHaveRequesterCount).to.equal(0);
			} finally {
				for (const release of releases) {
					release.resolve();
				}
				synchronizerEntryAdded.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("releases requester accounting when retained callback work expires", async () => {
		const session = await TestSession.disconnected(1);
		try {
			const store = await session.peers[0].open(new EventStore<string, any>(), {
				args: { replicate: false },
			});
			const { entry } = await store.add("pending-ihave-expiry");
			const log = store.log as any;
			const peer = "expiring-pending-ihave-requester";
			let pending: any;
			pending = {
				requesting: new Set<string>(),
				requestIdsByPeer: new Map(),
				resetTimeout: () => log.resetPendingIHaveTimeout(pending),
				clear: () => log.clearPendingIHaveTimeout(pending),
				callback: async () => {},
				expiresAt: Date.now() - 1,
			};
			expect(log.addPendingIHaveRequester(pending, peer)).to.be.true;
			log._pendingIHave.set(entry.hash, pending);

			log.expirePendingIHaves();

			expect(log._pendingIHave.size).to.equal(0);
			expect(log._pendingIHaveCountByPeer.size).to.equal(0);
			expect(log._pendingIHaveRequesterCount).to.equal(0);
		} finally {
			await session.stop();
		}
	});

	it("aborts callback work at its deadline and releases all accounting", async () => {
		const session = await TestSession.disconnected(1);
		try {
			const store = await session.peers[0].open(new EventStore<string, any>(), {
				args: { replicate: false },
			});
			const { entry } = await store.add("pending-ihave-callback-deadline");
			const log = store.log as any;
			const synchronizerEntryAdded = sinon
				.stub(log.syncronizer, "onEntryAdded")
				.callsFake(() => {});
			const clock = sinon.useFakeTimers({
				now: 1_000,
				shouldClearNativeTimers: true,
			});
			const callbackStarted = pDefer<void>();
			const peer = "deadline-pending-ihave-requester";
			let callbackSignal: AbortSignal | undefined;
			let pending: any;
			pending = {
				requesting: new Set<string>(),
				requestIdsByPeer: new Map(),
				resetTimeout: () => {},
				clear: () => {},
				callback: async (_entry: unknown, work: { signal: AbortSignal }) => {
					callbackSignal = work.signal;
					callbackStarted.resolve();
					if (work.signal.aborted) {
						return;
					}
					await new Promise<void>((resolve) => {
						work.signal.addEventListener("abort", () => resolve(), {
							once: true,
						});
					});
				},
			};

			try {
				expect(log.addPendingIHaveRequester(pending, peer)).to.be.true;
				log._pendingIHave.set(entry.hash, pending);

				log.onEntryAdded(entry);
				await callbackStarted.promise;

				expect(callbackSignal?.aborted).to.be.false;
				expect(log._pendingIHaveActiveCallbacks).to.equal(1);
				expect(log._pendingIHaveCallbacks.size).to.equal(1);
				expect(log._pendingIHave.size).to.equal(1);
				expect(log._pendingIHaveCountByPeer.get(peer)).to.equal(1);
				expect(log._pendingIHaveRequesterCount).to.equal(1);

				const [callbackWork] = [
					...log._pendingIHaveCallbacks,
				] as Promise<void>[];
				await clock.tickAsync(5_000);
				await callbackWork;
				await clock.tickAsync(0);

				expect(callbackSignal?.aborted).to.be.true;
				expect(log._pendingIHaveActiveCallbacks).to.equal(0);
				expect(log._pendingIHaveCallbacks.size).to.equal(0);
				expect(log._pendingIHave.size).to.equal(0);
				expect(log._pendingIHaveCountByPeer.size).to.equal(0);
				expect(log._pendingIHaveRequesterCount).to.equal(0);
			} finally {
				clock.restore();
				synchronizerEntryAdded.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("returns a pending-IHave callback at its deadline but retains its slot until a stuck leader wait settles", async () => {
		const session = await TestSession.disconnected(2);
		try {
			const store = await session.peers[0].open(new EventStore<string, any>(), {
				args: { replicate: false },
			});
			const { entry } = await store.add("pending-ihave-stuck-leader-wait");
			const log = store.log as any;
			const requester = session.peers[1].identity.publicKey;
			const backbonePlan = sinon
				.stub(log, "planCurrentNativeBackboneRequestPruneLeaderHints")
				.resolves(undefined);
			const nativeMetadata = sinon
				.stub(log, "getNativeLogEntryMetadataBatch")
				.returns([null]);
			const hasMany = sinon.stub(log.log.blocks, "hasMany").resolves([false]);
			const nativePlan = sinon
				.stub(log, "planCurrentNativeRequestPruneLeaderHints")
				.resolves({
					localLeaderHashes: new Set(),
					replicaCounts: new Map(),
					peerHistoryGids: [],
					peerHistoryRemovedHashes: new Set(),
				});
			const releaseLeaderWait = pDefer<void>();
			const leaderWaitEntered = pDefer<void>();
			const waitForReplicators = sinon
				.stub(log, "_waitForEntryReplicators")
				.callsFake(async () => {
					leaderWaitEntered.resolve();
					await releaseLeaderWait.promise;
					return false;
				});
			const clock = sinon.useFakeTimers({
				now: 1_000,
				shouldClearNativeTimers: true,
			});

			try {
				await store.log.onMessage(
					new RequestIPruneV2({
						requests: [{ hash: entry.hash, requestId: randomBytes(32) }],
					}),
					{ from: requester } as any,
				);
				const pending = log._pendingIHave.get(entry.hash);
				expect(pending).to.exist;

				expect(log.runPendingIHaveCallback(pending, entry)).to.be.true;
				await leaderWaitEntered.promise;
				expect(log._pendingIHaveActiveCallbacks).to.equal(1);
				expect(log._pendingIHaveCallbacks.size).to.equal(1);

				await clock.tickAsync(5_000);
				await Promise.all([...log._pendingIHaveCallbacks]);
				await clock.tickAsync(0);

				expect(log._pendingIHaveCallbacks.size).to.equal(0);
				expect(log._pendingIHave.size).to.equal(0);
				expect(log._pendingIHaveCountByPeer.size).to.equal(0);
				expect(log._pendingIHaveRequesterCount).to.equal(0);
				expect(log._pendingIHaveActiveCallbacks).to.equal(1);

				releaseLeaderWait.resolve();
				await clock.tickAsync(0);
				expect(log._pendingIHaveActiveCallbacks).to.equal(0);
			} finally {
				releaseLeaderWait.resolve();
				clock.restore();
				waitForReplicators.restore();
				nativePlan.restore();
				hasMany.restore();
				nativeMetadata.restore();
				backbonePlan.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("retains callback capacity for detached checks and retires old slots on reopen", async () => {
		const session = await TestSession.disconnected(1);
		try {
			const store = await session.peers[0].open(new EventStore<string, any>(), {
				args: { replicate: false },
			});
			const entries: Array<Awaited<ReturnType<typeof store.add>>["entry"]> = [];
			for (let index = 0; index < 5; index++) {
				entries.push((await store.add(`pending-detached-${index}`)).entry);
			}
			const log = store.log as any;
			const peer = "detached-pending-ihave-requester";
			const detachedReleases: ReturnType<typeof pDefer<void>>[] = [];
			let callbacksStarted = 0;
			const installPending = (
				entry: (typeof entries)[number],
				callback: (entry: unknown, work: any) => Promise<void>,
			) => {
				const pending = {
					requesting: new Set<string>(),
					requestIdsByPeer: new Map(),
					resetTimeout: () => {},
					clear: () => {},
					callback,
				};
				expect(log.addPendingIHaveRequester(pending, peer)).to.be.true;
				log._pendingIHave.set(entry.hash, pending);
				return pending;
			};
			const clock = sinon.useFakeTimers({
				now: 1_000,
				shouldClearNativeTimers: true,
			});
			let clockRestored = false;
			let freshRelease: ReturnType<typeof pDefer<void>> | undefined;

			try {
				const oldAdmission = log._pendingIHaveCallbackWorkAdmission;
				for (const entry of entries.slice(0, 4)) {
					const release = pDefer<void>();
					detachedReleases.push(release);
					const pending = installPending(entry, async (_entry, work) => {
						callbacksStarted++;
						work.settlingWork.track(release.promise);
						if (work.signal.aborted) {
							return;
						}
						await new Promise<void>((resolve) => {
							work.signal.addEventListener("abort", () => resolve(), {
								once: true,
							});
						});
					});
					expect(log.runPendingIHaveCallback(pending, entry)).to.be.true;
				}
				expect(callbacksStarted).to.equal(4);

				await clock.tickAsync(5_000);
				await Promise.all([...log._pendingIHaveCallbacks]);
				await clock.tickAsync(0);
				expect(log._pendingIHaveCallbacks.size).to.equal(0);
				expect(oldAdmission.active).to.equal(4);
				expect(log._pendingIHaveActiveCallbacks).to.equal(4);

				const overflow = installPending(entries[4], async () => {
					callbacksStarted++;
				});
				expect(log.runPendingIHaveCallback(overflow, entries[4])).to.be.false;
				expect(callbacksStarted).to.equal(4);
				expect(oldAdmission.active).to.equal(4);

				clock.restore();
				clockRestored = true;
				await store.close();
				await session.peers[0].open(store, {
					args: { replicate: false },
				});

				const freshAdmission = log._pendingIHaveCallbackWorkAdmission;
				expect(freshAdmission).to.not.equal(oldAdmission);
				expect(freshAdmission.active).to.equal(0);
				expect(log._pendingIHaveActiveCallbacks).to.equal(0);

				freshRelease = pDefer<void>();
				const freshPending = installPending(
					entries[4],
					async (_entry, work) => {
						work.settlingWork.track(freshRelease!.promise);
					},
				);
				expect(log.runPendingIHaveCallback(freshPending, entries[4])).to.be
					.true;
				await waitForResolved(() =>
					expect(log._pendingIHaveCallbacks.size).to.equal(0),
				);
				expect(freshAdmission.active).to.equal(1);
				expect(log._pendingIHaveActiveCallbacks).to.equal(1);

				for (const release of detachedReleases) {
					release.resolve();
				}
				await waitForResolved(() => expect(oldAdmission.active).to.equal(0));
				expect(freshAdmission.active).to.equal(1);
				expect(log._pendingIHaveActiveCallbacks).to.equal(1);

				freshRelease.resolve();
				await waitForResolved(() =>
					expect(log._pendingIHaveActiveCallbacks).to.equal(0),
				);
			} finally {
				for (const release of detachedReleases) {
					release.resolve();
				}
				freshRelease?.resolve();
				if (!clockRestored) {
					clock.restore();
				}
			}
		} finally {
			await session.stop();
		}
	});
});
