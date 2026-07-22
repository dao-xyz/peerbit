import { Entry } from "@peerbit/log";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer, { type DeferredPromise } from "p-defer";
import sinon from "sinon";
import { v4 as uuid } from "uuid";
import {
	EntryWithRefs,
	ExchangeHeadsMessage,
	RequestIPrune,
	ResponseIPrune,
	SyncCapabilitiesMessage,
} from "../src/exchange-heads.js";
import { createReplicationDomainHash } from "../src/replication-domain-hash.js";
import { AllReplicatingSegmentsMessage } from "../src/replication.js";
import {
	ConfirmEntriesMessage,
	RequestMaybeSync,
	SimpleSyncronizer,
} from "../src/sync/simple.js";
import { EventStore } from "./utils/stores/event-store.js";

const setup = {
	domain: createReplicationDomainHash("u32"),
	type: "u32" as const,
	syncronizer: SimpleSyncronizer,
	name: "u32-simple-receive-admission",
};

const exchange = (entry: Entry<any>, gidRefrences: string[] = []) =>
	new ExchangeHeadsMessage({
		heads: [new EntryWithRefs({ entry, gidRefrences })],
	});

const makeParentUnavailable = (parentHash: string) => {
	const originalFromMultihash = Entry.fromMultihash;
	return sinon.stub(Entry, "fromMultihash").callsFake((...args: any[]) => {
		if (args[1] === parentHash) {
			throw Object.assign(new Error("parent intentionally unavailable"), {
				name: "AbortError",
			});
		}
		return (originalFromMultihash as any)(...args);
	});
};

describe("receive admission", () => {
	it("rejects metadata messages after unsubscribe cleanup starts", async () => {
		const session = await TestSession.disconnected(2);
		const cleanupEntered = pDefer<void>();
		const releaseCleanup = pDefer<void>();
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const sharedLog = target.log as any;
			const sourceKey = source.node.identity.publicKey;
			const sourceHash = sourceKey.hashcode();
			const originalDisconnected =
				sharedLog.syncronizer.onPeerDisconnected.bind(sharedLog.syncronizer);
			const disconnected = sinon
				.stub(sharedLog.syncronizer, "onPeerDisconnected")
				.callsFake(async (...args: unknown[]) => {
					const peerHash = args[0] as string;
					if (peerHash === sourceHash) {
						cleanupEntered.resolve();
						await releaseCleanup.promise;
					}
					return originalDisconnected(peerHash);
				});

			try {
				const unsubscribe = sharedLog._onUnsubscription({
					detail: { from: sourceKey, topics: [target.log.topic] },
				});
				await cleanupEntered.promise;

				await Promise.all([
					target.log.onMessage(
						new ConfirmEntriesMessage({ hashes: ["late-confirm"] }),
						{ from: sourceKey } as any,
					),
					target.log.onMessage(new SyncCapabilitiesMessage(), {
						from: sourceKey,
					} as any),
				]);

				expect(sharedLog._entryKnownPeers.has("late-confirm")).to.be.false;
				expect(sharedLog._entryKnownPeerObservedAt.has("late-confirm")).to.be
					.false;
				expect(sharedLog._peerSyncCapabilities.has(sourceHash)).to.be.false;
				expect(sharedLog._replicatorLastActivityAt.has(sourceHash)).to.be.false;
				expect(sharedLog._activeReceiveHandlersByPeer.has(sourceHash)).to.be
					.false;

				releaseCleanup.resolve();
				await unsubscribe;
				expect(sharedLog._receiveCleanupGateByPeer.has(sourceHash)).to.be.false;
			} finally {
				releaseCleanup.resolve();
				disconnected.restore();
			}
		} finally {
			releaseCleanup.resolve();
			await session.stop();
		}
	});

	it("accepts a capability advertisement during the opening subscription fence", async () => {
		const session = await TestSession.disconnected(2);
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const sharedLog = target.log as any;
			const sourceKey = source.node.identity.publicKey;
			const sourceHash = sourceKey.hashcode();
			const scheduleRequests = sinon
				.stub(sharedLog, "scheduleReplicationInfoRequests")
				.callsFake(() => {});

			try {
				const subscription = sharedLog._onSubscription({
					detail: { from: sourceKey, topics: [target.log.topic] },
				});
				expect(sharedLog._replicationInfoBlockedPeers.has(sourceHash)).to.be
					.true;

				await target.log.onMessage(new SyncCapabilitiesMessage(), {
					from: sourceKey,
				} as any);
				await subscription;

				expect(sharedLog._peerSyncCapabilities.get(sourceHash)).to.equal(1);
				expect(sharedLog._subscriptionOpeningEpochByPeer.has(sourceHash)).to.be
					.false;
				expect(sharedLog._replicationInfoBlockedPeers.has(sourceHash)).to.be
					.false;
			} finally {
				scheduleRequests.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("keeps sync negotiation live during the opening subscription fence", async () => {
		const session = await TestSession.disconnected(2);
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const sharedLog = target.log as any;
			const sourceKey = source.node.identity.publicKey;
			const sourceHash = sourceKey.hashcode();
			const scheduleRequests = sinon
				.stub(sharedLog, "scheduleReplicationInfoRequests")
				.callsFake(() => {});
			const synchronizerOnMessage = sinon.spy(
				sharedLog.syncronizer,
				"onMessage",
			);

			try {
				const subscription = sharedLog._onSubscription({
					detail: { from: sourceKey, topics: [target.log.topic] },
				});
				expect(sharedLog._replicationInfoBlockedPeers.has(sourceHash)).to.be
					.true;

				await target.log.onMessage(new RequestMaybeSync({ hashes: [] }), {
					from: sourceKey,
				} as any);
				await subscription;

				expect(synchronizerOnMessage.calledOnce).to.be.true;
				expect(sharedLog._replicationInfoBlockedPeers.has(sourceHash)).to.be
					.false;
			} finally {
				synchronizerOnMessage.restore();
				scheduleRequests.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("drains only pre-transition receives while a subscription opens", async () => {
		const session = await TestSession.disconnected(2);
		const oldReceiveEntered = pDefer<void>();
		const currentReceiveEntered = pDefer<void>();
		const releaseOldReceive = pDefer<void>();
		const releaseCurrentReceive = pDefer<void>();
		let oldReceive: Promise<void> | undefined;
		let currentReceive: Promise<void> | undefined;
		let subscription: Promise<void> | undefined;
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const sharedLog = target.log as any;
			const sourceKey = source.node.identity.publicKey;
			const sourceHash = sourceKey.hashcode();
			const scheduleRequests = sinon
				.stub(sharedLog, "scheduleReplicationInfoRequests")
				.callsFake(() => {});
			let receiveCount = 0;
			const synchronizerOnMessage = sinon
				.stub(sharedLog.syncronizer, "onMessage")
				.callsFake(async () => {
					receiveCount += 1;
					if (receiveCount === 1) {
						oldReceiveEntered.resolve();
						await releaseOldReceive.promise;
					} else {
						currentReceiveEntered.resolve();
						await releaseCurrentReceive.promise;
					}
					return true;
				});

			try {
				oldReceive = target.log.onMessage(
					new RequestMaybeSync({ hashes: [] }),
					{ from: sourceKey } as any,
				);
				await oldReceiveEntered.promise;

				let subscriptionSettled = false;
				subscription = sharedLog
					._onSubscription({
						detail: { from: sourceKey, topics: [target.log.topic] },
					})
					.then(() => {
						subscriptionSettled = true;
					});
				await waitForResolved(() =>
					expect(sharedLog._receiveHandlerDrainByPeer.has(sourceHash)).to.be
						.true,
				);

				let currentReceiveSettled = false;
				currentReceive = target.log
					.onMessage(new RequestMaybeSync({ hashes: [] }), {
						from: sourceKey,
					} as any)
					.then(() => {
						currentReceiveSettled = true;
					});
				await currentReceiveEntered.promise;

				releaseOldReceive.resolve();
				await waitForResolved(
					() => expect(subscriptionSettled).to.be.true,
					{ timeout: 2_000 },
				);
				expect(currentReceiveSettled).to.be.false;
				expect(sharedLog._activeReceiveHandlersByPeer.has(sourceHash)).to.be
					.true;

				releaseCurrentReceive.resolve();
				await Promise.all([oldReceive, currentReceive, subscription]);
				expect(sharedLog._activeReceiveHandlersByPeer.has(sourceHash)).to.be
					.false;
				expect(sharedLog._receiveHandlerDrainByPeer.has(sourceHash)).to.be
					.false;
			} finally {
				releaseOldReceive.resolve();
				releaseCurrentReceive.resolve();
				await Promise.allSettled(
					[oldReceive, currentReceive, subscription].filter(
						(value): value is Promise<void> => value != null,
					),
				);
				synchronizerOnMessage.restore();
				scheduleRequests.restore();
			}
		} finally {
			releaseOldReceive.resolve();
			releaseCurrentReceive.resolve();
			await session.stop();
		}
	});

	it("drains a previously admitted exchange before disconnect cleanup", async () => {
		const session = await TestSession.disconnected(2);
		const hasManyEntered = pDefer<void>();
		const releaseHasMany = pDefer<void>();
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const { entry } = await source.add(uuid(), { meta: { next: [] } });
			await target.log.log.join([entry]);

			const sharedLog = target.log as any;
			const sourceKey = source.node.identity.publicKey;
			const sourceHash = sourceKey.hashcode();
			const originalHasMany = target.log.log.hasMany.bind(target.log.log);
			const hasMany = sinon.stub(target.log.log, "hasMany").callsFake(async (...args) => {
				hasManyEntered.resolve();
				await releaseHasMany.promise;
				return originalHasMany(...args);
			});
			const confirmation = sinon
				.stub(sharedLog, "sendRepairConfirmation")
				.resolves();

			try {
				const receive = target.log.onMessage(exchange(entry), {
					from: sourceKey,
				} as any);
				await hasManyEntered.promise;

				let unsubscribeSettled = false;
				const unsubscribe = sharedLog
					._onUnsubscription({
						detail: { from: sourceKey, topics: [target.log.topic] },
					})
					.then(() => {
						unsubscribeSettled = true;
					});
				await waitForResolved(() =>
					expect(sharedLog._receiveHandlerDrainByPeer.has(sourceHash)).to.be
						.true,
				);
				expect(unsubscribeSettled).to.be.false;

				releaseHasMany.resolve();
				await Promise.all([receive, unsubscribe]);

				expect(sharedLog._entryKnownPeers.get(entry.hash)?.has(sourceHash)).to
					.not.equal(true);
				expect(
					sharedLog._entryKnownPeerObservedAt.get(entry.hash)?.has(sourceHash),
				).to.not.equal(true);
				expect(sharedLog._replicatorLastActivityAt.has(sourceHash)).to.be.false;
				expect(sharedLog._activeReceiveHandlersByPeer.has(sourceHash)).to.be
					.false;
				expect(sharedLog._receiveHandlerDrainByPeer.has(sourceHash)).to.be.false;
				expect(sharedLog._receiveCleanupGateByPeer.has(sourceHash)).to.be.false;
			} finally {
				releaseHasMany.resolve();
				confirmation.restore();
				hasMany.restore();
			}
		} finally {
			releaseHasMany.resolve();
			await session.stop();
		}
	});

	it("drains asynchronous prune-response side effects before cleanup", async () => {
		const session = await TestSession.disconnected(2);
		const pendingResolveEntered = pDefer<void>();
		const lateResolveEntered = pDefer<void>();
		const pendingResolveFinished = pDefer<void>();
		const releasePendingResponse = pDefer<void>();
		const releaseLateResponse = pDefer<void>();
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const sharedLog = target.log as any;
			const sourceKey = source.node.identity.publicKey;
			const sourceHash = sourceKey.hashcode();
			const pendingHash = "pending-prune-response";
			const { entry: retryEntry } = await source.add(uuid(), {
				meta: { next: [] },
			});
			const lateHash = retryEntry.hash;
			const installPending = (
				hash: string,
				entered: DeferredPromise<void>,
				release: DeferredPromise<void>,
				finished?: DeferredPromise<void>,
			) => {
				const pendingPromise = pDefer<void>();
				sharedLog._checkedPrune.pendingDeletes.set(hash, {
					promise: pendingPromise,
					clear: () => {},
					reject: () => {},
					resolve: async (peerHash: string) => {
						entered.resolve();
						await release.promise;
						sharedLog._checkedPrune.addConfirmedReplicator(hash, peerHash);
						finished?.resolve();
					},
				});
			};
			installPending(
				pendingHash,
				pendingResolveEntered,
				releasePendingResponse,
				pendingResolveFinished,
			);
			const leaders = new Map([[sourceHash, { intersecting: true }]]);
			sharedLog._checkedPrune.setRetry(lateHash, {
				attempts: 1,
				entry: retryEntry,
				leaders,
			});
			const findLeaders = sinon
				.stub(sharedLog, "findLeadersFromEntry")
				.resolves(leaders);
			const prune = sinon.stub(sharedLog, "prune").callsFake((...args: unknown[]) => {
				const entries = args[0] as Map<string, unknown>;
				expect(entries.has(lateHash)).to.be.true;
				installPending(lateHash, lateResolveEntered, releaseLateResponse);
				return [Promise.resolve()];
			});

			try {
				const receive = target.log.onMessage(
					new ResponseIPrune({ hashes: [pendingHash, lateHash] }),
					{ from: sourceKey } as any,
				);
				await Promise.all([
					pendingResolveEntered.promise,
					lateResolveEntered.promise,
				]);

				let unsubscribeSettled = false;
				const unsubscribe = sharedLog
					._onUnsubscription({
						detail: { from: sourceKey, topics: [target.log.topic] },
					})
					.then(() => {
						unsubscribeSettled = true;
					});
				await waitForResolved(() =>
					expect(sharedLog._receiveHandlerDrainByPeer.has(sourceHash)).to.be
						.true,
				);
				expect(unsubscribeSettled).to.be.false;

				releasePendingResponse.resolve();
				await pendingResolveFinished.promise;
				await new Promise<void>((resolve) => setTimeout(resolve, 0));
				expect(sharedLog._activeReceiveHandlersByPeer.has(sourceHash)).to.be.true;
				expect(unsubscribeSettled).to.be.false;

				releaseLateResponse.resolve();
				await Promise.all([receive, unsubscribe]);
				for (const hash of [pendingHash, lateHash]) {
					expect(
						sharedLog._checkedPrune
							.getConfirmedReplicators(hash)
							?.has(sourceHash),
					).to.not.equal(true);
				}
				expect(sharedLog._activeReceiveHandlersByPeer.has(sourceHash)).to.be
					.false;
				expect(sharedLog._receiveCleanupGateByPeer.has(sourceHash)).to.be.false;
			} finally {
				releasePendingResponse.resolve();
				releaseLateResponse.resolve();
				prune.restore();
				findLeaders.restore();
			}
		} finally {
			releasePendingResponse.resolve();
			releaseLateResponse.resolve();
			await session.stop();
		}
	});

	it("cancels leader waits before draining prune responses on close", async () => {
		const session = await TestSession.disconnected(2);
		const firstWaitEntered = pDefer<void>();
		const secondWaitEntered = pDefer<void>();
		const releaseFirstCheck = pDefer<void>();
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const sharedLog = target.log as any;
			const sourceKey = source.node.identity.publicKey;
			const hash = "close-cancels-prune-response-leader-wait";
			const pendingPromise = pDefer<void>();
			let waitCount = 0;
			let secondWaitHasEntered = false;
			const waitForMissingLeader = () => {
				waitCount += 1;
				const currentWait = waitCount;
				if (currentWait === 2) {
					secondWaitHasEntered = true;
					secondWaitEntered.resolve();
				}
				return sharedLog.waitForLeaderSelection(
					[{ key: "missing-replicator", replicator: true }],
					{ timeout: 60_000 },
					async () => {
						if (currentWait === 1) {
							firstWaitEntered.resolve();
							await releaseFirstCheck.promise;
						}
						return new Map();
					},
				);
			};

			sharedLog._checkedPrune.pendingDeletes.set(hash, {
				promise: pendingPromise,
				clear: () => {},
				reject: () => {},
				resolve: async () => {
					expect(await waitForMissingLeader()).to.be.false;
					// This wait begins after the close signal has already fired. It must
					// observe the latched abort instead of waiting for its full timeout.
					expect(await waitForMissingLeader()).to.be.false;
				},
			});

			const receive = target.log.onMessage(
				new ResponseIPrune({ hashes: [hash] }),
				{ from: sourceKey } as any,
			);
			await firstWaitEntered.promise;

			let closeSettled = false;
			const close = target.close().then(() => {
				closeSettled = true;
			});
			await waitForResolved(() =>
				expect(sharedLog._replicationLifecycleController.signal.aborted).to.be
					.true,
			);
			expect(sharedLog._closeController.signal.aborted).to.be.false;
			await new Promise<void>((resolve) => setTimeout(resolve, 0));
			// Aborting the outer wait must not release the receive lease while its
			// already-admitted leader check can still perform local persistence.
			expect(secondWaitHasEntered).to.be.false;
			expect(closeSettled).to.be.false;
			releaseFirstCheck.resolve();
			await secondWaitEntered.promise;
			await waitForResolved(() => expect(closeSettled).to.be.true, {
				timeout: 2_000,
			});
			await Promise.all([receive, close]);
		} finally {
			releaseFirstCheck.resolve();
			await session.stop();
		}
	});

	it("drains an admitted pending-IHave callback before close", async () => {
		const session = await TestSession.disconnected(1);
		const callbackEntered = pDefer<void>();
		const releaseCallback = pDefer<void>();
		try {
			const target = await session.peers[0].open(
				new EventStore<string, any>(),
				{
					args: { replicate: false, setup },
				},
			);
			const { entry } = await target.add(uuid(), { meta: { next: [] } });
			const sharedLog = target.log as any;
			let callbackFinished = false;
			const synchronizerEntryAdded = sinon
				.stub(sharedLog.syncronizer, "onEntryAdded")
				.callsFake(() => {});

			try {
				sharedLog._pendingIHave.set(entry.hash, {
					requesting: new Set(),
					resetTimeout: () => {},
					clear: () => {},
					callback: async () => {
						callbackEntered.resolve();
						await releaseCallback.promise;
						callbackFinished = true;
					},
				});
				sharedLog.onEntryAdded(entry);
				await callbackEntered.promise;
				expect(sharedLog._pendingIHaveCallbacks.size).to.equal(1);

				let closeSettled = false;
				const close = target.close().then(() => {
					closeSettled = true;
				});
				await waitForResolved(() =>
					expect(sharedLog._replicationLifecycleController.signal.aborted).to.be
						.true,
				);
				await new Promise<void>((resolve) => setTimeout(resolve, 0));
				expect(closeSettled).to.be.false;
				expect(callbackFinished).to.be.false;

				releaseCallback.resolve();
				await waitForResolved(() => expect(closeSettled).to.be.true, {
					timeout: 2_000,
				});
				await close;
				expect(callbackFinished).to.be.true;
				expect(sharedLog._pendingIHaveCallbacks.size).to.equal(0);
			} finally {
				releaseCallback.resolve();
				synchronizerEntryAdded.restore();
			}
		} finally {
			releaseCallback.resolve();
			await session.stop();
		}
	});

	it("keeps coalesced pending-IHave requests for connected peers", async () => {
		const session = await TestSession.disconnected(3);
		try {
			const target = await session.peers[0].open(
				new EventStore<string, any>(),
				{
					args: { replicate: false, setup },
				},
			);
			const source = await session.peers[2].open(
				new EventStore<string, any>(),
				{
					args: { replicate: false, setup },
				},
			);
			const { entry } = await source.add(uuid(), { meta: { next: [] } });
			const sharedLog = target.log as any;
			const firstKey = session.peers[1].identity.publicKey;
			const firstHash = firstKey.hashcode();
			const secondKey = session.peers[2].identity.publicKey;
			const secondHash = secondKey.hashcode();

			await target.log.onMessage(
				new RequestIPrune({ hashes: [entry.hash] }),
				{ from: firstKey } as any,
			);
			await target.log.onMessage(
				new RequestIPrune({ hashes: [entry.hash] }),
				{ from: secondKey } as any,
			);
			const pending = sharedLog._pendingIHave.get(entry.hash);
			expect([...pending.requesting]).to.have.members([firstHash, secondHash]);

			sharedLog.cleanupPeerDisconnectTracking(firstHash);
			expect([...pending.requesting]).to.deep.equal([secondHash]);
			expect(sharedLog._pendingIHave.get(entry.hash)).to.equal(pending);

			const selfHash = target.node.identity.publicKey.hashcode();
			const waitForEntryReplicators = sinon
				.stub(sharedLog, "_waitForEntryReplicators")
				.callsFake(async (...args: any[]) => {
					args[3]?.onLeader?.(selfHash);
					return new Map([[selfHash, { intersecting: true }]]);
				});
			const responseAdd = sinon.spy(sharedLog.responseToPruneDebouncedFn, "add");
			const synchronizerEntryAdded = sinon
				.stub(sharedLog.syncronizer, "onEntryAdded")
				.callsFake(() => {});

			try {
				sharedLog.onEntryAdded(entry);
				await waitForResolved(() => expect(responseAdd.calledOnce).to.be.true);
				const response = responseAdd.firstCall.args[0];
				expect([...response.peers]).to.deep.equal([secondHash]);
				await waitForResolved(() =>
					expect(sharedLog._pendingIHave.has(entry.hash)).to.be.false,
				);
				expect(sharedLog._pendingIHaveCallbacks.size).to.equal(0);
			} finally {
				synchronizerEntryAdded.restore();
				responseAdd.restore();
				waitForEntryReplicators.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("keeps concurrent exchanges from the same peer independent", async () => {
		const session = await TestSession.disconnected(2);
		const firstHasManyEntered = pDefer<void>();
		const secondHasManyEntered = pDefer<void>();
		const releaseFirstHasMany = pDefer<void>();
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const originalHasMany = target.log.log.hasMany.bind(target.log.log);
			let hasManyCalls = 0;
			const hasMany = sinon.stub(target.log.log, "hasMany").callsFake(async (...args) => {
				hasManyCalls += 1;
				if (hasManyCalls === 1) {
					firstHasManyEntered.resolve();
					await releaseFirstHasMany.promise;
				} else if (hasManyCalls === 2) {
					secondHasManyEntered.resolve();
				}
				return originalHasMany(...args);
			});

			try {
				const context = { from: source.node.identity.publicKey } as any;
				const first = target.log.onMessage(
					new ExchangeHeadsMessage({ heads: [] }),
					context,
				);
				await firstHasManyEntered.promise;
				const second = target.log.onMessage(
					new ExchangeHeadsMessage({ heads: [] }),
					context,
				);

				await secondHasManyEntered.promise;
				expect(hasManyCalls).to.equal(2);

				releaseFirstHasMany.resolve();
				await Promise.all([first, second]);
			} finally {
				releaseFirstHasMany.resolve();
				hasMany.restore();
			}
		} finally {
			releaseFirstHasMany.resolve();
			await session.stop();
		}
	});

	it("fences a replication update queued behind generic peer cleanup", async () => {
		const session = await TestSession.connected(2);
		const synchronizerEntered = pDefer<void>();
		const releaseSynchronizer = pDefer<void>();
		try {
			const store = new EventStore<string, any>();
			const target = await session.peers[0].open(store, {
				args: { replicate: 1, setup, timeUntilRoleMaturity: 0 },
			});
			await session.peers[1].open(store.clone(), {
				args: { replicate: 1, setup, timeUntilRoleMaturity: 0 },
			});
			const sharedLog = target.log as any;
			const sourceKey = session.peers[1].identity.publicKey;
			const sourceHash = sourceKey.hashcode();
			let remoteRange: any;
			await waitForResolved(async () => {
				const ranges = await target.log.replicationIndex
					.iterate({ query: { hash: sourceHash } })
					.all();
				expect(ranges).to.have.length.greaterThan(0);
				remoteRange = ranges[0].value;
			});

			const delayedMessage = new AllReplicatingSegmentsMessage({
				segments: [remoteRange.toReplicationRange()],
			});
			const originalSynchronizerOnMessage =
				sharedLog.syncronizer.onMessage.bind(sharedLog.syncronizer);
			const synchronizer = sinon
				.stub(sharedLog.syncronizer, "onMessage")
				.callsFake(async (message: unknown, context: unknown) => {
					if (message === delayedMessage) {
						synchronizerEntered.resolve();
						await releaseSynchronizer.promise;
						return false;
					}
					return originalSynchronizerOnMessage(message, context);
				});

			try {
				const receive = target.log.onMessage(delayedMessage, {
					from: sourceKey,
					message: { header: { timestamp: BigInt(Date.now()) } },
				} as any);
				await synchronizerEntered.promise;

				const removal = sharedLog.removeReplicator(sourceKey, {
					noEvent: true,
				});
				await waitForResolved(() =>
					expect(sharedLog._receiveHandlerDrainByPeer.has(sourceHash)).to.be
						.true,
				);

				releaseSynchronizer.resolve();
				await Promise.all([receive, removal]);
				expect(
					await target.log.replicationIndex.count({
						query: { hash: sourceHash },
					}),
				).to.equal(0);
				expect(target.log.uniqueReplicators.has(sourceHash)).to.be.false;
				expect(sharedLog.latestReplicationInfoMessage.has(sourceHash)).to.be
					.false;
			} finally {
				releaseSynchronizer.resolve();
				synchronizer.restore();
			}
		} finally {
			releaseSynchronizer.resolve();
			await session.stop();
		}
	});

	it("only confirms and coordinates top-level entries admitted by the lower log", async () => {
		const session = await TestSession.disconnected(2);
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: {
					replicate: false,
					keep: () => true,
					setup,
					timeUntilRoleMaturity: 0,
				},
			});

			const { entry: parent } = await source.add(uuid(), {
				meta: { next: [] },
			});
			const { entry: child } = await source.add(uuid(), {
				meta: { next: [parent] },
			});
			const context = { from: source.node.identity.publicKey } as any;
			const sharedLog = target.log as any;
			const confirmationStub = sinon
				.stub(sharedLog, "sendRepairConfirmation")
				.resolves();
			const pruneSpy = sinon.spy(sharedLog, "pruneJoinedEntriesNoLongerLed");
			let missingParentStub: sinon.SinonStub | undefined =
				makeParentUnavailable(parent.hash);
			try {
				await target.log.onMessage(exchange(child), context);

				expect(await target.log.log.has(child.hash)).to.equal(false);
				expect(await target.log.entryCoordinatesIndex.count()).to.equal(0);
				expect(confirmationStub.callCount).to.equal(0);
				expect(pruneSpy.callCount).to.equal(1);
				expect(pruneSpy.firstCall.args[0]).to.deep.equal([]);

				missingParentStub.restore();
				missingParentStub = undefined;
				await target.log.onMessage(exchange(parent), context);
				await target.log.onMessage(exchange(child), context);

				expect(await target.log.log.has(parent.hash)).to.equal(true);
				expect(await target.log.log.has(child.hash)).to.equal(true);
				const coordinateHashes = (
					await target.log.entryCoordinatesIndex.iterate({}).all()
				).map((result) => result.value.hash);
				expect(coordinateHashes).to.include(child.hash);
				const confirmedHashes = confirmationStub
					.getCalls()
					.flatMap((call) => [...(call.args[1] as Set<string>)]);
				expect(confirmedHashes).to.include(parent.hash);
				expect(confirmedHashes).to.include(child.hash);
				expect(
					pruneSpy.lastCall.args[0].map((entry: any) => entry.hash),
				).to.deep.equal([child.hash]);
			} finally {
				missingParentStub?.restore();
				pruneSpy.restore();
				confirmationStub.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("does not enqueue a rejected reference-only child through toDelete", async () => {
		const session = await TestSession.disconnected(2);
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: {
					replicate: false,
					keep: () => false,
					setup,
					timeUntilRoleMaturity: 0,
				},
			});
			const { entry: parent } = await source.add(uuid(), {
				meta: { next: [] },
			});
			const { entry: child } = await source.add(uuid(), {
				meta: { next: [parent] },
			});

			const sharedLog = target.log as any;
			const sourceHash = source.node.identity.publicKey.hashcode();
			const leaderPlanStub = sinon
				.stub(sharedLog, "planEntryLeaderBatch")
				.resolves([
					{
						coordinates: [1, 2],
						leaders: new Map([[sourceHash, { intersecting: true }]]),
						isLeader: false,
					},
				]);
			const referenceHeadStub = sinon
				.stub(sharedLog, "hasAnyHeadForGidSets")
				.resolves([true]);
			const rebalanceStub = sharedLog.rebalanceParticipationDebounced
				? sinon
						.stub(sharedLog.rebalanceParticipationDebounced, "call")
						.returns(undefined)
				: undefined;
			const pruneSpy = sinon.spy(sharedLog, "pruneDebouncedFnAddIfNotKeeping");
			let missingParentStub: sinon.SinonStub | undefined =
				makeParentUnavailable(parent.hash);
			try {
				await target.log.onMessage(exchange(child, ["referenced-gid"]), {
					from: source.node.identity.publicKey,
				} as any);

				expect(referenceHeadStub.callCount).to.equal(1);
				expect(leaderPlanStub.callCount).to.equal(1);
				expect(await target.log.log.has(child.hash)).to.equal(false);
				expect(pruneSpy.callCount).to.equal(0);

				missingParentStub.restore();
				missingParentStub = undefined;
				await target.log.log.join([parent]);
				await sharedLog.pruneDebouncedFn.flush();
				expect(await target.log.log.has(parent.hash)).to.equal(true);
				expect(sharedLog._checkedPrune.hasActiveWork(child.hash)).to.equal(
					false,
				);
			} finally {
				missingParentStub?.restore();
				pruneSpy.restore();
				rebalanceStub?.restore();
				referenceHeadStub.restore();
				leaderPlanStub.restore();
			}
		} finally {
			await session.stop();
		}
	});

	it("does not enqueue a rejected lower-replica child through maybeDelete", async () => {
		const session = await TestSession.disconnected(2);
		try {
			const store = new EventStore<string, any>();
			const source = await session.peers[0].open(store.clone(), {
				args: { replicate: false, setup },
			});
			const target = await session.peers[1].open(store.clone(), {
				args: {
					replicate: false,
					setup,
					timeUntilRoleMaturity: 0,
				},
			});
			const { entry: parent } = await source.add(uuid(), {
				meta: { next: [] },
			});
			const { entry: child } = await source.add(uuid(), {
				meta: { next: [parent] },
			});

			const sharedLog = target.log as any;
			const targetHash = target.node.identity.publicKey.hashcode();
			const maxReplicasBatchStub = sinon
				.stub(sharedLog, "getMaxReplicasFromHeadsBatch")
				.callsFake(async (...args: unknown[]) => {
					const gids = args[0] as Iterable<string>;
					return new Map([...gids].map((gid) => [gid, 3]));
				});
			const leaderPlanStub = sinon
				.stub(sharedLog, "planEntryLeaderBatch")
				.resolves([
					{
						coordinates: [1, 2, 3],
						leaders: new Map([[targetHash, { intersecting: true }]]),
						isLeader: true,
					},
				]);
			const maxReplicasStub = sinon
				.stub(sharedLog, "getMaxReplicasFromHeads")
				.resolves(3);
			const isLeaderStub = sinon.stub(sharedLog, "isLeader").resolves(false);
			const rebalanceStub = sharedLog.rebalanceParticipationDebounced
				? sinon
						.stub(sharedLog.rebalanceParticipationDebounced, "call")
						.returns(undefined)
				: undefined;
			const pruneSpy = sinon.spy(sharedLog, "pruneDebouncedFnAddIfNotKeeping");
			let missingParentStub: sinon.SinonStub | undefined =
				makeParentUnavailable(parent.hash);
			try {
				await target.log.onMessage(exchange(child), {
					from: source.node.identity.publicKey,
				} as any);

				expect(maxReplicasBatchStub.callCount).to.equal(1);
				expect(leaderPlanStub.callCount).to.equal(1);
				expect(leaderPlanStub.firstCall.args[0][0].replicas).to.equal(3);
				expect(await target.log.log.has(child.hash)).to.equal(false);
				expect(maxReplicasStub.callCount).to.equal(0);
				expect(isLeaderStub.callCount).to.equal(0);
				expect(pruneSpy.callCount).to.equal(0);

				missingParentStub.restore();
				missingParentStub = undefined;
				await target.log.log.join([parent]);
				await sharedLog.pruneDebouncedFn.flush();
				expect(await target.log.log.has(parent.hash)).to.equal(true);
				expect(sharedLog._checkedPrune.hasActiveWork(child.hash)).to.equal(
					false,
				);
			} finally {
				missingParentStub?.restore();
				pruneSpy.restore();
				rebalanceStub?.restore();
				isLeaderStub.restore();
				maxReplicasStub.restore();
				leaderPlanStub.restore();
				maxReplicasBatchStub.restore();
			}
		} finally {
			await session.stop();
		}
	});
});
