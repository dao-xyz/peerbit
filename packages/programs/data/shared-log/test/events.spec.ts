import { type PublicSignKey, randomBytes } from "@peerbit/crypto";
import { TestSession } from "@peerbit/test-utils";
import { delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { v4 as uuid } from "uuid";
import { SyncCapabilitiesMessage } from "../src/exchange-heads.js";
import {
	AllReplicatingSegmentsMessage,
	StoppedReplicating,
} from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

describe("events", () => {
	let session: TestSession;

	afterEach(async () => {
		await session.stop();
	});

	it("replicator:(join|leave)", async () => {
		// Joining now includes a targeted replication-info handshake which can race on
		// slower CI machines. Use waitForResolved instead of a fixed delay.
		session = await TestSession.connected(2);

		let db1JoinEvents: string[] = [];
		let db1LeaveEvents: string[] = [];

		const db1a = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1 },
		});
		db1a.log.events.addEventListener("replicator:join", (event) => {
			db1JoinEvents.push(event.detail.publicKey.hashcode());
		});

		db1a.log.events.addEventListener("replicator:leave", (event) => {
			db1LeaveEvents.push(event.detail.publicKey.hashcode());
		});

		const db1b = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1 },
		});

		const db2a = await session.peers[1].open(db1a.clone(), {
			args: { replicate: 0.6 },
		});

		const db2b = await session.peers[1].open(db1b.clone(), {
			args: { replicate: 0.4 },
		});
		await waitForResolved(
			() =>
				expect(db1JoinEvents).to.have.members([
					session.peers[1].identity.publicKey.hashcode(),
				]),
			{ timeout: 20_000 },
		);

		await db2a.close();
		await db2b.close();

		// try open another db and make sure it does not trigger join event to db1
		await waitForResolved(
			() =>
				expect(db1LeaveEvents).to.have.members([
					session.peers[1].identity.publicKey.hashcode(),
				]),
			{ timeout: 20_000 },
		);
		expect(db1JoinEvents).to.have.length(1); // no new join event
	});

	it("cleans prune response tracking on unsubscribe", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1 },
		});

		const disconnectedPublicKey = session.peers[1].identity.publicKey;
		const disconnectedPeerHash = disconnectedPublicKey.hashcode();
		const entryHash = uuid();

		const responseMap = (db1.log as any)[
			"_requestIPruneResponseReplicatorSet"
		] as Map<string, Set<string>>;
		responseMap.set(entryHash, new Set([disconnectedPeerHash]));

		await db1.log.handleSubscriptionChange(
			disconnectedPublicKey,
			[db1.log.topic],
			false,
		);

		expect(responseMap.get(entryHash)).to.be.undefined;
	});

	it("accepts structural public keys when removing replicators", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1 },
		});
		const peerKey = session.peers[1].identity.publicKey as PublicSignKey & {
			publicKey?: Uint8Array;
		};
		const foreignKey = {
			publicKey: peerKey.publicKey,
			hashcode: () => peerKey.hashcode(),
			toString: () => peerKey.toString(),
			get bytes() {
				return peerKey.bytes;
			},
		} as unknown as PublicSignKey;

		const changes: string[] = [];
		db1.log.events.addEventListener("replication:change", (event) => {
			changes.push(event.detail.publicKey.hashcode());
		});

		await (
			db1.log as unknown as {
				removeReplicator(key: PublicSignKey): Promise<void>;
			}
		).removeReplicator(foreignKey);

		expect(changes).to.deep.equal([peerKey.hashcode()]);
	});

	it("fences a reconnect until an in-flight unsubscribe removal is coherent", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		await session.peers[1].open(db1.clone(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});

		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const log = db1.log as any;
		const replicationIndex = db1.log.replicationIndex as any;
		await waitForResolved(async () => {
			expect(
				await replicationIndex
					.iterate({ query: { hash: remoteHash } })
					.all(),
			).to.have.length.greaterThan(0);
			expect(db1.log.uniqueReplicators.has(remoteHash)).to.be.true;
		});

		const deleteStarted = pDefer<void>();
		const releaseDelete = pDefer<void>();
		const originalDel = replicationIndex.del.bind(replicationIndex);
		let blockNextRemovalDelete = true;
		const del = sinon
			.stub(replicationIndex, "del")
			.callsFake((async (query: any, options?: any) => {
				if (
					blockNextRemovalDelete &&
					query?.query?.hash === remoteHash
				) {
					blockNextRemovalDelete = false;
					deleteStarted.resolve();
					await releaseDelete.promise;
				}
				return originalDel(query, options);
			}) as any);
		const disconnected = sinon.spy(log.syncronizer, "onPeerDisconnected");
		const leaves: string[] = [];
		db1.log.events.addEventListener("replicator:leave", (event) => {
			leaves.push(event.detail.publicKey.hashcode());
		});

		try {
			const unsubscribeEvent = {
				detail: { from: remoteKey, topics: [db1.log.topic] },
			} as any;
			const subscribeEvent = {
				detail: { from: remoteKey, topics: [db1.log.topic] },
			} as any;

			const oldUnsubscribe = log._onUnsubscription(unsubscribeEvent);
			await deleteStarted.promise;

			// The old removal has crossed into its destructive queue item. Reconnect
			// must stay blocked behind it instead of creating state midway through.
			let reconnectSettled = false;
			const reconnect = log._onSubscription(subscribeEvent).finally(() => {
				reconnectSettled = true;
			});
			await Promise.resolve();
			expect(reconnectSettled).to.be.false;
			expect(log._replicationInfoBlockedPeers.has(remoteHash)).to.be.true;

			releaseDelete.resolve();
			await Promise.all([oldUnsubscribe, reconnect]);
			await waitForResolved(async () => {
				expect(
					await replicationIndex
						.iterate({ query: { hash: remoteHash } })
						.all(),
				).to.have.length.greaterThan(0);
				expect(log.uniqueReplicators.has(remoteHash)).to.be.true;
				expect(log._replicatorJoinEmitted.has(remoteHash)).to.be.true;
			});

			const gid = "reconnected-generation";
			log._peerSyncCapabilities.set(remoteHash, 7);
			log._gidPeersHistory.set(gid, new Set([remoteHash]));

			expect(log._peerSyncCapabilities.get(remoteHash)).to.equal(7);
			expect(log._replicatorLastActivityAt.has(remoteHash)).to.be.true;
			expect(log._gidPeersHistory.get(gid)?.has(remoteHash)).to.be.true;
			expect(log._replicationInfoBlockedPeers.has(remoteHash)).to.be.false;
			expect(disconnected.calledOnceWith(remoteHash)).to.be.true;
			expect(leaves).to.deep.equal([]);

			// A later unsubscribe still owns the current epoch and must perform the
			// complete removal, including sync-related cleanup and one leave event.
			await log._onUnsubscription(unsubscribeEvent);
			expect(
				await replicationIndex
					.iterate({ query: { hash: remoteHash } })
					.all(),
			).to.have.length(0);
			expect(log.uniqueReplicators.has(remoteHash)).to.be.false;
			expect(log._replicatorJoinEmitted.has(remoteHash)).to.be.false;
			expect(log._peerSyncCapabilities.has(remoteHash)).to.be.false;
			expect(log._replicatorLastActivityAt.has(remoteHash)).to.be.false;
			expect(log._gidPeersHistory.has(gid)).to.be.false;
			expect(disconnected.callCount).to.equal(2);
			expect(disconnected.alwaysCalledWith(remoteHash)).to.be.true;
			expect(leaves).to.deep.equal([remoteHash]);
		} finally {
			del.restore();
			releaseDelete.resolve();
			disconnected.restore();
		}
	});

	it("does not let a superseded reconnect reopen a peer", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		await session.peers[1].open(db1.clone(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});

		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const log = db1.log as any;
		const replicationIndex = db1.log.replicationIndex as any;
		await waitForResolved(async () => {
			expect(
				await replicationIndex
					.iterate({ query: { hash: remoteHash } })
					.all(),
			).to.have.length.greaterThan(0);
			expect(db1.log.uniqueReplicators.has(remoteHash)).to.be.true;
		});

		const deleteStarted = pDefer<void>();
		const releaseDelete = pDefer<void>();
		const originalDel = replicationIndex.del.bind(replicationIndex);
		let blockNextRemovalDelete = true;
		const del = sinon
			.stub(replicationIndex, "del")
			.callsFake((async (query: any, options?: any) => {
				if (
					blockNextRemovalDelete &&
					query?.query?.hash === remoteHash
				) {
					blockNextRemovalDelete = false;
					deleteStarted.resolve();
					await releaseDelete.promise;
				}
				return originalDel(query, options);
			}) as any);
		const unblock = sinon.spy(log._replicationInfoBlockedPeers, "delete");
		const scheduleRequests = sinon.spy(log, "scheduleReplicationInfoRequests");
		const disconnected = sinon.spy(log.syncronizer, "onPeerDisconnected");
		const leaves: string[] = [];
		db1.log.events.addEventListener("replicator:leave", (event) => {
			leaves.push(event.detail.publicKey.hashcode());
		});

		const unsubscribeEvent = {
			detail: { from: remoteKey, topics: [db1.log.topic] },
		} as any;
		const subscribeEvent = {
			detail: { from: remoteKey, topics: [db1.log.topic] },
		} as any;

		try {
			const firstUnsubscribe = log._onUnsubscription(unsubscribeEvent);
			await deleteStarted.promise;
			const supersededSubscribe = log._onSubscription(subscribeEvent);
			await Promise.resolve();
			const winningUnsubscribe = log._onUnsubscription(unsubscribeEvent);
			await Promise.resolve();
			expect(log._replicationInfoBlockedPeers.has(remoteHash)).to.be.true;

			releaseDelete.resolve();
			await Promise.all([
				firstUnsubscribe,
				supersededSubscribe,
				winningUnsubscribe,
			]);

			expect(
				await replicationIndex
					.iterate({ query: { hash: remoteHash } })
					.all(),
			).to.have.length(0);
			expect(log.uniqueReplicators.has(remoteHash)).to.be.false;
			expect(log._replicatorJoinEmitted.has(remoteHash)).to.be.false;
			expect(log._replicationInfoBlockedPeers.has(remoteHash)).to.be.true;
			expect(unblock.neverCalledWith(remoteHash)).to.be.true;
			expect(scheduleRequests.notCalled).to.be.true;
			expect(disconnected.callCount).to.equal(2);
			expect(disconnected.alwaysCalledWith(remoteHash)).to.be.true;
			expect(leaves).to.deep.equal([remoteHash]);
		} finally {
			del.restore();
			unblock.restore();
			scheduleRequests.restore();
			releaseDelete.resolve();
			disconnected.restore();
		}
	});

	it("cleans old sync state when reconnect supersedes a queued removal", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		await session.peers[1].open(db1.clone(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});

		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const log = db1.log as any;
		const replicationIndex = db1.log.replicationIndex as any;
		await waitForResolved(async () => {
			expect(
				await replicationIndex.count({ query: { hash: remoteHash } }),
			).to.be.greaterThan(0);
		});

		const blockerStarted = pDefer<void>();
		const releaseBlocker = pDefer<void>();
		const blocker = log.withReplicationInfoApplyQueue(
			remoteHash,
			async () => {
				blockerStarted.resolve();
				await releaseBlocker.promise;
			},
		);
		await blockerStarted.promise;

		const cleanupStarted = pDefer<void>();
		const releaseCleanup = pDefer<void>();
		const originalDisconnect =
			log.syncronizer.onPeerDisconnected.bind(log.syncronizer);
		const disconnected = sinon
			.stub(log.syncronizer, "onPeerDisconnected")
			.callsFake(async (...args: unknown[]) => {
				const peerHash = args[0] as string;
				cleanupStarted.resolve();
				await releaseCleanup.promise;
				return originalDisconnect(peerHash);
			});
		const leaves: string[] = [];
		db1.log.events.addEventListener("replicator:leave", (event) => {
			leaves.push(event.detail.publicKey.hashcode());
		});
		const gid = "stale-before-reconnect";
		log._peerSyncCapabilities.set(remoteHash, 7);
		log._gidPeersHistory.set(gid, new Set([remoteHash]));

		try {
			const unsubscribe = log._onUnsubscription({
				detail: { from: remoteKey, topics: [db1.log.topic] },
			});
			let reconnectSettled = false;
			const reconnect = log
				._onSubscription({
					detail: { from: remoteKey, topics: [db1.log.topic] },
				})
					.finally(() => {
						reconnectSettled = true;
					});
			releaseBlocker.resolve();
			await cleanupStarted.promise;
			expect(log._receiveCleanupGateByPeer.has(remoteHash)).to.be.true;
			await db1.log.onMessage(new SyncCapabilitiesMessage(), {
				from: remoteKey,
			} as any);
			expect(
				log._openingSyncCapabilitiesByPeer.get(remoteHash)?.capabilities,
			).to.equal(1);
			await Promise.resolve();
			expect(reconnectSettled).to.be.false;
			releaseCleanup.resolve();
			await Promise.all([blocker, unsubscribe, reconnect]);

			// The stale removal must preserve current membership, but its ordered
			// disconnect cleanup must run before reconnect starts using the lane.
			expect(
				await replicationIndex.count({ query: { hash: remoteHash } }),
			).to.be.greaterThan(0);
			expect(db1.log.uniqueReplicators.has(remoteHash)).to.be.true;
			expect(log._peerSyncCapabilities.get(remoteHash)).to.equal(1);
			expect(log._openingSyncCapabilitiesByPeer.has(remoteHash)).to.be.false;
			expect(log._gidPeersHistory.has(gid)).to.be.false;
			expect(disconnected.calledOnceWith(remoteHash)).to.be.true;
			expect(leaves).to.deep.equal([]);
		} finally {
			releaseBlocker.resolve();
			releaseCleanup.resolve();
			disconnected.restore();
		}
	});

	it("does not apply a replication message superseded while the synchronizer yields", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		await session.peers[1].open(db1.clone(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});

		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const log = db1.log as any;
		const replicationIndex = db1.log.replicationIndex as any;
		let remoteRange: any;
		await waitForResolved(async () => {
			const ranges = await replicationIndex
				.iterate({ query: { hash: remoteHash } })
				.all();
			expect(ranges).to.have.length.greaterThan(0);
			remoteRange = ranges[0].value;
		});

		await log.removeReplicator(remoteKey, { noEvent: true });
		expect(
			await replicationIndex.count({ query: { hash: remoteHash } }),
		).to.equal(0);

		const delayedMessage = new AllReplicatingSegmentsMessage({
			segments: [remoteRange.toReplicationRange()],
		});
		const synchronizerEntered = pDefer<void>();
		const releaseSynchronizer = pDefer<void>();
		const originalSynchronizerOnMessage =
			log.syncronizer.onMessage.bind(log.syncronizer);
		const synchronizer = sinon
			.stub(log.syncronizer, "onMessage")
			.callsFake(async (message: unknown, context: unknown) => {
				if (message === delayedMessage) {
					synchronizerEntered.resolve();
					await releaseSynchronizer.promise;
					return false;
				}
				return originalSynchronizerOnMessage(message, context);
			});
		const scheduleRequests = sinon
			.stub(log, "scheduleReplicationInfoRequests")
			.callsFake(() => {});

		try {
			const delayedReceive = db1.log.onMessage(delayedMessage, {
				from: remoteKey,
				message: { header: { timestamp: BigInt(Date.now()) } },
			} as any);
			await synchronizerEntered.promise;

			const unsubscribe = log._onUnsubscription({
				detail: { from: remoteKey, topics: [db1.log.topic] },
			});
			releaseSynchronizer.resolve();
			await Promise.all([delayedReceive, unsubscribe]);
			await log._onSubscription({
				detail: { from: remoteKey, topics: [db1.log.topic] },
			});
			expect(
				await replicationIndex.count({ query: { hash: remoteHash } }),
			).to.equal(0);
		} finally {
			releaseSynchronizer.resolve();
			synchronizer.restore();
			scheduleRequests.restore();
		}
	});

	it("scopes replication timestamps to a reconnect generation", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		await session.peers[1].open(db1.clone(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});

		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const log = db1.log as any;
		const replicationIndex = db1.log.replicationIndex as any;
		let remoteRange: any;
		await waitForResolved(async () => {
			const ranges = await replicationIndex
				.iterate({ query: { hash: remoteHash } })
				.all();
			expect(ranges).to.have.length.greaterThan(0);
			remoteRange = ranges[0].value;
		});
		const scheduleRequests = sinon
			.stub(log, "scheduleReplicationInfoRequests")
			.callsFake(() => {});

		try {
			await log._onUnsubscription({
				detail: { from: remoteKey, topics: [db1.log.topic] },
			});
			// Successful cleanup retires both the old receive generation and its
			// sender-clock watermark; the unsubscribe fence rejects late traffic.
			expect(log.latestReplicationInfoMessage.has(remoteHash)).to.be.false;

			await log._onSubscription({
				detail: { from: remoteKey, topics: [db1.log.topic] },
			});
			expect(log.latestReplicationInfoMessage.has(remoteHash)).to.be.false;

			await db1.log.onMessage(
				new AllReplicatingSegmentsMessage({
					segments: [remoteRange.toReplicationRange()],
				}),
				{
					from: remoteKey,
					// Simulate a sender whose wall clock trails this receiver.
					message: { header: { timestamp: 1n } },
				} as any,
			);

			expect(
				await replicationIndex.count({ query: { hash: remoteHash } }),
			).to.be.greaterThan(0);
			expect(log.latestReplicationInfoMessage.get(remoteHash)).to.equal(1n);
		} finally {
			scheduleRequests.restore();
		}
	});

	it("ignores a stopped-segment message older than the latest snapshot", async () => {
		session = await TestSession.connected(2);

		const db1 = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		await session.peers[1].open(db1.clone(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});

		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const log = db1.log as any;
		const replicationIndex = db1.log.replicationIndex as any;
		let remoteRange: any;
		await waitForResolved(async () => {
			const ranges = await replicationIndex
				.iterate({ query: { hash: remoteHash } })
				.all();
			expect(ranges).to.have.length.greaterThan(0);
			remoteRange = ranges[0].value;
		});

		log.latestReplicationInfoMessage.delete(remoteHash);
		const newerTimestamp = BigInt(Date.now() + 1_000);
		await db1.log.onMessage(
			new AllReplicatingSegmentsMessage({
				segments: [remoteRange.toReplicationRange()],
			}),
			{
				from: remoteKey,
				message: { header: { timestamp: newerTimestamp } },
			} as any,
		);
		await db1.log.onMessage(
			new StoppedReplicating({ segmentIds: [remoteRange.id] }),
			{
				from: remoteKey,
				message: { header: { timestamp: newerTimestamp - 1n } },
			} as any,
		);

		expect(
			await replicationIndex.count({ query: { hash: remoteHash } }),
		).to.be.greaterThan(0);
		expect(log.latestReplicationInfoMessage.get(remoteHash)).to.equal(
			newerTimestamp,
		);
	});

	it("drains admitted replication mutations before close and reopen", async () => {
		session = await TestSession.connected(1);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: 1, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const mutationStarted = pDefer<void>();
		const releaseMutation = pDefer<void>();
		const mutation = log.withReplicationInfoApplyQueue(
			"synthetic-remote-peer",
			async () => {
				mutationStarted.resolve();
				await releaseMutation.promise;
			},
		);
		await mutationStarted.promise;

		let closeSettled = false;
		const close = db.close().finally(() => {
			closeSettled = true;
		});
		await delay(25);
		expect(closeSettled).to.be.false;

		releaseMutation.resolve();
		await Promise.all([mutation, close]);
		await session.peers[0].open(db);
		expect(log._replicationInfoApplyQueueByPeer.size).to.equal(0);
	});

	it("replicate:join not emitted on update", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore();
		let db1JoinEvents: string[] = [];
		const store1 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
			},
		});
		store1.log.events.addEventListener("replicator:join", (event) => {
			db1JoinEvents.push(event.detail.publicKey.hashcode());
		});

		const store2 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
			},
		});
		await waitForResolved(() =>
			expect(db1JoinEvents).to.have.members([
				session.peers[1].identity.publicKey.hashcode(),
			]),
		);

		await store2.log.replicate({ factor: 0.5 }, { reset: true });

		await waitForResolved(async () => {
			const store2Role = await store1.log.replicationIndex
				.iterate({ query: { hash: store2.node.identity.publicKey.hashcode() } })
				.all();
			expect(store2Role).to.have.length(1);
			expect(store2Role[0].value.widthNormalized).to.be.closeTo(0.5, 0.01);
		});

		expect(db1JoinEvents).to.have.members([
			session.peers[1].identity.publicKey.hashcode(),
		]); // no new join events
	});

	it("replicator:mature not emitted more than once on update same same range id", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore();
		let db1JoinEvents: string[] = [];
		let timeUntilRoleMaturity = 1e3;
		const store1 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity,
			},
		});
		store1.log.events.addEventListener("replicator:mature", (event) => {
			db1JoinEvents.push(event.detail.publicKey.hashcode());
		});

		let rangeId = randomBytes(32);
		const store2 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: { id: rangeId, factor: 1 },
				timeUntilRoleMaturity,
			},
		});
		await waitForResolved(() =>
			expect(db1JoinEvents).to.have.members([
				session.peers[0].identity.publicKey.hashcode(),
				session.peers[1].identity.publicKey.hashcode(),
			]),
		);

		// reset: true means we will re-initalize hence we expect a maturity event
		await store2.log.replicate({ id: rangeId, factor: 0.5 }, { reset: true });

		await waitForResolved(async () => {
			const store2Role = await store1.log.replicationIndex
				.iterate({ query: { hash: store2.node.identity.publicKey.hashcode() } })
				.all();
			expect(store2Role).to.have.length(1);
			expect(store2Role[0].value.widthNormalized).to.be.closeTo(0.5, 0.01);
		});
		expect(store.log.pendingMaturity.size).to.be.eq(0);

		await delay(timeUntilRoleMaturity * 2); // wait a little bit more
		expect(db1JoinEvents).to.have.members([
			session.peers[0].identity.publicKey.hashcode(),
			session.peers[1].identity.publicKey.hashcode(),
		]); // no new join events

		expect(store.log.pendingMaturity.size).to.eq(0);
	});

	it("replicator:mature emit twice on update reset", async () => {
		session = await TestSession.connected(2);

		const store = new EventStore();
		let db1JoinEvents: string[] = [];
		let timeUntilRoleMaturity = 1e3;
		const store1 = await session.peers[0].open(store, {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity,
			},
		});
		store1.log.events.addEventListener("replicator:mature", (event) => {
			db1JoinEvents.push(event.detail.publicKey.hashcode());
		});

		const store2 = await session.peers[1].open(store.clone(), {
			args: {
				replicate: { factor: 1 },
				timeUntilRoleMaturity,
			},
		});
		await waitForResolved(() =>
			expect(db1JoinEvents).to.have.members([
				session.peers[0].identity.publicKey.hashcode(),
				session.peers[1].identity.publicKey.hashcode(),
			]),
		);

		// reset: true means we will re-initalize hence we expect a maturity event
		await store2.log.replicate({ factor: 0.5 }, { reset: true });

		await waitForResolved(async () => {
			const store2Role = await store1.log.replicationIndex
				.iterate({ query: { hash: store2.node.identity.publicKey.hashcode() } })
				.all();
			expect(store2Role).to.have.length(1);
			expect(store2Role[0].value.widthNormalized).to.be.closeTo(0.5, 0.01);
		});
		expect(store.log.pendingMaturity.size).to.be.greaterThan(0);

		await waitForResolved(() =>
			expect(db1JoinEvents).to.have.members([
				session.peers[0].identity.publicKey.hashcode(),
				session.peers[1].identity.publicKey.hashcode(),
				session.peers[1].identity.publicKey.hashcode(),
			]),
		);

		expect(store.log.pendingMaturity.size).to.eq(0);
	});

	describe("waitForReplicators", async () => {
		it("resolves immediately is offline and replicating and mature", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: {
						factor: 1,
					},
					timeUntilRoleMaturity: 0,
				},
			});
			let timeout = 1e4;
			let t0 = Date.now();
			await store1.log.waitForReplicators({
				timeout,
			});
			let t1 = Date.now();
			expect(t1 - t0).to.be.lessThan(1e3); // "immediately"
		});

		it("times out after mature when is offline and replicating and unmature", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();

			let t0 = Date.now();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: {
						factor: 1,
					},
					timeUntilRoleMaturity: 6e3, // > 3e3
				},
			});

			let timeout = 1e4;

			await store1.log.waitForReplicators({
				timeout,
			});

			let t1 = Date.now();
			expect(t1 - t0).to.be.greaterThan(3e3); // should wait for maturity
		});

		it("resolves when replication starts after wait is pending", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: false,
					timeUntilRoleMaturity: 1e3,
				},
			});

			const waitPromise = store1.log.waitForReplicators({
				timeout: 10e3,
				waitForNewPeers: true,
			});

			await delay(100);
			await store1.log.replicate({ factor: 1 });

			await waitPromise;
		});

		it("resolves even if maturity timers are cleared", async () => {
			session = await TestSession.connected(1);
			const store = new EventStore();
			const timeUntilRoleMaturity = 3e3;
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: {
						factor: 1,
					},
					timeUntilRoleMaturity,
				},
			});

			const hash = session.peers[0].identity.publicKey.hashcode();
			// @ts-ignore accessing internal state for test purposes
			const pending = store1.log.pendingMaturity.get(hash);
			expect(pending, "expected pending maturity timers").to.exist;
			if (pending) {
				for (const [_key, value] of pending) {
					clearTimeout(value.timeout);
				}
				pending.clear();
			}

			await store1.log.waitForReplicators({
				timeout: 10e3,
			});
		});

		it("times out after timeout if online", async () => {
			session = await TestSession.connected(2);
			const store = new EventStore();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: false,
				},
			});

			const store2 = await session.peers[1].open(store.clone(), {
				args: {
					replicate: false,
				},
			});

			await store1.log.waitFor(store2.log.node.identity.publicKey);

			let timeout = 3e3;
			let t0 = Date.now();
			await expect(
				store1.log.waitForReplicators({
					timeout,
				}),
			).to.be.eventually.rejectedWith("Timeout");
			let t1 = Date.now();
			// Allow small timer jitter on busy CI runners.
			expect(t1 - t0).to.be.greaterThanOrEqual(timeout - 25);
		});

		it("will wait for role age", async () => {
			session = await TestSession.connected(1);

			const store = new EventStore();
			const store1 = await session.peers[0].open(store, {
				args: {
					replicate: { factor: 1 },
				},
			});

			let waitForRoleAge = 2e3;
			let t0 = Date.now();
			await store1.log.waitForReplicators({
				roleAge: waitForRoleAge,
				waitForNewPeers: true, // prevent waitForReplicators from resolving immediately
			});
			let t1 = Date.now();
			// Allow some timer jitter across environments/CI
			expect(t1 - t0).to.be.greaterThanOrEqual(waitForRoleAge - 250);
		});

		it("will wait for warmup when restarting", async () => {
			session = await TestSession.connected(1, {
				directory:
					"./tmp/shared-log/waitForReplicators/wait-for-warmup/" + uuid(),
			});

			const store = new EventStore();
			let store1 = await session.peers[0].open(store, {
				args: {
					replicate: { factor: 1 },
				},
			});

			await delay(3e3);
			await store1.close();
			let waitForRoleAge = 3e3;
			let t0 = Date.now();
			store1 = await session.peers[0].open(store1, {
				args: {
					replicate: {
						type: "resume",
						default: {
							factor: 0.5,
						},
					},
				},
			});

			await store1.log.waitForReplicators({
				roleAge: waitForRoleAge,
				timeout: 1e4,
				waitForNewPeers: true, // prevent waitForReplicators from resolving immediately
			});
			let t1 = Date.now();
			// Restart warmup starts during reopen, before waitForReplicators is called.
			expect(t1 - t0).to.be.greaterThanOrEqual(waitForRoleAge - 250);
			expect(t1 - t0).to.be.lessThan(waitForRoleAge + 5e3);
		});

		it("will wait joining replicator role age", async () => {
			session = await TestSession.connected(2);
			const store = new EventStore();
			await session.peers[1].open(store.clone(), {
				args: {
					replicate: { factor: 1 },
				},
			});

			await delay(3e3);

			const store2 = await session.peers[0].open(store, {
				args: {
					replicate: false,
				},
			});

			let waitForRoleAge = 3e3;
			let t0 = Date.now();
			await store2.log.waitForReplicators({
				roleAge: waitForRoleAge,
				waitForNewPeers: true, // prevent waitForReplicators from resolving immediately
			});

			let t1 = Date.now();
			expect(t1 - t0).to.be.lessThanOrEqual(waitForRoleAge); // because store1
		});
	});
});
