import { Ed25519Keypair, randomBytes } from "@peerbit/crypto";
import { AcknowledgeDelivery } from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import {
	RequestIPrune,
	RequestIPruneV2,
	ResponseIPrune,
	ResponseIPruneV2,
} from "../src/exchange-heads.js";
import { EventStore } from "./utils/stores/index.js";

describe("checked prune correlated handoff", () => {
	let session: TestSession | undefined;

	afterEach(async () => {
		await session?.stop();
		session = undefined;
	});

	it("requires an exact id, contacted signer, and full quorum", async () => {
		session = await TestSession.disconnected(3);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-exact-quorum");
		const remoteKeys = session.peers
			.slice(1)
			.map((peer) => peer.identity.publicKey);
		const remoteHashes = remoteKeys.map((key) => key.hashcode());
		const leaders = new Map(
			remoteHashes.map((hash) => [hash, { intersecting: true }]),
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 2 });
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({ leaders, localLeader: false });
		const remove = sinon.spy(log.log, "remove");
		let pruning: Promise<unknown> | undefined;

		try {
			[pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			await waitForResolved(() => {
				expect(
					log._checkedPrune.getContactedReplicators(entry.hash)?.size,
				).to.equal(2);
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(pending).to.exist;
			const outboundRequests = send
				.getCalls()
				.map((call) => call.args[0])
				.filter(
					(message): message is RequestIPruneV2 =>
						message instanceof RequestIPruneV2,
				);
			expect(outboundRequests).to.have.length(2);
			for (const message of outboundRequests) {
				expect(message.requests).to.have.length(1);
				expect(message.requests[0]?.requestId).to.deep.equal(pending.requestId);
			}

			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [
						{ hash: entry.hash, requestId: pending.requestId },
						{ hash: entry.hash, requestId: pending.requestId },
					],
				}),
				{ from: remoteKeys[0] } as any,
			);
			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [
						{ hash: entry.hash, requestId: new Uint8Array(32).fill(9) },
					],
				}),
				{ from: remoteKeys[0] } as any,
			);
			const unknownKey = (await Ed25519Keypair.create()).publicKey;
			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [{ hash: entry.hash, requestId: pending.requestId }],
				}),
				{ from: unknownKey } as any,
			);
			expect(
				log._checkedPrune.getConfirmedReplicators(entry.hash)?.size ?? 0,
			).to.equal(0);

			const firstResponse = new ResponseIPruneV2({
				requests: [{ hash: entry.hash, requestId: pending.requestId }],
			});
			await db.log.onMessage(firstResponse, { from: remoteKeys[0] } as any);
			await db.log.onMessage(firstResponse, { from: remoteKeys[0] } as any);
			expect(
				log._checkedPrune.getConfirmedReplicators(entry.hash)?.size,
			).to.equal(1);
			expect(remove.called).false;

			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [{ hash: entry.hash, requestId: pending.requestId }],
				}),
				{ from: remoteKeys[1] } as any,
			);
			await pruning;
			expect(remove.calledOnce).true;
			expect(await log.log.has(entry.hash)).false;
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(pruning ? [pruning] : []);
			remove.restore();
			revalidate.restore();
			replicas.restore();
			send.restore();
		}
	});

	it("does not attach an old response to a replacement generation", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-stale-generation");
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const send = sinon.stub(log.rpc, "send").resolves();
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		let first: Promise<unknown> | undefined;
		let second: Promise<unknown> | undefined;

		try {
			[first] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			const firstOutcome = first!.catch((error) => error);
			await waitForResolved(() => {
				expect(log._checkedPrune.getPendingDelete(entry.hash)).to.exist;
				expect(
					log._checkedPrune
						.getContactedReplicators(entry.hash)
						?.has(remoteHash),
				).true;
			});
			const firstId = log._checkedPrune
				.getPendingDelete(entry.hash)
				.requestId.slice();
			const firstPending = log._checkedPrune.getPendingDelete(entry.hash);
			log._checkedPrune.cleanupPeer(remoteHash);
			expect(
				log._checkedPrune.addRequestSent(entry.hash, remoteHash, firstPending),
			).false;
			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [{ hash: entry.hash, requestId: firstId }],
				}),
				{ from: remoteKey } as any,
			);
			expect(
				log._checkedPrune.getConfirmedReplicators(entry.hash)?.size ?? 0,
			).to.equal(0);
			await log.cancelCheckedPruneForLocalLeader(entry.hash);
			expect(await firstOutcome).to.be.instanceOf(Error);

			[second] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			const secondOutcome = second!.catch((error) => error);
			await waitForResolved(() => {
				const pending = log._checkedPrune.getPendingDelete(entry.hash);
				expect(pending).to.exist;
				expect(
					log._checkedPrune
						.getContactedReplicators(entry.hash)
						?.has(remoteHash),
				).true;
				expect([...pending.requestId]).to.not.deep.equal([...firstId]);
			});

			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [{ hash: entry.hash, requestId: firstId }],
				}),
				{ from: remoteKey } as any,
			);
			expect(
				log._checkedPrune.getConfirmedReplicators(entry.hash)?.size ?? 0,
			).to.equal(0);

			await log.cancelCheckedPruneForLocalLeader(entry.hash);
			expect(await secondOutcome).to.be.instanceOf(Error);
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(
				[first, second].filter(
					(value): value is Promise<unknown> => value != null,
				),
			);
			replicas.restore();
			send.restore();
		}
	});

	it("uses a fresh generation when a revoked peer becomes a leader again", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-rejoined-leader");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const sent: RequestIPruneV2[] = [];
		const send = sinon.stub(log.rpc, "send").callsFake(async (message) => {
			if (message instanceof RequestIPruneV2) {
				sent.push(message);
			}
		});
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		let first: Promise<unknown> | undefined;
		let second: Promise<unknown> | undefined;

		try {
			[first] = log.prune(new Map([[entry.hash, { entry, leaders }]]));
			const firstOutcome = first!.catch((error) => error);
			await waitForResolved(() => {
				expect(
					log._checkedPrune
						.getContactedReplicators(entry.hash)
						?.has(remoteHash),
				).to.be.true;
			});
			const firstPending = log._checkedPrune.getPendingDelete(entry.hash);
			const firstId = firstPending.requestId.slice();

			log.removePruneRequestSent(entry.hash, remoteHash);
			[second] = log.prune(new Map([[entry.hash, { entry, leaders }]]));
			void second!.catch(() => {});

			expect(await firstOutcome).to.be.instanceOf(Error);
			const secondPending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(secondPending).to.exist;
			expect(secondPending).to.not.equal(firstPending);
			expect([...secondPending.requestId]).to.not.deep.equal([...firstId]);
			await waitForResolved(() => {
				expect(
					log._checkedPrune
						.getContactedReplicators(entry.hash)
						?.has(remoteHash),
				).to.be.true;
			});
			expect(
				sent.some((message) =>
					message.requests.some(
						(request) =>
							request.hash === entry.hash &&
							request.requestId.every(
								(value, index) => value === secondPending.requestId[index],
							),
					),
				),
			).to.be.true;
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(
				[first, second].filter(
					(value): value is Promise<unknown> => value != null,
				),
			);
			replicas.restore();
			send.restore();
		}
	});

	it("rejects confirmations excluded by the final leader set", async () => {
		session = await TestSession.disconnected(3);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-final-leader-set");
		const confirmingKey = session.peers[1].identity.publicKey;
		const confirmingHash = confirmingKey.hashcode();
		const replacementHash = session.peers[2].identity.publicKey.hashcode();
		const originalLeaders = new Map([[confirmingHash, { intersecting: true }]]);
		const replacementLeaders = new Map([
			[replacementHash, { intersecting: true }],
		]);
		const send = sinon.stub(log.rpc, "send").resolves();
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({
				leaders: replacementLeaders,
				localLeader: false,
			});
		const remove = sinon.spy(log.log, "remove");
		let pruning: Promise<unknown> | undefined;

		try {
			[pruning] = log.prune(
				new Map([[entry.hash, { entry, leaders: originalLeaders }]]),
				{ timeout: 5_000 },
			);
			await waitForResolved(() => {
				expect(log._checkedPrune.getPendingDelete(entry.hash)).to.exist;
				expect(
					log._checkedPrune
						.getContactedReplicators(entry.hash)
						?.has(confirmingHash),
				).true;
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [{ hash: entry.hash, requestId: pending.requestId }],
				}),
				{ from: confirmingKey } as any,
			);

			await expect(pruning).to.be.rejectedWith("is leader again");
			expect(remove.called).false;
			expect(await log.log.has(entry.hash)).true;
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(pruning ? [pruning] : []);
			remove.restore();
			revalidate.restore();
			replicas.restore();
			send.restore();
		}
	});

	it("rejects confirmations from a peer whose unsubscribe has started", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-blocked-peer");
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const send = sinon.stub(log.rpc, "send").resolves();
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({ leaders, localLeader: false });
		const remove = sinon.spy(log.log, "remove");
		let pruning: Promise<unknown> | undefined;

		try {
			[pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			await waitForResolved(
				() =>
					expect(
						log._checkedPrune
							.getContactedReplicators(entry.hash)
							?.has(remoteHash),
					).true,
			);
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			log._replicationInfoBlockedPeers.add(remoteHash);
			// Model a response that was already admitted by the receive handler just
			// before unsubscribe blocked the peer. The final in-lane boundary must
			// still exclude that peer from the destructive quorum.
			await pending.resolve(remoteHash, pending.requestId);

			await expect(pruning).to.be.rejectedWith("is leader again");
			expect(remove.called).false;
			expect(await log.log.has(entry.hash)).true;
		} finally {
			log._replicationInfoBlockedPeers.delete(remoteHash);
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(pruning ? [pruning] : []);
			remove.restore();
			revalidate.restore();
			replicas.restore();
			send.restore();
		}
	});

	it("temporarily fences liveness removal without poisoning a cancelled handoff", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry: fencedEntry } = await db.add(
			"checked-prune-liveness-removal-fenced",
			{ meta: { next: [] } },
		);
		const { entry: resumedEntry } = await db.add(
			"checked-prune-liveness-removal-resumed",
			{ meta: { next: [] } },
		);
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const send = sinon.stub(log.rpc, "send").resolves();
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({ leaders, localLeader: false });
		const remove = sinon.spy(log.log, "remove");
		const releaseApplyQueue = pDefer<void>();
		const applyQueueStarted = pDefer<void>();
		const applyQueueBlocker = log.withReplicationInfoApplyQueue(
			remoteHash,
			async () => {
				applyQueueStarted.resolve();
				await releaseApplyQueue.promise;
			},
		);
		const drain = sinon.spy(log, "drainPeerReceiveHandlers");
		const observedActivityAt = log._replicatorLastActivityAt.get(remoteHash);
		let fencedPruning: Promise<unknown> | undefined;
		let resumedPruning: Promise<unknown> | undefined;
		let removing: Promise<boolean> | undefined;

		try {
			await applyQueueStarted.promise;
			[fencedPruning, resumedPruning] = log.prune(
				new Map([
					[fencedEntry.hash, { entry: fencedEntry, leaders }],
					[resumedEntry.hash, { entry: resumedEntry, leaders }],
				]),
				{ timeout: 5_000 },
			);
			await waitForResolved(() => {
				expect(
					log._checkedPrune
						.getContactedReplicators(fencedEntry.hash)
						?.has(remoteHash),
				).true;
				expect(
					log._checkedPrune
						.getContactedReplicators(resumedEntry.hash)
						?.has(remoteHash),
				).true;
			});
			const fencedPending = log._checkedPrune.getPendingDelete(
				fencedEntry.hash,
			);
			const resumedPending = log._checkedPrune.getPendingDelete(
				resumedEntry.hash,
			);

			removing = log.removeReplicator(remoteKey, {
				shouldRemove: () =>
					log._replicatorLastActivityAt.get(remoteHash) === observedActivityAt,
			});
			expect(log._receiveCleanupGateByPeer.has(remoteHash)).to.be.false;
			expect(log._checkedPrune.isPeerRemovalFenced(remoteHash)).to.be.true;

			// A receipt admitted just before/during the speculative removal cannot
			// authorize deletion while the liveness fence is active.
			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [
						{
							hash: fencedEntry.hash,
							requestId: fencedPending.requestId,
						},
					],
				}),
				{ from: remoteKey } as any,
			);

			await expect(fencedPruning).to.be.rejectedWith("is leader again");
			expect(remove.called).to.be.false;
			expect(await log.log.has(fencedEntry.hash)).to.be.true;

			releaseApplyQueue.resolve();
			expect(await removing).to.be.false;
			expect(log._receiveCleanupGateByPeer.has(remoteHash)).to.be.false;
			expect(log._checkedPrune.isPeerRemovalFenced(remoteHash)).to.be.false;
			expect(drain.called).to.be.false;
			await applyQueueBlocker;

			// Fresh activity cancelled the speculative removal, so it must not have
			// permanently revoked this peer from the still-active generation.
			expect(
				log._checkedPrune
					.getContactedReplicators(resumedEntry.hash)
					?.has(remoteHash),
			).true;
			expect(log._checkedPrune.getPendingDelete(resumedEntry.hash)).to.equal(
				resumedPending,
			);
			expect(log._checkedPrune.hasRetry(resumedEntry.hash)).to.be.false;
			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [
						{
							hash: resumedEntry.hash,
							requestId: resumedPending.requestId,
						},
					],
				}),
				{ from: remoteKey } as any,
			);
			await resumedPruning;
			expect(remove.calledOnce).true;
			expect(await log.log.has(resumedEntry.hash)).to.be.false;
		} finally {
			log.markReplicatorActivity(remoteHash);
			releaseApplyQueue.resolve();
			await Promise.allSettled([
				applyQueueBlocker,
				...(fencedPruning ? [fencedPruning] : []),
				...(resumedPruning ? [resumedPruning] : []),
				...(removing ? [removing] : []),
			]);
			drain.restore();
			remove.restore();
			revalidate.restore();
			replicas.restore();
			send.restore();
		}
	});

	it("restarts only background handoffs after a committed peer removal", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry: backgroundEntry } = await db.add(
			"checked-prune-peer-cleanup-background",
			{ meta: { next: [] } },
		);
		const { entry: explicitEntry } = await db.add(
			"checked-prune-peer-cleanup-explicit",
			{ meta: { next: [] } },
		);
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const send = sinon.stub(log.rpc, "send").resolves();
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const retry = sinon.spy(log, "scheduleCheckedPruneRetry");
		let backgroundPruning: Promise<unknown> | undefined;
		let explicitPruning: Promise<unknown> | undefined;

		try {
			log._checkedPrune.setRetry(backgroundEntry.hash, {
				attempts: 1,
				entry: backgroundEntry,
				leaders,
			});
			[backgroundPruning] = log.prune(
				new Map([[backgroundEntry.hash, { entry: backgroundEntry, leaders }]]),
			);
			const backgroundOutcome = backgroundPruning!.then(
				() => ({ error: undefined }),
				(error) => ({ error }),
			);
			[explicitPruning] = log.prune(
				new Map([[explicitEntry.hash, { entry: explicitEntry, leaders }]]),
				{ timeout: 5_000 },
			);
			const explicitOutcome = explicitPruning!.then(
				() => ({ error: undefined }),
				(error) => ({ error }),
			);

			await waitForResolved(() => {
				expect(
					log._checkedPrune
						.getContactedReplicators(backgroundEntry.hash)
						?.has(remoteHash),
				).true;
				expect(
					log._checkedPrune
						.getContactedReplicators(explicitEntry.hash)
						?.has(remoteHash),
				).true;
			});
			const backgroundRequestId = log._checkedPrune
				.getPendingDelete(backgroundEntry.hash)
				.requestId.slice();
			const explicitRequestId = log._checkedPrune
				.getPendingDelete(explicitEntry.hash)
				.requestId.slice();
			expect([...backgroundRequestId]).to.not.deep.equal([
				...explicitRequestId,
			]);

			expect(
				await log.removeReplicator(remoteKey, {
					shouldRemove: () => true,
				}),
			).to.be.true;

			const [backgroundResult, explicitResult] = await Promise.all([
				backgroundOutcome,
				explicitOutcome,
			]);
			expect(backgroundResult.error).to.be.instanceOf(Error);
			expect((backgroundResult.error as Error).message).to.include(
				"generation invalidated by peer cleanup",
			);
			expect(explicitResult.error).to.be.instanceOf(Error);
			expect((explicitResult.error as Error).message).to.include(
				"generation invalidated by peer cleanup",
			);

			expect(log._checkedPrune.getPendingDelete(backgroundEntry.hash)).to.be
				.undefined;
			expect(log._checkedPrune.getPendingDelete(explicitEntry.hash)).to.be
				.undefined;
			expect(await log.log.has(backgroundEntry.hash)).to.be.true;
			expect(await log.log.has(explicitEntry.hash)).to.be.true;

			// A committed removal cleans the peer in the mutation lane and again
			// while retiring its disconnected state. The generation must only be
			// invalidated and requeued once across those idempotent cleanup passes.
			expect(retry.calledOnce).to.be.true;
			expect(retry.firstCall.args[0].entry.hash).to.equal(backgroundEntry.hash);
			const retryState = log._checkedPrune.getRetry(backgroundEntry.hash);
			expect(retryState?.attempts).to.equal(2);
			expect(retryState?.entry.hash).to.equal(backgroundEntry.hash);
			expect(log._checkedPrune.hasPendingDelete(backgroundEntry.hash)).to.be
				.false;
			expect(log._checkedPrune.hasRetry(explicitEntry.hash)).to.be.false;
		} finally {
			log._checkedPrune.clearRetry(backgroundEntry.hash);
			log._checkedPrune.clearRetry(explicitEntry.hash);
			await log
				.cancelCheckedPruneForLocalLeader(backgroundEntry.hash)
				.catch(() => {});
			await log
				.cancelCheckedPruneForLocalLeader(explicitEntry.hash)
				.catch(() => {});
			await Promise.allSettled(
				[backgroundPruning, explicitPruning].filter(
					(value): value is Promise<unknown> => value != null,
				),
			);
			retry.restore();
			replicas.restore();
			send.restore();
		}
	});

	it("does not retain a retry when peer cleanup belongs to a stale lifecycle", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-stale-cleanup");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const send = sinon.stub(log.rpc, "send").resolves();
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const retry = sinon.spy(log, "scheduleCheckedPruneRetry");
		let pruning: Promise<unknown> | undefined;

		try {
			[pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]));
			const outcome = pruning!.then(
				() => ({ error: undefined }),
				(error) => ({ error }),
			);
			await waitForResolved(() => {
				expect(
					log._checkedPrune
						.getContactedReplicators(entry.hash)
						?.has(remoteHash),
				).true;
			});

			const staleLifecycle = log._repairLifecycleController;
			log.startRepairLifecycle();
			log.cleanupCheckedPrunePeer(
				remoteHash,
				staleLifecycle,
				log._checkedPrune,
			);

			const result = await outcome;
			expect(result.error).to.be.instanceOf(Error);
			expect((result.error as Error).message).to.include(
				"generation invalidated by peer cleanup",
			);
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
			expect(log._checkedPrune.hasRetry(entry.hash)).to.be.false;
			expect(retry.called).to.be.false;
			expect(await log.log.has(entry.hash)).to.be.true;
		} finally {
			log._checkedPrune.clearRetry(entry.hash);
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(pruning ? [pruning] : []);
			retry.restore();
			replicas.restore();
			send.restore();
		}
	});

	it("rearms ownership after a provisional targeted-repair keep", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-targeted-repair-keep");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const send = sinon.stub(log.rpc, "send").resolves();
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		let pruning: Promise<unknown> | undefined;

		try {
			[pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]));
			const outcome = pruning!.catch((error) => error);
			await waitForResolved(() => {
				expect(log._checkedPrune.getPendingDelete(entry.hash)).to.exist;
			});

			log.rearmCheckedPruneAfterTemporaryReceive(entry.hash);

			expect(await outcome).to.be.instanceOf(Error);
			expect(log._checkedPrune.hasPendingDelete(entry.hash)).to.be.false;
			expect(log._checkedPrune.getRetry(entry.hash)?.attempts).to.equal(1);
			expect(log._checkedPrune.getRetry(entry.hash)?.timer).to.exist;

			log.rearmCheckedPruneAfterTemporaryReceive("never-admitted-hash");
			expect(log.hasActiveCheckedPruneWork("never-admitted-hash")).to.be.false;
		} finally {
			log._checkedPrune.clearRetry(entry.hash);
			log.clearCheckedPruneAuditTimer();
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(pruning ? [pruning] : []);
			replicas.restore();
			send.restore();
		}
	});

	it("orders a replacement request after an admitted grant send", async () => {
		session = await TestSession.disconnected(3);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-grant-barrier");
		const prunePeer = session.peers[1].identity.publicKey.hashcode();
		const requester = session.peers[2].identity.publicKey.hashcode();
		const leaders = new Map([[prunePeer, { intersecting: true }]]);
		const responseEntered = pDefer<void>();
		const releaseResponse = pDefer<void>();
		const events: string[] = [];
		const send = sinon
			.stub(log.rpc, "send")
			.callsFake(async (message: unknown, options: any) => {
				if (message instanceof ResponseIPruneV2) {
					expect(options.mode).to.be.instanceOf(AcknowledgeDelivery);
					events.push("grant:start");
					responseEntered.resolve();
					await releaseResponse.promise;
					events.push("grant:done");
				} else if (message instanceof RequestIPruneV2) {
					events.push("request");
				}
			});
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const grantLeaders = sinon
			.stub(log, "revalidateCheckedPruneGrantLocalLeaders")
			.resolves(new Set([entry.hash]));
		let first: Promise<unknown> | undefined;
		let second: Promise<unknown> | undefined;

		try {
			[first] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			const firstOutcome = first!.catch((error) => {
				[second] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
					timeout: 5_000,
				});
				void second!.catch(() => {});
				return error;
			});
			await waitForResolved(() => {
				expect(events).to.deep.equal(["request"]);
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			const granting = log.admitAndSendCheckedPruneGrants(requester, [
				{ hash: entry.hash, requestId: randomBytes(32) },
			]);
			await responseEntered.promise;
			expect(await firstOutcome).to.be.instanceOf(Error);
			expect(second).to.exist;
			await Promise.resolve();
			await Promise.resolve();
			expect(events).to.deep.equal(["request", "grant:start"]);
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.not.equal(
				pending,
			);

			releaseResponse.resolve();
			await granting;
			await waitForResolved(() => {
				expect(events).to.deep.equal([
					"request",
					"grant:start",
					"grant:done",
					"request",
				]);
			});
		} finally {
			releaseResponse.resolve();
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(
				[first, second].filter(
					(value): value is Promise<unknown> => value != null,
				),
			);
			grantLeaders.restore();
			replicas.restore();
			send.restore();
		}
	});

	it("waits for grant barriers added while a request is already waiting", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-overlapping-barriers");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const firstGrant = pDefer<void>();
		const secondGrant = pDefer<void>();
		const requests: RequestIPruneV2[] = [];
		const send = sinon.stub(log.rpc, "send").callsFake(async (message) => {
			if (message instanceof RequestIPruneV2) {
				requests.push(message);
			}
		});
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const waitForGrants = sinon.spy(log._checkedPrune, "waitForGrantSends");
		let pruning: Promise<unknown> | undefined;

		try {
			log._checkedPrune.trackGrantSend([entry.hash], firstGrant.promise);
			[pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			void pruning!.catch(() => {});
			await waitForResolved(() => {
				expect(waitForGrants.calledWith(entry.hash)).to.be.true;
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(pending).to.exist;

			log._checkedPrune.trackGrantSend([entry.hash], secondGrant.promise);
			firstGrant.resolve();
			await Promise.resolve();
			await Promise.resolve();
			await Promise.resolve();
			expect(requests).to.be.empty;

			secondGrant.resolve();
			await waitForResolved(() => {
				expect(requests).to.have.length(1);
			});
			expect(requests[0]?.requests[0]?.requestId).to.deep.equal(
				pending.requestId,
			);
		} finally {
			firstGrant.resolve();
			secondGrant.resolve();
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(pruning ? [pruning] : []);
			waitForGrants.restore();
			replicas.restore();
			send.restore();
		}
	});

	it("rearms only background or queued work after an admitted grant is sent", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry: backgroundEntry } = await db.add(
			"checked-prune-grant-background",
			{ meta: { next: [] } },
		);
		const { entry: explicitEntry } = await db.add(
			"checked-prune-grant-explicit",
			{ meta: { next: [] } },
		);
		const { entry: queuedEntry } = await db.add("checked-prune-grant-queued", {
			meta: { next: [] },
		});
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const responseEntered = pDefer<void>();
		const releaseResponse = pDefer<void>();
		const send = sinon.stub(log.rpc, "send").callsFake(async (message) => {
			if (message instanceof ResponseIPruneV2) {
				responseEntered.resolve();
				await releaseResponse.promise;
			}
		});
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const grantLeaders = sinon
			.stub(log, "revalidateCheckedPruneGrantLocalLeaders")
			.resolves(
				new Set([backgroundEntry.hash, explicitEntry.hash, queuedEntry.hash]),
			);
		const retry = sinon.spy(log, "scheduleCheckedPruneRetry");
		let backgroundPruning: Promise<unknown> | undefined;
		let explicitPruning: Promise<unknown> | undefined;
		let queuedAdd: Promise<unknown> | undefined;

		try {
			[backgroundPruning] = log.prune(
				new Map([[backgroundEntry.hash, { entry: backgroundEntry, leaders }]]),
			);
			const backgroundOutcome = backgroundPruning!.catch((error) => error);
			[explicitPruning] = log.prune(
				new Map([[explicitEntry.hash, { entry: explicitEntry, leaders }]]),
				{ timeout: 5_000 },
			);
			const explicitOutcome = explicitPruning!.catch((error) => error);
			const queuedWorkToken = log._checkedPrune.trackCandidate(
				queuedEntry.hash,
				queuedEntry,
				leaders,
			);
			queuedAdd = log.pruneDebouncedFn.add({
				key: queuedEntry.hash,
				value: {
					entry: queuedEntry,
					leaders,
					workToken: queuedWorkToken,
				},
			});
			void queuedAdd!.catch(() => {});
			await waitForResolved(() => {
				expect(
					log._checkedPrune
						.getContactedReplicators(backgroundEntry.hash)
						?.has(remoteHash),
				).true;
				expect(
					log._checkedPrune
						.getContactedReplicators(explicitEntry.hash)
						?.has(remoteHash),
				).true;
			});

			const granting = log.admitAndSendCheckedPruneGrants(remoteHash, [
				{ hash: backgroundEntry.hash, requestId: randomBytes(32) },
				{ hash: explicitEntry.hash, requestId: randomBytes(32) },
				{ hash: queuedEntry.hash, requestId: randomBytes(32) },
			]);
			await responseEntered.promise;
			const overlappingGrant = log.admitAndSendCheckedPruneGrants(remoteHash, [
				{ hash: backgroundEntry.hash, requestId: randomBytes(32) },
				{ hash: queuedEntry.hash, requestId: randomBytes(32) },
			]);
			await waitForResolved(() => {
				expect(
					send
						.getCalls()
						.filter((call) => call.args[0] instanceof ResponseIPruneV2).length,
				).to.equal(2);
			});
			expect(await backgroundOutcome).to.be.instanceOf(Error);
			expect(await explicitOutcome).to.be.instanceOf(Error);
			expect(retry.called).to.be.false;

			releaseResponse.resolve();
			await Promise.all([granting, overlappingGrant]);
			expect(retry.callCount).to.equal(2);
			expect(
				retry
					.getCalls()
					.map((call) => call.args[0].entry.hash)
					.sort(),
			).to.deep.equal([backgroundEntry.hash, queuedEntry.hash].sort());
			expect(
				log._checkedPrune.getRetry(backgroundEntry.hash)?.attempts,
			).to.equal(1);
			expect(log._checkedPrune.getRetry(queuedEntry.hash)?.attempts).to.equal(
				1,
			);
			expect(log._checkedPrune.hasRetry(explicitEntry.hash)).to.be.false;
		} finally {
			releaseResponse.resolve();
			log._checkedPrune.clearRetry(backgroundEntry.hash);
			log._checkedPrune.clearRetry(explicitEntry.hash);
			log._checkedPrune.clearRetry(queuedEntry.hash);
			await Promise.all([
				log
					.cancelCheckedPruneForLocalLeader(backgroundEntry.hash)
					.catch(() => {}),
				log
					.cancelCheckedPruneForLocalLeader(explicitEntry.hash)
					.catch(() => {}),
				log.cancelCheckedPruneForLocalLeader(queuedEntry.hash).catch(() => {}),
			]);
			await Promise.allSettled(
				[backgroundPruning, explicitPruning, queuedAdd].filter(
					(value): value is Promise<unknown> => value != null,
				),
			);
			retry.restore();
			grantLeaders.restore();
			replicas.restore();
			send.restore();
		}
	});

	it("preserves both copies under reciprocal crossed handoffs", async () => {
		session = await TestSession.disconnected(2);
		const store = new EventStore();
		const openOptions = {
			args: {
				replicate: false as const,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		};
		const first = await session.peers[0].open(store.clone(), openOptions);
		const second = await session.peers[1].open(store.clone(), openOptions);
		const firstLog = first.log as any;
		const secondLog = second.log as any;
		const { entry } = await first.add("checked-prune-reciprocal-handoff");
		await second.log.log.join([entry]);
		const firstKey = session.peers[0].identity.publicKey;
		const secondKey = session.peers[1].identity.publicKey;
		const firstHash = firstKey.hashcode();
		const secondHash = secondKey.hashcode();
		const firstLeaders = new Map([[secondHash, { intersecting: true }]]);
		const secondLeaders = new Map([[firstHash, { intersecting: true }]]);
		const firstSent: unknown[] = [];
		const secondSent: unknown[] = [];
		const firstSend = sinon
			.stub(firstLog.rpc, "send")
			.callsFake(async (message) => {
				firstSent.push(message);
			});
		const secondSend = sinon
			.stub(secondLog.rpc, "send")
			.callsFake(async (message) => {
				secondSent.push(message);
			});
		const firstReplicas = sinon
			.stub(firstLog, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const secondReplicas = sinon
			.stub(secondLog, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const firstGrantLeaders = sinon
			.stub(firstLog, "revalidateCheckedPruneGrantLocalLeaders")
			.resolves(new Set([entry.hash]));
		const secondGrantLeaders = sinon
			.stub(secondLog, "revalidateCheckedPruneGrantLocalLeaders")
			.resolves(new Set([entry.hash]));
		const firstRemove = sinon.spy(firstLog.log, "remove");
		const secondRemove = sinon.spy(secondLog.log, "remove");
		let firstPruning: Promise<unknown> | undefined;
		let secondPruning: Promise<unknown> | undefined;

		try {
			[firstPruning] = firstLog.prune(
				new Map([[entry.hash, { entry, leaders: firstLeaders }]]),
			);
			expect(firstSent.some((message) => message instanceof RequestIPruneV2)).to
				.be.true;
			[secondPruning] = secondLog.prune(
				new Map([[entry.hash, { entry, leaders: secondLeaders }]]),
			);
			expect(secondSent.some((message) => message instanceof RequestIPruneV2))
				.to.be.true;
			const firstOutcome = firstPruning!.catch((error) => error);
			const secondOutcome = secondPruning!.catch((error) => error);
			await waitForResolved(() => {
				expect(firstSent.some((message) => message instanceof RequestIPruneV2))
					.to.be.true;
				expect(secondSent.some((message) => message instanceof RequestIPruneV2))
					.to.be.true;
			});
			const firstRequest = firstSent.find(
				(message): message is RequestIPruneV2 =>
					message instanceof RequestIPruneV2,
			)!;
			const secondRequest = secondSent.find(
				(message): message is RequestIPruneV2 =>
					message instanceof RequestIPruneV2,
			)!;

			await Promise.all([
				second.log.onMessage(firstRequest, { from: firstKey } as any),
				first.log.onMessage(secondRequest, { from: secondKey } as any),
			]);
			expect(await firstOutcome).to.be.instanceOf(Error);
			expect(await secondOutcome).to.be.instanceOf(Error);

			const firstResponse = firstSent.find(
				(message): message is ResponseIPruneV2 =>
					message instanceof ResponseIPruneV2,
			)!;
			const secondResponse = secondSent.find(
				(message): message is ResponseIPruneV2 =>
					message instanceof ResponseIPruneV2,
			)!;
			expect(firstResponse).to.exist;
			expect(secondResponse).to.exist;
			await Promise.all([
				second.log.onMessage(firstResponse, { from: firstKey } as any),
				first.log.onMessage(secondResponse, { from: secondKey } as any),
			]);

			expect(firstRemove.called).to.be.false;
			expect(secondRemove.called).to.be.false;
			expect(await firstLog.log.has(entry.hash)).to.be.true;
			expect(await secondLog.log.has(entry.hash)).to.be.true;
			expect(firstLog._checkedPrune.getRetry(entry.hash)?.attempts).to.equal(1);
			expect(secondLog._checkedPrune.getRetry(entry.hash)?.attempts).to.equal(
				1,
			);
		} finally {
			await Promise.all([
				firstLog.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {}),
				secondLog.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {}),
			]);
			await Promise.allSettled(
				[firstPruning, secondPruning].filter(
					(value): value is Promise<unknown> => value != null,
				),
			);
			secondRemove.restore();
			firstRemove.restore();
			secondGrantLeaders.restore();
			firstGrantLeaders.restore();
			secondReplicas.restore();
			firstReplicas.restore();
			secondSend.restore();
			firstSend.restore();
		}
	});

	it("revokes an old receipt when that peer requests a reciprocal prune", async () => {
		session = await TestSession.disconnected(3);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-crossed-receipt");
		const remoteKeys = session.peers
			.slice(1)
			.map((peer) => peer.identity.publicKey);
		const remoteHashes = remoteKeys.map((key) => key.hashcode());
		const leaders = new Map(
			remoteHashes.map((hash) => [hash, { intersecting: true }]),
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 2 });
		const grantLeaders = sinon
			.stub(log, "revalidateCheckedPruneGrantLocalLeaders")
			.resolves(new Set());
		const remove = sinon.spy(log.log, "remove");
		let pruning: Promise<unknown> | undefined;

		try {
			[pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			void pruning!.catch(() => {});
			await waitForResolved(() => {
				expect(
					log._checkedPrune.getContactedReplicators(entry.hash)?.size,
				).to.equal(2);
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);

			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [{ hash: entry.hash, requestId: pending.requestId }],
				}),
				{ from: remoteKeys[0] } as any,
			);
			expect(
				log._checkedPrune.getConfirmedReplicators(entry.hash),
			).to.deep.equal(new Set([remoteHashes[0]]));

			await db.log.onMessage(
				new RequestIPruneV2({
					requests: [{ hash: entry.hash, requestId: randomBytes(32) }],
				}),
				{ from: remoteKeys[0] } as any,
			);
			expect(
				log._checkedPrune
					.getContactedReplicators(entry.hash)
					?.has(remoteHashes[0]),
			).to.be.false;
			expect(
				log._checkedPrune.getConfirmedReplicators(entry.hash)?.size ?? 0,
			).to.equal(0);

			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [{ hash: entry.hash, requestId: pending.requestId }],
				}),
				{ from: remoteKeys[0] } as any,
			);
			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [{ hash: entry.hash, requestId: pending.requestId }],
				}),
				{ from: remoteKeys[1] } as any,
			);
			expect(
				log._checkedPrune.getConfirmedReplicators(entry.hash),
			).to.deep.equal(new Set([remoteHashes[1]]));
			expect(remove.called).to.be.false;
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(pruning ? [pruning] : []);
			remove.restore();
			grantLeaders.restore();
			replicas.restore();
			send.restore();
		}
	});

	it("grants only while the block is present and this peer is a leader", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-grant-revalidation");
		const requester = session.peers[1].identity.publicKey.hashcode();
		const send = sinon.stub(log.rpc, "send").resolves();
		const leaders = sinon.stub(log, "revalidateCheckedPruneGrantLocalLeaders");
		leaders.resolves(new Set());

		try {
			await log.admitAndSendCheckedPruneGrants(requester, [
				{ hash: entry.hash, requestId: randomBytes(32) },
			]);
			await log.admitAndSendCheckedPruneGrants(requester, [
				{ hash: "missing", requestId: randomBytes(32) },
			]);
			expect(
				send
					.getCalls()
					.some((call) => call.args[0] instanceof ResponseIPruneV2),
			).false;

			leaders.resolves(new Set([entry.hash]));
			const requestId = randomBytes(32);
			await log.admitAndSendCheckedPruneGrants(requester, [
				{ hash: entry.hash, requestId },
			]);
			const response = send
				.getCalls()
				.map((call) => call.args[0])
				.find(
					(message): message is ResponseIPruneV2 =>
						message instanceof ResponseIPruneV2,
				);
			expect(response?.requests).to.have.length(1);
			expect(response?.requests[0]?.hash).to.equal(entry.hash);
			expect(response?.requests[0]?.requestId).to.deep.equal(requestId);
		} finally {
			leaders.restore();
			send.restore();
		}
	});

	it("echoes the exact id when a missing block arrives before pending install", async () => {
		session = await TestSession.disconnected(2);
		const store = new EventStore();
		const source = await session.peers[0].open(store.clone(), {
			args: { replicate: false, keep: () => true, timeUntilRoleMaturity: 0 },
		});
		const target = await session.peers[1].open(store.clone(), {
			args: { replicate: false, keep: () => true, timeUntilRoleMaturity: 0 },
		});
		const { entry } = await source.add("checked-prune-pending-arrival-race");
		const sourceKey = session.peers[0].identity.publicKey;
		const targetLog = target.log as any;
		const requestId = randomBytes(32);
		const originalHasMany = target.log.log.blocks.hasMany!.bind(
			target.log.log.blocks,
		);
		let firstPresenceCheck = true;
		const hasMany = sinon
			.stub(target.log.log.blocks, "hasMany")
			.callsFake(async (hashes) => {
				if (!firstPresenceCheck) {
					return originalHasMany(hashes);
				}
				firstPresenceCheck = false;
				await target.log.log.join([entry]);
				return [false];
			});
		const leaders = sinon
			.stub(targetLog, "revalidateCheckedPruneGrantLocalLeaders")
			.resolves(new Set([entry.hash]));
		const responses: ResponseIPruneV2[] = [];
		const send = sinon
			.stub(targetLog.rpc, "send")
			.callsFake(async (message) => {
				if (message instanceof ResponseIPruneV2) {
					responses.push(message);
				}
			});

		try {
			await target.log.onMessage(
				new RequestIPruneV2({
					requests: [{ hash: entry.hash, requestId }],
				}),
				{ from: sourceKey } as any,
			);
			await waitForResolved(() => {
				expect(responses).to.have.length(1);
				expect(targetLog._pendingIHave.has(entry.hash)).to.be.false;
			});
			expect(responses[0]?.requests).to.have.length(1);
			expect(responses[0]?.requests[0]?.hash).to.equal(entry.hash);
			expect(responses[0]?.requests[0]?.requestId).to.deep.equal(requestId);
		} finally {
			send.restore();
			leaders.restore();
			hasMany.restore();
		}
	});

	it("prunes newly exposed ancestors through independent generations", async () => {
		session = await TestSession.disconnected(2);
		let keep = true;
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => keep,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const parent = await db.add("checked-prune-parent", {
			meta: { next: [] },
		});
		const child = await db.add("checked-prune-child", {
			meta: { next: [parent.entry] },
		});
		keep = false;
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const sent: RequestIPruneV2[] = [];
		const send = sinon
			.stub(log.rpc, "send")
			.callsFake(async (message: unknown) => {
				if (message instanceof RequestIPruneV2) {
					sent.push(message);
				}
			});
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({ leaders, localLeader: false });
		const parentPlan = sinon
			.stub(log, "planEntryLeaderBatch")
			.resolves([{ coordinates: [], leaders, isLeader: false }]);

		try {
			const [pruning] = log.prune(
				new Map([[child.entry.hash, { entry: child.entry, leaders }]]),
				{ timeout: 5_000 },
			);
			await waitForResolved(() => {
				expect(log._checkedPrune.getPendingDelete(child.entry.hash)).to.exist;
				expect(
					log._checkedPrune
						.getContactedReplicators(child.entry.hash)
						?.has(remoteHash),
				).true;
			});
			const childPending = log._checkedPrune.getPendingDelete(child.entry.hash);
			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [
						{
							hash: child.entry.hash,
							requestId: childPending.requestId,
						},
					],
				}),
				{ from: remoteKey } as any,
			);
			await pruning;
			expect(await log.log.has(child.entry.hash)).false;
			expect(await log.log.has(parent.entry.hash)).true;

			await log.pruneDebouncedFn.flush();
			await waitForResolved(() => {
				expect(log._checkedPrune.getPendingDelete(parent.entry.hash)).to.exist;
			});
			const parentPending = log._checkedPrune.getPendingDelete(
				parent.entry.hash,
			);
			expect([...parentPending.requestId]).to.not.deep.equal([
				...childPending.requestId,
			]);
			expect(
				sent.flatMap((message) => message.requests).map(({ hash }) => hash),
			).to.include.members([child.entry.hash, parent.entry.hash]);
			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [
						{
							hash: parent.entry.hash,
							requestId: parentPending.requestId,
						},
					],
				}),
				{ from: remoteKey } as any,
			);
			await parentPending.promise.promise;
			expect(await log.log.has(parent.entry.hash)).false;
		} finally {
			await log
				.cancelCheckedPruneForLocalLeader(parent.entry.hash)
				.catch(() => {});
			parentPlan.restore();
			revalidate.restore();
			replicas.restore();
			send.restore();
		}
	});

	it("requeues a background prune after its timed-out generation clears", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-timeout-retry");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const send = sinon.stub(log.rpc, "send").resolves();
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const retry = sinon.stub(log, "scheduleCheckedPruneRetry").callsFake(() => {
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
		});
		const sentRequests: RequestIPruneV2[] = [];
		send.callsFake(async (message) => {
			if (message instanceof RequestIPruneV2) {
				sentRequests.push(message);
			}
		});
		const clock = sinon.useFakeTimers();
		let pruning: Promise<unknown> | undefined;

		try {
			[pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]));
			const outcome = pruning!.catch((error) => error);
			await Promise.resolve();
			await Promise.resolve();
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(pending).to.exist;
			const generationId = pending.requestId.slice();

			await clock.tickAsync(120_000);
			const error = await outcome;
			expect(error).to.be.instanceOf(Error);
			expect((error as Error).message).to.include(
				"Timeout for checked pruning",
			);
			expect(retry.calledOnce).to.be.true;
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
			expect(sentRequests.length).to.be.greaterThan(1);
			for (const request of sentRequests) {
				expect(request.requests[0]?.requestId).to.deep.equal(generationId);
			}
		} finally {
			clock.restore();
			await Promise.allSettled(pruning ? [pruning] : []);
			retry.restore();
			replicas.restore();
			send.restore();
		}
	});

	it("moves exhausted retries to one coalesced low-rate audit", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-retry-exhaustion");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const clock = sinon.useFakeTimers();

		try {
			log._checkedPrune.setRetry(entry.hash, {
				attempts: 3,
				entry,
				leaders,
			});
			expect(log._checkedPrune.hasRetry(entry.hash)).to.be.true;

			log.scheduleCheckedPruneRetry({ entry, leaders });

			expect(log._checkedPrune.hasRetry(entry.hash)).to.be.true;
			expect(log._checkedPrune.getRetry(entry.hash)?.timer).to.be.undefined;
			expect(log._checkedPruneAuditTimer).to.exist;
			expect(log.hasActiveCheckedPruneWork(entry.hash)).to.be.true;

			await clock.tickAsync(30_000);
			expect(log._checkedPruneAuditTimer).to.be.undefined;
			expect(log._checkedPrune.getRetry(entry.hash)?.attempts).to.equal(3);
			expect(log._checkedPrune.getRetry(entry.hash)?.timer).to.exist;
		} finally {
			log._checkedPrune.clearRetry(entry.hash);
			log.clearCheckedPruneAuditTimer();
			clock.restore();
		}
	});

	it("rotates the coalesced audit fairly beyond one batch", async () => {
		session = await TestSession.disconnected(1);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-audit-fairness");
		const leaders = new Map<string, { intersecting: boolean }>();
		const hashes = Array.from(
			{ length: 129 },
			(_value, index) => `checked-prune-audit-${index}`,
		);
		const clock = sinon.useFakeTimers();

		try {
			for (const hash of hashes) {
				log._checkedPrune.setRetry(hash, {
					attempts: 3,
					entry: { hash, meta: entry.meta },
					leaders,
				});
			}
			log.scheduleCheckedPruneRetry({
				entry: log._checkedPrune.getRetry(hashes[0])!.entry,
				leaders,
			});

			await clock.tickAsync(30_000);
			expect(log._checkedPrune.getRetry(hashes[0])?.timer).to.exist;
			expect(log._checkedPrune.getRetry(hashes[127])?.timer).to.exist;
			expect(log._checkedPrune.getRetry(hashes[128])?.timer).to.be.undefined;
			expect(log._checkedPruneAuditTimer).to.exist;

			for (const hash of hashes.slice(0, 128)) {
				const state = log._checkedPrune.getRetry(hash);
				if (state?.timer) {
					clearTimeout(state.timer);
					state.timer = undefined;
				}
			}
			await clock.tickAsync(30_000);
			expect(log._checkedPrune.getRetry(hashes[128])?.timer).to.exist;
		} finally {
			for (const hash of hashes) {
				log._checkedPrune.clearRetry(hash);
			}
			log.clearCheckedPruneAuditTimer();
			clock.restore();
		}
	});

	it("releases a background retry rejected by the keep policy", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-retry-keep");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(leaders);
		const clock = sinon.useFakeTimers();

		try {
			log.scheduleCheckedPruneRetry({ entry, leaders });
			expect(log._checkedPrune.hasRetry(entry.hash)).to.be.true;

			await clock.tickAsync(1_500);

			expect(log._checkedPrune.hasRetry(entry.hash)).to.be.false;
			expect(log.hasActiveCheckedPruneWork(entry.hash)).to.be.false;
		} finally {
			clock.restore();
			log._checkedPrune.clearRetry(entry.hash);
			findLeaders.restore();
		}
	});

	it("rechecks a first transient local-leader decision", async () => {
		session = await TestSession.disconnected(2);
		let keep = true;
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => keep,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-transient-local-leader");
		const selfHash = session.peers[0].identity.publicKey.hashcode();
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const localLeaders = new Map([[selfHash, { intersecting: true }]]);
		const remoteLeaders = new Map([[remoteHash, { intersecting: true }]]);
		const revalidate = sinon.stub(log, "revalidateCheckedPruneOwnership");
		revalidate.onFirstCall().resolves({
			leaders: localLeaders,
			localLeader: true,
		});
		revalidate.onSecondCall().resolves({
			leaders: remoteLeaders,
			localLeader: false,
		});
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(remoteLeaders);
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const send = sinon.stub(log.rpc, "send").resolves();
		const clock = sinon.useFakeTimers();

		try {
			keep = false;
			expect(
				await log.pruneDebouncedFnAddIfNotKeeping({
					key: entry.hash,
					value: { entry, leaders: remoteLeaders },
				}),
			).to.be.true;
			await log.pruneDebouncedFn.flush();

			expect(revalidate.calledOnce).to.be.true;
			expect(log._checkedPrune.hasPendingDelete(entry.hash)).to.be.false;
			expect(log._checkedPrune.getRetry(entry.hash)?.attempts).to.equal(1);

			await clock.tickAsync(1_500);
			await log.pruneDebouncedFn.flush();

			expect(revalidate.callCount).to.equal(2);
			expect(log._checkedPrune.hasPendingDelete(entry.hash)).to.be.true;
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			log._checkedPrune.clearRetry(entry.hash);
			log.clearCheckedPruneAuditTimer();
			clock.restore();
			send.restore();
			replicas.restore();
			findLeaders.restore();
			revalidate.restore();
		}
	});

	it("retries an empty transient leader view instead of dropping it", async () => {
		session = await TestSession.disconnected(1);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-empty-leader-view");
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(new Map());

		try {
			await log.pruneCurrentHeadsNoLongerLed();

			expect(findLeaders.called).to.be.true;
			expect(log._checkedPrune.getRetry(entry.hash)?.attempts).to.equal(1);
			expect(log._checkedPrune.getRetry(entry.hash)?.timer).to.exist;
		} finally {
			log._checkedPrune.clearRetry(entry.hash);
			log.clearCheckedPruneAuditTimer();
			findLeaders.restore();
		}
	});

	it("does not enqueue a retry superseded during leader planning", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-retry-superseded");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const planningStarted = pDefer<void>();
		const releasePlanning = pDefer<void>();
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.callsFake(async () => {
				planningStarted.resolve();
				await releasePlanning.promise;
				return leaders;
			});
		const enqueue = sinon.spy(log, "pruneDebouncedFnAddIfNotKeeping");
		const clock = sinon.useFakeTimers();

		try {
			log.scheduleCheckedPruneRetry({ entry, leaders });
			await clock.tickAsync(1_500);
			await planningStarted.promise;

			log._checkedPrune.clearRetry(entry.hash);
			releasePlanning.resolve();
			await Promise.resolve();
			await Promise.resolve();
			await Promise.resolve();

			expect(enqueue.called).to.be.false;
			expect(log._checkedPrune.hasRetry(entry.hash)).to.be.false;
			expect(log.hasActiveCheckedPruneWork(entry.hash)).to.be.false;
		} finally {
			releasePlanning.resolve();
			clock.restore();
			log._checkedPrune.clearRetry(entry.hash);
			enqueue.restore();
			findLeaders.restore();
		}
	});

	it("does not resume a debounced candidate superseded during planning", async () => {
		session = await TestSession.disconnected(2);
		let keep = true;
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => keep,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-candidate-superseded");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const planningStarted = pDefer<void>();
		const releasePlanning = pDefer<void>();
		const isReplicating = sinon
			.stub(log, "isReplicating")
			.callsFake(async () => {
				planningStarted.resolve();
				await releasePlanning.promise;
				return true;
			});
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({ leaders, localLeader: false });
		let flushing: Promise<unknown> | undefined;

		try {
			keep = false;
			expect(
				await log.pruneDebouncedFnAddIfNotKeeping({
					key: entry.hash,
					value: { entry, leaders },
				}),
			).to.be.true;
			flushing = log.pruneDebouncedFn.flush();
			await planningStarted.promise;

			await log.cancelCheckedPruneForLocalLeader(entry.hash);
			releasePlanning.resolve();
			await flushing;

			expect(revalidate.called).to.be.false;
			expect(log._checkedPrune.hasPendingDelete(entry.hash)).to.be.false;
			expect(log._checkedPrune.hasRetry(entry.hash)).to.be.false;
			expect(log.hasActiveCheckedPruneWork(entry.hash)).to.be.false;
		} finally {
			releasePlanning.resolve();
			await Promise.allSettled(flushing ? [flushing] : []);
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			revalidate.restore();
			isReplicating.restore();
		}
	});

	it("rearms an in-flight candidate after an admitted grant is sent", async () => {
		session = await TestSession.disconnected(2);
		let keep = true;
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => keep,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-in-flight-grant");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const planningStarted = pDefer<void>();
		const releasePlanning = pDefer<void>();
		const isReplicating = sinon
			.stub(log, "isReplicating")
			.callsFake(async () => {
				planningStarted.resolve();
				await releasePlanning.promise;
				return true;
			});
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({ leaders, localLeader: false });
		const grantLeaders = sinon
			.stub(log, "revalidateCheckedPruneGrantLocalLeaders")
			.resolves(new Set([entry.hash]));
		const send = sinon.stub(log.rpc, "send").resolves();
		const retry = sinon.spy(log, "scheduleCheckedPruneRetry");
		let flushing: Promise<unknown> | undefined;

		try {
			keep = false;
			expect(
				await log.pruneDebouncedFnAddIfNotKeeping({
					key: entry.hash,
					value: { entry, leaders },
				}),
			).to.be.true;
			flushing = log.pruneDebouncedFn.flush();
			await planningStarted.promise;

			expect(log.pruneDebouncedFn.has(entry.hash)).to.be.false;
			expect(log._checkedPrune.hasCandidate(entry.hash)).to.be.true;

			await log.admitAndSendCheckedPruneGrants(remoteHash, [
				{ hash: entry.hash, requestId: randomBytes(32) },
			]);
			expect(retry.calledOnce).to.be.true;
			expect(log._checkedPrune.getRetry(entry.hash)?.attempts).to.equal(1);

			releasePlanning.resolve();
			await flushing;

			expect(revalidate.called).to.be.false;
			expect(log._checkedPrune.hasPendingDelete(entry.hash)).to.be.false;
			expect(log._checkedPrune.hasRetry(entry.hash)).to.be.true;
		} finally {
			releasePlanning.resolve();
			log._checkedPrune.clearRetry(entry.hash);
			await Promise.allSettled(flushing ? [flushing] : []);
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			retry.restore();
			send.restore();
			grantLeaders.restore();
			revalidate.restore();
			isReplicating.restore();
		}
	});

	it("fails closed for legacy prune messages", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				keep: () => true,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-legacy");
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const send = sinon.stub(log.rpc, "send").resolves();
		const replicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		let pruning: Promise<unknown> | undefined;

		try {
			[pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			const outcome = pruning!.catch((error) => error);
			await waitForResolved(() => {
				expect(log._checkedPrune.getPendingDelete(entry.hash)).to.exist;
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			const sendCountBeforeLegacy = send.callCount;
			const pendingIHaveBeforeLegacy = [...log._pendingIHave.keys()];
			const contactedBeforeLegacy = new Set(
				log._checkedPrune.getContactedReplicators(entry.hash),
			);
			const confirmedBeforeLegacy = new Set(
				log._checkedPrune.getConfirmedReplicators(entry.hash),
			);
			await db.log.onMessage(new RequestIPrune({ hashes: [entry.hash] }), {
				from: remoteKey,
			} as any);
			await db.log.onMessage(new ResponseIPrune({ hashes: [entry.hash] }), {
				from: remoteKey,
			} as any);
			expect(log._checkedPrune.getPendingDelete(entry.hash)).equal(pending);
			expect(
				log._checkedPrune.getConfirmedReplicators(entry.hash)?.size ?? 0,
			).equal(0);
			expect(send.callCount).to.equal(sendCountBeforeLegacy);
			expect([...log._pendingIHave.keys()]).to.deep.equal(
				pendingIHaveBeforeLegacy,
			);
			expect(
				log._checkedPrune.getContactedReplicators(entry.hash),
			).to.deep.equal(contactedBeforeLegacy);
			expect(
				new Set(log._checkedPrune.getConfirmedReplicators(entry.hash)),
			).to.deep.equal(confirmedBeforeLegacy);

			await log.cancelCheckedPruneForLocalLeader(entry.hash);
			expect(await outcome).to.be.instanceOf(Error);
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(pruning ? [pruning] : []);
			replicas.restore();
			send.restore();
		}
	});
});
