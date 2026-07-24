import {
	Ed25519Keypair,
	fromHexString,
	randomBytes,
	toHexString,
} from "@peerbit/crypto";
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
	SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
} from "../src/exchange-heads.js";
import { AbsoluteReplicas } from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

describe("checked prune correlated handoff", () => {
	let session: TestSession | undefined;

	afterEach(async () => {
		await session?.stop();
		session = undefined;
	});

	it("orders a later prune request after an admitted grant send", async () => {
		session = await TestSession.disconnected(3);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-grant-send-barrier");
		const prunePeer = session.peers[1].identity.publicKey.hashcode();
		const requester = session.peers[2].identity.publicKey.hashcode();
		const leaders = new Map([[prunePeer, { intersecting: true }]]);
		const responseSendEntered = pDefer<void>();
		const releaseResponseSend = pDefer<void>();
		const events: string[] = [];

		log._peerSyncCapabilities.set(
			prunePeer,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon
			.stub(log.rpc, "send")
			.callsFake(async (message: unknown, options: any) => {
				if (message instanceof ResponseIPruneV2) {
					expect(options.mode).to.be.instanceOf(AcknowledgeDelivery);
					events.push("grant:start");
					responseSendEntered.resolve();
					await releaseResponseSend.promise;
					events.push("grant:done");
					return;
				}
				if (message instanceof RequestIPruneV2) {
					events.push("request");
				}
			});
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const revalidateGrantLeader = sinon
			.stub(log, "revalidateCheckedPruneGrantLocalLeaders")
			.resolves(new Set([entry.hash]));

		let firstPrune: Promise<unknown> | undefined;
		let secondPrune: Promise<unknown> | undefined;
		let secondOutcome: Promise<unknown> | undefined;
		try {
			[firstPrune] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			const firstOutcome = firstPrune!.catch((error) => {
				// Start the replacement directly from the rejection reaction. This
				// is the earliest a cancelled session can be replaced and catches
				// any await-before-barrier ordering gap in grant admission.
				[secondPrune] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
					timeout: 5_000,
				});
				secondOutcome = secondPrune!.catch((secondError) => secondError);
				return error;
			});
			await waitForResolved(() => {
				expect(events.filter((event) => event === "request")).to.have.length(1);
			});
			const firstPending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(firstPending).to.exist;

			const granting = log.admitCheckedPruneGrants(
				[
					{
						hash: entry.hash,
						requestId: randomBytes(32),
					},
				],
				requester,
			);
			await responseSendEntered.promise;
			expect(await firstOutcome).to.be.instanceOf(Error);
			expect(secondPrune).to.exist;
			expect(
				log._checkedPrune.getPendingDelete(entry.hash),
			).to.exist.and.not.equal(firstPending);
			let unrelatedMutationRan = false;
			await log.withReplicationRangeMutationQueue(async () => {
				unrelatedMutationRan = true;
			});
			expect(unrelatedMutationRan).to.be.true;
			await Promise.resolve();
			await Promise.resolve();
			expect(events).to.deep.equal(["request", "grant:start"]);

			releaseResponseSend.resolve();
			await granting;
			await waitForResolved(() => {
				expect(events.filter((event) => event === "request")).to.have.length(2);
			});
			expect(events).to.deep.equal([
				"request",
				"grant:start",
				"grant:done",
				"request",
			]);

			await log.cancelCheckedPruneForLocalLeader(entry.hash);
			expect(await secondOutcome!).to.be.instanceOf(Error);
		} finally {
			releaseResponseSend.resolve();
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(
				[firstPrune, secondPrune].filter(
					(value): value is Promise<unknown> => value != null,
				),
			);
			getClampedReplicas.restore();
			revalidateGrantLeader.restore();
			send.restore();
		}
	});

	it("drains an admitted grant before a reopened lifecycle can request pruning", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-grant-close-reopen");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const responseSendEntered = pDefer<void>();
		const releaseResponseSend = pDefer<void>();
		const events: string[] = [];
		const send = sinon
			.stub(log.rpc, "send")
			.callsFake(async (message: unknown) => {
				if (message instanceof ResponseIPruneV2) {
					events.push("grant:start");
					responseSendEntered.resolve();
					// Deliberately model a started transport that cannot be cancelled.
					await releaseResponseSend.promise;
					events.push("grant:done");
					return;
				}
				if (message instanceof RequestIPruneV2) {
					events.push("request");
				}
			});
		const finalLeaderPlan = sinon
			.stub(log, "revalidateCheckedPruneGrantLocalLeaders")
			.resolves(new Set([entry.hash]));
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		let reopenedPrune: Promise<unknown> | undefined;

		try {
			const granting = log.admitCheckedPruneGrants(
				[{ hash: entry.hash, requestId: randomBytes(32) }],
				remoteHash,
			);
			await responseSendEntered.promise;

			let closeSettled = false;
			const closing = db.close().finally(() => {
				closeSettled = true;
			});
			await new Promise<void>((resolve) => setTimeout(resolve, 25));
			expect(closeSettled).to.be.false;
			expect(events).to.deep.equal(["grant:start"]);

			releaseResponseSend.resolve();
			await Promise.all([granting, closing]);
			expect(events).to.deep.equal(["grant:start", "grant:done"]);

			await session.peers[0].open(db, {
				args: {
					replicate: false,
					timeUntilRoleMaturity: 0,
					waitForPruneDelay: 0,
				},
			});
			log._peerSyncCapabilities.set(
				remoteHash,
				SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
			);
			[reopenedPrune] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			void reopenedPrune!.catch(() => {});
			await waitForResolved(() => {
				expect(events).to.deep.equal(["grant:start", "grant:done", "request"]);
			});
		} finally {
			releaseResponseSend.resolve();
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(reopenedPrune ? [reopenedPrune] : []);
			getClampedReplicas.restore();
			finalLeaderPlan.restore();
			send.restore();
			log._replicationRangeMutationFailure = undefined;
		}
	});

	it("retries an acknowledged grant after its first delivery fails", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-grant-ack-retry");
		const requester = session.peers[1].identity.publicKey.hashcode();
		const request = { hash: entry.hash, requestId: randomBytes(32) };
		const send = sinon.stub(log.rpc, "send");
		const revalidateGrantLeader = sinon
			.stub(log, "revalidateCheckedPruneGrantLocalLeaders")
			.resolves(new Set([entry.hash]));
		send.onFirstCall().rejects(new Error("grant ACK failed"));
		send.onSecondCall().resolves();

		try {
			await expect(
				log.admitCheckedPruneGrants([request], requester),
			).to.be.rejectedWith("grant ACK failed");
			expect(log._checkedPrune.grantSends?.size ?? 0).to.equal(0);

			expect(
				await log.admitCheckedPruneGrants([request], requester),
			).to.deep.equal([entry.hash]);
			expect(send.callCount).to.equal(2);
			for (const call of send.getCalls()) {
				expect(call.args[0]).to.be.instanceOf(ResponseIPruneV2);
				expect(call.args[1].mode).to.be.instanceOf(AcknowledgeDelivery);
				expect(call.args[1].mode.to).to.deep.equal([requester]);
			}
		} finally {
			revalidateGrantLeader.restore();
			send.restore();
		}
	});

	it("rejects a stale leader plan and a missing block at final grant admission", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-final-grant-leader");
		const requesterKey = session.peers[1].identity.publicKey;
		const requester = requesterKey.hashcode();
		const leaders = new Map([[requester, { intersecting: true }]]);
		log._peerSyncCapabilities.set(
			requester,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const initialLeaderPlan = sinon
			.stub(log, "planCurrentNativeBackboneRequestPruneLeaderHints")
			.resolves({
				localLeaderHashes: new Set(),
				replicaCounts: new Map(),
				peerHistoryGids: [],
				peerHistoryRemovedHashes: new Set(),
				nativeAllConfirmed: true,
				nativeBackbonePeerHistoryCleaned: true,
			});
		const finalLeaderPlan = sinon
			.stub(log, "revalidateCheckedPruneGrantLocalLeaders")
			.resolves(new Set());
		let pruning: Promise<unknown> | undefined;
		let hasMany: sinon.SinonStub | undefined;

		try {
			[pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			void pruning!.catch(() => {});
			await waitForResolved(() => {
				expect(
					send
						.getCalls()
						.some((call) => call.args[0] instanceof RequestIPruneV2),
				).to.be.true;
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(pending).to.exist;

			await db.log.onMessage(
				new RequestIPruneV2({
					requests: [{ hash: entry.hash, requestId: randomBytes(32) }],
				}),
				{ from: requesterKey } as any,
			);
			expect(initialLeaderPlan.calledOnce).to.be.true;
			expect(finalLeaderPlan.calledOnce).to.be.true;
			expect(
				send
					.getCalls()
					.some((call) => call.args[0] instanceof ResponseIPruneV2),
			).to.be.false;
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.equal(pending);

			finalLeaderPlan.resolves(new Set([entry.hash]));
			hasMany = sinon.stub(log.log.blocks, "hasMany").resolves([false]);
			await db.log.onMessage(
				new RequestIPruneV2({
					requests: [{ hash: entry.hash, requestId: randomBytes(32) }],
				}),
				{ from: requesterKey } as any,
			);
			expect(
				send
					.getCalls()
					.some((call) => call.args[0] instanceof ResponseIPruneV2),
			).to.be.false;
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.equal(pending);
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(pruning ? [pruning] : []);
			hasMany?.restore();
			finalLeaderPlan.restore();
			initialLeaderPlan.restore();
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("bounds grant sends instance-wide and aborts queued delivery at its deadline", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const entries = await Promise.all(
			Array.from({ length: 5 }, (_, index) =>
				db.add(`checked-prune-grant-queue-${index}`),
			),
		);
		const requester = session.peers[1].identity.publicKey.hashcode();
		const releaseSends = pDefer<void>();
		let activeSends = 0;
		let maxActiveSends = 0;
		const sentHashes: string[] = [];
		const send = sinon
			.stub(log.rpc, "send")
			.callsFake(async (message: unknown) => {
				if (!(message instanceof ResponseIPruneV2)) {
					return;
				}
				sentHashes.push(...message.requests.map(({ hash }) => hash));
				activeSends++;
				maxActiveSends = Math.max(maxActiveSends, activeSends);
				await releaseSends.promise;
				activeSends--;
			});
		const revalidateGrantLeader = sinon
			.stub(log, "revalidateCheckedPruneGrantLocalLeaders")
			.callsFake(async (...args: unknown[]) => new Set(args[0] as string[]));
		const queuedDeadline = new AbortController();
		let grants: Promise<string[]>[] = [];

		try {
			grants = entries.map(({ entry }, index) =>
				log.admitCheckedPruneGrants(
					[{ hash: entry.hash, requestId: randomBytes(32) }],
					requester,
					undefined,
					index === entries.length - 1 ? queuedDeadline.signal : undefined,
				),
			);
			await waitForResolved(() => {
				expect(activeSends).to.equal(4);
				expect(log._checkedPruneGrantSendQueue.size).to.equal(1);
			});
			expect(maxActiveSends).to.equal(4);

			queuedDeadline.abort();
			expect(await grants[4]).to.deep.equal([entries[4].entry.hash]);
			expect(log._checkedPruneGrantSendQueue.size).to.equal(0);
			expect(sentHashes).to.not.include(entries[4].entry.hash);

			releaseSends.resolve();
			await Promise.all(grants);
			expect(maxActiveSends).to.equal(4);
			expect(sentHashes).to.have.length(4);
		} finally {
			releaseSends.resolve();
			await Promise.allSettled(grants);
			revalidateGrantLeader.restore();
			send.restore();
		}
	});

	it("aborts a started grant send and releases request admission at five seconds", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-started-grant-deadline");
		const requesterKey = session.peers[1].identity.publicKey;
		const sendEntered = pDefer<void>();
		const initialLeaderPlan = sinon
			.stub(log, "planCurrentNativeBackboneRequestPruneLeaderHints")
			.resolves({
				localLeaderHashes: new Set(),
				replicaCounts: new Map(),
				peerHistoryGids: [],
				peerHistoryRemovedHashes: new Set(),
				nativeAllConfirmed: true,
				nativeBackbonePeerHistoryCleaned: true,
			});
		const finalLeaderPlan = sinon
			.stub(log, "revalidateCheckedPruneGrantLocalLeaders")
			.resolves(new Set([entry.hash]));
		const send = sinon
			.stub(log.rpc, "send")
			.callsFake(async (...args: unknown[]) => {
				const message = args[0];
				const options = args[1] as { signal: AbortSignal };
				if (!(message instanceof ResponseIPruneV2)) {
					return;
				}
				sendEntered.resolve();
				await new Promise<void>((resolve, reject) => {
					const onAbort = () =>
						reject(options.signal.reason ?? new Error("aborted"));
					if (options.signal.aborted) {
						onAbort();
						return;
					}
					options.signal.addEventListener("abort", onAbort, { once: true });
				});
			});
		const clock = sinon.useFakeTimers({
			now: 1_000,
			shouldClearNativeTimers: true,
		});

		try {
			const receiving = db.log.onMessage(
				new RequestIPruneV2({
					requests: [{ hash: entry.hash, requestId: randomBytes(32) }],
				}),
				{ from: requesterKey } as any,
			);
			await sendEntered.promise;
			expect(log._checkedPruneRequestWorkAdmission.activeByPeer.size).to.equal(
				1,
			);
			expect(log._checkedPruneGrantSendQueue.running).to.equal(1);

			await clock.tickAsync(5_000);
			await receiving;

			expect(log._checkedPruneRequestWorkAdmission.activeByPeer.size).to.equal(
				0,
			);
			expect(log._checkedPruneGrantSendQueue.running).to.equal(0);
			expect(log._checkedPrune.grantSends?.size ?? 0).to.equal(0);
		} finally {
			clock.restore();
			send.restore();
			finalLeaderPlan.restore();
			initialLeaderPlan.restore();
		}
	});

	it("releases the ownership lane but retains admission until detached grant planning settles", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-final-plan-deadline");
		const requesterKey = session.peers[1].identity.publicKey;
		const initialLeaderPlan = sinon
			.stub(log, "planCurrentNativeBackboneRequestPruneLeaderHints")
			.resolves({
				localLeaderHashes: new Set(),
				replicaCounts: new Map(),
				peerHistoryGids: [],
				peerHistoryRemovedHashes: new Set(),
				nativeAllConfirmed: true,
				nativeBackbonePeerHistoryCleaned: true,
			});
		const hasMany = sinon.stub(log.log.blocks, "hasMany").resolves([true]);
		const hasCustomFindLeaders = sinon
			.stub(log, "hasCustomFindLeaders")
			.returns(true);
		const plannerEntered = pDefer<void>();
		const releasePlanner = pDefer<void>();
		const finalPlanner = sinon
			.stub(log, "planEntryLeaders")
			.callsFake(async () => {
				plannerEntered.resolve();
				await releasePlanner.promise;
				return { isLeader: true };
			});
		const send = sinon.stub(log.rpc, "send").resolves();
		const markCancelled = sinon.spy(log._checkedPrune, "markCancelled");
		const clock = sinon.useFakeTimers({
			now: 1_000,
			shouldClearNativeTimers: true,
		});
		let followerRan = false;

		try {
			const receiving = db.log.onMessage(
				new RequestIPruneV2({
					requests: [{ hash: entry.hash, requestId: randomBytes(32) }],
				}),
				{ from: requesterKey } as any,
			);
			await plannerEntered.promise;

			const follower = log.withReplicationRangeMutationQueue(async () => {
				followerRan = true;
			});
			await clock.tickAsync(4_999);
			expect(followerRan).to.be.false;

			await clock.tickAsync(1);
			await Promise.all([receiving, follower]);
			expect(followerRan).to.be.true;
			expect(log._checkedPruneRequestWorkAdmission.activeByPeer.size).to.equal(
				1,
			);
			expect(markCancelled.called).to.be.false;
			expect(
				send
					.getCalls()
					.some((call) => call.args[0] instanceof ResponseIPruneV2),
			).to.be.false;

			releasePlanner.resolve();
			await clock.tickAsync(0);
			expect(log._checkedPruneRequestWorkAdmission.activeByPeer.size).to.equal(
				0,
			);
		} finally {
			releasePlanner.resolve();
			await clock.tickAsync(0);
			clock.restore();
			markCancelled.restore();
			send.restore();
			finalPlanner.restore();
			hasCustomFindLeaders.restore();
			hasMany.restore();
			initialLeaderPlan.restore();
		}
	});

	it("does not attach an old response to a later contact generation", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-delayed-resolver");
		const remoteHash = (await Ed25519Keypair.create()).publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);

		log._peerSyncCapabilities.set(
			remoteHash,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });

		let firstPrune: Promise<unknown> | undefined;
		let secondPrune: Promise<unknown> | undefined;
		try {
			[firstPrune] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			const firstOutcome = firstPrune!.catch((error) => error);
			const firstPending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(firstPending).to.exist;
			await waitForResolved(() => {
				expect(log._checkedPrune.getRequestId(entry.hash, remoteHash)).to.be.a(
					"string",
				);
			});
			const firstRequestId = log._checkedPrune.getRequestId(
				entry.hash,
				remoteHash,
			);

			await log.cancelCheckedPruneForLocalLeader(entry.hash);
			expect(await firstOutcome).to.be.instanceOf(Error);

			[secondPrune] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			const secondOutcome = secondPrune!.catch((error) => error);
			await waitForResolved(() => {
				expect(log._checkedPrune.getRequestId(entry.hash, remoteHash)).to.be.a(
					"string",
				);
			});
			const secondRequestId = log._checkedPrune.getRequestId(
				entry.hash,
				remoteHash,
			);
			expect(secondRequestId).to.not.equal(firstRequestId);

			await firstPending.resolve(remoteHash, firstRequestId);
			expect(
				log._checkedPrune.getConfirmedReplicators(entry.hash)?.size ?? 0,
			).to.equal(0);

			await log.cancelCheckedPruneForLocalLeader(entry.hash);
			expect(await secondOutcome).to.be.instanceOf(Error);
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(
				[firstPrune, secondPrune].filter(
					(value): value is Promise<unknown> => value != null,
				),
			);
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("coalesces background duplicates while admitting newly discovered leaders", async () => {
		session = await TestSession.disconnected(3);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
				respondToIHaveTimeout: 500,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-coalesced-resends");
		const leaderA = session.peers[1].identity.publicKey.hashcode();
		const leaderB = session.peers[2].identity.publicKey.hashcode();
		log._peerSyncCapabilities.set(
			leaderA,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		log._peerSyncCapabilities.set(
			leaderB,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const destinations: string[] = [];
		const send = sinon
			.stub(log.rpc, "send")
			.callsFake(async (message: unknown, options: any) => {
				if (message instanceof RequestIPruneV2) {
					destinations.push(options.mode.to[0]);
				}
			});
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		const attempts: Promise<unknown>[] = [];

		try {
			// Begin as an explicit generation, which intentionally has no periodic
			// resend owner until the first background observer promotes it.
			const [firstExplicit] = log.prune(
				new Map([[entry.hash, { entry, leaders: new Map([[leaderA, {}]]) }]]),
				{ timeout: 100 },
			);
			attempts.push(firstExplicit);
			const firstExplicitOutcome = firstExplicit.catch(
				(error: unknown) => error,
			);
			await clock.tickAsync(0);
			expect(destinations).to.deep.equal([leaderA]);

			for (let i = 0; i < 50; i++) {
				const duplicate = log.prune(
					new Map([[entry.hash, { entry, leaders: new Map([[leaderA, {}]]) }]]),
				);
				attempts.push(...duplicate);
				for (const attempt of duplicate) void attempt.catch(() => {});
			}
			await clock.tickAsync(0);
			// Exactly the first background observer emits while claiming the sole
			// resend owner; the other 49 calls only share the pending promise.
			expect(destinations).to.deep.equal([leaderA, leaderA]);

			const expandedLeaders = new Map([
				[leaderA, {}],
				[leaderB, {}],
			]);
			const [expandedExplicit] = log.prune(
				new Map([[entry.hash, { entry, leaders: expandedLeaders }]]),
				{ timeout: 75 },
			);
			attempts.push(expandedExplicit);
			const expandedExplicitOutcome = expandedExplicit.catch(
				(error: unknown) => error,
			);
			await clock.tickAsync(0);
			expect(destinations.filter((peer) => peer === leaderA)).to.have.length(3);
			expect(destinations.filter((peer) => peer === leaderB)).to.have.length(1);

			for (let i = 0; i < 50; i++) {
				const duplicate = log.prune(
					new Map([[entry.hash, { entry, leaders: expandedLeaders }]]),
				);
				attempts.push(...duplicate);
				for (const attempt of duplicate) void attempt.catch(() => {});
			}
			await clock.tickAsync(0);
			expect(destinations.filter((peer) => peer === leaderA)).to.have.length(3);
			expect(destinations.filter((peer) => peer === leaderB)).to.have.length(1);

			await clock.tickAsync(100);
			expect(await firstExplicitOutcome).to.be.instanceOf(Error);
			expect(await expandedExplicitOutcome).to.be.instanceOf(Error);
			const promoted = log._checkedPrune.getPendingDelete(entry.hash);
			expect(promoted).to.exist;
			expect(promoted.background).to.be.true;

			await clock.tickAsync(150);
			// The inverse mixed-mode case above discovered B from an explicit
			// duplicate of a background generation; B still owns one periodic resend.
			expect(destinations.filter((peer) => peer === leaderA)).to.have.length(4);
			expect(destinations.filter((peer) => peer === leaderB)).to.have.length(2);
		} finally {
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			await pending?.reject(new Error("test cleanup"));
			await clock.tickAsync(0);
			await Promise.allSettled(attempts);
			clock.restore();
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("keeps a promoted generation's removal timer after its explicit caller times out", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 200,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-promoted-removal-timer");
		const leader = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[leader, { intersecting: true }]]);
		log._peerSyncCapabilities.set(
			leader,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({ leaders, localLeader: false });
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		let explicit: Promise<unknown> | undefined;
		let background: Promise<unknown> | undefined;

		try {
			[explicit] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 50,
			});
			const explicitOutcome = explicit!.catch((error: unknown) => error);
			await clock.tickAsync(0);

			[background] = log.prune(new Map([[entry.hash, { entry, leaders }]]));
			void background!.catch(() => {});
			await clock.tickAsync(0);
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(pending.background).to.be.true;
			const requestId = log._checkedPrune.getRequestId(entry.hash, leader);
			await pending.resolve(leader, requestId);

			await clock.tickAsync(50);
			expect(await explicitOutcome).to.be.instanceOf(Error);
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.equal(pending);
			expect(await log.log.has(entry.hash)).to.be.true;

			await clock.tickAsync(150);
			await background;
			expect(await log.log.has(entry.hash)).to.be.false;
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
		} finally {
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			await pending?.reject(new Error("test cleanup"));
			await clock.tickAsync(0);
			await Promise.allSettled(
				[explicit, background].filter(
					(value): value is Promise<unknown> => value != null,
				),
			);
			clock.restore();
			revalidate.restore();
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("does not let an old emit wave adopt a replacement generation", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-old-wave");
		const leader = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[leader, { intersecting: true }]]);
		log._peerSyncCapabilities.set(
			leader,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const blockedOldWave = pDefer<void>();
		let grantWaits = 0;
		const waitForGrantSends = sinon
			.stub(log._checkedPrune, "waitForGrantSends")
			.callsFake(async () => {
				grantWaits++;
				if (grantWaits === 1) {
					await blockedOldWave.promise;
				}
			});
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const attempts: Promise<unknown>[] = [];

		try {
			const [first] = log.prune(new Map([[entry.hash, { entry, leaders }]]));
			attempts.push(first);
			const firstOutcome = first.catch((error: unknown) => error);
			await waitForResolved(() => expect(grantWaits).to.equal(1));
			const firstId = log._checkedPrune.getRequestId(entry.hash, leader);

			await log.cancelCheckedPruneForLocalLeader(entry.hash);
			expect(await firstOutcome).to.be.instanceOf(Error);

			const [replacement] = log.prune(
				new Map([[entry.hash, { entry, leaders }]]),
				{ timeout: 5_000 },
			);
			attempts.push(replacement);
			void replacement.catch(() => {});
			await waitForResolved(() => {
				expect(
					send
						.getCalls()
						.filter((call) => call.args[0] instanceof RequestIPruneV2),
				).to.have.length(1);
			});
			const replacementId = log._checkedPrune.getRequestId(entry.hash, leader);
			expect(replacementId).to.not.equal(firstId);

			blockedOldWave.resolve();
			await new Promise<void>((resolve) => setTimeout(resolve, 20));
			const requests = send
				.getCalls()
				.map((call) => call.args[0])
				.filter(
					(message): message is RequestIPruneV2 =>
						message instanceof RequestIPruneV2,
				);
			expect(requests).to.have.length(1);
			expect(toHexString(requests[0]!.requests[0]!.requestId)).to.equal(
				replacementId,
			);
		} finally {
			blockedOldWave.resolve();
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(attempts);
			getClampedReplicas.restore();
			send.restore();
			waitForGrantSends.restore();
		}
	});

	it("cancels disconnected targets and retries only resilient generations", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const first = await db.add("checked-prune-disconnect-background");
		const second = await db.add("checked-prune-disconnect-explicit");
		const leader = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[leader, { intersecting: true }]]);
		log._peerSyncCapabilities.set(
			leader,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const random = sinon.stub(Math, "random").returns(0);
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		let replacementPending: any;

		try {
			const [background] = log.prune(
				new Map([[first.entry.hash, { entry: first.entry, leaders }]]),
			);
			const backgroundOutcome = background.catch((error: unknown) => error);
			await clock.tickAsync(0);
			const firstRequestId = log._checkedPrune.getRequestId(
				first.entry.hash,
				leader,
			);

			log.cleanupPeerDisconnectTracking(leader);
			await clock.tickAsync(0);
			const disconnected = await backgroundOutcome;
			expect(disconnected).to.be.instanceOf(Error);
			expect((disconnected as Error).name).to.equal(
				"CheckedPruneTargetUnavailableError",
			);
			expect(log._checkedPrune.getPendingDelete(first.entry.hash)).to.be
				.undefined;
			expect(log._checkedPrune.getRetry(first.entry.hash)?.attempts).to.equal(
				1,
			);

			log.recordPeerSyncCapabilities(
				leader,
				SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
			);
			await log.pruneDebouncedFn.flush();
			await clock.tickAsync(0);
			replacementPending = log._checkedPrune.getPendingDelete(first.entry.hash);
			expect(replacementPending).to.exist;
			void replacementPending.promise.promise.catch(() => {});
			expect(
				log._checkedPrune.getRequestId(first.entry.hash, leader),
			).to.not.equal(firstRequestId);

			await log.cancelCheckedPruneForLocalLeader(first.entry.hash);
			await clock.tickAsync(0);
			expect(log._checkedPrune.getRetry(first.entry.hash)).to.be.undefined;

			log._peerSyncCapabilities.set(
				leader,
				SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
			);
			const [explicit] = log.prune(
				new Map([[second.entry.hash, { entry: second.entry, leaders }]]),
				{ timeout: 5_000 },
			);
			const explicitOutcome = explicit.catch((error: unknown) => error);
			await clock.tickAsync(0);
			log.cleanupPeerDisconnectTracking(leader);
			await clock.tickAsync(0);
			const explicitDisconnected = await explicitOutcome;
			expect(explicitDisconnected).to.be.instanceOf(Error);
			expect((explicitDisconnected as Error).name).to.equal(
				"CheckedPruneTargetUnavailableError",
			);
			expect(log._checkedPrune.getPendingDelete(second.entry.hash)).to.be
				.undefined;
			expect(log._checkedPrune.getRetry(second.entry.hash)).to.be.undefined;
		} finally {
			await log
				.cancelCheckedPruneForLocalLeader(first.entry.hash)
				.catch(() => {});
			await log
				.cancelCheckedPruneForLocalLeader(second.entry.hash)
				.catch(() => {});
			clock.restore();
			random.restore();
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("observes an ignored background rejection when its target disconnects", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-background-disconnect");
		const leader = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[leader, { intersecting: true }]]);
		log._peerSyncCapabilities.set(
			leader,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		let attempt: Promise<unknown> | undefined;
		let unhandled: unknown;
		const onUnhandled = (reason: unknown, promise: Promise<unknown>) => {
			if (promise === attempt) {
				unhandled = reason;
			}
		};
		process.on("unhandledRejection", onUnhandled);

		try {
			[attempt] = log.prune(new Map([[entry.hash, { entry, leaders }]]));
			await waitForResolved(() => {
				expect(log._checkedPrune.getRequestId(entry.hash, leader)).to.be.a(
					"string",
				);
			});

			log.cleanupPeerDisconnectTracking(leader);
			await new Promise<void>((resolve) => setImmediate(resolve));
			await new Promise<void>((resolve) => setImmediate(resolve));
			expect(unhandled).to.be.undefined;
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
			expect(log._checkedPrune.getRetry(entry.hash)?.attempts).to.equal(1);
		} finally {
			process.off("unhandledRejection", onUnhandled);
			void attempt?.catch(() => {});
			log._checkedPrune.clearRetry(entry.hash);
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("does not let an old removal timer cancel a replacement generation", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-old-removal-timer");
		const leader = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[leader, { intersecting: true }]]);
		log._peerSyncCapabilities.set(
			leader,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const preliminaryStarted = pDefer<void>();
		const releasePreliminary = pDefer<void>();
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.callsFake(async () => {
				preliminaryStarted.resolve();
				await releasePreliminary.promise;
				return { leaders, localLeader: false };
			});
		let first: Promise<unknown> | undefined;
		let replacement: Promise<unknown> | undefined;

		try {
			[first] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			const firstOutcome = first!.catch((error: unknown) => error);
			await waitForResolved(() => {
				expect(log._checkedPrune.getRequestId(entry.hash, leader)).to.be.a(
					"string",
				);
			});
			const firstPending = log._checkedPrune.getPendingDelete(entry.hash);
			const firstRequestId = log._checkedPrune.getRequestId(entry.hash, leader);
			await firstPending.resolve(leader, firstRequestId);
			await preliminaryStarted.promise;

			await log.cancelCheckedPruneForLocalLeader(entry.hash);
			expect(await firstOutcome).to.be.instanceOf(Error);
			[replacement] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			void replacement!.catch(() => {});
			await waitForResolved(() => {
				expect(log._checkedPrune.getPendingDelete(entry.hash)).to.exist;
				expect(log._checkedPrune.getRequestId(entry.hash, leader)).to.not.equal(
					firstRequestId,
				);
			});
			const replacementPending = log._checkedPrune.getPendingDelete(entry.hash);
			const replacementRequestId = log._checkedPrune.getRequestId(
				entry.hash,
				leader,
			);

			releasePreliminary.resolve();
			await waitForResolved(() => expect(revalidate.calledOnce).to.be.true);
			await new Promise<void>((resolve) => setImmediate(resolve));
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.equal(
				replacementPending,
			);
			expect(log._checkedPrune.getRequestId(entry.hash, leader)).to.equal(
				replacementRequestId,
			);
		} finally {
			releasePreliminary.resolve();
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(
				[first, replacement].filter(
					(value): value is Promise<unknown> => value != null,
				),
			);
			revalidate.restore();
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("does not let an old admitted remove erase a replacement generation", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-old-admitted-remove");
		const leader = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[leader, { intersecting: true }]]);
		log._peerSyncCapabilities.set(
			leader,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({ leaders, localLeader: false });
		const removeStarted = pDefer<void>();
		const releaseRemove = pDefer<void>();
		const remove = sinon.stub(log.log, "remove").callsFake(async () => {
			removeStarted.resolve();
			await releaseRemove.promise;
		});
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		let first: Promise<unknown> | undefined;
		let replacement: Promise<unknown> | undefined;

		try {
			[first] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 100,
			});
			const firstOutcome = first!.catch((error: unknown) => error);
			await clock.tickAsync(0);
			const firstPending = log._checkedPrune.getPendingDelete(entry.hash);
			const firstRequestId = log._checkedPrune.getRequestId(entry.hash, leader);
			expect(firstPending).to.exist;
			expect(firstRequestId).to.be.a("string");
			await firstPending.resolve(leader, firstRequestId);
			await clock.tickAsync(0);
			await removeStarted.promise;

			await clock.tickAsync(100);
			expect(await firstOutcome).to.be.instanceOf(Error);
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;

			[replacement] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			void replacement!.catch(() => {});
			await clock.tickAsync(0);
			const replacementPending = log._checkedPrune.getPendingDelete(entry.hash);
			const replacementRequestId = log._checkedPrune.getRequestId(
				entry.hash,
				leader,
			);
			expect(replacementPending).to.exist;
			expect(replacementRequestId)
				.to.be.a("string")
				.and.not.equal(firstRequestId);

			releaseRemove.resolve();
			await clock.tickAsync(0);
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.equal(
				replacementPending,
			);
			expect(log._checkedPrune.getRequestId(entry.hash, leader)).to.equal(
				replacementRequestId,
			);
			expect(log._checkedPrune.isCurrentRequest(entry.hash, replacementPending))
				.to.be.true;
		} finally {
			releaseRemove.resolve();
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(
				[first, replacement].filter(
					(value): value is Promise<unknown> => value != null,
				),
			);
			clock.restore();
			remove.restore();
			revalidate.restore();
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("makes exact response replays idempotent without leader-wait work", async () => {
		session = await TestSession.disconnected(3);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-response-replay");
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const secondRemoteHash = session.peers[2].identity.publicKey.hashcode();
		const leaders = new Map([
			[remoteHash, { intersecting: true }],
			[secondRemoteHash, { intersecting: true }],
		]);

		for (const peerHash of leaders.keys()) {
			log._peerSyncCapabilities.set(
				peerHash,
				SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
			);
		}
		const send = sinon.stub(log.rpc, "send").resolves();
		const waitForReplicators = sinon.spy(log, "_waitForEntryReplicators");
		const addConfirmedReplicator = sinon.spy(
			log._checkedPrune,
			"addConfirmedReplicator",
		);
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 2 });
		let pruning: Promise<unknown> | undefined;

		try {
			[pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			const outcome = pruning!.catch((error) => error);
			await waitForResolved(() => {
				expect(log._checkedPrune.getRequestId(entry.hash, remoteHash)).to.be.a(
					"string",
				);
			});
			const requestId = log._checkedPrune.getRequestId(entry.hash, remoteHash);
			const response = new ResponseIPruneV2({
				requests: [
					{
						hash: entry.hash,
						requestId: fromHexString(requestId),
					},
				],
			});
			const replays = Array.from({ length: 100 }, () =>
				db.log.onMessage(response, { from: remoteKey } as any),
			);
			await Promise.all(replays);
			expect(waitForReplicators.called).to.be.false;
			expect(addConfirmedReplicator.calledOnce).to.be.true;
			expect(
				log._checkedPrune.getConfirmedReplicators(entry.hash)?.size,
			).to.equal(1);

			await db.log.onMessage(response, { from: remoteKey } as any);
			expect(waitForReplicators.called).to.be.false;
			expect(addConfirmedReplicator.calledOnce).to.be.true;

			await log.cancelCheckedPruneForLocalLeader(entry.hash);
			expect(await outcome).to.be.instanceOf(Error);
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(pruning ? [pruning] : []);
			getClampedReplicas.restore();
			addConfirmedReplicator.restore();
			waitForReplicators.restore();
			send.restore();
		}
	});

	it("requires every exact grant in a multi-replica quorum", async () => {
		session = await TestSession.disconnected(3);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-multi-replica-quorum");
		const remoteKeys = session.peers
			.slice(1)
			.map((peer) => peer.identity.publicKey);
		const remoteHashes = remoteKeys.map((key) => key.hashcode());
		const leaders = new Map(
			remoteHashes.map((hash) => [hash, { intersecting: true }]),
		);
		for (const hash of remoteHashes) {
			log._peerSyncCapabilities.set(
				hash,
				SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
			);
		}
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
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
				for (const hash of remoteHashes) {
					expect(log._checkedPrune.getRequestId(entry.hash, hash)).to.be.a(
						"string",
					);
				}
			});
			const requestIds = remoteHashes.map((hash) =>
				log._checkedPrune.getRequestId(entry.hash, hash),
			);

			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [
						{
							hash: entry.hash,
							requestId: fromHexString(requestIds[0]),
						},
					],
				}),
				{ from: remoteKeys[1] } as any,
			);
			expect(
				log._checkedPrune.getConfirmedReplicators(entry.hash)?.size ?? 0,
			).to.equal(0);

			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [
						{
							hash: entry.hash,
							requestId: fromHexString(requestIds[0]),
						},
					],
				}),
				{ from: remoteKeys[0] } as any,
			);
			await new Promise<void>((resolve) => setTimeout(resolve, 20));
			expect(remove.called).to.be.false;
			expect(revalidate.called).to.be.false;
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.exist;
			expect(await log.log.has(entry.hash)).to.be.true;

			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [
						{
							hash: entry.hash,
							requestId: fromHexString(requestIds[1]),
						},
					],
				}),
				{ from: remoteKeys[1] } as any,
			);
			await pruning;
			expect(remove.calledOnce).to.be.true;
			expect(await log.log.has(entry.hash)).to.be.false;
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(pruning ? [pruning] : []);
			remove.restore();
			revalidate.restore();
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("rejects a grant from a peer excluded by the final remote leader set", async () => {
		session = await TestSession.disconnected(3);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-replaced-remote-leader");
		const confirmingKey = session.peers[1].identity.publicKey;
		const confirmingHash = confirmingKey.hashcode();
		const replacementHash = session.peers[2].identity.publicKey.hashcode();
		const originalLeaders = new Map([[confirmingHash, { intersecting: true }]]);
		const replacementLeaders = new Map([
			[replacementHash, { intersecting: true }],
		]);
		log._peerSyncCapabilities.set(
			confirmingHash,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const revalidate = sinon.stub(log, "revalidateCheckedPruneOwnership");
		revalidate.onFirstCall().resolves({
			leaders: originalLeaders,
			localLeader: false,
		});
		revalidate.onSecondCall().resolves({
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
				expect(
					log._checkedPrune.getRequestId(entry.hash, confirmingHash),
				).to.be.a("string");
			});
			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [
						{
							hash: entry.hash,
							requestId: fromHexString(
								log._checkedPrune.getRequestId(entry.hash, confirmingHash),
							),
						},
					],
				}),
				{ from: confirmingKey } as any,
			);

			await expect(pruning).to.be.rejectedWith(
				"Checked prune confirmation is no longer active at the delete boundary",
			);
			expect(remove.called).to.be.false;
			expect(await log.log.has(entry.hash)).to.be.true;
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(pruning ? [pruning] : []);
			remove.restore();
			revalidate.restore();
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("does not revive a stale self leader after nonreplicating revalidation", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-stale-self-leader");
		const selfHash = session.peers[0].identity.publicKey.hashcode();
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		// The debounce union may retain an older self key even though its
		// authoritative revalidation already established localLeader=false.
		const leaders = new Map([
			[selfHash, { intersecting: true }],
			[remoteHash, { intersecting: true }],
		]);
		log._peerSyncCapabilities.set(
			remoteHash,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		let pruning: Promise<unknown> | undefined;

		try {
			[pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			void pruning!.catch(() => {});
			await waitForResolved(() => {
				const request = send
					.getCalls()
					.map((call) => call.args[0])
					.find((message) => message instanceof RequestIPruneV2);
				expect(request).to.exist;
				expect(
					request!.requests.map(({ hash }: { hash: string }) => hash),
				).to.deep.equal([entry.hash]);
			});
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.exist;
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(pruning ? [pruning] : []);
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("uses returned leader maps when a planner omits callbacks", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const selfHash = session.peers[0].identity.publicKey.hashcode();
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);

		const result = await log.waitForLeaderSelection(
			[
				{ key: remoteHash, replicator: true },
				{ key: selfHash, replicator: false },
			],
			{ timeout: 100 },
			async () => leaders,
		);
		expect(result).to.equal(leaders);
	});

	it("releases a read-only leader wait at its deadline without draining a stuck check", async () => {
		session = await TestSession.disconnected(1);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: false, timeUntilRoleMaturity: 0 },
		});
		const log = db.log as any;
		const checkEntered = pDefer<void>();
		const releaseCheck = pDefer<void>();

		try {
			const result = log.waitForLeaderSelection(
				[{ key: "missing-replicator", replicator: true }],
				{
					timeout: 20,
					persist: false,
					drainInFlightChecks: false,
				},
				async () => {
					checkEntered.resolve();
					await releaseCheck.promise;
					return new Map();
				},
			);
			await checkEntered.promise;
			expect(await result).to.be.false;
		} finally {
			releaseCheck.resolve();
		}
	});

	it("breaks a three-peer circular checked-prune handoff", async () => {
		session = await TestSession.disconnected(3);
		const stores = await Promise.all(
			session.peers.map((peer) =>
				peer.open(new EventStore(), {
					args: {
						replicate: false,
						timeUntilRoleMaturity: 0,
						waitForPruneDelay: 0,
					},
				}),
			),
		);
		const logs = stores.map((store) => store.log as any);
		const { entry } = await stores[0].add("checked-prune-three-peer-cycle");
		await stores[1].log.log.join([entry]);
		await stores[2].log.log.join([entry]);
		const peerHashes = session.peers.map((peer) =>
			peer.identity.publicKey.hashcode(),
		);
		const sent: unknown[][] = [[], [], []];
		const sendStubs = logs.map((log, index) =>
			sinon.stub(log.rpc, "send").callsFake(async (message: unknown) => {
				sent[index].push(message);
			}),
		);
		const replicaStubs = logs.map((log) =>
			sinon.stub(log, "getClampedReplicas").returns({ getValue: () => 1 }),
		);
		const revalidateGrantLeaderStubs = logs.map((log) =>
			sinon
				.stub(log, "revalidateCheckedPruneGrantLocalLeaders")
				.resolves(new Set([entry.hash])),
		);
		const removeSpies = logs.map((log) => sinon.spy(log.log, "remove"));
		const pruning: Promise<unknown>[] = [];

		try {
			for (let index = 0; index < logs.length; index++) {
				const leaderIndex = (index + 1) % logs.length;
				logs[index]._peerSyncCapabilities.set(
					peerHashes[leaderIndex],
					SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
				);
				const [attempt] = logs[index].prune(
					new Map([
						[
							entry.hash,
							{
								entry,
								leaders: new Map([
									[peerHashes[leaderIndex], { intersecting: true }],
								]),
							},
						],
					]),
					{ timeout: 5_000 },
				);
				pruning.push(attempt);
				void attempt.catch(() => {});
			}

			await waitForResolved(() => {
				for (const messages of sent) {
					expect(
						messages.filter((message) => message instanceof RequestIPruneV2),
					).to.have.length(1);
				}
			});
			const requests = sent.map(
				(messages) =>
					messages.find(
						(message) => message instanceof RequestIPruneV2,
					) as RequestIPruneV2,
			);

			await Promise.all(
				logs.map((_, requesterIndex) => {
					const responderIndex = (requesterIndex + 1) % logs.length;
					return logs[responderIndex].admitCheckedPruneGrants(
						requests[requesterIndex].requests,
						peerHashes[requesterIndex],
					);
				}),
			);
			const outcomes = await Promise.all(
				pruning.map((attempt) => attempt.catch((error) => error)),
			);
			expect(outcomes.every((outcome) => outcome instanceof Error)).to.be.true;

			for (
				let requesterIndex = 0;
				requesterIndex < logs.length;
				requesterIndex++
			) {
				const responderIndex = (requesterIndex + 1) % logs.length;
				const response = sent[responderIndex].find(
					(message) => message instanceof ResponseIPruneV2,
				) as ResponseIPruneV2;
				expect(response).to.exist;
				await stores[requesterIndex].log.onMessage(response, {
					from: session.peers[responderIndex].identity.publicKey,
				} as any);
			}

			for (let index = 0; index < logs.length; index++) {
				expect(removeSpies[index].called).to.be.false;
				expect(await logs[index].log.has(entry.hash)).to.be.true;
				expect(await logs[index].log.blocks.has(entry.hash)).to.be.true;
				expect(logs[index]._checkedPrune.getPendingDelete(entry.hash)).to.be
					.undefined;
			}
		} finally {
			await Promise.allSettled(pruning);
			for (const spy of removeSpies) spy.restore();
			for (const stub of revalidateGrantLeaderStubs) stub.restore();
			for (const stub of replicaStubs) stub.restore();
			for (const stub of sendStubs) stub.restore();
		}
	});

	it("does not recursively delete an ancestor under a child's grant", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
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
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		log._peerSyncCapabilities.set(
			remoteHash,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns(new AbsoluteReplicas(1));
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({ leaders, localLeader: false });
		const planEntryLeaderBatch = sinon
			.stub(log, "planEntryLeaderBatch")
			.resolves([{ coordinates: [], leaders, isLeader: false }]);
		const remove = sinon.spy(log.log, "remove");

		try {
			const [pruning] = log.prune(
				new Map([[child.entry.hash, { entry: child.entry, leaders }]]),
				{ timeout: 5_000 },
			);
			await waitForResolved(() => {
				expect(
					log._checkedPrune.getRequestId(child.entry.hash, remoteHash),
				).to.be.a("string");
			});
			const pending = log._checkedPrune.getPendingDelete(child.entry.hash);
			const requestId = log._checkedPrune.getRequestId(
				child.entry.hash,
				remoteHash,
			);
			await pending.resolve(remoteHash, requestId);
			await pruning;

			expect(remove.calledOnce).to.be.true;
			expect(remove.firstCall.args[0].hash).to.equal(child.entry.hash);
			expect(remove.firstCall.args[1]).to.deep.equal({ recursively: false });
			expect(await log.log.has(child.entry.hash)).to.be.false;
			expect(await log.log.has(parent.entry.hash)).to.be.true;
			await new Promise<void>((resolve) => setImmediate(resolve));
			await log.pruneDebouncedFn.flush();
			await new Promise<void>((resolve) => setImmediate(resolve));
			expect(log._checkedPrune.getRequestId(parent.entry.hash, remoteHash)).to
				.be.undefined;
			expect(
				send
					.getCalls()
					.flatMap((call) =>
						call.args[0] instanceof RequestIPruneV2
							? call.args[0].requests
							: [],
					)
					.some((request) => request.hash === parent.entry.hash),
			).to.be.false;
			expect(await log.log.has(parent.entry.hash)).to.be.true;
		} finally {
			remove.restore();
			planEntryLeaderBatch.restore();
			revalidate.restore();
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("wakes a retained candidate when checked-prune capability arrives", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-capability-wake");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({ leaders, localLeader: false });

		try {
			const [initialAttempt] = log.prune(
				new Map([[entry.hash, { entry, leaders }]]),
			);
			await expect(initialAttempt).to.be.rejectedWith(
				"Insufficient checked-prune capable leaders",
			);
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
			expect(log._checkedPrune.getRetry(entry.hash)).to.exist;
			log._checkedPrune.clearRetryTimer(entry.hash);

			log.recordPeerSyncCapabilities(
				remoteHash,
				SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
			);
			await waitForResolved(
				() => {
					expect(
						send
							.getCalls()
							.filter((call) => call.args[0] instanceof RequestIPruneV2),
					).to.have.length(1);
				},
				{ timeout: 3_000, delayInterval: 10 },
			);
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(pending).to.exist;
			void pending.promise.promise.catch(() => {});
			await log.cancelCheckedPruneForLocalLeader(entry.hash);
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			log._checkedPrune.clearRetry(entry.hash);
			revalidate.restore();
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("treats capabilities as monotonic within a subscription", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: { replicate: false },
		});
		const log = db.log as any;
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const wake = sinon.stub(log, "wakeCheckedPruneRetriesForPeer");

		try {
			log.recordPeerSyncCapabilities(
				remoteHash,
				SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
			);
			log.recordPeerSyncCapabilities(remoteHash, 0);
			log.recordPeerSyncCapabilities(
				remoteHash,
				SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
			);

			expect(wake.calledOnceWithExactly(remoteHash)).to.be.true;
			expect(log._peerSyncCapabilities.get(remoteHash)).to.equal(
				SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
			);

			log.cleanupPeerDisconnectTracking(remoteHash);
			expect(log._peerSyncCapabilities.has(remoteHash)).to.be.false;
			log.recordPeerSyncCapabilities(
				remoteHash,
				SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
			);
			expect(wake.callCount).to.equal(2);
		} finally {
			wake.restore();
		}
	});

	it("revokes an old confirmation when a peer contact id changes", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-contact-generation");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		log._peerSyncCapabilities.set(
			remoteHash,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		let pruning: Promise<unknown> | undefined;

		try {
			[pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			const outcome = pruning!.catch((error) => error);
			await waitForResolved(() => {
				expect(log._checkedPrune.getRequestId(entry.hash, remoteHash)).to.be.a(
					"string",
				);
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			const firstRequestId = log._checkedPrune.getRequestId(
				entry.hash,
				remoteHash,
			);
			expect(
				log._checkedPrune.addConfirmedReplicator(
					entry.hash,
					remoteHash,
					pending,
					firstRequestId,
				),
			).to.exist;

			const secondRequestId = toHexString(randomBytes(32));
			expect(secondRequestId).to.not.equal(firstRequestId);
			expect(
				log._checkedPrune.addRequestSent(
					entry.hash,
					remoteHash,
					secondRequestId,
				),
			).to.be.true;
			expect(
				log._checkedPrune
					.getConfirmedReplicators(entry.hash)
					?.has(remoteHash) ?? false,
			).to.be.false;
			expect(log._checkedPrune.getConfirmedRequestId(entry.hash, remoteHash)).to
				.be.undefined;

			await log.cancelCheckedPruneForLocalLeader(entry.hash);
			expect(await outcome).to.be.instanceOf(Error);
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(pruning ? [pruning] : []);
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("rejects duplicate, oversized, and overlong correlated prune input before work", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-vector-bounds");
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const planner = sinon.spy(
			log,
			"planCurrentNativeBackboneRequestPruneLeaderHints",
		);
		const removeKnown = sinon.spy(log, "removeEntriesKnownByPeer");
		const removeRequests = sinon.spy(log, "removePruneRequestsSent");
		const removeConfirmations = sinon.spy(
			log._checkedPrune,
			"removeConfirmedReplicators",
		);
		const removePeerHistory = sinon.spy(
			log,
			"removePeerFromGidPeerHistoryBatch",
		);
		const send = sinon.stub(log.rpc, "send").resolves();

		try {
			await db.log.onMessage(
				new RequestIPruneV2({
					requests: [
						{ hash: entry.hash, requestId: randomBytes(32) },
						{ hash: entry.hash, requestId: randomBytes(32) },
					],
				}),
				{ from: remoteKey } as any,
			);
			await db.log.onMessage(
				new RequestIPruneV2({
					requests: Array.from({ length: 1_025 }, (_, index) => ({
						hash: `oversized-${index}`,
						requestId: randomBytes(32),
					})),
				}),
				{ from: remoteKey } as any,
			);
			for (const invalidHash of ["", "x".repeat(257)]) {
				await db.log.onMessage(
					new RequestIPruneV2({
						requests: [
							{ hash: invalidHash, requestId: randomBytes(32) },
							{ hash: entry.hash, requestId: randomBytes(32) },
						],
					}),
					{ from: remoteKey } as any,
				);
			}
			expect(planner.called).to.be.false;
			expect(removeKnown.called).to.be.false;
			expect(removeRequests.called).to.be.false;
			expect(removeConfirmations.called).to.be.false;
			expect(removePeerHistory.called).to.be.false;
			expect(log._pendingIHave.size).to.equal(0);
			expect(log._pendingIHaveCountByPeer.size).to.equal(0);
			expect(log._pendingIHaveRequesterCount).to.equal(0);
			expect(
				send
					.getCalls()
					.some((call) => call.args[0] instanceof ResponseIPruneV2),
			).to.be.false;

			const deferred = pDefer<void>();
			const pending = {
				promise: deferred,
				clear: () => {},
				resolve: sinon.spy(),
				reject: () => {},
			};
			log._checkedPrune.setPendingDelete(
				entry.hash,
				pending,
				entry,
				new Map([[remoteHash, { intersecting: true }]]),
			);
			const requestIdBytes = randomBytes(32);
			const requestId = toHexString(requestIdBytes);
			log._checkedPrune.addRequestSent(entry.hash, remoteHash, requestId);
			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [
						{ hash: "x".repeat(257), requestId: randomBytes(32) },
						{ hash: entry.hash, requestId: requestIdBytes },
					],
				}),
				{ from: remoteKey } as any,
			);
			expect(pending.resolve.called).to.be.false;
			await db.log.onMessage(
				new ResponseIPruneV2({
					requests: [
						{ hash: entry.hash, requestId: requestIdBytes },
						{ hash: entry.hash, requestId: requestIdBytes },
					],
				}),
				{ from: remoteKey } as any,
			);
			expect(pending.resolve.called).to.be.false;
			log._checkedPrune.deletePendingDelete(entry.hash, pending);
		} finally {
			removePeerHistory.restore();
			removeConfirmations.restore();
			removeRequests.restore();
			removeKnown.restore();
			planner.restore();
			send.restore();
		}
	});

	it("bounds active prune-request work per peer and globally", async () => {
		session = await TestSession.disconnected(1);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const remoteKeys = (
			await Promise.all(
				Array.from({ length: 5 }, () => Ed25519Keypair.create()),
			)
		).map((keypair) => keypair.publicKey);
		const releasePlanner = pDefer<void>();
		const planner = sinon
			.stub(log, "planCurrentNativeBackboneRequestPruneLeaderHints")
			.callsFake(async () => {
				await releasePlanner.promise;
				return undefined;
			});
		const hasMany = sinon
			.stub(log.log.blocks, "hasMany")
			.callsFake(async (...args: unknown[]) =>
				(args[0] as string[]).map(() => false),
			);
		const request = (peerIndex: number, hash: string) =>
			db.log.onMessage(
				new RequestIPruneV2({
					requests: [{ hash, requestId: randomBytes(32) }],
				}),
				{ from: remoteKeys[peerIndex] } as any,
			);

		const active: Promise<void>[] = [];
		try {
			active.push(request(0, "active-request-peer-0"));
			await waitForResolved(() => expect(planner.callCount).to.equal(1));

			await request(0, "same-peer-overflow");
			expect(planner.callCount).to.equal(1);

			for (let peerIndex = 1; peerIndex < 4; peerIndex++) {
				active.push(request(peerIndex, `active-request-peer-${peerIndex}`));
			}
			await waitForResolved(() => expect(planner.callCount).to.equal(4));
			expect(log._checkedPruneRequestWorkAdmission.activeByPeer.size).to.equal(
				4,
			);

			await request(4, "global-overflow");
			expect(planner.callCount).to.equal(4);

			releasePlanner.resolve();
			await Promise.all(active);
			expect(log._checkedPruneRequestWorkAdmission.activeByPeer.size).to.equal(
				0,
			);

			await request(4, "retry-after-release");
			expect(planner.callCount).to.equal(5);
			expect(log._checkedPruneRequestWorkAdmission.activeByPeer.size).to.equal(
				0,
			);
		} finally {
			releasePlanner.resolve();
			await Promise.allSettled(active);
			log.clearPendingIHaves();
			hasMany.restore();
			planner.restore();
		}
	});

	it("retains timed-out request capacity until reads settle and retires the old generation on reopen", async () => {
		session = await TestSession.disconnected(1);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const remoteKeys = (
			await Promise.all(
				Array.from({ length: 5 }, () => Ed25519Keypair.create()),
			)
		).map((keypair) => keypair.publicKey);
		const plannerReleases: ReturnType<typeof pDefer<void>>[] = [];
		const planner = sinon
			.stub(log, "planCurrentNativeBackboneRequestPruneLeaderHints")
			.callsFake(() => {
				const release = pDefer<void>();
				plannerReleases.push(release);
				return release.promise.then(() => undefined);
			});
		const request = (peerIndex: number, hash: string) =>
			db.log.onMessage(
				new RequestIPruneV2({
					requests: [{ hash, requestId: randomBytes(32) }],
				}),
				{ from: remoteKeys[peerIndex] } as any,
			);
		const clock = sinon.useFakeTimers({
			now: 1_000,
			shouldClearNativeTimers: true,
		});
		let clockRestored = false;
		let freshRequest: Promise<void> | undefined;

		try {
			const oldAdmission = log._checkedPruneRequestWorkAdmission;
			const timedOut = Array.from({ length: 4 }, (_, peerIndex) =>
				request(peerIndex, `detached-request-${peerIndex}`),
			);
			expect(planner.callCount).to.equal(4);

			await clock.tickAsync(5_000);
			await Promise.all(timedOut);
			expect(oldAdmission.activeByPeer.size).to.equal(4);

			// Neither reconnecting the same peer nor rotating identities may turn
			// repeated deadline cycles into unbounded detached planner reads.
			await request(0, "same-peer-detached-overflow");
			await request(4, "global-detached-overflow");
			expect(planner.callCount).to.equal(4);
			expect(oldAdmission.activeByPeer.size).to.equal(4);

			clock.restore();
			clockRestored = true;
			await db.close();
			await session.peers[0].open(db, {
				args: {
					replicate: false,
					timeUntilRoleMaturity: 0,
				},
			});

			const freshAdmission = log._checkedPruneRequestWorkAdmission;
			expect(freshAdmission).to.not.equal(oldAdmission);
			expect(freshAdmission.activeByPeer.size).to.equal(0);

			freshRequest = request(4, "fresh-lifecycle-request");
			expect(planner.callCount).to.equal(5);
			expect(freshAdmission.activeByPeer.size).to.equal(1);

			for (const release of plannerReleases.slice(0, 4)) {
				release.resolve();
			}
			await waitForResolved(() =>
				expect(oldAdmission.activeByPeer.size).to.equal(0),
			);
			expect(freshAdmission.activeByPeer.size).to.equal(1);

			plannerReleases[4]!.resolve();
			await freshRequest;
			await waitForResolved(() =>
				expect(freshAdmission.activeByPeer.size).to.equal(0),
			);
		} finally {
			for (const release of plannerReleases) {
				release.resolve();
			}
			await Promise.allSettled(freshRequest ? [freshRequest] : []);
			if (!clockRestored) {
				clock.restore();
			}
			planner.restore();
		}
	});

	it("bounds sequential leader waits to one five-second request budget", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const first = await db.add("checked-prune-work-budget-first");
		const second = await db.add("checked-prune-work-budget-second");
		const remoteKey = session.peers[1].identity.publicKey;
		const backbonePlan = sinon
			.stub(log, "planCurrentNativeBackboneRequestPruneLeaderHints")
			.resolves(undefined);
		const nativeMetadata = sinon
			.stub(log, "getNativeLogEntryMetadataBatch")
			.returns([null, null]);
		const hasMany = sinon
			.stub(log.log.blocks, "hasMany")
			.resolves([true, true]);
		const nativePlan = sinon
			.stub(log, "planCurrentNativeRequestPruneLeaderHints")
			.resolves({
				localLeaderHashes: new Set(),
				replicaCounts: new Map(),
				peerHistoryGids: [],
				peerHistoryRemovedHashes: new Set(),
			});
		const clock = sinon.useFakeTimers({ now: 1_000, toFake: ["Date"] });
		const observedTimeouts: number[] = [];
		const waitForReplicators = sinon
			.stub(log, "_waitForEntryReplicators")
			.callsFake(async (...args: unknown[]) => {
				const timeout = (args[3] as { timeout: number }).timeout;
				observedTimeouts.push(timeout);
				clock.setSystemTime(Date.now() + timeout);
				return false;
			});
		const send = sinon.stub(log.rpc, "send").resolves();

		try {
			await db.log.onMessage(
				new RequestIPruneV2({
					requests: [first.entry, second.entry].map((entry) => ({
						hash: entry.hash,
						requestId: randomBytes(32),
					})),
				}),
				{ from: remoteKey } as any,
			);

			expect(observedTimeouts).to.deep.equal([5_000]);
			expect(waitForReplicators.calledOnce).to.be.true;
			expect(log._pendingIHave.size).to.equal(0);
			expect(
				send
					.getCalls()
					.some((call) => call.args[0] instanceof ResponseIPruneV2),
			).to.be.false;
			expect(log._checkedPruneRequestWorkAdmission.activeByPeer.size).to.equal(
				0,
			);
		} finally {
			clock.restore();
			send.restore();
			waitForReplicators.restore();
			nativePlan.restore();
			hasMany.restore();
			nativeMetadata.restore();
			backbonePlan.restore();
		}
	});

	it("stops correlated prune work when the sender subscription changes", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
			},
		});
		const log = db.log as any;
		const first = await db.add("checked-prune-epoch-first");
		const second = await db.add("checked-prune-epoch-second");
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const backbonePlan = sinon
			.stub(log, "planCurrentNativeBackboneRequestPruneLeaderHints")
			.resolves(undefined);
		const nativeMetadata = sinon
			.stub(log, "getNativeLogEntryMetadataBatch")
			.returns([null, null]);
		const hasMany = sinon
			.stub(log.log.blocks, "hasMany")
			.resolves([true, true]);
		const nativePlan = sinon
			.stub(log, "planCurrentNativeRequestPruneLeaderHints")
			.resolves({
				localLeaderHashes: new Set(),
				replicaCounts: new Map(),
				peerHistoryGids: [],
				peerHistoryRemovedHashes: new Set(),
			});
		const waitForReplicators = sinon
			.stub(log, "_waitForEntryReplicators")
			.callsFake(async () => {
				log.advanceSubscriptionEpoch(remoteHash);
				return false;
			});
		const send = sinon.stub(log.rpc, "send").resolves();

		try {
			await db.log.onMessage(
				new RequestIPruneV2({
					requests: [first.entry, second.entry].map((entry) => ({
						hash: entry.hash,
						requestId: randomBytes(32),
					})),
				}),
				{ from: remoteKey } as any,
			);

			expect(waitForReplicators.calledOnce).to.be.true;
			expect(log._pendingIHave.size).to.equal(0);
			expect(
				send
					.getCalls()
					.some((call) => call.args[0] instanceof ResponseIPruneV2),
			).to.be.false;
			expect(log._checkedPruneRequestWorkAdmission.activeByPeer.size).to.equal(
				0,
			);
		} finally {
			send.restore();
			waitForReplicators.restore();
			nativePlan.restore();
			hasMany.restore();
			nativeMetadata.restore();
			backbonePlan.restore();
		}
	});

	it("bounds retained missing-entry requests per peer and globally", async () => {
		session = await TestSession.disconnected(1);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
				respondToIHaveTimeout: 60_000,
			},
		});
		const log = db.log as any;
		const remoteKeys = (
			await Promise.all(
				Array.from({ length: 5 }, () => Ed25519Keypair.create()),
			)
		).map((keypair) => keypair.publicKey);
		const hashesByPeer = remoteKeys.map((_, peerIndex) =>
			Array.from(
				{ length: 1_024 },
				(__, hashIndex) => `pending-ihave-${peerIndex}-${hashIndex}`,
			),
		);
		const hasMany = sinon
			.stub(log.log.blocks, "hasMany")
			.callsFake(async (...args: unknown[]) =>
				(args[0] as string[]).map(() => false),
			);

		const request = async (
			peerIndex: number,
			hashes: string[],
			requestIdByte: number,
		) =>
			db.log.onMessage(
				new RequestIPruneV2({
					requests: hashes.map((hash) => ({
						hash,
						requestId: new Uint8Array(32).fill(requestIdByte),
					})),
				}),
				{ from: remoteKeys[peerIndex] } as any,
			);

		try {
			for (let peerIndex = 0; peerIndex < 4; peerIndex++) {
				await request(peerIndex, hashesByPeer[peerIndex], peerIndex + 1);
			}
			expect(log._pendingIHave.size).to.equal(4_096);
			for (let peerIndex = 0; peerIndex < 4; peerIndex++) {
				expect(
					log._pendingIHaveCountByPeer.get(remoteKeys[peerIndex].hashcode()),
				).to.equal(1_024);
			}
			expect(log._pendingIHaveRequesterCount).to.equal(4_096);

			await request(4, hashesByPeer[4], 5);
			expect(log._pendingIHave.size).to.equal(4_096);
			expect(log._pendingIHaveCountByPeer.has(remoteKeys[4].hashcode())).to.be
				.false;

			const firstHash = hashesByPeer[0][0];
			await request(4, [firstHash], 55);
			expect(
				log._pendingIHave
					.get(firstHash)
					.requesting.has(remoteKeys[4].hashcode()),
			).to.be.false;
			expect(log._pendingIHaveRequesterCount).to.equal(4_096);

			const refreshedRequestId = new Uint8Array(32).fill(99);
			await db.log.onMessage(
				new RequestIPruneV2({
					requests: [{ hash: firstHash, requestId: refreshedRequestId }],
				}),
				{ from: remoteKeys[0] } as any,
			);
			expect(
				log._pendingIHave
					.get(firstHash)
					.requestIdsByPeer.get(remoteKeys[0].hashcode()),
			).to.deep.equal(refreshedRequestId);
			expect(
				log._pendingIHaveCountByPeer.get(remoteKeys[0].hashcode()),
			).to.equal(1_024);

			log.cleanupPendingIHavePeer(remoteKeys[0].hashcode());
			expect(log._pendingIHave.size).to.equal(3_072);
			expect(log._pendingIHaveCountByPeer.has(remoteKeys[0].hashcode())).to.be
				.false;
			expect(log._pendingIHaveRequesterCount).to.equal(3_072);

			await request(4, hashesByPeer[4], 6);
			expect(log._pendingIHave.size).to.equal(4_096);
			expect(
				log._pendingIHaveCountByPeer.get(remoteKeys[4].hashcode()),
			).to.equal(1_024);
			expect(log._pendingIHaveRequesterCount).to.equal(4_096);
		} finally {
			log.clearPendingIHaves();
			hasMany.restore();
		}
	});

	it("chunks large outbound prune batches and reuses their request ids", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-outbound-chunking");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		log._peerSyncCapabilities.set(
			remoteHash,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const entries = new Map<string, { entry: any; leaders: typeof leaders }>();
		for (let index = 0; index < 1_025; index++) {
			const hash = `checked-prune-chunk-${index}`;
			entries.set(hash, {
				entry: { hash, meta: entry.meta },
				leaders,
			});
		}
		const attempts: Promise<unknown>[] = [];

		try {
			const firstAttempts = log.prune(entries, { timeout: 30_000 });
			attempts.push(...firstAttempts);
			for (const attempt of firstAttempts) void attempt.catch(() => {});
			await waitForResolved(() => {
				expect(
					send
						.getCalls()
						.filter((call) => call.args[0] instanceof RequestIPruneV2),
				).to.have.length(2);
			});
			const firstBatches = send
				.getCalls()
				.map((call) => call.args[0])
				.filter(
					(message): message is RequestIPruneV2 =>
						message instanceof RequestIPruneV2,
				);
			expect(firstBatches.map((batch) => batch.requests.length)).to.deep.equal([
				1_024, 1,
			]);
			const firstIds = new Map(
				firstBatches
					.flatMap((batch) => batch.requests)
					.map((request) => [request.hash, toHexString(request.requestId)]),
			);
			expect(firstIds.size).to.equal(1_025);

			const secondAttempts = log.prune(entries, { timeout: 30_000 });
			attempts.push(...secondAttempts);
			for (const attempt of secondAttempts) void attempt.catch(() => {});
			await waitForResolved(() => {
				expect(
					send
						.getCalls()
						.filter((call) => call.args[0] instanceof RequestIPruneV2),
				).to.have.length(4);
			});
			const secondBatches = send
				.getCalls()
				.slice(2)
				.map((call) => call.args[0])
				.filter(
					(message): message is RequestIPruneV2 =>
						message instanceof RequestIPruneV2,
				);
			expect(secondBatches.map((batch) => batch.requests.length)).to.deep.equal(
				[1_024, 1],
			);
			for (const request of secondBatches.flatMap((batch) => batch.requests)) {
				expect(toHexString(request.requestId)).to.equal(
					firstIds.get(request.hash),
				);
			}

			send.resetHistory();
			send.resetBehavior();
			send.onFirstCall().rejects(new Error("first chunk failed"));
			send.onSecondCall().resolves();
			const thirdAttempts = log.prune(entries, { timeout: 30_000 });
			attempts.push(...thirdAttempts);
			for (const attempt of thirdAttempts) void attempt.catch(() => {});
			await waitForResolved(() => {
				expect(
					send
						.getCalls()
						.filter((call) => call.args[0] instanceof RequestIPruneV2),
				).to.have.length(2);
			});
			expect(
				send
					.getCalls()
					.filter((call) => call.args[0] instanceof RequestIPruneV2)
					.map((call) => call.args[0].requests.length),
			).to.deep.equal([1_024, 1]);
			expect(
				send
					.getCalls()
					.filter((call) => call.args[0] instanceof RequestIPruneV2)
					.every(
						(call) =>
							call.args[1].mode instanceof AcknowledgeDelivery &&
							call.args[1].signal ===
								log.captureReplicationOwnershipLifecycle().signal,
					),
			).to.be.true;

			send.resetHistory();
			send.resetBehavior();
			const lastHash = "checked-prune-chunk-1024";
			send.onFirstCall().callsFake(() => {
				void log.cancelCheckedPruneForLocalLeader(lastHash);
				return Promise.resolve();
			});
			send.resolves();
			const fourthAttempts = log.prune(entries, { timeout: 30_000 });
			attempts.push(...fourthAttempts);
			for (const attempt of fourthAttempts) void attempt.catch(() => {});
			await waitForResolved(() => {
				expect(
					send
						.getCalls()
						.filter((call) => call.args[0] instanceof RequestIPruneV2),
				).to.have.length(1);
			});
			const fourthBatch = send
				.getCalls()
				.map((call) => call.args[0])
				.find(
					(message): message is RequestIPruneV2 =>
						message instanceof RequestIPruneV2,
				);
			expect(fourthBatch?.requests).to.have.length(1_024);
			expect(fourthBatch?.requests.some((request) => request.hash === lastHash))
				.to.be.false;
		} finally {
			const rejections: Promise<unknown>[] = [];
			for (const hash of entries.keys()) {
				const pending = log._checkedPrune.getPendingDelete(hash);
				if (!pending) continue;
				try {
					rejections.push(
						Promise.resolve(pending.reject(new Error("test cleanup"))),
					);
				} catch (error) {
					rejections.push(Promise.reject(error));
				}
			}
			await Promise.allSettled(rejections);
			await Promise.allSettled(attempts);
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("bounds acknowledged prune sends globally across destinations", async () => {
		session = await TestSession.disconnected(1);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-global-send-bound");
		const entries = new Map<
			string,
			{ entry: any; leaders: Map<string, { intersecting: boolean }> }
		>();
		for (let index = 0; index < 1_025; index++) {
			const hash = `checked-prune-global-send-${index}`;
			const peer = `checked-prune-peer-${index}`;
			const leaders = new Map([[peer, { intersecting: true }]]);
			entries.set(hash, { entry: { hash, meta: entry.meta }, leaders });
			log._peerSyncCapabilities.set(
				peer,
				SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
			);
		}
		const releaseSends = pDefer<void>();
		const fourSendsEntered = pDefer<void>();
		let activeSends = 0;
		let maximumActiveSends = 0;
		let totalSends = 0;
		const send = sinon
			.stub(log.rpc, "send")
			.callsFake(async (message: unknown) => {
				if (!(message instanceof RequestIPruneV2)) {
					return;
				}
				activeSends++;
				totalSends++;
				maximumActiveSends = Math.max(maximumActiveSends, activeSends);
				if (totalSends === 4) {
					fourSendsEntered.resolve();
				}
				try {
					await releaseSends.promise;
				} finally {
					activeSends--;
				}
			});
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const firstEntries = new Map(
			[...entries].filter((_, index) => index % 2 === 0),
		);
		const secondEntries = new Map(
			[...entries].filter((_, index) => index % 2 === 1),
		);
		// Two concurrent prune() calls prove this is an instance-wide bound, not
		// merely a queue local to one invocation.
		const attempts = [
			...log.prune(firstEntries, { timeout: 30_000 }),
			...log.prune(secondEntries, { timeout: 30_000 }),
		];
		for (const attempt of attempts) void attempt.catch(() => {});

		try {
			await fourSendsEntered.promise;
			await new Promise<void>((resolve) => setTimeout(resolve, 20));
			expect(totalSends).to.equal(4);
			expect(activeSends).to.equal(4);
			expect(maximumActiveSends).to.equal(4);

			releaseSends.resolve();
			await waitForResolved(() => expect(totalSends).to.equal(1_025), {
				timeout: 5_000,
				delayInterval: 10,
			});
			expect(maximumActiveSends).to.equal(4);
		} finally {
			releaseSends.resolve();
			const rejections: Promise<unknown>[] = [];
			for (const hash of entries.keys()) {
				const pending = log._checkedPrune.getPendingDelete(hash);
				if (!pending) continue;
				try {
					rejections.push(
						Promise.resolve(pending.reject(new Error("test cleanup"))),
					);
				} catch (error) {
					rejections.push(Promise.reject(error));
				}
			}
			await Promise.allSettled(rejections);
			await Promise.allSettled(attempts);
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("retries an initially failed background request with the same request id", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
				respondToIHaveTimeout: 500,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-initial-send-retry");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		log._peerSyncCapabilities.set(
			remoteHash,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const requestIds: string[] = [];
		const send = sinon
			.stub(log.rpc, "send")
			.callsFake(async (message: unknown) => {
				if (!(message instanceof RequestIPruneV2)) {
					return;
				}
				requestIds.push(toHexString(message.requests[0].requestId));
				if (requestIds.length === 1) {
					throw new Error("forced initial send failure");
				}
			});
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const attempts = log.prune(new Map([[entry.hash, { entry, leaders }]]));
		for (const attempt of attempts) void attempt.catch(() => {});

		try {
			await waitForResolved(
				() => expect(requestIds.length).to.be.greaterThan(1),
				{
					timeout: 2_000,
					delayInterval: 20,
				},
			);
			expect(requestIds[1]).to.equal(requestIds[0]);
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(pending).to.exist;
			expect(
				log._checkedPrune.isCurrentRequestForPeer(
					entry.hash,
					pending,
					remoteHash,
					requestIds[0],
				),
			).to.be.true;
		} finally {
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			await pending?.reject(new Error("test cleanup"));
			await Promise.allSettled(attempts);
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("retries a background generation when preliminary revalidation fails", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-preliminary-revalidation");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		log._peerSyncCapabilities.set(
			remoteHash,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const remove = sinon.spy(log.log, "remove");
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.rejects(new Error("forced preliminary planner failure"));
		const [pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]));
		const outcome = pruning.catch((error: unknown) => error);

		try {
			await waitForResolved(() => {
				expect(log._checkedPrune.getRequestId(entry.hash, remoteHash)).to.be.a(
					"string",
				);
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			const requestId = log._checkedPrune.getRequestId(entry.hash, remoteHash);
			await pending.resolve(remoteHash, requestId);
			expect(await outcome).to.be.instanceOf(Error);
			await waitForResolved(() => {
				expect(log._checkedPrune.getRetry(entry.hash)?.attempts).to.equal(1);
			});
			expect(remove.called).to.be.false;
		} finally {
			log._checkedPrune.clearRetry(entry.hash);
			revalidate.restore();
			getClampedReplicas.restore();
			remove.restore();
			send.restore();
		}
	});

	it("does not guess by retrying an ambiguous lower-log remove failure", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-remove-failure");
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		log._peerSyncCapabilities.set(
			remoteHash,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({ leaders, localLeader: false });
		const remove = sinon
			.stub(log.log, "remove")
			.rejects(new Error("ambiguous lower-log remove failure"));
		const [pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]));
		const outcome = pruning.catch((error: unknown) => error);

		try {
			await waitForResolved(() => {
				expect(log._checkedPrune.getRequestId(entry.hash, remoteHash)).to.be.a(
					"string",
				);
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			const requestId = log._checkedPrune.getRequestId(entry.hash, remoteHash);
			await pending.resolve(remoteHash, requestId);
			expect(await outcome).to.be.instanceOf(Error);
			expect(remove.calledOnce).to.be.true;
			expect(log._checkedPrune.getRetry(entry.hash)).to.be.undefined;
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
		} finally {
			log._checkedPrune.clearRetry(entry.hash);
			remove.restore();
			revalidate.restore();
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("fails closed for legacy prune requests and responses", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-legacy-fail-closed");
		const remoteKey = session.peers[1].identity.publicKey;
		const remoteHash = remoteKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		log._peerSyncCapabilities.set(
			remoteHash,
			SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
		);
		const send = sinon.stub(log.rpc, "send").resolves();
		const getClampedReplicas = sinon
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
				expect(log._checkedPrune.getRequestId(entry.hash, remoteHash)).to.be.a(
					"string",
				);
			});
			const pendingBeforeLegacy = log._checkedPrune.getPendingDelete(
				entry.hash,
			);
			const requestIdBeforeLegacy = log._checkedPrune.getRequestId(
				entry.hash,
				remoteHash,
			);
			await db.log.onMessage(
				new RequestIPrune({
					hashes: [
						entry.hash,
						...Array.from(
							{ length: 4_096 },
							(_, index) => `legacy-unbounded-${index}`,
						),
					],
				}),
				{ from: remoteKey } as any,
			);
			expect(
				send.getCalls().some((call) => call.args[0] instanceof ResponseIPrune),
			).to.be.false;
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.equal(
				pendingBeforeLegacy,
			);
			expect(log._checkedPrune.getRequestId(entry.hash, remoteHash)).to.equal(
				requestIdBeforeLegacy,
			);

			await db.log.onMessage(new ResponseIPrune({ hashes: [entry.hash] }), {
				from: remoteKey,
			} as any);
			expect(
				log._checkedPrune.getConfirmedReplicators(entry.hash)?.size ?? 0,
			).to.equal(0);
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.exist;

			await log.cancelCheckedPruneForLocalLeader(entry.hash);
			expect(await outcome).to.be.instanceOf(Error);
		} finally {
			await log.cancelCheckedPruneForLocalLeader(entry.hash).catch(() => {});
			await Promise.allSettled(pruning ? [pruning] : []);
			getClampedReplicas.restore();
			send.restore();
		}
	});
});
