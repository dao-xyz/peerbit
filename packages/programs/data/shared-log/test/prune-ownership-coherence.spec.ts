import { Ed25519Keypair, randomBytes } from "@peerbit/crypto";
import { TerminalOperationNotStartedError } from "@peerbit/program";
import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF } from "../src/exchange-heads.js";
import { EventStore } from "./utils/stores/index.js";

const enableCheckedPrunePeer = (log: any, peer: string) => {
	log._peerSyncCapabilities.set(peer, SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF);
};

const resolveCurrentCheckedPrune = async (
	log: any,
	hash: string,
	peer: string,
	pending: any,
) => {
	await waitForResolved(() => {
		expect(log._checkedPrune.getRequestId(hash, peer)).to.be.a("string");
	});
	const requestId = log._checkedPrune.getRequestId(hash, peer);
	await pending.resolve(peer, requestId);
};

describe("checked prune ownership coherence", () => {
	let session: TestSession | undefined;

	afterEach(async () => {
		await session?.stop();
	});

	it("retains an entry when local ownership commits at the final prune boundary", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-ownership-race");
		const selfHash = session.peers[0].identity.publicKey.hashcode();
		const remoteHash = (await Ed25519Keypair.create()).publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		enableCheckedPrunePeer(log, remoteHash);
		const preliminaryCheckFinished = pDefer<void>();
		const releasePreliminaryCheck = pDefer<void>();

		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const send = sinon.stub(log.rpc, "send").resolves();
		const remove = sinon.spy(log.log, "remove");
		let revalidationCount = 0;
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.callsFake(async (args: any) => {
				revalidationCount++;
				if (revalidationCount === 1) {
					preliminaryCheckFinished.resolve();
					await releasePreliminaryCheck.promise;
					return { leaders: args.leaders, localLeader: false };
				}
				expect(await db.log.countReplicationSegments()).to.equal(1);
				return {
					leaders: new Map([[selfHash, { intersecting: true }]]),
					localLeader: true,
				};
			});

		try {
			expect(await log.log.has(entry.hash)).to.be.true;
			expect(await log.log.blocks.has(entry.hash)).to.be.true;

			const [pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(pending).to.exist;
			await resolveCurrentCheckedPrune(log, entry.hash, remoteHash, pending);
			await preliminaryCheckFinished.promise;

			// Commit authoritative local ownership while the prune callback is
			// paused after its preliminary planner result. The destructive path
			// must revalidate in the range-mutation lane after this commit.
			await db.log.replicate(
				{ factor: 1, offset: 0 },
				{ reset: true, rebalance: false },
			);
			expect(await db.log.countReplicationSegments()).to.equal(1);

			releasePreliminaryCheck.resolve();
			await expect(pruning).to.be.rejectedWith(
				"Failed to delete, is leader again",
			);

			expect(revalidationCount).to.equal(2);
			expect(remove.called).to.be.false;
			expect(await log.log.has(entry.hash)).to.be.true;
			expect(await log.log.blocks.has(entry.hash)).to.be.true;
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
		} finally {
			releasePreliminaryCheck.resolve();
			getClampedReplicas.restore();
			send.restore();
			remove.restore();
			revalidate.restore();
		}
	});

	it("retains an entry when the final ownership planner rejects", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-final-planner-rejection");
		const remoteHash = (await Ed25519Keypair.create()).publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		enableCheckedPrunePeer(log, remoteHash);
		const plannerError = new Error("forced final ownership planner rejection");

		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const send = sinon.stub(log.rpc, "send").resolves();
		const cancelLocalLeader = sinon
			.stub(log, "cancelCheckedPruneForLocalLeader")
			.resolves();
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.rejects(plannerError);
		const originalRevalidate = log.revalidateCheckedPruneOwnership.bind(log);
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.callsFake((args: any) =>
				args.requireFreshLeaderDecision
					? originalRevalidate({
							...args,
							leaders: new Map([[remoteHash, { intersecting: true }]]),
						})
					: Promise.resolve({ leaders: args.leaders, localLeader: false }),
			);
		const remove = sinon.spy(log.log, "remove");

		try {
			const [pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(pending).to.exist;
			await resolveCurrentCheckedPrune(log, entry.hash, remoteHash, pending);

			await expect(pruning).to.be.rejectedWith(plannerError.message);
			expect(revalidate.callCount).to.equal(2);
			expect(revalidate.secondCall.args[0].requireFreshLeaderDecision).to.be
				.true;
			expect(findLeaders.calledOnce).to.be.true;
			expect(remove.called).to.be.false;
			expect(await log.log.has(entry.hash)).to.be.true;
			expect(await log.log.blocks.has(entry.hash)).to.be.true;
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
		} finally {
			getClampedReplicas.restore();
			send.restore();
			cancelLocalLeader.restore();
			findLeaders.restore();
			revalidate.restore();
			remove.restore();
		}
	});

	it("retains and retries a background prune when final ownership is empty", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-final-empty-ownership");
		const remoteHash = (await Ed25519Keypair.create()).publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		enableCheckedPrunePeer(log, remoteHash);

		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const send = sinon.stub(log.rpc, "send").resolves();
		const cancelLocalLeader = sinon
			.stub(log, "cancelCheckedPruneForLocalLeader")
			.resolves();
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(new Map());
		const originalRevalidate = log.revalidateCheckedPruneOwnership.bind(log);
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.callsFake((args: any) =>
				args.requireFreshLeaderDecision
					? originalRevalidate({
							...args,
							leaders: new Map([[remoteHash, { intersecting: true }]]),
						})
					: Promise.resolve({ leaders: args.leaders, localLeader: false }),
			);
		const scheduleRetry = sinon.spy(log, "scheduleCheckedPruneRetry");
		const remove = sinon.spy(log.log, "remove");

		try {
			const [pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]));
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(pending).to.exist;
			await resolveCurrentCheckedPrune(log, entry.hash, remoteHash, pending);

			await expect(pruning).to.be.rejectedWith(
				"Could not establish current leaders at the checked-prune delete boundary",
			);
			expect(revalidate.callCount).to.equal(2);
			expect(revalidate.secondCall.args[0].requireFreshLeaderDecision).to.be
				.true;
			expect(findLeaders.calledOnce).to.be.true;
			expect(scheduleRetry.callCount).to.be.greaterThanOrEqual(1);
			expect(log._checkedPrune.getRetry(entry.hash)).to.exist;
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
			log._checkedPrune.clearRetry(entry.hash);

			expect(remove.called).to.be.false;
			expect(await log.log.has(entry.hash)).to.be.true;
			expect(await log.log.blocks.has(entry.hash)).to.be.true;
		} finally {
			log._checkedPrune.clearRetry(entry.hash);
			getClampedReplicas.restore();
			send.restore();
			cancelLocalLeader.restore();
			findLeaders.restore();
			revalidate.restore();
			scheduleRetry.restore();
			remove.restore();
		}
	});

	it("does not delete when a queued final prune is cancelled before entering the ownership lane", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-cancelled-at-lane");
		const remoteHash = (await Ed25519Keypair.create()).publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		enableCheckedPrunePeer(log, remoteHash);
		const laneEntered = pDefer<void>();
		const releaseLane = pDefer<void>();

		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const send = sinon.stub(log.rpc, "send").resolves();
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({ leaders, localLeader: false });
		const remove = sinon.spy(log.log, "remove");
		const laneBlocker = log.withReplicationRangeMutationQueue(async () => {
			laneEntered.resolve();
			await releaseLane.promise;
		});

		try {
			await laneEntered.promise;
			const [pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			const pruningOutcome = pruning.then(
				() => ({ status: "fulfilled" as const }),
				(error: unknown) => ({ status: "rejected" as const, error }),
			);
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(pending).to.exist;
			await resolveCurrentCheckedPrune(log, entry.hash, remoteHash, pending);
			await waitForResolved(
				() => {
					expect(revalidate.calledOnce).to.be.true;
				},
				{ timeout: 2_000, delayInterval: 5 },
			);

			await log.cancelCheckedPruneForLocalLeader(entry.hash);
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
			releaseLane.resolve();
			await laneBlocker;

			const outcome = await pruningOutcome;
			expect(outcome.status).to.equal("rejected");
			expect("error" in outcome ? outcome.error : undefined).to.be.instanceOf(
				Error,
			);
			await new Promise((resolve) => setTimeout(resolve, 20));
			expect(remove.called).to.be.false;
			expect(await log.log.has(entry.hash)).to.be.true;
			expect(await log.log.blocks.has(entry.hash)).to.be.true;
		} finally {
			releaseLane.resolve();
			await laneBlocker.catch(() => {});
			getClampedReplicas.restore();
			send.restore();
			revalidate.restore();
			remove.restore();
		}
	});

	it("revokes a confirmation before the final delete boundary", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-revoked-confirmation");
		const remoteHash = (await Ed25519Keypair.create()).publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		enableCheckedPrunePeer(log, remoteHash);
		const laneEntered = pDefer<void>();
		const releaseLane = pDefer<void>();

		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const send = sinon.stub(log.rpc, "send").resolves();
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({ leaders, localLeader: false });
		const remove = sinon.spy(log.log, "remove");
		const laneBlocker = log.withReplicationRangeMutationQueue(async () => {
			laneEntered.resolve();
			await releaseLane.promise;
		});

		try {
			await laneEntered.promise;
			const [pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(pending).to.exist;
			await resolveCurrentCheckedPrune(log, entry.hash, remoteHash, pending);
			await waitForResolved(() => {
				expect(revalidate.calledOnce).to.be.true;
				expect(
					log._checkedPrune
						.getConfirmedReplicators(entry.hash)
						?.has(remoteHash),
				).to.be.true;
			});

			log._checkedPrune.removeRequestSent(entry.hash, remoteHash);
			expect(
				log._checkedPrune
					.getConfirmedReplicators(entry.hash)
					?.has(remoteHash) ?? false,
			).to.be.false;
			releaseLane.resolve();
			await laneBlocker;

			await expect(pruning).to.be.rejectedWith(
				"Checked prune confirmation is no longer active at the delete boundary",
			);
			expect(remove.called).to.be.false;
			expect(await log.log.has(entry.hash)).to.be.true;
			expect(await log.log.blocks.has(entry.hash)).to.be.true;
		} finally {
			releaseLane.resolve();
			await laneBlocker.catch(() => {});
			getClampedReplicas.restore();
			send.restore();
			revalidate.restore();
			remove.restore();
		}
	});

	it("rejects a reentrant local mutation while preserving queued remote ownership", async () => {
		session = await TestSession.disconnected(2);
		const reentrantAttempted = pDefer<void>();
		const removalCallbackHoldingLane = pDefer<void>();
		const releaseRemovalCallback = pDefer<void>();
		let reentrantError: unknown;
		let log: any;
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
				onChange: async (change) => {
					if (change.removed.length === 0 || !log) {
						return;
					}
					// Re-entry after an async suspension is still part of this
					// removal callback and must fail instead of waiting on the lane
					// that is waiting for the callback itself.
					await Promise.resolve();
					try {
						await log.replicate(
							{ factor: 1, offset: 0 },
							{ reset: true, rebalance: false },
						);
					} catch (error) {
						reentrantError = error;
					} finally {
						reentrantAttempted.resolve();
					}
					removalCallbackHoldingLane.resolve();
					await releaseRemovalCallback.promise;
				},
			},
		});
		log = db.log as any;
		const { entry } = await db.add("checked-prune-reentrant-callback");
		const remoteKey = (await Ed25519Keypair.create()).publicKey;
		const remoteHash = remoteKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		enableCheckedPrunePeer(log, remoteHash);
		const remoteRange = new log.indexableDomain.constructorRange({
			id: randomBytes(32),
			offset: log.indexableDomain.numbers.denormalize(0.25),
			width: log.indexableDomain.numbers.denormalize(0.25),
			publicKeyHash: remoteHash,
			timestamp: 1n,
		});

		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const send = sinon.stub(log.rpc, "send").resolves();
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({ leaders, localLeader: false });

		try {
			const [pruning] = log.prune(new Map([[entry.hash, { entry, leaders }]]), {
				timeout: 5_000,
			});
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			expect(pending).to.exist;
			await resolveCurrentCheckedPrune(log, entry.hash, remoteHash, pending);
			await Promise.all([
				reentrantAttempted.promise,
				removalCallbackHoldingLane.promise,
			]);

			expect(reentrantError).to.be.instanceOf(Error);
			expect((reentrantError as Error).message).to.include(
				"cannot start during a checked-prune removal callback",
			);
			expect(await log.countReplicationSegments()).to.equal(0);
			expect(log._isReplicating).to.be.false;

			let remoteMutationSettled = false;
			const remoteMutation = log
				.addReplicationRange([remoteRange], remoteKey, {
					checkDuplicates: false,
					rebalance: false,
				})
				.then(
					(result: unknown) => {
						remoteMutationSettled = true;
						return result;
					},
					(error: unknown) => {
						remoteMutationSettled = true;
						throw error;
					},
				);
			await Promise.resolve();
			await Promise.resolve();
			expect(remoteMutationSettled).to.be.false;

			releaseRemovalCallback.resolve();
			await Promise.all([pruning, remoteMutation]);

			const durableRanges = await log.getAllReplicationSegments();
			expect(durableRanges).to.have.length(1);
			expect(durableRanges[0].hash).to.equal(remoteHash);
			expect(durableRanges[0].rangeHash).to.equal(remoteRange.rangeHash);
			expect(await log.log.has(entry.hash)).to.be.false;
			expect(await log.log.blocks.has(entry.hash)).to.be.false;
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
		} finally {
			releaseRemovalCallback.resolve();
			getClampedReplicas.restore();
			send.restore();
			revalidate.restore();
		}
	});

	for (const operation of ["close", "drop"] as const) {
		it(`rejects reentrant ${operation} before terminal fences and permits a later ${operation}`, async () => {
			session = await TestSession.disconnected(2);
			const terminalAttempted = pDefer<void>();
			const releaseRemovalCallback = pDefer<void>();
			let terminalError: unknown;
			let log: any;
			const db = await session.peers[0].open(new EventStore(), {
				args: {
					replicate: false,
					timeUntilRoleMaturity: 0,
					waitForPruneDelay: 0,
					onChange: async (change) => {
						if (change.removed.length === 0 || !log) {
							return;
						}
						await Promise.resolve();
						try {
							await log[operation]();
						} catch (error) {
							terminalError = error;
						} finally {
							terminalAttempted.resolve();
						}
						await releaseRemovalCallback.promise;
					},
				},
			});
			log = db.log as any;
			const { entry } = await db.add(
				`checked-prune-reentrant-${operation}-callback`,
			);
			const remoteHash = (await Ed25519Keypair.create()).publicKey.hashcode();
			const leaders = new Map([[remoteHash, { intersecting: true }]]);
			enableCheckedPrunePeer(log, remoteHash);

			const getClampedReplicas = sinon
				.stub(log, "getClampedReplicas")
				.returns({ getValue: () => 1 });
			const send = sinon.stub(log.rpc, "send").resolves();
			const revalidate = sinon
				.stub(log, "revalidateCheckedPruneOwnership")
				.resolves({ leaders, localLeader: false });
			const lowerClose = sinon.spy(log.log, "close");
			const lowerDrop = sinon.spy(log.log, "drop");

			try {
				const [pruning] = log.prune(
					new Map([[entry.hash, { entry, leaders }]]),
					{ timeout: 5_000 },
				);
				const pending = log._checkedPrune.getPendingDelete(entry.hash);
				expect(pending).to.exist;
				await resolveCurrentCheckedPrune(log, entry.hash, remoteHash, pending);
				await terminalAttempted.promise;

				expect(terminalError).to.be.instanceOf(
					TerminalOperationNotStartedError,
				);
				expect((terminalError as Error).message).to.include(
					`${operation} cannot start during a checked-prune removal callback`,
				);
				expect(log.closed).to.be.false;
				expect(log._replicationRangeMutationsClosing).to.be.false;
				expect(log._pruneRemovesClosing).to.be.false;
				expect(lowerClose.called).to.be.false;
				expect(lowerDrop.called).to.be.false;

				releaseRemovalCallback.resolve();
				await pruning;
				expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
				expect(await log.log.has(entry.hash)).to.be.false;
				expect(await log.log.blocks.has(entry.hash)).to.be.false;

				expect(await log[operation]()).to.be.true;
				expect(log.closed).to.be.true;
				if (operation === "close") {
					expect(lowerClose.calledOnce).to.be.true;
					expect(lowerDrop.called).to.be.false;
				} else {
					expect(lowerDrop.calledOnce).to.be.true;
				}
			} finally {
				releaseRemovalCallback.resolve();
				getClampedReplicas.restore();
				send.restore();
				revalidate.restore();
				lowerClose.restore();
				lowerDrop.restore();
			}
		});
	}
});
