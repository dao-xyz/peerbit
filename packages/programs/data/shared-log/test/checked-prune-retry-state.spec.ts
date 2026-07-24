import { TestSession } from "@peerbit/test-utils";
import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import {
	RequestIPruneV2,
	SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
} from "../src/exchange-heads.js";
import { EventStore } from "./utils/stores/index.js";

describe("checked prune retry state", () => {
	let session: TestSession | undefined;

	afterEach(async () => {
		await session?.stop();
		session = undefined;
	});

	it("retires a capability-ineligible candidate after the bounded retry window", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-retry-capability-bound");
		log.pruneDebouncedFn.delete(entry.hash);
		log._checkedPrune.clearRetry(entry.hash);
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(leaders);
		const revalidate = sinon
			.stub(log, "revalidateCheckedPruneOwnership")
			.resolves({ leaders, localLeader: false });
		const random = sinon.stub(Math, "random").returns(0);
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });

		try {
			const [initial] = log.prune(new Map([[entry.hash, { entry, leaders }]]));
			await expect(initial).to.be.rejectedWith(
				"Insufficient checked-prune capable leaders",
			);
			expect(log._checkedPrune.getRetry(entry.hash)?.attempts).to.equal(1);

			// 1s, 2s, and 4s retry delays, with one 500ms prune debounce after
			// each attempt. The final debounce observes the exhausted generation
			// and retires it instead of pinning entry/leader objects indefinitely.
			await clock.tickAsync(9_000);
			expect(log._checkedPrune.getRetry(entry.hash)).to.be.undefined;
			expect(log._checkedPrune.hasActiveWork(entry.hash)).to.be.false;

			// A later ownership scan can create a fresh retry generation.
			const [rediscovered] = log.prune(
				new Map([[entry.hash, { entry, leaders }]]),
			);
			await expect(rediscovered).to.be.rejectedWith(
				"Insufficient checked-prune capable leaders",
			);
			expect(log._checkedPrune.getRetry(entry.hash)?.attempts).to.equal(1);
		} finally {
			log._checkedPrune.clearRetry(entry.hash);
			clock.restore();
			random.restore();
			revalidate.restore();
			findLeaders.restore();
			getClampedReplicas.restore();
		}
	});

	it("retires a retry generation after repeated callback failures", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-retry-failure-bound");
		log.pruneDebouncedFn.delete(entry.hash);
		log._checkedPrune.clearRetry(entry.hash);
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.resolves(leaders);
		const enqueue = sinon
			.stub(log, "pruneDebouncedFnAddIfNotKeeping")
			.rejects(new Error("forced retry callback failure"));
		const random = sinon.stub(Math, "random").returns(0);
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });

		try {
			log.scheduleCheckedPruneRetry({ entry, leaders });
			expect(log._checkedPrune.getRetry(entry.hash)?.attempts).to.equal(1);

			await clock.tickAsync(8_000);
			expect(enqueue.callCount).to.equal(3);
			expect(log._checkedPrune.getRetry(entry.hash)).to.be.undefined;
			expect(log._checkedPrune.hasActiveWork(entry.hash)).to.be.false;
		} finally {
			log._checkedPrune.clearRetry(entry.hash);
			clock.restore();
			random.restore();
			enqueue.restore();
			findLeaders.restore();
		}
	});

	it("retires a capability-woken retry when the entry is now retained", async () => {
		session = await TestSession.disconnected(2);
		const keep = sinon.stub().returns(false);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
				keep,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-capability-wake-retained");
		log.pruneDebouncedFn.delete(entry.hash);
		log._checkedPrune.clearRetry(entry.hash);
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });
		const send = sinon.stub(log.rpc, "send").resolves();

		try {
			const [initial] = log.prune(new Map([[entry.hash, { entry, leaders }]]));
			await expect(initial).to.be.rejectedWith(
				"Insufficient checked-prune capable leaders",
			);
			expect(log._checkedPrune.getRetry(entry.hash)?.timer).to.exist;

			keep.returns(true);
			log.recordPeerSyncCapabilities(
				remoteHash,
				SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
			);
			await waitForResolved(
				() => expect(log._checkedPrune.getRetry(entry.hash)).to.be.undefined,
			);

			expect(log._checkedPrune.hasActiveWork(entry.hash)).to.be.false;
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
			expect(
				send.getCalls().some((call) => call.args[0] instanceof RequestIPruneV2),
			).to.be.false;
		} finally {
			log._checkedPrune.clearRetry(entry.hash);
			send.restore();
			getClampedReplicas.restore();
		}
	});

	it("reschedules a capability-woken retry when its keep check fails", async () => {
		session = await TestSession.disconnected(2);
		const keep = sinon.stub().returns(false);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
				keep,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-capability-wake-error");
		log.pruneDebouncedFn.delete(entry.hash);
		log._checkedPrune.clearRetry(entry.hash);
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const getClampedReplicas = sinon
			.stub(log, "getClampedReplicas")
			.returns({ getValue: () => 1 });

		try {
			const [initial] = log.prune(new Map([[entry.hash, { entry, leaders }]]));
			await expect(initial).to.be.rejectedWith(
				"Insufficient checked-prune capable leaders",
			);
			expect(log._checkedPrune.getRetry(entry.hash)?.timer).to.exist;

			keep.rejects(new Error("forced capability-wake keep failure"));
			log.recordPeerSyncCapabilities(
				remoteHash,
				SYNC_CAPABILITY_CHECKED_PRUNE_HANDOFF,
			);
			await waitForResolved(() => {
				const retry = log._checkedPrune.getRetry(entry.hash);
				expect(retry?.attempts).to.equal(1);
				expect(retry?.timer).to.exist;
			});
		} finally {
			log._checkedPrune.clearRetry(entry.hash);
			getClampedReplicas.restore();
		}
	});

	it("does not retain entry objects while a debounce-only candidate is queued", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-debounce-only-candidate");
		log.pruneDebouncedFn.delete(entry.hash);
		log._checkedPrune.clearRetry(entry.hash);
		const queued = pDefer<void>();
		const add = sinon.stub(log.pruneDebouncedFn, "add").returns(queued.promise);
		const leaders = new Map([
			[session.peers[1].identity.publicKey.hashcode(), { intersecting: true }],
		]);

		try {
			expect(log._checkedPrune.sessions.size).to.equal(0);
			expect(
				await log.pruneDebouncedFnAddIfNotKeeping({
					key: entry.hash,
					value: { entry, leaders },
				}),
			).to.be.true;
			expect(add.calledOnce).to.be.true;
			expect(log._checkedPrune.sessions.size).to.equal(0);
		} finally {
			queued.resolve();
			add.restore();
		}
	});

	it("retires an ungranted background generation and schedules a bounded retry", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
				respondToIHaveTimeout: 60_000,
			},
		});
		const log = db.log as any;
		const { entry } = await db.add("checked-prune-ungranted-background");
		log.pruneDebouncedFn.delete(entry.hash);
		log._checkedPrune.clearRetry(entry.hash);
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
		const random = sinon.stub(Math, "random").returns(0);
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });
		let attempt: Promise<unknown> | undefined;

		try {
			[attempt] = log.prune(new Map([[entry.hash, { entry, leaders }]]));
			const outcome = attempt!.catch((error: unknown) => error);
			await clock.tickAsync(0);

			expect(
				send.getCalls().some((call) => call.args[0] instanceof RequestIPruneV2),
			).to.be.true;
			const requestId = log._checkedPrune.getRequestId(entry.hash, remoteHash);
			expect(requestId).to.be.a("string");
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.exist;

			// The generation deadline is exactly 120s for these settings. Stop at
			// that boundary so the separately scheduled 1s retry cannot execute.
			await clock.tickAsync(120_000);
			expect(await outcome).to.be.instanceOf(Error);
			expect(log._checkedPrune.getPendingDelete(entry.hash)).to.be.undefined;
			expect(log._checkedPrune.getRequestId(entry.hash, remoteHash)).to.be
				.undefined;
			expect(log._checkedPrune.getContactedReplicators(entry.hash)).to.be
				.undefined;
			expect(await log.log.has(entry.hash)).to.be.true;

			const retry = log._checkedPrune.getRetry(entry.hash);
			expect(retry?.attempts).to.equal(1);
			expect(retry?.timer).to.exist;
		} finally {
			const pending = log._checkedPrune.getPendingDelete(entry.hash);
			await pending?.reject(new Error("test cleanup"));
			log._checkedPrune.clearRetry(entry.hash);
			await Promise.allSettled(
				[attempt].filter((value): value is Promise<unknown> => value != null),
			);
			clock.restore();
			random.restore();
			getClampedReplicas.restore();
			send.restore();
		}
	});

	it("clears removed entries and fences their in-flight retry callbacks", async () => {
		session = await TestSession.disconnected(2);
		const db = await session.peers[0].open(new EventStore(), {
			args: {
				replicate: false,
				timeUntilRoleMaturity: 0,
				waitForPruneDelay: 0,
			},
		});
		const log = db.log as any;
		const first = await db.add("checked-prune-retry-removed-single");
		const second = await db.add("checked-prune-retry-removed-batch");
		for (const { entry } of [first, second]) {
			log.pruneDebouncedFn.delete(entry.hash);
			log._checkedPrune.clearRetry(entry.hash);
		}
		const remoteHash = session.peers[1].identity.publicKey.hashcode();
		const leaders = new Map([[remoteHash, { intersecting: true }]]);
		const plannerEntered = pDefer<void>();
		const releasePlanner = pDefer<void>();
		const findLeaders = sinon
			.stub(log, "findLeadersFromEntry")
			.callsFake(async () => {
				plannerEntered.resolve();
				await releasePlanner.promise;
				return leaders;
			});
		const enqueue = sinon
			.stub(log, "pruneDebouncedFnAddIfNotKeeping")
			.resolves(true);
		const random = sinon.stub(Math, "random").returns(0);
		const clock = sinon.useFakeTimers({ shouldClearNativeTimers: true });

		try {
			log.scheduleCheckedPruneRetry({ entry: first.entry, leaders });
			await clock.tickAsync(1_000);
			await plannerEntered.promise;

			log.onEntryRemoved(first.entry.hash);
			expect(log._checkedPrune.getRetry(first.entry.hash)).to.be.undefined;

			releasePlanner.resolve();
			await clock.tickAsync(0);
			expect(enqueue.called).to.be.false;

			const batchRetry = {
				attempts: 1,
				entry: second.entry,
				leaders,
				timer: setTimeout(() => {}, 60_000),
			};
			log._checkedPrune.setRetry(second.entry.hash, batchRetry);
			log.onEntryRemovedHashes([second.entry.hash]);
			expect(log._checkedPrune.getRetry(second.entry.hash)).to.be.undefined;
			expect(log._checkedPrune.hasActiveWork(second.entry.hash)).to.be.false;
		} finally {
			releasePlanner.resolve();
			log._checkedPrune.clearRetry(first.entry.hash);
			log._checkedPrune.clearRetry(second.entry.hash);
			clock.restore();
			random.restore();
			enqueue.restore();
			findLeaders.restore();
		}
	});
});
