import { TestSession } from "@peerbit/test-utils";
import {
	AbortError,
	TimeoutError,
	delay,
	waitForResolved,
} from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import {
	AddedReplicationSegmentMessage,
	AllReplicatingSegmentsMessage,
	StoppedReplicating,
} from "../src/replication.js";
import { EventStore } from "./utils/stores/index.js";

describe("replication announcement retries", () => {
	let session: TestSession | undefined;

	const openStore = async (replicate: any) => {
		session = await TestSession.disconnected(1);
		return session.peers[0].open(new EventStore<string, any>(), {
			args: {
				replicate,
				timeUntilRoleMaturity: 0,
			},
		});
	};

	const useFastAnnouncementRetry = (log: any) => {
		log.setupReplicationAnnouncementRetryFunction(10);
	};

	afterEach(async () => {
		sinon.restore();
		if (session) {
			await session.stop();
			session = undefined;
		}
	});

	it("contains an adaptive timeout and retries the locally committed range as a full snapshot", async () => {
		const store = await openStore({ limits: { interval: 60_000 } });
		const log = store.log as any;
		log.adaptiveRebalanceIdleMs = 0;
		log.setupRebalanceDebounceFunction(10);
		useFastAnnouncementRetry(log);

		let targetFactor: number | undefined;
		let stepCalls = 0;
		sinon
			.stub(log.replicationController, "step")
			.callsFake((properties: any) => {
				stepCalls++;
				const currentFactor = properties.currentFactor as number;
				targetFactor ??= currentFactor > 0.2 ? currentFactor / 2 : 0.5;
				return targetFactor;
			});

		const timeout = new TimeoutError("detached fanout shard timed out");
		let incrementalFailed = false;
		const snapshots: AllReplicatingSegmentsMessage[] = [];
		sinon.stub(store.log.rpc, "send").callsFake(async (message: any) => {
			if (
				message instanceof AddedReplicationSegmentMessage &&
				!incrementalFailed
			) {
				incrementalFailed = true;
				throw timeout;
			}
			if (message instanceof AllReplicatingSegmentsMessage) {
				snapshots.push(message);
			}
			return [] as any;
		});

		const unhandled: unknown[] = [];
		const onUnhandledRejection = (reason: unknown) => unhandled.push(reason);
		process.on("unhandledRejection", onUnhandledRejection);

		try {
			void log.rebalanceParticipationDebounced.call();
			await waitForResolved(() => expect(incrementalFailed).to.equal(true), {
				timeout: 2_000,
				delayInterval: 5,
			});

			const locallyCommitted = await store.log.getMyReplicationSegments();
			expect(locallyCommitted).to.have.length(1);
			expect(locallyCommitted[0].widthNormalized).to.be.closeTo(
				targetFactor!,
				1e-6,
			);

			await waitForResolved(() => expect(snapshots).to.have.length(1), {
				timeout: 2_000,
				delayInterval: 5,
			});
			expect(snapshots[0].segments).to.have.length(1);
			expect(snapshots[0].segments[0].id).to.deep.equal(locallyCommitted[0].id);
			expect(snapshots[0].segments[0].factor).to.deep.equal(
				locallyCommitted[0].width,
			);
			expect(log._replicationAnnouncementRetryPending).to.equal(false);
			await waitForResolved(() => expect(stepCalls).to.be.greaterThan(1), {
				timeout: 500,
				delayInterval: 5,
			});

			await delay(0);
			expect(unhandled).to.deep.equal([]);
		} finally {
			process.off("unhandledRejection", onUnhandledRejection);
		}
	});

	it("keeps explicit replicate rejection semantics without retrying an abort", async () => {
		const store = await openStore(false);
		const log = store.log as any;
		useFastAnnouncementRetry(log);

		const abort = new AbortError("fanout channel detached");
		let announcementCalls = 0;
		const snapshots: AllReplicatingSegmentsMessage[] = [];
		sinon.stub(store.log.rpc, "send").callsFake(async (message: any) => {
			if (
				(message instanceof AddedReplicationSegmentMessage ||
					message instanceof AllReplicatingSegmentsMessage) &&
				announcementCalls++ === 0
			) {
				throw abort;
			}
			if (message instanceof AllReplicatingSegmentsMessage) {
				snapshots.push(message);
			}
			return [] as any;
		});

		let rejected: unknown;
		try {
			await store.log.replicate({ factor: 0.25, offset: 0.1 });
		} catch (error) {
			rejected = error;
		}
		expect(rejected).to.equal(abort);

		const locallyCommitted = await store.log.getMyReplicationSegments();
		expect(locallyCommitted).to.have.length(1);
		await delay(40);
		expect(snapshots).to.deep.equal([]);
		expect(log._replicationAnnouncementRetryPending).to.equal(false);
	});

	it("uses exact timeout branding without retrying generic closed or mixed aggregate failures", async () => {
		const store = await openStore(false);
		const log = store.log as any;
		useFastAnnouncementRetry(log);

		const mixed = new Error("mixed replication delivery failure") as Error & {
			errors: unknown[];
		};
		mixed.errors = [
			new TimeoutError("one shard timed out"),
			new Error("invalid replication payload"),
		];

		expect(
			log.queueCurrentReplicationStateAnnouncementRetry(
				new Error("fanout channel closed while detached"),
			),
		).to.equal(false);
		expect(log.queueCurrentReplicationStateAnnouncementRetry(mixed)).to.equal(
			false,
		);
		expect(log._replicationAnnouncementRetryPending).to.equal(false);

		const crossPackageTimeout = {
			constructor: { name: "TimeoutError" },
			name: "Error",
		};
		expect(
			log.queueCurrentReplicationStateAnnouncementRetry(crossPackageTimeout),
		).to.equal(true);
		await waitForResolved(
			() => expect(log._replicationAnnouncementRetryPending).to.equal(false),
			{ timeout: 2_000, delayInterval: 5 },
		);
	});

	it("follows an in-flight stale snapshot with the state from a newer successful announcement", async () => {
		const store = await openStore(false);
		const log = store.log as any;
		useFastAnnouncementRetry(log);

		const firstRangeId = new Uint8Array(32).fill(7);
		const secondRangeId = new Uint8Array(32).fill(8);
		const timeout = new TimeoutError("detached shard timed out");
		let incrementalFailed = false;
		let releaseStaleSnapshot!: () => void;
		const staleSnapshotGate = new Promise<void>((resolve) => {
			releaseStaleSnapshot = resolve;
		});
		const snapshots: AllReplicatingSegmentsMessage[] = [];
		const successfulIncrementals: AddedReplicationSegmentMessage[] = [];
		sinon.stub(store.log.rpc, "send").callsFake(async (message: any) => {
			if (
				message instanceof AddedReplicationSegmentMessage &&
				!incrementalFailed
			) {
				incrementalFailed = true;
				throw timeout;
			}
			if (message instanceof AllReplicatingSegmentsMessage) {
				snapshots.push(message);
				if (snapshots.length === 1) {
					await staleSnapshotGate;
				}
			}
			if (message instanceof AddedReplicationSegmentMessage) {
				successfulIncrementals.push(message);
			}
			return [] as any;
		});

		try {
			let rejected: unknown;
			try {
				await store.log.replicate({
					id: firstRangeId,
					factor: 0.25,
					offset: 0.1,
				});
			} catch (error) {
				rejected = error;
			}
			expect(rejected).to.equal(timeout);

			await waitForResolved(() => expect(snapshots).to.have.length(1), {
				timeout: 2_000,
				delayInterval: 5,
			});

			// This successful incremental send contains only the second range. If the
			// stale reset arrives after it, a final current reset is required to
			// restore both ranges remotely.
			await store.log.replicate({
				id: secondRangeId,
				factor: 0.2,
				offset: 0.6,
			});
			const current = await store.log.getMyReplicationSegments();
			expect(current).to.have.length(2);
			expect(successfulIncrementals).to.have.length(1);
			expect(successfulIncrementals[0].segments).to.have.length(1);
			expect(successfulIncrementals[0].segments[0].id).to.deep.equal(
				secondRangeId,
			);
			expect(snapshots[0].segments).to.have.length(1);
			expect(snapshots[0].segments[0].id).to.deep.equal(firstRangeId);

			releaseStaleSnapshot();
			await waitForResolved(
				() => {
					expect(snapshots).to.have.length(2);
					expect(log._replicationAnnouncementRetryPending).to.equal(false);
				},
				{ timeout: 2_000, delayInterval: 5 },
			);
			expect(snapshots[1].segments).to.have.length(2);
			const finalIds = snapshots[1].segments.map((segment) =>
				Array.from(segment.id).join(","),
			);
			expect(finalIds).to.have.members([
				Array.from(firstRangeId).join(","),
				Array.from(secondRangeId).join(","),
			]);
		} finally {
			releaseStaleSnapshot();
		}
	});

	it("recollects current state when a newer announcement starts during snapshot collection", async () => {
		const store = await openStore(false);
		const log = store.log as any;
		useFastAnnouncementRetry(log);

		const firstRangeId = new Uint8Array(32).fill(11);
		const secondRangeId = new Uint8Array(32).fill(12);
		const timeout = new TimeoutError("detached shard timed out");
		let incrementalFailed = false;
		let collectionStarted = false;
		let releaseCollection!: () => void;
		const collectionGate = new Promise<void>((resolve) => {
			releaseCollection = resolve;
		});
		const originalGetSegments = store.log.getMyReplicationSegments.bind(
			store.log,
		);
		let heldCollection = false;
		sinon.stub(store.log, "getMyReplicationSegments").callsFake(async () => {
			const captured = await originalGetSegments();
			if (incrementalFailed && !heldCollection) {
				heldCollection = true;
				collectionStarted = true;
				await collectionGate;
			}
			return captured;
		});

		const snapshots: AllReplicatingSegmentsMessage[] = [];
		sinon.stub(store.log.rpc, "send").callsFake(async (message: any) => {
			if (
				message instanceof AddedReplicationSegmentMessage &&
				!incrementalFailed
			) {
				incrementalFailed = true;
				throw timeout;
			}
			if (message instanceof AllReplicatingSegmentsMessage) {
				snapshots.push(message);
			}
			return [] as any;
		});

		try {
			try {
				await store.log.replicate({
					id: firstRangeId,
					factor: 0.25,
					offset: 0.1,
				});
			} catch (error) {
				expect(error).to.equal(timeout);
			}

			await waitForResolved(() => expect(collectionStarted).to.equal(true), {
				timeout: 2_000,
				delayInterval: 5,
			});
			await store.log.replicate({
				id: secondRangeId,
				factor: 0.2,
				offset: 0.6,
			});
			releaseCollection();

			await waitForResolved(
				() => {
					expect(snapshots).to.have.length(1);
					expect(log._replicationAnnouncementRetryPending).to.equal(false);
				},
				{ timeout: 2_000, delayInterval: 5 },
			);
			const snapshotIds = snapshots[0].segments.map((segment) =>
				Array.from(segment.id).join(","),
			);
			expect(snapshotIds).to.have.members([
				Array.from(firstRangeId).join(","),
				Array.from(secondRangeId).join(","),
			]);
		} finally {
			releaseCollection();
		}
	});

	it("clears pending after a non-timeout retry-worker failure", async () => {
		const store = await openStore(false);
		const log = store.log as any;
		useFastAnnouncementRetry(log);

		const timeout = new TimeoutError("detached shard timed out");
		const workerError = new Error("synthetic snapshot encoding failure");
		let incrementalFailed = false;
		let snapshotAttempts = 0;
		sinon.stub(store.log.rpc, "send").callsFake(async (message: any) => {
			if (
				message instanceof AddedReplicationSegmentMessage &&
				!incrementalFailed
			) {
				incrementalFailed = true;
				throw timeout;
			}
			if (message instanceof AllReplicatingSegmentsMessage) {
				snapshotAttempts++;
				throw workerError;
			}
			return [] as any;
		});

		try {
			await store.log.replicate({ factor: 0.25, offset: 0.1 });
		} catch (error) {
			expect(error).to.equal(timeout);
		}
		await waitForResolved(
			() => {
				expect(snapshotAttempts).to.equal(1);
				expect(log._replicationAnnouncementRetryPending).to.equal(false);
			},
			{ timeout: 2_000, delayInterval: 5 },
		);
		await delay(40);
		expect(snapshotAttempts).to.equal(1);
	});

	it("retries an authoritative empty snapshot after unreplicate removes local state", async () => {
		const store = await openStore({ factor: 0.5, offset: 0.1 });
		const log = store.log as any;
		useFastAnnouncementRetry(log);

		const detached = new TimeoutError("detached shard timed out");
		let emptyAnnouncementCalls = 0;
		const successfulEmptySnapshots: AllReplicatingSegmentsMessage[] = [];
		sinon.stub(store.log.rpc, "send").callsFake(async (message: any) => {
			if (
				message instanceof AllReplicatingSegmentsMessage &&
				message.segments.length === 0
			) {
				if (emptyAnnouncementCalls++ === 0) {
					throw detached;
				}
				successfulEmptySnapshots.push(message);
			}
			return [] as any;
		});

		let rejected: unknown;
		try {
			await store.log.unreplicate();
		} catch (error) {
			rejected = error;
		}
		expect(rejected).to.equal(detached);
		expect(await store.log.getMyReplicationSegments()).to.deep.equal([]);

		await waitForResolved(
			() => expect(successfulEmptySnapshots).to.have.length(1),
			{ timeout: 2_000, delayInterval: 5 },
		);
		expect(log._isReplicating).to.equal(false);
		expect(log._replicationAnnouncementRetryPending).to.equal(false);
	});

	it("repairs a partial removal after an aggregate delivery timeout", async () => {
		const store = await openStore({ factor: 0.5, offset: 0.1 });
		const log = store.log as any;
		useFastAnnouncementRetry(log);
		const [segment] = await store.log.getMyReplicationSegments();
		expect(segment).to.exist;

		const aggregate = new Error("replication delivery failed") as Error & {
			errors: unknown[];
		};
		aggregate.errors = [new TimeoutError("detached shard timed out")];
		let stoppedFailed = false;
		const snapshots: AllReplicatingSegmentsMessage[] = [];
		sinon.stub(store.log.rpc, "send").callsFake(async (message: any) => {
			if (message instanceof StoppedReplicating && !stoppedFailed) {
				stoppedFailed = true;
				throw aggregate;
			}
			if (message instanceof AllReplicatingSegmentsMessage) {
				snapshots.push(message);
			}
			return [] as any;
		});

		let rejected: unknown;
		try {
			await store.log.unreplicate([{ id: segment.id }]);
		} catch (error) {
			rejected = error;
		}
		expect(rejected).to.equal(aggregate);
		expect(await store.log.getMyReplicationSegments()).to.deep.equal([]);

		await waitForResolved(() => expect(snapshots).to.have.length(1), {
			timeout: 2_000,
			delayInterval: 5,
		});
		expect(snapshots[0].segments).to.deep.equal([]);
		expect(log._replicationAnnouncementRetryPending).to.equal(false);
	});

	it("cancels a pending authoritative retry before the close announcement", async () => {
		const store = await openStore(false);
		const log = store.log as any;
		log.setupReplicationAnnouncementRetryFunction(100);

		const timeout = new TimeoutError("detached shard timed out");
		let incrementalFailed = false;
		const nonEmptySnapshots: AllReplicatingSegmentsMessage[] = [];
		sinon.stub(store.log.rpc, "send").callsFake(async (message: any) => {
			if (
				message instanceof AddedReplicationSegmentMessage &&
				!incrementalFailed
			) {
				incrementalFailed = true;
				throw timeout;
			}
			if (
				message instanceof AllReplicatingSegmentsMessage &&
				message.segments.length > 0
			) {
				nonEmptySnapshots.push(message);
			}
			return [] as any;
		});

		try {
			await store.log.replicate({ factor: 0.25, offset: 0.1 });
		} catch (error) {
			expect(error).to.equal(timeout);
		}
		expect(incrementalFailed).to.equal(true);
		expect(log._replicationAnnouncementRetryPending).to.equal(true);

		await store.close();
		await delay(150);
		expect(nonEmptySnapshots).to.deep.equal([]);
		expect(log._replicationAnnouncementRetryPending).to.equal(false);
	});
});
