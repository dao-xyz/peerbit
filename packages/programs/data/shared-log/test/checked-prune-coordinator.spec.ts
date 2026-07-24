import { expect } from "chai";
import pDefer from "p-defer";
import {
	CheckedPruneCoordinator,
	type CheckedPrunePendingDelete,
} from "../src/checked-prune.js";

const requestId = (value: number) => new Uint8Array(32).fill(value);

const pendingDelete = (
	id: Uint8Array,
	onClear: () => void = () => {},
): CheckedPrunePendingDelete => ({
	requestId: id,
	promise: pDefer<void>(),
	clear: onClear,
	resolve: () => {},
	reject: () => {},
});

const setPending = (
	coordinator: CheckedPruneCoordinator<Uint8Array, "u32">,
	hash: string,
	pending: CheckedPrunePendingDelete,
) => coordinator.setPendingDelete(hash, pending, {} as any, new Map());

describe("checked prune coordinator", () => {
	it("accepts only exact responses from contacted peers", () => {
		const coordinator = new CheckedPruneCoordinator<Uint8Array, "u32">();
		const pending = pendingDelete(requestId(1));
		setPending(coordinator, "hash", pending);
		expect(coordinator.addRequestSent("hash", "peer", pending)).true;

		expect(
			coordinator.addConfirmedReplicator("hash", "peer", pending, requestId(2)),
		).equal(undefined);
		expect(
			coordinator.addConfirmedReplicator(
				"hash",
				"other-peer",
				pending,
				pending.requestId,
			),
		).equal(undefined);

		const confirmed = coordinator.addConfirmedReplicator(
			"hash",
			"peer",
			pending,
			pending.requestId,
		);
		expect(confirmed).to.deep.equal(new Set(["peer"]));
		expect(
			coordinator.addConfirmedReplicator(
				"hash",
				"peer",
				pending,
				pending.requestId,
			),
		).to.deep.equal(new Set(["peer"]));
		expect(
			coordinator.getExactConfirmedReplicators("hash", pending),
		).to.deep.equal(new Set(["peer"]));
	});

	it("fences stale pending generations", () => {
		const coordinator = new CheckedPruneCoordinator<Uint8Array, "u32">();
		const first = pendingDelete(requestId(1));
		const second = pendingDelete(requestId(2));
		setPending(coordinator, "hash", first);
		expect(coordinator.addRequestSent("hash", "peer", first)).true;
		expect(
			coordinator.addConfirmedReplicator(
				"hash",
				"peer",
				first,
				first.requestId,
			),
		).to.not.equal(undefined);

		setPending(coordinator, "hash", second);
		expect(coordinator.getContactedReplicators("hash")).equal(undefined);
		expect(coordinator.getConfirmedReplicators("hash")).equal(undefined);
		expect(coordinator.addRequestSent("hash", "peer", first)).false;
		expect(coordinator.markRemoving("hash", first)).false;
		expect(coordinator.markDone("hash", first)).false;
		expect(coordinator.markCancelled("hash", first)).false;
		expect(coordinator.getPendingDelete("hash")).equal(second);
	});

	it("permanently revokes a removed peer for the current generation", () => {
		const coordinator = new CheckedPruneCoordinator<Uint8Array, "u32">();
		const pending = pendingDelete(requestId(1));
		setPending(coordinator, "hash", pending);
		coordinator.addRequestSent("hash", "peer", pending);
		coordinator.addConfirmedReplicator(
			"hash",
			"peer",
			pending,
			pending.requestId,
		);

		coordinator.removeRequestSent("hash", "peer");
		expect(
			coordinator.getExactConfirmedReplicators("hash", pending).size,
		).equal(0);
		expect(coordinator.addRequestSent("hash", "peer", pending)).false;

		const replacement = pendingDelete(requestId(2));
		setPending(coordinator, "hash", replacement);
		expect(coordinator.addRequestSent("hash", "peer", replacement)).true;
		coordinator.addConfirmedReplicator(
			"hash",
			"peer",
			replacement,
			replacement.requestId,
		);
		coordinator.cleanupPeer("peer");
		expect(
			coordinator.getExactConfirmedReplicators("hash", replacement).size,
		).equal(0);
		expect(coordinator.addRequestSent("hash", "peer", replacement)).false;
	});

	it("revokes a cleanup peer before the first request is emitted", () => {
		const coordinator = new CheckedPruneCoordinator<Uint8Array, "u32">();
		const pending = pendingDelete(requestId(1));
		setPending(coordinator, "hash", pending);

		coordinator.cleanupPeer("peer");
		expect(coordinator.addRequestSent("hash", "peer", pending)).false;

		const replacement = pendingDelete(requestId(2));
		setPending(coordinator, "hash", replacement);
		expect(coordinator.addRequestSent("hash", "peer", replacement)).true;
	});

	it("invalidates exact generations led by or sent to a removed peer", () => {
		const coordinator = new CheckedPruneCoordinator<Uint8Array, "u32">();
		const leaderPending = pendingDelete(requestId(1));
		const contactedPending = pendingDelete(requestId(2));
		const unrelatedPending = pendingDelete(requestId(3));
		const leaderEntry = { hash: "leader" } as any;
		const contactedEntry = { hash: "contacted" } as any;
		const unrelatedEntry = { hash: "unrelated" } as any;
		const leaderMap = new Map([["peer", { intersecting: true }]]);
		const contactedLeaders = new Set(["other-peer"]);

		coordinator.setPendingDelete(
			"leader",
			leaderPending,
			leaderEntry,
			leaderMap,
		);
		coordinator.setPendingDelete(
			"contacted",
			contactedPending,
			contactedEntry,
			contactedLeaders,
		);
		coordinator.setPendingDelete(
			"unrelated",
			unrelatedPending,
			unrelatedEntry,
			new Set(["other-peer"]),
		);
		expect(coordinator.addRequestSent("contacted", "peer", contactedPending))
			.true;

		const invalidated = coordinator.cleanupPeer("peer");
		expect(invalidated.map(({ hash }) => hash).sort()).to.deep.equal([
			"contacted",
			"leader",
		]);
		expect(invalidated.find(({ hash }) => hash === "leader")?.pending).equal(
			leaderPending,
		);
		expect(invalidated.find(({ hash }) => hash === "contacted")?.entry).equal(
			contactedEntry,
		);
		const snapshottedLeaderMap = invalidated.find(
			({ hash }) => hash === "leader",
		)?.leaders;
		expect(snapshottedLeaderMap).to.deep.equal(leaderMap);
		expect(snapshottedLeaderMap).to.not.equal(leaderMap);
		expect(coordinator.addRequestSent("leader", "peer", leaderPending)).false;
		expect(coordinator.addRequestSent("contacted", "peer", contactedPending))
			.false;
		expect(coordinator.addRequestSent("unrelated", "peer", unrelatedPending))
			.false;

		for (const generation of invalidated) {
			coordinator.markCancelled(generation.hash, generation.pending);
		}
		expect(coordinator.cleanupPeer("peer")).to.deep.equal([]);
	});

	it("keeps overlapping peer-removal fences until every owner releases", () => {
		const coordinator = new CheckedPruneCoordinator<Uint8Array, "u32">();
		const pending = pendingDelete(requestId(1));
		setPending(coordinator, "hash", pending);
		coordinator.addRequestSent("hash", "peer", pending);
		coordinator.addConfirmedReplicator(
			"hash",
			"peer",
			pending,
			pending.requestId,
		);

		const releaseFirst = coordinator.fencePeerRemoval("peer");
		const releaseSecond = coordinator.fencePeerRemoval("peer");
		expect(coordinator.isPeerRemovalFenced("peer")).true;
		expect(
			coordinator.getExactConfirmedReplicators("hash", pending).size,
		).equal(0);

		releaseFirst();
		releaseFirst();
		expect(coordinator.isPeerRemovalFenced("peer")).true;
		expect(
			coordinator.getExactConfirmedReplicators("hash", pending).size,
		).equal(0);

		releaseSecond();
		expect(coordinator.isPeerRemovalFenced("peer")).false;
		expect(
			coordinator.getExactConfirmedReplicators("hash", pending),
		).deep.equal(new Set(["peer"]));
	});

	it("waits for every grant send and releases rejected barriers", async () => {
		const coordinator = new CheckedPruneCoordinator<Uint8Array, "u32">();
		const first = pDefer<void>();
		const second = pDefer<void>();
		coordinator.trackGrantSend(["hash"], first.promise);
		coordinator.trackGrantSend(["hash"], second.promise);

		const waiting = coordinator.waitForGrantSends("hash");
		expect(waiting).to.exist;
		let settled = false;
		void waiting!.then(() => {
			settled = true;
		});

		first.resolve();
		await Promise.resolve();
		await Promise.resolve();
		expect(settled).to.be.false;

		second.reject(new Error("grant send aborted"));
		await waiting;
		await Promise.resolve();
		expect(settled).to.be.true;
		expect(coordinator.waitForGrantSends("hash")).to.be.undefined;
	});

	it("consumes only the current deferred restart reservation", () => {
		const coordinator = new CheckedPruneCoordinator<Uint8Array, "u32">();
		const first = coordinator.reserveRestart("hash");
		expect(coordinator.consumeRestartReservation("hash", first)).to.be.true;
		expect(coordinator.consumeRestartReservation("hash", first)).to.be.false;

		const supersededByCandidate = coordinator.reserveRestart("hash");
		const candidateToken = coordinator.trackCandidate(
			"hash",
			{ hash: "hash" } as any,
			new Set(["peer"]),
		);
		expect(coordinator.hasCandidate("hash")).to.be.true;
		expect(coordinator.isCandidateTokenCurrent("hash", candidateToken)).to.be
			.true;
		expect(coordinator.consumeRestartReservation("hash", supersededByCandidate))
			.to.be.false;
		coordinator.invalidateCandidateToken("hash");
		expect(coordinator.hasCandidate("hash")).to.be.false;
		expect(coordinator.isCandidateTokenCurrent("hash", candidateToken)).to.be
			.false;

		const supersededByGeneration = coordinator.reserveRestart("hash");
		const stalePending = pendingDelete(requestId(1));
		coordinator.setPendingDelete(
			"hash",
			stalePending,
			{ hash: "hash" } as any,
			new Set(["peer"]),
		);
		expect(
			coordinator.consumeRestartReservation("hash", supersededByGeneration),
		).to.be.false;

		const currentPending = pendingDelete(requestId(2));
		coordinator.setPendingDelete(
			"hash",
			currentPending,
			{ hash: "hash" } as any,
			new Set(["peer"]),
		);
		const preservedAcrossStaleCancel = coordinator.reserveRestart("hash");
		expect(coordinator.markCancelled("hash", stalePending)).to.be.false;
		expect(
			coordinator.consumeRestartReservation("hash", preservedAcrossStaleCancel),
		).to.be.true;

		const cancelled = coordinator.reserveRestart("hash");
		expect(coordinator.markCancelled("hash", currentPending)).to.be.true;
		expect(coordinator.consumeRestartReservation("hash", cancelled)).to.be
			.false;
	});

	it("clears every authorization generation on close", () => {
		let clears = 0;
		const coordinator = new CheckedPruneCoordinator<Uint8Array, "u32">();
		const pending = pendingDelete(requestId(1), () => {
			clears += 1;
		});
		setPending(coordinator, "hash", pending);
		coordinator.addRequestSent("hash", "peer", pending);
		coordinator.addConfirmedReplicator(
			"hash",
			"peer",
			pending,
			pending.requestId,
		);

		coordinator.close();
		expect(clears).equal(1);
		expect(coordinator.pendingDeletes.size).equal(0);
		expect(coordinator.requestIPruneSent.size).equal(0);
		expect(coordinator.responseReplicatorSet.size).equal(0);
	});
});
