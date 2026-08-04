// Lifecycle refactor guard: PeerSession instances ARE the opaque
// subscription-epoch tokens, so this spec pins the identity semantics the
// host relies on (rotation supersedes, null is a valid current value, the
// open()-time map swap) and proves the registry's receive-admission
// predicate is truth-table equivalent to an INLINE transcription of the
// legacy 4-term SharedLog.isPeerReceiveAdmissionOpen predicate over every
// input combination. The host facade is gone, so the legacy side here stays
// this independent inline replica (expected booleans computed in the test
// itself) rather than becoming a tautological registry comparison.
import { expect } from "chai";
import {
	type PeerReceiveAdmissionOptions,
	type PeerSessionDeps,
	PeerSessionRegistry,
} from "../src/peer-session.js";

type StubHost = {
	replicationLifecycleController?: AbortController;
	terminating: boolean;
};

const createHost = (): StubHost => ({
	replicationLifecycleController: new AbortController(),
	terminating: false,
});

// Mirrors SharedLog.isReplicationLifecycleActive term for term.
const isReplicationLifecycleActive = (
	host: StubHost,
	controller: AbortController | undefined,
) =>
	controller != null &&
	controller === host.replicationLifecycleController &&
	!controller.signal.aborted &&
	!host.terminating;

const createDeps = (host: StubHost): PeerSessionDeps => ({
	isReplicationLifecycleActive: (controller) =>
		isReplicationLifecycleActive(host, controller),
	getReplicationLifecycleController: () => host.replicationLifecycleController,
});

// Literal transcription of the legacy (pre-stage-3) body of
// SharedLog.isPeerReceiveAdmissionOpen, reading the same host state the
// registry deps read. This inline replica is the regression oracle after the
// host facade's removal.
const legacyIsPeerReceiveAdmissionOpen = (
	host: StubHost,
	blockedPeers: Set<string>,
	cleanupGateByPeer: Map<string, number>,
	peerHash: string,
	replicationLifecycleController: AbortController | undefined,
	subscriptionEpoch: object | null,
	currentEpoch: object | null,
	options?: PeerReceiveAdmissionOptions,
) =>
	isReplicationLifecycleActive(host, replicationLifecycleController) &&
	currentEpoch === subscriptionEpoch &&
	(options?.allowReplicationInfoBlocked === true ||
		!blockedPeers.has(peerHash)) &&
	(options?.allowCleanupGate === true ||
		(cleanupGateByPeer.get(peerHash) ?? 0) === 0);

const PEER = "peer-a";

describe("receive admission peer session parity", () => {
	it("rotate supersedes the previous session with a fresh identity", () => {
		const host = createHost();
		const registry = new PeerSessionRegistry(createDeps(host));

		const first = registry.rotate(PEER, "opening");
		expect(first.kind).to.equal("opening");
		expect(first.phase).to.equal("opening");
		expect(first.isCurrent()).to.be.true;
		expect(registry.current(PEER)).to.equal(first);

		const second = registry.rotate(PEER, "departing");
		expect(second).to.not.equal(first);
		expect(second.kind).to.equal("departing");
		expect(second.phase).to.equal("departing");
		expect(second.isCurrent()).to.be.true;
		expect(first.isCurrent()).to.be.false;
		expect(first.phase).to.equal("superseded");
		expect(registry.isCurrent(PEER, first)).to.be.false;
		expect(registry.isCurrent(PEER, second)).to.be.true;
	});

	it("rotate captures the replication lifecycle controller live at rotation", () => {
		const host = createHost();
		const registry = new PeerSessionRegistry(createDeps(host));

		const first = registry.rotate(PEER, "opening");
		expect(first.replicationLifecycleController).to.equal(
			host.replicationLifecycleController,
		);
		expect(first.isActive()).to.be.true;

		const previousController = host.replicationLifecycleController;
		host.replicationLifecycleController = new AbortController();
		// Still current, but the captured controller lost the lifecycle: the
		// A5+B1 pair fails exactly like today's paired checks.
		expect(first.isCurrent()).to.be.true;
		expect(first.isActive()).to.be.false;
		expect(previousController).to.not.equal(
			host.replicationLifecycleController,
		);

		const second = registry.rotate(PEER, "opening");
		expect(second.replicationLifecycleController).to.equal(
			host.replicationLifecycleController,
		);
		expect(second.isActive()).to.be.true;
		expect(first.isActive()).to.be.false;
	});

	it("treats null as the valid current value for a pre-session peer", () => {
		const host = createHost();
		const registry = new PeerSessionRegistry(createDeps(host));

		expect(registry.current(PEER)).to.equal(null);
		expect(registry.isCurrent(PEER, null)).to.be.true;
		expect(
			registry.isReceiveAdmissionOpen(
				PEER,
				null,
				host.replicationLifecycleController,
			),
		).to.be.true;

		registry.rotate(PEER, "opening");
		expect(registry.isCurrent(PEER, null)).to.be.false;
		expect(
			registry.isReceiveAdmissionOpen(
				PEER,
				null,
				host.replicationLifecycleController,
			),
		).to.be.false;
	});

	it("matches the legacy receive-admission predicate over the full truth table", () => {
		for (const lifecycleState of [
			"active",
			"aborted",
			"replaced",
			"terminating",
			"undefined-controller",
		] as const) {
			for (const sessionState of ["current", "superseded", "null"] as const) {
				for (const blocked of [false, true]) {
					for (const gate of [0, 1]) {
						for (const allowReplicationInfoBlocked of [
							undefined,
							true,
						] as const) {
							for (const allowCleanupGate of [undefined, true] as const) {
								const host = createHost();
								const registry = new PeerSessionRegistry(createDeps(host));

								let session: ReturnType<typeof registry.rotate> | null = null;
								if (sessionState === "current") {
									session = registry.rotate(PEER, "opening");
								} else if (sessionState === "superseded") {
									session = registry.rotate(PEER, "opening");
									registry.rotate(PEER, "opening");
								}
								const controller =
									lifecycleState === "undefined-controller"
										? undefined
										: host.replicationLifecycleController;
								if (lifecycleState === "aborted") {
									host.replicationLifecycleController?.abort();
								} else if (lifecycleState === "replaced") {
									host.replicationLifecycleController = new AbortController();
								} else if (lifecycleState === "terminating") {
									host.terminating = true;
								}
								if (blocked) {
									// The blocked set moved into the registry (fence B5);
									// seed it through the registry's block method, mirroring
									// the legacy host-set add.
									registry.blockReplicationInfo(PEER);
								}
								// The gate refcounts moved into the registry (fence B6);
								// seed its map directly, mirroring the legacy host-map
								// write.
								registry._receiveCleanupGateByPeer.set(PEER, gate);
								const options: PeerReceiveAdmissionOptions = {
									allowReplicationInfoBlocked,
									allowCleanupGate,
								};

								// The oracle must not read the registry state it just
								// seeded (a silent no-op in block/set would make oracle
								// and implementation agree wrongly): feed it independent
								// structures built from the loop variables.
								const expected = legacyIsPeerReceiveAdmissionOpen(
									host,
									blocked ? new Set([PEER]) : new Set(),
									new Map(gate === 0 ? [] : [[PEER, gate]]),
									PEER,
									controller,
									session,
									registry.current(PEER),
									options,
								);
								const label = `lifecycle=${lifecycleState} session=${sessionState} blocked=${blocked} gate=${gate} allowBlocked=${allowReplicationInfoBlocked} allowGate=${allowCleanupGate}`;
								expect(
									registry.isReceiveAdmissionOpen(
										PEER,
										session,
										controller,
										options,
									),
									label,
								).to.equal(expected);
								if (sessionState !== "null") {
									// The session-level convenience wrapper reads through the
									// captured controller; parity only when that capture is the
									// controller under test.
									if (controller === session!.replicationLifecycleController) {
										expect(
											session!.isReceiveAdmissionOpen(options),
											label,
										).to.equal(expected);
									}
								}
							}
						}
					}
				}
			}
		}
	});

	it("resetForOpen replaces the map instance and demotes pre-reset sessions", () => {
		const host = createHost();
		const registry = new PeerSessionRegistry(createDeps(host));

		const before = registry.rotate(PEER, "opening");
		const mapBefore = registry.sessions;
		registry.resetForOpen();
		expect(registry.sessions).to.not.equal(mapBefore);
		expect(registry.current(PEER)).to.equal(null);
		expect(before.isCurrent()).to.be.false;
		// Late continuations still resolve their captured token against the
		// fresh map, mirroring today's open()-time map replacement.
		expect(registry.isCurrent(PEER, before)).to.be.false;
		expect(registry.isCurrent(PEER, null)).to.be.true;
	});

	it("markOpen is identity-guarded and only promotes opening sessions", () => {
		const host = createHost();
		const registry = new PeerSessionRegistry(createDeps(host));

		const departing = registry.rotate(PEER, "departing");
		registry.markOpen(PEER, departing);
		expect(departing.phase).to.equal("departing");

		const opening = registry.rotate(PEER, "opening");
		registry.markOpen(PEER, {});
		expect(opening.phase).to.equal("opening");
		registry.markOpen(PEER, opening);
		expect(opening.phase).to.equal("open");

		const next = registry.rotate(PEER, "opening");
		// Superseded token: no-op, and the superseded phase is terminal.
		registry.markOpen(PEER, opening);
		expect(opening.phase).to.equal("superseded");
		expect(next.phase).to.equal("opening");
	});

	it("noteReplicatorRemoved marks only the current session", () => {
		const host = createHost();
		const registry = new PeerSessionRegistry(createDeps(host));

		// No session: no-op, no throw.
		registry.noteReplicatorRemoved(PEER);

		const first = registry.rotate(PEER, "opening");
		const second = registry.rotate(PEER, "departing");
		registry.noteReplicatorRemoved(PEER);
		expect(first.replicatorRemoved).to.be.false;
		expect(second.replicatorRemoved).to.be.true;
	});

	it("advances the receive recovery epoch per peer, independent of sessions", () => {
		const host = createHost();
		const registry = new PeerSessionRegistry(createDeps(host));

		// Legacy map semantics: no advance yet -> current epoch is null, and a
		// null capture is current — including for a peer with no session.
		expect(registry.receiveEpoch(PEER)).to.equal(null);
		expect(registry.isReceiveEpochCurrent(PEER, null)).to.be.true;

		// Advancing works for a session-less peer (removeReplicator can fence a
		// peer that never subscribed) and fences the null capture.
		const first = registry.advanceReceiveEpoch(PEER);
		expect(registry.isReceiveEpochCurrent(PEER, first)).to.be.true;
		expect(registry.isReceiveEpochCurrent(PEER, null)).to.be.false;

		// Session rotation does NOT rotate the receive epoch (the fence's whole
		// point: removeReplicator advances it while the peer stays subscribed)…
		registry.rotate(PEER, "opening");
		expect(registry.isReceiveEpochCurrent(PEER, first)).to.be.true;

		// …and the advance does not rotate the session.
		const session = registry.current(PEER);
		const second = registry.advanceReceiveEpoch(PEER);
		expect(registry.isReceiveEpochCurrent(PEER, first)).to.be.false;
		expect(registry.isReceiveEpochCurrent(PEER, second)).to.be.true;
		expect(registry.current(PEER)).to.equal(session);

		// _close clears receive epochs (captures compare against null) while
		// sessions deliberately survive; open() replaces the map instance.
		registry.clearReceiveEpochsForClose();
		expect(registry.isReceiveEpochCurrent(PEER, second)).to.be.false;
		expect(registry.isReceiveEpochCurrent(PEER, null)).to.be.true;
		expect(registry.current(PEER)).to.equal(session);
		const third = registry.advanceReceiveEpoch(PEER);
		registry.resetForOpen();
		expect(registry.isReceiveEpochCurrent(PEER, third)).to.be.false;
		expect(registry.isReceiveEpochCurrent(PEER, null)).to.be.true;
	});

	it("keeps receive epochs and cleanup gates isolated per peer", () => {
		const host = createHost();
		const registry = new PeerSessionRegistry(createDeps(host));
		const otherPeer = "peer-b";

		const peerAEpoch = registry.advanceReceiveEpoch(PEER);
		expect(registry.isReceiveEpochCurrent(PEER, peerAEpoch)).to.be.true;
		expect(registry.receiveEpoch(otherPeer)).to.equal(null);
		expect(registry.isReceiveEpochCurrent(otherPeer, null)).to.be.true;

		const peerBEpoch = registry.advanceReceiveEpoch(otherPeer);
		expect(registry.isReceiveEpochCurrent(PEER, peerAEpoch)).to.be.true;
		expect(registry.isReceiveEpochCurrent(otherPeer, peerBEpoch)).to.be.true;

		const releasePeerA = registry.acquireReceiveCleanupGate(PEER);
		expect(registry.isReceiveCleanupGateOpen(PEER)).to.be.false;
		expect(registry.isReceiveCleanupGateOpen(otherPeer)).to.be.true;
		releasePeerA();
		expect(registry.isReceiveCleanupGateOpen(PEER)).to.be.true;
	});

	it("refcounts the receive cleanup gate with idempotent releases", () => {
		const host = createHost();
		const registry = new PeerSessionRegistry(createDeps(host));

		expect(registry.isReceiveCleanupGateOpen(PEER)).to.be.true;
		const releaseFirst = registry.acquireReceiveCleanupGate(PEER);
		const releaseSecond = registry.acquireReceiveCleanupGate(PEER);
		expect(registry._receiveCleanupGateByPeer.get(PEER)).to.equal(2);
		expect(registry.isReceiveCleanupGateOpen(PEER)).to.be.false;

		releaseFirst();
		// Idempotent: a double release must not decrement twice.
		releaseFirst();
		expect(registry._receiveCleanupGateByPeer.get(PEER)).to.equal(1);
		expect(registry.isReceiveCleanupGateOpen(PEER)).to.be.false;

		releaseSecond();
		// Entry deleted at zero, matching the legacy decrement-or-delete.
		expect(registry._receiveCleanupGateByPeer.has(PEER)).to.be.false;
		expect(registry.isReceiveCleanupGateOpen(PEER)).to.be.true;
	});

	it("binds a cleanup-gate release to the map instance it incremented", () => {
		const host = createHost();
		const registry = new PeerSessionRegistry(createDeps(host));

		// Reopen during an in-flight removeReplicator: the release captured
		// before resetForOpen must drain the OLD map, never the fresh open's.
		const release = registry.acquireReceiveCleanupGate(PEER);
		const mapBefore = registry._receiveCleanupGateByPeer;
		registry.resetForOpen();
		expect(registry._receiveCleanupGateByPeer).to.not.equal(mapBefore);
		expect(registry.isReceiveCleanupGateOpen(PEER)).to.be.true;

		const freshRelease = registry.acquireReceiveCleanupGate(PEER);
		release();
		// The late release drained the stale map without corrupting the fresh
		// open's refcount.
		expect(mapBefore.has(PEER)).to.be.false;
		expect(registry._receiveCleanupGateByPeer.get(PEER)).to.equal(1);
		freshRelease();
		expect(registry.isReceiveCleanupGateOpen(PEER)).to.be.true;

		// _close clears in place; a release straddling close decrements the
		// cleared entry to zero (legacy `?? 1` shape) and stays deleted.
		const releaseAcrossClose = registry.acquireReceiveCleanupGate(PEER);
		registry.clearCleanupGatesForClose();
		expect(registry._receiveCleanupGateByPeer.has(PEER)).to.be.false;
		releaseAcrossClose();
		expect(registry._receiveCleanupGateByPeer.has(PEER)).to.be.false;
	});
});
