// Stage-2 lifecycle refactor guard: PeerSession instances ARE the opaque
// subscription-epoch tokens, so this spec pins the identity semantics the
// host relies on (rotation supersedes, null is a valid current value, the
// open()-time map swap) and proves the registry's receive-admission
// predicate is truth-table equivalent to a literal transcription of
// SharedLog.isPeerReceiveAdmissionOpen over every input combination.
import { expect } from "chai";
import {
	type PeerReceiveAdmissionOptions,
	type PeerSessionDeps,
	PeerSessionRegistry,
} from "../src/peer-session.js";

type StubHost = {
	replicationLifecycleController?: AbortController;
	terminating: boolean;
	blockedPeers: Set<string>;
	cleanupGateByPeer: Map<string, number>;
};

const createHost = (): StubHost => ({
	replicationLifecycleController: new AbortController(),
	terminating: false,
	blockedPeers: new Set(),
	cleanupGateByPeer: new Map(),
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
	isReplicationInfoBlocked: (hash) => host.blockedPeers.has(hash),
	getReceiveCleanupGate: (hash) => host.cleanupGateByPeer.get(hash) ?? 0,
});

// Literal transcription of SharedLog.isPeerReceiveAdmissionOpen
// (src/index.ts), reading the same host state the registry deps read.
const legacyIsPeerReceiveAdmissionOpen = (
	host: StubHost,
	peerHash: string,
	replicationLifecycleController: AbortController | undefined,
	subscriptionEpoch: object | null,
	currentEpoch: object | null,
	options?: PeerReceiveAdmissionOptions,
) =>
	isReplicationLifecycleActive(host, replicationLifecycleController) &&
	currentEpoch === subscriptionEpoch &&
	(options?.allowReplicationInfoBlocked === true ||
		!host.blockedPeers.has(peerHash)) &&
	(options?.allowCleanupGate === true ||
		(host.cleanupGateByPeer.get(peerHash) ?? 0) === 0);

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
									host.blockedPeers.add(PEER);
								}
								host.cleanupGateByPeer.set(PEER, gate);
								const options: PeerReceiveAdmissionOptions = {
									allowReplicationInfoBlocked,
									allowCleanupGate,
								};

								const expected = legacyIsPeerReceiveAdmissionOpen(
									host,
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
		const mapBefore = registry._subscriptionEpochByPeer;
		registry.resetForOpen();
		expect(registry._subscriptionEpochByPeer).to.not.equal(mapBefore);
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
});
