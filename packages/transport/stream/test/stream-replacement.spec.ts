import type { Connection, Stream } from "@libp2p/interface";
import { noise } from "@libp2p/noise";
import { tcp } from "@libp2p/tcp";
import { yamux } from "@libp2p/yamux";
import { Ed25519Keypair, type PublicSignKey } from "@peerbit/crypto";
import {
	DataMessage,
	Goodbye,
	MessageHeader,
	SilentDelivery,
} from "@peerbit/stream-interface";
import { AbortError, delay, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { createLibp2p } from "libp2p";
import pDefer from "p-defer";
import sinon from "sinon";
import { Uint8ArrayList } from "uint8arraylist";
import {
	DirectStream,
	type DirectStreamComponents,
	type InboundStreamRecord,
	PeerStreams,
} from "../src/index.js";

const protocol = "/replacement-test/1.0.0";
class ReplacementStream extends DirectStream {
	constructor(components: DirectStreamComponents) {
		super(components, [protocol], { connectionManager: false });
	}
}
const createNode = () =>
	createLibp2p<{ directstream: ReplacementStream }>({
		transports: [tcp()],
		streamMuxers: [yamux()],
		connectionEncrypters: [noise()],
		connectionMonitor: { enabled: false },
		connectionManager: { reconnectRetries: 0 },
		services: {
			directstream: (components) => new ReplacementStream(components),
		},
	});
const raw = (id: string) =>
	Object.assign(new EventTarget(), {
		id,
		protocol,
		send: () => true,
		abort: sinon.spy(),
		close: sinon.stub().resolves(),
	}) as unknown as Stream;

const pauseClose = async (peer: PeerStreams) => {
	await peer.attachOutboundStream(raw("old-outbound"));
	const queue = peer._getActiveOutboundPushable()!;
	const original = queue.return!.bind(queue);
	const gate = pDefer<void>();
	queue.return = async () => {
		await gate.promise;
		return original();
	};
	return gate.resolve;
};
const tick = async () => {
	for (let i = 0; i < 12; i++) await Promise.resolve();
};

describe("stream same-identity replacement ownership", () => {
	it("retains failed retirement and drains other closes before reporting failure", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		const first = (await Ed25519Keypair.create()).publicKey;
		const failed = subject.addPeer(first.toPeerId(), first, protocol, "failed");
		await failed.close();
		await tick();
		// The failed test double has already released its real resources.
		subject.peers.set(first.hashcode(), failed);
		const failure = new Error("retired teardown failed");
		const attemptedByBarrier = pDefer<void>();
		let attempts = 0;
		const failClose = sinon.stub(failed, "close").callsFake(() => {
			if (++attempts === 2) attemptedByBarrier.resolve();
			return Promise.reject(failure);
		});
		subject.addPeer(first.toPeerId(), first, protocol, "first-replacement");
		const second = (await Ed25519Keypair.create()).publicKey;
		const pending = subject.addPeer(
			second.toPeerId(),
			second,
			protocol,
			"pending",
		);
		const release = await pauseClose(pending);
		const closing = pending.close();
		subject.addPeer(second.toPeerId(), second, protocol, "second-replacement");
		let settled = false;
		let result: unknown;
		const barrier = subject.beforeStop().then(
			() => {
				settled = true;
			},
			(error) => {
				settled = true;
				result = error;
			},
		);
		try {
			await attemptedByBarrier.promise;
			await tick();
			expect((subject as any).retiredPeerStreams.has(failed)).to.equal(true);
			expect(failClose.callCount).to.equal(2);
			expect(settled).to.equal(false);
			release();
			await barrier;
			expect(result).to.be.instanceOf(AggregateError);
			expect((result as AggregateError).errors).to.deep.equal([failure]);
		} finally {
			release();
			await closing;
			await barrier;
			failClose.restore();
			await Promise.resolve(node.stop()).catch(() => {});
		}
	});

	it("preserves negotiated session state and ignores retired readiness events", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		const key = (await Ed25519Keypair.create()).publicKey;
		const old = subject.addPeer(key.toPeerId(), key, protocol, "old");
		subject.routes.updateSession(key.hashcode(), undefined);
		subject.routes.updateSession(key.hashcode(), 123);
		const release = await pauseClose(old);
		const closing = old.close();
		const session = sinon.spy(subject, "onPeerSession");
		const outbound = sinon.spy();
		const inbound = sinon.spy();
		const writable = sinon.spy();
		(subject as EventTarget).addEventListener("stream:outbound", outbound);
		(subject as EventTarget).addEventListener("stream:inbound", inbound);
		subject.addEventListener("peer:stream-ready", writable);
		try {
			const current = subject.addPeer(key.toPeerId(), key, protocol, "new");
			expect(subject.routes.getSession(key.hashcode())).to.equal(123);
			expect(session.called).to.equal(false);
			old.dispatchEvent(new CustomEvent("stream:outbound"));
			old.dispatchEvent(new CustomEvent("stream:inbound"));
			expect(outbound.called || inbound.called).to.equal(false);
			current.dispatchEvent(new CustomEvent("stream:outbound"));
			expect(outbound.calledOnce).to.equal(true);
			expect(writable.called).to.equal(false);
			await current.attachOutboundStream(raw("replacement"));
			expect(writable.calledOnce).to.equal(true);
			expect(writable.firstCall.args[0].detail).to.equal(key);
			current.dispatchEvent(new CustomEvent("stream:outbound"));
			old.dispatchEvent(new CustomEvent("stream:outbound"));
			expect(writable.calledOnce).to.equal(true);
		} finally {
			session.restore();
			release();
			await closing;
			await node.stop();
		}
	});

	it("announces first writable readiness again after complete peer removal", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		const key = (await Ed25519Keypair.create()).publicKey;
		const writable = sinon.spy();
		subject.addEventListener("peer:stream-ready", writable);
		try {
			const old = subject.addPeer(key.toPeerId(), key, protocol, "first");
			expect(writable.called).to.equal(false);
			await old.attachOutboundStream(raw("first"));
			expect(writable.calledOnce).to.equal(true);
			await subject["onPeerDisconnected"](key.toPeerId());
			expect(subject.peers.has(key.hashcode())).to.equal(false);
			const current = subject.addPeer(key.toPeerId(), key, protocol, "next");
			await current.attachOutboundStream(raw("next"));
			expect(writable.calledTwice).to.equal(true);
			expect(writable.secondCall.args[0].detail).to.equal(key);
			current.dispatchEvent(new CustomEvent("stream:outbound"));
			expect(writable.calledTwice).to.equal(true);
		} finally {
			await node.stop();
		}
	});

	it("removes routes when a disconnected peer has no outbound streams", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		const key = (await Ed25519Keypair.create()).publicKey;
		subject.addPeer(key.toPeerId(), key, protocol, "closed");
		const unreachable = sinon.spy(subject, "onPeerUnreachable");
		try {
			expect(
				subject.routes.isReachable(subject.publicKeyHash, key.hashcode()),
			).to.equal(true);
			await subject["onPeerDisconnected"](key.toPeerId());
			expect(subject.peers.has(key.hashcode())).to.equal(false);
			expect(
				subject.routes.isReachable(subject.publicKeyHash, key.hashcode()),
			).to.equal(false);
			expect(unreachable.calledOnceWithExactly(key.hashcode())).to.equal(true);
		} finally {
			unreachable.restore();
			await node.stop();
		}
	});

	it("keeps a current relay route when only the direct connection retires", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		const target = (await Ed25519Keypair.create()).publicKey;
		const relay = (await Ed25519Keypair.create()).publicKey;
		const dependent = (await Ed25519Keypair.create()).publicKey;
		const targetHash = target.hashcode();
		const relayHash = relay.hashcode();
		const direct = subject.addPeer(
			target.toPeerId(),
			target,
			protocol,
			"direct",
		);
		const viaRelay = subject.addPeer(
			relay.toPeerId(),
			relay,
			protocol,
			"relay",
		);
		const downstream = subject.addPeer(
			dependent.toPeerId(),
			dependent,
			protocol,
			"dependent",
		);
		const publications = sinon.spy(subject, "publishMessageMaybe");
		const goodbyes = () =>
			publications.getCalls().filter((call) => call.args[1] instanceof Goodbye);
		const lost: string[] = [];
		const onLost = ({ detail }: CustomEvent<PublicSignKey>) => {
			lost.push(detail.hashcode());
		};
		subject.addEventListener("peer:unreachable", onLost);
		try {
			await direct.attachOutboundStream(raw("direct-outbound"));
			await viaRelay.attachOutboundStream(raw("relay-outbound"));
			await downstream.attachOutboundStream(raw("dependent-outbound"));
			const routeSession = Date.now();
			subject.updateSession(target, 123);
			subject.updateSession(relay, 456);
			// Admit two current paths through the same public seam used by ACKs.
			// Retiring one neighbour must not declare the remote identity departed.
			subject.addRouteConnection(
				subject.publicKeyHash,
				relayHash,
				relay,
				-1,
				routeSession,
				456,
			);
			for (const nextHop of [targetHash, relayHash]) {
				subject.addRouteConnection(
					subject.publicKeyHash,
					nextHop,
					target,
					1,
					routeSession,
					123,
				);
				// The downstream peer previously reached the target through us.
				subject.addRouteConnection(
					dependent.hashcode(),
					nextHop,
					target,
					1,
					routeSession,
					123,
				);
			}
			expect(subject.routes.getDependent(targetHash)).to.include(
				dependent.hashcode(),
			);
			expect(
				subject.getRouteHints(targetHash).map((hint) => hint.nextHop),
			).to.have.members([targetHash, relayHash]);
			expect(
				subject.routes.isReachable(subject.publicKeyHash, targetHash),
			).to.equal(true);
			expect(
				subject.routes.isReachable(subject.publicKeyHash, relayHash),
			).to.equal(true);
			// The direct-neighbour sentinel must become the surviving route's
			// admitted remote session when the target is only reachable by relay.
			expect(subject.routes.getSession(targetHash)).to.equal(-1);

			await subject["onPeerDisconnected"](target.toPeerId(), {
				id: "direct",
				remotePeer: target.toPeerId(),
				status: "closed",
			} as Connection);
			expect(direct.isClosed).to.equal(true);
			expect(subject.peers.has(targetHash)).to.equal(false);
			expect(subject.peers.get(relayHash)).to.equal(viaRelay);
			expect(viaRelay.isWritable).to.equal(true);
			expect(subject.routes.getSession(targetHash)).to.equal(123);
			expect(
				subject.getRouteHints(targetHash).map((hint) => hint.nextHop),
			).to.deep.equal([relayHash]);
			expect(
				subject.routes.isReachable(subject.publicKeyHash, targetHash),
			).to.equal(true);
			expect(lost).to.deep.equal([]);
			expect(goodbyes()).to.have.length(0);
			expect(subject.routes.getDependent(relayHash)).to.include(
				dependent.hashcode(),
			);

			await subject["onPeerDisconnected"](relay.toPeerId(), {
				id: "relay",
				remotePeer: relay.toPeerId(),
				status: "closed",
			} as Connection);
			expect(
				subject.routes.isReachable(subject.publicKeyHash, targetHash),
			).to.equal(false);
			expect(subject.getRouteHints(targetHash)).to.have.length(0);
			expect(lost.filter((hash) => hash === targetHash)).to.deep.equal([
				targetHash,
			]);
			expect(goodbyes()).to.have.length(1);
			const goodbye = goodbyes()[0]!.args[1] as Goodbye;
			expect(goodbye.leaving).to.deep.equal([relayHash]);
			expect(goodbye.header.mode.to).to.include(dependent.hashcode());
			await subject["onPeerDisconnected"](target.toPeerId());
			await subject["onPeerDisconnected"](relay.toPeerId());
			expect(lost.filter((hash) => hash === targetHash)).to.deep.equal([
				targetHash,
			]);
		} finally {
			publications.restore();
			subject.removeEventListener("peer:unreachable", onLost);
			await node.stop();
		}
	});

	it("accepts a newer signed session after direct-to-relay fallback without rollback", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		const targetKey = await Ed25519Keypair.create();
		const target = targetKey.publicKey;
		const targetHash = target.hashcode();
		const relay = (await Ed25519Keypair.create()).publicKey;
		const direct = subject.addPeer(
			target.toPeerId(),
			target,
			protocol,
			"direct",
		);
		const viaRelay = subject.addPeer(
			relay.toPeerId(),
			relay,
			protocol,
			"relay",
		);
		const sessionChanged = sinon.spy(subject, "onPeerSession");
		try {
			await direct.attachOutboundStream(raw("direct-outbound"));
			await viaRelay.attachOutboundStream(raw("relay-outbound"));
			const routeSession = Date.now();
			// Route admission uses the ACK callback's public seam. This is a signed
			// session-processing regression, not an actual wire restart fixture.
			for (const nextHop of [targetHash, relay.hashcode()]) {
				subject.addRouteConnection(
					subject.publicKeyHash,
					nextHop,
					target,
					1,
					routeSession,
					123,
				);
			}
			expect(subject.routes.getSession(targetHash)).to.equal(-1);
			await subject["onPeerDisconnected"](target.toPeerId(), {
				id: "direct",
				remotePeer: target.toPeerId(),
				status: "closed",
			} as Connection);
			expect(subject.peers.has(targetHash)).to.equal(false);
			expect(viaRelay.isWritable).to.equal(true);
			expect(
				subject.getRouteHints(targetHash).map((hint) => hint.nextHop),
			).to.deep.equal([relay.hashcode()]);
			expect(sessionChanged.called).to.equal(false);
			let currentSession = 123;
			for (const session of [124, 123, 124, 122]) {
				const message = await new DataMessage({
					header: new MessageHeader({
						session,
						mode: new SilentDelivery({
							to: [subject.publicKeyHash],
							redundancy: 1,
						}),
					}),
					data: new Uint8Array([session]),
				}).sign((bytes) => targetKey.sign(bytes));
				expect(await subject.verifyAndProcess(message)).to.equal(true);
				currentSession = Math.max(currentSession, session);
				expect(subject.routes.getSession(targetHash)).to.equal(currentSession);
				expect(sessionChanged.callCount).to.equal(1);
			}
			expect(sessionChanged.calledOnceWithExactly(target, 124)).to.equal(true);
		} finally {
			sessionChanged.restore();
			await node.stop();
		}
	});

	it("does not publish a stale goodbye if a relay route arrives during signing", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		const target = (await Ed25519Keypair.create()).publicKey;
		const relay = (await Ed25519Keypair.create()).publicKey;
		const dependent = (await Ed25519Keypair.create()).publicKey;
		const direct = subject.addPeer(
			target.toPeerId(),
			target,
			protocol,
			"direct",
		);
		const viaRelay = subject.addPeer(
			relay.toPeerId(),
			relay,
			protocol,
			"relay",
		);
		const downstream = subject.addPeer(
			dependent.toPeerId(),
			dependent,
			protocol,
			"dependent",
		);
		const signing = pDefer<void>();
		const release = pDefer<void>();
		const sign = subject.sign.bind(subject);
		const publications = sinon.spy(subject, "publishMessageMaybe");
		let signingStub: sinon.SinonStub | undefined;
		let disconnect: Promise<void> | undefined;
		try {
			await direct.attachOutboundStream(raw("direct-outbound"));
			await viaRelay.attachOutboundStream(raw("relay-outbound"));
			await downstream.attachOutboundStream(raw("dependent-outbound"));
			const routeSession = Date.now();
			subject.updateSession(target, 123);
			subject.addRouteConnection(
				subject.publicKeyHash,
				target.hashcode(),
				target,
				-1,
				routeSession,
				123,
			);
			subject.addRouteConnection(
				dependent.hashcode(),
				target.hashcode(),
				target,
				1,
				routeSession,
				123,
			);
			expect(subject.routes.getDependent(target.hashcode())).to.include(
				dependent.hashcode(),
			);
			signingStub = sinon.stub(subject, "sign").callsFake(async (bytes) => {
				const signature = await sign(bytes);
				signing.resolve();
				await release.promise;
				return signature;
			});
			disconnect = subject["onPeerDisconnected"](target.toPeerId(), {
				id: "direct",
				remotePeer: target.toPeerId(),
				status: "closed",
			} as Connection);
			await signing.promise;
			expect(subject.peers.has(target.hashcode())).to.equal(false);
			expect(
				subject.routes.isReachable(subject.publicKeyHash, target.hashcode()),
			).to.equal(false);
			// Complete a genuine route-admission transition while the old loss
			// notification is awaiting its signature. No new direct peer is installed.
			subject.updateSession(target, 123);
			subject.addRouteConnection(
				subject.publicKeyHash,
				relay.hashcode(),
				target,
				1,
				routeSession,
				123,
			);
			expect(
				subject.routes.isReachable(subject.publicKeyHash, target.hashcode()),
			).to.equal(true);
			release.resolve();
			await disconnect;
			expect(
				publications
					.getCalls()
					.filter((call) => call.args[1] instanceof Goodbye),
			).to.have.length(0);
			expect(
				subject.getRouteHints(target.hashcode()).map((hint) => hint.nextHop),
			).to.deep.equal([relay.hashcode()]);
		} finally {
			release.resolve();
			try {
				await disconnect;
			} finally {
				signingStub?.restore();
				publications.restore();
				await node.stop();
			}
		}
	});

	for (const probe of [
		"answers",
		"fails",
		"fails before its health check",
		"fails behind a fresh route",
		"fails after a restart",
	]) {
		it(`sends a withheld goodbye only after the retained path fails its probe (${probe})`, async () => {
			const node = await createNode();
			const subject = node.services.directstream;
			const target = (await Ed25519Keypair.create()).publicKey;
			const relay = (await Ed25519Keypair.create()).publicKey;
			const targetHash = target.hashcode();
			const relayHash = relay.hashcode();
			const direct = subject.addPeer(
				target.toPeerId(),
				target,
				protocol,
				"direct",
			);
			const viaRelay = subject.addPeer(
				relay.toPeerId(),
				relay,
				protocol,
				"relay",
			);
			const settled = pDefer<boolean>();
			const checkIsAlive = sinon
				.stub(subject as any, "checkIsAlive")
				.returns(settled.promise);
			const publications = sinon.spy(subject, "publishMessageMaybe");
			const goodbyes = () =>
				publications
					.getCalls()
					.map((call) => call.args[1])
					.filter((message) => message instanceof Goodbye);
			const lost: string[] = [];
			const onLost = ({ detail }: CustomEvent<PublicSignKey>) => {
				lost.push(detail.hashcode());
			};
			subject.addEventListener("peer:unreachable", onLost);
			try {
				await direct.attachOutboundStream(raw("direct-outbound"));
				await viaRelay.attachOutboundStream(raw("relay-outbound"));
				const routeSession = Date.now();
				subject.updateSession(target, 123);
				subject.updateSession(relay, 456);
				subject.addRouteConnection(
					subject.publicKeyHash,
					relayHash,
					relay,
					-1,
					routeSession,
					456,
				);
				for (const nextHop of [targetHash, relayHash]) {
					subject.addRouteConnection(
						subject.publicKeyHash,
						nextHop,
						target,
						1,
						routeSession,
						123,
					);
				}
				// The relay reaches the target through us. Once the direct edge closes,
				// the path retained through the relay may be that route back through us.
				subject.addRouteConnection(
					relayHash,
					targetHash,
					target,
					1,
					routeSession,
					123,
				);
				expect(subject.routes.getDependent(targetHash)).to.deep.equal([
					relayHash,
				]);

				await subject["onPeerDisconnected"](target.toPeerId(), {
					id: "direct",
					remotePeer: target.toPeerId(),
					status: "closed",
				} as Connection);
				const disconnectedBy = Date.now();
				expect(checkIsAlive.calledOnceWithExactly([targetHash])).to.equal(true);
				expect(
					subject.getRouteHints(targetHash).map((hint) => hint.nextHop),
				).to.deep.equal([relayHash]);
				expect(goodbyes()).to.have.length(0);
				expect(lost).to.deep.equal([]);

				if (probe === "fails") {
					// The probe's ACK health check retires the identity before it settles.
					subject.removePeerFromRoutes(targetHash);
				} else if (probe === "fails behind a fresh route") {
					// An ACK through the relay after the probe was sent confirmed the path.
					await waitForResolved(
						() => expect(Date.now()).to.be.greaterThan(disconnectedBy),
						{ timeout: 1_000, delayInterval: 1 },
					);
					subject.addRouteConnection(
						subject.publicKeyHash,
						relayHash,
						target,
						1,
						Date.now(),
						123,
					);
				} else if (probe === "fails after a restart") {
					// A probe from the previous lifecycle must not speak for this one.
					await subject.stop();
					await subject.start();
				}
				// "fails before its health check": nothing retired the path before the
				// probe settled, as when one of its flood writes rejected early.
				settled.resolve(probe === "answers");
				if (probe === "fails" || probe === "fails before its health check") {
					await waitForResolved(() => expect(goodbyes()).to.have.length(1));
					const goodbye = goodbyes()[0] as Goodbye;
					expect(goodbye.leaving).to.deep.equal([targetHash]);
					expect(goodbye.header.mode.to).to.deep.equal([relayHash]);
					expect(
						subject.routes.isReachable(subject.publicKeyHash, targetHash),
					).to.equal(false);
					expect(subject.getRouteHints(targetHash)).to.have.length(0);
					expect(lost).to.deep.equal([targetHash]);
				} else {
					await delay(200);
					expect(goodbyes()).to.have.length(0);
					if (probe !== "fails after a restart") {
						expect(
							subject.routes.isReachable(subject.publicKeyHash, targetHash),
						).to.equal(true);
						expect(
							subject.getRouteHints(targetHash).map((hint) => hint.nextHop),
						).to.deep.equal([relayHash]);
						expect(lost).to.deep.equal([]);
					}
				}
			} finally {
				settled.resolve(false);
				checkIsAlive.restore();
				publications.restore();
				subject.removeEventListener("peer:unreachable", onLost);
				await node.stop();
			}
		});
	}

	for (const dependents of ["without", "with"]) {
		it(`retires a retained relay path whose real probe fails before its health check (${dependents} dependents)`, async () => {
			const node = await createNode();
			const subject = node.services.directstream;
			const target = (await Ed25519Keypair.create()).publicKey;
			const relay = (await Ed25519Keypair.create()).publicKey;
			const closing = (await Ed25519Keypair.create()).publicKey;
			const dependent = (await Ed25519Keypair.create()).publicKey;
			const targetHash = target.hashcode();
			const relayHash = relay.hashcode();
			const neighbours = [
				subject.addPeer(target.toPeerId(), target, protocol, "direct"),
				subject.addPeer(relay.toPeerId(), relay, protocol, "relay"),
				subject.addPeer(closing.toPeerId(), closing, protocol, "closing"),
				...(dependents === "with"
					? [
							subject.addPeer(
								dependent.toPeerId(),
								dependent,
								protocol,
								"dependent",
							),
						]
					: []),
			];
			// A neighbour that is being closed stays in `peers` until its close
			// resolves, and its writes reject. One rejected flood write fails the
			// whole probe and releases the probe's ACK health check.
			const closingWrites = sinon
				.stub(neighbours[2]!, "waitForWrite")
				.rejects(new AbortError("Closed"));
			const probes = sinon.spy(subject as any, "checkIsAlive");
			const publications = sinon.spy(subject, "publishMessageMaybe");
			const goodbyes = () =>
				publications
					.getCalls()
					.map((call) => call.args[1])
					.filter((message) => message instanceof Goodbye);
			const lost: string[] = [];
			const onLost = ({ detail }: CustomEvent<PublicSignKey>) => {
				lost.push(detail.hashcode());
			};
			subject.addEventListener("peer:unreachable", onLost);
			try {
				for (const [index, peer] of neighbours.entries()) {
					await peer.attachOutboundStream(raw(`outbound-${index}`));
				}
				const routeSession = Date.now();
				subject.updateSession(relay, 456);
				subject.addRouteConnection(
					subject.publicKeyHash,
					relayHash,
					relay,
					-1,
					routeSession,
					456,
				);
				for (const nextHop of [targetHash, relayHash]) {
					subject.addRouteConnection(
						subject.publicKeyHash,
						nextHop,
						target,
						1,
						routeSession,
						123,
					);
				}
				if (dependents === "with") {
					subject.addRouteConnection(
						dependent.hashcode(),
						targetHash,
						target,
						1,
						routeSession,
						123,
					);
				}

				const disconnectedAt = Date.now();
				await subject["onPeerDisconnected"](target.toPeerId(), {
					id: "direct",
					remotePeer: target.toPeerId(),
					status: "closed",
				} as Connection);
				expect(probes.calledOnceWithExactly([targetHash])).to.equal(true);
				expect(await probes.firstCall.returnValue).to.equal(false);
				// Retired after the failed probe, well before its health check
				// (seekTimeout) could have fired.
				await waitForResolved(
					() => {
						expect(
							subject.routes.isReachable(subject.publicKeyHash, targetHash),
						).to.equal(false);
						expect(lost).to.deep.equal([targetHash]);
						expect(goodbyes()).to.have.length(dependents === "with" ? 1 : 0);
					},
					{ timeout: subject.seekTimeout / 2, delayInterval: 10 },
				);
				expect(Date.now() - disconnectedAt).to.be.lessThan(subject.seekTimeout);
				expect(subject.getRouteHints(targetHash)).to.have.length(0);
				expect((subject as any).healthChecks.has(targetHash)).to.equal(false);
				if (dependents === "with") {
					const goodbye = goodbyes()[0] as Goodbye;
					expect(goodbye.leaving).to.deep.equal([targetHash]);
					expect(goodbye.header.mode.to).to.deep.equal([dependent.hashcode()]);
				}
			} finally {
				closingWrites.restore();
				probes.restore();
				publications.restore();
				subject.removeEventListener("peer:unreachable", onLost);
				await node.stop();
			}
		});
	}

	it("reports a peer whose session a relay already invalidated as unreachable at its direct disconnect", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		const target = (await Ed25519Keypair.create()).publicKey;
		const relay = (await Ed25519Keypair.create()).publicKey;
		const dependent = (await Ed25519Keypair.create()).publicKey;
		const targetHash = target.hashcode();
		const relayHash = relay.hashcode();
		const neighbours = [
			subject.addPeer(target.toPeerId(), target, protocol, "direct"),
			subject.addPeer(relay.toPeerId(), relay, protocol, "relay"),
			subject.addPeer(dependent.toPeerId(), dependent, protocol, "dependent"),
		];
		const checkIsAlive = sinon
			.stub(subject as any, "checkIsAlive")
			.resolves(false);
		const publications = sinon.spy(subject, "publishMessageMaybe");
		const goodbyes = () =>
			publications
				.getCalls()
				.map((call) => call.args[1])
				.filter((message) => message instanceof Goodbye);
		const lost: string[] = [];
		const onLost = ({ detail }: CustomEvent<PublicSignKey>) => {
			lost.push(detail.hashcode());
		};
		subject.addEventListener("peer:unreachable", onLost);
		try {
			for (const [index, peer] of neighbours.entries()) {
				await peer.attachOutboundStream(raw(`outbound-${index}`));
			}
			const routeSession = Date.now();
			subject.updateSession(relay, 456);
			subject.addRouteConnection(
				subject.publicKeyHash,
				relayHash,
				relay,
				-1,
				routeSession,
				456,
			);
			for (const nextHop of [targetHash, relayHash]) {
				subject.addRouteConnection(
					subject.publicKeyHash,
					nextHop,
					target,
					1,
					routeSession,
					123,
				);
			}
			subject.addRouteConnection(
				dependent.hashcode(),
				targetHash,
				target,
				1,
				routeSession,
				123,
			);
			// What a relay's departure hint does when it arrives before the direct
			// connection's own close is processed.
			subject.invalidateSession(targetHash);
			expect(
				subject.routes.isReachable(subject.publicKeyHash, targetHash),
			).to.equal(false);
			expect(
				subject.routes
					.findNeighbor(subject.publicKeyHash, targetHash)
					?.list.map((route) => route.hash),
			).to.have.members([targetHash, relayHash]);

			await subject["onPeerDisconnected"](target.toPeerId(), {
				id: "direct",
				remotePeer: target.toPeerId(),
				status: "closed",
			} as Connection);
			// The relay row that is left has no valid session, so it is not a
			// retained path: the loss is reported now, not by the probe's health check.
			expect(lost).to.deep.equal([targetHash]);
			expect(
				subject.routes.findNeighbor(subject.publicKeyHash, targetHash),
			).to.equal(undefined);
			expect(subject.routes.getSession(targetHash)).to.equal(undefined);
			expect(goodbyes()).to.have.length(1);
			const goodbye = goodbyes()[0] as Goodbye;
			expect(goodbye.leaving).to.deep.equal([targetHash]);
			expect(goodbye.header.mode.to).to.deep.equal([dependent.hashcode()]);
			expect(checkIsAlive.calledOnceWithExactly([targetHash])).to.equal(true);
		} finally {
			checkIsAlive.restore();
			publications.restore();
			subject.removeEventListener("peer:unreachable", onLost);
			await node.stop();
		}
	});

	it("does not remove replacement routes after an awaited old disconnect", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		const key = (await Ed25519Keypair.create()).publicKey;
		const old = subject.addPeer(key.toPeerId(), key, protocol, "old");
		const release = await pauseClose(old);
		const disconnected = subject["onPeerDisconnected"](key.toPeerId(), {
			id: "old",
			remotePeer: key.toPeerId(),
			status: "closed",
		} as Connection);
		const removeRoutes = sinon.spy(subject, "removePeerFromRoutes");
		try {
			const current = subject.addPeer(key.toPeerId(), key, protocol, "new");
			release();
			await disconnected;
			expect(subject.peers.get(key.hashcode()) === current).to.equal(true);
			expect(removeRoutes.called).to.equal(false);
		} finally {
			release();
			await disconnected;
			removeRoutes.restore();
			await node.stop();
		}
	});

	it("bounds unsettled retirements, disposes refused streams, and recovers capacity", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		const key = (await Ed25519Keypair.create()).publicKey;
		const releases: Array<() => void> = [];
		const closings: Promise<void>[] = [];
		let current = subject.addPeer(key.toPeerId(), key, protocol, "0");
		try {
			for (let i = 0; i < 256; i++) {
				releases.push(await pauseClose(current));
				closings.push(current.close());
				current = subject.addPeer(key.toPeerId(), key, protocol, String(i + 1));
			}
			expect((subject as any).retiredPeerStreams.size).to.equal(256);
			releases.push(await pauseClose(current));
			closings.push(current.close());
			expect(() =>
				subject.addPeer(key.toPeerId(), key, protocol, "overflow"),
			).to.throw("Too many pending peer stream closes");
			for (const direction of ["inbound", "outbound"]) {
				const refused = raw(direction);
				const connection = {
					id: "overflow",
					remotePeer: key.toPeerId(),
					status: "open",
					streams: [],
					newStream: sinon.stub().resolves(refused),
				} as unknown as Connection;
				let failure: unknown;
				try {
					if (direction === "inbound")
						await subject["_onIncomingStream"](refused, connection);
					else
						await subject["createOutboundStream"](key.toPeerId(), connection);
				} catch (error) {
					failure = error;
				}
				expect((failure as Error)?.message).to.equal(
					"Too many pending peer stream closes",
				);
				expect((refused.abort as sinon.SinonSpy).calledOnce).to.equal(true);
				expect(subject.peers.get(key.hashcode()) === current).to.equal(true);
				expect((subject as any).retiredPeerStreams.size).to.equal(256);
			}
			releases[0]!();
			await closings[0];
			await tick();
			expect((subject as any).retiredPeerStreams.size).to.equal(255);
			const recovered = subject.addPeer(
				key.toPeerId(),
				key,
				protocol,
				"recovered",
			);
			expect(recovered === current).to.equal(false);
			await recovered.attachOutboundStream(raw("recovered"));
			expect(recovered.isWritable).to.equal(true);
		} finally {
			for (const release of releases) release();
			await Promise.all(closings);
			await node.stop();
		}
	});

	it("allocates a usable replacement while old removal is suspended", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		const key = (await Ed25519Keypair.create()).publicKey;
		const old = subject.addPeer(key.toPeerId(), key, protocol, "old");
		const release = await pauseClose(old);
		const removing = subject["_removePeer"](key);
		try {
			const replacement = subject.addPeer(key.toPeerId(), key, protocol, "new");
			expect(replacement === old).to.equal(false);
			await replacement.attachOutboundStream(raw("new-outbound"));
			expect(replacement.isWritable).to.equal(true);
			release();
			expect((await removing) === undefined).to.equal(true);
			expect(subject.peers.get(key.hashcode()) === replacement).to.equal(true);
			expect(replacement.isWritable).to.equal(true);
		} finally {
			release();
			await removing;
			await node.stop();
		}
	});

	it("does not let old close callbacks or removal delete a newer object", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		const key = (await Ed25519Keypair.create()).publicKey;
		const old = subject.addPeer(key.toPeerId(), key, protocol, "old");
		const release = await pauseClose(old);
		const removing = subject["_removePeer"](key);
		const replacement = new PeerStreams({
			peerId: key.toPeerId(),
			publicKey: key,
			protocol,
			connId: "new",
		});
		subject.peers.set(key.hashcode(), replacement);
		try {
			release();
			expect((await removing) === undefined).to.equal(true);
			await tick();
			expect(subject.peers.get(key.hashcode()) === replacement).to.equal(true);
			await replacement.attachOutboundStream(raw("new-outbound"));
		} finally {
			release();
			await removing;
			await replacement.close();
			await node.stop();
		}
	});

	it("ignores an old connection's disconnect after replacement", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		const key = (await Ed25519Keypair.create()).publicKey;
		const current = subject.addPeer(key.toPeerId(), key, protocol, "new");
		const remove = sinon.spy(subject as any, "_removePeer");
		try {
			await subject["onPeerDisconnected"](key.toPeerId(), {
				id: "old",
				remotePeer: key.toPeerId(),
				status: "closed",
			} as Connection);
			expect(remove.called).to.equal(false);
			expect(subject.peers.get(key.hashcode()) === current).to.equal(true);
		} finally {
			remove.restore();
			await node.stop();
		}
	});

	for (const failure of [false, true]) {
		it(`ignores an obsolete inbound reader's ${failure ? "failure" : "data and completion"}`, async () => {
			const node = await createNode();
			const subject = node.services.directstream;
			const key = (await Ed25519Keypair.create()).publicKey;
			const old = subject.addPeer(key.toPeerId(), key, protocol, "old");
			const current = new PeerStreams({
				peerId: key.toPeerId(),
				publicKey: key,
				protocol,
				connId: "new",
			});
			subject.peers.set(key.hashcode(), current);
			const disconnect = sinon.spy(subject as any, "onPeerDisconnected");
			const process = sinon.stub(subject, "processRpc").resolves();
			const record = {
				iterable: (async function* () {
					if (failure) throw new Error("old reader ended");
					yield new Uint8ArrayList(new Uint8Array([1]));
				})(),
				bytesReceived: 0,
			} as unknown as InboundStreamRecord;
			try {
				await subject.processMessages(key, record, old);
				await tick();
				expect(disconnect.called).to.equal(false);
				expect(process.called).to.equal(false);
				expect(subject.peers.get(key.hashcode()) === current).to.equal(true);
			} finally {
				disconnect.restore();
				process.restore();
				await old.close();
				await current.close();
				await node.stop();
			}
		});
	}

	it("keeps retired closes inside the network shutdown barrier", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		const key = (await Ed25519Keypair.create()).publicKey;
		const old = subject.addPeer(key.toPeerId(), key, protocol, "old");
		const release = await pauseClose(old);
		const closing = old.close();
		let barrier: Promise<void> | undefined;
		try {
			const current = subject.addPeer(key.toPeerId(), key, protocol, "new");
			expect(current === old).to.equal(false);
			const currentDrained = pDefer<void>();
			const close = current.close.bind(current);
			sinon.stub(current, "close").callsFake(async () => {
				await close();
				currentDrained.resolve();
			});
			let drained = false;
			barrier = subject.beforeStop().then(() => {
				drained = true;
			});
			await currentDrained.promise;
			await tick();
			expect(drained).to.equal(false);
			release();
			await barrier;
			expect(drained).to.equal(true);
			expect((subject as any).retiredPeerStreams.size).to.equal(0);
		} finally {
			release();
			await closing;
			await barrier;
			await node.stop();
		}
	});
});
