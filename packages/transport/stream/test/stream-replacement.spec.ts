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
