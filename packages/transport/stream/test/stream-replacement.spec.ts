import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import type { Connection, Stream } from "@libp2p/interface";
import { tcp } from "@libp2p/tcp";
import { Ed25519Keypair } from "@peerbit/crypto";
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
		(subject as EventTarget).addEventListener("stream:outbound", outbound);
		(subject as EventTarget).addEventListener("stream:inbound", inbound);
		try {
			const current = subject.addPeer(key.toPeerId(), key, protocol, "new");
			expect(subject.routes.getSession(key.hashcode())).to.equal(123);
			expect(session.called).to.equal(false);
			old.dispatchEvent(new CustomEvent("stream:outbound"));
			old.dispatchEvent(new CustomEvent("stream:inbound"));
			expect(outbound.called || inbound.called).to.equal(false);
			current.dispatchEvent(new CustomEvent("stream:outbound"));
			expect(outbound.calledOnce).to.equal(true);
		} finally {
			session.restore();
			release();
			await closing;
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
