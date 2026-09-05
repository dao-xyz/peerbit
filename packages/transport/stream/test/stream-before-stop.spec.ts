import { noise } from "@chainsafe/libp2p-noise";
import { yamux } from "@chainsafe/libp2p-yamux";
import type { Connection, Stream } from "@libp2p/interface";
import { tcp } from "@libp2p/tcp";
import { Ed25519Keypair } from "@peerbit/crypto";
import { expect } from "chai";
import { createLibp2p } from "libp2p";
import pDefer from "p-defer";
import sinon from "sinon";
import { DirectStream, type DirectStreamComponents } from "../src/index.js";

class GuardedStream extends DirectStream {
	cleanupCount = 0;

	constructor(components: DirectStreamComponents) {
		super(components, ["/before-stop/1.0.0"], {
			connectionManager: false,
		});
	}

	override async stop() {
		if (!this.isStarted()) return;
		this.cleanupCount++;
		await super.stop();
	}
}

const createNode = (start = true) =>
	createLibp2p<{ directstream: GuardedStream }>({
		start,
		transports: [tcp()],
		streamMuxers: [yamux()],
		connectionEncrypters: [noise()],
		connectionMonitor: { enabled: false },
		connectionManager: { reconnectRetries: 0 },
		services: { directstream: (components) => new GuardedStream(components) },
	});

const createRaw = (id: string): Stream =>
	Object.assign(new EventTarget(), {
		id,
		protocol: "/before-stop/1.0.0",
		send: () => true,
		abort: sinon.spy(),
		close: sinon.stub().resolves(),
	}) as unknown as Stream;

describe("stream before-stop barrier", () => {
	it("preserves guarded subclass cleanup and permits a complete restart", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		try {
			const first = subject.beforeStop();
			expect(subject.beforeStop()).to.equal(first);
			await first;
			expect(subject.isStarted()).to.equal(true);
			expect(subject.cleanupCount).to.equal(0);
			await subject.start(); // must not reopen the quiescing service
			expect(subject.beforeStop()).to.equal(first);
			await node.stop();
			expect(subject.cleanupCount).to.equal(1);
			expect(subject.isStarted()).to.equal(false);
			await node.start();
			expect(subject.isStarted()).to.equal(true);
			expect(subject.beforeStop()).not.to.equal(first);
			await node.stop();
			expect(subject.cleanupCount).to.equal(2);
		} finally {
			await node.stop();
		}
	});

	for (const priorClose of ["peer", "service"] as const) {
		it(`awaits a pending ${priorClose} close before connection-manager shutdown`, async () => {
			const node = await createNode();
			const subject = node.services.directstream;
			const key = await Ed25519Keypair.create();
			const peer = subject.addPeer(
				key.publicKey.toPeerId(),
				key.publicKey,
				"/before-stop/1.0.0",
				"synthetic",
			);
			const raw = createRaw("pending-close");
			const rawAbort = raw.abort as sinon.SinonSpy;
			await peer.attachOutboundStream(raw);
			const queue = peer._getActiveOutboundPushable()!;
			const originalReturn = queue.return!.bind(queue);
			const entered = pDefer<void>();
			const release = pDefer<void>();
			const returnStub = sinon.stub(queue, "return").callsFake(async () => {
				entered.resolve();
				await release.promise;
				return originalReturn();
			});
			const beforeEntered = pDefer<void>();
			const originalBefore = subject.beforeStop.bind(subject);
			const beforeStub = sinon.stub(subject, "beforeStop").callsFake(() => {
				const pending = originalBefore();
				beforeEntered.resolve();
				return pending;
			});
			const manager = subject.components
				.connectionManager as typeof subject.components.connectionManager & {
				stop(): Promise<void>;
			};
			const originalStop = manager.stop.bind(manager);
			const snapshots: boolean[] = [];
			const managerStub = sinon.stub(manager, "stop").callsFake(async () => {
				snapshots.push(rawAbort.calledOnce);
				await originalStop();
			});
			let stopping: Promise<void> | undefined;
			const closing = priorClose === "peer" ? peer.close() : subject.stop();
			try {
				await entered.promise;
				stopping = Promise.resolve(node.stop());
				await beforeEntered.promise;
				expect(managerStub.called).to.equal(false);
				expect(rawAbort.called).to.equal(false);
				release.resolve();
				await Promise.all([closing, stopping]);
				expect(snapshots).to.deep.equal([true]);
				expect(returnStub.calledOnce).to.equal(true);
				expect(subject.cleanupCount).to.equal(1);
			} finally {
				release.resolve();
				await Promise.all([closing, stopping]);
				returnStub.restore();
				beforeStub.restore();
				managerStub.restore();
				await node.stop();
			}
		});
	}

	it("drains a pending outbound open and disposes its late raw stream before transport shutdown", async () => {
		const node = await createNode();
		const subject = node.services.directstream;
		const key = await Ed25519Keypair.create();
		const raw = createRaw("late-open");
		const entered = pDefer<void>();
		const aborted = pDefer<void>();
		const release = pDefer<Stream>();
		const connection = {
			id: "pending-open",
			status: "open",
			streams: [],
			newStream: async (
				_protocols: string[],
				options: { signal: AbortSignal },
			) => {
				options.signal.addEventListener("abort", () => aborted.resolve(), {
					once: true,
				});
				entered.resolve();
				return release.promise;
			},
		} as unknown as Connection;
		const manager = subject.components
			.connectionManager as typeof subject.components.connectionManager & {
			stop(): Promise<void>;
		};
		const originalStop = manager.stop.bind(manager);
		const snapshots: boolean[] = [];
		const managerStub = sinon.stub(manager, "stop").callsFake(async () => {
			snapshots.push((raw.abort as sinon.SinonSpy).calledOnce);
			await originalStop();
		});
		let stopping: Promise<void> | undefined;
		try {
			await subject.onPeerConnected(key.publicKey.toPeerId(), connection);
			await entered.promise;
			stopping = Promise.resolve(node.stop());
			await aborted.promise;
			expect(managerStub.called).to.equal(false);
			release.resolve(raw);
			await stopping;
			expect(snapshots).to.deep.equal([true]);
			expect(subject.peers.size).to.equal(0);
			expect(subject._outboundPump).to.equal(undefined);
		} finally {
			release.resolve(raw);
			await stopping;
			managerStub.restore();
			await node.stop();
		}
	});

	it("does not prevent startup after a never-started or failed-start barrier", async () => {
		const node = await createNode(false);
		const subject = node.services.directstream;
		await subject.beforeStop();
		const entered = pDefer<void>();
		const release = pDefer<void>();
		const failure = new Error("startup failed before acquiring resources");
		const startStub = sinon
			.stub(subject as any, "_startImpl")
			.callsFake(async () => {
				entered.resolve();
				await release.promise;
				throw failure;
			});
		try {
			const starting = subject.start();
			const result = starting.catch((error) => error);
			await entered.promise;
			const barrier = subject.beforeStop();
			release.resolve();
			expect(await result).to.equal(failure);
			await barrier;
			await subject.stop();
			startStub.restore();
			await node.start();
			expect(subject.isStarted()).to.equal(true);
		} finally {
			release.resolve();
			startStub.restore();
			await node.stop();
		}
	});

	for (const stopTiming of ["during startup", "after startup fails"] as const) {
		it(`awaits failed-start network teardown when stop is requested ${stopTiming}`, async () => {
			const node = await createNode(false);
			const subject = node.services.directstream;
			const startEntered = pDefer<void>();
			const releaseStart = pDefer<void>();
			const drainEntered = pDefer<void>();
			const releaseDrain = pDefer<void>();
			const failure = new Error("startup failed");
			const startStub = sinon
				.stub(subject as any, "_startImpl")
				.callsFake(async () => {
					startEntered.resolve();
					await releaseStart.promise;
					throw failure;
				});
			const unhandle = subject.components.registrar.unhandle.bind(
				subject.components.registrar,
			);
			const unhandleStub = sinon
				.stub(subject.components.registrar, "unhandle")
				.callsFake(async (...args) => {
					drainEntered.resolve();
					await releaseDrain.promise;
					await unhandle(...args);
				});
			let stopping: Promise<unknown> | undefined;
			let barrier: Promise<void> | undefined;
			let stopSettled = false;
			const stop = () => {
				// Exercise the base lifecycle when startup never reached the subclass's
				// started guard; there are no subclass resources to clean up yet.
				stopping = DirectStream.prototype.stop
					.call(subject)
					.catch((error) => error)
					.then((result) => {
						stopSettled = true;
						return result;
					});
			};
			try {
				const starting = subject.start().catch((error) => error);
				await startEntered.promise;
				barrier = subject.beforeStop();
				if (stopTiming === "during startup") stop();
				releaseStart.resolve();
				expect(await starting).to.equal(failure);
				await drainEntered.promise;
				if (stopTiming === "after startup fails") stop();
				await subject.start();
				expect(startStub.calledOnce).to.equal(true);
				expect(stopSettled).to.equal(false);
				releaseDrain.resolve();
				await Promise.all([barrier, stopping]);
				expect(stopSettled).to.equal(true);
				startStub.restore();
				unhandleStub.restore();
				await node.start();
				expect(subject.isStarted()).to.equal(true);
			} finally {
				releaseStart.resolve();
				releaseDrain.resolve();
				await Promise.all([barrier, stopping]);
				startStub.restore();
				unhandleStub.restore();
				await node.stop();
			}
		});
	}
});
