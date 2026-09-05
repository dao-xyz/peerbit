import type { PeerId, Stream } from "@libp2p/interface";
import { Ed25519Keypair } from "@peerbit/crypto";
import { AbortError, delay } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import { PeerStreams } from "../src/index.js";

const createInboundStream = (id: string, close: () => Promise<void>): Stream =>
	({
		id,
		protocol: "/test",
		[Symbol.asyncIterator]: async function* () {},
		abort: () => {},
		close,
	}) as unknown as Stream;

describe("peer stream shutdown", () => {
	let streams: PeerStreams;
	let clock: sinon.SinonFakeTimers;

	beforeEach(async () => {
		const keypair = await Ed25519Keypair.create();
		clock = sinon.useFakeTimers({
			now: 1_000_000,
			toFake: [
				"Date",
				"setTimeout",
				"clearTimeout",
				"setInterval",
				"clearInterval",
			],
		});
		streams = new PeerStreams({
			peerId: { toString: () => "remote-peer" } as PeerId,
			publicKey: keypair.publicKey,
			protocol: "/test",
			connId: "test-connection",
		});
	});

	afterEach(async () => {
		try {
			await streams.close();
		} finally {
			clock.restore();
		}
	});

	for (const inboundCount of [1, 2]) {
		it(`cancels inbound pruning on repeated close with ${inboundCount} inbound streams`, async () => {
			const rawStreams = Array.from({ length: inboundCount }, (_, i) =>
				createInboundStream(String(i), sinon.stub().resolves()),
			);
			const aborts = rawStreams.map((raw) => sinon.spy(raw, "abort"));
			const records = rawStreams.map((raw) => streams.attachInboundStream(raw));
			const onClose = sinon.spy();
			streams.addEventListener("close", onClose);
			expect(streams["_inboundPruneTimer"]).to.exist;
			expect(clock.countTimers()).to.equal(2); // pruning and bandwidth tracking

			await streams.close();
			expect(streams["_inboundPruneTimer"]).to.equal(undefined);
			expect(clock.countTimers()).to.equal(0);
			expect(streams._getInboundCount()).to.equal(0);
			expect(streams.inboundStream).to.equal(undefined);
			expect(streams.rawInboundStream).to.equal(undefined);
			expect(
				records.every((record) => record.abortController.signal.aborted),
			).to.equal(true);

			await streams.close();
			await clock.tickAsync(PeerStreams.INBOUND_IDLE_MS * 2);
			expect(onClose.callCount).to.equal(1);
			expect(aborts.every((abort) => abort.calledOnce)).to.equal(true);
			expect(clock.countTimers()).to.equal(0);
		});
	}

	for (const direction of ["inbound", "outbound"] as const) {
		it(`disposes and rejects a late ${direction} attachment after close`, async () => {
			await streams.close();
			const close = sinon.stub().resolves();
			const raw = createInboundStream("late", close);
			const abort = sinon.spy(raw, "abort");
			const onStream = sinon.spy();
			streams.addEventListener(`stream:${direction}`, onStream);

			if (direction === "inbound") {
				expect(() => streams.attachInboundStream(raw)).to.throw(
					AbortError,
					"Closed",
				);
			} else {
				await expect(streams.attachOutboundStream(raw)).to.be.rejectedWith(
					AbortError,
					"Closed",
				);
			}

			expect(abort.calledOnce).to.equal(true);
			expect(abort.firstCall.args[0]).to.be.instanceOf(AbortError);
			expect(close.calledOnce).to.equal(true);
			expect(onStream.called).to.equal(false);
			expect(streams.isReadable).to.equal(false);
			expect(streams.isWritable).to.equal(false);
			expect(streams._getInboundCount()).to.equal(0);
			expect(streams._getOutboundCount()).to.equal(0);
			expect(streams["_inboundPruneTimer"]).to.equal(undefined);
			expect(streams["_outboundPruneTimer"]).to.equal(undefined);
			await streams.close();
			expect(clock.countTimers()).to.equal(0);
		});
	}

	it("does not prune, rearm, or accept attachments while outbound shutdown is pending", async () => {
		const outbound = Object.assign(new EventTarget(), {
			id: "outbound",
			send: () => true,
			abort: () => {},
			close: async () => {},
		}) as unknown as Stream;
		await streams.attachOutboundStream(outbound);
		const pushable = streams._getActiveOutboundPushable()!;
		const originalReturn = pushable.return!.bind(pushable);
		let releaseReturn!: () => void;
		const returnGate = new Promise<void>((resolve) => {
			releaseReturn = resolve;
		});
		const returnStub = sinon.stub(pushable, "return").callsFake(async () => {
			await returnGate;
			return originalReturn();
		});
		const onClose = sinon.spy();
		streams.addEventListener("close", onClose);
		const rawClose = sinon.stub().resolves();
		const schedule = sinon.spy(globalThis, "setTimeout");
		let pruneCallback!: () => void;
		try {
			streams.attachInboundStream(createInboundStream("one", rawClose));
			pruneCallback = schedule.firstCall.args[0] as () => void;
			streams.attachInboundStream(createInboundStream("two", rawClose));
		} finally {
			schedule.restore();
		}

		const closing = streams.close();
		try {
			expect(returnStub.calledOnce).to.equal(true);
			expect(onClose.called).to.equal(false);
			expect(streams["_inboundPruneTimer"]).to.equal(undefined);
			await clock.tickAsync(PeerStreams.INBOUND_IDLE_MS + 1);
			// A callback already queued when close began must also remain inert.
			pruneCallback();
			streams["_scheduleInboundPrune"]();
			expect(streams["_inboundPruneTimer"]).to.equal(undefined);
			expect(rawClose.called).to.equal(false);
			expect(streams._getInboundCount()).to.equal(2);
			expect(clock.countTimers()).to.equal(1); // bandwidth until close completes

			const lateInbound = createInboundStream(
				"late-inbound",
				sinon.stub().resolves(),
			);
			const lateOutbound = createInboundStream(
				"late-outbound",
				sinon.stub().resolves(),
			);
			const inboundAbort = sinon.spy(lateInbound, "abort");
			const outboundAbort = sinon.spy(lateOutbound, "abort");
			expect(() => streams.attachInboundStream(lateInbound)).to.throw(
				AbortError,
				"Closed",
			);
			await expect(
				streams.attachOutboundStream(lateOutbound),
			).to.be.rejectedWith(AbortError, "Closed");
			expect(inboundAbort.calledOnce).to.equal(true);
			expect(outboundAbort.calledOnce).to.equal(true);
			expect(streams._getInboundCount()).to.equal(2);
			expect(streams._getOutboundCount()).to.equal(1);
			await streams.close();
			expect(returnStub.calledOnce).to.equal(true);
		} finally {
			// Release the real shutdown even when an assertion fails.
			releaseReturn();
			await closing;
			returnStub.restore();
		}
		await clock.tickAsync(0);
		expect(onClose.callCount).to.equal(1);
		expect(streams._getInboundCount()).to.equal(0);
		expect(streams._getOutboundCount()).to.equal(0);
		expect(streams["_inboundPruneTimer"]).to.equal(undefined);
		expect(clock.countTimers()).to.equal(0);
	});

	it("keeps scheduling normal inbound pruning until only the active stream remains", async () => {
		const survivorClose = sinon.stub().resolves();
		const staleClose = sinon.stub().resolves();
		const survivor = streams.attachInboundStream(
			createInboundStream("survivor", survivorClose),
		);
		const stale = streams.attachInboundStream(
			createInboundStream("stale", staleClose),
		);
		await clock.tickAsync(PeerStreams.INBOUND_IDLE_MS);
		expect(streams.inboundStreams).to.deep.equal([survivor, stale]);
		expect(streams["_inboundPruneTimer"]).to.exist;
		survivor.lastActivity = Date.now();
		await clock.tickAsync(PeerStreams.INBOUND_IDLE_MS);
		expect(streams.inboundStreams).to.deep.equal([survivor]);
		expect(stale.abortController.signal.aborted).to.equal(true);
		expect(staleClose.calledOnce).to.equal(true);
		expect(survivorClose.called).to.equal(false);
		expect(streams["_inboundPruneTimer"]).to.equal(undefined);
		expect(clock.countTimers()).to.equal(1); // bandwidth tracking remains active
	});
});

describe("peer stream cleanup", () => {
	it("handles a rejected close while pruning an inactive inbound stream", async () => {
		const keypair = await Ed25519Keypair.create();
		const streams = new PeerStreams({
			peerId: { toString: () => "remote-peer" } as PeerId,
			publicKey: keypair.publicKey,
			protocol: "/test",
			connId: "test-connection",
		});
		const closeError = new AggregateError(
			[new Error("FIN_ACK timed out")],
			"All promises were rejected",
		);
		const unhandled: unknown[] = [];
		const onUnhandledRejection = (reason: unknown) => {
			unhandled.push(reason);
		};
		let staleCloseCalls = 0;

		process.on("unhandledRejection", onUnhandledRejection);
		try {
			const survivor = streams.attachInboundStream(
				createInboundStream("survivor", async () => {}),
			);
			const stale = streams.attachInboundStream(
				createInboundStream("stale", () => {
					staleCloseCalls += 1;
					return Promise.reject(closeError);
				}),
			);

			survivor.lastActivity = Date.now();
			stale.lastActivity = 0;
			streams.forcePruneInbound();

			expect(staleCloseCalls).to.equal(1);
			expect(streams.inboundStreams).to.deep.equal([survivor]);
			await delay(25);
			expect(unhandled).to.deep.equal([]);
		} finally {
			process.off("unhandledRejection", onUnhandledRejection);
			await streams.close();
		}
	});
});
