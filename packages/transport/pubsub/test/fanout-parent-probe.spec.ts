import { TestSession } from "@peerbit/libp2p-test-utils";
import { AnyWhere } from "@peerbit/stream-interface";
import { expect } from "chai";
import sinon from "sinon";
import {
	type ParentProbeReply,
	tsFanoutWireCodec,
} from "../src/fanout-tree-codec.js";
import { FanoutTree } from "../src/index.js";

const probeParentCandidate = Reflect.get(
	FanoutTree.prototype,
	"probeParentCandidate",
) as (
	this: any,
	channel: any,
	parent: string,
	timeoutMs: number,
	signal: AbortSignal,
) => Promise<ParentProbeReply | undefined>;
const sendControl = Reflect.get(FanoutTree.prototype, "_sendControl");

const deferred = <T>() => {
	let resolve!: (value: T) => void;
	const promise = new Promise<T>((done) => {
		resolve = done;
	});
	return { promise, resolve };
};

const reply: ParentProbeReply = {
	hash: "parent",
	rooted: true,
	accepting: false,
	repairing: false,
	overloaded: false,
	reservationToken: 0,
	level: 0,
	maxChildren: 1,
	freeSlots: 0,
	children: 1,
	haveToExclusive: 0,
	missingSeqs: 0,
	dataWriteDrops: 0,
	droppedForwards: 0,
};

const fixture = () => {
	const peer = { publicKey: { hashcode: () => "parent" } };
	const channel = {
		id: { key: new Uint8Array(32) },
		pendingParentProbe: new Map<
			number,
			{ peer: typeof peer; resolve(value?: ParentProbeReply): void }
		>(),
	};
	const context = {
		peers: new Map([["parent", peer]]),
		random: () => 0,
		codec: tsFanoutWireCodec,
		_sendControl: sinon.stub().resolves(),
	};
	const controller = new AbortController();
	const added = sinon.spy(controller.signal, "addEventListener");
	const removed = sinon.spy(controller.signal, "removeEventListener");
	const probe = () =>
		probeParentCandidate.call(
			context,
			channel,
			"parent",
			100,
			controller.signal,
		);
	const expectClean = (clock: sinon.SinonFakeTimers) => {
		expect(channel.pendingParentProbe.size).to.equal(0);
		expect(clock.countTimers()).to.equal(0);
		for (const call of added.getCalls()) {
			expect(removed.calledWith("abort", call.args[1])).to.equal(true);
		}
	};
	return { context, channel, controller, probe, expectClean };
};

describe("fanout parent probe", () => {
	let clock: sinon.SinonFakeTimers | undefined;
	let session: TestSession<{ fanout: FanoutTree }> | undefined;

	afterEach(async () => {
		clock?.restore();
		clock = undefined;
		sinon.restore();
		await session?.stop();
		session = undefined;
	});

	for (const blocked of ["signing", "send ignoring abort"] as const) {
		it(`bounds blocked ${blocked} and cleans up without awaiting it`, async () => {
			clock = sinon.useFakeTimers();
			const f = fixture();
			const gate = deferred<any>();
			const publish = sinon.stub().resolves(true);
			if (blocked === "signing") {
				Object.assign(f.context, {
					_sendControl: sendControl,
					recordControlSend: sinon.stub(),
					createMessage: sinon.stub().returns(gate.promise),
					publishMessageMaybe: publish,
				});
			} else {
				f.context._sendControl.returns(gate.promise);
			}
			const pending = f.probe();
			expect(f.channel.pendingParentProbe.size).to.equal(1);
			await clock.tickAsync(100);
			expect(await pending).to.equal(undefined);
			f.expectClean(clock);
			if (blocked === "send ignoring abort") {
				expect(f.context._sendControl.firstCall.args[2].aborted).to.equal(true);
			}
			gate.resolve({});
			await clock.tickAsync(0);
			expect(publish.notCalled).to.equal(true);
			f.expectClean(clock);
		});
	}

	for (const cancellation of ["caller abort", "timeout"] as const) {
		it(`fences a just-finished signer before its send continuation after ${cancellation}`, async () => {
			clock = sinon.useFakeTimers();
			const f = fixture();
			const signing = deferred<any>();
			const publish = sinon.stub().resolves(true);
			Object.assign(f.context, {
				_sendControl: sendControl,
				recordControlSend: sinon.stub(),
				createMessage: sinon.stub().returns(signing.promise),
				publishMessageMaybe: publish,
			});
			const reason = new Error("caller left");
			const outcome = f.probe().catch((error) => error);
			// Queue the send continuation first, then cancel before microtasks drain.
			signing.resolve({});
			if (cancellation === "caller abort") f.controller.abort(reason);
			else clock.tick(100);
			expect(await outcome).to.equal(
				cancellation === "caller abort" ? reason : undefined,
			);
			await clock.tickAsync(0);
			expect(publish.notCalled).to.equal(true);
			f.expectClean(clock);
		});
	}

	for (const completion of [
		"success",
		"timeout",
		"abort",
		"send failure",
	] as const) {
		it(`cleans pending records, timers and caller listeners after ${completion}`, async () => {
			clock = sinon.useFakeTimers();
			const f = fixture();
			const reason = new Error(completion);
			if (completion === "send failure") f.context._sendControl.rejects(reason);
			const outcome = f.probe().catch((error) => error);
			const signal = f.context._sendControl.firstCall.args[2] as AbortSignal;
			if (completion === "success") {
				f.channel.pendingParentProbe.get(0)!.resolve(reply);
			} else if (completion === "timeout") {
				await clock.tickAsync(100);
			} else if (completion === "abort") {
				f.controller.abort(reason);
			}
			expect(await outcome).to.equal(
				completion === "success"
					? reply
					: completion === "abort"
						? reason
						: undefined,
			);
			expect(signal.aborted).to.equal(true);
			f.expectClean(clock);
		});
	}

	it("does not start a send for a pre-aborted caller", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		const reason = new Error("already aborted");
		f.controller.abort(reason);
		expect(await f.probe().catch((error) => error)).to.equal(reason);
		expect(f.context._sendControl.notCalled).to.equal(true);
		f.expectClean(clock);
	});

	it("keeps overlapping probes distinct when random request IDs collide", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		const first = f.probe();
		const second = f.probe();
		expect([...f.channel.pendingParentProbe.keys()]).to.deep.equal([0, 1]);
		f.channel.pendingParentProbe.get(1)!.resolve({ ...reply, level: 2 });
		expect((await second)?.level).to.equal(2);
		expect([...f.channel.pendingParentProbe.keys()]).to.deep.equal([0]);
		f.channel.pendingParentProbe.get(0)!.resolve(reply);
		expect(await first).to.equal(reply);
		f.expectClean(clock);
	});

	it("does not let a settled probe delete its reused request ID's successor", async () => {
		clock = sinon.useFakeTimers();
		const f = fixture();
		const first = f.probe();
		const old = f.channel.pendingParentProbe.get(0)!;
		// Reply handling removes the old record synchronously, before finally runs.
		f.channel.pendingParentProbe.delete(0);
		old.resolve(reply);
		const second = f.probe();
		const successor = f.channel.pendingParentProbe.get(0)!;
		expect(successor).not.to.equal(old);
		expect(await first).to.equal(reply);
		expect(f.channel.pendingParentProbe.get(0)).to.equal(successor);
		successor.resolve(reply);
		expect(await second).to.equal(reply);
		f.expectClean(clock);
	});

	for (const mismatch of ["peer", "stream", "current stream"] as const) {
		it(`does not consume an expected reply after a signed reply from the wrong ${mismatch}`, async () => {
			session = await TestSession.disconnected<{ fanout: FanoutTree }>(3, {
				services: {
					fanout: (components) =>
						new FanoutTree(components, { connectionManager: false }),
				},
			});
			const [receiver, parent, other] = session.peers.map(
				(peer) => peer.services.fanout,
			);
			const internals = receiver as any;
			const parentStream = receiver!.addPeer(
				parent!.peerId,
				parent!.publicKey,
				"/peerbit/fanout-tree/0.5.0",
				"parent-probe",
			);
			const otherStream = receiver!.addPeer(
				other!.peerId,
				other!.publicKey,
				"/peerbit/fanout-tree/0.5.0",
				"other-probe",
			);
			const id = receiver!.openChannel(
				"parent-probe-owner",
				receiver!.publicKeyHash,
				{
					role: "root",
					msgRate: 1,
					msgSize: 8,
					uploadLimitBps: 1_000_000,
					maxChildren: 1,
					repair: false,
				},
			);
			const ch = internals.channelsBySuffixKey.get(id.suffixKey);
			sinon.stub(internals, "_sendControl").resolves();
			const controller = new AbortController();
			const pending = internals.probeParentCandidate(
				ch,
				parent!.publicKeyHash,
				5_000,
				controller.signal,
			);
			const [reqId, record] = [...ch.pendingParentProbe.entries()][0] as [
				number,
				any,
			];
			const bytes = tsFanoutWireCodec.encodeParentProbeReply(id.key, reqId, {
				...reply,
				flags: 1,
			});
			const sender = mismatch === "peer" ? other! : parent!;
			const wrongReply = await sender.createMessage(bytes, {
				mode: new AnyWhere(),
			});
			expect(await wrongReply.verify(true)).to.equal(true);
			const replacement = Object.create(parentStream);
			if (mismatch === "current stream")
				receiver!.peers.set(parent!.publicKeyHash, replacement);
			try {
				await receiver!.onDataMessage(
					sender.publicKey,
					mismatch === "peer"
						? otherStream
						: mismatch === "stream"
							? replacement
							: parentStream,
					wrongReply,
					0,
				);
				expect(ch.pendingParentProbe.get(reqId)).to.equal(record);
			} finally {
				receiver!.peers.set(parent!.publicKeyHash, parentStream);
			}
			// Once the expected stream is current, its real signed response still wins.
			const validReply = await parent!.createMessage(bytes, {
				mode: new AnyWhere(),
			});
			expect(await validReply.verify(true)).to.equal(true);
			await receiver!.onDataMessage(
				parent!.publicKey,
				parentStream,
				validReply,
				0,
			);
			expect((await pending)?.hash).to.equal(parent!.publicKeyHash);
			expect(ch.pendingParentProbe.size).to.equal(0);
		});
	}
});
