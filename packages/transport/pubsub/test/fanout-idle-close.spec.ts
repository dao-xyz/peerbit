import { TestSession } from "@peerbit/libp2p-test-utils";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import {
	FanoutChannel,
	FanoutTree,
	TopicControlPlane,
	TopicRootControlPlane,
} from "../src/index.js";

type Services = { pubsub: TopicControlPlane; fanout: FanoutTree };
type ChannelState = {
	channel: FanoutChannel;
	join: Promise<void>;
	ephemeral: boolean;
	lastUsedAt: number;
	idleCloseTimeout?: ReturnType<typeof setTimeout>;
};

describe("pubsub (fanout idle-close ownership)", () => {
	let session: TestSession<Services>;
	let pubsub: TopicControlPlane;
	let roots: TopicRootControlPlane;
	let timers: sinon.SinonSpy;
	const topic = "idle-close-ownership";
	let shard: string;
	const state = () =>
		(pubsub as any).fanoutChannels.get(shard) as ChannelState | undefined;
	const open = async () => {
		roots.setTopicRoot(shard, pubsub.publicKeyHash);
		await (pubsub as any).ensureFanoutChannel(shard, { ephemeral: true });
		return state()!;
	};
	const callbackFor = (handle: ReturnType<typeof setTimeout>) => {
		const call = timers.getCalls().find((call) => call.returnValue === handle);
		expect(call, "timer was created by the real scheduler").to.exist;
		return call!.args[0] as () => void;
	};

	beforeEach(async () => {
		roots = new TopicRootControlPlane();
		let fanout: FanoutTree | undefined;
		const getFanout = (components: any) =>
			(fanout ??= new FanoutTree(components, {
				connectionManager: false,
				topicRootControlPlane: roots,
			}));
		session = await TestSession.disconnected<Services>(1, {
			services: {
				fanout: getFanout,
				pubsub: (components) =>
					new TopicControlPlane(components, {
						connectionManager: false,
						fanout: getFanout(components),
						topicRootControlPlane: roots,
					}),
			},
		});
		pubsub = session.peers[0].services.pubsub;
		shard = (pubsub as any).getShardTopicForUserTopic(topic);
		timers = sinon.spy(globalThis, "setTimeout");
	});

	afterEach(async () => {
		// Real owned timers are inspected before cleanup. Clear even leaked handles
		// on a failing baseline so the negative control does not need a 60s sleep.
		for (const call of timers.getCalls()) {
			if (call.args[1] === 60_000) clearTimeout(call.returnValue);
		}
		timers.restore();
		await session.stop();
	});

	it("does not rearm an idle timer when a pending publish completes during stop", async () => {
		const current = await open();
		const publishEntered = pDefer<void>();
		const publishGate = pDefer<void>();
		const leaveEntered = pDefer<void>();
		const leaveGate = pDefer<void>();
		const originalLeave = current.channel.leave.bind(current.channel);
		const publishStub = sinon
			.stub(current.channel, "publish")
			.callsFake(async () => {
				publishEntered.resolve();
				await publishGate.promise;
			});
		const leaveStub = sinon
			.stub(current.channel, "leave")
			.callsFake(async (options) => {
				leaveEntered.resolve();
				await leaveGate.promise;
				await originalLeave(options);
			});
		let stopping: Promise<void> | undefined;
		const publishing = pubsub.publish(new Uint8Array([1]), { topics: [topic] });
		try {
			await publishEntered.promise;
			const timerCount = timers
				.getCalls()
				.filter((call) => call.args[1] === 60_000).length;
			stopping = pubsub.stop();
			await leaveEntered.promise;
			publishGate.resolve();
			await publishing;
			expect(
				timers.getCalls().filter((call) => call.args[1] === 60_000),
			).to.have.length(timerCount);
			leaveGate.resolve();
			await stopping;
			expect(current.idleCloseTimeout).to.equal(undefined);
			expect(state()).to.equal(undefined);
		} finally {
			publishGate.resolve();
			leaveGate.resolve();
			try {
				await publishing;
			} finally {
				try {
					await stopping;
				} finally {
					publishStub.restore();
					leaveStub.restore();
				}
			}
		}
	});

	it("does not arm an idle timer when a pending join completes during stop", async () => {
		const joinEntered = pDefer<void>();
		const joinGate = pDefer<void>();
		const leaveEntered = pDefer<void>();
		const leaveGate = pDefer<void>();
		roots.setTopicRoot(shard, "controlled-remote-root");
		const confirmStub = sinon
			.stub(pubsub as any, "confirmDirectShardRoot")
			.resolves("controlled-remote-root");
		const waitStub = sinon
			.stub(session.peers[0].services.fanout, "waitFor")
			.resolves();
		const joinStub = sinon
			.stub(FanoutChannel.prototype, "join")
			.callsFake(async () => {
				joinEntered.resolve();
				await joinGate.promise;
			});
		// The channel is real; only remote admission/leave timing is controlled.
		const opening: Promise<unknown> = (pubsub as any).ensureFanoutChannel(
			shard,
			{ ephemeral: true },
		);
		const settled = opening.then(
			(): undefined => undefined,
			(error: unknown) => error,
		);
		let stopping: Promise<void> | undefined;
		let leaveStub: sinon.SinonStub | undefined;
		try {
			await joinEntered.promise;
			const current = state()!;
			const originalLeave = current.channel.leave.bind(current.channel);
			leaveStub = sinon
				.stub(current.channel, "leave")
				.callsFake(async (options) => {
					leaveEntered.resolve();
					await leaveGate.promise;
					await originalLeave(options);
				});
			stopping = pubsub.stop();
			await leaveEntered.promise;
			joinGate.resolve();
			await current.join;
			expect(current.idleCloseTimeout).to.equal(undefined);
			expect(
				timers.getCalls().filter((call) => call.args[1] === 60_000),
			).to.have.length(0);
			leaveGate.resolve();
			await stopping;
			expect(await settled).to.be.instanceOf(Error);
		} finally {
			joinGate.resolve();
			leaveGate.resolve();
			try {
				await settled;
			} finally {
				try {
					await stopping;
				} finally {
					leaveStub?.restore();
					joinStub.restore();
					waitStub.restore();
					confirmStub.restore();
				}
			}
		}
	});

	for (const restart of [false, true]) {
		it(`ignores an old idle callback after ${restart ? "same-instance restart" : "channel replacement"}`, async () => {
			const previous = await open();
			const oldCallback = callbackFor(previous.idleCloseTimeout!);
			if (restart) {
				await pubsub.stop();
				await pubsub.start();
			} else {
				await (pubsub as any).closeFanoutChannel(shard);
			}
			const replacement = await open();
			const replacementTimer = replacement.idleCloseTimeout;
			replacement.lastUsedAt = Date.now() - 60_001;
			// Model a callback already queued when its old handle was cancelled.
			oldCallback();
			expect(state()).to.equal(replacement);
			expect(replacement.idleCloseTimeout).to.equal(replacementTimer);
		});
	}

	it("does not let an old callback consume a refreshed timer on the same channel", async () => {
		const current = await open();
		const oldCallback = callbackFor(current.idleCloseTimeout!);
		await pubsub.publish(new Uint8Array([2]), { topics: [topic] });
		const refreshedTimer = current.idleCloseTimeout;
		current.lastUsedAt = Date.now() - 60_001;
		oldCallback();
		expect(state()).to.equal(current);
		expect(current.idleCloseTimeout).to.equal(refreshedTimer);
	});

	for (const restart of [false, true]) {
		it(`does not touch a replacement after an old publish settles across ${restart ? "restart" : "channel replacement"}`, async () => {
			const previous = await open();
			const entered = pDefer<void>();
			const gate = pDefer<void>();
			const publishStub = sinon
				.stub(previous.channel, "publish")
				.callsFake(async () => {
					entered.resolve();
					await gate.promise;
				});
			const publishing = pubsub.publish(new Uint8Array([3]), {
				topics: [topic],
			});
			try {
				await entered.promise;
				if (restart) {
					await pubsub.stop();
					await pubsub.start();
				} else {
					await (pubsub as any).closeFanoutChannel(shard);
				}
				const replacement = await open();
				const timer = replacement.idleCloseTimeout;
				replacement.lastUsedAt = 1;
				gate.resolve();
				await publishing;
				expect(state()).to.equal(replacement);
				expect(replacement.lastUsedAt).to.equal(1);
				expect(replacement.idleCloseTimeout).to.equal(timer);
			} finally {
				gate.resolve();
				try {
					await publishing;
				} finally {
					publishStub.restore();
				}
			}
		});
	}

	it("still closes a current idle channel and releases its timer", async () => {
		const current = await open();
		const callback = callbackFor(current.idleCloseTimeout!);
		const closed = pDefer<void>();
		const originalLeave = current.channel.leave.bind(current.channel);
		const leaveStub = sinon
			.stub(current.channel, "leave")
			.callsFake(async (options) => {
				try {
					await originalLeave(options);
				} finally {
					closed.resolve();
				}
			});
		try {
			current.lastUsedAt = Date.now() - 60_001;
			callback();
			await closed.promise;
			expect(state()).to.equal(undefined);
			expect(current.idleCloseTimeout).to.equal(undefined);
		} finally {
			leaveStub.restore();
		}
	});

	it("still schedules a new idle check when the current channel was recently used", async () => {
		const current = await open();
		const firstTimer = current.idleCloseTimeout!;
		const callback = callbackFor(firstTimer);
		callback();
		expect(state()).to.equal(current);
		expect(current.idleCloseTimeout).not.to.equal(firstTimer);
		expect(current.idleCloseTimeout).not.to.equal(undefined);
	});
});
