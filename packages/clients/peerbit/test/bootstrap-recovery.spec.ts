import { waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import {
	BootstrapRecoveryController,
	type BootstrapRecoveryRuntime,
	validateBootstrapRecoveryOptions,
} from "../src/bootstrap-recovery.js";
import { Peerbit } from "../src/peer.js";
import { expectRejectedWith } from "./utils/rejection.js";

const peerIdA = "12D3KooWKj1J1hHxrYyB37qDDGCi9aU2vcHzDZhtMk7te7dEmqqT";
const peerIdB = "12D3KooWAYyiQBc1ti51riCkNX6Nvh33pWWvNfyrcPHrq373qCju";
const addressA = `/dns4/node-a.peerchecker.com/tcp/4003/wss/p2p/${peerIdA}`;
const addressB = `/dns4/node-b.peerchecker.com/tcp/4003/wss/p2p/${peerIdB}`;
const isNode = typeof process !== "undefined" && !!process.versions?.node;

const makeRuntime = (properties: {
	bootstrap: BootstrapRecoveryRuntime["bootstrap"];
	connected: () => boolean;
	online?: () => boolean;
	connectionEvents?: EventTarget;
	onlineEvents?: EventTarget;
	random?: () => number;
}): BootstrapRecoveryRuntime => ({
	bootstrap: properties.bootstrap,
	isConnected: properties.connected,
	isOnline: properties.online,
	connectionEvents: properties.connectionEvents ?? new EventTarget(),
	onlineEvents: properties.onlineEvents,
	random: properties.random,
});

describe("bootstrap recovery policy", () => {
	let clock: sinon.SinonFakeTimers;
	let controller: BootstrapRecoveryController | undefined;

	beforeEach(() => {
		clock = sinon.useFakeTimers({
			now: 0,
			shouldClearNativeTimers: true,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
	});

	afterEach(async () => {
		await controller?.stop();
		controller = undefined;
		clock.restore();
	});

	it("enforces the portable JavaScript timer boundary", () => {
		const maximumTimerDelayMs = 2_147_483_647;
		expect(() =>
			validateBootstrapRecoveryOptions({
				initialDelayMs: maximumTimerDelayMs,
				maxDelayMs: maximumTimerDelayMs,
				cooldownMs: maximumTimerDelayMs,
			}),
		).not.to.throw();

		for (const field of [
			"initialDelayMs",
			"maxDelayMs",
			"cooldownMs",
		] as const) {
			expect(() =>
				validateBootstrapRecoveryOptions({
					[field]: maximumTimerDelayMs + 1,
				}),
			).to.throw(
				`bootstrapRecovery.${field} must be <= ${maximumTimerDelayMs}`,
			);
		}
	});

	it("recovers an offline-at-start browser on its online signal", async () => {
		const connectionEvents = new EventTarget();
		const onlineEvents = new EventTarget();
		let connected = false;
		let online = false;
		let attempts = 0;
		controller = new BootstrapRecoveryController(
			makeRuntime({
				connectionEvents,
				onlineEvents,
				connected: () => connected,
				online: () => online,
				bootstrap: async () => {
					attempts += 1;
					connected = true;
				},
			}),
			{
				initialDelayMs: 100,
				maxDelayMs: 800,
				cooldownMs: 50,
				jitter: 0,
			},
		);

		controller.start();
		await clock.tickAsync(0);
		expect(attempts).to.equal(0);
		expect(clock.countTimers()).to.equal(1);

		online = true;
		onlineEvents.dispatchEvent(new Event("online"));
		await clock.tickAsync(0);
		expect(attempts).to.equal(1);
		expect(clock.countTimers()).to.equal(0);
	});

	it("recovers after the last connection closes while respecting cooldown", async () => {
		const connectionEvents = new EventTarget();
		let connected = true;
		let attempts = 0;
		controller = new BootstrapRecoveryController(
			makeRuntime({
				connectionEvents,
				connected: () => connected,
				bootstrap: async () => {
					attempts += 1;
					connected = true;
				},
			}),
			{
				initialDelayMs: 100,
				maxDelayMs: 100,
				cooldownMs: 50,
				jitter: 0,
			},
		);

		controller.start();
		connected = false;
		connectionEvents.dispatchEvent(new Event("connection:close"));
		await clock.tickAsync(49);
		expect(attempts).to.equal(0);
		await clock.tickAsync(1);
		expect(attempts).to.equal(1);
	});

	it("bounds exponential backoff and deterministic jitter", async () => {
		const attemptTimes: number[] = [];
		controller = new BootstrapRecoveryController(
			makeRuntime({
				connected: () => false,
				random: () => 1,
				bootstrap: async () => {
					attemptTimes.push(Date.now());
					throw new Error("offline");
				},
			}),
			{
				initialDelayMs: 100,
				maxDelayMs: 250,
				backoffFactor: 2,
				cooldownMs: 0,
				jitter: 0.5,
			},
		);

		controller.start();
		await clock.tickAsync(0);
		await clock.tickAsync(150);
		await clock.tickAsync(250);
		await clock.tickAsync(250);
		expect(attemptTimes).to.deep.equal([0, 150, 400, 650]);
	});

	it("keeps a non-zero retry floor under maximum negative jitter", async () => {
		let attempts = 0;
		controller = new BootstrapRecoveryController(
			makeRuntime({
				connected: () => false,
				random: () => 0,
				bootstrap: async () => {
					attempts += 1;
					throw new Error("offline");
				},
			}),
			{
				initialDelayMs: 1,
				maxDelayMs: 1,
				cooldownMs: 0,
				jitter: 1,
			},
		);

		controller.start();
		await clock.tickAsync(0);
		expect(attempts).to.equal(1);
		await clock.tickAsync(0);
		expect(attempts).to.equal(1);
		await clock.tickAsync(1);
		expect(attempts).to.equal(2);
	});

	it("keeps retries single-flight with one timer under event storms", async () => {
		const connectionEvents = new EventTarget();
		const onlineEvents = new EventTarget();
		let attempts = 0;
		let rejectAttempt!: (error: Error) => void;
		controller = new BootstrapRecoveryController(
			makeRuntime({
				connectionEvents,
				onlineEvents,
				connected: () => false,
				bootstrap: () => {
					attempts += 1;
					return new Promise<void>((_resolve, reject) => {
						rejectAttempt = reject;
					});
				},
			}),
			{
				initialDelayMs: 100,
				maxDelayMs: 400,
				cooldownMs: 0,
				jitter: 0,
			},
		);

		controller.start();
		await clock.tickAsync(0);
		for (let index = 0; index < 20; index++) {
			connectionEvents.dispatchEvent(new Event("connection:close"));
			onlineEvents.dispatchEvent(new Event("online"));
		}
		await clock.tickAsync(1_000);
		expect(attempts).to.equal(1);
		expect(clock.countTimers()).to.equal(0);

		rejectAttempt(new Error("offline"));
		await clock.tickAsync(0);
		expect(clock.countTimers()).to.equal(1);
		for (let index = 0; index < 20; index++) {
			connectionEvents.dispatchEvent(new Event("connection:close"));
		}
		expect(clock.countTimers()).to.equal(1);
	});

	it("preserves exponential backoff across online event storms", async () => {
		const onlineEvents = new EventTarget();
		const attemptTimes: number[] = [];
		controller = new BootstrapRecoveryController(
			makeRuntime({
				onlineEvents,
				connected: () => false,
				bootstrap: async () => {
					attemptTimes.push(Date.now());
					throw new Error("offline");
				},
			}),
			{
				initialDelayMs: 100,
				maxDelayMs: 800,
				backoffFactor: 2,
				cooldownMs: 0,
				jitter: 0,
			},
		);

		controller.start();
		await clock.tickAsync(0);
		await clock.tickAsync(10);
		for (let index = 0; index < 20; index++) {
			onlineEvents.dispatchEvent(new Event("online"));
		}
		await clock.tickAsync(89);
		expect(attemptTimes).to.deep.equal([0]);
		await clock.tickAsync(1);
		expect(attemptTimes).to.deep.equal([0, 100]);

		await clock.tickAsync(10);
		for (let index = 0; index < 20; index++) {
			onlineEvents.dispatchEvent(new Event("online"));
		}
		await clock.tickAsync(189);
		expect(attemptTimes).to.deep.equal([0, 100]);
		await clock.tickAsync(1);
		expect(attemptTimes).to.deep.equal([0, 100, 300]);
	});

	it("lets an event-emitting successful bootstrap finish", async () => {
		const connectionEvents = new EventTarget();
		let connected = false;
		let completed = false;
		let attemptSignal: AbortSignal | undefined;
		controller = new BootstrapRecoveryController(
			makeRuntime({
				connectionEvents,
				connected: () => connected,
				bootstrap: async (signal) => {
					attemptSignal = signal;
					connected = true;
					connectionEvents.dispatchEvent(new Event("connection:open"));
					await Promise.resolve();
					completed = true;
				},
			}),
			{ cooldownMs: 0 },
		);

		controller.start();
		await clock.tickAsync(0);
		expect(attemptSignal?.aborted).to.equal(false);
		expect(completed).to.equal(true);
	});

	it("aborts active work and removes listeners and timers on stop", async () => {
		const connectionEvents = new EventTarget();
		const onlineEvents = new EventTarget();
		let attempts = 0;
		let attemptSignal: AbortSignal | undefined;
		controller = new BootstrapRecoveryController(
			makeRuntime({
				connectionEvents,
				onlineEvents,
				connected: () => false,
				bootstrap: (signal) => {
					attempts += 1;
					attemptSignal = signal;
					return new Promise<void>((_resolve, reject) => {
						signal.addEventListener("abort", () => reject(signal.reason), {
							once: true,
						});
					});
				},
			}),
			{
				initialDelayMs: 100,
				maxDelayMs: 400,
				cooldownMs: 0,
				jitter: 0,
			},
		);

		controller.start();
		await clock.tickAsync(0);
		controller.stop();
		await clock.tickAsync(0);
		expect(attemptSignal?.aborted).to.equal(true);
		expect(clock.countTimers()).to.equal(0);

		connectionEvents.dispatchEvent(new Event("connection:close"));
		onlineEvents.dispatchEvent(new Event("online"));
		await clock.tickAsync(1_000);
		expect(attempts).to.equal(1);
	});
});

describe("Peerbit bootstrap recovery integration", () => {
	it("keeps recovery opt-in and supports the create-time policy", async () => {
		await expectRejectedWith(
			Peerbit.create({ bootstrapRecovery: { addresses: [] } }),
			"bootstrapRecovery.addresses must not be empty",
		);
		const disabled = await Peerbit.create();
		const enabled = await Peerbit.create({
			bootstrapRecovery: {
				addresses: [addressA],
				initialDelayMs: 100,
				maxDelayMs: 100,
			},
		});
		try {
			expect(disabled.bootstrapRecoveryEnabled).to.equal(false);
			expect(enabled.bootstrapRecoveryEnabled).to.equal(true);
			expect(() =>
				enabled.enableBootstrapRecovery({ initialDelayMs: 0 }),
			).to.throw(
				"bootstrapRecovery.initialDelayMs must be a finite number >= 1",
			);
			expect(enabled.bootstrapRecoveryEnabled).to.equal(true);
			enabled.disableBootstrapRecovery();
			expect(enabled.bootstrapRecoveryEnabled).to.equal(false);
		} finally {
			disabled.disableBootstrapRecovery();
			enabled.disableBootstrapRecovery();
			await disabled.stop();
			await enabled.stop();
		}
	});

	it("refreshes the default bootstrap list and rotates endpoints", async () => {
		const peer = await Peerbit.create();
		const originalFetch = globalThis.fetch;
		const requested: string[] = [];
		const dialed: string[] = [];
		let responseIndex = 0;
		const dialStub = sinon.stub(peer, "dial").callsFake(async (address) => {
			dialed.push(address.toString());
			throw new Error("offline");
		});
		globalThis.fetch = (async (input: string | URL | Request) => {
			requested.push(
				typeof input === "string"
					? input
					: input instanceof URL
						? input.toString()
						: input.url,
			);
			const address = responseIndex++ === 0 ? addressA : addressB;
			return new Response(`${address}\n`, { status: 200 });
		}) as typeof fetch;
		const clock = sinon.useFakeTimers({
			now: 0,
			shouldClearNativeTimers: true,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});

		try {
			peer.enableBootstrapRecovery({
				initialDelayMs: 100,
				maxDelayMs: 100,
				cooldownMs: 0,
				jitter: 0,
			});
			await clock.tickAsync(0);
			await clock.tickAsync(100);

			expect(requested).to.deep.equal([
				"https://bootstrap.peerbit.org/bootstrap-5.env",
				"https://bootstrap.peerbit.org/bootstrap-5.env",
			]);
			expect(dialed).to.deep.equal([addressA, addressB]);
		} finally {
			peer.disableBootstrapRecovery();
			clock.restore();
			globalThis.fetch = originalFetch;
			dialStub.restore();
			await peer.stop();
		}
	});

	it("serializes delayed cancellation before re-enabling", async () => {
		const peer = await Peerbit.create();
		const clock = sinon.useFakeTimers({
			now: 0,
			shouldClearNativeTimers: true,
			toFake: ["Date", "setTimeout", "clearTimeout"],
		});
		const attempts: Array<{
			addresses: Array<string>;
			signal: AbortSignal;
			release: () => void;
		}> = [];
		let active = 0;
		let maxActive = 0;
		const bootstrapStub = sinon.stub(peer, "bootstrap").callsFake(
			(addresses, options = {}) =>
				new Promise((resolve) => {
					active += 1;
					maxActive = Math.max(maxActive, active);
					let released = false;
					attempts.push({
						addresses: (addresses ?? []).map((address) => address.toString()),
						signal: options.signal!,
						release: () => {
							if (released) return;
							released = true;
							active -= 1;
							resolve({ connectedPeerIds: [], failures: [] });
						},
					});
				}),
		);

		try {
			peer.enableBootstrapRecovery({
				addresses: [addressA],
				initialDelayMs: 100,
				maxDelayMs: 100,
				cooldownMs: 0,
				jitter: 0,
			});
			await clock.tickAsync(0);
			expect(attempts).to.have.length(1);

			peer.disableBootstrapRecovery();
			peer.enableBootstrapRecovery({
				addresses: [addressB],
				initialDelayMs: 100,
				maxDelayMs: 100,
				cooldownMs: 0,
				jitter: 0,
			});
			expect(attempts[0]!.signal.aborted).to.equal(true);
			await clock.tickAsync(1_000);
			expect(attempts).to.have.length(1);

			attempts[0]!.release();
			await clock.tickAsync(0);
			expect(attempts).to.have.length(2);
			expect(attempts[1]!.addresses).to.deep.equal([addressB]);
			expect(maxActive).to.equal(1);
		} finally {
			peer.disableBootstrapRecovery();
			for (const attempt of attempts) attempt.release();
			await clock.tickAsync(0);
			clock.restore();
			bootstrapStub.restore();
			await peer.stop();
		}
	});

	(isNode ? it : it.skip)(
		"completes Peerbit bootstrap side effects after a real connection event",
		async function () {
			this.timeout(180_000);
			const bootstrapPeer = await Peerbit.create();
			const peer = await Peerbit.create();
			const setCandidatesSpy = sinon.spy(
				peer.services.pubsub,
				"setTopicRootCandidates",
			);

			try {
				peer.enableBootstrapRecovery({
					addresses: bootstrapPeer.getMultiaddrs(),
					initialDelayMs: 100,
					maxDelayMs: 100,
					cooldownMs: 0,
					jitter: 0,
				});
				await waitForResolved(
					() => {
						expect(peer.libp2p.getConnections(bootstrapPeer.peerId)).not.to.be
							.empty;
						expect(setCandidatesSpy.called).to.equal(true);
					},
					{ timeout: 30_000, delayInterval: 20 },
				);
				expect(setCandidatesSpy.lastCall.args[0]).to.deep.equal([
					bootstrapPeer.services.pubsub.publicKeyHash,
				]);
			} finally {
				peer.disableBootstrapRecovery();
				setCandidatesSpy.restore();
				await peer.stop();
				await bootstrapPeer.stop();
			}
		},
	);
});
