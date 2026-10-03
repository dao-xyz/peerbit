import { multiaddr } from "@multiformats/multiaddr";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { expect } from "chai";
import { FanoutTree } from "../src/fanout-tree.js";

type FanoutServices = { fanout: FanoutTree };

const settlePromptly = async <T>(promise: Promise<T>): Promise<T> => {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<never>((_resolve, reject) => {
				timer = setTimeout(() => reject(new Error("dial did not settle")), 500);
			}),
		]);
	} finally {
		if (timer) clearTimeout(timer);
	}
};

const holdDial = (fanout: FanoutTree, cooperate = true) => {
	const manager = (fanout as any).components.connectionManager;
	const original = manager.openConnection;
	let signal: AbortSignal | undefined;
	let onAbort: (() => void) | undefined;
	let resolve!: (connection: unknown) => void;
	let reject!: (error: Error) => void;
	const pending = new Promise((res, rej) => {
		resolve = res;
		reject = rej;
	});
	// Cleanup can reject before a failed test has installed an awaiting caller.
	void pending.catch(() => {});
	let calls = 0;
	manager.openConnection = (
		_address: unknown,
		options?: { signal?: AbortSignal },
	) => {
		calls++;
		// Closing a provider handle may start a best-effort withdrawal dial.
		if (calls > 1) return Promise.reject(new Error("additional dial refused"));
		signal = options?.signal;
		if (cooperate && signal) {
			onAbort = () => reject(signal!.reason);
			if (signal.aborted) onAbort();
			else signal.addEventListener("abort", onAbort, { once: true });
		}
		return pending;
	};
	return {
		get signal() {
			return signal;
		},
		resolve,
		cleanup: () => {
			if (onAbort) signal?.removeEventListener("abort", onAbort);
			reject(new Error("test dial cleanup"));
			manager.openConnection = original;
		},
	};
};

describe("fanout-tree non-diagnostic dial bounds", () => {
	let session: TestSession<FanoutServices>;
	let fanout: FanoutTree;
	let dial: ReturnType<typeof holdDial> | undefined;
	let operation: Promise<unknown> | undefined;

	beforeEach(async () => {
		session = await TestSession.disconnected<FanoutServices>(1, {
			services: {
				fanout: (components) =>
					new FanoutTree(components, { connectionManager: false }),
			},
		});
		fanout = session.peers[0]!.services.fanout;
		const bootstraps = session.peers[0]!.getMultiaddrs().slice(0, 1);
		expect(bootstraps).to.have.length(1);
		fanout.setBootstraps(bootstraps);
		dial = undefined;
		operation = undefined;
	});

	afterEach(async () => {
		// Also release the intentionally non-cooperative dial on assertion failure.
		dial?.cleanup();
		await session.stop();
		await operation?.catch(() => {});
	});

	it("bounds a public provider announcement whose bootstrap dial waits for abort", async () => {
		dial = holdDial(fanout);
		operation = fanout.announceProvider("bounded-provider", {
			bootstrapDialTimeoutMs: 20,
		});
		expect(dial.signal, "bootstrap dial signal").to.exist;
		await settlePromptly(operation);
		expect(dial.signal!.aborted).to.equal(true);
	});

	it("bounds candidate connection attempts without join diagnostics", async () => {
		dial = holdDial(fanout);
		operation = (fanout as any).ensurePeerConnection(
			"unavailable-candidate",
			session.peers[0]!.getMultiaddrs().slice(0, 1),
			20,
			new AbortController().signal,
		);
		expect(dial.signal, "candidate dial signal").to.exist;
		expect(await settlePromptly(operation!)).to.equal(false);
		expect(dial.signal!.aborted).to.equal(true);
	});

	it("shares the bootstrap dial's abort budget with stream readiness", async () => {
		dial = holdDial(fanout, false);
		const internals = fanout as any;
		const originalWaitFor = internals.waitFor;
		let readinessSignal: AbortSignal | undefined;
		let onAbort: (() => void) | undefined;
		let rejectReadiness: (() => void) | undefined;
		let markReadiness!: () => void;
		const readinessStarted = new Promise<void>((resolve) => {
			markReadiness = resolve;
		});
		internals.waitFor = (_hash: string, options: { signal: AbortSignal }) => {
			readinessSignal = options.signal;
			markReadiness();
			return new Promise<never>((_resolve, reject) => {
				rejectReadiness = () => reject(new Error("test readiness cleanup"));
				onAbort = () => reject(options.signal.reason);
				if (options.signal.aborted) onAbort();
				else options.signal.addEventListener("abort", onAbort, { once: true });
			});
		};
		try {
			operation = fanout.announceProvider("readiness-budget", {
				bootstrapDialTimeoutMs: 20,
			});
			expect(dial.signal, "bootstrap dial signal").to.exist;
			dial.resolve({ remotePeer: session.peers[0]!.peerId });
			await settlePromptly(readinessStarted);
			// Readiness must inherit the already-running dial deadline, not start
			// a fresh timeout once openConnection completes.
			expect(readinessSignal).to.equal(dial.signal);
			await settlePromptly(operation);
			expect(readinessSignal!.aborted).to.equal(true);
		} finally {
			rejectReadiness?.();
			if (onAbort) readinessSignal?.removeEventListener("abort", onAbort);
			internals.waitFor = originalWaitFor;
		}
	});

	it("tries another bootstrap after a bounded dial fails", async () => {
		fanout.setBootstraps([
			multiaddr("/ip4/127.0.0.1/tcp/1"),
			multiaddr("/ip4/127.0.0.1/tcp/2"),
		]);
		dial = holdDial(fanout);
		const internals = fanout as any;
		const manager = internals.components.connectionManager;
		const firstDial = manager.openConnection;
		const originalWaitFor = internals.waitFor;
		const originalSend = internals._sendControlMany;
		const addresses: string[] = [];
		const signals: Array<AbortSignal | undefined> = [];
		const sentTo: string[][] = [];
		manager.openConnection = (
			address: { toString(): string },
			options?: { signal?: AbortSignal },
		) => {
			addresses.push(address.toString());
			signals.push(options?.signal);
			return addresses.length === 1
				? firstDial(address, options)
				: Promise.resolve({ remotePeer: session.peers[0]!.peerId });
		};
		internals.waitFor = async (): Promise<never[]> => [];
		internals._sendControlMany = async (peers: string[]) => {
			sentTo.push(peers);
		};
		try {
			operation = fanout.announceProvider("fallback-provider", {
				bootstrapDialTimeoutMs: 20,
				bootstrapMaxPeers: 1,
			});
			expect(dial.signal, "first bootstrap dial signal").to.exist;
			await settlePromptly(operation);
			expect(new Set(addresses).size).to.equal(2);
			expect(signals[0]!.aborted).to.equal(true);
			expect(signals[1]).to.exist.and.not.equal(signals[0]);
			expect(signals[1]!.aborted).to.equal(false);
			expect(sentTo).to.deep.equal([[fanout.publicKeyHash]]);
		} finally {
			internals.waitFor = originalWaitFor;
			internals._sendControlMany = originalSend;
		}
	});

	it("cancels an in-flight one-shot provider dial when the service stops", async () => {
		dial = holdDial(fanout);
		operation = fanout.announceProvider("stopped-provider", {
			bootstrapDialTimeoutMs: 60_000,
		});
		expect(dial.signal, "bootstrap dial signal").to.exist;
		await settlePromptly(Promise.all([fanout.stop(), operation]));
		expect(dial.signal!.aborted).to.equal(true);
	});

	it("cancels an in-flight provider loop dial when its handle closes", async () => {
		dial = holdDial(fanout);
		const handle = fanout.provide("closed-provider", {
			bootstrapDialTimeoutMs: 60_000,
			announceIntervalMs: 60_000,
		});
		const state = [...(fanout as any).providerAnnounceBySuffixKey.values()][0];
		operation = state.loop;
		expect(dial.signal, "bootstrap dial signal").to.exist;
		handle.close();
		await settlePromptly(operation!);
		expect(dial.signal!.aborted).to.equal(true);
	});

	it("does not send after a dial ignoring cancellation completes following stop", async () => {
		dial = holdDial(fanout, false);
		const internals = fanout as any;
		const originalSend = internals._sendControlMany;
		let sends = 0;
		internals._sendControlMany = async () => {
			sends++;
		};
		try {
			operation = fanout.announceProvider("late-provider", {
				bootstrapDialTimeoutMs: 60_000,
			});
			expect(dial.signal, "bootstrap dial signal").to.exist;
			await settlePromptly(fanout.stop());
			expect(dial.signal!.aborted).to.equal(true);
			// A signal does not force this mock to settle. Release it explicitly;
			// the completed dial must not publish into the closed generation.
			dial.resolve({ remotePeer: session.peers[0]!.peerId });
			await settlePromptly(operation);
			expect(sends).to.equal(0);
		} finally {
			internals._sendControlMany = originalSend;
		}
	});

	it("does not announce after a late dial completes for a closed provider handle", async () => {
		dial = holdDial(fanout, false);
		const internals = fanout as any;
		const originalSend = internals._sendControlMany;
		let sends = 0;
		internals._sendControlMany = async () => {
			sends++;
		};
		const handle = fanout.provide("late-closed-provider", {
			bootstrapDialTimeoutMs: 60_000,
			announceIntervalMs: 60_000,
		});
		const state = [...internals.providerAnnounceBySuffixKey.values()][0];
		operation = state.loop;
		try {
			expect(dial.signal, "bootstrap dial signal").to.exist;
			handle.close();
			expect(dial.signal!.aborted).to.equal(true);
			// waitFor can immediately return an empty admission snapshot. The
			// expired dial must be rejected before that fast path accepts it.
			dial.resolve({ remotePeer: session.peers[0]!.peerId });
			await settlePromptly(operation!);
			expect(sends).to.equal(0);
		} finally {
			handle.close();
			internals._sendControlMany = originalSend;
		}
	});
});
