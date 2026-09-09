import { TestSession } from "@peerbit/libp2p-test-utils";
import { AnyWhere } from "@peerbit/stream-interface";
import { AbortError } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { FanoutTree } from "../src/fanout-tree.js";

type Services = { fanout: FanoutTree };

describe("fanout-tree (route lookup lifecycle)", () => {
	let session: TestSession<Services>;
	let tree: FanoutTree;
	let state: any;
	let timers: sinon.SinonSpy;
	let clearTimer: sinon.SinonSpy;
	let send: sinon.SinonStub;
	let sendMany: sinon.SinonStub;
	let pending: Promise<unknown>[];
	let resolvers: Array<(route?: string[]) => void>;
	let controllers: AbortController[];
	const topic = "route-lookup-lifecycle";
	const timeoutMs = 5_000;
	const target = "route-target";
	const open = (root = false) => {
		const id = tree.openChannel(topic, tree.publicKeyHash, {
			role: "root",
			msgRate: 1,
			msgSize: 8,
			uploadLimitBps: 1_000_000,
			maxChildren: 2,
			repair: false,
		});
		state = (tree as any).channelsBySuffixKey.get(id.suffixKey);
		// Component-only ownership fixture: no join/readiness claim. Keep a real
		// service/channel and control only the route-query send boundary.
		state.isRoot = root;
		if (!root) state.parent = "route-parent";
		if (root) {
			state.children.set("route-child", { bidPerByte: 0 });
			tree.peers.set("route-child", { isWritable: true } as any);
		}
		return state;
	};
	const begin = (milliseconds = timeoutMs) => {
		const controller = new AbortController();
		controllers.push(controller);
		const add = sinon.spy(controller.signal, "addEventListener");
		const remove = sinon.spy(controller.signal, "removeEventListener");
		const result = tree.resolveRouteToken(topic, tree.publicKeyHash, target, {
			timeoutMs: milliseconds,
			signal: controller.signal,
		});
		// Attach rejection ownership immediately, including the negative control.
		pending.push(result.catch((): undefined => undefined));
		for (const entry of state.pendingRouteQuery.values()) {
			resolvers.push(entry.resolve);
		}
		return { result, controller, add, remove };
	};
	const routeTimers = () =>
		timers.getCalls().filter((call) => call.args[1] === timeoutMs);
	const released = (
		query: ReturnType<typeof begin>,
		handles = routeTimers(),
	) => {
		for (const call of handles) {
			expect(clearTimer.calledWith(call.returnValue), "owned timer released").to
				.be.true;
		}
		const listener = query.add
			.getCalls()
			.find((call) => call.args[0] === "abort");
		expect(listener, "abort listener was installed").to.exist;
		expect(query.remove.calledWith("abort", listener!.args[1])).to.be.true;
	};
	const promptly = async (result: Promise<unknown>) => {
		let deadline: ReturnType<typeof setTimeout> | undefined;
		try {
			return await Promise.race([
				result,
				new Promise<never>((_, reject) => {
					deadline = setTimeout(
						() => reject(new Error("lookup not settled")),
						250,
					);
				}),
			]);
		} finally {
			clearTimeout(deadline);
		}
	};

	beforeEach(async () => {
		session = await TestSession.disconnected<Services>(2, {
			services: {
				fanout: (components) =>
					new FanoutTree(components, { connectionManager: false }),
			},
		});
		tree = session.peers[0].services.fanout;
		open();
		pending = [];
		resolvers = [];
		controllers = [];
		timers = sinon.spy(globalThis, "setTimeout");
		clearTimer = sinon.spy(globalThis, "clearTimeout");
		send = sinon.stub(tree as any, "_sendControl").resolves();
		sendMany = sinon.stub(tree as any, "_sendControlMany").resolves();
	});

	afterEach(async () => {
		// Assertions inspect real scheduler ownership before cleanup. The baseline
		// drops resolver entries on close; retain callbacks only in this fixture so
		// failing controls settle without waiting five seconds or forcing an exit.
		for (const controller of controllers) controller.abort();
		for (const resolve of resolvers) resolve();
		for (const call of routeTimers()) {
			(call.args[0] as () => void)();
			clearTimeout(call.returnValue);
		}
		await Promise.all(pending);
		send.restore();
		sendMany.restore();
		timers.restore();
		clearTimer.restore();
		tree.peers.delete("route-child");
		await session.stop();
	});

	it("releases the non-root timeout and listener on caller abort", async () => {
		const query = begin();
		query.controller.abort();
		await expect(query.result).to.be.rejectedWith(AbortError);
		released(query);
		expect(state.pendingRouteQuery.size).to.equal(0);
	});

	it("does not start a lookup for an already aborted caller", async () => {
		const controller = new AbortController();
		controller.abort();
		const result = tree.resolveRouteToken(topic, tree.publicKeyHash, target, {
			timeoutMs: 40,
			signal: controller.signal,
		});
		pending.push(result.catch((): undefined => undefined));
		await expect(result).to.be.rejectedWith(AbortError);
		expect(send.called).to.be.false;
	});

	it("removes the abort listener when the real deadline expires", async () => {
		const query = begin(40);
		expect(await query.result).to.equal(undefined);
		released(
			query,
			timers.getCalls().filter((call) => call.args[1] === 40),
		);
		expect(state.pendingRouteQuery.size).to.equal(0);
	});

	for (const operation of ["detach", "kick", "close", "stop"] as const) {
		it(`settles a pending non-root lookup on ${operation}`, async () => {
			const remote = session.peers[1].services.fanout;
			if (operation === "kick") state.parent = remote.publicKeyHash;
			const query = begin();
			if (operation === "detach") (tree as any).detachFromParent(state);
			else if (operation === "kick") {
				const message = await remote.createMessage(
					(tree as any).codec.encodeKick(state.id.key),
					{ mode: new AnyWhere() },
				);
				await tree.onDataMessage(remote.publicKey, {} as any, message, 0);
			} else if (operation === "close") {
				await tree.closeChannel(topic, tree.publicKeyHash);
			} else await tree.stop();
			expect(await promptly(query.result)).to.equal(undefined);
			released(query);
			expect(state.pendingRouteQuery.size).to.equal(0);
		});
	}

	it("preserves active success, cache reuse and caller-owned route bytes", async () => {
		const query = begin();
		const entry = [...state.pendingRouteQuery.values()][0] as any;
		const route = [tree.publicKeyHash, target];
		entry.resolve(route);
		const result = await query.result;
		expect(result).to.deep.equal(route);
		expect(result).not.to.equal(route);
		released(query);
		expect(
			await tree.resolveRouteToken(topic, tree.publicKeyHash, target),
		).to.deep.equal(route);
		expect(send.callCount).to.equal(1);
	});

	it("preserves active send rejection while releasing lookup resources", async () => {
		const error = new Error("route send sentinel");
		send.rejects(error);
		const query = begin();
		await expect(query.result).to.be.rejectedWith(error);
		released(query);
		expect(state.pendingRouteQuery.size).to.equal(0);
	});

	it("old local callbacks and send rejection leave a replacement channel untouched", async () => {
		const gate = pDefer<void>();
		send.onFirstCall().returns(gate.promise);
		const oldState = state;
		const oldQuery = begin();
		const oldEntry = [...oldState.pendingRouteQuery.values()][0] as any;
		await tree.closeChannel(topic, tree.publicKeyHash);
		open();
		const replacement = begin();
		const currentEntry = [...state.pendingRouteQuery.values()][0] as any;
		try {
			oldEntry.resolve([tree.publicKeyHash, target]);
			gate.reject(new Error("old send completion"));
			expect(await promptly(oldQuery.result)).to.equal(undefined);
			await Promise.resolve();
			expect(state.pendingRouteQuery.size).to.equal(1);
			expect(state.routeByPeer.has(target)).to.be.false;
			expect(oldState.routeByPeer.has(target)).to.be.false;
			currentEntry.resolve([tree.publicKeyHash, target]);
			expect(await replacement.result).to.deep.equal([
				tree.publicKeyHash,
				target,
			]);
		} finally {
			gate.resolve();
		}
	});

	it("keeps coalesced root callers alive when the originating caller aborts", async () => {
		state.isRoot = true;
		state.children.set("route-child", { bidPerByte: 0 });
		tree.peers.set("route-child", { isWritable: true } as any);
		const first = begin();
		const second = begin();
		const id = [...state.pendingRouteProxy.keys()][0];
		expect(sendMany.callCount).to.equal(1);
		first.controller.abort();
		await expect(first.result).to.be.rejectedWith(AbortError);
		expect(state.pendingRouteProxy.size).to.equal(1);
		const route = [tree.publicKeyHash, "route-child", target];
		(tree as any).completeRouteProxy(state, id, route);
		expect(await second.result).to.deep.equal(route);
		released(first);
		released(second);
	});

	for (const operation of ["close", "stop"] as const) {
		it(`releases every coalesced caller backstop and listener on root ${operation}`, async () => {
			state.isRoot = true;
			state.children.set("route-child", { bidPerByte: 0 });
			tree.peers.set("route-child", { isWritable: true } as any);
			const first = begin();
			const second = begin();
			if (operation === "close")
				await tree.closeChannel(topic, tree.publicKeyHash, {
					kickChildren: false,
				});
			else {
				// The synthetic writable child has no transport lifetime to stop.
				tree.peers.delete("route-child");
				await tree.stop();
			}
			expect(
				await promptly(Promise.all([first.result, second.result])),
			).to.deep.equal([undefined, undefined]);
			released(first);
			released(second);
			expect(state.pendingRouteProxy.size).to.equal(0);
		});
	}

	it("does not allocate new waits after direct stop but preserves cached fast paths", async () => {
		const route = [tree.publicKeyHash, "cached-target"];
		(tree as any).cacheRoute(state, route);
		await tree.stop();
		const controller = new AbortController();
		controller.abort();
		expect(
			await tree.resolveRouteToken(topic, tree.publicKeyHash, "cached-target", {
				signal: controller.signal,
			}),
		).to.deep.equal(route);
		const query = begin();
		await expect(query.result).to.be.rejectedWith(AbortError);
		expect(routeTimers()).to.have.length(0);
		expect(send.called).to.be.false;
	});

	it("keeps a coalesced caller's shorter deadline independent of the shared search", async () => {
		state.isRoot = true;
		state.children.set("route-child", { bidPerByte: 0 });
		tree.peers.set("route-child", { isWritable: true } as any);
		const first = begin();
		const second = begin(40);
		expect(await second.result).to.equal(undefined);
		released(
			second,
			timers.getCalls().filter((call) => call.args[1] === 40),
		);
		expect(state.pendingRouteProxy.size).to.equal(1);
		const id = [...state.pendingRouteProxy.keys()][0];
		const route = [tree.publicKeyHash, "route-child", target];
		(tree as any).completeRouteProxy(state, id, route);
		expect(await first.result).to.deep.equal(route);
		released(first);
	});

	it("does not allocate a proxy for a remote query arriving after direct stop begins", async () => {
		state.isRoot = true;
		state.children.set("route-child", { bidPerByte: 0 });
		tree.peers.set("route-child", { isWritable: true } as any);
		const remote = session.peers[1].services.fanout;
		const message = await remote.createMessage(
			(tree as any).codec.encodeRouteQuery(state.id.key, 77, target),
			{ mode: new AnyWhere() },
		);
		const stopping = tree.stop();
		const timerCount = timers.callCount;
		const processing = tree.onDataMessage(
			remote.publicKey,
			{} as any,
			message,
			0,
		);
		// Remove the fixture-only writable snapshot before actual stream teardown.
		tree.peers.delete("route-child");
		try {
			expect(timers.callCount).to.equal(timerCount);
			await processing;
			expect(state.pendingRouteProxy.size).to.equal(0);
			expect(sendMany.called).to.be.false;
		} finally {
			(tree as any).clearRouteProxies(state);
			await processing;
			await stopping;
		}
	});

	it("stale proxy send and timer callbacks cannot complete a new search reusing a local id", async () => {
		state.isRoot = true;
		state.children.set("route-child", { bidPerByte: 0 });
		tree.peers.set("route-child", { isWritable: true } as any);
		const ids = sinon.stub(tree as any, "nextReqId").returns(123);
		const gate = pDefer<void>();
		sendMany.onFirstCall().returns(gate.promise);
		try {
			const first = begin();
			const oldTimer = routeTimers().at(-1)!;
			(tree as any).clearRouteProxies(state);
			expect(await first.result).to.equal(undefined);
			const second = begin();
			const current = state.pendingRouteProxy.get(123);
			(oldTimer.args[0] as () => void)();
			gate.reject(new Error("old proxy send"));
			await Promise.resolve();
			await Promise.resolve();
			expect(state.pendingRouteProxy.get(123)).to.equal(current);
			expect(state.metrics.routeProxyTimeouts).to.equal(0);
			const route = [tree.publicKeyHash, "route-child", target];
			(tree as any).completeRouteProxy(state, 123, route);
			expect(await second.result).to.deep.equal(route);
		} finally {
			gate.resolve();
			ids.restore();
		}
	});
});
