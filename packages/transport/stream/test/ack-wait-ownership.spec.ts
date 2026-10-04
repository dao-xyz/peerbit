import { ready } from "@peerbit/crypto";
import {
	AcknowledgeDelivery,
	DataMessage,
	DeliveryError,
	MessageHeader,
} from "@peerbit/stream-interface";
import { AbortError } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { Uint8ArrayList } from "uint8arraylist";
import { DirectStream } from "../src/index.js";
import { Routes } from "../src/routes.js";

// Import the implementation under test relatively, never through a workspace
// package link which can resolve to another checkout during isolated builds.
const createDeliveryPromise = Reflect.get(
	DirectStream.prototype,
	"createDeliveryPromise",
);
const key = (hash: string) => ({
	hashcode: () => hash,
	equals: (other: { hashcode(): string }) => other.hashcode() === hash,
});

const fixture = () => {
	const from = key("self");
	const target = key("target");
	const relay = { publicKey: key("relay") };
	const subject: any = Object.assign(new EventTarget(), {
		started: true,
		stopping: false,
		publicKey: from,
		publicKeyHash: "self",
		seekTimeout: 100,
		_ackCallbacks: new Map(),
		healthChecks: new Map(),
		peers: new Map([["relay", relay]]),
		routes: new Routes("self"),
		peerKeyHashToPublicKey: new Map(),
		onPeerUnreachable: sinon.spy(),
		onPeerReachable: sinon.spy(),
		modifySeenCache: sinon.stub().resolves(0),
		formatDeliveryDebugState: () => "test routes",
		waitForPeerWrite: sinon.stub().resolves(),
		createDeliveryPromise,
		clearHealthcheckTimer: Reflect.get(
			DirectStream.prototype,
			"clearHealthcheckTimer",
		),
		removePeerFromRoutes: DirectStream.prototype.removePeerFromRoutes,
		addRouteConnection: DirectStream.prototype.addRouteConnection,
	});
	subject.routes.updateSession("target", 1);
	subject.routes.add("self", "relay", "target", 0, 1, 1);
	let nextId = 1;
	const message = (redundancy = 1) => {
		const message = new DataMessage({
			header: new MessageHeader({
				id: new Uint8Array(32).fill(nextId++),
				session: 1,
				mode: new AcknowledgeDelivery({ to: ["target"], redundancy }),
			}),
		});
		// These tests start below signing/verification and isolate wait ownership.
		(message.header as any).signatures = { publicKeys: [from] };
		sinon
			.stub(message, "bytes")
			.returns(new Uint8ArrayList(new Uint8Array([1])));
		return message;
	};
	const begin = async (
		controller = new AbortController(),
		value = message(),
		relayed = false,
	) => {
		const wait = await createDeliveryPromise.call(
			subject,
			from,
			value,
			relayed,
			controller.signal,
		);
		const outcome = wait.promise.catch((error: unknown) => error);
		return { ...wait, outcome, controller, message: value };
	};
	return { subject, from, target, relay, message, begin };
};

describe("stream ACK wait ownership", () => {
	let clock: sinon.SinonFakeTimers;
	before(async () => ready);
	beforeEach(() => {
		clock = sinon.useFakeTimers();
	});
	afterEach(() => {
		sinon.restore();
	});

	it("does not let a cancelled wait prune a replacement indirect route", async () => {
		const { subject, begin } = fixture();
		const wait = await begin();
		wait.startTimeout();
		wait.controller.abort();
		expect(await wait.outcome).to.be.instanceOf(AbortError);
		// addPeer(target) would clear its timer and conceal this regression.
		subject.routes.add("self", "new-relay", "target", 0, 2, 2);
		expect(subject.routes.isReachable("self", "target")).to.equal(true);
		await clock.tickAsync(101);
		expect(subject.routes.isReachable("self", "target")).to.equal(true);
		expect(subject.onPeerUnreachable.notCalled).to.equal(true);
		expect(subject._ackCallbacks.size).to.equal(0);
		expect(subject.healthChecks.size).to.equal(0);
	});

	for (const unavailable of ["pre-aborted", "no peers"] as const) {
		it(`does not retain ACK or health state when ${unavailable}`, async () => {
			const { subject, begin } = fixture();
			const controller = new AbortController();
			if (unavailable === "pre-aborted") controller.abort();
			else subject.peers.clear();
			const result = await begin(controller).then(
				(wait) => wait.outcome,
				(error) => error,
			);
			expect(result).to.be.instanceOf(
				unavailable === "pre-aborted" ? AbortError : DeliveryError,
			);
			expect(subject._ackCallbacks.size).to.equal(0);
			expect(subject.healthChecks.size).to.equal(0);
			expect(clock.countTimers()).to.equal(0);
		});
	}

	it("rejects a pre-aborted same-ID attempt without clearing the original wait", async () => {
		const { subject, begin } = fixture();
		const original = await begin();
		const record = [...subject._ackCallbacks.values()][0];
		const health = subject.healthChecks.get("target");
		const controller = new AbortController();
		controller.abort();
		let rejection: unknown;
		let settled = false;
		const attempt = begin(controller, original.message)
			.then(
				(wait) => wait.outcome,
				(error) => error,
			)
			.then((error) => {
				settled = true;
				rejection = error;
			});
		await clock.tickAsync(0);
		try {
			expect(settled).to.equal(true);
			expect(rejection).to.be.instanceOf(AbortError);
			expect([...subject._ackCallbacks.values()]).to.deep.equal([record]);
			expect(subject.healthChecks.get("target")).to.equal(health);
		} finally {
			original.controller.abort();
			await original.outcome;
			await attempt;
		}
	});

	it("keeps a shared health check until its final wait is cancelled", async () => {
		const { subject, begin } = fixture();
		const first = await begin();
		const second = await begin();
		const health = subject.healthChecks.get("target");
		first.controller.abort();
		await first.outcome;
		expect(subject.healthChecks.get("target")).to.equal(health);
		expect(subject._ackCallbacks.size).to.equal(1);
		second.controller.abort();
		await second.outcome;
		expect(subject.healthChecks.size).to.equal(0);
		expect(subject._ackCallbacks.size).to.equal(0);
		expect(clock.countTimers()).to.equal(0);
	});

	it("still prunes for another active owner and removes the expired record", async () => {
		const { subject, begin } = fixture();
		const first = await begin();
		await clock.tickAsync(40);
		const second = await begin();
		second.startTimeout();
		first.controller.abort();
		await first.outcome;
		await clock.tickAsync(60);
		expect(subject.onPeerUnreachable.calledOnceWithExactly("target")).to.equal(
			true,
		);
		expect(subject.healthChecks.size).to.equal(0);
		await clock.tickAsync(40);
		expect(await second.outcome).to.be.instanceOf(DeliveryError);
		expect(subject._ackCallbacks.size).to.equal(0);
	});

	it("does not let old cleanup cancel a rearmed target health check", async () => {
		const { subject, begin } = fixture();
		const old = await begin();
		// A verified ACK or authenticated direct reconnection clears this generation.
		subject.clearHealthcheckTimer("target");
		const current = await begin();
		const health = subject.healthChecks.get("target");
		old.controller.abort();
		await old.outcome;
		expect(subject.healthChecks.get("target")).to.equal(health);
		current.controller.abort();
		await current.outcome;
		expect(subject.healthChecks.size).to.equal(0);
	});

	it("ignores a queued timeout after its health record was replaced", async () => {
		const timeouts = sinon.spy(globalThis, "setTimeout");
		const { subject, begin } = fixture();
		const old = await begin();
		const staleTimeout = timeouts.firstCall.args[0] as () => void;
		subject.clearHealthcheckTimer("target");
		const current = await begin();
		const health = subject.healthChecks.get("target");
		try {
			staleTimeout();
			expect(subject.onPeerUnreachable.notCalled).to.equal(true);
			expect(subject.healthChecks.get("target")).to.equal(health);
		} finally {
			old.controller.abort();
			current.controller.abort();
			await Promise.all([old.outcome, current.outcome]);
		}
	});

	it("does not clear a same-ID successor after a cancelled write fails late", async () => {
		const { subject, from, relay, message } = fixture();
		const blocked = pDefer<void>();
		const entered = pDefer<void>();
		const failure = new Error("late old write failure");
		subject.waitForPeerWrite.onFirstCall().callsFake(() => {
			entered.resolve();
			return blocked.promise;
		});
		const oldController = new AbortController();
		const currentController = new AbortController();
		const value = message();
		const publish = (signal: AbortSignal) =>
			DirectStream.prototype.publishMessage
				.call(subject, from as any, value, [relay as any], false, signal)
				.catch((error: unknown) => error);
		const old = publish(oldController.signal);
		await entered.promise;
		oldController.abort();
		expect(subject._ackCallbacks.size).to.equal(0);
		const current = publish(currentController.signal);
		await clock.tickAsync(0);
		const successor = [...subject._ackCallbacks.values()][0];
		expect(successor).to.exist;
		try {
			blocked.reject(failure);
			expect(await old).to.equal(failure);
			expect([...subject._ackCallbacks.values()]).to.deep.equal([successor]);
			expect(subject.healthChecks.size).to.equal(1);
		} finally {
			currentController.abort();
			await current;
		}
		expect(subject.healthChecks.size).to.equal(0);
	});

	it("does not restart timers when a cancelled write succeeds late", async () => {
		const { subject, from, relay, message } = fixture();
		const blocked = pDefer<void>();
		const entered = pDefer<void>();
		subject.waitForPeerWrite.callsFake(() => {
			entered.resolve();
			return blocked.promise;
		});
		const controller = new AbortController();
		const result = DirectStream.prototype.publishMessage
			.call(
				subject,
				from as any,
				message(),
				[relay as any],
				false,
				controller.signal,
			)
			.catch((error: unknown) => error);
		await entered.promise;
		controller.abort();
		expect(subject._ackCallbacks.size).to.equal(0);
		expect(subject.healthChecks.size).to.equal(0);
		blocked.resolve();
		expect(await result).to.be.instanceOf(AbortError);
		await clock.tickAsync(101);
		expect(clock.countTimers()).to.equal(0);
		expect(subject.onPeerUnreachable.notCalled).to.equal(true);
	});

	it("releases timers when a publish write fails without cancellation", async () => {
		const { subject, from, relay, message } = fixture();
		const failure = new Error("write failed");
		subject.waitForPeerWrite.rejects(failure);
		const result = await DirectStream.prototype.publishMessage
			.call(subject, from as any, message(), [relay as any], false)
			.catch((error: unknown) => error);
		expect(result).to.equal(failure);
		expect(subject._ackCallbacks.size).to.equal(0);
		expect(subject.healthChecks.size).to.equal(0);
		expect(clock.countTimers()).to.equal(0);
		await clock.tickAsync(101);
		expect(subject.onPeerUnreachable.notCalled).to.equal(true);
	});

	for (const redundancy of [1, 2]) {
		it(`preserves successful ACK and route-learning lifetime at redundancy ${redundancy}`, async () => {
			const { subject, target, relay, message, begin } = fixture();
			const wait = await begin(undefined, message(redundancy));
			wait.startTimeout();
			const record = [...subject._ackCallbacks.values()][0] as any;
			record.callback(
				{
					header: { signatures: { publicKeys: [target] }, session: 1 },
					seenCounter: 0,
				},
				relay,
			);
			expect(await wait.outcome).to.equal(undefined);
			expect(subject.healthChecks.size).to.equal(0);
			expect(subject._ackCallbacks.size).to.equal(redundancy === 1 ? 0 : 1);
			if (redundancy === 2) {
				record.callback(
					{
						header: { signatures: { publicKeys: [target] }, session: 1 },
						seenCounter: 1,
					},
					{ publicKey: key("second-relay") },
				);
				expect(
					subject.routes
						.findNeighbor("self", "target")
						.list.some((route: any) => route.hash === "second-relay"),
				).to.equal(true);
			}
			await clock.tickAsync(100);
			expect(subject._ackCallbacks.size).to.equal(0);
			expect(subject.onPeerUnreachable.notCalled).to.equal(true);
		});
	}

	it("does not allocate health checks for relayed ACK waits", async () => {
		const { subject, begin } = fixture();
		const wait = await begin(undefined, undefined, true);
		expect(subject.healthChecks.size).to.equal(0);
		wait.controller.abort();
		await wait.outcome;
		expect(subject._ackCallbacks.size).to.equal(0);
	});

	const directFixture = () => {
		const f = fixture();
		const old = { publicKey: f.target, isClosed: false, isWritable: true };
		f.subject.peers = new Map([["target", old]]);
		f.subject.routes = new Routes("self");
		f.subject.routes.updateSession("target", 1);
		f.subject.routes.add("self", "target", "target", 0, 1, 1);
		const controller = new AbortController();
		const publish = (value = f.message(), to?: any[], relayed = false) =>
			DirectStream.prototype.publishMessage
				.call(f.subject, f.from as any, value, to, relayed, controller.signal)
				.catch((error: unknown) => error);
		const replacement = () => {
			old.isClosed = true;
			old.isWritable = false;
			const current = {
				publicKey: f.target,
				isClosed: false,
				isWritable: true,
			};
			f.subject.peers.set("target", current);
			return current;
		};
		return { ...f, old, controller, publish, replacement };
	};

	it("retires only an unresolved delivery written to the replaced direct owner", async () => {
		const f = directFixture();
		const result = f.publish();
		await clock.tickAsync(0);
		const record = [...f.subject._ackCallbacks.values()][0] as any;
		expect(f.subject.waitForPeerWrite.firstCall.args[0]).to.equal(f.old);
		const current = f.replacement();
		record.onPeerReplacement(current);
		expect(await result).to.be.instanceOf(DeliveryError);
		expect(f.subject._ackCallbacks.size).to.equal(0);
		expect(f.subject.healthChecks.size).to.equal(0);
		expect(clock.countTimers()).to.equal(0);
		expect(f.subject.routes.isReachable("self", "target")).to.equal(true);
		expect(f.subject.onPeerUnreachable.called).to.equal(false);
	});

	it("ignores unrelated, non-writable and retired events but follows replacement chains", async () => {
		const f = directFixture();
		let settled = false;
		const result = f.publish().then((value) => {
			settled = true;
			return value;
		});
		await clock.tickAsync(0);
		const record = [...f.subject._ackCallbacks.values()][0] as any;
		const unrelated = { publicKey: key("other"), isWritable: true };
		f.subject.peers.set("other", unrelated);
		record.onPeerReplacement(unrelated);
		record.onPeerReplacement(f.old);
		f.subject.dispatchEvent(
			new CustomEvent("peer:unreachable", { detail: f.target }),
		);
		const intermediate = f.replacement();
		intermediate.isWritable = false;
		record.onPeerReplacement(intermediate);
		const current = f.replacement();
		intermediate.isClosed = true;
		intermediate.isWritable = true; // Late event from an already retired owner.
		record.onPeerReplacement(intermediate);
		await clock.tickAsync(0);
		expect(settled).to.equal(false);
		record.onPeerReplacement(current);
		expect(await result).to.be.instanceOf(DeliveryError);
	});

	for (const mode of [
		"explicit",
		"relay",
		"redundant",
		"indirect",
		"flood",
		"multiple",
	] as const) {
		it(`does not retire ${mode} delivery on a direct replacement`, async () => {
			const f = directFixture();
			if (mode === "indirect") {
				f.subject.peers.set("relay", f.relay);
				f.subject.routes = new Routes("self");
				f.subject.routes.add("self", "relay", "target", 0, 1, 1);
			}
			if (mode === "flood") f.subject.routes = new Routes("self");
			const value = f.message(mode === "redundant" ? 2 : 1);
			if (mode === "multiple")
				(value.header.mode as AcknowledgeDelivery).to.push("other");
			let settled = false;
			const result = f
				.publish(
					value,
					mode === "explicit" ? [f.old] : undefined,
					mode === "relay",
				)
				.then((value) => {
					settled = true;
					return value;
				});
			await clock.tickAsync(0);
			const record = [...f.subject._ackCallbacks.values()][0] as any;
			record.onPeerReplacement?.(f.replacement());
			await clock.tickAsync(0);
			expect(settled).to.equal(false);
			f.controller.abort();
			expect(await result).to.be.instanceOf(AbortError);
		});
	}

	it("cannot let a retired replacement callback clear a same-ID successor", async () => {
		const f = directFixture();
		const value = f.message();
		const result = f.publish(value);
		await clock.tickAsync(0);
		const oldRecord = [...f.subject._ackCallbacks.values()][0] as any;
		const current = f.replacement();
		oldRecord.onPeerReplacement(current);
		expect(await result).to.be.instanceOf(DeliveryError);
		const successor = f.publish(value);
		await clock.tickAsync(0);
		const newRecord = [...f.subject._ackCallbacks.values()][0];
		expect(newRecord).not.to.equal(oldRecord);
		oldRecord.onPeerReplacement(current);
		expect([...f.subject._ackCallbacks.values()]).to.deep.equal([newRecord]);
		f.controller.abort();
		expect(await successor).to.be.instanceOf(AbortError);
		expect(clock.countTimers()).to.equal(0);
	});

	it("does not turn an acknowledged direct delivery into a replacement failure", async () => {
		const f = directFixture();
		const result = f.publish();
		await clock.tickAsync(0);
		const record = [...f.subject._ackCallbacks.values()][0] as any;
		record.callback(
			{
				header: { signatures: { publicKeys: [f.target] }, session: 1 },
				seenCounter: 0,
			},
			f.old,
		);
		expect(await result).to.equal(undefined);
		record.onPeerReplacement(f.replacement());
		expect(f.subject._ackCallbacks.size).to.equal(0);
		expect(f.subject.healthChecks.size).to.equal(0);
		f.subject.routes.clear(); // ACK route learning has its own cleanup timer.
		expect(clock.countTimers()).to.equal(0);
	});

	for (const directFirst of [true, false]) {
		it(`preserves a same-ID wait shared with a viable explicit attempt (direct first: ${directFirst})`, async () => {
			const f = directFixture();
			const value = f.message();
			let settled = 0;
			const first = f
				.publish(value, directFirst ? undefined : [f.relay])
				.then((value) => {
					settled++;
					return value;
				});
			await clock.tickAsync(0);
			const second = f
				.publish(value, directFirst ? [f.relay] : undefined)
				.then((value) => {
					settled++;
					return value;
				});
			await clock.tickAsync(0);
			const record = [...f.subject._ackCallbacks.values()][0] as any;
			expect(f.subject._ackCallbacks.size).to.equal(1);
			record.onPeerReplacement(f.replacement());
			await clock.tickAsync(0);
			expect(settled).to.equal(0);
			// The relay can still deliver the valid acknowledgement for both callers.
			record.callback(
				{
					header: { signatures: { publicKeys: [f.target] }, session: 1 },
					seenCounter: 0,
				},
				f.relay,
			);
			expect(await Promise.all([first, second])).to.deep.equal([
				undefined,
				undefined,
			]);
			expect(f.subject._ackCallbacks.size).to.equal(0);
			f.subject.routes.clear();
			expect(clock.countTimers()).to.equal(0);
		});
	}
});
