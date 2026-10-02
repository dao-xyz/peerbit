import { AbortError, TimeoutError, delay } from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import type { ProgramClient } from "../src/index.js";
import { TestProgramWithTopics } from "./samples.js";
import { creatMockPeer } from "./utils.js";

describe("Program.waitFor neighbor cancellation", () => {
	let peers: ProgramClient[];
	let program: TestProgramWithTopics;
	let releases: (() => void)[];
	let pending: Promise<unknown>[];

	beforeEach(async () => {
		releases = [];
		pending = [];
		const state = {
			pubsubEventHandlers: new Map(),
			subsribers: new Map(),
			peers: new Map(),
		};
		peers = await Promise.all(
			Array.from({ length: 3 }, () => creatMockPeer(state)),
		);
		program = await peers[0].open(new TestProgramWithTopics());
		await Promise.all(peers.slice(1).map((peer) => peer.open(program.clone())));
	});

	afterEach(async () => {
		try {
			for (const release of releases) release();
			await Promise.all(pending);
		} finally {
			sinon.restore();
			await Promise.all(peers.map((peer) => peer.stop()));
		}
	});

	for (const reason of [new Error("caller cancelled readiness"), "cancelled"]) {
		it(`cancels every neighbor probe with ${reason instanceof Error ? "an Error" : "a non-Error"} reason`, async () => {
			const pubsub = peers[0].services.pubsub;
			const keys = peers.slice(1).map((peer) => peer.identity.publicKey);
			const hashes = keys.map((key) => key.hashcode());
			const controller = new AbortController();
			let allEntered!: () => void;
			const entered = new Promise<void>((resolve) => (allEntered = resolve));
			let probesStarted = 0;
			let probesSettled = 0;
			const request = sinon.spy(pubsub, "requestSubscribers");
			const listen = sinon.spy(pubsub, "addEventListener");
			const programListen = sinon.spy(program.events, "addEventListener");
			const wait = sinon
				.stub(pubsub, "waitFor")
				.callsFake(async (_other, options) => {
					if (options?.target !== "neighbor") return hashes;
					if (++probesStarted === hashes.length) allEntered();
					try {
						return await new Promise<string[]>((resolve, reject) => {
							const signal = options.signal;
							const abort = () => reject(signal?.reason);
							const cleanup = () => signal?.removeEventListener("abort", abort);
							signal?.addEventListener("abort", abort, { once: true });
							if (signal?.aborted) abort();
							releases.push(() => {
								cleanup();
								resolve(hashes);
							});
						});
					} finally {
						probesSettled++;
					}
				});
			let settled = false;
			const outcome = program
				.waitFor(keys, { signal: controller.signal, timeout: 100 })
				.then(
					(value) => ({ value }),
					(error: unknown) => ({ error }),
				)
				.then((result) => {
					settled = true;
					return result;
				});
			pending.push(outcome);
			await Promise.race([
				entered,
				outcome.then(() => {
					throw new Error("Wait settled before both neighbor probes started");
				}),
			]);
			controller.abort(reason);
			// One event-loop turn observes settlement without releasing the held
			// probes or waiting for their configured timeout.
			await delay(0);
			expect(settled).to.equal(true);
			expect(probesSettled).to.equal(hashes.length);
			if (reason instanceof Error) {
				expect(await outcome).to.have.property("error", reason);
			} else {
				expect(await outcome)
					.to.have.property("error")
					.instanceOf(AbortError);
			}
			expect(
				wait
					.getCalls()
					.slice(1)
					.every((call) => call.args[1]?.signal === controller.signal),
			).to.equal(true);
			expect(request.called).to.equal(false);
			expect(listen.called).to.equal(false);
			expect(programListen.called).to.equal(false);
		});
	}

	it("keeps a non-abort neighbor timeout best-effort for subscribed peers", async () => {
		const pubsub = peers[0].services.pubsub;
		const keys = peers.slice(1).map((peer) => peer.identity.publicKey);
		const hashes = keys.map((key) => key.hashcode());
		let probes = 0;
		sinon.stub(pubsub, "waitFor").callsFake(async (_other, options) => {
			if (options?.target === "neighbor") {
				probes++;
				throw new TimeoutError("no direct neighbor");
			}
			return hashes;
		});
		const controller = new AbortController();
		expect(
			await program.waitFor(keys, { signal: controller.signal, timeout: 100 }),
		).to.deep.equal(hashes);
		expect(probes).to.equal(hashes.length);
		expect(controller.signal.aborted).to.equal(false);
		for (const topic of program["getAllTopicsIncludingThis"]()) {
			const subscribers = await pubsub.getSubscribers(topic);
			for (const key of keys) {
				expect(
					subscribers?.some((subscriber) => subscriber.equals(key)),
				).to.equal(true);
			}
		}
	});
});
