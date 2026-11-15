import { field, variant } from "@dao-xyz/borsh";
import type { DataMessage } from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { AbortError } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { RPC, type RPCResponse } from "../src/index.js";

@variant("rpc-physical-settlement-body")
class Body {
	@field({ type: "u8" })
	value = 1;
}

const turn = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
const observe = <T>(promise: Promise<T>) =>
	promise.then(
		(value) => ({ value, error: undefined as unknown }),
		(error: unknown) => ({ value: undefined as T | undefined, error }),
	);

const physicalObserver = () => {
	let promise: Promise<void> | undefined;
	let completed = false;
	let calls = 0;
	return {
		onPhysicalSettlement: (settled: Promise<void>) => {
			calls++;
			promise = settled;
			void settled.then(() => {
				completed = true;
			});
		},
		get promise() {
			if (!promise) throw new Error("Physical settlement was not observed");
			return promise;
		},
		get completed() {
			return completed;
		},
		get calls() {
			return calls;
		},
	};
};

describe("rpc request physical settlement", () => {
	let session: TestSession;
	let rpc: RPC<Body, Body>;
	beforeEach(async () => {
		session = await TestSession.disconnected(1);
		rpc = await session.peers[0].open(new RPC<Body, Body>(), {
			args: {
				topic: "rpc-physical-settlement",
				queryType: Body,
				responseType: Body,
			},
		});
	});
	afterEach(async () => {
		sinon.restore();
		await session.stop();
	});

	// Control only the physical transport boundary. The real request setup,
	// cancellation/deadline, resolver cleanup and settlement accounting run.
	for (const completion of ["response", "abort", "timeout"] as const) {
		it(`retains physical work after ${completion} until a non-cooperative publish settles`, async () => {
			const entered = pDefer<void>();
			const release = pDefer<Uint8Array | undefined>();
			const controller = new AbortController();
			const reason = new AbortError("caller cancellation");
			const physical = physicalObserver();
			let receive!: (response: RPCResponse<Body>) => void;
			let transportSignal: AbortSignal | undefined;
			sinon
				.stub(rpc.node.services.pubsub, "publish")
				.callsFake((_data, options) => {
					transportSignal = options?.signal;
					entered.resolve();
					return release.promise;
				});
			const result = observe(
				rpc.request(new Body(), {
					amount: 1,
					timeout: completion === "timeout" ? 25 : undefined,
					signal: controller.signal,
					onPhysicalSettlement: physical.onPhysicalSettlement,
					responseInterceptor: (callback) => {
						receive = callback;
					},
				}),
			);
			try {
				expect(physical.calls).to.equal(1); // Admission is synchronous.
				await entered.promise;
				if (completion === "abort") controller.abort(reason);
				if (completion === "response") {
					receive({
						from: rpc.node.identity.publicKey,
						response: new Body(),
						message: {} as DataMessage,
					});
				}
				const outcome = await result;
				expect(outcome.error).to.equal(
					completion === "abort" ? reason : undefined,
				);
				if (completion !== "abort")
					expect(outcome.value).to.have.length(
						completion === "response" ? 1 : 0,
					);
				expect((rpc as any)._responseResolver.size).to.equal(0);
				expect(transportSignal?.aborted).to.equal(true);
				await turn();
				expect(physical.completed).to.equal(false);
				// Late rejection must be owned and must not rewrite the logical result.
				release.reject(new Error("late physical rejection"));
				await physical.promise;
				await turn();
				expect(physical.completed).to.equal(true);
				expect(physical.calls).to.equal(1);
			} finally {
				controller.abort();
				release.resolve(undefined);
				await result;
				await physical.promise;
			}
		});
	}

	it("retains cancelled setup until sealing settles without publishing", async () => {
		const entered = pDefer<void>(),
			release = pDefer<void>();
		const controller = new AbortController();
		const reason = new AbortError("setup cancellation");
		const physical = physicalObserver();
		const internal = rpc as any;
		sinon.stub(internal, "seal").callsFake(async () => {
			entered.resolve();
			await release.promise;
			throw new Error("late setup failure");
		});
		const publish = sinon.spy(rpc.node.services.pubsub, "publish");
		const result = observe(
			rpc.request(new Body(), {
				signal: controller.signal,
				onPhysicalSettlement: physical.onPhysicalSettlement,
			}),
		);
		try {
			await entered.promise;
			controller.abort(reason);
			expect((await result).error).to.equal(reason);
			await turn();
			expect(physical.completed).to.equal(false);
			release.resolve();
			await physical.promise;
			await turn();
			expect(physical.completed).to.equal(true);
			expect(publish.notCalled).to.equal(true);
			expect((rpc as any)._responseResolver.size).to.equal(0);
		} finally {
			controller.abort();
			release.resolve();
			await result;
			await physical.promise;
		}
	});

	for (const failureAt of ["seal", "publish"] as const) {
		it(`settles physical accounting for a synchronous ${failureAt} failure`, async () => {
			const physical = physicalObserver();
			const reason = new Error(`${failureAt} failure`);
			if (failureAt === "seal") sinon.stub(rpc as any, "seal").throws(reason);
			else sinon.stub(rpc.node.services.pubsub, "publish").throws(reason);
			const result = await observe(
				rpc.request(new Body(), {
					onPhysicalSettlement: physical.onPhysicalSettlement,
				}),
			);
			expect(result.error).to.equal(reason);
			await physical.promise;
			expect(physical.completed).to.equal(true);
			expect(physical.calls).to.equal(1);
		});
	}

	for (const operation of ["send", "request"] as const) {
		it(`rechecks ${operation} ownership after asynchronous sealing before publishing`, async () => {
			const entered = pDefer<void>(),
				release = pDefer<void>();
			const physical = physicalObserver();
			const internal = rpc as any;
			const original = internal.seal.bind(internal);
			let current = true;
			sinon.stub(internal, "seal").callsFake(async (...args: unknown[]) => {
				entered.resolve();
				await release.promise;
				return original(...args);
			});
			const publish = sinon.spy(rpc.node.services.pubsub, "publish");
			const options = { isCurrent: () => current };
			const result = observe<void | RPCResponse<Body>[]>(
				operation === "send"
					? rpc.send(new Body(), options)
					: rpc.request(new Body(), {
							...options,
							onPhysicalSettlement: physical.onPhysicalSettlement,
						}),
			);
			try {
				await entered.promise;
				current = false;
				release.resolve();
				expect((await result).error).to.be.instanceOf(AbortError);
				expect(publish.notCalled).to.equal(true);
				if (operation === "request") {
					await physical.promise;
					expect(physical.completed).to.equal(true);
					expect((rpc as any)._responseResolver.size).to.equal(0);
				}
			} finally {
				release.resolve();
				await result;
				if (operation === "request") await physical.promise;
			}
		});
	}

	it("settles an already-aborted request without starting setup or publish", async () => {
		const physical = physicalObserver();
		const controller = new AbortController();
		const reason = new AbortError("already cancelled");
		controller.abort(reason);
		const seal = sinon.spy(rpc as any, "seal");
		const publish = sinon.spy(rpc.node.services.pubsub, "publish");
		const result = await observe(
			rpc.request(new Body(), {
				signal: controller.signal,
				onPhysicalSettlement: physical.onPhysicalSettlement,
			}),
		);
		expect(result.error).to.equal(reason);
		await physical.promise;
		expect(physical.calls).to.equal(1);
		expect(seal.notCalled).to.equal(true);
		expect(publish.notCalled).to.equal(true);
	});

	for (const failure of ["throw", "reject"] as const) {
		it(`does not change the request result when its observer ${failure}s`, async () => {
			const controller = new AbortController();
			const reason = new AbortError("original request result");
			controller.abort(reason);
			const result = await observe(
				rpc.request(new Body(), {
					signal: controller.signal,
					onPhysicalSettlement: () => {
						if (failure === "throw") throw new Error("observer failure");
						return Promise.reject(new Error("observer failure"));
					},
				}),
			);
			expect(result.error).to.equal(reason);
			await turn();
		});
	}
});
