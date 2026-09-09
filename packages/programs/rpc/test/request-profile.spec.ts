import { field, serialize, variant } from "@dao-xyz/borsh";
import {
	DecryptedThing,
	Ed25519Keypair,
	type MaybeEncrypted,
} from "@peerbit/crypto";
import type { DiagnosticEvent, DiagnosticSink } from "@peerbit/diagnostics";
import { type DataMessage, SilentDelivery } from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { AbortError, TimeoutError } from "@peerbit/time";
import { expect } from "chai";
import pDefer from "p-defer";
import sinon from "sinon";
import { RPC, type RPCResponse, ResponseV0 } from "../src/index.js";

@variant("rpc-profile-body")
class Body {
	@field({ type: Uint8Array })
	data = new Uint8Array([7, 11, 19]);
}

const turn = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
type Settled<T> = { value: T | undefined; error: unknown };
const settled = <T>(promise: Promise<T>): Promise<Settled<T>> =>
	promise.then<Settled<T>, Settled<T>>(
		(value) => ({ value, error: undefined }),
		(error: unknown) => ({ value: undefined, error }),
	);

describe("rpc request profiling", () => {
	let session: TestSession;
	let rpc: RPC<Body, Body>;
	let identities: Ed25519Keypair[];
	before(async () => {
		identities = await Promise.all(
			Array.from({ length: 18 }, () => Ed25519Keypair.create()),
		);
	});
	beforeEach(async () => {
		session = await TestSession.disconnected(1);
		rpc = await session.peers[0].open(new RPC<Body, Body>(), {
			args: {
				topic: "rpc-request-profile",
				queryType: Body,
				responseType: Body,
			},
		});
	});
	afterEach(async () => {
		sinon.restore();
		await session.stop();
	});
	const response = (index: number): RPCResponse<Body> => ({
		from: identities[index].publicKey,
		response: new Body(),
		message: {
			header: { signatures: { publicKeys: [identities[index].publicKey] } },
		} as unknown as DataMessage,
	});
	const mode = () =>
		new SilentDelivery({
			to: identities.slice(0, 2).map((key) => key.publicKey),
			redundancy: 1,
		});
	const terminal = (events: DiagnosticEvent[]) => {
		const terminals = events.filter(
			(event) => event.name === "rpc.request.settle",
		);
		expect(terminals).to.have.length(1);
		expect(events.at(-1)).to.equal(terminals[0]);
		return terminals[0].details!;
	};

	// Real request setup/accounting, controlled public response interceptor and
	// transport seam. These are scheduling controls, not an authenticated wire proof.
	for (const sinkKind of [
		"disabled",
		"recording",
		"throwing",
		"async-rejecting",
	] as const) {
		it(`preserves request/accounting order with a ${sinkKind} sink`, async () => {
			const events: DiagnosticEvent[] = [];
			const calls: string[] = [];
			const profile: DiagnosticSink | undefined =
				sinkKind === "disabled"
					? undefined
					: (event) => {
							events.push(event);
							if (sinkKind === "throwing") throw new Error("diagnostic-only");
							if (sinkKind === "async-rejecting")
								return Promise.reject(new Error("diagnostic-only"));
						};
			const published = pDefer<void>();
			const releasePublish = pDefer<Uint8Array | undefined>();
			let transportSignal: AbortSignal | undefined;
			const publish = sinon
				.stub(rpc.node.services.pubsub, "publish")
				.callsFake((_data, options) => {
					calls.push("publish");
					transportSignal = options?.signal;
					published.resolve();
					return releasePublish.promise;
				});
			let receive!: (value: RPCResponse<Body>) => void;
			const controller = new AbortController();
			const request = settled(
				rpc.request(new Body(), {
					profile,
					mode: mode(),
					amount: 1,
					signal: controller.signal,
					responseInterceptor: (callback) => {
						calls.push("interceptor");
						receive = callback;
					},
					onResponse: (_value, from) => {
						calls.push(
							`response:${identities.findIndex((key) => key.publicKey.equals(from!))}`,
						);
					},
				}),
			);
			try {
				await published.promise;
				receive(response(2)); // An untargeted identity never counts.
				receive(response(0));
				receive(response(0)); // Nor does a duplicate targeted response.
				let done = false;
				void request.then(() => {
					done = true;
				});
				await turn(); // Also proves async sink rejections are owned, without test-side catches.
				expect(done).to.equal(false); // amount:1 does not weaken the two-target obligation.
				receive(response(1));
				const result = await request;
				expect(result.error).to.equal(undefined);
				expect(
					result.value?.map((value) => value.from?.hashcode()),
				).to.deep.equal(
					identities.slice(0, 2).map((key) => key.publicKey.hashcode()),
				);
				expect(calls).to.deep.equal([
					"interceptor",
					"publish",
					"response:0",
					"response:1",
				]);
				expect(publish.calledOnce).to.equal(true);
				expect(transportSignal).not.to.equal(controller.signal);
				expect(transportSignal?.aborted).to.equal(true);
				expect(controller.signal.aborted).to.equal(false);
				if (profile) {
					expect(events.map((event) => event.name)).to.deep.equal([
						"rpc.request.setup",
						"rpc.request.setup",
						"rpc.request.publish",
						"rpc.request.response",
						"rpc.request.response",
						"rpc.request.settle",
					]);
					expect(terminal(events)).to.include({
						outcome: "fulfilled",
						reason: "responses",
						timeoutMs: 10_000,
						expectedResponders: 2,
						receivedResponses: 2,
						unresolvedResponders: 0,
					});
					expect(new Set(events.map((event) => event.traceId)).size).to.equal(
						1,
					);
					for (const event of events) {
						expect(event.component).to.equal("rpc");
						for (const value of Object.values(event.details ?? {}))
							expect(
								value === undefined ||
									["string", "number", "boolean"].includes(typeof value),
							).to.equal(true);
						expect(event).not.to.have.any.keys(
							"request",
							"response",
							"message",
							"payload",
						);
					}
				}
				const eventCount = events.length;
				releasePublish.resolve(undefined);
				await turn();
				expect(events).to.have.length(eventCount); // Late transport settlement emits nothing.
			} finally {
				controller.abort();
				releasePublish.resolve(undefined);
				await request;
				await releasePublish.promise;
			}
		});
	}

	it("separates a fulfilled publish from an actual partial response deadline", async () => {
		const events: DiagnosticEvent[] = [];
		const publish = sinon
			.stub(rpc.node.services.pubsub, "publish")
			.resolves(undefined);
		const result = await rpc.request(new Body(), {
			mode: mode(),
			timeout: 25,
			profile: (event) => events.push(event),
			responseInterceptor: (receive) => receive(response(0)),
		});
		expect(result).to.have.length(1);
		expect(publish.calledOnce).to.equal(true);
		expect(
			events
				.filter((event) => event.name === "rpc.request.publish")
				.map((event) => ({
					edge: event.details?.edge,
					outcome: event.details?.outcome,
				})),
		).to.deep.equal([
			{ edge: "start", outcome: undefined },
			{ edge: "end", outcome: "fulfilled" },
		]);
		const deadline = events.find(
			(event) => event.name === "rpc.request.deadline",
		)!;
		expect(deadline.details).to.include({
			timeoutMs: 25,
			expectedResponders: 2,
			receivedResponses: 1,
			unresolvedResponders: 1,
		});
		expect(terminal(events)).to.include({
			outcome: "partial",
			reason: "deadline",
			receivedResponses: 1,
			unresolvedResponders: 1,
		});
	});

	it("samples only the first sixteen admitted response identities", async () => {
		const events: DiagnosticEvent[] = [];
		sinon.stub(rpc.node.services.pubsub, "publish").resolves(undefined);
		const result = await rpc.request(new Body(), {
			mode: new SilentDelivery({
				to: identities.map((key) => key.publicKey),
				redundancy: 1,
			}),
			profile: (event) => events.push(event),
			responseInterceptor: (receive) =>
				identities.forEach((_key, index) => receive(response(index))),
		});
		expect(result).to.have.length(18);
		const admitted = events.filter(
			(event) => event.name === "rpc.request.response",
		);
		expect(admitted).to.have.length(18);
		for (const [index, event] of admitted.entries()) {
			expect(event.peer).to.equal(
				index < 16 ? identities[index].publicKey.hashcode() : undefined,
			);
			expect(event.details?.peerSampleOmitted).to.equal(index >= 16);
		}
		expect(terminal(events)).to.include({
			receivedResponses: 18,
			unresolvedResponders: 0,
		});
	});

	it("observes a synchronous publish rejection without changing its error", async () => {
		const events: DiagnosticEvent[] = [];
		const error = new Error("transport-control");
		sinon.stub(rpc.node.services.pubsub, "publish").throws(error);
		const result = await settled(
			rpc.request(new Body(), { profile: (event) => events.push(event) }),
		);
		expect(result.error).to.equal(error);
		expect(
			events.find(
				(event) =>
					event.name === "rpc.request.publish" && event.details?.edge === "end",
			)?.details?.outcome,
		).to.equal("rejected");
		expect(terminal(events)).to.include({ outcome: "failed", reason: "error" });
	});

	it("terminates an already-aborted request without setup or publication", async () => {
		const events: DiagnosticEvent[] = [];
		const controller = new AbortController();
		const reason = new TimeoutError("caller-timeout-not-rpc-deadline");
		controller.abort(reason);
		const publish = sinon.spy(rpc.node.services.pubsub, "publish");
		const result = await settled(
			rpc.request(new Body(), {
				signal: controller.signal,
				profile: (event) => events.push(event),
			}),
		);
		expect(result.error).to.equal(reason);
		expect(publish.notCalled).to.equal(true);
		expect(events).to.have.length(1);
		expect(terminal(events)).to.include({
			outcome: "aborted",
			reason: "signal",
		});
	});

	it("ends tracing on setup cancellation even when sealing settles later", async () => {
		const events: DiagnosticEvent[] = [];
		const entered = pDefer<void>(),
			release = pDefer<void>(),
			sealed = pDefer<void>();
		const controller = new AbortController();
		const reason = new AbortError("setup-control");
		const internal = rpc as any;
		const original = internal.seal.bind(internal);
		sinon.stub(internal, "seal").callsFake(async (...args: unknown[]) => {
			entered.resolve();
			await release.promise;
			try {
				return await original(...args);
			} finally {
				sealed.resolve();
			}
		});
		const publish = sinon.spy(rpc.node.services.pubsub, "publish");
		const result = settled(
			rpc.request(new Body(), {
				signal: controller.signal,
				profile: (event) => events.push(event),
			}),
		);
		try {
			await entered.promise;
			controller.abort(reason);
			expect((await result).error).to.equal(reason);
			expect(terminal(events)).to.include({
				outcome: "aborted",
				reason: "signal",
			});
			const count = events.length;
			release.resolve();
			await sealed.promise;
			await turn();
			expect(events).to.have.length(count);
			expect(publish.notCalled).to.equal(true);
			expect(internal._responseResolver.size).to.equal(0);
		} finally {
			controller.abort();
			release.resolve();
			await result;
			await sealed.promise;
		}
	});

	for (const lifecycle of ["close", "drop"] as const) {
		it(`preserves the first signal cause when ${lifecycle} follows in the same turn`, async () => {
			const events: DiagnosticEvent[] = [];
			const published = pDefer<void>();
			const controller = new AbortController();
			const reason = new AbortError("first-signal-cause");
			const listeners = sinon.spy(rpc.events, "addEventListener");
			sinon.stub(rpc.node.services.pubsub, "publish").callsFake(async () => {
				published.resolve();
				return undefined;
			});
			const result = settled(
				rpc.request(new Body(), {
					signal: controller.signal,
					profile: (event) => events.push(event),
				}),
			);
			try {
				await published.promise;
				// Invoke the actual registered lifecycle callback without claiming
				// that this controlled scheduling test closes or drops the program.
				const callback = listeners
					.getCalls()
					.find((call) => call.args[0] === lifecycle)!.args[1] as () => void;
				controller.abort(reason);
				callback();
				expect((await result).error).to.equal(reason);
				expect(terminal(events)).to.include({
					outcome: "aborted",
					reason: "signal",
				});
				expect((rpc as any)._responseResolver.size).to.equal(0);
			} finally {
				controller.abort();
				await result;
			}
		});
	}

	it("suppresses late diagnostics when a real response decode outlives cancellation", async () => {
		const events: DiagnosticEvent[] = [];
		const published = pDefer<void>(),
			entered = pDefer<void>(),
			release = pDefer<void>();
		const controller = new AbortController();
		const reason = new AbortError("decode-control");
		sinon.stub(rpc.node.services.pubsub, "publish").callsFake(async () => {
			published.resolve();
			return undefined;
		});
		const result = settled(
			rpc.request(new Body(), {
				mode: mode(),
				signal: controller.signal,
				profile: (event) => events.push(event),
			}),
		);
		let decoding: Promise<void> | undefined;
		try {
			await published.promise;
			const handler = (rpc as any)._responseResolver.values().next().value;
			const payload = new DecryptedThing<Uint8Array>({
				data: serialize(new Body()),
			});
			const original = payload.decrypt.bind(payload);
			sinon
				.stub(payload as MaybeEncrypted<Uint8Array>, "decrypt")
				.callsFake(async () => {
					entered.resolve();
					await release.promise;
					return original();
				});
			decoding = handler({
				response: new ResponseV0({
					requestId: new Uint8Array(32),
					response: payload,
				}),
				message: response(0).message,
			});
			await entered.promise;
			controller.abort(reason);
			expect((await result).error).to.equal(reason);
			expect(terminal(events)).to.include({
				outcome: "aborted",
				reason: "signal",
				receivedResponses: 0,
			});
			const count = events.length;
			release.resolve();
			await decoding;
			expect(events).to.have.length(count);
			expect((rpc as any)._responseResolver.size).to.equal(0);
		} finally {
			controller.abort();
			release.resolve();
			await result;
			await decoding;
		}
	});
});
