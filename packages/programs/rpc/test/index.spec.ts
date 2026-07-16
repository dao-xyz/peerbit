import {
	BorshError,
	deserialize,
	field,
	serialize,
	variant,
	vec,
} from "@dao-xyz/borsh";
import {
	Ed25519Keypair,
	PublicSignKey,
	getPublicKeyFromPeerId,
} from "@peerbit/crypto";
import { Program } from "@peerbit/program";
import { type PeerId } from "@peerbit/pubsub";
import {
	CONVERGENCE_MESSAGE_PRIORITY,
	FOREGROUND_READ_MESSAGE_PRIORITY,
	SilentDelivery,
} from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import {
	AbortError,
	TimeoutError,
	delay,
	waitFor,
	waitForResolved,
} from "@peerbit/time";
import { expect } from "chai";
import sinon from "sinon";
import {
	type CodecErrorEvent,
	MissingResponsesError,
	RPC,
	type RPCResponse,
	type RequestEvent,
	type ResponseEvent,
	queryAll,
} from "../src/index.js";

@variant("payload")
class Body {
	@field({ type: Uint8Array })
	arr!: Uint8Array;
	constructor(properties?: { arr: Uint8Array }) {
		if (properties) {
			this.arr = properties.arr;
		}
	}
}

@variant("rpc-test")
class RPCTest extends Program {
	@field({ type: RPC })
	query!: RPC<Body, Body>;

	@field({ type: vec(PublicSignKey) })
	responders: PublicSignKey[];

	delay: number | undefined;
	resolveRequestHook: ((message: any) => Body | undefined) | undefined;

	constructor(responders: PeerId[]) {
		super();
		this.responders = responders.map((x) =>
			x instanceof PublicSignKey ? x : getPublicKeyFromPeerId(x),
		);
	}

	async open(): Promise<void> {
		await this.query.open({
			topic: "topic",
			responseType: Body,
			queryType: Body,
			resolveRequest: this.resolveRequestHook,
			responseHandler: this.responders.find((x) =>
				this.node.identity.publicKey.equals(x),
			)
				? async (query, _from) => {
						if (this.delay) {
							const controller = new AbortController();
							this.events.addEventListener("close", () => {
								controller.abort(new AbortError("Closed"));
							});
							this.events.addEventListener("drop", () => {
								controller.abort(new AbortError("Dropped"));
							});
							await delay(this.delay, { signal: controller.signal });
						}
						const resp = query;
						return resp;
					}
				: undefined,
		});
	}
}

describe("rpc", () => {
	describe("request", () => {
		let session: TestSession, responder: RPCTest, reader: RPCTest;
		beforeEach(async () => {
			session = await TestSession.connected(3);
			//await delay(2000)

			responder = new RPCTest([session.peers[0].peerId]);
			responder.query = new RPC();

			await session.peers[0].open(responder);

			reader = deserialize(serialize(responder), RPCTest);
			await session.peers[1].open(reader);

			await reader.waitFor(session.peers[0].peerId);
			await responder.waitFor(session.peers[1].peerId);
		});
		afterEach(async () => {
			await reader.close();
			await responder.close();
			await session.stop();
		});

		it("any", async () => {
			const requestEventFromResponder: CustomEvent<RequestEvent<Body>>[] = [];
			const responseEventsFromResponder: CustomEvent<ResponseEvent<Body>>[] =
				[];

			responder.query.events.addEventListener("request", (e) => {
				requestEventFromResponder.push(e);
			});
			responder.query.events.addEventListener("response", (e) => {
				responseEventsFromResponder.push(e);
			});

			const requestEventFromRequester: CustomEvent<RequestEvent<Body>>[] = [];
			const responseEventsFromRequester: CustomEvent<ResponseEvent<Body>>[] =
				[];

			reader.query.events.addEventListener("request", (e) => {
				requestEventFromRequester.push(e);
			});

			reader.query.events.addEventListener("response", (e) => {
				responseEventsFromRequester.push(e);
			});

			let results: RPCResponse<Body>[] = await reader.query.request(
				new Body({
					arr: new Uint8Array([0, 1, 2]),
				}),
				{ amount: 1 },
			);

			await waitForResolved(() => expect(results).to.have.length(1));
			expect(results[0].from?.hashcode()).equal(
				responder.node.identity.publicKey.hashcode(),
			);

			expect(results[0].response.arr).to.deep.equal(new Uint8Array([0, 1, 2]));
			expect(requestEventFromResponder).to.have.length(1);
			expect(responseEventsFromResponder).to.have.length(0);
			expect(responseEventsFromRequester).to.have.length(1);
			expect(requestEventFromRequester).to.have.length(0);
			expect(requestEventFromResponder[0].detail.request.arr).to.deep.equal(
				new Uint8Array([0, 1, 2]),
			);

			let from =
				requestEventFromResponder[0].detail.message.header.signatures!
					.publicKeys[0]!;
			expect(from.hashcode()).equal(reader.node.identity.publicKey.hashcode());

			expect(responseEventsFromRequester[0].detail.response.arr).to.deep.equal(
				new Uint8Array([0, 1, 2]),
			);

			from =
				responseEventsFromRequester[0].detail.message.header.signatures!
					.publicKeys[0]!;
			expect(from.hashcode()).equal(
				responder.node.identity.publicKey.hashcode(),
			);
		});

		it("surfaces response serialization failures as codecError", async () => {
			const codecErrors: CustomEvent<CodecErrorEvent>[] = [];
			responder.query.events.addEventListener("codecError", (e) => {
				codecErrors.push(e as CustomEvent<CodecErrorEvent>);
			});

			// Make the responder produce a response borsh cannot serialize.
			// This used to be swallowed as "message for a different namespace"
			// and the requester would just time out with no trace anywhere.
			(responder.query as any)._responseHandler = async () => {
				const broken = new Body({ arr: new Uint8Array([1]) });
				(broken as any).arr = undefined; // violates the borsh schema
				return broken;
			};

			const results = await reader.query.request(
				new Body({ arr: new Uint8Array([0]) }),
				{ timeout: 2_000 },
			);
			expect(results).to.have.length(0);
			await waitForResolved(() => expect(codecErrors).to.have.length(1));
			expect(codecErrors[0].detail.stage).to.equal("encode-response");
			expect(codecErrors[0].detail.error).to.be.instanceOf(BorshError);

			// The responder must stay usable after the failure.
			(responder.query as any)._responseHandler = async (q: Body) => q;
			const ok = await reader.query.request(
				new Body({ arr: new Uint8Array([7]) }),
				{ amount: 1 },
			);
			expect(ok).to.have.length(1);
		});

		it("falls back to the decode path when resolveRequest throws", async () => {
			// A throwing resolveRequest hook must not drop the message; it
			// falls back to the normal decrypt + deserialize path so the
			// responder still answers.
			let hookInvoked = 0;
			// Replace the beforeEach-opened pair with a responder whose
			// resolveRequest hook throws.
			await reader.close();
			await responder.close();
			responder = new RPCTest([session.peers[0].peerId]);
			responder.query = new RPC();
			responder.resolveRequestHook = () => {
				hookInvoked++;
				throw new Error("resolve hook boom");
			};
			await session.peers[0].open(responder);

			reader = deserialize(serialize(responder), RPCTest);
			await session.peers[1].open(reader);
			await reader.waitFor(session.peers[0].peerId);
			await responder.waitFor(session.peers[1].peerId);

			const results = await reader.query.request(
				new Body({ arr: new Uint8Array([9, 8, 7]) }),
				{ amount: 1 },
			);
			await waitForResolved(() => expect(results).to.have.length(1));
			expect(results[0].response.arr).to.deep.equal(
				new Uint8Array([9, 8, 7]),
			);
			expect(hookInvoked).to.be.greaterThan(0);
		});

		it("inherits response transport hints from request envelope", async () => {
			const requestEventFromResponder: CustomEvent<RequestEvent<Body>>[] = [];
			const responseEventsFromRequester: CustomEvent<ResponseEvent<Body>>[] =
				[];

			responder.query.events.addEventListener("request", (e) => {
				requestEventFromResponder.push(e);
			});

			reader.query.events.addEventListener("response", (e) => {
				responseEventsFromRequester.push(e);
			});

			await reader.query.request(
				new Body({
					arr: new Uint8Array([0, 1, 2]),
				}),
				{
					amount: 1,
					priority: CONVERGENCE_MESSAGE_PRIORITY,
					responsePriority: FOREGROUND_READ_MESSAGE_PRIORITY,
					expiresAt: Date.now() + 5_000,
				},
			);

			await waitForResolved(() => expect(requestEventFromResponder).to.have.length(1));
			await waitForResolved(() =>
				expect(responseEventsFromRequester).to.have.length(1),
			);

			const requestMessage = requestEventFromResponder[0]!.detail.message;
			const responseMessage = responseEventsFromRequester[0]!.detail.message;
			expect(requestMessage.header.priority).to.equal(
				CONVERGENCE_MESSAGE_PRIORITY,
			);
			expect(requestMessage.header.responsePriority).to.equal(
				FOREGROUND_READ_MESSAGE_PRIORITY,
			);
			expect(responseMessage.header.priority).to.equal(
				FOREGROUND_READ_MESSAGE_PRIORITY,
			);
			expect(Number(responseMessage.header.expires)).to.equal(
				Number(requestMessage.header.expires),
			);
		});

		it("ignores duplicate responses from an expected responder", () => {
			const from = responder.node.identity.publicKey;
			const allResults: RPCResponse<Body>[] = [];
			const responders = new Set<string>();
			const expectedResponders = new Set<string>([from.hashcode()]);
			const deferred = {
				resolve: sinon.spy(),
				reject: sinon.spy(),
				promise: Promise.resolve(),
			};
			const onResponse = sinon.spy();
			const decoded = {
				response: new Body({ arr: new Uint8Array([1, 2, 3]) }),
				from,
				message: undefined as any,
			};

			(reader.query as any).handleDecodedResponse(
				decoded,
				deferred,
				allResults,
				responders,
				expectedResponders,
				{ onResponse },
			);
			(reader.query as any).handleDecodedResponse(
				decoded,
				deferred,
				allResults,
				responders,
				expectedResponders,
				{ onResponse },
			);

			expect(allResults).to.have.length(1);
			expect(onResponse.calledOnce).to.be.true;
			expect(deferred.resolve.calledOnce).to.be.true;
		});

		it("bounds slow publishes by the request timeout", async () => {
			let observedSignal: AbortSignal | undefined;
			const publishStub = sinon
				.stub(reader.node.services.pubsub, "publish")
				.callsFake((_data, options?: { signal?: AbortSignal }) => {
					observedSignal = options?.signal;
					return new Promise<Uint8Array | undefined>((_resolve, reject) => {
						observedSignal?.addEventListener(
							"abort",
							() => {
								reject(observedSignal?.reason ?? new AbortError("Aborted"));
							},
							{ once: true },
						);
					});
				});

			try {
				const started = Date.now();
				const result = await reader.query.request(
					new Body({ arr: new Uint8Array([1, 2, 3]) }),
					{ timeout: 25 },
				);

				expect(result).to.deep.equal([]);
				expect(observedSignal?.aborted).to.be.true;
				expect(Date.now() - started).to.be.lessThan(1_000);
			} finally {
				publishStub.restore();
			}
		});

		it("ignores a synchronous transport TimeoutError", async () => {
			const publishTimeout = new TimeoutError("SYNCHRONOUS_PUBLISH_TIMEOUT");
			const publishStub = sinon
				.stub(reader.node.services.pubsub, "publish")
				.throws(publishTimeout);

			try {
				const result = await reader.query.request(
					new Body({ arr: new Uint8Array([1, 2, 3]) }),
					{ timeout: 25 },
				);

				expect(result).to.deep.equal([]);
				expect(publishStub.calledOnce).to.be.true;
			} finally {
				publishStub.restore();
			}
		});

		it("preserves an already-aborted request signal", async () => {
			const controller = new AbortController();
			const reason = new AbortError("ALREADY_ABORTED");
			controller.abort(reason);
			const publishSpy = sinon.spy(reader.node.services.pubsub, "publish");

			try {
				let failure: unknown;
				try {
					await reader.query.request(
						new Body({ arr: new Uint8Array([1, 2, 3]) }),
						{ signal: controller.signal, timeout: 5_000 },
					);
				} catch (error) {
					failure = error;
				}

				expect(failure).to.equal(reason);
				expect(publishSpy.notCalled).to.be.true;
			} finally {
				publishSpy.restore();
			}
		});

		it("observes a request signal aborted during envelope setup", async () => {
			const controller = new AbortController();
			const reason = new AbortError("ABORTED_DURING_SETUP");
			const query = reader.query as any;
			const originalSeal = query.seal.bind(query);
			const sealStub = sinon
				.stub(query, "seal")
				.callsFake(async (...args: any[]) => {
					const sealed = await originalSeal(...args);
					controller.abort(reason);
					return sealed;
				});
			const publishSpy = sinon.spy(reader.node.services.pubsub, "publish");
			const removeListenerSpy = sinon.spy(
				controller.signal,
				"removeEventListener",
			);
			const resolverCountBefore = query._responseResolver.size;

			try {
				let failure: unknown;
				try {
					await reader.query.request(
						new Body({ arr: new Uint8Array([1, 2, 3]) }),
						{ signal: controller.signal, timeout: 5_000 },
					);
				} catch (error) {
					failure = error;
				}

				expect(failure).to.equal(reason);
				expect(publishSpy.notCalled).to.be.true;
				expect(query._responseResolver.size).to.equal(resolverCountBefore);
				expect(removeListenerSpy.calledWith("abort")).to.be.true;
			} finally {
				removeListenerSpy.restore();
				publishSpy.restore();
				sealStub.restore();
			}
		});

		it("settles an abort while request envelope setup remains pending", async () => {
			const controller = new AbortController();
			const reason = new AbortError("ABORTED_WHILE_SETUP_PENDING");
			const query = reader.query as any;
			const originalSeal = query.seal.bind(query);
			let resolveSetupStarted!: () => void;
			const setupStarted = new Promise<void>((resolve) => {
				resolveSetupStarted = resolve;
			});
			let releaseSetup!: () => void;
			const setupRelease = new Promise<void>((resolve) => {
				releaseSetup = resolve;
			});
			const sealStub = sinon
				.stub(query, "seal")
				.callsFake(async (...args: any[]) => {
					resolveSetupStarted();
					await setupRelease;
					return originalSeal(...args);
				});
			const publishSpy = sinon.spy(reader.node.services.pubsub, "publish");

			const request = reader.query.request(
				new Body({ arr: new Uint8Array([1, 2, 3]) }),
				{ signal: controller.signal, timeout: 5_000 },
			);
			try {
				await setupStarted;
				controller.abort(reason);
				const failure = await Promise.race([
					request.then(
						(): undefined => undefined,
						(error: unknown) => error,
					),
					delay(500).then(() => new Error("SETUP_ABORT_DID_NOT_SETTLE")),
				]);
				expect(failure).to.equal(reason);
				expect(publishSpy.notCalled).to.be.true;
			} finally {
				releaseSetup();
				await request.catch((): undefined => undefined);
				publishSpy.restore();
				sealStub.restore();
			}
		});

		it("preserves a TimeoutError used as the request abort reason", async () => {
			const controller = new AbortController();
			const reason = new TimeoutError("ABORT_TIMEOUT_REASON");
			const publishSpy = sinon.spy(reader.node.services.pubsub, "publish");

			try {
				let failure: unknown;
				try {
					await reader.query.request(
						new Body({ arr: new Uint8Array([1, 2, 3]) }),
						{
							signal: controller.signal,
							timeout: 5_000,
							responseInterceptor: () => controller.abort(reason),
						},
					);
				} catch (error) {
					failure = error;
				}

				expect(failure).to.equal(reason);
				expect(publishSpy.notCalled).to.be.true;
			} finally {
				publishSpy.restore();
			}
		});

		it("custom signer", async () => {
			const requestEventFromResponder: CustomEvent<RequestEvent<Body>>[] = [];
			const responseEventsFromResponder: CustomEvent<ResponseEvent<Body>>[] =
				[];

			responder.query.events.addEventListener("request", (e) => {
				requestEventFromResponder.push(e);
			});
			responder.query.events.addEventListener("response", (e) => {
				responseEventsFromResponder.push(e);
			});

			const requestEventFromRequester: CustomEvent<RequestEvent<Body>>[] = [];
			const responseEventsFromRequester: CustomEvent<ResponseEvent<Body>>[] =
				[];

			reader.query.events.addEventListener("request", (e) => {
				requestEventFromRequester.push(e);
			});

			reader.query.events.addEventListener("response", (e) => {
				responseEventsFromRequester.push(e);
			});

			const keypair1 = await Ed25519Keypair.create();
			const keypair2 = await Ed25519Keypair.create();

			let results: RPCResponse<Body>[] = await reader.query.request(
				new Body({
					arr: new Uint8Array([0, 1, 2]),
				}),
				{
					amount: 1,
					extraSigners: [
						keypair1.sign.bind(keypair1),
						keypair2.sign.bind(keypair2),
					],
				},
			);

			await waitForResolved(() => expect(results).to.have.length(1));
			const expectedSigner = [
				reader.node.identity.publicKey.hashcode(),
				keypair1.publicKey.hashcode(),
				keypair2.publicKey.hashcode(),
			];
			expect(
				results[0].message.header.signatures!.publicKeys.map((x) =>
					x.hashcode(),
				),
			).deep.equal([responder.node.identity.publicKey.hashcode()]);

			expect(results[0].response.arr).to.deep.equal(new Uint8Array([0, 1, 2]));
			expect(requestEventFromResponder).to.have.length(1);
			expect(responseEventsFromResponder).to.have.length(0);
			expect(responseEventsFromRequester).to.have.length(1);
			expect(requestEventFromRequester).to.have.length(0);
			expect(requestEventFromResponder[0].detail.request.arr).to.deep.equal(
				new Uint8Array([0, 1, 2]),
			);

			let from =
				requestEventFromResponder[0].detail.message.header.signatures!.publicKeys.map(
					(x) => x.hashcode(),
				);
			expect(from).to.deep.eq(expectedSigner);

			expect(responseEventsFromRequester[0].detail.response.arr).to.deep.equal(
				new Uint8Array([0, 1, 2]),
			);

			from =
				responseEventsFromRequester[0].detail.message.header.signatures!.publicKeys.map(
					(x) => x.hashcode(),
				);
			expect(from).to.deep.eq([responder.node.identity.publicKey.hashcode()]);
		});

		it("send with custom signer", async () => {
			const requestEventFromResponder: CustomEvent<RequestEvent<Body>>[] = [];
			const responseEventsFromResponder: CustomEvent<ResponseEvent<Body>>[] =
				[];

			responder.query.events.addEventListener("request", (e) => {
				requestEventFromResponder.push(e);
			});
			responder.query.events.addEventListener("response", (e) => {
				responseEventsFromResponder.push(e);
			});

			const requestEventFromRequester: CustomEvent<RequestEvent<Body>>[] = [];
			const responseEventsFromRequester: CustomEvent<ResponseEvent<Body>>[] =
				[];

			reader.query.events.addEventListener("request", (e) => {
				requestEventFromRequester.push(e);
			});

			reader.query.events.addEventListener("response", (e) => {
				responseEventsFromRequester.push(e);
			});

			const keypair1 = await Ed25519Keypair.create();
			const keypair2 = await Ed25519Keypair.create();

			await reader.query.send(
				new Body({
					arr: new Uint8Array([0, 1, 2]),
				}),
				{
					extraSigners: [
						keypair1.sign.bind(keypair1),
						keypair2.sign.bind(keypair2),
					],
				},
			);

			const expectedSigner = [
				reader.node.identity.publicKey.hashcode(),
				keypair1.publicKey.hashcode(),
				keypair2.publicKey.hashcode(),
			];

			await waitForResolved(() =>
				expect(requestEventFromResponder).to.have.length(1),
			);
			expect(responseEventsFromResponder).to.have.length(0);
			expect(responseEventsFromRequester).to.have.length(0);
			expect(requestEventFromRequester).to.have.length(0);
			expect(requestEventFromResponder[0].detail.request.arr).to.deep.equal(
				new Uint8Array([0, 1, 2]),
			);

			let from =
				requestEventFromResponder[0].detail.message.header.signatures!.publicKeys.map(
					(x) => x.hashcode(),
				);
			expect(from).to.deep.eq(expectedSigner);
		});

		it("send normalizes bare to into silent delivery", async () => {
			const publish = sinon.spy(reader.node.services.pubsub, "publish");

			try {
				await reader.query.send(
					new Body({
						arr: new Uint8Array([0, 1, 2]),
					}),
					{
						to: [responder.node.identity.publicKey],
					},
				);
			} finally {
				publish.restore();
			}

			expect(publish.calledOnce).to.be.true;
			const options = publish.firstCall.args[1];
			expect(options.mode).to.be.instanceOf(SilentDelivery);
			const mode = options.mode as SilentDelivery;
			expect(mode.to).to.deep.equal([
				responder.node.identity.publicKey.hashcode(),
			]);
			expect(mode.redundancy).to.equal(1);
		});

		it("onResponse", async () => {
			let results: Body[] = [];
			await reader.query.request(
				new Body({
					arr: new Uint8Array([0, 1, 2]),
				}),

				{
					amount: 1,
					onResponse: (resp) => {
						results.push(resp);
					},
				},
			);

			await waitFor(() => results.length === 1);
		});

		it("to", async () => {
			let results: Body[] = (
				await reader.query.request(
					new Body({
						arr: new Uint8Array([0, 1, 2]),
					}),
					{ timeout: 3000, amount: 1 },
				)
			).map((x) => x.response);
			// TODO should requesting without receivers yield any results?
			// + ease of use
			// - performance reason, message my be read by peers that does not need it
			expect(results.length).equal(1); // for now assume all peers should get it, hence we get 1 result here

			results = (
				await reader.query.request(
					new Body({
						arr: new Uint8Array([0, 1, 2]),
					}),
					{
						mode: new SilentDelivery({
							to: [responder.node.identity.publicKey],
							redundancy: 1,
						}),
					},
				)
			).map((x) => x.response);
			await waitFor(() => results.length === 1);
		});

		it("resubscribe", async () => {
			expect(
				(responder.node.services.pubsub as any)["subscriptions"].get("topic")
					.counter,
			).equal(1);
			expect(
				(responder.node.services.pubsub as any)["listenerCount"]("data"),
			).equal(1);
			expect(
				(reader.node.services.pubsub as any)["topics"]
					.get("topic")
					.get(responder.node.identity.publicKey.hashcode()).data,
			).equal(undefined);
			await responder.query.subscribe();
			await waitForResolved(
				() =>
					expect(
						(reader.node.services.pubsub as any)["topics"]
							.get("topic")
							.get(responder.node.identity.publicKey.hashcode()),
					).to.exist,
			);
			await responder.query.subscribe();

			// no change since already subscribed
			expect(
				(reader.node.services.pubsub as any)["topics"]
					.get("topic")
					.get(responder.node.identity.publicKey.hashcode()),
			).to.exist;

			expect(
				(responder.node.services.pubsub as any)["listenerCount"]("data"),
			).equal(1);
			expect(
				(responder.node.services.pubsub as any)["subscriptions"].get("topic")
					.counter,
			).equal(1);
		});

		it("close", async () => {
			let listenerCount = (reader.node.services.pubsub as any)["listenerCount"](
				"data",
			);
			expect(listenerCount).equal(1);
			expect(reader.closed).to.be.false;
			await reader.close();
			expect(reader.closed).to.be.true;
			listenerCount = (reader.node.services.pubsub as any)["listenerCount"](
				"data",
			);
			expect(listenerCount).equal(0);
		});

		for (const operation of ["close", "drop"] as const) {
			it(`${operation} keeps the subscription for a non-terminal owner release`, async () => {
				await session.peers[1].open(reader.query, {
					parent: reader as any,
					existing: "reuse",
				});
				expect(reader.query.parents).to.deep.equal([reader, reader]);

				expect(await reader.query[operation](reader)).to.be.false;
				expect(reader.query.closed).to.be.false;
				expect(
					(reader.node.services.pubsub as any)["listenerCount"]("data"),
				).equal(1);
				expect(
					await reader.query.request(
						new Body({ arr: new Uint8Array([8, 9]) }),
						{ amount: 1 },
					),
				).to.have.length(1);
			});

			it(`${operation} does not unsubscribe for an invalid owner`, async () => {
				const wrongOwner = new RPCTest([]);
				wrongOwner.query = new RPC();
				await expect(reader.query[operation](wrongOwner)).to.be.rejectedWith(
					"Could not find from in parents",
				);
				expect(reader.query.closed).to.be.false;
				expect(
					(reader.node.services.pubsub as any)["listenerCount"]("data"),
				).equal(1);
				expect(
					await reader.query.request(
						new Body({ arr: new Uint8Array([10, 11]) }),
						{ amount: 1 },
					),
				).to.have.length(1);
			});
		}

		for (const operation of ["close", "drop"] as const) {
			it(`${operation} makes a committed base failure network-inert before exact retry`, async () => {
				const cleanupError = new Error(
					`synthetic committed ${operation} failure`,
				);
				let handled = 0;
				(responder.query as any)._responseHandler = async (query: Body) => {
					handled += 1;
					return query;
				};

				if (operation === "close") {
					const eventOptions = (responder.query as any)._eventOptions;
					const originalOnClose = eventOptions.onClose;
					let callbackAttempts = 0;
					eventOptions.onClose = async (program: Program) => {
						callbackAttempts += 1;
						if (callbackAttempts === 1) {
							throw cleanupError;
						}
						await originalOnClose?.(program);
					};
				} else {
					const blocks = responder.node.services.blocks;
					const originalRm = blocks.rm.bind(blocks);
					let deleteAttempts = 0;
					blocks.rm = async (address) => {
						if (address === responder.query.address) {
							deleteAttempts += 1;
							if (deleteAttempts === 1) {
								throw cleanupError;
							}
						}
						return originalRm(address);
					};
				}

				await expect(responder.query[operation](responder)).to.be.rejectedWith(
					cleanupError.message,
				);
				expect(responder.query.closed).to.be.true;
				expect((responder.query as any)._subscribed).to.be.false;
				expect(
					(responder.node.services.pubsub as any)["listenerCount"]("data"),
				).equal(0);

				const responses = await reader.query.request(
					new Body({ arr: new Uint8Array([12, 13]) }),
					{ timeout: 50 },
				);
				expect(responses).to.have.length(0);
				expect(handled).to.equal(0);

				expect(await responder.query[operation](responder)).to.be.true;
				expect((responder.query as any)._subscribed).to.be.false;
			});
		}

		it("retries listener removal without spending another topic subscription", async () => {
			const pubsub = responder.node.services.pubsub as any;
			await pubsub.subscribe(responder.query.topic);
			expect(pubsub["subscriptions"].get("topic").counter).equal(2);
			let handled = 0;
			(responder.query as any)._responseHandler = async (query: Body) => {
				handled += 1;
				return query;
			};

			const originalRemoveEventListener =
				pubsub.removeEventListener.bind(pubsub);
			let dataRemovalAttempts = 0;
			pubsub.removeEventListener = (type: string, listener: unknown) => {
				if (type === "data") {
					dataRemovalAttempts += 1;
					if (dataRemovalAttempts === 1) {
						throw new Error("synthetic listener removal failure");
					}
				}
				return originalRemoveEventListener(type, listener);
			};

			await expect(responder.query.close(responder)).to.be.rejectedWith(
				"synthetic listener removal failure",
			);
			expect(responder.query.closed).to.be.true;
			expect((responder.query as any)._subscribed).to.be.false;
			expect((responder.query as any)._listenerAttached).to.be.true;
			expect(pubsub["subscriptions"].get("topic").counter).equal(1);

			const responses = await reader.query.request(
				new Body({ arr: new Uint8Array([14, 15]) }),
				{ timeout: 50 },
			);
			expect(responses).to.have.length(0);
			expect(handled).to.equal(0);

			pubsub.removeEventListener = originalRemoveEventListener;
			expect(await responder.query.close(responder)).to.be.true;
			expect((responder.query as any)._listenerAttached).to.be.false;
			expect(pubsub["subscriptions"].get("topic").counter).equal(1);
			await pubsub.unsubscribe(responder.query.topic);
			expect(pubsub["subscriptions"].has("topic")).to.be.false;
		});

		it("drop", async () => {
			let listenerCount = (reader.node.services.pubsub as any)["listenerCount"](
				"data",
			);
			expect(listenerCount).equal(1);
			expect(reader.closed).to.be.false;
			await reader.drop();
			expect(reader.closed).to.be.true;
			listenerCount = (reader.node.services.pubsub as any)["listenerCount"](
				"data",
			);
			expect(listenerCount).equal(0);
		});

		it("concurrency", async () => {
			let promises: Promise<RPCResponse<Body>[]>[] = [];
			let concurrency = 100;
			for (let i = 0; i < concurrency; i++) {
				promises.push(
					reader.query.request(
						new Body({
							arr: new Uint8Array([i]),
						}),
						{ amount: 1 },
					),
				);
			}
			const results = await Promise.all(promises);
			for (let i = 0; i < concurrency; i++) {
				expect(results[i]).to.have.length(1);
				expect(results[i][0].response.arr).to.deep.equal(new Uint8Array([i]));
			}
		});

		it("timeout", async () => {
			let waitFor = 5000;

			const t0 = +new Date();
			let results: Body[] = (
				await reader.query.request(
					new Body({
						arr: new Uint8Array([0, 1, 2]),
					}),
					{
						timeout: waitFor,
					},
				)
			).map((x) => x.response);
			const t1 = +new Date();
			expect(Math.abs(t1 - t0 - waitFor)).lessThan(500); // some threshold
			expect(results).to.have.length(1);
		});

		it("responseInterceptor", async () => {
			// create a request that intentially will not resolve
			let waitFor = 5000;
			const keypair = await Ed25519Keypair.create();
			const t0 = +new Date();
			let results: Body[] = (
				await reader.query.request(
					new Body({
						arr: new Uint8Array([0, 1, 2]),
					}),
					{
						mode: new SilentDelivery({
							to: [keypair.publicKey],
							redundancy: 1,
						}),
						responseInterceptor: (
							fn: (response: RPCResponse<Body>) => void,
						) => {
							fn({
								from: keypair.publicKey,
								response: new Body({ arr: new Uint8Array([9, 9, 9]) }),
								message: {
									header: {
										signatures: {
											publicKeys: [keypair.publicKey],
										},
									},
								} as any, // TODO types
							});
						},
					},
				)
			).map((x) => x.response);
			const t1 = +new Date();
			expect(t1 - t0).lessThan(waitFor); // because we intercepted the response immediately
			expect(results).to.have.length(1);
			expect(results[0].arr).to.deep.equal(new Uint8Array([9, 9, 9]));
		});

		it("responseInterceptor does not leak unhandled rejections on abort", async () => {
			const unhandledRejections: unknown[] = [];
			const onUnhandledRejection = (reason: unknown) => {
				unhandledRejections.push(reason);
			};
			process.on("unhandledRejection", onUnhandledRejection);

			try {
				const abortController = new AbortController();
				let responseInterceptorInstalled = false;

				const keypair = await Ed25519Keypair.create();
				const requestPromise = reader.query.request(
					new Body({
						arr: new Uint8Array([0, 1, 2]),
					}),
					{
						mode: new SilentDelivery({
							to: [keypair.publicKey],
							redundancy: 1,
						}),
						signal: abortController.signal,
						responseInterceptor: () => {
							responseInterceptorInstalled = true;
						},
					},
				);

				await waitFor(() => responseInterceptorInstalled, { timeout: 1000 });
				abortController.abort(new AbortError("INTENTIONAL_ABORT"));

				await requestPromise.catch(() => {});
				await delay(0);

				expect(unhandledRejections).to.have.length(0);
			} finally {
				process.removeListener("unhandledRejection", onUnhandledRejection);
			}
		});
	});

	describe("init", () => {
		let session: TestSession, rpcs: RPCTest[];

		beforeEach(async () => {
			rpcs = [];
			session = await TestSession.connected(3);
		});
		afterEach(async () => {
			await Promise.all(rpcs.map((x) => x.close()));
			await session.stop();
		});
		it("will request subscribers on initialization", async () => {
			for (const peer of session.peers) {
				const rpc = new RPCTest(session.peers.map((x) => x.peerId));
				rpc.query = new RPC();
				await peer.open(rpc);
				rpcs.push(rpc);
				await delay(500); // add a little delay, so that new peers will not receive old subscription events
			}
			for (let i = 0; i < rpcs.length; i++) {
				for (let j = 0; j < rpcs.length; j++) {
					if (j !== i) {
						// Test that even if we did not receive the old subsription events, we have requested subscribers
						// Hence the line below will resolve
						await rpcs[i].waitFor(session.peers[j].peerId);
					}
				}
			}
		});
	});
});
describe("queryAll", () => {
	let session: TestSession, clients: RPCTest[];

	beforeEach(async () => {
		session = await TestSession.connected(3);
		const t = new RPCTest(session.peers.map((x) => x.peerId));
		t.query = new RPC();

		clients = [];
		for (let i = 0; i < session.peers.length; i++) {
			const c = deserialize(serialize(t), RPCTest);

			await session.peers[i].open(c);
			clients.push(c);
		}
		for (let i = 0; i < session.peers.length; i++) {
			await clients[i].waitFor(
				session.peers.filter((p, ix) => ix !== i).map((x) => x.peerId),
			);
		}
	});

	afterEach(async () => {
		await session.stop();
	});

	it("none", async () => {
		let r: RPCResponse<Body>[][] = [];

		// groups = [[me, 1, 2]]
		await queryAll(
			clients[0].query,
			[session.peers.map((x) => x.identity.publicKey.hashcode())],
			new Body({ arr: new Uint8Array([1]) }),
			(e) => {
				r.push(e);
			},
		);
		expect(r).to.be.empty; // because I am in the group, and it does not make sense then to query someone else
	});

	it("one of", async () => {
		let r: RPCResponse<Body>[][] = [];
		await queryAll(
			clients[0].query,
			[
				session.peers
					.filter((x, ix) => ix !== 0)
					.map((x) => x.identity.publicKey.hashcode()),
			],
			new Body({ arr: new Uint8Array([1]) }),
			(e) => {
				r.push(e);
			},
		);
		expect(r).to.have.length(1);
		expect(r[0]).to.have.length(1);
	});

	it("series", async () => {
		const fn = async (index: number) => {
			const i = index % session.peers.length;
			let r: RPCResponse<Body>[][] = [];
			await queryAll(
				clients[i].query,
				session.peers.map((x) => [x.identity.publicKey.hashcode()]),
				new Body({ arr: new Uint8Array([1]) }),
				(e) => {
					r.push(e);
				},
			);
			expect(r).to.have.length(1);
			expect(r[0]).to.have.length(2);
		};

		for (let i = 0; i < 100; i++) {
			await fn(i);
		}
	});

	it("concurrently", async () => {
		let promises: Promise<any>[] = [];
		for (let i = 0; i < 100; i++) {
			const fn = async () => {
				let r: RPCResponse<Body>[][] = [];
				try {
					await queryAll(
						clients[i % session.peers.length].query,
						session.peers.map((x) => [x.identity.publicKey.hashcode()]),
						new Body({ arr: new Uint8Array([1]) }),
						(e) => {
							r.push(e);
						},
					);

					expect(r).to.have.length(1);
					expect(r[0]).to.have.length(2);
				} catch (error) {
					console.error(i);
					throw error;
				}
			};
			promises.push(fn());
		}
		await Promise.all(promises);
	});
	it("aborts when closing", async () => {
		let r: RPCResponse<Body>[][] = [];

		let t1 = +new Date();

		clients[1].delay = 1e4; // make sure client 1 never responds
		const promise = queryAll(
			clients[0].query,
			[[clients[1].node.identity.publicKey.hashcode()]],
			new Body({ arr: new Uint8Array([1]) }),
			(e) => {
				r.push(e);
			},
		);

		await delay(200); // make sure the request is sent
		clients[0].close();

		await expect(promise).rejectedWith(AbortError);
		expect(+new Date() - t1).lessThan(1000);
	});

	it("aborts already closed", async () => {
		let r: RPCResponse<Body>[][] = [];

		let t1 = +new Date();
		await clients[0].close();
		const promise = queryAll(
			clients[0].query,
			[[clients[1].node.identity.publicKey.hashcode()]],
			new Body({ arr: new Uint8Array([1]) }),
			(e) => {
				r.push(e);
			},
		);

		await expect(promise).rejectedWith(AbortError);
		expect(+new Date() - t1).lessThan(1000);
	});

	it("signal", async () => {
		let r: RPCResponse<Body>[][] = [];

		const controller = new AbortController();
		let t1 = +new Date();
		const promise = queryAll(
			clients[0].query,
			[[clients[1].node.identity.publicKey.hashcode()]],
			new Body({ arr: new Uint8Array([1]) }),
			(e) => {
				r.push(e);
			},
			{ signal: controller.signal },
		);

		clients[1].delay = 1e4; // make sure client 1 never responds

		await delay(500); // make sure the request is sent
		controller.abort(new Error("TestAborted"));

		await expect(promise).rejectedWith("TestAborted");
		expect(+new Date() - t1).lessThan(1000);
	});

	it("reports missing groups on timeout", async () => {
		clients[1].delay = 200;
		const missingGroup = [[clients[1].node.identity.publicKey.hashcode()]];
		try {
			await queryAll(
				clients[0].query,
				missingGroup,
				new Body({ arr: new Uint8Array([1]) }),
				() => {},
				{ timeout: 50 },
			);
			expect.fail("Expected MissingResponsesError");
		} catch (error) {
			expect(error).to.be.instanceOf(MissingResponsesError);
			expect((error as MissingResponsesError).missingGroups).to.deep.equal(
				missingGroup,
			);
		}
	});
});
