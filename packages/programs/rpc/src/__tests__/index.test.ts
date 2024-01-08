import { delay, waitForResolved, waitFor } from "@peerbit/time";
import { TestSession } from "@peerbit/test-utils";
import { RPC, RPCResponse, queryAll } from "../index.js";
import { Program } from "@peerbit/program";
import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import { PublicSignKey, getPublicKeyFromPeerId } from "@peerbit/crypto";
import { DirectSub, PeerId } from "@peerbit/pubsub";
import { SilentDelivery } from "@peerbit/stream-interface";

@variant("payload")
class Body {
	@field({ type: Uint8Array })
	arr: Uint8Array;
	constructor(properties?: { arr: Uint8Array }) {
		if (properties) {
			this.arr = properties.arr;
		}
	}
}

@variant("rpc-test")
class RPCTest extends Program {
	@field({ type: RPC })
	query: RPC<Body, Body>;

	@field({ type: vec(PublicSignKey) })
	responders: PublicSignKey[];

	constructor(responders: PeerId[]) {
		super();
		this.responders = responders.map((x) =>
			x instanceof PublicSignKey ? x : getPublicKeyFromPeerId(x)
		);
	}

	async open(): Promise<void> {
		await this.query.open({
			topic: "topic",
			responseType: Body,
			queryType: Body,
			responseHandler: this.responders.find((x) =>
				this.node.identity.publicKey.equals(x)
			)
				? (query, from) => {
						const resp = query;
						return resp;
					}
				: undefined
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
			let results: RPCResponse<Body>[] = await reader.query.request(
				new Body({
					arr: new Uint8Array([0, 1, 2])
				}),
				{ amount: 1 }
			);

			await waitForResolved(() => expect(results).toHaveLength(1));
			expect(results[0].from?.hashcode()).toEqual(
				responder.node.identity.publicKey.hashcode()
			);
		});

		it("onResponse", async () => {
			let results: Body[] = [];
			await reader.query.request(
				new Body({
					arr: new Uint8Array([0, 1, 2])
				}),

				{
					amount: 1,
					onResponse: (resp) => {
						results.push(resp);
					}
				}
			);

			await waitFor(() => results.length === 1);
		});

		it("to", async () => {
			let results: Body[] = (
				await reader.query.request(
					new Body({
						arr: new Uint8Array([0, 1, 2])
					}),
					{ timeout: 3000, amount: 1 }
				)
			).map((x) => x.response);
			// TODO should requesting without receivers yield any results?
			// + ease of use
			// - performance reason, message my be read by peers that does not need it
			expect(results.length).toEqual(1); // for now assume all peers should get it, hence we get 1 result here

			results = (
				await reader.query.request(
					new Body({
						arr: new Uint8Array([0, 1, 2])
					}),
					{
						mode: new SilentDelivery({
							to: [responder.node.identity.publicKey],
							redundancy: 1
						})
					}
				)
			).map((x) => x.response);
			await waitFor(() => results.length === 1);
		});

		it("resubscribe", async () => {
			expect(
				responder.node.services.pubsub["subscriptions"].get("topic").counter
			).toEqual(1);
			expect(responder.node.services.pubsub["listenerCount"]("data")).toEqual(
				1
			);
			expect(
				reader.node.services.pubsub["topics"]
					.get("topic")
					.get(responder.node.identity.publicKey.hashcode()).data
			).toBeUndefined();
			await responder.query.subscribe();
			await waitForResolved(() =>
				expect(
					reader.node.services.pubsub["topics"]
						.get("topic")
						.get(responder.node.identity.publicKey.hashcode())
				).toBeDefined()
			);
			await responder.query.subscribe();

			// no change since already subscribed
			expect(
				reader.node.services.pubsub["topics"]
					.get("topic")
					.get(responder.node.identity.publicKey.hashcode())
			).toBeDefined();

			expect(responder.node.services.pubsub["listenerCount"]("data")).toEqual(
				1
			);
			expect(
				responder.node.services.pubsub["subscriptions"].get("topic").counter
			).toEqual(1);
		});

		it("close", async () => {
			let listenerCount = reader.node.services.pubsub["listenerCount"]("data");
			expect(listenerCount).toEqual(1);
			expect(reader.closed).toBeFalse();
			await reader.close();
			expect(reader.closed).toBeTrue();
			listenerCount = reader.node.services.pubsub["listenerCount"]("data");
			expect(listenerCount).toEqual(0);
		});

		it("drop", async () => {
			let listenerCount = reader.node.services.pubsub["listenerCount"]("data");
			expect(listenerCount).toEqual(1);
			expect(reader.closed).toBeFalse();
			await reader.drop();
			expect(reader.closed).toBeTrue();
			listenerCount = reader.node.services.pubsub["listenerCount"]("data");
			expect(listenerCount).toEqual(0);
		});

		it("concurrency", async () => {
			let promises: Promise<RPCResponse<Body>[]>[] = [];
			let concurrency = 100;
			for (let i = 0; i < concurrency; i++) {
				promises.push(
					reader.query.request(
						new Body({
							arr: new Uint8Array([i])
						}),
						{ amount: 1 }
					)
				);
			}
			const results = await Promise.all(promises);
			for (let i = 0; i < concurrency; i++) {
				expect(results[i]).toHaveLength(1);
				expect(results[i][0].response.arr).toEqual(new Uint8Array([i]));
			}
		});

		it("timeout", async () => {
			let waitFor = 5000;

			const t0 = +new Date();
			let results: Body[] = (
				await reader.query.request(
					new Body({
						arr: new Uint8Array([0, 1, 2])
					}),
					{
						timeout: waitFor
					}
				)
			).map((x) => x.response);
			const t1 = +new Date();
			expect(Math.abs(t1 - t0 - waitFor)).toBeLessThan(500); // some threshold
			expect(results).toHaveLength(1);
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
				session.peers.filter((p, ix) => ix !== i).map((x) => x.peerId)
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
			}
		);
		expect(r).toHaveLength(0); // because I am in the group, and it does not make sense then to query someone else
	});

	it("one of", async () => {
		let r: RPCResponse<Body>[][] = [];
		await queryAll(
			clients[0].query,
			[
				session.peers
					.filter((x, ix) => ix !== 0)
					.map((x) => x.identity.publicKey.hashcode())
			],
			new Body({ arr: new Uint8Array([1]) }),
			(e) => {
				r.push(e);
			}
		);
		expect(r).toHaveLength(1);
		expect(r[0]).toHaveLength(1);
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
				}
			);
			expect(r).toHaveLength(1);
			expect(r[0]).toHaveLength(2);
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
						}
					);

					expect(r).toHaveLength(1);
					expect(r[0]).toHaveLength(2);
				} catch (error) {
					console.error(i);
					throw error;
				}
			};
			promises.push(fn());
		}
		await Promise.all(promises);
	});
});

/* it("amount", async () => {
	let amount = 2;
	let timeout = 2000;

	const topic = uuid();
	const kp = await X25519Keypair.create();

	for (let i = 1; i < 3; i++) {
		session.peers[i].services.pubsub.subscribe(topic);
		session.peers[i].services.pubsub.addEventListener(
			"data",
			async (evt: CustomEvent<PubSubData>) => {
				const message = evt.detail;
				if (message && message.topics.includes(topic)) {
					try {
						let { result: request } = await decryptVerifyInto(
							message.data,
							RPCMessage,
							kp
						);
						if (request instanceof RequestV0) {
							await respond(
								session.peers[i],
								topic,
								request,
								new ResponseV0({
									response: serialize(
										new Body({
											arr: new Uint8Array([0, 1, 2]),
										})
									),
									context: Buffer.from("context"),
								})
							);
						}
					} catch (error) {
						if (error instanceof AccessError) {
							return;
						}
						throw error;
					}
				}
			}
		);
	}

	await waitForPeers(
		session.peers[0],
		[session.peers[1].peerId, session.peers[2].peerId],
		topic
	);

	let results: Body[] = [];
	await send(
		session.peers[0],
		topic,
		topic,
		new RequestV0({
			request: serialize(new Body({ arr: new Uint8Array([0, 1, 2]) })),
			respondTo: kp.publicKey,
			context: Buffer.from("context")
		}),
		Body,
		kp,
		{
			timeout,
			amount,
			onResponse: (resp) => {
				results.push(resp);
			},
		}
	);

	await waitFor(() => results.length == amount);
});

it("signed", async () => {
	let amount = 1;

	let timeout = 3000;

	const sender = await createIdentity();
	const responder = await createIdentity();
	const topic = uuid();
	let x = false;
	await session.peers[1].services.pubsub.subscribe(topic);
	session.peers[1].services.pubsub.addEventListener(
		"data",
		async (evt: CustomEvent<PubSubData>) => {
			const message = evt.detail;
			if (message && message.topics.includes(topic)) {
				try {
					let { result: request, from } = await decryptVerifyInto(
						message.data,
						RPCMessage,
						() => Promise.resolve(undefined)
					);
					if (request instanceof RequestV0) {
						x = true;

						// Check that it was signed by the sender
						expect(from).toBeInstanceOf(Ed25519PublicKey);
						expect(
							(from as Ed25519PublicKey).equals(sender.publicKey)
						).toBeTrue();

						await respond(
							session.peers[1],
							topic,
							request,
							new ResponseV0({
								context: Buffer.from("context"),
								response: serialize(
									new Body({ arr: new Uint8Array([0, 1, 2]) })
								),
							}),
							{ signer: responder.sign.bind(responder) }
						);
					}
				} catch (error) {
					console.error(error);
					if (error instanceof AccessError) {
						return;
					}
					throw error;
				}
			}
		}
	);

	await waitForPeers(session.peers[0], [session.peers[1].peerId], topic);

	let results: Body[] = [];
	const kp = await X25519Keypair.create();

	await send(
		session.peers[0],
		topic,
		topic,
		new RequestV0({
			context: Buffer.from("context"),
			request: new Uint8Array([0, 1, 2]),
			respondTo: kp.publicKey,
		}),
		Body,
		kp,
		{
			timeout: timeout,
			amount,
			signer: sender,
			onResponse: (resp, from) => {
				if (!from) {
					return; // from message
				}

				// Check that it was signed by the responder
				expect(from).toBeInstanceOf(Ed25519PublicKey);
				expect(
					(from as Ed25519PublicKey).equals(responder.publicKey)
				).toBeTrue();
				results.push(resp);
			},
		}
	);

	try {
		await waitFor(() => results.length == amount);
	} catch (error) {
		throw error;
	}
});

it("encrypted", async () => {
	// query encrypted and respond encrypted
	let amount = 1;
	let timeout = 3000;

	const responder = await createIdentity();
	const requester = await createIdentity();
	const topic = uuid();
	await session.peers[1].services.pubsub.subscribe(topic);
	session.peers[1].services.pubsub.addEventListener(
		"data",
		async (evt: CustomEvent<PubSubData>) => {
			//  if (evt.detail.type === "signed")
			{
				const message = evt.detail;
				if (message) {
					try {
						let { result: request } = await decryptVerifyInto(
							message.data,
							RequestV0,
							async (keys) => {
								return {
									index: 0,
									keypair: await X25519Keypair.from(
										new Ed25519Keypair({ ...responder })
									),
								};
							}
						);
						if (request instanceof RequestV0) {
							await respond(
								session.peers[1],
								topic,
								request,
								new ResponseV0({
									context: Buffer.from("context"),
									response: serialize(
										new Body({ arr: new Uint8Array([0, 1, 2]) })
									),
								})
							);
						}
					} catch (error) {
						if (error instanceof AccessError) {
							return;
						}
						throw error;
					}
				}
			}
		}
	);

	await waitForPeers(session.peers[0], [session.peers[1].peerId], topic);

	let results: Body[] = [];
	await send(
		session.peers[0],
		topic,
		topic,
		new RequestV0({
			context: Buffer.from("context"),
			request: new Uint8Array([0, 1, 2]),
			respondTo: await X25519PublicKey.from(requester.publicKey),
		}),
		Body,
		await X25519Keypair.from(new Ed25519Keypair({ ...requester })),
		{
			timeout,
			amount,
			signer: requester,
			onResponse: (resp) => {
				results.push(resp);
			},
			encryption: {
				key: () => new Ed25519Keypair({ ...requester }),
				responders: [await X25519PublicKey.from(responder.publicKey)],
			},
		}
	);

	await waitFor(() => results.length == amount);
}); */
