import { deserialize, field, serialize, variant, vec } from "@dao-xyz/borsh";
import { PublicSignKey, getPublicKeyFromPeerId } from "@peerbit/crypto";
import { Program } from "@peerbit/program";
import { type PeerId } from "@peerbit/pubsub";
import { SilentDelivery } from "@peerbit/stream-interface";
import { TestSession } from "@peerbit/test-utils";
import { AbortError, delay, waitFor, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { RPC, type RPCResponse, queryAll } from "../src/index.js";

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
		const promise = queryAll(
			clients[0].query,
			[[clients[1].node.identity.publicKey.hashcode()]],
			new Body({ arr: new Uint8Array([1]) }),
			(e) => {
				r.push(e);
			},
		);

		clients[1].delay = 1e4; // make sure client 1 never responds

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
});
