import { v4 as uuid } from "uuid";
import { delay, waitFor } from "@dao-xyz/peerbit-time";
import { LSession, waitForPeers } from "@dao-xyz/peerbit-test-utils";
import {
	AccessError,
	decryptVerifyInto,
	Ed25519Keypair,
	Ed25519PublicKey,
	X25519Keypair,
	X25519PublicKey,
} from "@dao-xyz/peerbit-crypto";
import { RequestV0, ResponseV0, send, respond, RPC, RPCMessage } from "../";
import { Ed25519Identity } from "@dao-xyz/peerbit-log";
import { Program } from "@dao-xyz/peerbit-program";
import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import { PubSubData } from "@dao-xyz/libp2p-direct-sub";

const createIdentity = async () => {
	const ed = await Ed25519Keypair.create();
	return {
		publicKey: ed.publicKey,
		privateKey: ed.privateKey,
		sign: (data) => ed.sign(data),
	} as Ed25519Identity;
};

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

	constructor() {
		super();
	}

	async setup(topic?: string): Promise<void> {
		await this.query.setup({
			topic,
			responseType: Body,
			queryType: Body,
			context: this,
			responseHandler: (query, from) => {
				const resp = query;
				return resp;
			},
		});
	}
}

describe("rpc", () => {
	let session: LSession, responder: RPCTest, reader: RPCTest;
	beforeAll(async () => {
		session = await LSession.connected(3);

		const topic = uuid();

		responder = new RPCTest();
		responder.query = new RPC();

		responder.setup(topic); // set topic manually because we are not going to have a parent program with address
		await responder.init(session.peers[0], await createIdentity(), {
			replicate: true,
			store: {} as any,
		});
		reader = deserialize(serialize(responder), RPCTest);
		reader.setup(topic); // set topic manually because we are not going to have a parent program with address

		await reader.init(session.peers[1], await createIdentity(), {
			store: {} as any,
		});

		await waitForPeers(
			session.peers[1],
			[session.peers[0].peerId],
			responder.query.rpcTopic
		);
	});
	afterAll(async () => {
		await session.stop();
	});

	it("any", async () => {
		let results: Body[] = [];
		await reader.query.send(
			new Body({
				arr: new Uint8Array([0, 1, 2]),
			}),
			(resp) => {
				results.push(resp);
			},
			{ amount: 1 }
		);

		await waitFor(() => results.length === 1);
	});

	it("context", async () => {
		let results: Body[] = [];

		// Unknown context (expect no results)
		await reader.query.send(
			new Body({
				arr: new Uint8Array([0, 1, 2]),
			}),
			(resp) => {
				results.push(resp);
			},
			{ timeout: 3000, context: "wrong context" }
		);

		// Explicit
		await reader.query.send(
			new Body({
				arr: new Uint8Array([0, 1, 2]),
			}),
			(resp) => {
				results.push(resp);
			},
			{ amount: 1, context: reader.address.toString() }
		);
		expect(results).toHaveLength(1);

		// Implicit
		await reader.query.send(
			new Body({
				arr: new Uint8Array([0, 1, 2]),
			}),
			(resp) => {
				results.push(resp);
			},
			{ amount: 1 }
		);
		expect(results).toHaveLength(2);
	});

	it("timeout", async () => {
		let waitFor = 5000;

		let results: Body[] = [];
		const t0 = +new Date();
		await reader.query.send(
			new Body({
				arr: new Uint8Array([0, 1, 2]),
			}),
			(resp) => {
				results.push(resp);
			},
			{
				timeout: waitFor,
			}
		);
		const t1 = +new Date();
		expect(Math.abs(t1 - t0 - waitFor)).toBeLessThan(200); // some threshold
		expect(results).toHaveLength(1);
	});

	it("amount", async () => {
		let amount = 2;
		let timeout = 2000;

		const topic = uuid();
		const kp = await X25519Keypair.create();

		for (let i = 1; i < 3; i++) {
			session.peers[i].directsub.subscribe(topic);
			session.peers[i].directsub.addEventListener(
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
										context: "context",
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

		let results: Uint8Array[] = [];
		await send(
			session.peers[0],
			topic,
			topic,
			new RequestV0({
				request: serialize(new Body({ arr: new Uint8Array([0, 1, 2]) })),
				respondTo: kp.publicKey,
			}),
			(resp) => {
				results.push(resp.response);
			},
			kp,
			{
				timeout,
				amount,
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
		await session.peers[1].directsub.subscribe(topic);
		session.peers[1].directsub.addEventListener(
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
									response: new Uint8Array([0, 1, 2]),
									context: "context",
								}),
								{ signer: responder }
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

		let results: Uint8Array[] = [];
		const kp = await X25519Keypair.create();

		await send(
			session.peers[0],
			topic,
			topic,
			new RequestV0({
				request: new Uint8Array([0, 1, 2]),
				respondTo: kp.publicKey,
			}),
			(resp, from) => {
				if (!from) {
					return; // from message
				}

				// Check that it was signed by the responder
				expect(from).toBeInstanceOf(Ed25519PublicKey);
				expect(
					(from as Ed25519PublicKey).equals(responder.publicKey)
				).toBeTrue();

				results.push(resp.response);
			},
			kp,
			{
				timeout: timeout,
				amount,
				signer: sender,
			}
		);

		try {
			await waitFor(() => results.length == amount);
		} catch (error) {
			console.log("????", results.length, amount, x);
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
		await session.peers[1].directsub.subscribe(topic);
		session.peers[1].directsub.addEventListener(
			"data",
			async (evt: CustomEvent<PubSubData>) => {
				//  if (evt.detail.type === "signed")
				{
					const message = evt.detail;
					if (message) {
						/* if (message.from.equals(session.peers[1].peerId)) {
							return;
						} */
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
										response: new Uint8Array([0, 1, 2]),
										context: "context",
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

		let results: Uint8Array[] = [];
		await send(
			session.peers[0],
			topic,
			topic,
			new RequestV0({
				request: new Uint8Array([0, 1, 2]),
				respondTo: await X25519PublicKey.from(requester.publicKey),
			}),
			(resp) => {
				results.push(resp.response);
			},
			await X25519Keypair.from(new Ed25519Keypair({ ...requester })),
			{
				timeout,
				amount,
				signer: requester,

				encryption: {
					key: () => new Ed25519Keypair({ ...requester }),
					responders: [await X25519PublicKey.from(responder.publicKey)],
				},
			}
		);

		await waitFor(() => results.length == amount);
	});
});
