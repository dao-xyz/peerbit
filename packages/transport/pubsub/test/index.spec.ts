import { deserialize } from "@dao-xyz/borsh";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import * as filters from "@libp2p/websockets/filters";
import {
	Ed25519Keypair,
	type PublicSignKey,
	randomBytes,
} from "@peerbit/crypto";
import { TestSession } from "@peerbit/libp2p-test-utils";
import {
	type DataEvent,
	GetSubscribers,
	PubSubData,
	PubSubMessage,
	PublishEvent,
	Subscribe,
	type SubscriptionEvent,
	type UnsubcriptionEvent,
	Unsubscribe,
} from "@peerbit/pubsub-interface";
import { waitForNeighbour } from "@peerbit/stream";
import {
	AcknowledgeDelivery,
	DataMessage,
	type Message,
	SilentDelivery,
} from "@peerbit/stream-interface";
import { delay, waitFor, waitForResolved } from "@peerbit/time";
import { expect } from "chai";
import { equals } from "uint8arrays";
import { DirectSub, waitForSubscribers } from "../src/index.js";

const checkShortestPathIsNeighbours = (sub: DirectSub) => {
	const routes = sub.routes.routes.get(sub.routes.me)!;
	for (let peer of sub.peers) {
		const found = routes.get(peer[0])?.list.find((x) => x.hash === peer[0]);
		expect(found?.distance).equal(-1);
	}
};

const subscribAndWait = async (
	session: TestSession<{ pubsub: DirectSub }>,
	topic: string,
) => {
	for (const peer of session.peers) {
		await peer.services.pubsub.subscribe(topic);
	}

	for (let i = 0; i < session.peers.length; i++) {
		for (let j = 0; j < session.peers.length; j++) {
			if (i === j) {
				continue;
			}
			await waitForSubscribers(session.peers[i], [session.peers[j]], topic);
		}
	}
};
const createSubscriptionMetrics = (pubsub: DirectSub) => {
	let m: {
		subscriptions: DataMessage[];
		unsubscriptions: DataMessage[];
		getSubscriptions: DataMessage[];
	} = { getSubscriptions: [], subscriptions: [], unsubscriptions: [] };
	const onDataMessage = pubsub.onDataMessage.bind(pubsub);
	pubsub.onDataMessage = async (f, s, message, seenBefore) => {
		const result = await onDataMessage(f, s, message, seenBefore);
		const pubsubMessage = message.data
			? deserialize(message.data, PubSubMessage)
			: undefined;
		if (pubsubMessage instanceof Subscribe) {
			m.subscriptions.push(message);
		} else if (pubsubMessage instanceof Unsubscribe) {
			m.unsubscriptions.push(message);
		} else if (pubsubMessage instanceof GetSubscribers) {
			m.getSubscriptions.push(message);
		}
		return result;
	};

	return m;
};

const createMetrics = (pubsub: DirectSub) => {
	const m: {
		stream: DirectSub;
		relayedData: PubSubData[];
		messages: Message[];
		received: PubSubData[];
		allReceived: PubSubData[];

		reachable: PublicSignKey[];
		unrechable: PublicSignKey[];
		subscriptionEvents: SubscriptionEvent[];
		unsubscriptionEvents: UnsubcriptionEvent[];
	} = {
		messages: [],
		received: [],
		reachable: [],
		unrechable: [],
		relayedData: [],
		allReceived: [],
		stream: pubsub,
		subscriptionEvents: [],
		unsubscriptionEvents: [],
	};
	pubsub.addEventListener("message", (msg) => {
		m.messages.push(msg.detail);
	});
	pubsub.addEventListener("data", (msg) => {
		m.received.push(msg.detail.data);
	});
	pubsub.addEventListener("subscribe", (msg) => {
		m.subscriptionEvents.push(msg.detail);
	});
	pubsub.addEventListener("unsubscribe", (msg) => {
		m.unsubscriptionEvents.push(msg.detail);
	});
	pubsub.addEventListener("peer:reachable", (msg) => {
		m.reachable.push(msg.detail);
	});
	pubsub.addEventListener("peer:unreachable", (msg) => {
		m.unrechable.push(msg.detail);
	});

	const onDataMessageFn = pubsub.onDataMessage.bind(pubsub);
	pubsub.onDataMessage = (from, stream, message, seenBefore) => {
		const pubsubMessage = message.data
			? PubSubMessage.from(message.data)
			: undefined;
		if (pubsubMessage instanceof PubSubData) {
			m.allReceived.push(pubsubMessage);
		}
		return onDataMessageFn(from, stream, message, seenBefore);
	};

	const relayMessageFn = pubsub.relayMessage.bind(pubsub);
	pubsub.relayMessage = (from, message, to) => {
		if (message instanceof DataMessage && message.data) {
			const pubsubMessage = PubSubMessage.from(message.data);
			if (pubsubMessage instanceof PubSubData) {
				m.relayedData.push(pubsubMessage);
			}
		}
		return relayMessageFn(from, message, to);
	};

	return m;
};

describe("pubsub", function () {
	describe("topic", () => {
		let session: TestSession<{ pubsub: DirectSub }>;
		let streams: ReturnType<typeof createMetrics>[] = [];

		beforeEach(async () => {
			streams = [];
			session = await TestSession.disconnected(3, {
				services: {
					pubsub: (c) =>
						new DirectSub(c, {
							canRelayMessage: true,
							connectionManager: false,
						}),
				},
			});

			for (const peer of session.peers) {
				streams.push(createMetrics(peer.services.pubsub));
			}
		});
		afterEach(async () => {
			await Promise.all(streams.map((s) => s.stream.stop()));
			await session.stop();
		});

		it("can share topics when connecting after subscribe, 2 peers", async () => {
			const TOPIC = "world";
			streams[0].stream.subscribe(TOPIC);
			streams[1].stream.subscribe(TOPIC);

			await delay(1000); // wait for subscription message to propagate (if any)
			// now connect peers and make sure that subscription information is passed on as they connect
			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitForSubscribers(session.peers[0], [session.peers[1]], TOPIC);
			await waitForSubscribers(session.peers[1], [session.peers[0]], TOPIC);
		});

		it("can share topics when connecting after subscribe, 3 peers and 1 relay", async () => {
			const TOPIC = "world";
			streams[0].stream.subscribe(TOPIC);
			// peers[1] is not subscribing
			streams[2].stream.subscribe(TOPIC);

			await delay(1000); // wait for subscription message to propagate (if any)
			// now connect peers and make sure that subscription information is passed on as they connect
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);
			await waitForSubscribers(session.peers[0], [session.peers[2]], TOPIC);
			await waitForSubscribers(session.peers[2], [session.peers[0]], TOPIC);
		});
	});

	describe("publish", () => {
		let session: TestSession<{ pubsub: DirectSub }>;
		let streams: ReturnType<typeof createMetrics>[];
		const data = new Uint8Array([1, 2, 3]);
		const TOPIC = "world";

		describe("topology", () => {
			describe("line", () => {
				beforeEach(async () => {
					// 0 and 2 not connected
					session = await TestSession.disconnected(3, {
						services: {
							pubsub: (c) =>
								new DirectSub(c, {
									canRelayMessage: true,
									connectionManager: false,
								}),
						},
					});

					/* 
					┌─┐
					│1│
					└┬┘
					┌▽┐
					│2│
					└┬┘
					┌▽┐
					│3│
					└─┘
					*/

					await session.connect([
						[session.peers[0], session.peers[1]],
						[session.peers[1], session.peers[2]],
					]);

					streams = [];
					for (const peer of session.peers) {
						streams.push(createMetrics(peer.services.pubsub));
					}
					await waitForNeighbour(streams[0].stream, streams[1].stream);
					await waitForNeighbour(streams[1].stream, streams[2].stream);
					await subscribAndWait(session, TOPIC);
				});

				afterEach(async () => {
					for (let i = 0; i < streams.length; i++) {
						streams[i].stream.unsubscribe(TOPIC);
					}
					for (let i = 0; i < streams.length; i++) {
						await waitFor(
							() => !streams[i].stream.getSubscribers(TOPIC)?.length,
						);
						expect(streams[i].stream.topics.has(TOPIC)).to.be.false;
						expect(streams[i].stream.subscriptions.has(TOPIC)).to.be.false;
					}

					await session.stop();
				});

				it("0->TOPIC", async () => {
					await streams[0].stream.publish(data, { topics: [TOPIC] });
					await waitFor(() => streams[1].received.length === 1);
					expect(new Uint8Array(streams[1].received[0].data)).to.deep.equal(
						data,
					);
					expect(streams[1].received[0].topics).to.deep.equal([TOPIC]);
					await waitFor(() => streams[2].received.length === 1);
					expect(new Uint8Array(streams[2].received[0].data)).to.deep.equal(
						data,
					);
					await delay(1500); // wait some more time to make sure we dont get more messages
					expect(streams[1].received).to.have.length(1);
					expect(streams[2].received).to.have.length(1);
				});

				it("0->TOPIC strict to", async () => {
					await streams[0].stream.publish(data, {
						topics: [TOPIC],
						mode: new SilentDelivery({
							to: [streams[2].stream.publicKey],
							redundancy: 1,
						}),
					});
					await waitForResolved(() =>
						expect(streams[2].received).to.have.length(1),
					);
					expect(new Uint8Array(streams[2].received[0].data)).to.deep.equal(
						data,
					);
					expect(streams[2].received[0].topics).to.deep.equal([TOPIC]);
					expect(streams[1].received).to.be.empty;
					expect(streams[1].allReceived).to.have.length(1); // because the message has to travel through this node

					await delay(1500); // wait some more time to make sure we dont get more messages
					expect(streams[1].received).to.be.empty;
					expect(streams[1].allReceived).to.have.length(1); // because the message has to travel through this node
					expect(streams[2].received).to.have.length(1);
				});
				it("1->TOPIC strict to", async () => {
					await streams[1].stream.publish(data, {
						topics: [TOPIC],
						mode: new SilentDelivery({
							to: [streams[2].stream.publicKey],
							redundancy: 1,
						}),
					});
					await waitForResolved(() =>
						expect(streams[2].received).to.have.length(1),
					);
					expect(new Uint8Array(streams[2].received[0].data)).to.deep.equal(
						data,
					);
					expect(streams[2].received[0].topics).to.deep.equal([TOPIC]);
					expect(streams[0].allReceived).to.be.empty;
					await delay(1500); // wait some more time to make sure we dont get more messages
					expect(streams[0].allReceived).to.be.empty;
					expect(streams[2].received).to.have.length(1);
				});

				it("sends only in necessary directions", async () => {
					await streams[2].stream.unsubscribe(TOPIC);

					await waitForResolved(() =>
						expect(streams[1].stream.getSubscribers(TOPIC)!).to.have.length(2),
					);

					const sendBytes = randomBytes(32);
					await streams[1].stream.publish(sendBytes, { topics: [TOPIC] });

					await waitFor(() => streams[0].received.length === 1);
					await delay(1500); // wait some more time to make sure we dont get more messages

					// Make sure we never received the data message in node 2
					for (const message of streams[2].allReceived) {
						expect(
							equals(new Uint8Array(message.data), new Uint8Array(sendBytes)),
						).to.be.false;
					}
				});

				/* TODO wanted feature?
				
				it("send without topic directly", async () => {
					await streams[0].stream.publish(data, {
						to: [streams[1].stream.components.peerId]
					});
					await waitFor(() => streams[1].received.length === 1);
					expect(new Uint8Array(streams[1].received[0].data)).to.deep.equal(data);
					await delay(1500); // wait some more time to make sure we dont get more messages
					expect(streams[1].received).to.have.length(1);
					expect(streams[2].received).to.be.empty;
				});
	
				it("send without topic over relay", async () => {
					await streams[0].stream.publish(data, {
						to: [streams[2].stream.components.peerId]
					});
					await waitFor(() => streams[2].received.length === 1);
					expect(new Uint8Array(streams[2].received[0].data)).to.deep.equal(data);
					await delay(1500); // wait some more time to make sure we dont get more messages
					expect(streams[2].received).to.have.length(1);
					expect(streams[1].received).to.be.empty;
				}); */

				/* TODO feature
				it("can send as non subscribeer", async () => {
	
					await streams[0].stream.unsubscribe(TOPIC);
					await streams[1].stream.unsubscribe(TOPIC);
					await streams[0].stream.publish(data, { topics: [TOPIC] });
					await waitFor(() => streams[2].received.length === 1);
					expect(new Uint8Array(streams[2].received[0].data)).to.deep.equal(data);
					await delay(1500); // wait some more time to make sure we dont get more messages
					expect(streams[1].received).to.be.empty;
					expect(streams[2].received).to.have.length(1);
	
				}); */
			});
		});

		describe("concurrency", () => {
			beforeEach(async () => {
				// 0 and 2 not connected
				session = await TestSession.connected(3, {
					services: {
						pubsub: (c) =>
							new DirectSub(c, {
								canRelayMessage: true,
								connectionManager: false,
							}),
					},
				});

				streams = [];
				for (const peer of session.peers) {
					streams.push(createMetrics(peer.services.pubsub));
				}
			});

			afterEach(async () => {
				await session.stop();
			});

			it("publish", async () => {
				await session.connect();

				// Purpose of this test is to check if there exist any dead-locks
				// possibly than can arise from bi-directional writing (?)
				// for examples, is processRpc does result in sending a message back to the same sender
				// it could cause issues. The exact circumstances/reasons for this is unknown, not specified

				await subscribAndWait(session, TOPIC);
				const hasData = (d: Uint8Array, i: number) => {
					return !!streams[i].received.find((x) => equals(x.data, d));
				};
				const fn = async (i: number) => {
					const d = randomBytes(999);
					await streams[i % 3].stream.publish(d, {
						mode: new SilentDelivery({
							to: [
								streams[(i + 1) % session.peers.length].stream.publicKeyHash,
								streams[(i + 2) % session.peers.length].stream.publicKeyHash,
							],
							redundancy: 1,
						}),
						topics: [TOPIC],
					});
					streams.forEach((s) => {
						checkShortestPathIsNeighbours(s.stream);
					});

					expect(hasData(d, i % session.peers.length)).to.be.false;
					await waitFor(() => hasData(d, (i + 1) % session.peers.length));
					await waitFor(() => hasData(d, (i + 2) % session.peers.length));
				};
				let p: Promise<any>[] = [];
				for (let i = 0; i < 100; i++) {
					p.push(fn(i));
				}
				await Promise.all(p);
			});

			it("subscribe", async () => {
				await session.connect();

				let totalAmountOfTopics = 100;
				for (let i = 0; i < totalAmountOfTopics; i++) {
					streams[0].stream.subscribe(String(totalAmountOfTopics - i - 1));
					streams[1].stream.subscribe(String(i));
					streams[2].stream.subscribe(String(i));
				}

				for (let i = 0; i < totalAmountOfTopics; i++) {
					await waitForResolved(() => {
						expect(
							streams[0].stream.topics
								.get(String(i))
								?.has(streams[1].stream.publicKeyHash),
						).to.be.true;
						expect(
							streams[0].stream.topics
								.get(String(i))
								?.has(streams[2].stream.publicKeyHash),
						).to.be.true;
						expect(
							streams[1].stream.topics
								.get(String(i))
								?.has(streams[0].stream.publicKeyHash),
						).to.be.true;
						expect(
							streams[1].stream.topics
								.get(String(i))
								?.has(streams[2].stream.publicKeyHash),
						).to.be.true;
						expect(
							streams[2].stream.topics
								.get(String(i))
								?.has(streams[0].stream.publicKeyHash),
						).to.be.true;
						expect(
							streams[2].stream.topics
								.get(String(i))
								?.has(streams[1].stream.publicKeyHash),
						).to.be.true;
					});
				}
			});

			it("subscribe and connect", async () => {
				let totalAmountOfTopics = 100;

				for (let i = 0; i < totalAmountOfTopics; i++) {
					streams[0].stream.subscribe(String(totalAmountOfTopics - i - 1));
					streams[1].stream.subscribe(String(i));
					streams[2].stream.subscribe(String(i));
				}

				session.connect();

				for (let i = 0; i < totalAmountOfTopics; i++) {
					await waitForResolved(() => {
						expect(
							streams[0].stream.topics
								.get(String(i))
								?.has(streams[1].stream.publicKeyHash),
						).to.be.true;
						expect(
							streams[0].stream.topics
								.get(String(i))
								?.has(streams[2].stream.publicKeyHash),
						).to.be.true;
						expect(
							streams[1].stream.topics
								.get(String(i))
								?.has(streams[0].stream.publicKeyHash),
						).to.be.true;
						expect(
							streams[1].stream.topics
								.get(String(i))
								?.has(streams[2].stream.publicKeyHash),
						).to.be.true;
						expect(
							streams[2].stream.topics
								.get(String(i))
								?.has(streams[0].stream.publicKeyHash),
						).to.be.true;
						expect(
							streams[2].stream.topics
								.get(String(i))
								?.has(streams[1].stream.publicKeyHash),
						).to.be.true;
					});
				}
			});
		});
		describe("mode", () => {
			beforeEach(async () => {
				// 0 and 2 not connected
				session = await TestSession.connected(2, {
					services: {
						pubsub: (c) =>
							new DirectSub(c, {
								canRelayMessage: true,
								connectionManager: false,
							}),
					},
				});
				streams = [];
				for (const peer of session.peers) {
					streams.push(createMetrics(peer.services.pubsub));
				}
				await waitForNeighbour(streams[0].stream, streams[1].stream);
			});

			afterEach(async () => {
				await session.stop();
			});

			it("acknowledges even if not subscribing", async () => {
				await streams[0].stream.publish(new Uint8Array([0]), {
					topics: ["topic"],
					mode: new AcknowledgeDelivery({
						to: [streams[1].stream.publicKeyHash],
						redundancy: 1,
					}),
				});
			});

			it("rejects when silent delivery to a peer that does not exist", async () => {
				await streams[0].stream.publish(new Uint8Array([0]), {
					topics: ["topic"],
					mode: new SilentDelivery({
						to: [(await Ed25519Keypair.create()).publicKey.hashcode()],
						redundancy: 1,
					}),
				});
			});
		});
	});

	describe("events", () => {
		let session: TestSession<{ pubsub: DirectSub }>;

		beforeEach(async () => {
			session = await TestSession.disconnected(2, {
				services: {
					pubsub: (c) =>
						new DirectSub(c, {
							canRelayMessage: true,
							connectionManager: false,
						}),
				},
			});
		});

		afterEach(async () => {
			await session.stop();
		});

		it("publish", async () => {
			const dataMessages: DataEvent[] = [];
			session.peers[0].services.pubsub.addEventListener("publish", (e) => {
				dataMessages.push(e.detail);
			});
			await session.peers[0].services.pubsub.publish(
				new Uint8Array([1, 2, 3]),
				{
					mode: new SilentDelivery({
						to: [session.peers[1].peerId],
						redundancy: 1,
					}),
					topics: ["abc"],
				},
			);
			expect(dataMessages).to.have.length(1);
			expect(dataMessages[0]).to.be.instanceOf(PublishEvent);
			expect(dataMessages[0].data.data).to.deep.equal(
				new Uint8Array([1, 2, 3]),
			);
		});
	});

	describe("routing", () => {
		describe("fork", () => {
			/* 
			┌─┐   
			│0│   
			└┬┘   
			┌▽┐   
			│1│   
			└┬┘   
			┌▽───┐
			│2   │ // we will test whether 2 will send messages correctly to 3 or 4 depending on what topics 3 and 4 are subscribing to
			└┬──┬┘
			┌▽┐┌▽┐
			│3││4│
			└─┘└─┘
			
			*/

			let session: TestSession<{ pubsub: DirectSub }>;
			let streams: ReturnType<typeof createMetrics>[];

			const data = new Uint8Array([1, 2, 3]);
			const TOPIC = "world";
			before(async () => {});
			beforeEach(async () => {
				session = await TestSession.disconnected(5, {
					services: {
						pubsub: (c) =>
							new DirectSub(c, {
								canRelayMessage: true,
								connectionManager: false,
							}),
					},
				});

				streams = [];
				for (const [i, peer] of session.peers.entries()) {
					streams.push(createMetrics(peer.services.pubsub));
					if (i === 3) {
						peer.services.pubsub.subscribe(TOPIC);
					}
				}

				await session.connect([
					[session.peers[0], session.peers[1]],
					[session.peers[1], session.peers[2]],
					[session.peers[2], session.peers[3]],
					[session.peers[2], session.peers[4]],
				]);

				await waitForNeighbour(
					session.peers[0].services.pubsub,
					session.peers[1].services.pubsub,
				);
				await waitForNeighbour(
					session.peers[1].services.pubsub,
					session.peers[2].services.pubsub,
				);
				await waitForNeighbour(
					session.peers[2].services.pubsub,
					session.peers[3].services.pubsub,
				);
				await waitForNeighbour(
					session.peers[2].services.pubsub,
					session.peers[4].services.pubsub,
				);

				for (const [i, peer] of streams.entries()) {
					if (i !== 3) {
						await peer.stream.requestSubscribers(TOPIC);
					}
					await waitFor(
						() => peer.stream.topics.get(TOPIC)?.size === (i === 3 ? 0 : 1),
					); // all others (except 4 which is not subscribing)
				}
				await waitForResolved(() =>
					expect(streams[0].stream.routes.count()).equal(4),
				);
			});

			afterEach(async () => {
				await Promise.all(streams.map((peer) => peer.stream.stop()));
				await session.stop();
			});
			after(async () => {});

			it("will publish on routes", async () => {
				streams[3].received = [];
				streams[4].received = [];

				await streams[0].stream.publish(data, { topics: [TOPIC] });
				await waitFor(() => streams[3].received.length === 1);
				expect(new Uint8Array(streams[3].received[0].data)).to.deep.equal(data);

				await delay(1000); // some delay to allow all messages to progagate
				expect(streams[4].received).to.be.empty;
				// make sure data message did not arrive to peer 4
				for (const message of streams[4].messages) {
					if (message instanceof DataMessage && message.data) {
						const pubsubMessage = deserialize(message.data, PubSubMessage);
						expect(pubsubMessage).not.to.be.instanceOf(PubSubData);
					}
				}
			});
		});

		describe("3", () => {
			let session: TestSession<{ pubsub: DirectSub }>;
			let streams: ReturnType<typeof createMetrics>[];

			const data = new Uint8Array([1, 2, 3]);
			const TOPIC = "world";
			before(async () => {});
			beforeEach(async () => {
				session = await TestSession.disconnected(3, {
					services: {
						pubsub: (c) =>
							new DirectSub(c, {
								canRelayMessage: true,
								connectionManager: false,
							}),
					},
				});
				streams = [];

				for (const [_i, peer] of session.peers.entries()) {
					streams.push(createMetrics(peer.services.pubsub));
				}
			});

			afterEach(async () => {
				await Promise.all(streams.map((peer) => peer.stream.stop()));
				await session.stop();
			});
			after(async () => {});

			it("line", async () => {
				/* 
				┌─┐   
				│0│   // Sender of message
				└┬┘   
				┌▽┐   
				│1│   // Subscribes to topic
				└┬┘   
				┌▽┐   
				│2│   // Does not subscribe 
				└─┘  
				*/

				await session.connect([
					[session.peers[0], session.peers[1]],
					[session.peers[1], session.peers[2]],
				]);

				for (const [i, peer] of session.peers.entries()) {
					if (i === 1) {
						peer.services.pubsub.subscribe(TOPIC);
					}
				}

				for (const [i, peer] of streams.entries()) {
					if (i !== 1) {
						await peer.stream.requestSubscribers(TOPIC);
					}
					await waitForResolved(() =>
						expect(peer.stream.getSubscribers(TOPIC)).to.have.length(1),
					); // all others (except 4 which is not subscribing)
				}

				streams[1].received = [];
				streams[2].received = [];
				await streams[0].stream.publish(data, { topics: [TOPIC] });
				await waitFor(() => streams[1].received.length === 1);
				expect(new Uint8Array(streams[1].received[0].data)).to.deep.equal(data);

				await delay(1000); // some delay to allow all messages to progagate
				expect(streams[2].received).to.be.empty;

				// make sure data message did not arrive to peer 4
				for (const message of streams[2].messages) {
					if (message instanceof DataMessage && message.data) {
						const pubsubMessage = deserialize(message.data, PubSubMessage);
						expect(pubsubMessage).not.to.be.instanceOf(PubSubData);
					}
				}
			});

			it("fully connected", async () => {
				await session.connect([
					[session.peers[0], session.peers[1]],
					[session.peers[1], session.peers[2]],
					[session.peers[0], session.peers[2]],
				]);

				await delay(5000);

				for (const [_i, peer] of session.peers.entries()) {
					await peer.services.pubsub.subscribe(TOPIC);
				}
				for (const [_i, peer] of streams.entries()) {
					await waitForResolved(() =>
						expect(peer.stream.getSubscribers(TOPIC)).to.have.length(3),
					); // all others (except 4 which is not subscribing)
				}

				await streams[0].stream.publish(data, {
					topics: [TOPIC],
					mode: new SilentDelivery({
						to: streams.map((x) => x.stream.publicKeyHash),
						redundancy: 1,
					}),
				});

				await delay(3000);

				await streams[0].stream.publish(data, {
					topics: [TOPIC],
					mode: new SilentDelivery({
						to: streams.map((x) => x.stream.publicKeyHash),
						redundancy: 1,
					}),
				});
				await streams[0].stream.publish(data, {
					topics: [TOPIC],
					mode: new SilentDelivery({
						to: streams.map((x) => x.stream.publicKeyHash),
						redundancy: 1,
					}),
				});

				await delay(3000);

				streams[1].received = [];
				streams[2].received = [];

				await streams[0].stream.publish(data, {
					topics: [TOPIC],
					mode: new SilentDelivery({
						to: streams.map((x) => x.stream.publicKeyHash),
						redundancy: 1,
					}),
				});
				await waitFor(() => streams[1].received.length === 1);
				await waitFor(() => streams[2].received.length === 1);

				expect(new Uint8Array(streams[1].received[0].data)).to.deep.equal(data);
				expect(new Uint8Array(streams[2].received[0].data)).to.deep.equal(data);

				await delay(1000); // some delay to allow all messages to progagate
				expect(streams[1].received).to.have.length(1);
				expect(streams[2].received).to.have.length(1);
			});
		});
	});

	// test sending "0" to "3" only 1 message should appear even though not in strict mode

	describe("join/leave", () => {
		let session: TestSession<{ pubsub: DirectSub }>;
		let streams: ReturnType<typeof createMetrics>[] = [];
		const TOPIC_1 = "topic";

		beforeEach(async () => {
			streams = [];
			session = await TestSession.disconnected(3, {
				services: {
					pubsub: (c) =>
						new DirectSub(c, {
							canRelayMessage: true,
							connectionManager: false,
						}),
				},
			});
			for (const peer of session.peers) {
				streams.push(createMetrics(peer.services.pubsub));
			}
		});
		afterEach(async () => {
			await session.stop();
		});

		const checkSubscriptions = async () => {
			await waitForResolved(
				() =>
					expect(
						streams[0].stream.topics
							.get(TOPIC_1)
							?.has(streams[1].stream.publicKeyHash),
					).to.be.true,
			);
			await waitForResolved(
				() =>
					expect(
						streams[1].stream.topics
							.get(TOPIC_1)
							?.has(streams[0].stream.publicKeyHash),
					).to.be.true,
			);

			streams[1].received = [];
			await streams[0].stream.publish(new Uint8Array([1, 2, 3]), {
				topics: [TOPIC_1],
			});
			await waitForResolved(
				() => expect(streams[1].received).to.have.length(1),
				{
					timeout: 2000,
					delayInterval: 50,
				},
			);
		};

		it("join then subscribe", async () => {
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
				[session.peers[0], session.peers[2]],
			]);

			streams[0].stream.subscribe(TOPIC_1);
			streams[1].stream.subscribe(TOPIC_1);
			await checkSubscriptions();
		});

		it("subscribe then join", async () => {
			streams[0].stream.subscribe(TOPIC_1);
			streams[1].stream.subscribe(TOPIC_1);

			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
				[session.peers[0], session.peers[2]],
			]);

			await checkSubscriptions();
		});

		it("join subscribe join", async () => {
			await streams[0].stream.subscribe(TOPIC_1);
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
				[session.peers[0], session.peers[2]],
			]);
			await streams[1].stream.subscribe(TOPIC_1);
			await checkSubscriptions();
		});

		it("rejoin will re-emit subscriptions", async () => {
			const subscription0: SubscriptionEvent[] = [];

			streams[0].stream.addEventListener("subscribe", (e) => {
				subscription0.push(e.detail);
			});

			const subscription1: SubscriptionEvent[] = [];

			streams[1].stream.addEventListener("subscribe", (e) => {
				subscription1.push(e.detail);
			});

			await streams[0].stream.subscribe(TOPIC_1);
			await streams[1].stream.subscribe(TOPIC_1);

			await session.connect([
				[session.peers[0], session.peers[2]],
				[session.peers[1], session.peers[2]],
			]);

			await checkSubscriptions();

			await waitForResolved(() =>
				expect(subscription0.map((x) => x.from.hashcode())).to.deep.equal([
					streams[1].stream.publicKeyHash,
				]),
			);
			await waitForResolved(() =>
				expect(subscription1.map((x) => x.from.hashcode())).to.deep.equal([
					streams[0].stream.publicKeyHash,
				]),
			);

			await session.peers[0].stop();

			await session.peers[0].start();

			await session.connect([[session.peers[0], session.peers[2]]]);

			await waitForResolved(() =>
				expect(session.peers[0].services.pubsub.peers.size).equal(1),
			);

			await streams[0].stream.subscribe(TOPIC_1);

			await waitForResolved(() =>
				expect(subscription0.map((x) => x.from.hashcode())).to.deep.equal([
					streams[1].stream.publicKeyHash,
					streams[1].stream.publicKeyHash,
				]),
			);
			await waitForResolved(() =>
				expect(subscription1.map((x) => x.from.hashcode())).to.deep.equal([
					streams[0].stream.publicKeyHash,
					streams[0].stream.publicKeyHash,
				]),
			);

			await checkSubscriptions();
		});

		it("rejoin with different subscriptions", async () => {
			await streams[0].stream.subscribe("a");
			await streams[1].stream.subscribe("a");

			await session.connect([[session.peers[0], session.peers[1]]]);
			await delay(1000); /// TODO remove when https://github.com/ChainSafe/js-libp2p-yamux/issues/72 fixed
			await session.peers[0].stop();
			await session.peers[0].start();
			await streams[0].stream.subscribe("b");
			await streams[1].stream.subscribe("b");

			await session.connect([[session.peers[0], session.peers[1]]]);

			await waitForResolved(() => {
				expect(streams[0].stream.topics.get("b")?.size || 0).equal(1);
				expect(streams[1].stream.topics.get("b")?.size || 0).equal(1);
				expect(streams[0].stream.topics.get("a")?.size || 0).equal(0);
				expect(streams[1].stream.topics.get("a")?.size || 0).equal(0);
			});
		});

		it("can handle direct connection drop", async () => {
			await session.connect();
			await streams[0].stream.subscribe("a");
			await streams[1].stream.subscribe("a");

			await waitForResolved(() => {
				expect(streams[0].stream.topics.get("a")?.size || 0).equal(1);
				expect(streams[1].stream.topics.get("a")?.size || 0).equal(1);
			});

			await delay(3000); // wait for all Subscribe message to have propagated in the network
			await session.peers[0].hangUp(session.peers[1].peerId);

			// when https://github.com/libp2p/js-libp2p/issues/2623 fixed, set to equal 2
			// right now we will dial relayed addresses before direct hence also establishing a connection to the relay

			await waitForResolved(() =>
				expect(streams[0].stream.peers.size).to.be.greaterThanOrEqual(1),
			);
			await waitForResolved(() =>
				expect(streams[1].stream.peers.size).to.be.greaterThanOrEqual(1),
			);
			await waitForResolved(() => {
				expect(
					streams[0].stream.topics.get("a")?.size || 0,
				).to.be.greaterThanOrEqual(1);
				expect(
					streams[1].stream.topics.get("a")?.size || 0,
				).to.be.greaterThanOrEqual(1);
			});

			/* await waitForResolved(() =>
				expect(streams[0].stream.peers.size).equal(1),
			);
			await waitForResolved(() =>
				expect(streams[1].stream.peers.size).equal(1),
			);
			await waitForResolved(() => {
				expect(streams[0].stream.topics.get("a")?.size || 0).equal(1);
				expect(streams[1].stream.topics.get("a")?.size || 0).equal(1);
			}); */
		});
	});

	describe("subscription", () => {
		let session: TestSession<{ pubsub: DirectSub }>;
		let streams: ReturnType<typeof createMetrics>[];
		const TOPIC_1 = "hello";
		const TOPIC_2 = "world";

		beforeEach(async () => {
			// 0 and 2 not connected
			session = await TestSession.disconnected(3, {
				transports: [tcp(), webSockets({ filter: filters.all })],
				services: {
					pubsub: (c) =>
						new DirectSub(c, {
							canRelayMessage: true,
							connectionManager: false,
							seekTimeout: 5000, // set seekTimeout to make GoodBye/leaving events to take effect faster
						}),
				},
			});

			/* 
			┌─┐
			│0│
			└┬┘
			┌▽┐
			│1│
			└┬┘
			┌▽┐
			│2│
			└─┘
			*/

			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);
			streams = [];
			for (const peer of session.peers) {
				streams.push(createMetrics(peer.services.pubsub));
			}

			await waitForNeighbour(streams[0].stream, streams[1].stream);
			await waitForNeighbour(streams[1].stream, streams[2].stream);
		});

		afterEach(async () => {
			await Promise.all(streams.map((peer) => peer.stream.stop()));
			await session.stop();
		});

		/* TMP disable flaky test for Github CI
		it("concurrent", async () => {
			await session.peers[1].stop();
			await session.peers[1].start();
			streams[0].stream.subscribe(TOPIC_1);
			streams[2].stream.subscribe(TOPIC_1);

			expect(session.peers[1].services.pubsub.peers.size).equal(0);
			session.connect([[session.peers[1], session.peers[2]]]);
			session.connect([[session.peers[0], session.peers[1]]]);

			await waitForResolved(
				() =>
					expect(
						streams[0].stream.topics
							.get(TOPIC_1)
							?.has(streams[2].stream.publicKeyHash),
					).to.be.true,
			);
			await waitForResolved(
				() =>
					expect(
						streams[2].stream.topics
							.get(TOPIC_1)
							?.has(streams[0].stream.publicKeyHash),
					).to.be.true,
			);
			expect(session.peers[1].services.pubsub.peers.size).equal(2);
		}); */

		it("it can track subscriptions across peers", async () => {
			for (const peer of streams) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}

			await streams[0].stream.subscribe(TOPIC_1);
			await waitFor(() =>
				streams[2].stream.topics
					.get(TOPIC_1)
					?.has(streams[0].stream.publicKeyHash),
			);
			await waitFor(() =>
				streams[1].stream.topics
					.get(TOPIC_1)
					?.has(streams[0].stream.publicKeyHash),
			);
			expect(streams[2].subscriptionEvents).to.have.length(1);
			expect(streams[1].subscriptionEvents).to.have.length(1);
			expect(
				streams[2].subscriptionEvents[0].from.equals(
					streams[0].stream.publicKey,
				),
			).to.be.true;
			expect(streams[2].subscriptionEvents[0].subscriptions).to.have.length(1);
			expect(streams[2].subscriptionEvents[0].subscriptions[0]).equal(TOPIC_1);

			await delay(2000);
			await streams[0].stream.stop();
			await waitFor(
				() =>
					!streams[1].stream.topics
						.get(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash),
			);
			await waitFor(
				() =>
					!streams[2].stream.topics
						.get(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash),
				{
					timeout: 20 * 1000,
				},
			);

			expect(streams[2].subscriptionEvents).to.have.length(1);
			expect(streams[1].subscriptionEvents).to.have.length(1);
			expect(streams[2].unsubscriptionEvents).to.have.length(1);
			expect(streams[1].unsubscriptionEvents).to.have.length(1);
			expect(
				streams[2].unsubscriptionEvents[0].from.equals(
					streams[0].stream.publicKey,
				),
			).to.be.true;
			expect(streams[2].unsubscriptionEvents[0].unsubscriptions).to.have.length(
				1,
			);
			expect(streams[2].unsubscriptionEvents[0].unsubscriptions[0]).equal(
				TOPIC_1,
			);
		});

		it("can unsubscribe across peers", async () => {
			for (const peer of streams) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}

			streams[0].stream.subscribe(TOPIC_1);
			streams[0].stream.subscribe(TOPIC_2);

			await waitFor(() =>
				streams[2].stream.topics
					.get(TOPIC_1)
					?.has(streams[0].stream.publicKeyHash),
			);

			await waitFor(() =>
				streams[1].stream.topics
					.get(TOPIC_1)
					?.has(streams[0].stream.publicKeyHash),
			);

			await waitFor(() =>
				streams[2].stream.topics
					.get(TOPIC_2)
					?.has(streams[0].stream.publicKeyHash),
			);

			await waitFor(() =>
				streams[1].stream.topics
					.get(TOPIC_2)
					?.has(streams[0].stream.publicKeyHash),
			);

			expect(streams[2].subscriptionEvents).to.have.length(2);
			expect(streams[1].subscriptionEvents).to.have.length(2);
			expect(
				streams[2].subscriptionEvents[0].from.equals(
					streams[0].stream.publicKey,
				),
			).to.be.true;
			expect(streams[2].subscriptionEvents[0].subscriptions).to.have.length(1);
			expect(streams[2].subscriptionEvents[0].subscriptions[0]).equal(TOPIC_1);

			expect(streams[2].subscriptionEvents[1].subscriptions).to.have.length(1);
			expect(streams[2].subscriptionEvents[1].subscriptions[0]).equal(TOPIC_2);

			await delay(8000);

			await streams[0].stream.unsubscribe(TOPIC_1);
			await waitFor(
				() =>
					!streams[2].stream.topics
						.get(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash),
			);
			await waitFor(
				() =>
					!streams[1].stream.topics
						.get(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash),
			);
			await waitFor(() =>
				streams[2].stream.topics
					.get(TOPIC_2)
					?.has(streams[0].stream.publicKeyHash),
			);
			await waitFor(() =>
				streams[1].stream.topics
					.get(TOPIC_2)
					?.has(streams[0].stream.publicKeyHash),
			);
			expect(streams[2].unsubscriptionEvents).to.have.length(1);
			expect(streams[1].unsubscriptionEvents).to.have.length(1);
			expect(
				streams[2].unsubscriptionEvents[0].from.equals(
					streams[0].stream.publicKey,
				),
			).to.be.true;
			expect(streams[2].unsubscriptionEvents[0].unsubscriptions).to.have.length(
				1,
			);
			expect(streams[2].unsubscriptionEvents[0].unsubscriptions[0]).equal(
				TOPIC_1,
			);
			streams[0].stream.unsubscribe(TOPIC_2);
			await waitFor(
				() =>
					!streams[2].stream.topics
						.get(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash),
			);
			await waitFor(
				() =>
					!streams[1].stream.topics
						.get(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash),
			);
			await waitFor(
				() =>
					!streams[2].stream.topics
						.get(TOPIC_2)
						?.has(streams[0].stream.publicKeyHash),
			);
			await waitFor(
				() =>
					!streams[1].stream.topics
						.get(TOPIC_2)
						?.has(streams[0].stream.publicKeyHash),
			);
			expect(streams[2].unsubscriptionEvents).to.have.length(2);
			expect(streams[1].unsubscriptionEvents).to.have.length(2);
			expect(
				streams[2].unsubscriptionEvents[1].from.equals(
					streams[0].stream.publicKey,
				),
			).to.be.true;
			expect(streams[2].unsubscriptionEvents[1].unsubscriptions).to.have.length(
				1,
			);
			expect(streams[2].unsubscriptionEvents[1].unsubscriptions[0]).equal(
				TOPIC_2,
			);
		});

		it("can handle multiple subscriptions", async () => {
			for (const peer of streams) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}
			streams[0].stream.subscribe(TOPIC_1); // 1
			streams[0].stream.subscribe(TOPIC_1); // 2
			streams[0].stream.subscribe(TOPIC_1); // 3

			await waitFor(() =>
				streams[2].stream.topics
					.get(TOPIC_1)
					?.has(streams[0].stream.publicKeyHash),
			);
			await waitFor(() =>
				streams[1].stream.topics
					.get(TOPIC_1)
					?.has(streams[0].stream.publicKeyHash),
			);
			streams[0].stream.unsubscribe(TOPIC_1); // 3
			streams[0].stream.unsubscribe(TOPIC_1); // 2
			await delay(3000); // allow some communications
			await waitFor(() =>
				streams[2].stream.topics
					.get(TOPIC_1)
					?.has(streams[0].stream.publicKeyHash),
			);
			await waitFor(() =>
				streams[1].stream.topics
					.get(TOPIC_1)
					?.has(streams[0].stream.publicKeyHash),
			);
			await streams[0].stream.unsubscribe(TOPIC_1); // 1
			await waitFor(
				() =>
					!streams[2].stream.topics
						.get(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash),
			);
			await waitFor(
				() =>
					!streams[1].stream.topics
						.get(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash),
			);
		});

		it("resubscription will not emit uncessary message", async () => {
			// Subscribe with some metadata
			let sentMessages = 0;
			const publishMessage = streams[0].stream.publishMessage.bind(
				streams[0].stream,
			);
			streams[0].stream.publishMessage = async (a: any, b: any, c: any) => {
				sentMessages += 1;
				return publishMessage(a, b, c);
			};
			await streams[0].stream.subscribe(TOPIC_1);
			expect(sentMessages).equal(1);
			await streams[0].stream.subscribe(TOPIC_1);
			expect(sentMessages).equal(1); // no new messages sent
		});

		it("requesting subscribers will not overwrite subscriptions", async () => {
			for (const peer of streams) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}

			// Subscribe with some metadata

			await streams[0].stream.subscribe(TOPIC_1);

			await waitForResolved(
				() =>
					expect(
						streams[2].stream.topics
							.get(TOPIC_1)
							?.get(streams[0].stream.publicKeyHash),
					).to.exist,
			);
			await waitForResolved(
				() =>
					expect(
						streams[1].stream.topics
							.get(TOPIC_1)
							?.get(streams[0].stream.publicKeyHash),
					).to.exist,
			);

			// Request subscribers and makes sure we don't get any wierd overwrites
			await streams[1].stream.requestSubscribers(TOPIC_1);
			await streams[2].stream.requestSubscribers(TOPIC_1);

			await delay(3000); // wait for some messages
			expect(
				streams[1].stream.topics
					.get(TOPIC_1)
					?.get(streams[0].stream.publicKeyHash),
			).to.exist;
			expect(
				streams[2].stream.topics
					.get(TOPIC_1)
					?.get(streams[0].stream.publicKeyHash),
			).to.exist;
			expect(streams[1].subscriptionEvents).to.have.length(1); // Emits are only the unique ones
			expect(streams[2].subscriptionEvents).to.have.length(1); // Emits are only the unique ones
		});

		describe("invalidation", () => {
			it("uses timestamp to ignore old events", async () => {
				const pubsubMetrics0 = createSubscriptionMetrics(streams[0].stream);
				const pubsubMetrics1 = createSubscriptionMetrics(streams[1].stream);
				const pubsubMetrics2 = createSubscriptionMetrics(streams[2].stream);

				await streams[1].stream.requestSubscribers(TOPIC_1);

				await waitForResolved(() =>
					expect(pubsubMetrics0.getSubscriptions).to.have.length(1),
				);

				pubsubMetrics1.subscriptions = [];

				await streams[0].stream.subscribe(TOPIC_1);
				await waitForResolved(
					() => expect(pubsubMetrics1.subscriptions).to.have.length(2), // one Subscribe message to this and one that is relayed to 2
				);

				await waitForResolved(
					() => expect(pubsubMetrics2.subscriptions).to.have.length(2), // one Subscribe message to this and one that is sent once this node becomes reachable
				);

				expect(streams[1].stream.topics.get(TOPIC_1)!.size).equal(1);

				await streams[0].stream.unsubscribe(TOPIC_1);
				await waitForResolved(() =>
					expect(pubsubMetrics1.unsubscriptions).to.have.length(1),
				);

				expect(streams[1].stream.topics.get(TOPIC_1)!.size).equal(0);

				// reprocess first subscription message and make sure its ignored
				await streams[1].stream.onDataMessage(
					session.peers[0].services.pubsub.publicKey,
					[...streams[1].stream.peers.values()][0],
					pubsubMetrics1.subscriptions[0],
					0,
				);

				expect(streams[1].stream.topics.get(TOPIC_1)!.size).equal(0);

				// resubscribe again and try to send old unsubscription
				pubsubMetrics1.subscriptions = [];
				await streams[0].stream.subscribe(TOPIC_1);
				await waitForResolved(() =>
					expect(pubsubMetrics1.subscriptions).to.have.length(1),
				);
				expect(streams[1].stream.topics.get(TOPIC_1)!.size).equal(1);

				await streams[1].stream.onDataMessage(
					session.peers[0].services.pubsub.publicKey,
					[...streams[1].stream.peers.values()][0],
					pubsubMetrics1.unsubscriptions[0],
					0,
				);
				expect(streams[1].stream.topics.get(TOPIC_1)!.size).equal(1); // No change, since message was old

				expect(streams[1].stream.lastSubscriptionMessages.size).equal(1);
				await session.peers[0].stop();
				await waitForResolved(() =>
					expect(streams[1].stream.lastSubscriptionMessages.size).equal(0),
				);
			});

			it("will clear lastSubscriptionMessages on unsubscribe", async () => {
				await streams[1].stream.requestSubscribers(TOPIC_1);

				await streams[0].stream.subscribe(TOPIC_1);
				await waitForResolved(() =>
					expect(streams[1].stream.topics.get(TOPIC_1)!.size).equal(1),
				);
				expect(streams[1].stream.lastSubscriptionMessages.size).equal(1);
				let dummyPeer = "x";
				streams[1].stream.lastSubscriptionMessages.set(dummyPeer, new Map());
				expect(streams[1].stream.lastSubscriptionMessages.size).equal(2);

				await streams[1].stream.unsubscribe(TOPIC_1);
				expect(streams[1].stream.lastSubscriptionMessages.size).equal(1);

				streams[1].stream.lastSubscriptionMessages.delete(dummyPeer);
				expect(streams[1].stream.lastSubscriptionMessages.size).equal(0);
			});
		});
	});
});
