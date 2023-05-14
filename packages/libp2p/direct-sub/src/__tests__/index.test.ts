import {
	DataMessage,
	Message,
	waitForPeers,
} from "@dao-xyz/libp2p-direct-stream";
import { LSession } from "@dao-xyz/libp2p-test-utils";
import { waitFor, delay } from "@dao-xyz/peerbit-time";
import { PubSubMessage, PubSubData } from "../messages.js";
import {
	DirectSub,
	SubscriptionEvent,
	UnsubcriptionEvent,
} from "./../index.js";
import { deserialize } from "@dao-xyz/borsh";
import { equals } from "uint8arrays";

describe("pubsub", function () {
	describe("topic", () => {
		let session: LSession<{ pubsub: DirectSub }>;
		let peers: {
			stream: DirectSub;
			messages: Message[];
			recieved: PubSubData[];
		}[] = [];

		beforeEach(async () => {
			peers = [];
			session = await LSession.disconnected(3, {
				services: {
					pubsub: (c) =>
						new DirectSub(c, {
							canRelayMessage: true,
							connectionManager: { autoDial: false },
						}),
				},
			});
		});
		afterEach(async () => {
			await session.stop();
		});

		it("can share topics when connecting after subscribe, 2 peers", async () => {
			let streams: DirectSub[] = [];
			for (const peer of session.peers.slice(0, 2)) {
				const client: {
					stream: DirectSub;
					messages: Message[];
					recieved: PubSubData[];
				} = {
					messages: [],
					recieved: [],
					stream: peer.services.pubsub,
				};

				peers.push(client);
				peer.services.pubsub.addEventListener("message", (msg) => {
					client.messages.push(msg.detail);
				});
				peer.services.pubsub.addEventListener("data", (msg) => {
					client.recieved.push(msg.detail);
				});
				streams.push(peer.services.pubsub);
			}

			const TOPIC = "world";
			peers[0].stream.subscribe(TOPIC);
			peers[1].stream.subscribe(TOPIC);

			await delay(1000); // wait for subscription message to propagate (if any)
			// now connect peers and make sure that subscription information is passed on as they connect
			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitFor(() =>
				peers[0].stream
					.getSubscribers(TOPIC)
					?.has(peers[1].stream.publicKeyHash)
			);
			await waitFor(() =>
				peers[1].stream
					.getSubscribers(TOPIC)
					?.has(peers[0].stream.publicKeyHash)
			);

			await Promise.all(streams.map((s) => s.stop()));
		});

		it("can share topics when connecting after subscribe, 3 peers and 1 relay", async () => {
			let peers: {
				stream: DirectSub;
				messages: Message[];
				recieved: PubSubData[];
			}[] = [];
			for (const peer of session.peers) {
				const client: {
					stream: DirectSub;
					messages: Message[];
					recieved: PubSubData[];
				} = {
					messages: [],
					recieved: [],
					stream: peer.services.pubsub,
				};

				peers.push(client);
				peer.services.pubsub.addEventListener("message", (msg) => {
					client.messages.push(msg.detail);
				});
				peer.services.pubsub.addEventListener("data", (msg) => {
					client.recieved.push(msg.detail);
				});
				await peer.services.pubsub.start();
			}

			const TOPIC = "world";
			peers[0].stream.subscribe(TOPIC);
			// peers[1] is not subscribing
			peers[2].stream.subscribe(TOPIC);

			await delay(1000); // wait for subscription message to propagate (if any)
			// now connect peers and make sure that subscription information is passed on as they connect
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);
			await waitFor(() =>
				peers[0].stream
					.getSubscribers(TOPIC)
					?.has(peers[2].stream.publicKeyHash)
			);
			await waitFor(() =>
				peers[2].stream
					.getSubscribers(TOPIC)
					?.has(peers[0].stream.publicKeyHash)
			);
			await Promise.all(peers.map((x) => x.stream.stop()));
		});
	});

	describe("publish", () => {
		let session: LSession<{ pubsub: DirectSub }>;
		let peers: {
			stream: DirectSub;
			messages: Message[];
			recieved: PubSubData[];
		}[];
		const data = new Uint8Array([1, 2, 3]);
		const TOPIC = "world";

		beforeEach(async () => {
			// 0 and 2 not connected
			session = await LSession.disconnected(3, {
				services: {
					pubsub: (c) =>
						new DirectSub(c, {
							canRelayMessage: true,
							connectionManager: { autoDial: false },
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
			peers = [];
			for (const peer of session.peers) {
				const client: {
					stream: DirectSub;
					messages: Message[];
					recieved: PubSubData[];
				} = {
					messages: [],
					recieved: [],
					stream: peer.services.pubsub,
				};
				peers.push(client);
				peer.services.pubsub.addEventListener("message", (msg) => {
					client.messages.push(msg.detail);
				});
				peer.services.pubsub.addEventListener("data", (msg) => {
					client.recieved.push(msg.detail);
				});
			}
			await waitForPeers(peers[0].stream, peers[1].stream);
			await waitForPeers(peers[1].stream, peers[2].stream);
			await delay(1000);

			await peers[0].stream.subscribe(TOPIC);
			await peers[1].stream.subscribe(TOPIC);
			await peers[2].stream.subscribe(TOPIC);

			for (let i = 0; i < peers.length; i++) {
				for (let j = 0; j < peers.length; j++) {
					if (i == j) {
						continue;
					}
					await waitFor(() =>
						peers[i].stream
							.getSubscribers(TOPIC)
							?.has(peers[j].stream.publicKeyHash)
					);
				}
			}
		});

		afterEach(async () => {
			for (let i = 0; i < peers.length; i++) {
				peers[i].stream.unsubscribe(TOPIC);
			}
			for (let i = 0; i < peers.length; i++) {
				await waitFor(() => !peers[i].stream.getSubscribers(TOPIC)?.size);
				expect(peers[i].stream.topics.has(TOPIC)).toBeFalse();
				expect(peers[i].stream.subscriptions.has(TOPIC)).toBeFalse();
			}

			await Promise.all(peers.map((peer) => peer.stream.stop()));
			await session.stop();
		});

		it("1->TOPIC", async () => {
			await peers[0].stream.publish(data, { topics: [TOPIC] });
			await waitFor(() => peers[1].recieved.length === 1);
			expect(new Uint8Array(peers[1].recieved[0].data)).toEqual(data);
			expect(peers[1].recieved[0].topics).toEqual([TOPIC]);
			await waitFor(() => peers[2].recieved.length === 1);
			expect(new Uint8Array(peers[2].recieved[0].data)).toEqual(data);
			await delay(3000); // wait some more time to make sure we dont get more messages
			expect(peers[1].recieved).toHaveLength(1);
			expect(peers[2].recieved).toHaveLength(1);
		});

		it("1->TOPIC strict to", async () => {
			await peers[0].stream.publish(data, {
				topics: [TOPIC],
				to: [peers[2].stream.publicKey],
				strict: true,
			});
			await waitFor(() => peers[2].recieved.length === 1);
			expect(new Uint8Array(peers[2].recieved[0].data)).toEqual(data);
			expect(peers[2].recieved[0].topics).toEqual([TOPIC]);
			expect(peers[1].recieved).toHaveLength(0);
			await delay(3000); // wait some more time to make sure we dont get more messages
			expect(peers[1].recieved).toHaveLength(0);
			expect(peers[2].recieved).toHaveLength(1);
		});

		it("send without topic directly", async () => {
			await peers[0].stream.publish(data, {
				to: [peers[1].stream.components.peerId],
			});
			await waitFor(() => peers[1].recieved.length === 1);
			expect(new Uint8Array(peers[1].recieved[0].data)).toEqual(data);
			await delay(3000); // wait some more time to make sure we dont get more messages
			expect(peers[1].recieved).toHaveLength(1);
			expect(peers[2].recieved).toHaveLength(0);
		});

		it("send without topic over relay", async () => {
			await peers[0].stream.publish(data, {
				to: [peers[2].stream.components.peerId],
			});
			await waitFor(() => peers[2].recieved.length === 1);
			expect(new Uint8Array(peers[2].recieved[0].data)).toEqual(data);
			await delay(3000); // wait some more time to make sure we dont get more messages
			expect(peers[2].recieved).toHaveLength(1);
			expect(peers[1].recieved).toHaveLength(0);
		});
		it("can send as non subscribeer", async () => {
			peers[0].stream.unsubscribe(TOPIC);
			peers[1].stream.unsubscribe(TOPIC);
			await peers[0].stream.publish(data, { topics: [TOPIC] });
			await waitFor(() => peers[2].recieved.length === 1);
			expect(new Uint8Array(peers[2].recieved[0].data)).toEqual(data);
			await delay(3000); // wait some more time to make sure we dont get more messages
			expect(peers[1].recieved).toHaveLength(0);
			expect(peers[2].recieved).toHaveLength(1);
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

			let session: LSession<{ pubsub: DirectSub }>;
			let peers: {
				stream: DirectSub;
				messages: Message[];
				recieved: PubSubData[];
			}[];

			const data = new Uint8Array([1, 2, 3]);
			const TOPIC = "world";
			beforeAll(async () => {});
			beforeEach(async () => {
				session = await LSession.disconnected(5, {
					services: {
						pubsub: (c) =>
							new DirectSub(c, {
								canRelayMessage: true,
								connectionManager: { autoDial: false },
							}),
					},
				});

				peers = [];
				for (const [i, peer] of session.peers.entries()) {
					const client: {
						stream: DirectSub;
						messages: Message[];
						recieved: PubSubData[];
					} = {
						messages: [],
						recieved: [],
						stream: peer.services.pubsub,
					};
					peers.push(client);
					peer.services.pubsub.addEventListener("message", (msg) => {
						client.messages.push(msg.detail);
					});
					peer.services.pubsub.addEventListener("data", (msg) => {
						client.recieved.push(msg.detail);
					});

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

				for (const [i, peer] of peers.entries()) {
					if (i !== 3) {
						await peer.stream.requestSubscribers(TOPIC);
					}
					await waitFor(
						() => peer.stream.getSubscribers(TOPIC)?.size === (i === 3 ? 0 : 1)
					); // all others (except 4 which is not subscribing)
				}
			});

			afterEach(async () => {
				await Promise.all(peers.map((peer) => peer.stream.stop()));
				await session.stop();
			});
			afterAll(async () => {});

			it("will publish on routes", async () => {
				peers[3].recieved = [];
				peers[4].recieved = [];
				await peers[0].stream.publish(data, { topics: [TOPIC] });
				await waitFor(() => peers[3].recieved.length === 1);
				expect(new Uint8Array(peers[3].recieved[0].data)).toEqual(data);

				await delay(1000); // some delay to allow all messages to progagate
				expect(peers[4].recieved).toHaveLength(0);
				// make sure data message did not arrive to peer 4
				for (const message of peers[4].messages) {
					if (message instanceof DataMessage) {
						const pubsubMessage = deserialize(message.data, PubSubMessage);
						expect(pubsubMessage).not.toBeInstanceOf(PubSubData);
					}
				}
			});
		});

		describe("line", () => {
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

			let session: LSession<{ pubsub: DirectSub }>;
			let peers: {
				stream: DirectSub;
				messages: Message[];
				recieved: PubSubData[];
			}[];

			const data = new Uint8Array([1, 2, 3]);
			const TOPIC = "world";
			beforeAll(async () => {});
			beforeEach(async () => {
				session = await LSession.disconnected(3, {
					services: {
						pubsub: (c) =>
							new DirectSub(c, {
								canRelayMessage: true,
								connectionManager: { autoDial: false },
							}),
					},
				});
				peers = [];
				for (const [i, peer] of session.peers.entries()) {
					const client: {
						stream: DirectSub;
						messages: Message[];
						recieved: PubSubData[];
					} = {
						messages: [],
						recieved: [],
						stream: peer.services.pubsub,
					};
					peers.push(client);
					peer.services.pubsub.addEventListener("message", (msg) => {
						client.messages.push(msg.detail);
					});
					peer.services.pubsub.addEventListener("data", (msg) => {
						client.recieved.push(msg.detail);
					});

					if (i === 1) {
						peer.services.pubsub.subscribe(TOPIC);
					}
				}

				await session.connect([
					[session.peers[0], session.peers[1]],
					[session.peers[1], session.peers[2]],
				]);

				for (const [i, peer] of peers.entries()) {
					if (i !== 1) {
						await peer.stream.requestSubscribers(TOPIC);
					}
					await waitFor(
						() => peer.stream.getSubscribers(TOPIC)?.size === (i === 1 ? 0 : 1)
					); // all others (except 4 which is not subscribing)
				}
			});

			afterEach(async () => {
				await Promise.all(peers.map((peer) => peer.stream.stop()));
				await session.stop();
			});
			afterAll(async () => {});

			it("will not forward unless necessary", async () => {
				peers[1].recieved = [];
				peers[2].recieved = [];
				await delay(5000);
				await peers[0].stream.publish(data, { topics: [TOPIC] });
				await waitFor(() => peers[1].recieved.length === 1);
				expect(new Uint8Array(peers[1].recieved[0].data)).toEqual(data);

				await delay(1000); // some delay to allow all messages to progagate
				expect(peers[2].recieved).toHaveLength(0);
				// make sure data message did not arrive to peer 4
				for (const message of peers[2].messages) {
					if (message instanceof DataMessage) {
						const pubsubMessage = deserialize(message.data, PubSubMessage);
						expect(pubsubMessage).not.toBeInstanceOf(PubSubData);
					}
				}
			});
		});
	});

	/* 
	TODO
	┌────┐
	│0   │
	└┬──┬┘
	┌▽┐┌▽┐
	│2││1│
	└┬┘└┬┘
	┌▽──▽┐
	│3   │
	└────┘
	
	*/
	// test sending "0" to "3" only 1 message should appear even though not in strict mode

	describe("join/leave", () => {
		let session: LSession<{ pubsub: DirectSub }>;
		let peers: {
			stream: DirectSub;
			messages: Message[];
			recieved: PubSubData[];
			subscriptionEvents: SubscriptionEvent[];
			unsubscriptionEvents: UnsubcriptionEvent[];
		}[];
		const data = new Uint8Array([1, 2, 3]);
		const TOPIC_1 = "hello";
		const TOPIC_2 = "world";

		beforeEach(async () => {
			// 0 and 2 not connected
			session = await LSession.disconnected(3, {
				services: {
					pubsub: (c) =>
						new DirectSub(c, {
							canRelayMessage: true,
							connectionManager: { autoDial: false },
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
			peers = [];
			for (const peer of session.peers) {
				const client: {
					stream: DirectSub;
					messages: Message[];
					recieved: PubSubData[];
					subscriptionEvents: SubscriptionEvent[];
					unsubscriptionEvents: UnsubcriptionEvent[];
				} = {
					messages: [],
					recieved: [],
					stream: peer.services.pubsub,
					subscriptionEvents: [],
					unsubscriptionEvents: [],
				};
				peers.push(client);
				peer.services.pubsub.addEventListener("message", (msg) => {
					client.messages.push(msg.detail);
				});
				peer.services.pubsub.addEventListener("data", (msg) => {
					client.recieved.push(msg.detail);
				});
				peer.services.pubsub.addEventListener("subscribe", (msg) => {
					client.subscriptionEvents.push(msg.detail);
				});
				peer.services.pubsub.addEventListener("unsubscribe", (msg) => {
					client.unsubscriptionEvents.push(msg.detail);
				});
			}
			await waitForPeers(peers[0].stream, peers[1].stream);
			await waitForPeers(peers[1].stream, peers[2].stream);
		});

		afterEach(async () => {
			await Promise.all(peers.map((peer) => peer.stream.stop()));
			await session.stop();
		});
		it("it can track subscriptions across peers", async () => {
			for (const peer of peers) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}

			await peers[0].stream.subscribe(TOPIC_1);
			await waitFor(() =>
				peers[2].stream
					.getSubscribers(TOPIC_1)
					?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(() =>
				peers[1].stream
					.getSubscribers(TOPIC_1)
					?.has(peers[0].stream.publicKeyHash)
			);
			expect(peers[2].subscriptionEvents).toHaveLength(1);
			expect(peers[1].subscriptionEvents).toHaveLength(1);
			expect(
				peers[2].subscriptionEvents[0].from.equals(peers[0].stream.publicKey)
			).toBeTrue();
			expect(peers[2].subscriptionEvents[0].subscriptions).toHaveLength(1);
			expect(peers[2].subscriptionEvents[0].subscriptions[0].topic).toEqual(
				TOPIC_1
			);
			expect(
				peers[2].subscriptionEvents[0].subscriptions[0].data
			).toBeUndefined();
			await peers[0].stream.stop();
			await waitFor(
				() =>
					!peers[2].stream
						.getSubscribers(TOPIC_1)
						?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					!peers[1].stream
						.getSubscribers(TOPIC_1)
						?.has(peers[0].stream.publicKeyHash)
			);
			expect(peers[2].subscriptionEvents).toHaveLength(1);
			expect(peers[1].subscriptionEvents).toHaveLength(1);
			expect(peers[2].unsubscriptionEvents).toHaveLength(1);
			expect(peers[1].unsubscriptionEvents).toHaveLength(1);
			expect(
				peers[2].unsubscriptionEvents[0].from.equals(peers[0].stream.publicKey)
			).toBeTrue();
			expect(peers[2].unsubscriptionEvents[0].unsubscriptions).toHaveLength(1);
			expect(peers[2].unsubscriptionEvents[0].unsubscriptions[0].topic).toEqual(
				TOPIC_1
			);
		});

		it("can unsubscribe across peers", async () => {
			for (const peer of peers) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}

			peers[0].stream.subscribe(TOPIC_1);
			peers[0].stream.subscribe(TOPIC_2);
			await waitFor(() =>
				peers[2].stream
					.getSubscribers(TOPIC_1)
					?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(() =>
				peers[1].stream
					.getSubscribers(TOPIC_1)
					?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(() =>
				peers[2].stream
					.getSubscribers(TOPIC_2)
					?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(() =>
				peers[1].stream
					.getSubscribers(TOPIC_2)
					?.has(peers[0].stream.publicKeyHash)
			);

			expect(peers[2].subscriptionEvents).toHaveLength(2);
			expect(peers[1].subscriptionEvents).toHaveLength(2);
			expect(
				peers[2].subscriptionEvents[0].from.equals(peers[0].stream.publicKey)
			).toBeTrue();
			expect(peers[2].subscriptionEvents[0].subscriptions).toHaveLength(1);
			expect(peers[2].subscriptionEvents[0].subscriptions[0].topic).toEqual(
				TOPIC_1
			);
			expect(
				peers[2].subscriptionEvents[0].subscriptions[0].data
			).toBeUndefined();
			expect(peers[2].subscriptionEvents[1].subscriptions).toHaveLength(1);
			expect(peers[2].subscriptionEvents[1].subscriptions[0].topic).toEqual(
				TOPIC_2
			);
			expect(
				peers[2].subscriptionEvents[1].subscriptions[0].data
			).toBeUndefined();

			peers[0].stream.unsubscribe(TOPIC_1);
			await waitFor(
				() =>
					!peers[2].stream
						.getSubscribers(TOPIC_1)
						?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					!peers[1].stream
						.getSubscribers(TOPIC_1)
						?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(() =>
				peers[2].stream
					.getSubscribers(TOPIC_2)
					?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(() =>
				peers[1].stream
					.getSubscribers(TOPIC_2)
					?.has(peers[0].stream.publicKeyHash)
			);
			expect(peers[2].unsubscriptionEvents).toHaveLength(1);
			expect(peers[1].unsubscriptionEvents).toHaveLength(1);
			expect(
				peers[2].unsubscriptionEvents[0].from.equals(peers[0].stream.publicKey)
			).toBeTrue();
			expect(peers[2].unsubscriptionEvents[0].unsubscriptions).toHaveLength(1);
			expect(peers[2].unsubscriptionEvents[0].unsubscriptions[0].topic).toEqual(
				TOPIC_1
			);
			peers[0].stream.unsubscribe(TOPIC_2);
			await waitFor(
				() =>
					!peers[2].stream
						.getSubscribers(TOPIC_1)
						?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					!peers[1].stream
						.getSubscribers(TOPIC_1)
						?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					!peers[2].stream
						.getSubscribers(TOPIC_2)
						?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					!peers[1].stream
						.getSubscribers(TOPIC_2)
						?.has(peers[0].stream.publicKeyHash)
			);
			expect(peers[2].unsubscriptionEvents).toHaveLength(2);
			expect(peers[1].unsubscriptionEvents).toHaveLength(2);
			expect(
				peers[2].unsubscriptionEvents[1].from.equals(peers[0].stream.publicKey)
			).toBeTrue();
			expect(peers[2].unsubscriptionEvents[1].unsubscriptions).toHaveLength(1);
			expect(peers[2].unsubscriptionEvents[1].unsubscriptions[0].topic).toEqual(
				TOPIC_2
			);
		});

		it("can handle multiple subscriptions", async () => {
			for (const peer of peers) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}
			peers[0].stream.subscribe(TOPIC_1); // 1
			peers[0].stream.subscribe(TOPIC_1); // 2
			peers[0].stream.subscribe(TOPIC_1); // 3
			await waitFor(() =>
				peers[2].stream
					.getSubscribers(TOPIC_1)
					?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(() =>
				peers[1].stream
					.getSubscribers(TOPIC_1)
					?.has(peers[0].stream.publicKeyHash)
			);
			peers[0].stream.unsubscribe(TOPIC_1); // 3
			peers[0].stream.unsubscribe(TOPIC_1); // 2
			await delay(3000); // allow some communications
			await waitFor(() =>
				peers[2].stream
					.getSubscribers(TOPIC_1)
					?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(() =>
				peers[1].stream
					.getSubscribers(TOPIC_1)
					?.has(peers[0].stream.publicKeyHash)
			);
			await peers[0].stream.unsubscribe(TOPIC_1); // 1
			await waitFor(
				() =>
					!peers[2].stream
						.getSubscribers(TOPIC_1)
						?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					!peers[1].stream
						.getSubscribers(TOPIC_1)
						?.has(peers[0].stream.publicKeyHash)
			);
		});

		it("can override subscription metadata", async () => {
			for (const peer of peers) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}

			peers[0].stream.subscribe(TOPIC_1); // 1
			await waitFor(
				() =>
					peers[2].stream
						.getSubscribers(TOPIC_1)
						?.get(peers[0].stream.publicKeyHash)?.data === undefined
			);
			await waitFor(
				() =>
					!!peers[2].stream
						.getSubscribers(TOPIC_1)
						?.get(peers[0].stream.publicKeyHash)?.timestamp
			);
			await waitFor(
				() =>
					peers[1].stream
						.getSubscribers(TOPIC_1)
						?.get(peers[0].stream.publicKeyHash)?.data === undefined
			);
			await waitFor(
				() =>
					!!peers[1].stream
						.getSubscribers(TOPIC_1)
						?.get(peers[0].stream.publicKeyHash)?.timestamp
			);
			expect(
				peers[1].stream.getSubscribersWithData(TOPIC_1, new Uint8Array())
			).toHaveLength(0);
			expect(
				peers[2].stream.getSubscribersWithData(TOPIC_1, new Uint8Array())
			).toHaveLength(0);

			// Subscribe with some metadata
			const data1 = new Uint8Array([1, 2, 3]);
			await peers[0].stream.subscribe(TOPIC_1, { data: data1 }); // 2
			let equalsDefined = (a: Uint8Array | undefined, b: Uint8Array) => {
				if (!a) {
					return false;
				}
				return equals(a, b);
			};
			await waitFor(() =>
				equalsDefined(
					peers[2].stream
						.getSubscribers(TOPIC_1)
						?.get(peers[0].stream.publicKeyHash)?.data,
					data1
				)
			);
			await waitFor(() =>
				equalsDefined(
					peers[1].stream
						.getSubscribers(TOPIC_1)
						?.get(peers[0].stream.publicKeyHash)?.data!,
					data1
				)
			);
			expect(
				peers[1].stream.getSubscribersWithData(TOPIC_1, new Uint8Array())
			).toHaveLength(0);
			expect(
				peers[2].stream.getSubscribersWithData(TOPIC_1, new Uint8Array())
			).toHaveLength(0);
			expect(peers[1].stream.getSubscribersWithData(TOPIC_1, data1)!).toEqual([
				peers[0].stream.publicKeyHash,
			]);
			expect(peers[2].stream.getSubscribersWithData(TOPIC_1, data1)!).toEqual([
				peers[0].stream.publicKeyHash,
			]);
			expect(peers[2].subscriptionEvents).toHaveLength(2);
			expect(peers[1].subscriptionEvents).toHaveLength(2);
			expect(
				peers[2].subscriptionEvents[1].from.equals(peers[0].stream.publicKey)
			).toBeTrue();
			expect(peers[2].subscriptionEvents[1].subscriptions).toHaveLength(1);
			expect(peers[2].subscriptionEvents[1].subscriptions[0].topic).toEqual(
				TOPIC_1
			);
			expect(
				new Uint8Array(peers[2].subscriptionEvents[1].subscriptions[0].data!)
			).toEqual(data1);

			let data2 = new Uint8Array([3, 2, 1]);
			peers[0].stream.subscribe(TOPIC_1, { data: data2 }); // 3
			await waitFor(() =>
				equalsDefined(
					peers[2].stream
						.getSubscribers(TOPIC_1)
						?.get(peers[0].stream.publicKeyHash)?.data!,
					data2
				)
			);
			await waitFor(() =>
				equalsDefined(
					peers[1].stream
						.getSubscribers(TOPIC_1)
						?.get(peers[0].stream.publicKeyHash)?.data!,
					data2
				)
			);
			expect(
				peers[1].stream.getSubscribersWithData(TOPIC_1, data1)
			).toHaveLength(0);
			expect(
				peers[2].stream.getSubscribersWithData(TOPIC_1, data1)
			).toHaveLength(0);
			expect(peers[1].stream.getSubscribersWithData(TOPIC_1, data2)!).toEqual([
				peers[0].stream.publicKeyHash,
			]);
			expect(peers[2].stream.getSubscribersWithData(TOPIC_1, data2)!).toEqual([
				peers[0].stream.publicKeyHash,
			]);
			expect(peers[2].subscriptionEvents).toHaveLength(3);
			expect(peers[1].subscriptionEvents).toHaveLength(3);
			expect(
				peers[2].subscriptionEvents[2].from.equals(peers[0].stream.publicKey)
			).toBeTrue();
			expect(peers[2].subscriptionEvents[2].subscriptions).toHaveLength(1);
			expect(peers[2].subscriptionEvents[2].subscriptions[0].topic).toEqual(
				TOPIC_1
			);
			expect(
				new Uint8Array(peers[2].subscriptionEvents[2].subscriptions[0].data!)
			).toEqual(data2);

			peers[0].stream.unsubscribe(TOPIC_1); // 2
			peers[0].stream.unsubscribe(TOPIC_1); // 1
			await delay(3000); // allow some communications
			await waitFor(() =>
				peers[2].stream
					.getSubscribers(TOPIC_1)
					?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(() =>
				peers[1].stream
					.getSubscribers(TOPIC_1)
					?.has(peers[0].stream.publicKeyHash)
			);
			await peers[0].stream.unsubscribe(TOPIC_1); // 1
			await waitFor(
				() =>
					!peers[2].stream
						.getSubscribers(TOPIC_1)
						?.has(peers[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					!peers[1].stream
						.getSubscribers(TOPIC_1)
						?.has(peers[0].stream.publicKeyHash)
			);
			expect(
				peers[1].stream.getSubscribersWithData(TOPIC_1, data2)
			).toHaveLength(0);
			expect(
				peers[2].stream.getSubscribersWithData(TOPIC_1, data2)
			).toHaveLength(0);
			expect(peers[2].unsubscriptionEvents).toHaveLength(1);
			expect(peers[1].unsubscriptionEvents).toHaveLength(1);
			expect(
				peers[2].unsubscriptionEvents[0].from.equals(peers[0].stream.publicKey)
			).toBeTrue();
			expect(peers[2].unsubscriptionEvents[0].unsubscriptions).toHaveLength(1);
			expect(peers[2].unsubscriptionEvents[0].unsubscriptions[0].topic).toEqual(
				TOPIC_1
			);
			expect(
				new Uint8Array(
					peers[2].unsubscriptionEvents[0].unsubscriptions[0].data!
				)
			).toEqual(data2);
		});

		it("resubscription will not emit uncessary message", async () => {
			// Subscribe with some metadata
			const data1 = new Uint8Array([1, 2, 3]);

			let sentMessages = 0;
			const publishMessage = peers[0].stream.publishMessage.bind(
				peers[0].stream
			);
			peers[0].stream.publishMessage = async (a: any, b: any, c: any) => {
				sentMessages += 1;
				return publishMessage(a, b, c);
			};
			await peers[0].stream.subscribe(TOPIC_1, { data: data1 });
			expect(sentMessages).toEqual(1);
			await peers[0].stream.subscribe(TOPIC_1, { data: data1 });
			expect(sentMessages).toEqual(1); // no new messages sent
		});

		it("resubscription will not emit uncessary message", async () => {
			// Subscribe with some metadata
			const data1 = new Uint8Array([1, 2, 3]);

			let sentMessages = 0;
			const publishMessage = peers[0].stream.publishMessage.bind(
				peers[0].stream
			);
			peers[0].stream.publishMessage = async (a: any, b: any, c: any) => {
				sentMessages += 1;
				return publishMessage(a, b, c);
			};
			await peers[0].stream.subscribe(TOPIC_1, { data: data1 });
			expect(sentMessages).toEqual(1);
			await peers[0].stream.subscribe(TOPIC_1, { data: data1 });
			expect(sentMessages).toEqual(1); // no new messages sent
		});

		it("requesting subscribers will not overwrite subscriptions", async () => {
			for (const peer of peers) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}

			// Subscribe with some metadata
			const data1 = new Uint8Array([1, 2, 3]);
			await peers[0].stream.subscribe(TOPIC_1, { data: data1 });
			let equalsDefined = (a: Uint8Array | undefined, b: Uint8Array) => {
				if (!a) {
					return false;
				}
				return equals(a, b);
			};
			await waitFor(() =>
				equalsDefined(
					peers[2].stream
						.getSubscribers(TOPIC_1)
						?.get(peers[0].stream.publicKeyHash)?.data,
					data1
				)
			);
			await waitFor(() =>
				equalsDefined(
					peers[1].stream
						.getSubscribers(TOPIC_1)
						?.get(peers[0].stream.publicKeyHash)?.data!,
					data1
				)
			);
			//	await delay(3000)
			expect(
				peers[1].stream.getSubscribersWithData(TOPIC_1, new Uint8Array())
			).toHaveLength(0);
			expect(
				peers[2].stream.getSubscribersWithData(TOPIC_1, new Uint8Array())
			).toHaveLength(0);
			expect(peers[1].stream.getSubscribersWithData(TOPIC_1, data1)!).toEqual([
				peers[0].stream.publicKeyHash,
			]);
			expect(peers[2].stream.getSubscribersWithData(TOPIC_1, data1)!).toEqual([
				peers[0].stream.publicKeyHash,
			]);

			// Request subscribers and makes sure we don't get any wierd overwrites
			await peers[1].stream.requestSubscribers(TOPIC_1);
			await peers[2].stream.requestSubscribers(TOPIC_1);

			await delay(3000); // wait for some messages
			expect(peers[1].stream.getSubscribersWithData(TOPIC_1, data1)!).toEqual([
				peers[0].stream.publicKeyHash,
			]);
			expect(peers[2].stream.getSubscribersWithData(TOPIC_1, data1)!).toEqual([
				peers[0].stream.publicKeyHash,
			]);

			expect(peers[1].subscriptionEvents).toHaveLength(1); // Emits are only the unique ones
			expect(peers[2].subscriptionEvents).toHaveLength(1); // Emits are only the unique ones
		});

		it("get subscribers with metadata prefix", async () => {
			for (const peer of peers) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}

			// Subscribe with some metadata
			const data1 = new Uint8Array([1, 2, 3]);
			await peers[0].stream.subscribe(TOPIC_1, { data: data1 });
			let equalsDefined = (a: Uint8Array | undefined, b: Uint8Array) => {
				if (!a) {
					return false;
				}
				return equals(a, b);
			};
			await waitFor(
				() =>
					peers[2].stream.getSubscribersWithData(TOPIC_1, data, {
						prefix: true,
					})?.length === 1
			);
			expect(
				peers[2].stream.getSubscribersWithData(TOPIC_1, new Uint8Array([1]), {
					prefix: true,
				})
			).toHaveLength(1);
			expect(
				peers[2].stream.getSubscribersWithData(TOPIC_1, Buffer.from([1]), {
					prefix: true,
				})
			).toHaveLength(1);
			expect(
				peers[2].stream.getSubscribersWithData(
					TOPIC_1,
					new Uint8Array([1, 2]),
					{ prefix: true }
				)
			).toHaveLength(1);
			expect(
				peers[2].stream.getSubscribersWithData(TOPIC_1, new Uint8Array([]), {
					prefix: true,
				})
			).toHaveLength(1); // prefix with empty means all
			expect(
				peers[2].stream.getSubscribersWithData(TOPIC_1, new Uint8Array([2]), {
					prefix: true,
				})
			).toHaveLength(0);
			expect(
				peers[2].stream.getSubscribersWithData(
					TOPIC_1,
					new Uint8Array([1, 2, 3, 4]),
					{ prefix: true }
				)
			).toHaveLength(0);
		});
	});
});
