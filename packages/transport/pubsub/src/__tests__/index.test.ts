import { DataMessage, Message } from "@peerbit/stream-interface";
import { waitForPeers } from "@peerbit/stream";
import { TestSession } from "@peerbit/libp2p-test-utils";
import { waitFor, delay, waitForResolved } from "@peerbit/time";
import {
	PubSubMessage,
	PubSubData,
	Subscribe,
	Unsubscribe,
	GetSubscribers,
	SubscriptionEvent,
	UnsubcriptionEvent,
	DataEvent
} from "@peerbit/pubsub-interface";
import { DirectSub, waitForSubscribers } from "./../index.js";
import { deserialize } from "@dao-xyz/borsh";
import { equals } from "uint8arrays";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import * as filters from "@libp2p/websockets/filters";
import { randomBytes } from "@peerbit/crypto";

const createSubscriptionMetrics = (pubsub: DirectSub) => {
	let m: {
		subscriptions: DataMessage[];
		unsubscriptions: DataMessage[];
		getSubscriptions: DataMessage[];
	} = { getSubscriptions: [], subscriptions: [], unsubscriptions: [] };
	const onDataMessage = pubsub.onDataMessage.bind(pubsub);
	pubsub.onDataMessage = async (f, s, message) => {
		const result = await onDataMessage(f, s, message);
		const pubsubMessage = deserialize(message.data, PubSubMessage);
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
		messages: Message[];
		received: PubSubData[];
		allReceived: PubSubData[];
		subscriptionEvents: SubscriptionEvent[];
		unsubscriptionEvents: UnsubcriptionEvent[];
	} = {
		messages: [],
		received: [],
		allReceived: [],
		stream: pubsub,
		subscriptionEvents: [],
		unsubscriptionEvents: []
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
	const onDataMessageFn = pubsub.onDataMessage.bind(pubsub);
	pubsub.onDataMessage = (from, stream, message) => {
		const pubsubMessage = PubSubMessage.from(message.data);
		if (pubsubMessage instanceof PubSubData) {
			m.allReceived.push(pubsubMessage);
		}
		return onDataMessageFn(from, stream, message);
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
							connectionManager: { autoDial: false }
						})
				}
			});
		});
		afterEach(async () => {
			await session.stop();
		});

		it("can share topics when connecting after subscribe, 2 peers", async () => {
			for (const peer of session.peers.slice(0, 2)) {
				streams.push(createMetrics(peer.services.pubsub));
			}

			const TOPIC = "world";
			streams[0].stream.subscribe(TOPIC);
			streams[1].stream.subscribe(TOPIC);

			await delay(1000); // wait for subscription message to propagate (if any)
			// now connect peers and make sure that subscription information is passed on as they connect
			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitForSubscribers(session.peers[0], [session.peers[1]], TOPIC);
			await waitForSubscribers(session.peers[1], [session.peers[0]], TOPIC);
			await Promise.all(streams.map((s) => s.stream.stop()));
		});

		it("can share topics when connecting after subscribe, 3 peers and 1 relay", async () => {
			let streams: ReturnType<typeof createMetrics>[] = [];
			for (const peer of session.peers) {
				streams.push(createMetrics(peer.services.pubsub));
			}

			const TOPIC = "world";
			streams[0].stream.subscribe(TOPIC);
			// peers[1] is not subscribing
			streams[2].stream.subscribe(TOPIC);

			await delay(1000); // wait for subscription message to propagate (if any)
			// now connect peers and make sure that subscription information is passed on as they connect
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]]
			]);
			await waitForSubscribers(session.peers[0], [session.peers[2]], TOPIC);
			await waitForSubscribers(session.peers[2], [session.peers[0]], TOPIC);
			await Promise.all(streams.map((x) => x.stream.stop()));
		});
	});

	describe("publish", () => {
		let session: TestSession<{ pubsub: DirectSub }>;
		let streams: ReturnType<typeof createMetrics>[];
		const data = new Uint8Array([1, 2, 3]);
		const TOPIC = "world";

		describe("line", () => {
			beforeEach(async () => {
				// 0 and 2 not connected
				session = await TestSession.disconnected(3, {
					services: {
						pubsub: (c) =>
							new DirectSub(c, {
								canRelayMessage: true,
								connectionManager: { autoDial: false }
							})
					}
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
					[session.peers[1], session.peers[2]]
				]);

				streams = [];
				for (const peer of session.peers) {
					streams.push(createMetrics(peer.services.pubsub));
				}
				await waitForPeers(streams[0].stream, streams[1].stream);
				await waitForPeers(streams[1].stream, streams[2].stream);
				await delay(1000);

				await streams[0].stream.subscribe(TOPIC);
				await streams[1].stream.subscribe(TOPIC);
				await streams[2].stream.subscribe(TOPIC);

				for (let i = 0; i < streams.length; i++) {
					for (let j = 0; j < streams.length; j++) {
						if (i == j) {
							continue;
						}
						await waitForSubscribers(
							session.peers[i],
							[session.peers[j]],
							TOPIC
						);
					}
				}
			});

			afterEach(async () => {
				for (let i = 0; i < streams.length; i++) {
					streams[i].stream.unsubscribe(TOPIC);
				}
				for (let i = 0; i < streams.length; i++) {
					await waitFor(() => !streams[i].stream.getSubscribers(TOPIC)?.size);
					expect(streams[i].stream.topics.has(TOPIC)).toBeFalse();
					expect(streams[i].stream.subscriptions.has(TOPIC)).toBeFalse();
				}

				await session.stop();
			});

			it("0->TOPIC", async () => {
				await streams[0].stream.publish(data, { topics: [TOPIC] });
				await waitFor(() => streams[1].received.length === 1);
				expect(new Uint8Array(streams[1].received[0].data)).toEqual(data);
				expect(streams[1].received[0].topics).toEqual([TOPIC]);
				await waitFor(() => streams[2].received.length === 1);
				expect(new Uint8Array(streams[2].received[0].data)).toEqual(data);
				await delay(3000); // wait some more time to make sure we dont get more messages
				expect(streams[1].received).toHaveLength(1);
				expect(streams[2].received).toHaveLength(1);
			});

			it("0->TOPIC strict to", async () => {
				await streams[0].stream.publish(data, {
					topics: [TOPIC],
					to: [streams[2].stream.publicKey],
					strict: true
				});
				await waitForResolved(() =>
					expect(streams[2].received).toHaveLength(1)
				);
				expect(new Uint8Array(streams[2].received[0].data)).toEqual(data);
				expect(streams[2].received[0].topics).toEqual([TOPIC]);
				expect(streams[1].received).toHaveLength(0);
				expect(streams[1].allReceived).toHaveLength(1); // because the message has to travel through this node

				await delay(3000); // wait some more time to make sure we dont get more messages
				expect(streams[1].received).toHaveLength(0);
				expect(streams[1].allReceived).toHaveLength(1); // because the message has to travel through this node
				expect(streams[2].received).toHaveLength(1);
			});
			it("1->TOPIC strict to", async () => {
				await streams[1].stream.publish(data, {
					topics: [TOPIC],
					to: [streams[2].stream.publicKey],
					strict: true
				});
				await waitForResolved(() =>
					expect(streams[2].received).toHaveLength(1)
				);
				expect(new Uint8Array(streams[2].received[0].data)).toEqual(data);
				expect(streams[2].received[0].topics).toEqual([TOPIC]);
				expect(streams[0].allReceived).toHaveLength(0);
				await delay(3000); // wait some more time to make sure we dont get more messages
				expect(streams[0].allReceived).toHaveLength(0);
				expect(streams[2].received).toHaveLength(1);
			});
			it("sends only in necessary directions", async () => {
				await streams[2].stream.unsubscribe(TOPIC);
				await waitForResolved(() =>
					expect(streams[1].stream.getSubscribers(TOPIC)!.size).toEqual(1)
				);

				const sendBytes = randomBytes(32);
				await streams[1].stream.publish(sendBytes, { topics: [TOPIC] });

				await waitFor(() => streams[0].received.length === 1);
				await delay(3000); // wait some more time to make sure we dont get more messages

				// Make sure we never received the data message in node 2
				for (const message of streams[2].allReceived) {
					expect(
						equals(new Uint8Array(message.data), new Uint8Array(sendBytes))
					).toBeFalse();
				}
			});

			it("send without topic directly", async () => {
				await streams[0].stream.publish(data, {
					to: [streams[1].stream.components.peerId]
				});
				await waitFor(() => streams[1].received.length === 1);
				expect(new Uint8Array(streams[1].received[0].data)).toEqual(data);
				await delay(3000); // wait some more time to make sure we dont get more messages
				expect(streams[1].received).toHaveLength(1);
				expect(streams[2].received).toHaveLength(0);
			});

			it("send without topic over relay", async () => {
				await streams[0].stream.publish(data, {
					to: [streams[2].stream.components.peerId]
				});
				await waitFor(() => streams[2].received.length === 1);
				expect(new Uint8Array(streams[2].received[0].data)).toEqual(data);
				await delay(3000); // wait some more time to make sure we dont get more messages
				expect(streams[2].received).toHaveLength(1);
				expect(streams[1].received).toHaveLength(0);
			});
			it("can send as non subscribeer", async () => {
				streams[0].stream.unsubscribe(TOPIC);
				streams[1].stream.unsubscribe(TOPIC);
				await streams[0].stream.publish(data, { topics: [TOPIC] });
				await waitFor(() => streams[2].received.length === 1);
				expect(new Uint8Array(streams[2].received[0].data)).toEqual(data);
				await delay(3000); // wait some more time to make sure we dont get more messages
				expect(streams[1].received).toHaveLength(0);
				expect(streams[2].received).toHaveLength(1);
			});
		});

		describe("fully connected", () => {
			beforeEach(async () => {
				// 0 and 2 not connected
				session = await TestSession.disconnected(3, {
					services: {
						pubsub: (c) =>
							new DirectSub(c, {
								canRelayMessage: true,
								connectionManager: { autoDial: false }
							})
					}
				});

				await session.connect([
					[session.peers[0], session.peers[1]],
					[session.peers[1], session.peers[2]],
					[session.peers[0], session.peers[2]]
				]);

				streams = [];
				for (const peer of session.peers) {
					streams.push(createMetrics(peer.services.pubsub));
				}
				await waitForPeers(streams[0].stream, streams[1].stream);
				await waitForPeers(streams[1].stream, streams[2].stream);
				await delay(1000);

				await streams[0].stream.subscribe(TOPIC);
				await streams[1].stream.subscribe(TOPIC);
				await streams[2].stream.subscribe(TOPIC);

				for (let i = 0; i < streams.length; i++) {
					for (let j = 0; j < streams.length; j++) {
						if (i == j) {
							continue;
						}
						await waitForSubscribers(
							session.peers[i],
							[session.peers[j]],
							TOPIC
						);
					}
				}
			});

			afterEach(async () => {
				for (let i = 0; i < streams.length; i++) {
					streams[i].stream.unsubscribe(TOPIC);
				}
				for (let i = 0; i < streams.length; i++) {
					await waitFor(() => !streams[i].stream.getSubscribers(TOPIC)?.size);
					expect(streams[i].stream.topics.has(TOPIC)).toBeFalse();
					expect(streams[i].stream.subscriptions.has(TOPIC)).toBeFalse();
				}

				await session.stop();
			});

			it("concurrently", async () => {
				// Purpose of this test is to check if there exist any dead-locks
				// possibly than can arise from bi-directional writing (?)
				// for examples, is processRpc does result in sending a message back to the same sender
				// it could cause issues. The exact circumstances/reasons for this is unknown, not specified

				const hasData = (d: Uint8Array, i: number) => {
					return !!streams[i].received.find((x) => equals(x.data, d));
				};
				const fn = async (i: number) => {
					const d = randomBytes(999);
					await streams[i % 3].stream.publish(d, {
						to: [
							streams[(i + 1) % session.peers.length].stream.publicKeyHash,
							streams[(i + 2) % session.peers.length].stream.publicKeyHash
						],
						strict: true,
						topics: [TOPIC]
					});

					expect(hasData(d, i % session.peers.length)).toBeFalse();
					await waitFor(() => hasData(d, (i + 1) % session.peers.length));
					await waitFor(() => hasData(d, (i + 2) % session.peers.length));
				};
				let p: Promise<any>[] = [];
				for (let i = 0; i < 100; i++) {
					p.push(fn(i));
				}
				await Promise.all(p);
			});
		});

		describe("emitSelf", () => {
			beforeEach(async () => {
				session = await TestSession.disconnected(2, {
					services: {
						pubsub: (c) =>
							new DirectSub(c, {
								emitSelf: true,
								canRelayMessage: true,
								connectionManager: { autoDial: false }
							})
					}
				});
			});

			afterEach(async () => {
				await session.stop();
			});

			it("publish", async () => {
				const dataMessages: DataEvent[] = [];
				session.peers[0].services.pubsub.addEventListener("data", (e) => {
					dataMessages.push(e.detail);
				});
				await session.peers[0].services.pubsub.publish(
					new Uint8Array([1, 2, 3]),
					{ to: [session.peers[1].peerId] }
				);
				expect(dataMessages).toHaveLength(1);
				expect(dataMessages[0]).toBeInstanceOf(DataEvent);
				expect(dataMessages[0].data.data).toEqual(new Uint8Array([1, 2, 3]));
			});
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
			beforeAll(async () => {});
			beforeEach(async () => {
				session = await TestSession.disconnected(5, {
					services: {
						pubsub: (c) =>
							new DirectSub(c, {
								canRelayMessage: true,
								connectionManager: { autoDial: false }
							})
					}
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
					[session.peers[2], session.peers[4]]
				]);

				for (const [i, peer] of streams.entries()) {
					if (i !== 3) {
						await peer.stream.requestSubscribers(TOPIC);
					}
					await waitFor(
						() => peer.stream.getSubscribers(TOPIC)?.size === (i === 3 ? 0 : 1)
					); // all others (except 4 which is not subscribing)
				}
			});

			afterEach(async () => {
				await Promise.all(streams.map((peer) => peer.stream.stop()));
				await session.stop();
			});
			afterAll(async () => {});

			it("will publish on routes", async () => {
				streams[3].received = [];
				streams[4].received = [];
				await streams[0].stream.publish(data, { topics: [TOPIC] });
				await waitFor(() => streams[3].received.length === 1);
				expect(new Uint8Array(streams[3].received[0].data)).toEqual(data);

				await delay(1000); // some delay to allow all messages to progagate
				expect(streams[4].received).toHaveLength(0);
				// make sure data message did not arrive to peer 4
				for (const message of streams[4].messages) {
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

			let session: TestSession<{ pubsub: DirectSub }>;
			let streams: ReturnType<typeof createMetrics>[];

			const data = new Uint8Array([1, 2, 3]);
			const TOPIC = "world";
			beforeAll(async () => {});
			beforeEach(async () => {
				session = await TestSession.disconnected(3, {
					services: {
						pubsub: (c) =>
							new DirectSub(c, {
								canRelayMessage: true,
								connectionManager: { autoDial: false }
							})
					}
				});
				streams = [];
				for (const [i, peer] of session.peers.entries()) {
					streams.push(createMetrics(peer.services.pubsub));

					if (i === 1) {
						peer.services.pubsub.subscribe(TOPIC);
					}
				}

				await session.connect([
					[session.peers[0], session.peers[1]],
					[session.peers[1], session.peers[2]]
				]);

				for (const [i, peer] of streams.entries()) {
					if (i !== 1) {
						await peer.stream.requestSubscribers(TOPIC);
					}
					await waitFor(
						() => peer.stream.getSubscribers(TOPIC)?.size === (i === 1 ? 0 : 1)
					); // all others (except 4 which is not subscribing)
				}
			});

			afterEach(async () => {
				await Promise.all(streams.map((peer) => peer.stream.stop()));
				await session.stop();
			});
			afterAll(async () => {});

			it("will not forward unless necessary", async () => {
				streams[1].received = [];
				streams[2].received = [];
				await delay(5000);
				await streams[0].stream.publish(data, { topics: [TOPIC] });
				await waitFor(() => streams[1].received.length === 1);
				expect(new Uint8Array(streams[1].received[0].data)).toEqual(data);

				await delay(1000); // some delay to allow all messages to progagate
				expect(streams[2].received).toHaveLength(0);
				// make sure data message did not arrive to peer 4
				for (const message of streams[2].messages) {
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
		let session: TestSession<{ pubsub: DirectSub }>;
		let streams: ReturnType<typeof createMetrics>[];
		const data = new Uint8Array([1, 2, 3]);
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
							connectionManager: { autoDial: false }
						})
				}
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
				[session.peers[1], session.peers[2]]
			]);
			streams = [];
			for (const peer of session.peers) {
				streams.push(createMetrics(peer.services.pubsub));
			}
			await waitForPeers(streams[0].stream, streams[1].stream);
			await waitForPeers(streams[1].stream, streams[2].stream);
		});

		afterEach(async () => {
			await Promise.all(streams.map((peer) => peer.stream.stop()));
			await session.stop();
		});

		it("it can track subscriptions across peers", async () => {
			for (const peer of streams) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}

			await streams[0].stream.subscribe(TOPIC_1);
			await waitFor(
				() =>
					streams[2].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					streams[1].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			expect(streams[2].subscriptionEvents).toHaveLength(1);
			expect(streams[1].subscriptionEvents).toHaveLength(1);
			expect(
				streams[2].subscriptionEvents[0].from.equals(
					streams[0].stream.publicKey
				)
			).toBeTrue();
			expect(streams[2].subscriptionEvents[0].subscriptions).toHaveLength(1);
			expect(streams[2].subscriptionEvents[0].subscriptions[0].topic).toEqual(
				TOPIC_1
			);
			expect(
				streams[2].subscriptionEvents[0].subscriptions[0].data
			).toBeUndefined();
			await streams[0].stream.stop();
			await waitFor(
				() =>
					!streams[2].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					!streams[1].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			expect(streams[2].subscriptionEvents).toHaveLength(1);
			expect(streams[1].subscriptionEvents).toHaveLength(1);
			expect(streams[2].unsubscriptionEvents).toHaveLength(1);
			expect(streams[1].unsubscriptionEvents).toHaveLength(1);
			expect(
				streams[2].unsubscriptionEvents[0].from.equals(
					streams[0].stream.publicKey
				)
			).toBeTrue();
			expect(streams[2].unsubscriptionEvents[0].unsubscriptions).toHaveLength(
				1
			);
			expect(
				streams[2].unsubscriptionEvents[0].unsubscriptions[0].topic
			).toEqual(TOPIC_1);
		});

		it("can unsubscribe across peers", async () => {
			for (const peer of streams) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}

			streams[0].stream.subscribe(TOPIC_1);
			streams[0].stream.subscribe(TOPIC_2);
			await waitFor(
				() =>
					streams[2].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					streams[1].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					streams[2].stream
						.getSubscribers(TOPIC_2)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					streams[1].stream
						.getSubscribers(TOPIC_2)
						?.has(streams[0].stream.publicKeyHash)
			);

			expect(streams[2].subscriptionEvents).toHaveLength(2);
			expect(streams[1].subscriptionEvents).toHaveLength(2);
			expect(
				streams[2].subscriptionEvents[0].from.equals(
					streams[0].stream.publicKey
				)
			).toBeTrue();
			expect(streams[2].subscriptionEvents[0].subscriptions).toHaveLength(1);
			expect(streams[2].subscriptionEvents[0].subscriptions[0].topic).toEqual(
				TOPIC_1
			);
			expect(
				streams[2].subscriptionEvents[0].subscriptions[0].data
			).toBeUndefined();
			expect(streams[2].subscriptionEvents[1].subscriptions).toHaveLength(1);
			expect(streams[2].subscriptionEvents[1].subscriptions[0].topic).toEqual(
				TOPIC_2
			);
			expect(
				streams[2].subscriptionEvents[1].subscriptions[0].data
			).toBeUndefined();

			streams[0].stream.unsubscribe(TOPIC_1);
			await waitFor(
				() =>
					!streams[2].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					!streams[1].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					streams[2].stream
						.getSubscribers(TOPIC_2)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					streams[1].stream
						.getSubscribers(TOPIC_2)
						?.has(streams[0].stream.publicKeyHash)
			);
			expect(streams[2].unsubscriptionEvents).toHaveLength(1);
			expect(streams[1].unsubscriptionEvents).toHaveLength(1);
			expect(
				streams[2].unsubscriptionEvents[0].from.equals(
					streams[0].stream.publicKey
				)
			).toBeTrue();
			expect(streams[2].unsubscriptionEvents[0].unsubscriptions).toHaveLength(
				1
			);
			expect(
				streams[2].unsubscriptionEvents[0].unsubscriptions[0].topic
			).toEqual(TOPIC_1);
			streams[0].stream.unsubscribe(TOPIC_2);
			await waitFor(
				() =>
					!streams[2].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					!streams[1].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					!streams[2].stream
						.getSubscribers(TOPIC_2)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					!streams[1].stream
						.getSubscribers(TOPIC_2)
						?.has(streams[0].stream.publicKeyHash)
			);
			expect(streams[2].unsubscriptionEvents).toHaveLength(2);
			expect(streams[1].unsubscriptionEvents).toHaveLength(2);
			expect(
				streams[2].unsubscriptionEvents[1].from.equals(
					streams[0].stream.publicKey
				)
			).toBeTrue();
			expect(streams[2].unsubscriptionEvents[1].unsubscriptions).toHaveLength(
				1
			);
			expect(
				streams[2].unsubscriptionEvents[1].unsubscriptions[0].topic
			).toEqual(TOPIC_2);
		});

		it("can handle multiple subscriptions", async () => {
			for (const peer of streams) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}
			streams[0].stream.subscribe(TOPIC_1); // 1
			streams[0].stream.subscribe(TOPIC_1); // 2
			streams[0].stream.subscribe(TOPIC_1); // 3

			await waitFor(
				() =>
					streams[2].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					streams[1].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			streams[0].stream.unsubscribe(TOPIC_1); // 3
			streams[0].stream.unsubscribe(TOPIC_1); // 2
			await delay(3000); // allow some communications
			await waitFor(
				() =>
					streams[2].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					streams[1].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			await streams[0].stream.unsubscribe(TOPIC_1); // 1
			await waitFor(
				() =>
					!streams[2].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					!streams[1].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
		});

		it("can override subscription metadata", async () => {
			for (const peer of streams) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}

			streams[0].stream.subscribe(TOPIC_1); // 1
			await waitFor(
				() =>
					streams[2].stream
						.getSubscribers(TOPIC_1)
						?.get(streams[0].stream.publicKeyHash)?.data === undefined
			);
			await waitFor(
				() =>
					!!streams[2].stream
						.getSubscribers(TOPIC_1)
						?.get(streams[0].stream.publicKeyHash)?.timestamp
			);
			await waitFor(
				() =>
					streams[1].stream
						.getSubscribers(TOPIC_1)
						?.get(streams[0].stream.publicKeyHash)?.data === undefined
			);
			await waitFor(
				() =>
					!!streams[1].stream
						.getSubscribers(TOPIC_1)
						?.get(streams[0].stream.publicKeyHash)?.timestamp
			);
			expect(
				streams[1].stream.getSubscribersWithData(TOPIC_1, new Uint8Array())
			).toHaveLength(0);
			expect(
				streams[2].stream.getSubscribersWithData(TOPIC_1, new Uint8Array())
			).toHaveLength(0);

			// Subscribe with some metadata
			const data1 = new Uint8Array([1, 2, 3]);
			await streams[0].stream.subscribe(TOPIC_1, { data: data1 }); // 2
			let equalsDefined = (a: Uint8Array | undefined, b: Uint8Array) => {
				if (!a) {
					return false;
				}
				return equals(a, b);
			};
			await waitFor(() =>
				equalsDefined(
					streams[2].stream
						.getSubscribers(TOPIC_1)
						?.get(streams[0].stream.publicKeyHash)?.data,
					data1
				)
			);
			await waitFor(() =>
				equalsDefined(
					streams[1].stream
						.getSubscribers(TOPIC_1)
						?.get(streams[0].stream.publicKeyHash)?.data!,
					data1
				)
			);
			expect(
				streams[1].stream.getSubscribersWithData(TOPIC_1, new Uint8Array())
			).toHaveLength(0);
			expect(
				streams[2].stream.getSubscribersWithData(TOPIC_1, new Uint8Array())
			).toHaveLength(0);
			expect(streams[1].stream.getSubscribersWithData(TOPIC_1, data1)!).toEqual(
				[streams[0].stream.publicKeyHash]
			);
			expect(streams[2].stream.getSubscribersWithData(TOPIC_1, data1)!).toEqual(
				[streams[0].stream.publicKeyHash]
			);
			expect(streams[2].subscriptionEvents).toHaveLength(2);
			expect(streams[1].subscriptionEvents).toHaveLength(2);
			expect(
				streams[2].subscriptionEvents[1].from.equals(
					streams[0].stream.publicKey
				)
			).toBeTrue();
			expect(streams[2].subscriptionEvents[1].subscriptions).toHaveLength(1);
			expect(streams[2].subscriptionEvents[1].subscriptions[0].topic).toEqual(
				TOPIC_1
			);
			expect(
				new Uint8Array(streams[2].subscriptionEvents[1].subscriptions[0].data!)
			).toEqual(data1);

			let data2 = new Uint8Array([3, 2, 1]);
			streams[0].stream.subscribe(TOPIC_1, { data: data2 }); // 3
			await waitFor(() =>
				equalsDefined(
					streams[2].stream
						.getSubscribers(TOPIC_1)
						?.get(streams[0].stream.publicKeyHash)?.data!,
					data2
				)
			);
			await waitFor(() =>
				equalsDefined(
					streams[1].stream
						.getSubscribers(TOPIC_1)
						?.get(streams[0].stream.publicKeyHash)?.data!,
					data2
				)
			);
			expect(
				streams[1].stream.getSubscribersWithData(TOPIC_1, data1)
			).toHaveLength(0);
			expect(
				streams[2].stream.getSubscribersWithData(TOPIC_1, data1)
			).toHaveLength(0);
			expect(streams[1].stream.getSubscribersWithData(TOPIC_1, data2)!).toEqual(
				[streams[0].stream.publicKeyHash]
			);
			expect(streams[2].stream.getSubscribersWithData(TOPIC_1, data2)!).toEqual(
				[streams[0].stream.publicKeyHash]
			);
			expect(streams[2].subscriptionEvents).toHaveLength(3);
			expect(streams[1].subscriptionEvents).toHaveLength(3);
			expect(
				streams[2].subscriptionEvents[2].from.equals(
					streams[0].stream.publicKey
				)
			).toBeTrue();
			expect(streams[2].subscriptionEvents[2].subscriptions).toHaveLength(1);
			expect(streams[2].subscriptionEvents[2].subscriptions[0].topic).toEqual(
				TOPIC_1
			);
			expect(
				new Uint8Array(streams[2].subscriptionEvents[2].subscriptions[0].data!)
			).toEqual(data2);

			streams[0].stream.unsubscribe(TOPIC_1); // 2
			streams[0].stream.unsubscribe(TOPIC_1); // 1
			await delay(3000); // allow some communications
			await waitFor(
				() =>
					streams[2].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					streams[1].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			await streams[0].stream.unsubscribe(TOPIC_1); // 1
			await waitFor(
				() =>
					!streams[2].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			await waitFor(
				() =>
					!streams[1].stream
						.getSubscribers(TOPIC_1)
						?.has(streams[0].stream.publicKeyHash)
			);
			expect(
				streams[1].stream.getSubscribersWithData(TOPIC_1, data2)
			).toHaveLength(0);
			expect(
				streams[2].stream.getSubscribersWithData(TOPIC_1, data2)
			).toHaveLength(0);
			expect(streams[2].unsubscriptionEvents).toHaveLength(1);
			expect(streams[1].unsubscriptionEvents).toHaveLength(1);
			expect(
				streams[2].unsubscriptionEvents[0].from.equals(
					streams[0].stream.publicKey
				)
			).toBeTrue();
			expect(streams[2].unsubscriptionEvents[0].unsubscriptions).toHaveLength(
				1
			);
			expect(
				streams[2].unsubscriptionEvents[0].unsubscriptions[0].topic
			).toEqual(TOPIC_1);
			expect(
				new Uint8Array(
					streams[2].unsubscriptionEvents[0].unsubscriptions[0].data!
				)
			).toEqual(data2);
		});

		it("resubscription will not emit uncessary message", async () => {
			// Subscribe with some metadata
			const data1 = new Uint8Array([1, 2, 3]);

			let sentMessages = 0;
			const publishMessage = streams[0].stream.publishMessage.bind(
				streams[0].stream
			);
			streams[0].stream.publishMessage = async (a: any, b: any, c: any) => {
				sentMessages += 1;
				return publishMessage(a, b, c);
			};
			await streams[0].stream.subscribe(TOPIC_1, { data: data1 });
			expect(sentMessages).toEqual(1);
			await streams[0].stream.subscribe(TOPIC_1, { data: data1 });
			expect(sentMessages).toEqual(1); // no new messages sent
		});

		it("resubscription will not emit uncessary message", async () => {
			// Subscribe with some metadata
			const data1 = new Uint8Array([1, 2, 3]);

			let sentMessages = 0;
			const publishMessage = streams[0].stream.publishMessage.bind(
				streams[0].stream
			);
			streams[0].stream.publishMessage = async (a: any, b: any, c: any) => {
				sentMessages += 1;
				return publishMessage(a, b, c);
			};
			await streams[0].stream.subscribe(TOPIC_1, { data: data1 });
			expect(sentMessages).toEqual(1);
			await streams[0].stream.subscribe(TOPIC_1, { data: data1 });
			expect(sentMessages).toEqual(1); // no new messages sent
		});

		it("requesting subscribers will not overwrite subscriptions", async () => {
			for (const peer of streams) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}

			// Subscribe with some metadata
			const data1 = new Uint8Array([1, 2, 3]);
			await streams[0].stream.subscribe(TOPIC_1, { data: data1 });
			let equalsDefined = (a: Uint8Array | undefined, b: Uint8Array) => {
				if (!a) {
					return false;
				}
				return equals(a, b);
			};
			await waitFor(() =>
				equalsDefined(
					streams[2].stream
						.getSubscribers(TOPIC_1)
						?.get(streams[0].stream.publicKeyHash)?.data,
					data1
				)
			);
			await waitFor(() =>
				equalsDefined(
					streams[1].stream
						.getSubscribers(TOPIC_1)
						?.get(streams[0].stream.publicKeyHash)?.data!,
					data1
				)
			);
			//	await delay(3000)
			expect(
				streams[1].stream.getSubscribersWithData(TOPIC_1, new Uint8Array())
			).toHaveLength(0);
			expect(
				streams[2].stream.getSubscribersWithData(TOPIC_1, new Uint8Array())
			).toHaveLength(0);
			expect(streams[1].stream.getSubscribersWithData(TOPIC_1, data1)!).toEqual(
				[streams[0].stream.publicKeyHash]
			);
			expect(streams[2].stream.getSubscribersWithData(TOPIC_1, data1)!).toEqual(
				[streams[0].stream.publicKeyHash]
			);

			// Request subscribers and makes sure we don't get any wierd overwrites
			await streams[1].stream.requestSubscribers(TOPIC_1);
			await streams[2].stream.requestSubscribers(TOPIC_1);

			await delay(3000); // wait for some messages
			expect(streams[1].stream.getSubscribersWithData(TOPIC_1, data1)!).toEqual(
				[streams[0].stream.publicKeyHash]
			);
			expect(streams[2].stream.getSubscribersWithData(TOPIC_1, data1)!).toEqual(
				[streams[0].stream.publicKeyHash]
			);

			expect(streams[1].subscriptionEvents).toHaveLength(1); // Emits are only the unique ones
			expect(streams[2].subscriptionEvents).toHaveLength(1); // Emits are only the unique ones
		});

		it("get subscribers with metadata prefix", async () => {
			for (const peer of streams) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}

			// Subscribe with some metadata
			const data1 = new Uint8Array([1, 2, 3]);
			await streams[0].stream.subscribe(TOPIC_1, { data: data1 });

			await waitFor(
				() =>
					streams[2].stream.getSubscribersWithData(TOPIC_1, data, {
						prefix: true
					})?.length === 1
			);
			expect(
				streams[2].stream.getSubscribersWithData(TOPIC_1, new Uint8Array([1]), {
					prefix: true
				})
			).toHaveLength(1);
			expect(
				streams[2].stream.getSubscribersWithData(TOPIC_1, Buffer.from([1]), {
					prefix: true
				})
			).toHaveLength(1);
			expect(
				streams[2].stream.getSubscribersWithData(
					TOPIC_1,
					new Uint8Array([1, 2]),
					{ prefix: true }
				)
			).toHaveLength(1);
			expect(
				streams[2].stream.getSubscribersWithData(TOPIC_1, new Uint8Array([]), {
					prefix: true
				})
			).toHaveLength(1); // prefix with empty means all
			expect(
				streams[2].stream.getSubscribersWithData(TOPIC_1, new Uint8Array([2]), {
					prefix: true
				})
			).toHaveLength(0);
			expect(
				streams[2].stream.getSubscribersWithData(
					TOPIC_1,
					new Uint8Array([1, 2, 3, 4]),
					{ prefix: true }
				)
			).toHaveLength(0);
		});

		describe("invalidation", () => {
			it("uses timestamp to ignore old events", async () => {
				const pubsubMetrics0 = createSubscriptionMetrics(streams[0].stream);
				const pubsubMetrics1 = createSubscriptionMetrics(streams[1].stream);
				await streams[1].stream.requestSubscribers(TOPIC_1);

				await waitForResolved(() =>
					expect(pubsubMetrics0.getSubscriptions).toHaveLength(1)
				);

				pubsubMetrics1.subscriptions = [];

				await streams[0].stream.subscribe(TOPIC_1);
				await waitForResolved(() =>
					expect(pubsubMetrics1.subscriptions).toHaveLength(1)
				);

				expect(streams[1].stream.getSubscribers(TOPIC_1)!.size).toEqual(1);

				await streams[0].stream.unsubscribe(TOPIC_1);
				await waitForResolved(() =>
					expect(pubsubMetrics1.unsubscriptions).toHaveLength(1)
				);

				expect(streams[1].stream.getSubscribers(TOPIC_1)!.size).toEqual(0);

				// reprocess first subscription message and make sure its ignored
				await streams[1].stream.onDataMessage(
					session.peers[0].peerId,
					[...streams[1].stream.peers.values()][0],
					pubsubMetrics1.subscriptions[0]
				);

				expect(streams[1].stream.getSubscribers(TOPIC_1)!.size).toEqual(0);

				// resubscribe again and try to send old unsubscription
				pubsubMetrics1.subscriptions = [];
				await streams[0].stream.subscribe(TOPIC_1);
				await waitForResolved(() =>
					expect(pubsubMetrics1.subscriptions).toHaveLength(1)
				);
				expect(streams[1].stream.getSubscribers(TOPIC_1)!.size).toEqual(1);

				await streams[1].stream.onDataMessage(
					session.peers[0].peerId,
					[...streams[1].stream.peers.values()][0],
					pubsubMetrics1.unsubscriptions[0]
				);
				expect(streams[1].stream.getSubscribers(TOPIC_1)!.size).toEqual(1); // No change, since message was old

				expect(streams[1].stream.lastSubscriptionMessages.size).toEqual(1);
				await session.peers[0].stop();
				await waitForResolved(() =>
					expect(streams[1].stream.lastSubscriptionMessages.size).toEqual(0)
				);
			});

			it("will clear lastSubscriptionMessages on unsubscribe", async () => {
				await streams[1].stream.requestSubscribers(TOPIC_1);

				await streams[0].stream.subscribe(TOPIC_1);
				await waitForResolved(() =>
					expect(streams[1].stream.getSubscribers(TOPIC_1)!.size).toEqual(1)
				);
				expect(streams[1].stream.lastSubscriptionMessages.size).toEqual(1);
				let dummyPeer = "x";
				streams[1].stream.lastSubscriptionMessages.set(dummyPeer, new Map());
				expect(streams[1].stream.lastSubscriptionMessages.size).toEqual(2);

				await streams[1].stream.unsubscribe(TOPIC_1);
				expect(streams[1].stream.lastSubscriptionMessages.size).toEqual(1);

				streams[1].stream.lastSubscriptionMessages.delete(dummyPeer);
				expect(streams[1].stream.lastSubscriptionMessages.size).toEqual(0);
			});
		});
	});
});
