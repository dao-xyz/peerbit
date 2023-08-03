import { DataMessage, Message } from "@peerbit/stream-interface";
import { waitForPeers } from "@peerbit/stream";
import { LSession } from "@peerbit/libp2p-test-utils";
import { waitFor, delay, waitForResolved } from "@peerbit/time";
import {
	PubSubMessage,
	PubSubData,
	Subscribe,
	Unsubscribe,
	GetSubscribers,
	SubscriptionEvent,
	UnsubcriptionEvent,
	DataEvent,
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
		subscriptionEvents: SubscriptionEvent[];
		unsubscriptionEvents: UnsubcriptionEvent[];
	} = {
		messages: [],
		received: [],
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
	return m;
};

describe("pubsub", function () {
	describe("topic", () => {
		let session: LSession<{ pubsub: DirectSub }>;
		let metrics: ReturnType<typeof createMetrics>[] = [];

		beforeEach(async () => {
			metrics = [];
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
			for (const peer of session.peers.slice(0, 2)) {
				metrics.push(createMetrics(peer.services.pubsub));
			}

			const TOPIC = "world";
			metrics[0].stream.subscribe(TOPIC);
			metrics[1].stream.subscribe(TOPIC);

			await delay(1000); // wait for subscription message to propagate (if any)
			// now connect peers and make sure that subscription information is passed on as they connect
			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitForSubscribers(session.peers[0], [session.peers[1]], TOPIC);
			await waitForSubscribers(session.peers[1], [session.peers[0]], TOPIC);
			await Promise.all(metrics.map((s) => s.stream.stop()));
		});

		it("can share topics when connecting after subscribe, 3 peers and 1 relay", async () => {
			let metrics: ReturnType<typeof createMetrics>[] = [];
			for (const peer of session.peers) {
				metrics.push(createMetrics(peer.services.pubsub));
			}

			const TOPIC = "world";
			metrics[0].stream.subscribe(TOPIC);
			// peers[1] is not subscribing
			metrics[2].stream.subscribe(TOPIC);

			await delay(1000); // wait for subscription message to propagate (if any)
			// now connect peers and make sure that subscription information is passed on as they connect
			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
			]);
			await waitForSubscribers(session.peers[0], [session.peers[2]], TOPIC);
			await waitForSubscribers(session.peers[2], [session.peers[0]], TOPIC);
			await Promise.all(metrics.map((x) => x.stream.stop()));
		});
	});

	describe("publish", () => {
		let session: LSession<{ pubsub: DirectSub }>;
		let metrics: ReturnType<typeof createMetrics>[];
		const data = new Uint8Array([1, 2, 3]);
		const TOPIC = "world";

		describe("line", () => {
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

				metrics = [];
				for (const peer of session.peers) {
					metrics.push(createMetrics(peer.services.pubsub));
				}
				await waitForPeers(metrics[0].stream, metrics[1].stream);
				await waitForPeers(metrics[1].stream, metrics[2].stream);
				await delay(1000);

				await metrics[0].stream.subscribe(TOPIC);
				await metrics[1].stream.subscribe(TOPIC);
				await metrics[2].stream.subscribe(TOPIC);

				for (let i = 0; i < metrics.length; i++) {
					for (let j = 0; j < metrics.length; j++) {
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
				for (let i = 0; i < metrics.length; i++) {
					metrics[i].stream.unsubscribe(TOPIC);
				}
				for (let i = 0; i < metrics.length; i++) {
					await waitFor(() => !metrics[i].stream.getSubscribers(TOPIC)?.size);
					expect(metrics[i].stream.topics.has(TOPIC)).toBeFalse();
					expect(metrics[i].stream.subscriptions.has(TOPIC)).toBeFalse();
				}

				await session.stop();
			});

			it("1->TOPIC", async () => {
				await metrics[0].stream.publish(data, { topics: [TOPIC] });
				await waitFor(() => metrics[1].received.length === 1);
				expect(new Uint8Array(metrics[1].received[0].data)).toEqual(data);
				expect(metrics[1].received[0].topics).toEqual([TOPIC]);
				await waitFor(() => metrics[2].received.length === 1);
				expect(new Uint8Array(metrics[2].received[0].data)).toEqual(data);
				await delay(3000); // wait some more time to make sure we dont get more messages
				expect(metrics[1].received).toHaveLength(1);
				expect(metrics[2].received).toHaveLength(1);
			});

			it("1->TOPIC strict to", async () => {
				await metrics[0].stream.publish(data, {
					topics: [TOPIC],
					to: [metrics[2].stream.publicKey],
					strict: true,
				});
				await waitForResolved(() =>
					expect(metrics[2].received).toHaveLength(1)
				);
				expect(new Uint8Array(metrics[2].received[0].data)).toEqual(data);
				expect(metrics[2].received[0].topics).toEqual([TOPIC]);
				expect(metrics[1].received).toHaveLength(0);
				await delay(3000); // wait some more time to make sure we dont get more messages
				expect(metrics[1].received).toHaveLength(0);
				expect(metrics[2].received).toHaveLength(1);
			});

			it("send without topic directly", async () => {
				await metrics[0].stream.publish(data, {
					to: [metrics[1].stream.components.peerId],
				});
				await waitFor(() => metrics[1].received.length === 1);
				expect(new Uint8Array(metrics[1].received[0].data)).toEqual(data);
				await delay(3000); // wait some more time to make sure we dont get more messages
				expect(metrics[1].received).toHaveLength(1);
				expect(metrics[2].received).toHaveLength(0);
			});

			it("send without topic over relay", async () => {
				await metrics[0].stream.publish(data, {
					to: [metrics[2].stream.components.peerId],
				});
				await waitFor(() => metrics[2].received.length === 1);
				expect(new Uint8Array(metrics[2].received[0].data)).toEqual(data);
				await delay(3000); // wait some more time to make sure we dont get more messages
				expect(metrics[2].received).toHaveLength(1);
				expect(metrics[1].received).toHaveLength(0);
			});
			it("can send as non subscribeer", async () => {
				metrics[0].stream.unsubscribe(TOPIC);
				metrics[1].stream.unsubscribe(TOPIC);
				await metrics[0].stream.publish(data, { topics: [TOPIC] });
				await waitFor(() => metrics[2].received.length === 1);
				expect(new Uint8Array(metrics[2].received[0].data)).toEqual(data);
				await delay(3000); // wait some more time to make sure we dont get more messages
				expect(metrics[1].received).toHaveLength(0);
				expect(metrics[2].received).toHaveLength(1);
			});
		});

		describe("fully connected", () => {
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

				await session.connect([
					[session.peers[0], session.peers[1]],
					[session.peers[1], session.peers[2]],
					[session.peers[0], session.peers[2]],
				]);

				metrics = [];
				for (const peer of session.peers) {
					metrics.push(createMetrics(peer.services.pubsub));
				}
				await waitForPeers(metrics[0].stream, metrics[1].stream);
				await waitForPeers(metrics[1].stream, metrics[2].stream);
				await delay(1000);

				await metrics[0].stream.subscribe(TOPIC);
				await metrics[1].stream.subscribe(TOPIC);
				await metrics[2].stream.subscribe(TOPIC);

				for (let i = 0; i < metrics.length; i++) {
					for (let j = 0; j < metrics.length; j++) {
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
				for (let i = 0; i < metrics.length; i++) {
					metrics[i].stream.unsubscribe(TOPIC);
				}
				for (let i = 0; i < metrics.length; i++) {
					await waitFor(() => !metrics[i].stream.getSubscribers(TOPIC)?.size);
					expect(metrics[i].stream.topics.has(TOPIC)).toBeFalse();
					expect(metrics[i].stream.subscriptions.has(TOPIC)).toBeFalse();
				}

				await session.stop();
			});

			it("concurrently", async () => {
				// Purpose of this test is to check if there exist any dead-locks
				// possibly than can arise from bi-directional writing (?)
				// for examples, is processRpc does result in sending a message back to the same sender
				// it could cause issues. The exact circumstances/reasons for this is unknown, not specified

				const hasData = (d: Uint8Array, i: number) => {
					return !!metrics[i].received.find((x) => equals(x.data, d));
				};
				const fn = async (i: number) => {
					const d = randomBytes(999);
					await metrics[i % 3].stream.publish(d, {
						to: [
							metrics[(i + 1) % session.peers.length].stream.publicKeyHash,
							metrics[(i + 2) % session.peers.length].stream.publicKeyHash,
						],
						strict: true,
						topics: [TOPIC],
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
				session = await LSession.disconnected(2, {
					services: {
						pubsub: (c) =>
							new DirectSub(c, {
								emitSelf: true,
								canRelayMessage: true,
								connectionManager: { autoDial: false },
							}),
					},
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

			let session: LSession<{ pubsub: DirectSub }>;
			let metrics: ReturnType<typeof createMetrics>[];

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

				metrics = [];
				for (const [i, peer] of session.peers.entries()) {
					metrics.push(createMetrics(peer.services.pubsub));
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

				for (const [i, peer] of metrics.entries()) {
					if (i !== 3) {
						await peer.stream.requestSubscribers(TOPIC);
					}
					await waitFor(
						() => peer.stream.getSubscribers(TOPIC)?.size === (i === 3 ? 0 : 1)
					); // all others (except 4 which is not subscribing)
				}
			});

			afterEach(async () => {
				await Promise.all(metrics.map((peer) => peer.stream.stop()));
				await session.stop();
			});
			afterAll(async () => {});

			it("will publish on routes", async () => {
				metrics[3].received = [];
				metrics[4].received = [];
				await metrics[0].stream.publish(data, { topics: [TOPIC] });
				await waitFor(() => metrics[3].received.length === 1);
				expect(new Uint8Array(metrics[3].received[0].data)).toEqual(data);

				await delay(1000); // some delay to allow all messages to progagate
				expect(metrics[4].received).toHaveLength(0);
				// make sure data message did not arrive to peer 4
				for (const message of metrics[4].messages) {
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
			let peers: ReturnType<typeof createMetrics>[];

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
					peers.push(createMetrics(peer.services.pubsub));

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
				peers[1].received = [];
				peers[2].received = [];
				await delay(5000);
				await peers[0].stream.publish(data, { topics: [TOPIC] });
				await waitFor(() => peers[1].received.length === 1);
				expect(new Uint8Array(peers[1].received[0].data)).toEqual(data);

				await delay(1000); // some delay to allow all messages to progagate
				expect(peers[2].received).toHaveLength(0);
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
		let peers: ReturnType<typeof createMetrics>[];
		const data = new Uint8Array([1, 2, 3]);
		const TOPIC_1 = "hello";
		const TOPIC_2 = "world";

		beforeEach(async () => {
			// 0 and 2 not connected
			session = await LSession.disconnected(3, {
				transports: [tcp(), webSockets({ filter: filters.all })],
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
				peers.push(createMetrics(peer.services.pubsub));
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

		describe("invalidation", () => {
			it("uses timestamp to ignore old events", async () => {
				const pubsubMetrics0 = createSubscriptionMetrics(peers[0].stream);
				const pubsubMetrics1 = createSubscriptionMetrics(peers[1].stream);
				await peers[1].stream.requestSubscribers(TOPIC_1);

				await waitForResolved(() =>
					expect(pubsubMetrics0.getSubscriptions).toHaveLength(1)
				);

				pubsubMetrics1.subscriptions = [];

				await peers[0].stream.subscribe(TOPIC_1);
				await waitForResolved(() =>
					expect(pubsubMetrics1.subscriptions).toHaveLength(1)
				);

				expect(peers[1].stream.getSubscribers(TOPIC_1)!.size).toEqual(1);

				await peers[0].stream.unsubscribe(TOPIC_1);
				await waitForResolved(() =>
					expect(pubsubMetrics1.unsubscriptions).toHaveLength(1)
				);

				expect(peers[1].stream.getSubscribers(TOPIC_1)!.size).toEqual(0);

				// reprocess first subscription message and make sure its ignored
				await peers[1].stream.onDataMessage(
					session.peers[0].peerId,
					[...peers[1].stream.peers.values()][0],
					pubsubMetrics1.subscriptions[0]
				);

				expect(peers[1].stream.getSubscribers(TOPIC_1)!.size).toEqual(0);

				// resubscribe again and try to send old unsubscription
				pubsubMetrics1.subscriptions = [];
				await peers[0].stream.subscribe(TOPIC_1);
				await waitForResolved(() =>
					expect(pubsubMetrics1.subscriptions).toHaveLength(1)
				);
				expect(peers[1].stream.getSubscribers(TOPIC_1)!.size).toEqual(1);

				await peers[1].stream.onDataMessage(
					session.peers[0].peerId,
					[...peers[1].stream.peers.values()][0],
					pubsubMetrics1.unsubscriptions[0]
				);
				expect(peers[1].stream.getSubscribers(TOPIC_1)!.size).toEqual(1); // No change, since message was old

				expect(peers[1].stream.lastSubscriptionMessages.size).toEqual(1);
				await session.peers[0].stop();
				await waitForResolved(() =>
					expect(peers[1].stream.lastSubscriptionMessages.size).toEqual(0)
				);
			});

			it("will clear lastSubscriptionMessages on unsubscribe", async () => {
				await peers[1].stream.requestSubscribers(TOPIC_1);

				await peers[0].stream.subscribe(TOPIC_1);
				await waitForResolved(() =>
					expect(peers[1].stream.getSubscribers(TOPIC_1)!.size).toEqual(1)
				);
				expect(peers[1].stream.lastSubscriptionMessages.size).toEqual(1);
				let dummyPeer = "x";
				peers[1].stream.lastSubscriptionMessages.set(dummyPeer, new Map());
				expect(peers[1].stream.lastSubscriptionMessages.size).toEqual(2);

				await peers[1].stream.unsubscribe(TOPIC_1);
				expect(peers[1].stream.lastSubscriptionMessages.size).toEqual(1);

				peers[1].stream.lastSubscriptionMessages.delete(dummyPeer);
				expect(peers[1].stream.lastSubscriptionMessages.size).toEqual(0);
			});
		});
	});
});
