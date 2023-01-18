import {
	DataMessage,
	Message,
	waitForPeers,
	logger,
} from "@dao-xyz/libp2p-direct-stream";
import { LSession } from "@dao-xyz/libp2p-test-utils";
import { waitFor, delay } from "@dao-xyz/peerbit-time";
import { PubSubMessage, PubSubData } from "../messages.js";
import { DirectSub } from "./../index.js";
import { deserialize } from "@dao-xyz/borsh";

describe("pubsub", function () {
	describe("topic", () => {
		let session: LSession;
		let peers: {
			stream: DirectSub;
			messages: Message[];
			recieved: PubSubData[];
		}[] = [];

		beforeEach(async () => {
			peers = [];
			session = await LSession.disconnected(3);
		});
		afterEach(async () => {
			await Promise.all(peers.map((peer) => peer.stream.stop()));
			await session.stop();
		});
		it("can share topics when connecting after subscribe, 2 peers", async () => {
			for (const peer of session.peers.slice(0, 2)) {
				const stream = new DirectSub(peer, { canRelayMessage: true });
				const client: {
					stream: DirectSub;
					messages: Message[];
					recieved: PubSubData[];
				} = {
					messages: [],
					recieved: [],
					stream,
				};

				peers.push(client);
				stream.addEventListener("message", (msg) => {
					client.messages.push(msg.detail);
				});
				stream.addEventListener("data", (msg) => {
					client.recieved.push(msg.detail);
				});
				await stream.start();
			}

			const TOPIC = "world";
			peers[0].stream.subscribe(TOPIC);
			peers[1].stream.subscribe(TOPIC);

			await delay(1000); // wait for subscription message to propagate (if any)
			// now connect peers and make sure that subscription information is passed on as they connect
			await session.connect([[session.peers[0], session.peers[1]]]);
			await waitFor(() =>
				peers[0].stream
					.getPeersOnTopic(TOPIC)
					?.has(peers[1].stream.publicKeyHash)
			);
			await waitFor(() =>
				peers[1].stream
					.getPeersOnTopic(TOPIC)
					?.has(peers[0].stream.publicKeyHash)
			);
		});

		it("can share topics when connecting after subscribe, 3 peers and 1 relay", async () => {
			let peers: {
				stream: DirectSub;
				messages: Message[];
				recieved: PubSubData[];
			}[] = [];
			for (const peer of session.peers) {
				const stream = new DirectSub(peer, { canRelayMessage: true });
				const client: {
					stream: DirectSub;
					messages: Message[];
					recieved: PubSubData[];
				} = {
					messages: [],
					recieved: [],
					stream,
				};

				peers.push(client);
				stream.addEventListener("message", (msg) => {
					client.messages.push(msg.detail);
				});
				stream.addEventListener("data", (msg) => {
					client.recieved.push(msg.detail);
				});
				await stream.start();
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
					.getPeersOnTopic(TOPIC)
					?.has(peers[2].stream.publicKeyHash)
			);
			await waitFor(() =>
				peers[2].stream
					.getPeersOnTopic(TOPIC)
					?.has(peers[0].stream.publicKeyHash)
			);
		});
	});

	describe("publish", () => {
		let session: LSession;
		let peers: {
			stream: DirectSub;
			messages: Message[];
			recieved: PubSubData[];
		}[];
		const data = new Uint8Array([1, 2, 3]);
		const TOPIC = "world";


		beforeEach(async () => {
			// 0 and 2 not connected
			session = await LSession.disconnected(3);

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
				const stream = new DirectSub(peer, { canRelayMessage: true });
				const client: {
					stream: DirectSub;
					messages: Message[];
					recieved: PubSubData[];
				} = {
					messages: [],
					recieved: [],
					stream,
				};
				peers.push(client);
				stream.addEventListener("message", (msg) => {
					client.messages.push(msg.detail);
				});
				stream.addEventListener("data", (msg) => {
					client.recieved.push(msg.detail);
				});
				await stream.start();
			}
			await waitForPeers(peers[0].stream, peers[1].stream);
			await waitForPeers(peers[1].stream, peers[2].stream);
			await delay(1000);

			peers[0].stream.subscribe(TOPIC);
			peers[1].stream.subscribe(TOPIC);
			peers[2].stream.subscribe(TOPIC);

			for (let i = 0; i < peers.length; i++) {
				for (let j = 0; j < peers.length; j++) {
					if (i == j) {
						continue;
					}
					await waitFor(() =>
						peers[i].stream
							.getPeersOnTopic(TOPIC)
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
				await waitFor(
					() => !peers[i].stream.getPeersOnTopic(TOPIC)?.size
				);
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

		it("send without topic directly", async () => {
			await peers[0].stream.publish(data, {
				to: [peers[1].stream.libp2p.peerId],
			});
			await waitFor(() => peers[1].recieved.length === 1);
			expect(new Uint8Array(peers[1].recieved[0].data)).toEqual(data);
			await delay(3000); // wait some more time to make sure we dont get more messages
			expect(peers[1].recieved).toHaveLength(1);
			expect(peers[2].recieved).toHaveLength(0);
		});

		it("send without topic over relay", async () => {
			await peers[0].stream.publish(data, {
				to: [peers[2].stream.libp2p.peerId],
			});
			await waitFor(() => peers[2].recieved.length === 1);
			expect(new Uint8Array(peers[2].recieved[0].data)).toEqual(data);
			await delay(3000); // wait some more time to make sure we dont get more messages
			expect(peers[2].recieved).toHaveLength(1);
			expect(peers[1].recieved).toHaveLength(0);
		});
	});

	describe("routing", () => {
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

		let session: LSession;
		let peers: {
			stream: DirectSub;
			messages: Message[];
			recieved: PubSubData[];
		}[];

		const data = new Uint8Array([1, 2, 3]);
		const TOPIC = "world";
		beforeAll(async () => { });
		beforeEach(async () => {
			session = await LSession.disconnected(5);
			peers = [];
			for (const [i, peer] of session.peers.entries()) {
				const stream = new DirectSub(peer, { canRelayMessage: true });
				const client: {
					stream: DirectSub;
					messages: Message[];
					recieved: PubSubData[];
				} = {
					messages: [],
					recieved: [],
					stream,
				};
				peers.push(client);
				stream.addEventListener("message", (msg) => {
					client.messages.push(msg.detail);
				});
				stream.addEventListener("data", (msg) => {
					client.recieved.push(msg.detail);
				});
				await stream.start();

				if (i === 4) {
					// dont subscribe
				} else {
					stream.subscribe(TOPIC);
				}
			}

			await session.connect([
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
				[session.peers[2], session.peers[3]],
				[session.peers[2], session.peers[4]],
			]);

			for (const [i, peer] of peers.entries()) {
				try {
					if (i === 4) {
						await peer.stream.requestSubscribers(TOPIC);
					}
					await waitFor(
						() =>
							peer.stream.getPeersOnTopic(TOPIC)?.size ===
							(i === 4 ? 4 : 3)
					); // all others (except 4 which is not subscribing)
				} catch (error) {
					const x = 123;
				}
			}
		});

		afterEach(async () => {
			await Promise.all(peers.map((peer) => peer.stream.stop()));
			await session.stop();
		});
		afterAll(async () => { });

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
					const pubsubMessage = deserialize(
						message.data,
						PubSubMessage
					);
					expect(pubsubMessage).not.toBeInstanceOf(PubSubData);
				}
			}
		});
	});

	describe('join/leave', () => {
		let session: LSession;
		let peers: {
			stream: DirectSub;
			messages: Message[];
			recieved: PubSubData[];
		}[];
		const data = new Uint8Array([1, 2, 3]);
		const TOPIC_1 = "hello";
		const TOPIC_2 = "world";

		beforeEach(async () => {
			// 0 and 2 not connected
			session = await LSession.disconnected(3);

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
				const stream = new DirectSub(peer, { canRelayMessage: true });
				const client: {
					stream: DirectSub;
					messages: Message[];
					recieved: PubSubData[];
				} = {
					messages: [],
					recieved: [],
					stream,
				};
				peers.push(client);
				stream.addEventListener("message", (msg) => {
					client.messages.push(msg.detail);
				});
				stream.addEventListener("data", (msg) => {
					client.recieved.push(msg.detail);
				});
				await stream.start();
			}
			await waitForPeers(peers[0].stream, peers[1].stream);
			await waitForPeers(peers[1].stream, peers[2].stream);
			await delay(1000);
			for (let i = 0; i < peers.length; i++) {
				for (let j = 0; j < peers.length; j++) {
					if (i == j) {
						continue;
					}
				}
			}
		});

		afterEach(async () => {
			await Promise.all(peers.map((peer) => peer.stream.stop()));
			await session.stop();
		});
		it('it can track subscriptions across peers', async () => {
			for (const peer of peers) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}

			peers[0].stream.subscribe(TOPIC_1);
			await waitFor(() => peers[2].stream.getSubscribers(TOPIC_1).has(peers[0].stream.publicKeyHash));
			await waitFor(() => peers[1].stream.getSubscribers(TOPIC_1).has(peers[0].stream.publicKeyHash));
			peers[0].stream.stop()
			await waitFor(() => !peers[2].stream.getSubscribers(TOPIC_1).has(peers[0].stream.publicKeyHash));
			await waitFor(() => !peers[1].stream.getSubscribers(TOPIC_1).has(peers[0].stream.publicKeyHash));
		})

		it('can unsubscribe across peers', async () => {
			for (const peer of peers) {
				await peer.stream.requestSubscribers(TOPIC_1);
				await peer.stream.requestSubscribers(TOPIC_2);
			}

			peers[0].stream.subscribe(TOPIC_1);
			await waitFor(() => peers[2].stream.getSubscribers(TOPIC_1).has(peers[0].stream.publicKeyHash));
			await waitFor(() => peers[1].stream.getSubscribers(TOPIC_1).has(peers[0].stream.publicKeyHash));
			peers[0].stream.unsubscribe(TOPIC_1);
			await waitFor(() => !peers[2].stream.getSubscribers(TOPIC_1).has(peers[0].stream.publicKeyHash));
			await waitFor(() => !peers[1].stream.getSubscribers(TOPIC_1).has(peers[0].stream.publicKeyHash));
		})
	})
});
