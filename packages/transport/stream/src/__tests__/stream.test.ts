import { LibP2POptions, TestSession } from "@peerbit/libp2p-test-utils";
import { waitFor, delay, waitForResolved } from "@peerbit/time";
import crypto from "crypto";
import {
	waitForPeers as waitForPeerStreams,
	DirectStream,
	ConnectionManagerOptions,
	DirectStreamComponents
} from "..";
import { DataMessage, Message, getMsgId } from "@peerbit/stream-interface";
import { PublicSignKey } from "@peerbit/crypto";
import { PeerId, isPeerId } from "@libp2p/interface/peer-id";
import { Multiaddr } from "@multiformats/multiaddr";
import { multiaddr } from "@multiformats/multiaddr";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import * as filters from "@libp2p/websockets/filters";
import { serialize } from "@dao-xyz/borsh";

const createMetrics = (stream: DirectStream) => {
	const s: {
		stream: TestDirectStream;
		messages: Message[];
		received: DataMessage[];
		reachable: PublicSignKey[];
		unrechable: PublicSignKey[];
		seen: Map<string, number>;
	} = {
		messages: [],
		received: [],
		reachable: [],
		unrechable: [],
		seen: new Map(),
		stream
	};
	s.stream.addEventListener("message", (msg) => {
		s.messages.push(msg.detail);
	});
	s.stream.addEventListener("data", (msg) => {
		s.received.push(msg.detail);
	});
	s.stream.addEventListener("peer:reachable", (msg) => {
		s.reachable.push(msg.detail);
	});
	s.stream.addEventListener("peer:unreachable", (msg) => {
		s.unrechable.push(msg.detail);
	});

	let seenHas = s.stream.seenCache.has.bind(s.stream.seenCache);
	s.stream.seenCache.has = (k) => {
		let prev = s.seen.get(k);
		s.seen.set(k, (prev ?? 0) + 1);
		return seenHas(k);
	};
	return s;
};
class TestDirectStream extends DirectStream {
	constructor(
		components: DirectStreamComponents,
		options: {
			id?: string;
			pingInterval?: number | null;
			connectionManager?: ConnectionManagerOptions;
		} = {}
	) {
		super(components, [options.id || "test/0.0.0"], {
			canRelayMessage: true,
			emitSelf: true,
			connectionManager: options.connectionManager || {
				autoDial: false
			},
			...options
		});
	}
}
type TestSessionStream = TestSession<{ directstream: DirectStream }>;
const connected = async (
	n: number,
	options?:
		| LibP2POptions<{ directstream: TestDirectStream }>
		| LibP2POptions<{ directstream: TestDirectStream }>[]
) => {
	let session: TestSessionStream = await TestSession.connected(
		n,
		options || {
			services: {
				directstream: (components) => new TestDirectStream(components, options)
			}
		}
	);
	return session;
};

const disconnected = async (
	n: number,
	options?:
		| LibP2POptions<{ directstream: TestDirectStream }>
		| LibP2POptions<{ directstream: TestDirectStream }>[]
) => {
	let session: TestSessionStream = await TestSession.disconnected(
		n,
		options || {
			services: {
				directstream: (components) => new TestDirectStream(components, options)
			}
		}
	);
	return session;
};

const stream = (s: TestSessionStream, i: number): TestDirectStream =>
	service(s, i, "directstream") as TestDirectStream;
const service = (s: TestSessionStream, i: number, service: string) =>
	s.peers[i].services[service];
const waitForPeers = (s: TestSessionStream) =>
	waitForPeerStreams(...s.peers.map((x) => x.services.directstream));

describe("streams", function () {
	describe("ping", () => {
		let session: TestSessionStream;

		afterEach(async () => {
			await session?.stop();
		});

		it("2-ping", async () => {
			// 0 and 2 not connected
			session = await connected(2);
			await waitForPeers(session);

			// Pings can be aborted, by the interval pinging, so we just need to check that eventually we get results
			await stream(session, 0).ping(
				stream(session, 0).peers.get(stream(session, 1).publicKeyHash)!
			);
			await waitFor(
				() =>
					stream(session, 0).peers.get(stream(session, 1).publicKeyHash)
						?.pingLatency! < 1000
			);
		});

		it("4-ping", async () => {
			// 0 and 2 not connected
			session = await connected(4);
			await waitForPeers(session);

			// Pings can be aborted, by the interval pinging, so we just need to check that eventually we get results
			await stream(session, 0).ping(
				stream(session, 0).peers.get(stream(session, 1).publicKeyHash)!
			);
			await waitFor(
				() =>
					stream(session, 0).peers.get(stream(session, 1).publicKeyHash)
						?.pingLatency! < 1000
			);
		});
		// TODO add test to make sure Hello's are not resent uneccessary amount of times

		it("ping interval", async () => {
			// 0 and 2 not connected
			session = await connected(2, {
				services: {
					directstream: (c) => new TestDirectStream(c, { pingInterval: 1000 })
				}
			});
			await waitForPeers(session);

			let counter = 0;
			const pingFn = stream(session, 0).onPing.bind(stream(session, 0));
			stream(session, 0).onPing = (a, b, c) => {
				counter += 1;
				return pingFn(a, b, c);
			};
			await waitFor(() => counter > 5);
		});
	});

	describe("publish", () => {
		const data = new Uint8Array([1, 2, 3]);

		describe("shortest path", () => {
			let session: TestSessionStream;
			let streams: ReturnType<typeof createMetrics>[];

			beforeAll(async () => {});

			beforeEach(async () => {
				// 0 and 2 not connected
				session = await disconnected(4, {
					services: {
						directstream: (c) =>
							new TestDirectStream(c, {
								pingInterval: null,
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
				└┬┘
				┌▽┐
				│3│
				└─┘
				*/

				streams = [];
				for (const peer of session.peers) {
					streams.push(createMetrics(peer.services.directstream));
				}
				await session.connect([
					// behaviour seems to be more predictable if we connect after start (TODO improve startup to use existing connections in a better way)
					[session.peers[0], session.peers[1]],
					[session.peers[1], session.peers[2]],
					[session.peers[2], session.peers[3]]
				]);

				await waitForPeerStreams(streams[0].stream, streams[1].stream);
				await waitForPeerStreams(streams[1].stream, streams[2].stream);
				await waitForPeerStreams(streams[2].stream, streams[3].stream);
			});

			afterEach(async () => {
				await session.stop();
			});

			it("many", async () => {
				let iterations = 300;

				for (let i = 0; i < iterations; i++) {
					const small = crypto.randomBytes(1e3); // 1kb
					streams[0].stream.publish(small);
				}
				await waitFor(() => streams[2].received.length === iterations, {
					delayInterval: 300,
					timeout: 30 * 1000
				});
			});

			it("1->unknown", async () => {
				await streams[0].stream.publish(data);
				await waitFor(() => streams[1].received.length === 1);
				expect(new Uint8Array(streams[1].received[0].data)).toEqual(data);
				await waitFor(() => streams[2].received.length === 1);
				expect(new Uint8Array(streams[2].received[0].data)).toEqual(data);
				await delay(1000); // wait some more time to make sure we dont get more messages
				expect(streams[1].received).toHaveLength(1);
				expect(streams[2].received).toHaveLength(1);
			});

			it("1->2", async () => {
				await streams[0].stream.publish(data, {
					to: [streams[1].stream.components.peerId]
				});

				await waitFor(() => streams[1].received.length === 1);
				let receivedMessage = streams[1].received[0];
				expect(new Uint8Array(receivedMessage.data)).toEqual(data);
				await delay(1000); // wait some more time to make sure we dont get more messages
				expect(streams[1].received).toHaveLength(1);
				expect(streams[2].received).toHaveLength(0);

				// Never seen a message twice
				expect(
					[...streams[0].seen.values()].find((x) => x > 1)
				).toBeUndefined();
				expect(
					[...streams[1].seen.values()].find((x) => x > 1)
				).toBeUndefined();
				expect(
					[...streams[2].seen.values()].find((x) => x > 1)
				).toBeUndefined();
			});

			it("1->3", async () => {
				await streams[0].stream.publish(data, {
					to: [streams[2].stream.components.peerId]
				});
				await waitForResolved(() =>
					expect(streams[2].received).toHaveLength(1)
				);
				expect(new Uint8Array(streams[2].received[0].data)).toEqual(data);
				await delay(1000); // wait some more time to make sure we dont get more messages
				expect(streams[2].received).toHaveLength(1);
				expect(streams[1].received).toHaveLength(0);
			});

			it("1->3 10mb data", async () => {
				const bigData = crypto.randomBytes(1e7);
				await streams[0].stream.publish(bigData, {
					to: [streams[2].stream.components.peerId]
				});

				await waitForResolved(() =>
					expect(streams[2].received).toHaveLength(1)
				);

				expect(new Uint8Array(streams[2].received[0].data)).toHaveLength(
					bigData.length
				);
				expect(streams[2].received).toHaveLength(1);
				expect(streams[1].received).toHaveLength(0);
			});

			it("1->3 still works even if routing is missing", async () => {
				streams[0].stream.routes.clear();
				streams[1].stream.routes.clear();
				await streams[0].stream.publish(data, {
					to: [streams[2].stream.components.peerId]
				});
				await waitForResolved(() =>
					expect(streams[2].received).toHaveLength(1)
				);
				expect(new Uint8Array(streams[2].received[0].data)).toEqual(data);
				await delay(1000); // wait some more time to make sure we dont get more messages
				expect(streams[2].received).toHaveLength(1);
				expect(streams[1].received).toHaveLength(0);
			});

			it("publishes on direct stream, even path is longer", async () => {
				await session.connect([[session.peers[0], session.peers[2]]]);
				await waitForPeerStreams(streams[0].stream, streams[2].stream);

				// make path 1->3 longest, to make sure we send over it directly anyways because it is a direct path
				streams[0].stream.routes.graph.setEdgeAttribute(
					streams[0].stream.routes.getLink(
						streams[0].stream.publicKeyHash,
						streams[2].stream.publicKeyHash
					),
					"weight",
					1e5
				);
				await streams[0].stream.publish(crypto.randomBytes(1e2), {
					to: [streams[2].stream.components.peerId]
				});
				streams[1].messages = [];
				await waitForResolved(() =>
					expect(streams[2].received).toHaveLength(1)
				);

				expect(
					streams[1].messages.filter((x) => x instanceof DataMessage)
				).toHaveLength(0);
			});

			it("will favor shortest path", async () => {
				/* 
				┌───┐
				│0  │
				└┬─┬┘
				 │┌▽┐
				 ││1│
				 │└┬┘
				┌▽─▽┐
				│2  │
				└┬──┘
				┌▽┐  
				│3│  
				└─┘   
				*/

				await session.connect([[session.peers[0], session.peers[2]]]);
				await waitForPeerStreams(streams[0].stream, streams[2].stream);

				const defaultEdgeWeightFnPeer0 =
					streams[0].stream.routes.graph.getEdgeAttribute.bind(
						streams[0].stream.routes.graph
					);

				let link02 = streams[0].stream.routes.getLink(
					streams[0].stream.publicKeyHash,
					streams[2].stream.publicKeyHash
				);

				// make path from 0 -> 2 long, so data will be sent in the path 0 -> 1 -> 2 -> 3
				streams[0].stream.routes.graph.getEdgeAttribute = (
					edge: unknown,
					name: any
				) => {
					if (edge === link02) {
						return 1e5;
					}
					return defaultEdgeWeightFnPeer0(edge, name);
				};

				await streams[0].stream.publish(crypto.randomBytes(1e2), {
					to: [streams[3].stream.components.peerId]
				});

				streams[1].messages = [];

				// will send through peer [1] since path [0] -> [2] -> [3] directly is currently longer
				await waitForResolved(() =>
					expect(
						streams[1].messages.filter((x) => x instanceof DataMessage)
					).toHaveLength(1)
				);

				await waitForResolved(() =>
					expect(streams[3].received).toHaveLength(1)
				);

				streams[1].messages = [];

				// Make [0] -> [2] path short
				streams[0].stream.routes.graph.getEdgeAttribute = (
					edge: unknown,
					name: any
				) => {
					if (edge === link02) {
						return 0;
					}
					return defaultEdgeWeightFnPeer0(edge, name);
				};

				expect(
					streams[0].stream.routes.getPath(
						streams[0].stream.publicKeyHash,
						streams[2].stream.publicKeyHash
					).length
				).toEqual(2);
				await streams[0].stream.publish(crypto.randomBytes(1e2), {
					to: [streams[3].stream.components.peerId]
				});
				await waitFor(() => streams[3].received.length === 1);
				const messages = streams[1].messages.filter(
					(x) => x instanceof DataMessage
				);

				// no new messages for peer 1, because sending 0 -> 2 -> 3 directly is now faster
				expect(messages).toHaveLength(0);
				expect(streams[1].received).toHaveLength(0);
			});
		});

		describe("fanout", () => {
			describe("basic", () => {
				let session: TestSessionStream;
				let streams: ReturnType<typeof createMetrics>[];

				beforeAll(async () => {});

				beforeEach(async () => {
					session = await connected(3, {
						services: {
							directstream: (c) =>
								new TestDirectStream(c, {
									connectionManager: { autoDial: false }
								})
						}
					});
					streams = [];
					for (const peer of session.peers) {
						streams.push(createMetrics(peer.services.directstream));
					}

					await waitForPeerStreams(streams[0].stream, streams[1].stream);
				});

				afterEach(async () => {
					await session.stop();
				});

				/**
				 * If tests below fails, dead-locks can apphear in unpredictable ways
				 */
				it("will not publish to from when explicitly providing to", async () => {
					const msg = new DataMessage({ data: new Uint8Array([0]) });
					await msg.sign(streams[1].stream.sign);
					streams[2].stream.canRelayMessage = false; // so that 2 does not relay to 0
					await streams[1].stream.publishMessage(session.peers[0].peerId, msg, [
						streams[1].stream.peers.get(streams[0].stream.publicKeyHash)!,
						streams[1].stream.peers.get(streams[2].stream.publicKeyHash)!
					]);
					const msgId = await getMsgId(msg.bytes());
					await waitForResolved(() =>
						expect(streams[2].seen.get(msgId)).toEqual(1)
					);

					await delay(1000); // wait for more messages eventually propagate
					expect(streams[0].seen.get(msgId)).toBeUndefined();
					expect(streams[1].seen.get(msgId)).toBeUndefined();
				});

				it("to in message will not send back", async () => {
					const msg = new DataMessage({
						data: new Uint8Array([0]),
						to: [
							streams[0].stream.publicKeyHash,
							streams[2].stream.publicKeyHash
						]
					});
					await msg.sign(streams[1].stream.sign);
					await streams[1].stream.publishMessage(session.peers[0].peerId, msg);
					await delay(1000);
					const msgId = await getMsgId(msg.bytes());
					expect(streams[0].seen.get(msgId)).toBeUndefined();
					expect(streams[1].seen.get(msgId)).toBeUndefined();
					expect(streams[2].seen.get(msgId)).toEqual(1);
				});

				it("rejects when to peers is from", async () => {
					const msg = new DataMessage({ data: new Uint8Array([0]) });
					await msg.sign(streams[1].stream.sign);
					await expect(
						streams[1].stream.publishMessage(session.peers[0].peerId, msg, [
							streams[1].stream.peers.get(streams[0].stream.publicKeyHash)!
						])
					).rejects.toThrowError("Message did not have any valid receivers");
				});
				it("rejects when only to is from", async () => {
					const msg = new DataMessage({
						data: new Uint8Array([0]),
						to: [streams[1].stream.publicKeyHash]
					});
					await msg.sign(streams[1].stream.sign);
					await streams[1].stream.publishMessage(session.peers[0].peerId, msg);
					const msgId = await getMsgId(msg.bytes());
					await delay(1000);
					expect(streams[0].seen.get(msgId)).toBeUndefined();
					expect(streams[1].seen.get(msgId)).toBeUndefined();
					expect(streams[2].seen.get(msgId)).toBeUndefined();
				});

				it("will send through peer", async () => {
					await session.peers[0].hangUp(session.peers[1].peerId);

					// send a message with to=[2]
					// make sure message is received
					const msg = new DataMessage({
						data: new Uint8Array([0]),
						to: [streams[2].stream.publicKeyHash]
					});
					await msg.sign(streams[1].stream.sign);
					await streams[0].stream.publishMessage(session.peers[0].peerId, msg);
					await waitForResolved(() =>
						expect(streams[2].received).toHaveLength(1)
					);
				});
			});
			describe("1->2->2", () => {
				/** 
			┌─────┐ 
			│0    │ 
			└┬───┬┘ 
			┌▽─┐┌▽─┐
			│2 ││1 │
			└┬┬┘└┬┬┘
			 ││  ││ 
			 ││  ││ 
			 ││  └│┐
			 └│──┐││
			 ┌│──│┘│
			 ││  │┌┘
			┌▽▽┐┌▽▽┐
			│3 ││4 │
			└──┘└──┘
			*/

				let session: TestSessionStream;
				let streams: ReturnType<typeof createMetrics>[];
				const data = new Uint8Array([1, 2, 3]);

				beforeAll(async () => {});

				beforeEach(async () => {
					session = await disconnected(5, {
						services: {
							directstream: (c) =>
								new TestDirectStream(c, {
									connectionManager: { autoDial: false }
								})
						}
					});
					streams = [];
					for (const peer of session.peers) {
						streams.push(createMetrics(peer.services.directstream));
					}
					await session.connect([
						// behaviour seems to be more predictable if we connect after start (TODO improve startup to use existing connections in a better way)
						[session.peers[0], session.peers[1]],
						[session.peers[0], session.peers[2]],

						[session.peers[1], session.peers[3]],
						[session.peers[1], session.peers[4]],

						[session.peers[2], session.peers[3]],
						[session.peers[2], session.peers[4]]
					]);

					await waitForPeerStreams(streams[0].stream, streams[1].stream);
					await waitForPeerStreams(streams[0].stream, streams[2].stream);
					await waitForPeerStreams(streams[1].stream, streams[3].stream);
					await waitForPeerStreams(streams[1].stream, streams[4].stream);
					await waitForPeerStreams(streams[2].stream, streams[3].stream);
					await waitForPeerStreams(streams[2].stream, streams[4].stream);

					await waitForResolved(() =>
						expect(streams[0].stream.routes.nodeCount).toEqual(5)
					);
					await waitForResolved(() =>
						expect(streams[1].stream.routes.nodeCount).toEqual(5)
					);
					await waitForResolved(() =>
						expect(streams[2].stream.routes.nodeCount).toEqual(5)
					);
					await waitForResolved(() =>
						expect(streams[3].stream.routes.nodeCount).toEqual(5)
					);
					await waitForResolved(() =>
						expect(streams[4].stream.routes.nodeCount).toEqual(5)
					);
				});

				afterEach(async () => {
					await session.stop();
				});

				it("messages are only sent once to each peer", async () => {
					streams[0].stream.publish(data, {
						to: [
							streams[3].stream.publicKeyHash,
							streams[4].stream.publicKeyHash
						]
					});
					await waitForResolved(() =>
						expect(streams[3].received).toHaveLength(1)
					);
					await waitForResolved(() =>
						expect(streams[4].received).toHaveLength(1)
					);

					const id1 = await getMsgId(serialize(streams[3].received[0]));

					await delay(3000); // Wait some extra time if additional messages are propagating through

					expect(streams[3].seen.get(id1)).toEqual(1); // 1 delivery even though there are multiple path leading to this node
					expect(streams[4].seen.get(id1)).toEqual(1); // 1 delivery even though there are multiple path leading to this node
				});
			});
		});
	});

	// TODO test that messages are not sent backward, triangles etc

	describe("join/leave", () => {
		let session: TestSessionStream;
		let streams: ReturnType<typeof createMetrics>[];
		const data = new Uint8Array([1, 2, 3]);
		let autoDialRetryDelay = 5 * 1000;

		describe("direct connections", () => {
			beforeEach(async () => {
				session = await disconnected(
					4,
					new Array(4).fill(0).map((_x, i) => {
						return {
							services: {
								directstream: (c) =>
									new TestDirectStream(c, {
										connectionManager: {
											autoDial: i === 0, // allow client 0 to auto dial
											retryDelay: autoDialRetryDelay
										}
									})
							}
						};
					})
				); // Second arg is due to https://github.com/transport/js-libp2p/issues/1690
				streams = [];

				for (const [i, peer] of session.peers.entries()) {
					if (i === 0) {
						expect(
							peer.services.directstream["connectionManagerOptions"].autoDial
						).toBeTrue();
					} else {
						expect(
							peer.services.directstream["connectionManagerOptions"].autoDial
						).toBeFalse();
					}

					streams.push(createMetrics(peer.services.directstream));
				}

				// slowly connect to that the route maps are deterministic
				await session.connect([[session.peers[0], session.peers[1]]]);
				await waitFor(() => streams[0].stream.routes.linksCount === 1);
				await waitFor(() => streams[1].stream.routes.linksCount === 1);
				await session.connect([[session.peers[1], session.peers[2]]]);
				await waitFor(() => streams[0].stream.routes.linksCount === 2);
				await waitFor(() => streams[1].stream.routes.linksCount === 2);
				await session.connect([[session.peers[2], session.peers[3]]]);
				await waitFor(() => streams[0].stream.routes.linksCount === 3);
				await waitFor(() => streams[1].stream.routes.linksCount === 3);
				await waitFor(() => streams[2].stream.routes.linksCount === 3);
				await waitForPeerStreams(streams[0].stream, streams[1].stream);
				await waitForPeerStreams(streams[1].stream, streams[2].stream);
				await waitForPeerStreams(streams[2].stream, streams[3].stream);

				for (const peer of streams) {
					await waitFor(() => peer.reachable.length === 3);
					expect(peer.reachable.map((x) => x.hashcode())).toContainAllValues(
						streams
							.map((x) => x.stream.publicKeyHash)
							.filter((x) => x !== peer.stream.publicKeyHash)
					); // peer has recevied reachable event from everone
				}
			});

			afterEach(async () => {
				await session.stop();
			});

			it("directly if possible", async () => {
				let dials = 0;
				const dialFn =
					streams[0].stream.components.connectionManager.openConnection.bind(
						streams[0].stream.components.connectionManager
					);
				streams[0].stream.components.connectionManager.openConnection = (
					a,
					b
				) => {
					dials += 1;
					return dialFn(a, b);
				};

				streams[3].received = [];
				expect(streams[0].stream.peers.size).toEqual(1);

				await streams[0].stream.publish(data, {
					to: [streams[3].stream.components.peerId]
				});

				await waitFor(() => streams[3].received.length === 1);
				expect(
					streams[3].messages.find((x) => x instanceof DataMessage)
				).toBeDefined();

				// Dialing will yield a new connection
				try {
					await waitFor(() => streams[0].stream.peers.size === 2);
				} catch (error) {
					const q = 12;
					throw q;
				}
				expect(dials).toEqual(1);

				// Republishing will not result in an additional dial
				await streams[0].stream.publish(data, {
					to: [streams[3].stream.components.peerId]
				});
				await waitFor(() => streams[3].received.length === 2);
				expect(dials).toEqual(1);
				expect(streams[0].stream.peers.size).toEqual(2);
				expect(
					streams[0].stream.peers.has(streams[3].stream.publicKeyHash)
				).toBeTrue();
				expect(
					streams[0].stream.peers.has(streams[1].stream.publicKeyHash)
				).toBeTrue();
			});

			it("retry dial after a while", async () => {
				let dials: (PeerId | Multiaddr | Multiaddr[])[] = [];
				streams[0].stream.components.connectionManager.openConnection = (
					a,
					b
				) => {
					dials.push(a);
					throw new Error("Mock Error");
				};

				streams[3].received = [];
				expect(streams[0].stream.peers.size).toEqual(1);

				await streams[0].stream.publish(data, {
					to: [streams[3].stream.components.peerId]
				});

				await waitFor(() => streams[3].received.length === 1);
				expect(
					streams[3].messages.find((x) => x instanceof DataMessage)
				).toBeDefined();

				// Dialing will yield a new connection
				await waitFor(() => streams[0].stream.peers.size === 1);
				let expectedDialsCount = 1 + session.peers[2].getMultiaddrs().length; // 1 dial directly, X dials through neighbour as relay
				expect(dials).toHaveLength(expectedDialsCount);

				// Republishing will not result in an additional dial
				await streams[0].stream.publish(data, {
					to: [streams[3].stream.components.peerId]
				});
				let t1 = +new Date();
				expect(dials).toHaveLength(expectedDialsCount); // No change, because TTL > autoDialRetryTimeout

				await waitFor(() => streams[3].received.length === 2);
				await waitFor(() => +new Date() - t1 > autoDialRetryDelay);

				// Try again, now expect another dial call, since the retry interval has been reached
				await streams[0].stream.publish(data, {
					to: [streams[3].stream.components.peerId]
				});
				expect(dials).toHaveLength(expectedDialsCount * 2); // 1 dial directly, X dials through neighbour as relay
			});

			it("through relay if fails", async () => {
				const dialFn =
					streams[0].stream.components.connectionManager.openConnection.bind(
						streams[0].stream.components.connectionManager
					);

				let directlyDialded = false;
				const filteredDial = (address: PeerId | Multiaddr | Multiaddr[]) => {
					if (
						isPeerId(address) &&
						address.toString() === streams[3].stream.peerIdStr
					) {
						throw new Error("Mock fail"); // don't allow connect directly
					}

					let addresses: Multiaddr[] = Array.isArray(address)
						? address
						: [address as Multiaddr];
					for (const a of addresses) {
						if (
							!a.protoNames().includes("p2p-circuit") &&
							a.toString().includes(streams[3].stream.peerIdStr)
						) {
							throw new Error("Mock fail"); // don't allow connect directly
						}
					}
					addresses = addresses.map((x) =>
						x.protoNames().includes("p2p-circuit")
							? multiaddr(x.toString().replace("/webrtc/", "/"))
							: x
					); // TODO use webrtc in node

					directlyDialded = true;
					return dialFn(addresses);
				};

				streams[0].stream.components.connectionManager.openConnection =
					filteredDial;
				expect(streams[0].stream.peers.size).toEqual(1);
				await streams[0].stream.publish(data, {
					to: [streams[3].stream.components.peerId]
				});
				await waitFor(() => streams[3].received.length === 1);
				await waitForResolved(() => expect(directlyDialded).toBeTrue());
			});

			it("tries multiple relays", async () => {
				await session.connect([[session.peers[1], session.peers[3]]]);
				await waitForPeerStreams(streams[1].stream, streams[3].stream);

				/* 
				┌───┐ 
				│ 0 │ 
				└┬─┬┘ 
				 │┌▽┐ 
				 ││1│ 
				 │└┬┘ 
				┌▽┐│  
				│2││  
				└┬┘│  
				┌▽─▽─┐
				│ 3  │
				└────┘ 
				
				*/

				const dialedCircuitRelayAddresses: Set<string> = new Set();

				const dialFn =
					streams[0].stream.components.connectionManager.openConnection.bind(
						streams[0].stream.components.connectionManager
					);
				const filteredDial = (address: PeerId | Multiaddr | Multiaddr[]) => {
					if (
						isPeerId(address) &&
						address.toString() === streams[3].stream.peerIdStr
					) {
						throw new Error("Mock fail"); // don't allow connect directly
					}

					let addresses: Multiaddr[] = Array.isArray(address)
						? address
						: [address as Multiaddr];
					for (const a of addresses) {
						if (
							!a.protoNames().includes("p2p-circuit") &&
							a.toString().includes(streams[3].stream.peerIdStr)
						) {
							throw new Error("Mock fail"); // don't allow connect directly
						}
					}
					addresses
						.filter((x) => x.protoNames().includes("p2p-circuit"))
						.forEach((x) => {
							dialedCircuitRelayAddresses.add(x.toString());
						});
					addresses = addresses.map((x) =>
						x.protoNames().includes("p2p-circuit")
							? multiaddr(x.toString().replace("/webrtc/", "/"))
							: x
					); // TODO use webrtc in node

					if (dialedCircuitRelayAddresses.size === 1) {
						throw new Error("Mock fail"); // only succeed with the dial once we have tried two unique addresses (both neighbors)
					}
					return dialFn(addresses);
				};

				streams[0].stream.components.connectionManager.openConnection =
					filteredDial;

				expect(streams[0].stream.peers.size).toEqual(1);
				await streams[0].stream.publish(data, {
					to: [streams[3].stream.components.peerId]
				});
				await waitFor(() => streams[3].received.length === 1);
				expect(dialedCircuitRelayAddresses.size).toEqual(2);
			});
		});

		describe("4", () => {
			beforeEach(async () => {
				session = await disconnected(4, {
					services: {
						directstream: (c) =>
							new TestDirectStream(c, {
								connectionManager: { autoDial: false }
							})
					}
				});

				/* 
				┌─┐
				│3│
				└┬┘
				┌▽┐
				│0│
				└┬┘
				┌▽┐
				│1│
				└┬┘
				┌▽┐
				│2│
				└─┘
				
				 */

				streams = [];
				for (const peer of session.peers) {
					streams.push(createMetrics(peer.services.directstream));
				}

				// slowly connect to that the route maps are deterministic
				await session.connect([[session.peers[0], session.peers[1]]]);
				await waitFor(() => streams[0].stream.routes.linksCount === 1);
				await waitFor(() => streams[1].stream.routes.linksCount === 1);
				await session.connect([[session.peers[1], session.peers[2]]]);
				await waitFor(() => streams[0].stream.routes.linksCount === 2);
				await waitFor(() => streams[1].stream.routes.linksCount === 2);
				await waitFor(() => streams[2].stream.routes.linksCount === 2);
				await session.connect([[session.peers[0], session.peers[3]]]);
				await waitFor(() => streams[0].stream.routes.linksCount === 3);
				await waitFor(() => streams[1].stream.routes.linksCount === 3);
				await waitFor(() => streams[2].stream.routes.linksCount === 3);
				await waitFor(() => streams[3].stream.routes.linksCount === 3);
				await waitForPeerStreams(streams[0].stream, streams[1].stream);
				await waitForPeerStreams(streams[1].stream, streams[2].stream);
				await waitForPeerStreams(streams[0].stream, streams[3].stream);

				expect([...streams[0].stream.peers.keys()]).toEqual([
					streams[1].stream.publicKeyHash,
					streams[3].stream.publicKeyHash
				]);
				expect([...streams[1].stream.peers.keys()]).toEqual([
					streams[0].stream.publicKeyHash,
					streams[2].stream.publicKeyHash
				]);
				expect([...streams[2].stream.peers.keys()]).toEqual([
					streams[1].stream.publicKeyHash
				]);
				expect([...streams[3].stream.peers.keys()]).toEqual([
					streams[0].stream.publicKeyHash
				]);

				for (const peer of streams) {
					await waitFor(() => peer.reachable.length === 3);
					expect(peer.reachable.map((x) => x.hashcode())).toContainAllValues(
						streams
							.map((x) => x.stream.publicKeyHash)
							.filter((x) => x !== peer.stream.publicKeyHash)
					); // peer has recevied reachable event from everone
				}

				for (const peer of streams) {
					expect(peer.unrechable).toHaveLength(0); // No unreachable events before stopping
				}
			});

			afterEach(async () => {
				await session.stop();
			});

			it("will emit unreachable events on shutdown", async () => {
				/** Shut down slowly and check that all unreachable events are fired */

				let reachableBeforeStop = streams[2].reachable.length;
				await session.peers[0].stop();
				const hasAll = (arr: PublicSignKey[], cmp: PublicSignKey[]) => {
					let a = new Set(arr.map((x) => x.hashcode()));
					let b = new Set(cmp.map((x) => x.hashcode()));
					if (a.size === b.size) {
						for (const key of cmp) {
							if (!arr.find((x) => x.equals(key))) {
								return false;
							}
						}
						return true;
					}
					return false;
				};

				expect(reachableBeforeStop).toEqual(streams[1].reachable.length);
				expect(reachableBeforeStop).toEqual(streams[2].reachable.length);
				expect(reachableBeforeStop).toEqual(streams[0].reachable.length);

				expect(streams[0].unrechable).toHaveLength(0);
				await waitFor(() =>
					hasAll(streams[1].unrechable, [
						streams[0].stream.publicKey,
						streams[3].stream.publicKey
					])
				);

				await session.peers[1].stop();
				await waitFor(() =>
					hasAll(streams[2].unrechable, [
						streams[0].stream.publicKey,
						streams[1].stream.publicKey,
						streams[3].stream.publicKey
					])
				);

				await session.peers[2].stop();
				await waitFor(() =>
					hasAll(streams[3].unrechable, [
						streams[0].stream.publicKey,
						streams[1].stream.publicKey,
						streams[2].stream.publicKey
					])
				);
				await session.peers[3].stop();
			});

			it("will publish on routes", async () => {
				streams[2].received = [];
				streams[3].received = [];

				await streams[0].stream.publish(data, {
					to: [streams[2].stream.components.peerId]
				});
				await waitFor(() => streams[2].received.length === 1);
				expect(
					streams[2].messages.find((x) => x instanceof DataMessage)
				).toBeDefined();

				await delay(1000); // some delay to allow all messages to progagate
				expect(streams[3].received).toHaveLength(0);
				expect(
					streams[3].messages.find((x) => x instanceof DataMessage)
				).toBeUndefined();
			});

			it("re-route new connection", async () => {
				/* 					
				┌───┐ 
				│3  │ 
				└┬─┬┘ 
				│┌▽┐ 
				││0│ 
				│└┬┘ 
				│┌▽─┐
				││1 │
				│└┬─┘
				┌▽─▽┐ 
				│2  │ 
				└───┘ 
				 */

				expect(
					streams[3].stream.routes.getPath(
						streams[3].stream.publicKeyHash,
						streams[2].stream.publicKeyHash
					)
				).toHaveLength(4);
				await session.connect([[session.peers[2], session.peers[3]]]);
				await waitFor(
					() =>
						streams[3].stream.routes.getPath(
							streams[3].stream.publicKeyHash,
							streams[2].stream.publicKeyHash
						).length === 2
				);
			});

			it("handle on drop no routes", async () => {
				expect(
					streams[3].stream.routes.getPath(
						streams[3].stream.publicKeyHash,
						streams[2].stream.publicKeyHash
					)
				).toHaveLength(4);
				expect(streams[1].stream.earlyGoodbyes.size).toEqual(2);
				expect(streams[3].stream.earlyGoodbyes.size).toEqual(1);

				await session.peers[0].stop();
				await waitFor(() => streams[3].stream.routes.linksCount === 0); // because 1, 2 are now disconnected
				await delay(1000); // make sure nothing get readded
				expect(streams[3].stream.routes.linksCount).toEqual(0);
				expect(
					streams[3].stream.routes.getPath(
						streams[3].stream.publicKeyHash,
						streams[2].stream.publicKeyHash
					)
				).toHaveLength(0);
				expect(streams[3].stream.earlyGoodbyes.size).toEqual(0);
			});
		});

		describe("6", () => {
			/* 
			┌─┐
			│0│
			└△┘
			┌▽┐
			│1│
			└△┘
			┌▽┐
			│2│
			└─┘
		
			< 2 connects with 3 >
		
			┌─┐
			│3│
			└△┘
			┌▽┐
			│4│
			└△┘
			┌▽┐
			│5│
			└─┘ 
			*/

			beforeEach(async () => {
				session = await disconnected(6, {
					services: {
						directstream: (c) =>
							new TestDirectStream(c, {
								connectionManager: { autoDial: false }
							})
					}
				});
				await session.connect([
					[session.peers[0], session.peers[1]],
					[session.peers[1], session.peers[2]],
					[session.peers[3], session.peers[4]],
					[session.peers[4], session.peers[5]]
				]);

				streams = [];
				for (const [i, peer] of session.peers.entries()) {
					streams.push(createMetrics(peer.services.directstream));
				}

				for (const peer of streams.values()) {
					await waitFor(() => peer.stream.routes.linksCount === 2);
				}

				for (let i = 0; i < 2; i++) {
					await waitForPeerStreams(streams[i].stream, streams[i + 1].stream);
				}
				for (let i = 3; i < 5; i++) {
					await waitForPeerStreams(streams[i].stream, streams[i + 1].stream);
				}
			});

			afterAll(async () => {
				await session.stop();
			});
			it("will replay on connect", async () => {
				for (let i = 3; i < 5; i++) {
					await waitForPeerStreams(streams[i].stream, streams[i + 1].stream);
				}
				expect(streams[2].stream.helloMap.size).toEqual(2); // these hellos will be forwarded on connect
				expect(streams[3].stream.helloMap.size).toEqual(2); // these hellos will be forwarded on connect
				await session.connect([[session.peers[2], session.peers[3]]]);

				for (const peer of streams) {
					await waitFor(() => peer.stream.routes.linksCount === 5); // everyone knows everone
				}
			});
		});

		describe("invalidation", () => {
			let extraSession: TestSessionStream;
			beforeEach(async () => {
				session = await connected(3);

				for (let i = 0; i < session.peers.length; i++) {
					await waitForResolved(() =>
						expect(
							session.peers[i].services.directstream.routes.nodeCount
						).toEqual(3)
					);
				}
			});
			afterEach(async () => {
				await session?.stop();
				await extraSession?.stop();
			});
			it("old hellos are purged", async () => {
				session.peers[1].stop();
				extraSession = await disconnected(1);
				await extraSession.peers[0].dial(session.peers[2].getMultiaddrs());
				await waitForResolved(() =>
					expect(
						extraSession.peers[0].services.directstream.routes.nodeCount
					).toEqual(3)
				);
				await delay(3000);
				expect(
					extraSession.peers[0].services.directstream.routes.nodeCount
				).toEqual(3);
			});

			it("will not get blocked for slow writes", async () => {
				let slowPeer = [1, 2];
				let fastPeer = [2, 1];
				let directDelivery = [true, false];
				for (let i = 0; i < slowPeer.length; i++) {
					const slow = session.peers[0].services.directstream.peers.get(
						session.peers[slowPeer[i]].services.directstream.publicKeyHash
					)!;
					const fast = session.peers[0].services.directstream.peers.get(
						session.peers[fastPeer[i]].services.directstream.publicKeyHash
					)!;

					expect(slow).toBeDefined();
					const waitForWriteDefaultFn = slow.waitForWrite.bind(slow);
					slow.waitForWrite = async (bytes) => {
						await delay(3000);
						return waitForWriteDefaultFn(bytes);
					};

					const t0 = +new Date();
					let t1: number | undefined = undefined;
					await session.peers[0].services.directstream.publish(
						new Uint8Array([1, 2, 3]),
						{
							to: directDelivery[i]
								? [slow.publicKey, fast.publicKey]
								: undefined
						}
					);

					let listener = () => {
						t1 = +new Date();
					};
					session.peers[fastPeer[i]].services.directstream.addEventListener(
						"data",
						listener
					);
					await waitForResolved(() => expect(t1).toBeDefined());

					expect(t1! - t0).toBeLessThan(3000);

					// reset
					slow.waitForWrite = waitForWriteDefaultFn;
					session.peers[fastPeer[i]].services.directstream.removeEventListener(
						"data",
						listener
					);
				}
			});
		});
	});

	describe("start/stop", () => {
		let session: TestSessionStream;

		afterEach(async () => {
			await session.stop();
		});

		it("can restart", async () => {
			session = await connected(2, {
				transports: [tcp(), webSockets({ filter: filters.all })],
				services: {
					directstream: (c) =>
						new TestDirectStream(c, {
							connectionManager: { autoDial: false }
						})
				}
			}); // use 2 transports as this might cause issues if code is not handling multiple connections correctly

			await waitFor(() => stream(session, 1).helloMap.size == 1);
			await stream(session, 0).stop();
			await waitFor(() => stream(session, 1).helloMap.size === 0);

			await stream(session, 1).stop();
			expect(stream(session, 0).peers.size).toEqual(0);
			await delay(3000);
			await stream(session, 0).start();
			expect(stream(session, 0).helloMap.size).toEqual(0);
			await stream(session, 1).start();

			await waitFor(() => stream(session, 0).peers.size === 1);
			await waitFor(() => stream(session, 0).helloMap.size === 1);
			await waitFor(() => stream(session, 1).helloMap.size === 1);
			await waitForPeerStreams(stream(session, 0), stream(session, 1));
		});
		it("can connect after start", async () => {
			session = await disconnected(2, {
				transports: [tcp(), webSockets({ filter: filters.all })],
				services: {
					directstream: (c) => new TestDirectStream(c)
				}
			});

			await session.connect();
			await waitForPeerStreams(stream(session, 0), stream(session, 1));
		});

		it("can connect before start", async () => {
			session = await connected(2, {
				transports: [tcp(), webSockets({ filter: filters.all })],
				services: {
					directstream: (c) => new TestDirectStream(c)
				}
			});
			await delay(3000);

			await stream(session, 0).start();
			await stream(session, 1).start();

			await waitForPeerStreams(stream(session, 0), stream(session, 1));
		});

		it("can connect with delay", async () => {
			session = await connected(2, {
				transports: [tcp(), webSockets({ filter: filters.all })],
				services: {
					directstream: (c) => new TestDirectStream(c)
				}
			});
			await waitForPeerStreams(stream(session, 0), stream(session, 1));
			await session.peers[0].services.directstream.stop();
			await session.peers[1].services.directstream.stop();
			await waitFor(
				() => session.peers[0].services.directstream.peers.size === 0
			);
			await waitFor(
				() => session.peers[1].services.directstream.peers.size === 0
			);
			await session.peers[1].services.directstream.start();
			await delay(3000);
			await session.peers[0].services.directstream.start();
			await waitForPeerStreams(stream(session, 0), stream(session, 1));
		});
	});

	describe("multistream", () => {
		let session: TestSessionStream;
		beforeEach(async () => {
			session = await TestSession.connected(2, {
				transports: [tcp(), webSockets({ filter: filters.all })],
				services: {
					directstream: (c) => new TestDirectStream(c),
					directstream2: (c) =>
						new TestDirectStream(c, { id: "another-protocol" })
				}
			}); // use 2 transports as this might cause issues if code is not handling multiple connections correctly
		});

		afterEach(async () => {
			await session.stop();
		});

		it("can setup multiple streams at once", async () => {
			await waitFor(() => !!stream(session, 0).peers.size);
			await waitFor(() => !!stream(session, 1).peers.size);
			await waitFor(() => !!service(session, 0, "directstream2").peers.size);
			await waitFor(() => !!service(session, 1, "directstream2").peers.size);
		});
	});
});
