import { LSession, LibP2POptions } from "@dao-xyz/libp2p-test-utils";
import { waitFor, delay } from "@dao-xyz/peerbit-time";
import crypto from "crypto";
import {
	waitForPeers as waitForPeerStreams,
	DirectStream as X,
	ConnectionManagerOptions,
	DirectStreamComponents,
} from "..";
import { DataMessage, Message } from "../messages";
import { PublicSignKey } from "@dao-xyz/peerbit-crypto";
import { PeerId, isPeerId } from "@libp2p/interface-peer-id";
import { Multiaddr } from "@multiformats/multiaddr";
import { multiaddr } from "@multiformats/multiaddr";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import * as filters from "@libp2p/websockets/filters";

class TX extends X {
	constructor(
		components: DirectStreamComponents,
		options: {
			id?: string;
			pingInterval?: number;
			connectionManager?: ConnectionManagerOptions;
		} = {}
	) {
		super(components, [options.id || "test/0.0.0"], {
			canRelayMessage: true,
			emitSelf: true,
			connectionManager: options.connectionManager || {
				autoDial: false,
			},
			...options,
		});
	}
}
type TestSession = LSession<{ directstream: X }>;
const connected = async (
	n: number,
	options?:
		| LibP2POptions<{ directstream: TX }>
		| LibP2POptions<{ directstream: TX }>[]
) => {
	let session: TestSession = await LSession.connected(
		n,
		options || {
			services: { directstream: (components) => new TX(components, options) },
		}
	);
	return session;
};

const disconnected = async (
	n: number,
	options?:
		| LibP2POptions<{ directstream: TX }>
		| LibP2POptions<{ directstream: TX }>[]
) => {
	let session: TestSession = await LSession.disconnected(
		n,
		options || {
			services: { directstream: (components) => new TX(components, options) },
		}
	);
	return session;
};

const stream = (s: TestSession, i: number): TX =>
	service(s, i, "directstream") as TX;
const service = (s: TestSession, i: number, service: string) =>
	s.peers[i].services[service];
const waitForPeers = (s: TestSession) =>
	waitForPeerStreams(...s.peers.map((x) => x.services.directstream));

describe("streams", function () {
	describe("ping", () => {
		let session: TestSession;

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

		it("ping interval", async () => {
			// 0 and 2 not connected
			session = await connected(2, {
				services: { directstream: (c) => new TX(c, { pingInterval: 1000 }) },
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
		let session: TestSession;
		let peers: {
			stream: TX;
			messages: Message[];
			recieved: DataMessage[];
			reachable: PublicSignKey[];
			unrechable: PublicSignKey[];
		}[];
		const data = new Uint8Array([1, 2, 3]);

		beforeAll(async () => {});

		beforeEach(async () => {
			// 0 and 2 not connected
			session = await disconnected(4);

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

			peers = [];
			for (const peer of session.peers) {
				const client: {
					stream: TX;
					messages: Message[];
					recieved: DataMessage[];
					reachable: PublicSignKey[];
					unrechable: PublicSignKey[];
				} = {
					messages: [],
					recieved: [],
					reachable: [],
					unrechable: [],
					stream: peer.services.directstream,
				};
				peers.push(client);
				client.stream.addEventListener("message", (msg) => {
					client.messages.push(msg.detail);
				});
				client.stream.addEventListener("data", (msg) => {
					client.recieved.push(msg.detail);
				});
				client.stream.addEventListener("peer:reachable", (msg) => {
					client.reachable.push(msg.detail);
				});
				client.stream.addEventListener("peer:unreachable", (msg) => {
					client.unrechable.push(msg.detail);
				});
			}
			await session.connect([
				// behaviour seems to be more predictable if we connect after start (TODO improve startup to use existing connections in a better way)
				[session.peers[0], session.peers[1]],
				[session.peers[1], session.peers[2]],
				[session.peers[2], session.peers[3]],
			]);

			await waitForPeerStreams(peers[0].stream, peers[1].stream);
			await waitForPeerStreams(peers[1].stream, peers[2].stream);
			await waitForPeerStreams(peers[2].stream, peers[3].stream);
		});

		afterEach(async () => {
			await session.stop();
		});

		afterAll(async () => {});

		it("many", async () => {
			let iterations = 300;

			for (let i = 0; i < iterations; i++) {
				const small = crypto.randomBytes(1e3); // 1kb
				peers[0].stream.publish(small);
			}
			await waitFor(() => peers[2].recieved.length === iterations, {
				delayInterval: 300,
				timeout: 30 * 1000,
			});
		});

		it("1->unknown", async () => {
			await peers[0].stream.publish(data);
			await waitFor(() => peers[1].recieved.length === 1);
			expect(new Uint8Array(peers[1].recieved[0].data)).toEqual(data);
			await waitFor(() => peers[2].recieved.length === 1);
			expect(new Uint8Array(peers[2].recieved[0].data)).toEqual(data);
			await delay(1000); // wait some more time to make sure we dont get more messages
			expect(peers[1].recieved).toHaveLength(1);
			expect(peers[2].recieved).toHaveLength(1);
		});

		it("1->2", async () => {
			await peers[0].stream.publish(data, {
				to: [peers[1].stream.components.peerId],
			});
			await waitFor(() => peers[1].recieved.length === 1);
			expect(new Uint8Array(peers[1].recieved[0].data)).toEqual(data);
			await delay(1000); // wait some more time to make sure we dont get more messages
			expect(peers[1].recieved).toHaveLength(1);
			expect(peers[2].recieved).toHaveLength(0);
		});

		it("1->3", async () => {
			await peers[0].stream.publish(data, {
				to: [peers[2].stream.components.peerId],
			});
			await waitFor(() => peers[2].recieved.length === 1);
			expect(new Uint8Array(peers[2].recieved[0].data)).toEqual(data);
			await delay(1000); // wait some more time to make sure we dont get more messages
			expect(peers[2].recieved).toHaveLength(1);
			expect(peers[1].recieved).toHaveLength(0);
		});

		it("1->3 10mb data", async () => {
			const bigData = crypto.randomBytes(1e7);
			await peers[0].stream.publish(bigData, {
				to: [peers[2].stream.components.peerId],
			});
			await waitFor(() => peers[2].recieved.length === 1, {
				delayInterval: 10,
				timeout: 10 * 1000,
			});
			expect(new Uint8Array(peers[2].recieved[0].data)).toHaveLength(
				bigData.length
			);
			expect(peers[2].recieved).toHaveLength(1);
			expect(peers[1].recieved).toHaveLength(0);
		});
		it("publishes on direct stream, even path is longer", async () => {
			await session.connect([[session.peers[0], session.peers[2]]]);
			await waitForPeerStreams(peers[0].stream, peers[2].stream);

			// make path 1->3 longest, to make sure we send over it directly anyways because it is a direct path
			peers[0].stream.routes.graph.setEdgeAttribute(
				peers[0].stream.routes.getLink(
					peers[0].stream.publicKeyHash,
					peers[2].stream.publicKeyHash
				),
				"weight",
				1e5
			);
			await peers[0].stream.publish(crypto.randomBytes(1e2), {
				to: [peers[2].stream.components.peerId],
			});
			peers[1].messages = [];
			await waitFor(() => peers[2].recieved.length === 1, {
				delayInterval: 10,
				timeout: 10 * 1000,
			});
			expect(
				peers[1].messages.filter((x) => x instanceof DataMessage)
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

			await waitForPeerStreams(peers[0].stream, peers[2].stream);
			const defaultEdgeWeightFnPeer0 =
				peers[0].stream.routes.graph.getEdgeAttribute.bind(
					peers[0].stream.routes.graph
				);

			// make path long
			peers[0].stream.routes.graph.getEdgeAttribute = (
				edge: unknown,
				name: any
			) => {
				if (edge === link) {
					return 1e5;
				}
				return defaultEdgeWeightFnPeer0(edge, name);
			};

			await peers[0].stream.publish(crypto.randomBytes(1e2), {
				to: [peers[3].stream.components.peerId],
			});

			peers[1].messages = [];

			await waitFor(
				() =>
					peers[1].messages.filter((x) => x instanceof DataMessage).length === 1
			); // will send through peer [1] since path [0] -> [2] -> [3] directly is currently longer
			await waitFor(() => peers[3].recieved.length === 1);

			peers[1].messages = [];

			// Make [0] -> [2] path short
			let link = peers[0].stream.routes.getLink(
				peers[0].stream.publicKeyHash,
				peers[2].stream.publicKeyHash
			);
			peers[0].stream.routes.graph.getEdgeAttribute = (
				edge: unknown,
				name: any
			) => {
				if (edge === link) {
					return 0;
				}
				return defaultEdgeWeightFnPeer0(edge, name);
			};

			expect(
				peers[0].stream.routes.getPath(
					peers[0].stream.publicKeyHash,
					peers[2].stream.publicKeyHash
				).length
			).toEqual(2);
			await peers[0].stream.publish(crypto.randomBytes(1e2), {
				to: [peers[3].stream.components.peerId],
			});
			await waitFor(() => peers[3].recieved.length === 1);
			const messages = peers[1].messages.filter(
				(x) => x instanceof DataMessage
			);
			expect(messages).toHaveLength(0); // no new messages for peer 1, because sending 0 -> 2 -> 3 directly is now faster
			expect(peers[1].recieved).toHaveLength(0);
		});
	});

	// TODO test that messages are not sent backward, triangles etc

	describe("join/leave", () => {
		let session: TestSession;
		let peers: {
			stream: TX;
			messages: Message[];
			recieved: DataMessage[];
			reachable: PublicSignKey[];
			unrechable: PublicSignKey[];
		}[];
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
									new TX(c, {
										connectionManager: {
											autoDial: i === 0, // allow client 0 to auto dial
											retryDelay: autoDialRetryDelay,
										},
									}),
							},
						};
					})
				); // Second arg is due to https://github.com/libp2p/js-libp2p/issues/1690
				peers = [];

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

					const client: {
						stream: TX;
						messages: Message[];
						recieved: DataMessage[];
						reachable: PublicSignKey[];
						unrechable: PublicSignKey[];
					} = {
						messages: [],
						recieved: [],
						reachable: [],
						unrechable: [],
						stream: peer.services.directstream,
					};
					peers.push(client);
					peer.services.directstream.addEventListener("message", (msg) => {
						client.messages.push(msg.detail);
					});
					peer.services.directstream.addEventListener("data", (msg) => {
						client.recieved.push(msg.detail);
					});
					peer.services.directstream.addEventListener(
						"peer:reachable",
						(msg) => {
							client.reachable.push(msg.detail);
						}
					);
					peer.services.directstream.addEventListener(
						"peer:unreachable",
						(msg) => {
							client.unrechable.push(msg.detail);
						}
					);
				}

				// slowly connect to that the route maps are deterministic
				await session.connect([[session.peers[0], session.peers[1]]]);
				await waitFor(() => peers[0].stream.routes.linksCount === 1);
				await waitFor(() => peers[1].stream.routes.linksCount === 1);
				await session.connect([[session.peers[1], session.peers[2]]]);
				await waitFor(() => peers[0].stream.routes.linksCount === 2);
				await waitFor(() => peers[1].stream.routes.linksCount === 2);
				await session.connect([[session.peers[2], session.peers[3]]]);
				await waitFor(() => peers[0].stream.routes.linksCount === 3);
				await waitFor(() => peers[1].stream.routes.linksCount === 3);
				await waitFor(() => peers[2].stream.routes.linksCount === 3);
				await waitForPeerStreams(peers[0].stream, peers[1].stream);
				await waitForPeerStreams(peers[1].stream, peers[2].stream);
				await waitForPeerStreams(peers[2].stream, peers[3].stream);

				for (const peer of peers) {
					await waitFor(() => peer.reachable.length === 3);
					expect(peer.reachable.map((x) => x.hashcode())).toContainAllValues(
						peers
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
					peers[0].stream.components.connectionManager.openConnection.bind(
						peers[0].stream.components.connectionManager
					);
				peers[0].stream.components.connectionManager.openConnection = (
					a,
					b
				) => {
					dials += 1;
					return dialFn(a, b);
				};

				peers[3].recieved = [];
				expect(peers[0].stream.peers.size).toEqual(1);

				await peers[0].stream.publish(data, {
					to: [peers[3].stream.components.peerId],
				});

				await waitFor(() => peers[3].recieved.length === 1);
				expect(
					peers[3].messages.find((x) => x instanceof DataMessage)
				).toBeDefined();

				// Dialing will yield a new connection
				try {
					await waitFor(() => peers[0].stream.peers.size === 2);
				} catch (error) {
					const q = 12;
					throw q;
				}
				expect(dials).toEqual(1);

				// Republishing will not result in an additional dial
				await peers[0].stream.publish(data, {
					to: [peers[3].stream.components.peerId],
				});
				await waitFor(() => peers[3].recieved.length === 2);
				expect(dials).toEqual(1);
				expect(peers[0].stream.peers.size).toEqual(2);
				expect(
					peers[0].stream.peers.has(peers[3].stream.publicKeyHash)
				).toBeTrue();
				expect(
					peers[0].stream.peers.has(peers[1].stream.publicKeyHash)
				).toBeTrue();
			});

			it("retry dial after a while", async () => {
				let dials: (PeerId | Multiaddr | Multiaddr[])[] = [];
				peers[0].stream.components.connectionManager.openConnection = (
					a,
					b
				) => {
					dials.push(a);
					throw new Error("Mock Error");
				};

				peers[3].recieved = [];
				expect(peers[0].stream.peers.size).toEqual(1);

				await peers[0].stream.publish(data, {
					to: [peers[3].stream.components.peerId],
				});

				await waitFor(() => peers[3].recieved.length === 1);
				expect(
					peers[3].messages.find((x) => x instanceof DataMessage)
				).toBeDefined();

				// Dialing will yield a new connection
				await waitFor(() => peers[0].stream.peers.size === 1);
				let expectedDialsCount = 1 + session.peers[2].getMultiaddrs().length; // 1 dial directly, X dials through neighbour as relay
				expect(dials).toHaveLength(expectedDialsCount);

				// Republishing will not result in an additional dial
				await peers[0].stream.publish(data, {
					to: [peers[3].stream.components.peerId],
				});
				let t1 = +new Date();
				expect(dials).toHaveLength(expectedDialsCount); // No change, because TTL > autoDialRetryTimeout

				await waitFor(() => peers[3].recieved.length === 2);
				await waitFor(() => +new Date() - t1 > autoDialRetryDelay);

				// Try again, now expect another dial call, since the retry interval has been reached
				await peers[0].stream.publish(data, {
					to: [peers[3].stream.components.peerId],
				});
				expect(dials).toHaveLength(expectedDialsCount * 2); // 1 dial directly, X dials through neighbour as relay
			});

			it("through relay if fails", async () => {
				const dialFn =
					peers[0].stream.components.connectionManager.openConnection.bind(
						peers[0].stream.components.connectionManager
					);
				const filteredDial = (address: PeerId | Multiaddr | Multiaddr[]) => {
					if (
						isPeerId(address) &&
						address.toString() === peers[3].stream.peerIdStr
					) {
						throw new Error("Mock fail"); // don't allow connect directly
					}

					let addresses: Multiaddr[] = Array.isArray(address)
						? address
						: [address as Multiaddr];
					for (const a of addresses) {
						if (
							!a.protoNames().includes("p2p-circuit") &&
							a.toString().includes(peers[3].stream.peerIdStr)
						) {
							throw new Error("Mock fail"); // don't allow connect directly
						}
					}
					const q = 123;
					addresses = addresses.map((x) =>
						x.protoCodes().includes(281)
							? multiaddr(x.toString().replace("/webrtc/", "/"))
							: x
					); // TODO use webrtc in node
					return dialFn(addresses);
				};

				peers[0].stream.components.connectionManager.openConnection =
					filteredDial;
				expect(peers[0].stream.peers.size).toEqual(1);
				await peers[0].stream.publish(data, {
					to: [peers[3].stream.components.peerId],
				});
				await waitFor(() => peers[3].recieved.length === 1);
			});
		});

		describe("4", () => {
			beforeEach(async () => {
				session = await disconnected(4, {
					services: {
						directstream: (c) =>
							new TX(c, { connectionManager: { autoDial: false } }),
					},
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

				peers = [];
				for (const peer of session.peers) {
					const client: {
						stream: TX;
						messages: Message[];
						recieved: DataMessage[];
						reachable: PublicSignKey[];
						unrechable: PublicSignKey[];
					} = {
						messages: [],
						recieved: [],
						reachable: [],
						unrechable: [],
						stream: peer.services.directstream,
					};
					peers.push(client);

					peer.services.directstream.addEventListener("message", (msg) => {
						client.messages.push(msg.detail);
					});
					peer.services.directstream.addEventListener("data", (msg) => {
						client.recieved.push(msg.detail);
					});
					peer.services.directstream.addEventListener(
						"peer:reachable",
						(msg) => {
							client.reachable.push(msg.detail);
						}
					);
					peer.services.directstream.addEventListener(
						"peer:unreachable",
						(msg) => {
							client.unrechable.push(msg.detail);
						}
					);
				}

				// slowly connect to that the route maps are deterministic
				try {
					await session.connect([[session.peers[0], session.peers[1]]]);
					await waitFor(() => peers[0].stream.routes.linksCount === 1);
					await waitFor(() => peers[1].stream.routes.linksCount === 1);
					await session.connect([[session.peers[1], session.peers[2]]]);
					await waitFor(() => peers[0].stream.routes.linksCount === 2);
					await waitFor(() => peers[1].stream.routes.linksCount === 2);
					await waitFor(() => peers[2].stream.routes.linksCount === 2);
					await session.connect([[session.peers[0], session.peers[3]]]);
					await waitFor(() => peers[0].stream.routes.linksCount === 3);
					await waitFor(() => peers[1].stream.routes.linksCount === 3);
					await waitFor(() => peers[2].stream.routes.linksCount === 3);
					await waitFor(() => peers[3].stream.routes.linksCount === 3);
					await waitForPeerStreams(peers[0].stream, peers[1].stream);
					await waitForPeerStreams(peers[1].stream, peers[2].stream);
					await waitForPeerStreams(peers[0].stream, peers[3].stream);
				} catch (error) {
					console.log(
						[peers.map((x) => x.stream.peerIdStr)],
						[...peers[0].stream.multiaddrsMap.values()],
						peers[0].stream.routes.linksCount,
						peers[1].stream.routes.linksCount,
						peers[2].stream.routes.linksCount,
						peers[3].stream.routes.linksCount
					);
					console.log([...peers[0].stream.multiaddrsMap.values()]);
					throw error;
				}
				for (const peer of peers) {
					await waitFor(() => peer.reachable.length === 3);
					expect(peer.reachable.map((x) => x.hashcode())).toContainAllValues(
						peers
							.map((x) => x.stream.publicKeyHash)
							.filter((x) => x !== peer.stream.publicKeyHash)
					); // peer has recevied reachable event from everone
				}

				for (const peer of peers) {
					expect(peer.unrechable).toHaveLength(0); // No unreachable events before stopping
				}
			});

			afterEach(async () => {
				await Promise.all(peers.map((peer) => peer.stream.stop()));
				await session.stop();
			});

			it("will emit unreachable events on shutdown", async () => {
				/** Shut down slowly and check that all unreachable events are fired */
				let reachableBeforeStop = peers[2].reachable.length;
				await peers[0].stream.stop();
				const hasAll = (arr: PublicSignKey[], cmp: PublicSignKey[]) => {
					let a = new Set(arr.map((x) => x.hashcode()));
					let b = new Set(cmp.map((x) => x.hashcode()));
					if (
						a.size === b.size &&
						a.size === arr.length &&
						arr.length === cmp.length
					) {
						for (const key of cmp) {
							if (!arr.find((x) => x.equals(key))) {
								return false;
							}
						}
						return true;
					}
					return false;
				};

				expect(reachableBeforeStop).toEqual(peers[1].reachable.length);
				expect(reachableBeforeStop).toEqual(peers[2].reachable.length);
				expect(reachableBeforeStop).toEqual(peers[0].reachable.length);

				expect(peers[0].unrechable).toHaveLength(0);
				await waitFor(() =>
					hasAll(peers[1].unrechable, [
						peers[0].stream.publicKey,
						peers[3].stream.publicKey,
					])
				);
				await peers[1].stream.stop();
				await waitFor(() =>
					hasAll(peers[2].unrechable, [
						peers[0].stream.publicKey,
						peers[1].stream.publicKey,
						peers[3].stream.publicKey,
					])
				);

				await peers[2].stream.stop();
				await waitFor(() =>
					hasAll(peers[3].unrechable, [
						peers[0].stream.publicKey,
						peers[1].stream.publicKey,
						peers[2].stream.publicKey,
					])
				);
				await peers[3].stream.stop();
			});

			it("will publish on routes", async () => {
				peers[2].recieved = [];
				peers[3].recieved = [];

				await peers[0].stream.publish(data, {
					to: [peers[2].stream.components.peerId],
				});
				await waitFor(() => peers[2].recieved.length === 1);
				expect(
					peers[2].messages.find((x) => x instanceof DataMessage)
				).toBeDefined();

				await delay(1000); // some delay to allow all messages to progagate
				expect(peers[3].recieved).toHaveLength(0);
				expect(
					peers[3].messages.find((x) => x instanceof DataMessage)
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
					peers[3].stream.routes.getPath(
						peers[3].stream.publicKeyHash,
						peers[2].stream.publicKeyHash
					)
				).toHaveLength(4);
				await session.connect([[session.peers[2], session.peers[3]]]);
				await waitFor(
					() =>
						peers[3].stream.routes.getPath(
							peers[3].stream.publicKeyHash,
							peers[2].stream.publicKeyHash
						).length === 2
				);
			});

			it("handle on drop no routes", async () => {
				expect(
					peers[3].stream.routes.getPath(
						peers[3].stream.publicKeyHash,
						peers[2].stream.publicKeyHash
					)
				).toHaveLength(4);
				expect(peers[1].stream.earlyGoodbyes.size).toEqual(2);
				expect(peers[3].stream.earlyGoodbyes.size).toEqual(1);

				await peers[0].stream.stop();
				await waitFor(() => peers[3].stream.routes.linksCount === 0); // because 1, 2 are now disconnected
				await delay(1000); // make sure nothing get readded
				expect(peers[3].stream.routes.linksCount).toEqual(0);
				expect(
					peers[3].stream.routes.getPath(
						peers[3].stream.publicKeyHash,
						peers[2].stream.publicKeyHash
					)
				).toHaveLength(0);
				expect(peers[3].stream.earlyGoodbyes.size).toEqual(0);
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
							new TX(c, { connectionManager: { autoDial: false } }),
					},
				});
				await session.connect([
					[session.peers[0], session.peers[1]],
					[session.peers[1], session.peers[2]],
					[session.peers[3], session.peers[4]],
					[session.peers[4], session.peers[5]],
				]);

				peers = [];
				for (const [i, peer] of session.peers.entries()) {
					const client: {
						stream: TX;
						messages: Message[];
						recieved: DataMessage[];
						reachable: PublicSignKey[];
						unrechable: PublicSignKey[];
					} = {
						messages: [],
						recieved: [],
						reachable: [],
						unrechable: [],
						stream: peer.services.directstream,
					};
					peers.push(client);
					peer.services.directstream.addEventListener("message", (msg) => {
						client.messages.push(msg.detail);
					});
					peer.services.directstream.addEventListener("data", (msg) => {
						client.recieved.push(msg.detail);
					});
				}

				for (const peer of peers.values()) {
					await waitFor(() => peer.stream.routes.linksCount === 2);
				}

				for (let i = 0; i < 2; i++) {
					await waitForPeerStreams(peers[i].stream, peers[i + 1].stream);
				}
				for (let i = 3; i < 5; i++) {
					await waitForPeerStreams(peers[i].stream, peers[i + 1].stream);
				}
			});

			afterAll(async () => {
				await Promise.all(peers.map((peer) => peer.stream.stop()));
				await session.stop();
			});
			it("will replay on connect", async () => {
				for (let i = 3; i < 5; i++) {
					await waitForPeerStreams(peers[i].stream, peers[i + 1].stream);
				}
				expect(peers[2].stream.helloMap.size).toEqual(2); // these hellos will be forwarded on connect
				expect(peers[3].stream.helloMap.size).toEqual(2); // these hellos will be forwarded on connect
				await session.connect([[session.peers[2], session.peers[3]]]);

				for (const peer of peers) {
					await waitFor(() => peer.stream.routes.linksCount === 5); // everyone knows everone
				}
			});
		});
	});

	describe("start/stop", () => {
		let session: TestSession;

		afterEach(async () => {
			await session.stop();
		});

		it("can restart", async () => {
			session = await connected(2, {
				transports: [tcp(), webSockets({ filter: filters.all })],
				services: {
					directstream: (c) =>
						new TX(c, {
							connectionManager: { autoDial: false },
						}),
				},
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
					directstream: (c) => new TX(c),
				},
			});

			await session.connect();
			await waitForPeerStreams(stream(session, 0), stream(session, 1));
		});

		it("can connect before start", async () => {
			session = await connected(2, {
				transports: [tcp(), webSockets({ filter: filters.all })],
				services: {
					directstream: (c) => new TX(c),
				},
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
					directstream: (c) => new TX(c),
				},
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
		let session: TestSession;
		beforeEach(async () => {
			session = await LSession.connected(2, {
				transports: [tcp(), webSockets({ filter: filters.all })],
				services: {
					directstream: (c) => new TX(c),
					directstream2: (c) => new TX(c, { id: "another-protocol" }),
				},
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
