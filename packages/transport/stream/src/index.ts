import { EventEmitter, CustomEvent } from "@libp2p/interface/events";
import { pipe } from "it-pipe";
import Queue from "p-queue";
import type { PeerId } from "@libp2p/interface/peer-id";
import type { Connection } from "@libp2p/interface/connection";
import type { Pushable } from "it-pushable";
import { pushable } from "it-pushable";
import type { Stream } from "@libp2p/interface/connection";
import { Uint8ArrayList } from "uint8arraylist";
import { abortableSource } from "abortable-iterator";
import * as lp from "it-length-prefixed";
import { Routes } from "./routes.js";
import { multiaddr } from "@multiformats/multiaddr";
import { PeerMap } from "./peer-map.js";
import type {
	IncomingStreamData,
	Registrar
} from "@libp2p/interface-internal/registrar";
import type { AddressManager } from "@libp2p/interface-internal/address-manager";
import type { ConnectionManager } from "@libp2p/interface-internal/connection-manager";

import { PeerStore } from "@libp2p/interface/peer-store";

import { delay, TimeoutError, waitFor } from "@peerbit/time";
import {
	Ed25519PublicKey,
	getKeypairFromPeerId,
	getPublicKeyFromPeerId,
	PublicSignKey,
	SignatureWithKey
} from "@peerbit/crypto";
import { Errors } from "datastore-core";

export type SignaturePolicy = "StictSign" | "StrictNoSign";
import { logger } from "./logger.js";
import { Cache } from "@peerbit/cache";
export { logger };
import type { Libp2pEvents } from "@libp2p/interface";
import {
	PeerEvents,
	Message as Message,
	Goodbye,
	Hello,
	DataMessage,
	Ping,
	PingPong,
	Pong,
	getMsgId,
	WaitForPeer
} from "@peerbit/stream-interface";
export interface PeerStreamsInit {
	peerId: PeerId;
	publicKey: PublicSignKey;
	protocol: string;
	connId: string;
}

const isWebsocketConnection = (c: Connection) =>
	c.remoteAddr.protoNames().find((x) => x === "ws" || x === "wss");

export interface PeerStreamEvents {
	"stream:inbound": CustomEvent<never>;
	"stream:outbound": CustomEvent<never>;
	close: CustomEvent<never>;
}
/**
 * Thin wrapper around a peer's inbound / outbound pubsub streams
 */
export class PeerStreams extends EventEmitter<PeerStreamEvents> {
	public counter = 0;
	public readonly peerId: PeerId;
	public readonly publicKey: PublicSignKey;
	public readonly protocol: string;
	/**
	 * Write stream - it's preferable to use the write method
	 */
	public outboundStream?: Pushable<Uint8ArrayList>;
	/**
	 * Read stream
	 */
	public inboundStream?: AsyncIterable<Uint8ArrayList>;
	/**
	 * The raw outbound stream, as retrieved from conn.newStream
	 */
	public _rawOutboundStream?: Stream;
	/**
	 * The raw inbound stream, as retrieved from the callback from libp2p.handle
	 */
	public _rawInboundStream?: Stream;
	/**
	 * An AbortController for controlled shutdown of the  treams
	 */
	private readonly inboundAbortController: AbortController;

	private closed: boolean;

	public pingJob: { resolve: () => void; abort: () => void };
	public pingLatency: number | undefined;

	public connId: string;
	constructor(init: PeerStreamsInit) {
		super();

		this.peerId = init.peerId;
		this.publicKey = init.publicKey;
		this.protocol = init.protocol;
		this.inboundAbortController = new AbortController();
		this.closed = false;
		this.connId = init.connId;
		this.counter = 1;
	}

	/**
	 * Do we have a connection to read from?
	 */
	get isReadable() {
		return Boolean(this.inboundStream);
	}

	/**
	 * Do we have a connection to write on?
	 */
	get isWritable() {
		return Boolean(this.outboundStream);
	}

	/**
	 * Send a message to this peer.
	 * Throws if there is no `stream` to write to available.
	 */
	write(data: Uint8Array | Uint8ArrayList) {
		if (this.outboundStream == null) {
			logger.error("No writable connection to " + this.peerId.toString());
			throw new Error("No writable connection to " + this.peerId.toString());
		}
		this.outboundStream.push(
			data instanceof Uint8Array ? new Uint8ArrayList(data) : data
		);
	}

	async waitForWrite(bytes: Uint8Array | Uint8ArrayList) {
		if (!this.isWritable) {
			// Catch the event where the outbound stream is attach, but also abort if we shut down
			const outboundPromise = new Promise<void>((rs, rj) => {
				const resolve = () => {
					this.removeEventListener("stream:outbound", listener);
					clearTimeout(timer);
					rs();
				};
				const reject = (err: Error) => {
					this.removeEventListener("stream:outbound", listener);
					clearTimeout(timer);
					rj(err);
				};
				const timer = setTimeout(() => {
					reject(new Error("Timed out"));
				}, 3 * 1000); // TODO if this timeout > 10s we run into issues in the tests when running in CI
				const abortHandler = () => {
					this.removeEventListener("close", abortHandler);
					reject(new Error("Aborted"));
				};
				this.addEventListener("close", abortHandler);

				const listener = () => {
					resolve();
				};
				this.addEventListener("stream:outbound", listener);
				if (this.isWritable) {
					resolve();
				}
			});

			await outboundPromise
				.then(() => {
					this.write(bytes);
				})
				.catch((error) => {
					logger.error(
						"Failed to send to stream: " +
							this.peerId +
							". " +
							(error?.message || error?.toString())
					);
				});
		} else {
			this.write(bytes);
		}
	}

	/**
	 * Attach a raw inbound stream and setup a read stream
	 */
	attachInboundStream(stream: Stream) {
		// Create and attach a new inbound stream
		// The inbound stream is:
		// - abortable, set to only return on abort, rather than throw
		// - transformed with length-prefix transform
		this._rawInboundStream = stream;
		this.inboundStream = abortableSource(
			pipe(this._rawInboundStream, (source) =>
				lp.decode(source, { maxDataLength: 100001000 })
			),
			this.inboundAbortController.signal,
			{
				returnOnAbort: true,
				onReturnError: (err) => {
					logger.error("Inbound stream error", err?.message);
				}
			}
		);

		this.dispatchEvent(new CustomEvent("stream:inbound"));
		return this.inboundStream;
	}

	/**
	 * Attach a raw outbound stream and setup a write stream
	 */

	async attachOutboundStream(stream: Stream) {
		// If an outbound stream already exists, gently close it
		const _prevStream = this.outboundStream;

		this._rawOutboundStream = stream;
		this.pingJob?.abort();

		this.outboundStream = pushable<Uint8ArrayList>({
			objectMode: true,
			onEnd: () => {
				return stream.close().then(() => {
					if (this._rawOutboundStream === stream) {
						this.dispatchEvent(new CustomEvent("close"));
						this._rawOutboundStream = undefined;
						this.outboundStream = undefined;
					}
				});
			}
		});

		pipe(
			this.outboundStream,
			(source) => lp.encode(source),
			this._rawOutboundStream
		).catch((err: Error) => {
			logger.error(err);
		});

		// Emit if the connection is new
		this.dispatchEvent(new CustomEvent("stream:outbound"));

		if (_prevStream != null) {
			// End the stream without emitting a close event
			await _prevStream.end();
		}
		return this.outboundStream;
	}

	/**
	 * Closes the open connection to peer
	 */
	async close() {
		if (this.closed) {
			return;
		}

		this.closed = true;

		// End the outbound stream
		if (this.outboundStream != null) {
			await this.outboundStream.return();
			await this._rawOutboundStream?.close();
		}
		// End the inbound stream
		if (this.inboundStream != null) {
			this.inboundAbortController.abort();
			await this._rawInboundStream?.close();
		}

		this.pingJob?.abort();
		this.pingLatency = undefined;

		//this.dispatchEvent(new CustomEvent('close'))
		this._rawOutboundStream = undefined;
		this.outboundStream = undefined;
		this._rawInboundStream = undefined;
		this.inboundStream = undefined;
	}
}

export interface MessageEvents {
	message: CustomEvent<Message>;
}

export interface StreamEvents extends PeerEvents, MessageEvents {
	data: CustomEvent<DataMessage>;
}

export type ConnectionManagerOptions = {
	autoDial?: boolean;
	retryDelay?: number;
};
export type DirectStreamOptions = {
	canRelayMessage?: boolean;
	emitSelf?: boolean;
	messageProcessingConcurrency?: number;
	maxInboundStreams?: number;
	maxOutboundStreams?: number;
	signaturePolicy?: SignaturePolicy;
	pingInterval?: number | null;
	connectionManager?: ConnectionManagerOptions;
};

import { Components } from "libp2p/components";

export interface DirectStreamComponents extends Components {
	peerId: PeerId;
	addressManager: AddressManager;
	registrar: Registrar;
	connectionManager: ConnectionManager;
	peerStore: PeerStore;
	events: EventEmitter<Libp2pEvents>;
}

export abstract class DirectStream<
		Events extends { [s: string]: any } = StreamEvents
	>
	extends EventEmitter<Events>
	implements WaitForPeer
{
	public peerId: PeerId;
	public peerIdStr: string;
	public publicKey: PublicSignKey;
	public publicKeyHash: string;
	public sign: (bytes: Uint8Array) => Promise<SignatureWithKey>;

	public started: boolean;

	/**
	 * Map of peer streams
	 */
	public peers: PeerMap<PeerStreams>;
	public peerKeyHashToPublicKey: Map<string, PublicSignKey>;
	public peerIdToPublicKey: Map<string, PublicSignKey>;
	public routes: Routes;
	/**
	 * If router can relay received messages, even if not subscribed
	 */
	public canRelayMessage: boolean;
	/**
	 * if publish should emit to self, if subscribed
	 */

	public signaturePolicy: SignaturePolicy;
	public emitSelf: boolean;
	public queue: Queue;
	public multicodecs: string[];
	public seenCache: Cache;
	public earlyGoodbyes: Map<string, Goodbye>;
	public helloMap: Map<string, Map<string, Hello>>; // key is hash of publicKey, value is map whey key is hash of signature bytes, and value is latest Hello
	public multiaddrsMap: Map<string, string[]>;
	private _registrarTopologyIds: string[] | undefined;
	private readonly maxInboundStreams?: number;
	private readonly maxOutboundStreams?: number;
	private topology: any;
	private pingJobPromise: any;
	private pingJob: any;
	private pingInterval: number | null;
	private connectionManagerOptions: ConnectionManagerOptions;
	private recentDials: Cache<string>;

	constructor(
		readonly components: DirectStreamComponents,
		multicodecs: string[],
		options?: DirectStreamOptions
	) {
		super();
		const {
			canRelayMessage = false,
			emitSelf = false,
			messageProcessingConcurrency = 10,
			pingInterval = 10 * 1000,
			maxInboundStreams,
			maxOutboundStreams,
			signaturePolicy = "StictSign",
			connectionManager = { autoDial: true }
		} = options || {};

		const signKey = getKeypairFromPeerId(components.peerId);
		this.sign = signKey.sign.bind(signKey);
		this.peerId = components.peerId;
		this.peerIdStr = components.peerId.toString();
		this.publicKey = signKey.publicKey;
		this.publicKeyHash = signKey.publicKey.hashcode();
		this.multicodecs = multicodecs;
		this.started = false;
		this.peers = new Map<string, PeerStreams>();
		this.helloMap = new Map();
		this.multiaddrsMap = new Map();
		this.routes = new Routes(this.publicKeyHash);
		this.canRelayMessage = canRelayMessage;
		this.emitSelf = emitSelf;
		this.queue = new Queue({ concurrency: messageProcessingConcurrency });
		this.earlyGoodbyes = new Map();
		this.maxInboundStreams = maxInboundStreams;
		this.maxOutboundStreams = maxOutboundStreams;
		this.seenCache = new Cache({ max: 1e3, ttl: 10 * 60 * 1e3 });
		this.peerKeyHashToPublicKey = new Map();
		this.peerIdToPublicKey = new Map();
		this.pingInterval = pingInterval;
		this._onIncomingStream = this._onIncomingStream.bind(this);
		this.onPeerConnected = this.onPeerConnected.bind(this);
		this.onPeerDisconnected = this.onPeerDisconnected.bind(this);
		this.signaturePolicy = signaturePolicy;
		this.connectionManagerOptions = connectionManager;
		this.recentDials = new Cache({
			ttl: connectionManager.retryDelay || 60 * 1000,
			max: 1e3
		});
	}

	async start() {
		if (this.started) {
			return;
		}

		logger.debug("starting");

		// register protocol with topology
		// Topology callbacks called on connection manager changes
		this._registrarTopologyIds = await Promise.all(
			this.multicodecs.map((multicodec) =>
				this.components.registrar.register(multicodec, {
					onConnect: this.onPeerConnected.bind(this),
					onDisconnect: this.onPeerDisconnected.bind(this)
				})
			)
		);

		// Incoming streams
		// Called after a peer dials us
		await Promise.all(
			this.multicodecs.map((multicodec) =>
				this.components.registrar.handle(multicodec, this._onIncomingStream, {
					maxInboundStreams: this.maxInboundStreams,
					maxOutboundStreams: this.maxOutboundStreams
				})
			)
		);

		// TODO remove/modify when https://github.com/libp2p/js-libp2p/issues/2036 is resolved
		this.components.events.addEventListener("connection:open", (e) => {
			this.onPeerConnected(e.detail.remotePeer, e.detail, {
				skipIfVisited: true
			});
		});
		this.started = true;

		// All existing connections are like new ones for us. To deduplication on remotes so we only resuse one connection for this protocol (we could be connected with many connections)
		const peerToConnections: Map<string, Connection[]> = new Map();
		const connections = this.components.connectionManager.getConnections();
		for (const conn of connections) {
			let arr = peerToConnections.get(conn.remotePeer.toString());
			if (!arr) {
				arr = [];
				peerToConnections.set(conn.remotePeer.toString(), arr);
			}
			arr.push(conn);
		}
		for (const [_peer, arr] of peerToConnections) {
			let conn = arr[0]; // TODO choose TCP when both websocket and tcp exist
			for (const c of arr) {
				if (!isWebsocketConnection(c)) {
					// TODO what is correct connection prioritization?
					conn = c; // always favor non websocket address
					break;
				}
			}

			await this.onPeerConnected(conn.remotePeer, conn, { fromExisting: true });
		}

		const pingJob = async () => {
			// TODO don't use setInterval but waitFor previous done to be done
			await this.pingJobPromise;
			const promises: Promise<any>[] = [];
			this.peers.forEach((peer) => {
				promises.push(
					this.ping(peer).catch((e) => {
						if (e instanceof TimeoutError) {
							// Ignore
						} else {
							logger.error(e);
						}
					})
				);
			});
			promises.push(this.hello()); // Repetedly say hello to everyone to create traces in the network to measure latencies
			this.pingJobPromise = Promise.all(promises)
				.catch((e) => {
					logger.error(e?.message);
				})
				.finally(() => {
					if (!this.started || !this.pingInterval) {
						return;
					}
					this.pingJob = setTimeout(pingJob, this.pingInterval);
				});
		};
		pingJob();
	}

	/**
	 * Unregister the pubsub protocol and the streams with other peers will be closed.
	 */
	async stop() {
		if (!this.started) {
			return;
		}
		this.started = false;

		clearTimeout(this.pingJob);
		await this.pingJobPromise;

		await Promise.all(
			this.multicodecs.map((x) => this.components.registrar.unhandle(x))
		);

		logger.debug("stopping");
		for (const peerStreams of this.peers.values()) {
			await peerStreams.close();
		}

		// unregister protocol and handlers
		if (this._registrarTopologyIds != null) {
			this._registrarTopologyIds?.map((id) =>
				this.components.registrar.unregister(id)
			);
		}

		this.queue.clear();
		this.helloMap.clear();
		this.multiaddrsMap.clear();
		this.earlyGoodbyes.clear();
		this.peers.clear();
		this.seenCache.clear();
		this.routes.clear();
		this.peerKeyHashToPublicKey.clear();
		this.peerIdToPublicKey.clear();
		logger.debug("stopped");
	}

	isStarted() {
		return this.started;
	}

	/**
	 * On an inbound stream opened
	 */

	protected async _onIncomingStream(data: IncomingStreamData) {
		if (!this.isStarted()) {
			return;
		}

		const { stream, connection } = data;
		const peerId = connection.remotePeer;
		if (stream.protocol == null) {
			stream.abort(new Error("Stream was not multiplexed"));
			return;
		}

		const publicKey = getPublicKeyFromPeerId(peerId);
		const peer = this.addPeer(
			peerId,
			publicKey,
			stream.protocol,
			connection.id
		);
		const inboundStream = peer.attachInboundStream(stream);
		this.processMessages(peerId, inboundStream, peer).catch((err) => {
			logger.error(err);
		});
	}

	/**
	 * Registrar notifies an established connection with protocol
	 */
	public async onPeerConnected(
		peerId: PeerId,
		conn: Connection,
		properties?: { skipIfVisited?: boolean; fromExisting?: boolean }
	) {
		if (conn.transient) {
			return;
		}

		if (!this.isStarted() || conn.status !== "open") {
			return;
		}

		try {
			const hasProtocol = await this.components.peerStore
				.get(peerId)
				.then((x) => this.multicodecs.find((y) => x.protocols.includes(y)));
			if (!hasProtocol) {
				return;
			}
		} catch (error: any) {
			if (error.code === "ERR_NOT_FOUND") {
				return;
			}
			throw error;
		}

		try {
			const peerKey = getPublicKeyFromPeerId(peerId);
			const peerKeyHash = peerKey.hashcode();

			for (const existingStreams of conn.streams) {
				if (
					existingStreams.protocol &&
					this.multicodecs.includes(existingStreams.protocol) &&
					existingStreams.direction === "outbound"
				) {
					return;
				}
			}

			// This condition seem to work better than the one above, for some reason.
			// The reason we need this at all is because we will connect to existing connection and receive connection that
			// some times, yields a race connections where connection drop each other by reset

			let stream: Stream = undefined as any; // TODO types
			let tries = 0;
			let peer: PeerStreams = undefined as any;
			while (tries <= 3) {
				tries++;
				if (!this.started) {
					return;
				}

				try {
					stream = await conn.newStream(this.multicodecs);
					if (stream.protocol == null) {
						stream.abort(new Error("Stream was not multiplexed"));
						return;
					}
					peer = this.addPeer(peerId, peerKey, stream.protocol!, conn.id); // TODO types
					await peer.attachOutboundStream(stream);

					// TODO do we want to do this?
					/* if (!peer.inboundStream) {
						const inboundStream = conn.streams.find(
							(x) =>
								x.stat.protocol &&
								this.multicodecs.includes(x.stat.protocol) &&
								x.stat.direction === "inbound"
						);
						if (inboundStream) {
							this._onIncomingStream({
								connection: conn,
								stream: inboundStream,
							});
						}
					} */
				} catch (error: any) {
					if (error.code === "ERR_UNSUPPORTED_PROTOCOL") {
						await delay(100);
						continue; // Retry
					}

					if (
						conn.status !== "open" ||
						error?.message === "Muxer already closed" ||
						error.code === "ERR_STREAM_RESET"
					) {
						return; // fail silenty
					}

					throw error;
				}
				break;
			}
			if (!stream) {
				return;
			}

			if (properties?.fromExisting) {
				return; // we return here because we will enter this method once more once the protocol has been registered for the remote peer
			}

			// Add connection with assumed large latency
			this.peerIdToPublicKey.set(peerId.toString(), peerKey);
			const promises: Promise<any>[] = [];

			/* if (!existingStream)  */ {
				this.addRouteConnection(
					this.publicKey,
					peerKey,
					Number.MAX_SAFE_INTEGER
				);

				// Get accurate latency
				promises.push(this.ping(peer));

				// Say hello
				promises.push(
					this.publishMessage(
						this.components.peerId,
						await new Hello({
							multiaddrs: this.components.addressManager
								.getAddresses()
								.map((x) => x.toString())
						}).sign(this.sign),
						[peer]
					)
				);
				// Send my goodbye early if I disconnect for some reason, (so my peer can say goodbye for me)
				// TODO add custom condition fn for doing below
				promises.push(
					this.publishMessage(
						this.components.peerId,
						await new Goodbye({ early: true }).sign(this.sign),
						[peer]
					)
				);

				// replay all hellos
				for (const [sender, hellos] of this.helloMap) {
					if (sender === peerKeyHash) {
						// Don't say hellos from sender to same sender (uneccessary)
						continue;
					}

					outer: for (const [key, hello] of hellos) {
						if (!hello.header.verify()) {
							hellos.delete(key);
						}
						for (const signer of hello.signatures.publicKeys) {
							if (!this.routes.hasNode(signer.hashcode())) {
								// purge this hello since it has travelled a path that no longer exist
								hellos.delete(key);
								continue outer;
							}
						}

						promises.push(
							this.publishMessage(this.components.peerId, hello, [peer])
						);
					}
				}
			}
			const resolved = await Promise.all(promises);
			return resolved;
		} catch (err: any) {
			logger.error(err);
		}
	}

	private addRouteConnection(
		from: PublicSignKey,
		to: PublicSignKey,
		latency: number
	) {
		this.peerKeyHashToPublicKey.set(from.hashcode(), from);
		this.peerKeyHashToPublicKey.set(to.hashcode(), to);
		const links = this.routes.addLink(from.hashcode(), to.hashcode(), latency);
		for (const added of links) {
			const key = this.peerKeyHashToPublicKey.get(added);
			if (key?.equals(this.publicKey) === false) {
				this.onPeerReachable(key!);
			}
		}
	}

	removeRouteConnection(from: PublicSignKey, to: PublicSignKey) {
		const has = this.routes.hasNode(to.hashcode());
		if (!has) {
			this.onPeerUnreachable(to);
		} else {
			const links = this.routes.deleteLink(from.hashcode(), to.hashcode());
			for (const deleted of links) {
				const key = this.peerKeyHashToPublicKey.get(deleted)!;
				this.peerKeyHashToPublicKey.delete(deleted);
				if (key?.equals(this.publicKey) === false) {
					this.onPeerUnreachable(key!);
				}
			}
		}
	}

	/**
	 * Registrar notifies a closing connection with pubsub protocol
	 */
	protected async onPeerDisconnected(peerId: PeerId, conn?: Connection) {
		// PeerId could be me, if so, it means that I am disconnecting
		const peerKey = getPublicKeyFromPeerId(peerId);
		const peerKeyHash = peerKey.hashcode();
		const connections = this.components.connectionManager
			.getConnectionsMap()
			.get(peerId);
		if (
			conn?.id &&
			connections &&
			connections.length > 0 &&
			!this.components.connectionManager
				.getConnectionsMap()
				.get(peerId)
				?.find(
					(x) => x.id === conn.id
				) /* TODO this should work but does not? peer?.connId !== conn.id */
		) {
			return;
		}

		if (!this.publicKey.equals(peerKey)) {
			await this._removePeer(peerKey);
			this.removeRouteConnection(this.publicKey, peerKey);

			// Notify network
			const earlyGoodBye = this.earlyGoodbyes.get(peerKeyHash);
			if (earlyGoodBye) {
				earlyGoodBye.early = false;
				await earlyGoodBye.sign(this.sign);
				await this.publishMessage(this.components.peerId, earlyGoodBye);
				this.earlyGoodbyes.delete(peerKeyHash);
			}
			this.peerIdToPublicKey.delete(peerId.toString());
		}

		logger.debug("connection ended:" + peerKey.toString());
	}

	/**
	 * invoked when a new peer becomes reachable
	 * @param publicKeyHash
	 */
	public onPeerReachable(publicKey: PublicSignKey) {
		// override this fn
		this.dispatchEvent(
			new CustomEvent("peer:reachable", { detail: publicKey })
		);
	}

	/**
	 * invoked when a new peer becomes unreachable
	 * @param publicKeyHash
	 */
	public onPeerUnreachable(publicKey: PublicSignKey) {
		// override this fn
		this.helloMap.delete(publicKey.hashcode());
		this.multiaddrsMap.delete(publicKey.hashcode());

		this.dispatchEvent(
			new CustomEvent("peer:unreachable", { detail: publicKey })
		);
	}

	/**
	 * Notifies the router that a peer has been connected
	 */
	addPeer(
		peerId: PeerId,
		publicKey: PublicSignKey,
		protocol: string,
		connId: string
	): PeerStreams {
		const publicKeyHash = publicKey.hashcode();
		const existing = this.peers.get(publicKeyHash);

		// If peer streams already exists, do nothing
		if (existing != null) {
			existing.counter += 1;
			existing.connId = connId;
			return existing;
		}

		// else create a new peer streams
		const peerIdStr = peerId.toString();
		logger.debug("new peer" + peerIdStr);

		const peerStreams: PeerStreams = new PeerStreams({
			peerId,
			publicKey,
			protocol,
			connId
		});

		this.peers.set(publicKeyHash, peerStreams);
		peerStreams.addEventListener("close", () => this._removePeer(publicKey), {
			once: true
		});
		return peerStreams;
	}

	/**
	 * Notifies the router that a peer has been disconnected
	 */
	protected async _removePeer(publicKey: PublicSignKey) {
		const hash = publicKey.hashcode();
		const peerStreams = this.peers.get(hash);

		if (peerStreams == null) {
			return;
		}

		// close peer streams
		await peerStreams.close();

		// delete peer streams
		logger.debug("delete peer" + publicKey.toString());
		this.peers.delete(hash);
		return peerStreams;
	}

	// MESSAGE METHODS

	/**
	 * Responsible for processing each RPC message received by other peers.
	 */
	async processMessages(
		peerId: PeerId,
		stream: AsyncIterable<Uint8ArrayList>,
		peerStreams: PeerStreams
	) {
		try {
			await pipe(stream, async (source) => {
				for await (const data of source) {
					const msgId = await getMsgId(data);
					if (this.seenCache.has(msgId)) {
						// we got message that WE sent?

						/**
							 * Most propobable reason why we arrive  here is a race condition/issue
							
							┌─┐
							│0│
							└△┘
							┌▽┐
							│1│
							└△┘
							┌▽┐
							│2│
							└─┘
							
							from 2s perspective, 
	
							if everyone conents to each other at the same time, then 0 will say hello to 1 and 1 will save that hello to resend to 2 if 2 ever connects
							but two is already connected by onPeerConnected has not been invoked yet, so the hello message gets forwarded,
							and later onPeerConnected gets invoked on 1, and the same message gets resent to 2
							 */

						continue;
					}

					this.seenCache.add(msgId);
					this.processRpc(peerId, peerStreams, data).catch((err) =>
						logger.warn(err)
					);
				}
			});
		} catch (err: any) {
			logger.error(
				"error on processing messages to id: " +
					peerStreams.peerId.toString() +
					". " +
					err?.message
			);
			this.onPeerDisconnected(peerStreams.peerId);
		}
	}

	/**
	 * Handles an rpc request from a peer
	 */
	async processRpc(
		from: PeerId,
		peerStreams: PeerStreams,
		message: Uint8ArrayList
	): Promise<boolean> {
		if (!this.acceptFrom(from)) {
			logger.debug("received message from unacceptable peer %p", from);
			return false;
		}

		// logger.debug("rpc from " + from + ", " + this.peerIdStr);

		if (message.length > 0) {
			//	logger.debug("messages from " + from);
			await this.queue
				.add(async () => {
					try {
						await this.processMessage(from, peerStreams, message);
					} catch (err: any) {
						logger.error(err);
					}
				})
				.catch((err) => logger.warn(err));
		}

		return true;
	}

	/**
	 * Handles a message from a peer
	 */
	async processMessage(
		from: PeerId,
		peerStream: PeerStreams,
		msg: Uint8ArrayList
	) {
		if (!from.publicKey) {
			return;
		}

		if (this.components.peerId.equals(from) && !this.emitSelf) {
			return;
		}

		// Ensure the message is valid before processing it
		const message: Message | undefined = Message.from(msg);

		this.dispatchEvent(
			new CustomEvent("message", {
				detail: message
			})
		);

		if (message instanceof DataMessage) {
			await this.onDataMessage(from, peerStream, message);
		} else if (message instanceof Hello) {
			await this.onHello(from, peerStream, message);
		} else if (message instanceof Goodbye) {
			await this.onGoodbye(from, peerStream, message);
		} else if (message instanceof PingPong) {
			await this.onPing(from, peerStream, message);
		} else {
			throw new Error("Unsupported");
		}
	}

	async onDataMessage(
		from: PeerId,
		peerStream: PeerStreams,
		message: DataMessage
	) {
		const isFromSelf = this.components.peerId.equals(from);
		if (!isFromSelf || this.emitSelf) {
			const isForAll = message.to.length === 0;
			const isForMe =
				!isForAll && message.to.find((x) => x === this.publicKeyHash);
			if (isForAll || isForMe) {
				if (
					this.signaturePolicy === "StictSign" &&
					!(await message.verify(this.signaturePolicy === "StictSign"))
				) {
					// we don't verify messages we don't dispatch because of the performance penalty // TODO add opts for this
					logger.warn("Recieved message with invalid signature or timestamp");
					return false;
				}

				this.dispatchEvent(
					new CustomEvent("data", {
						detail: message
					})
				);
			}
			if (isForMe && message.to.length === 1) {
				// dont forward this message anymore because it was meant ONLY for me
				return true;
			}
		}

		// Forward
		await this.relayMessage(from, message);
		return true;
	}

	async onHello(from: PeerId, peerStream: PeerStreams, message: Hello) {
		if (!(await message.verify(false))) {
			const a = message.header.verify();
			const b =
				message.networkInfo.pingLatencies.length ===
				message.signatures.signatures.length - 1;
			logger.warn(
				`Recieved hello message that did not verify. Header: ${a}, Ping info ${b}, Signatures ${
					a && b
				}`
			);
			return false;
		}

		const sender = message.sender?.hashcode();
		if (!sender) {
			logger.warn("Recieved hello without sender");
			return false;
		}

		const signatures = message.signatures;
		for (let i = 0; i < signatures.signatures.length - 1; i++) {
			this.addRouteConnection(
				signatures.signatures[i].publicKey,
				signatures.signatures[i + 1].publicKey,
				message.networkInfo.pingLatencies[i]
			);
		}

		message.networkInfo.pingLatencies.push(
			peerStream.pingLatency || 4294967295
		); // TODO don't propagate if latency is high?

		await message.sign(this.sign); // sign it so othere peers can now I have seen it (and can build a network graph from trace info)

		let hellos = this.helloMap.get(sender);
		if (!hellos) {
			hellos = new Map();
			this.helloMap.set(sender, hellos);
		}

		this.multiaddrsMap.set(sender, message.multiaddrs);

		const helloSignaturHash = await message.signatures.hashPublicKeys();
		const existingHello = hellos.get(helloSignaturHash);
		if (existingHello) {
			if (existingHello.header.expires < message.header.expires) {
				hellos.set(helloSignaturHash, message);
			}
		} else {
			hellos.set(helloSignaturHash, message);
		}

		// Forward
		await this.relayMessage(from, message);
		return true;
	}

	async onGoodbye(from: PeerId, peerStream: PeerStreams, message: Goodbye) {
		if (!(await message.verify(false))) {
			logger.warn("Recieved message with invalid signature or timestamp");
			return false;
		}

		const sender = message.sender?.hashcode();
		if (!sender) {
			logger.warn("Recieved hello without sender");
			return false;
		}

		const peerKey = getPublicKeyFromPeerId(from);
		const peerKeyHash = peerKey.hashcode();
		if (message.early) {
			this.earlyGoodbyes.set(peerKeyHash, message);
		} else {
			const signatures = message.signatures;
			/*  TODO Should we update routes on goodbye?
			for (let i = 1; i < signatures.signatures.length - 1; i++) {
				this.addRouteConnection(
					signatures.signatures[i].publicKey,
					signatures.signatures[i + 1].publicKey
				);
			} 
			*/

			//let neighbour = message.trace[1] || this.peerIdStr;
			this.removeRouteConnection(
				signatures.signatures[0].publicKey,
				signatures.signatures[1].publicKey || this.publicKey
			);

			const relayToPeers: PeerStreams[] = [];
			for (const stream of this.peers.values()) {
				if (stream.peerId.equals(from)) {
					continue;
				}
				relayToPeers.push(stream);
			}
			await message.sign(this.sign); // sign it so othere peers can now I have seen it (and can build a network graph from trace info)

			const hellos = this.helloMap.get(sender);
			if (hellos) {
				const helloSignaturHash = await message.signatures.hashPublicKeys();
				hellos.delete(helloSignaturHash);
			}

			// Forward
			await this.relayMessage(from, message);
		}
		return true;
	}

	async onPing(from: PeerId, peerStream: PeerStreams, message: PingPong) {
		if (message instanceof Ping) {
			// respond with pong
			await this.publishMessage(
				this.components.peerId,
				new Pong(message.pingBytes),
				[peerStream]
			);
		} else if (message instanceof Pong) {
			// Let the (waiting) thread know that we have received the pong
			peerStream.pingJob?.resolve();
		} else {
			throw new Error("Unsupported");
		}
	}

	async ping(stream: PeerStreams): Promise<number | undefined> {
		return new Promise<number | undefined>((resolve, reject) => {
			stream.pingJob?.abort();
			const ping = new Ping();
			const start = +new Date();
			const timeout = setTimeout(() => {
				reject(new TimeoutError("Ping timed out"));
			}, 10000);
			const resolver = () => {
				const end = +new Date();
				clearTimeout(timeout);

				// TODO what happens if a peer send a ping back then leaves? Any problems?
				const latency = end - start;
				stream.pingLatency = latency;
				this.addRouteConnection(this.publicKey, stream.publicKey, latency);
				resolve(undefined);
			};
			stream.pingJob = {
				resolve: resolver,
				abort: () => {
					clearTimeout(timeout);
					resolve(undefined);
				}
			};
			this.publishMessage(this.components.peerId, ping, [stream]).catch(
				(err) => {
					clearTimeout(timeout);
					reject(err);
				}
			);
		});
	}

	/**
	 * Whether to accept a message from a peer
	 * Override to create a graylist
	 */
	acceptFrom(id: PeerId) {
		return true;
	}

	async createMessage(
		data: Uint8Array | Uint8ArrayList,
		options?: { to?: (string | PublicSignKey | PeerId)[] | Set<string> }
	) {
		// dispatch the event if we are interested
		let toHashes: string[];
		if (options?.to) {
			if (options.to instanceof Set) {
				toHashes = new Array(options.to.size);
			} else {
				toHashes = new Array(options.to.length);
			}

			let i = 0;
			for (const to of options.to) {
				toHashes[i++] =
					to instanceof PublicSignKey
						? to.hashcode()
						: typeof to === "string"
						? to
						: getPublicKeyFromPeerId(to).hashcode();
			}
		} else {
			toHashes = [];
		}
		const message = new DataMessage({
			data: data instanceof Uint8ArrayList ? data.subarray() : data,
			to: toHashes
		});
		if (this.signaturePolicy === "StictSign") {
			await message.sign(this.sign);
		}
		return message;
	}
	/**
	 * Publishes messages to all peers
	 */
	async publish(
		data: Uint8Array | Uint8ArrayList,
		options?: { to?: (string | PublicSignKey | PeerId)[] | Set<string> }
	): Promise<Uint8Array> {
		if (!this.started) {
			throw new Error("Not started");
		}

		const message = await this.createMessage(data, options);

		if (this.emitSelf) {
			super.dispatchEvent(
				new CustomEvent("data", {
					detail: message
				})
			);
		}

		// send to all the other peers
		await this.publishMessage(this.components.peerId, message, undefined);
		return message.id;
	}

	public async hello(data?: Uint8Array): Promise<void> {
		if (!this.started) {
			return;
		}

		// send to all the other peers
		await this.publishMessage(
			this.components.peerId,
			await new Hello({
				multiaddrs: this.components.addressManager
					.getAddresses()
					.map((x) => x.toString()),
				data
			}).sign(this.sign.bind(this))
		);
	}

	public async relayMessage(
		from: PeerId,
		message: Message,
		to?: PeerStreams[] | PeerMap<PeerStreams>
	) {
		if (this.canRelayMessage) {
			return this.publishMessage(from, message, to, true);
		} else {
			logger.debug("received message we didn't subscribe to. Dropping.");
		}
	}
	public async publishMessage(
		from: PeerId,
		message: Message,
		to?: PeerStreams[] | PeerMap<PeerStreams>,
		relayed?: boolean
	): Promise<void> {
		if (message instanceof DataMessage && !to) {
			// message.to can be distant peers, but "to" are neighbours
			const fanoutMap = new Map<string, string[]>();

			// Message to > 0
			if (message.to.length > 0) {
				const missingPathsFor: string[] = [];
				for (const to of message.to) {
					const fromKey = this.peerIdToPublicKey
						.get(from.toString())
						?.hashcode();
					if (to === this.publicKeyHash || fromKey === to) {
						continue; // don't send to me or backwards
					}

					const directStream = this.peers.get(to);
					if (directStream) {
						// always favor direct stream, even path seems longer
						const fanout = fanoutMap.get(to);
						if (!fanout) {
							fanoutMap.set(to, [to]);
						} else {
							fanout.push(to);
						}
						continue;
					} else {
						const fromMe = from.equals(this.components.peerId);
						const block = !fromMe ? fromKey : undefined;
						const path = this.routes.getPath(this.publicKeyHash, to, {
							block // prevent send message backwards
						});

						if (path && path.length > 0) {
							const fanout = fanoutMap.get(path[1]);
							if (!fanout) {
								fanoutMap.set(path[1], [to]);
								if (
									this.connectionManagerOptions.autoDial &&
									path.length >= 3
								) {
									await this.maybeConnectDirectly(path).catch((e) => {
										logger.error(
											"Failed to request direct connection: " + e.message
										);
									});
								}
								continue;
							} else {
								fanout.push(to);
								continue;
							}
						} else {
							missingPathsFor.push(to);
						}
					}

					// we can't find path, send message to all peers
					fanoutMap.clear();
					break;
				}

				// update to's
				let sentOnce = false;
				if (missingPathsFor.length === 0) {
					if (fanoutMap.size > 0) {
						for (const [neighbour, distantPeers] of fanoutMap) {
							message.to = distantPeers;
							const bytes = message.bytes();
							if (!sentOnce) {
								// if relayed = true, we have already added it to seenCache
								if (!relayed) {
									this.seenCache.add(await getMsgId(bytes));
								}
								sentOnce = true;
							}

							const stream = this.peers.get(neighbour);
							stream?.waitForWrite(bytes).catch((e) => {
								logger.error("Failed to publish message: " + e.message);
							});
						}
						return; // we are done sending the message in all direction with updates 'to' lists
					}
					return;
				}

				// else send to all (fallthrough to code below)
			}
		}

		// We fils to send the message directly, instead fallback to floodsub
		const peers: PeerStreams[] | PeerMap<PeerStreams> = to || this.peers;
		if (
			peers == null ||
			(Array.isArray(peers) && peers.length === 0) ||
			(peers instanceof Map && peers.size === 0)
		) {
			logger.debug("no peers are subscribed");
			return;
		}

		const bytes = message.bytes();
		let sentOnce = false;
		for (const stream of peers.values()) {
			const id = stream as PeerStreams;
			if (id.peerId.equals(from)) {
				continue;
			}

			if (!sentOnce) {
				sentOnce = true;
				if (!relayed) {
					// if relayed = true, we have already added it to seenCache
					const msgId = await getMsgId(bytes);
					this.seenCache.add(msgId);
				}
			}
			id.waitForWrite(bytes).catch((e) => {
				logger.error("Failed to publish message: " + e.message);
			});
		}

		if (!sentOnce && !relayed) {
			throw new Error("Message did not have any valid receivers. ");
		}
	}

	async maybeConnectDirectly(path: string[]) {
		if (path.length < 3) {
			return;
		}

		const toHash = path[path.length - 1];

		if (this.peers.has(toHash)) {
			return; // TODO, is this expected, or are we to dial more addresses?
		}

		// Try to either connect directly
		if (!this.recentDials.has(toHash)) {
			this.recentDials.add(toHash);
			const addrs = this.multiaddrsMap.get(toHash);
			try {
				if (addrs && addrs.length > 0) {
					await this.components.connectionManager.openConnection(
						addrs.map((x) => multiaddr(x))
					);
					return;
				}
			} catch (error) {
				// continue regardless of error
			}
		}

		// Connect through a closer relay that maybe does holepunch for us
		const nextToHash = path[path.length - 2];
		const routeKey = nextToHash + toHash;
		if (!this.recentDials.has(routeKey)) {
			this.recentDials.add(routeKey);
			const to = this.peerKeyHashToPublicKey.get(toHash)! as Ed25519PublicKey;
			const toPeerId = await to.toPeerId();
			const addrs = this.multiaddrsMap.get(path[path.length - 2]);
			if (addrs && addrs.length > 0) {
				const addressesToDial = addrs.sort((a, b) => {
					if (a.includes("/wss/")) {
						if (b.includes("/wss/")) {
							return 0;
						}
						return -1;
					}
					if (a.includes("/ws/")) {
						if (b.includes("/ws/")) {
							return 0;
						}
						if (b.includes("/wss/")) {
							return 1;
						}
						return -1;
					}
					return 0;
				});

				for (const addr of addressesToDial) {
					const circuitAddress = multiaddr(
						addr + "/p2p-circuit/webrtc/p2p/" + toPeerId.toString()
					);
					try {
						await this.components.connectionManager.openConnection(
							circuitAddress
						);
						return;
					} catch (error: any) {
						logger.error(
							"Failed to connect directly to: " +
								circuitAddress.toString() +
								". " +
								error?.message
						);
					}
				}
			}
		}
	}

	async waitFor(peer: PeerId | PublicSignKey) {
		const hash = (
			peer instanceof PublicSignKey ? peer : getPublicKeyFromPeerId(peer)
		).hashcode();
		try {
			await waitFor(() => {
				if (!this.peers.has(hash)) {
					return false;
				}
				if (!this.routes.hasLink(this.publicKeyHash, hash)) {
					return false;
				}

				return true;
			});
		} catch (error) {
			throw new Error(
				"Stream to " +
					hash +
					" does not exist. Connection exist: " +
					this.peers.has(hash) +
					". Route exist: " +
					this.routes.hasLink(this.publicKeyHash, hash)
			);
		}
		const stream = this.peers.get(hash)!;
		try {
			await waitFor(() => stream.isReadable && stream.isWritable);
		} catch (error) {
			throw new Error(
				"Stream to " +
					stream.publicKey.hashcode() +
					" not ready. Readable: " +
					stream.isReadable +
					". Writable " +
					stream.isWritable
			);
		}
	}
}

export const waitForPeers = async (
	...libs: {
		waitFor: (peer: PeerId | PublicSignKey) => Promise<void>;
		peerId: PeerId;
	}[]
) => {
	for (let i = 0; i < libs.length; i++) {
		for (let j = 0; j < libs.length; j++) {
			if (i === j) {
				continue;
			}
			await libs[i].waitFor(libs[j].peerId);
		}
	}
};
