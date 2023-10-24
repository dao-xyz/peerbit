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
import { PeerMap } from "./peer-map.js";
import type {
	IncomingStreamData,
	Registrar
} from "@libp2p/interface-internal/registrar";
import type { AddressManager } from "@libp2p/interface-internal/address-manager";
import type { ConnectionManager } from "@libp2p/interface-internal/connection-manager";

import { PeerStore } from "@libp2p/interface/peer-store";
import pDefer, { DeferredPromise } from "p-defer";

import { AbortError, delay, TimeoutError, waitFor } from "@peerbit/time";

import {
	getKeypairFromPeerId,
	getPublicKeyFromPeerId,
	PublicSignKey,
	sha256Base64,
	sha256Base64Sync,
	SignatureWithKey
} from "@peerbit/crypto";

import { multiaddr } from "@multiformats/multiaddr";
import { Components } from "libp2p/components";

export type SignaturePolicy = "StictSign" | "StrictNoSign";

import { logger } from "./logger.js";

export { logger };

import { Cache } from "@peerbit/cache";
import type { Libp2pEvents } from "@libp2p/interface";

import {
	PeerEvents,
	Message as Message,
	DataMessage,
	getMsgId,
	WaitForPeer,
	ACK,
	SeekDelivery,
	AcknowledgeDelivery,
	SilentDelivery,
	MessageHeader,
	Goodbye
} from "@peerbit/stream-interface";

import { DeliveryMode } from "@peerbit/stream-interface";
import { MultiAddrinfo } from "@peerbit/stream-interface";

export interface PeerStreamsInit {
	peerId: PeerId;
	publicKey: PublicSignKey;
	protocol: string;
	connId: string;
}
const DEFAULT_MESSAGE_REDUDANCY = 2;

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
	public seenCache: Cache<number>;
	private _registrarTopologyIds: string[] | undefined;
	private readonly maxInboundStreams?: number;
	private readonly maxOutboundStreams?: number;
	private connectionManagerOptions: ConnectionManagerOptions;
	private recentDials: Cache<string>;
	private traces: Cache<string>;
	private closeController: AbortController;

	private _ackCallbacks: Map<
		string,
		{
			promise: Promise<void>;
			callback: (ack: ACK, prev: PeerStreams, next?: PeerStreams) => void;
			timeout: ReturnType<typeof setTimeout>;
		}
	> = new Map();

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

		this.routes = new Routes(this.publicKeyHash);
		this.canRelayMessage = canRelayMessage;
		this.emitSelf = emitSelf;
		this.queue = new Queue({ concurrency: messageProcessingConcurrency });
		this.maxInboundStreams = maxInboundStreams;
		this.maxOutboundStreams = maxOutboundStreams;
		this.seenCache = new Cache({ max: 1e3, ttl: 10 * 60 * 1e3 });
		this.peerKeyHashToPublicKey = new Map();
		this._onIncomingStream = this._onIncomingStream.bind(this);
		this.onPeerConnected = this.onPeerConnected.bind(this);
		this.onPeerDisconnected = this.onPeerDisconnected.bind(this);
		this.signaturePolicy = signaturePolicy;
		this.connectionManagerOptions = connectionManager;
		this.recentDials = new Cache({
			ttl: connectionManager.retryDelay || 60 * 1000,
			max: 1e3
		});

		this.traces = new Cache({
			ttl: 10 * 1000,
			max: 1e6
		});
	}

	async start() {
		if (this.started) {
			return;
		}

		this.closeController = new AbortController();

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
			if (e.detail.multiplexer === "/webrtc") {
				this.onPeerConnected(e.detail.remotePeer, e.detail);
			}
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

		/* const pingJob = async () => {
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
		pingJob(); */
	}

	/**
	 * Unregister the pubsub protocol and the streams with other peers will be closed.
	 */
	async stop() {
		if (!this.started) {
			return;
		}
		this.started = false;
		this.closeController.abort();

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
		this.peers.clear();
		this.seenCache.clear();
		this.routes.clear();
		this.peerKeyHashToPublicKey.clear();
		for (const [k, v] of this._ackCallbacks) {
			clearTimeout(v.timeout);
		}
		this._ackCallbacks.clear();
		this.traces.clear();
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
		this.processMessages(peer.publicKey, inboundStream, peer).catch((err) => {
			logger.error(err);
		});
	}

	/**
	 * Registrar notifies an established connection with protocol
	 */
	public async onPeerConnected(
		peerId: PeerId,
		conn: Connection,
		properties?: { fromExisting?: boolean }
	) {
		if (conn.transient) {
			return;
		}

		if (!this.isStarted() || conn.status !== "open") {
			return;
		}

		try {
			// TODO remove/modify when https://github.com/libp2p/js-libp2p/issues/2036 is resolved

			const result = await waitFor(
				async () => {
					try {
						const hasProtocol = await this.components.peerStore
							.get(peerId)
							.then((x) =>
								this.multicodecs.find((y) => x.protocols.includes(y))
							);
						if (!hasProtocol) {
							return;
						}
					} catch (error: any) {
						if (error.code === "ERR_NOT_FOUND") {
							return;
						}
						throw error;
					}

					return true;
				},
				{
					timeout: 1e4,
					signal: this.closeController.signal
				}
			);
			if (!result) {
				return;
			}
		} catch (error) {
			return;
		}
		try {
			const peerKey = getPublicKeyFromPeerId(peerId);

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

			this.addRouteConnection(
				this.publicKeyHash,
				peerKey.hashcode(),
				peerKey,
				0,
				+new Date()
			);
		} catch (err: any) {
			logger.error(err);
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

			// Notify network
			const dependent = this.routes.getDependent(peerKeyHash);
			this.removeRouteConnection(peerKeyHash, true);

			if (dependent.length > 0) {
				await this.publishMessage(
					this.publicKey,
					await new Goodbye({
						leaving: [peerKeyHash],
						header: new MessageHeader({ to: dependent })
					}).sign(this.sign)
				);
			}
		}

		logger.debug("connection ended:" + peerKey.toString());
	}

	public removeRouteConnection(hash: string, neigbour: boolean) {
		const unreachable = neigbour
			? this.routes.removeNeighbour(hash)
			: this.routes.removeTarget(hash);
		for (const node of unreachable) {
			this.onPeerUnreachable(node); // TODO types
			this.peerKeyHashToPublicKey.delete(node);
		}
	}

	public addRouteConnection(
		from: string,
		neighbour: string,
		target: PublicSignKey,
		distance: number,
		session: number
	) {
		const targetHash = target.hashcode();
		const wasReachable =
			from === this.publicKeyHash
				? this.routes.isReachable(from, targetHash)
				: true;
		this.routes.add(from, neighbour, targetHash, distance, session);
		const newPeer =
			wasReachable === false && this.routes.isReachable(from, targetHash);
		if (newPeer) {
			this.peerKeyHashToPublicKey.set(target.hashcode(), target);
			this.onPeerReachable(target); // TODO types
		}
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
	public onPeerUnreachable(hash: string) {
		// override this fn

		const wasReachable = this.routes.isReachable(this.publicKeyHash, hash);
		if (wasReachable) {
			this.dispatchEvent(
				// TODO types
				new CustomEvent("peer:unreachable", {
					detail: this.peerKeyHashToPublicKey.get(hash)!
				})
			);
		}
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
		peerId: PublicSignKey,
		stream: AsyncIterable<Uint8ArrayList>,
		peerStreams: PeerStreams
	) {
		try {
			await pipe(stream, async (source) => {
				for await (const data of source) {
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
		from: PublicSignKey,
		peerStreams: PeerStreams,
		message: Uint8ArrayList
	): Promise<boolean> {
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

	private async modifySeenCache(message: Uint8Array) {
		const msgId = await getMsgId(message);
		const seen = this.seenCache.get(msgId);
		this.seenCache.add(msgId, seen ? seen + 1 : 1);
		return seen || 0;
	}

	/**
	 * Handles a message from a peer
	 */
	async processMessage(
		from: PublicSignKey,
		peerStream: PeerStreams,
		msg: Uint8ArrayList
	) {
		if (this.publicKey.equals(from) && !this.emitSelf) {
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
			await this._onDataMessage(from, peerStream, msg, message);
		} else {
			const seenBefore = await this.modifySeenCache(
				msg instanceof Uint8Array ? msg : msg.subarray()
			);

			if (seenBefore > 0) {
				logger.debug(
					"Received message already seen of type: " + message.constructor.name
				);
				return;
			}
			if (message instanceof ACK) {
				await this.onAck(from, peerStream, message);
			} else if (message instanceof Goodbye) {
				await this.onGoodBye(from, peerStream, message);
			} else {
				throw new Error("Unsupported message type");
			}
		}
	}

	public async onDataMessage(
		from: PublicSignKey,
		peerStream: PeerStreams,
		message: DataMessage,
		seenBefore: number
	) {
		let isForMe = false;
		const isFromSelf = this.publicKey.equals(from);
		if (!isFromSelf || this.emitSelf) {
			const isForAll = message.header.to.length === 0;
			isForMe =
				isForAll ||
				message.header.to.find((x) => x === this.publicKeyHash) != null;
		}

		if (isForMe) {
			if ((await this.maybeVerifyMessage(message)) === false) {
				// we don't verify messages we don't dispatch because of the performance penalty // TODO add opts for this
				logger.warn("Recieved message with invalid signature or timestamp");
				return false;
			}

			await this.acknowledgeMessage(peerStream, message, seenBefore);
			if (seenBefore === 0 && message.data) {
				this.dispatchEvent(
					new CustomEvent("data", {
						detail: message
					})
				);
			}
		}

		if (
			message.header.to.length === 1 &&
			message.header.to[0] === this.publicKeyHash
		) {
			// dont forward this message anymore because it was meant ONLY for me
			return true;
		}

		// Forward
		if (!seenBefore) {
			await this.relayMessage(from, message);
		}
		return true;
	}

	public async maybeVerifyMessage(message: DataMessage) {
		return (
			this.signaturePolicy !== "StictSign" ||
			message.verify(this.signaturePolicy === "StictSign")
		);
	}

	async acknowledgeMessage(
		peerStream: PeerStreams,
		message: DataMessage,
		seenBefore: number
	) {
		if (
			message.deliveryMode instanceof SeekDelivery ||
			message.deliveryMode instanceof AcknowledgeDelivery
		) {
			// Send ACK backwards
			await this.publishMessage(
				this.publicKey,
				await new ACK({
					messageIdToAcknowledge: message.id,
					seenCounter: seenBefore,

					// TODO only give origin info to peers we want to connect to us
					header: new MessageHeader({
						to: message.header.signatures!.publicKeys.map((x) => x.hashcode()),
						origin:
							message.deliveryMode instanceof SeekDelivery
								? new MultiAddrinfo(
										this.components.addressManager
											.getAddresses()
											.map((x) => x.toString())
								  )
								: undefined
					})
				}).sign(this.sign),
				[peerStream]
			);
		}
	}

	private async _onDataMessage(
		from: PublicSignKey,
		peerStream: PeerStreams,
		messageBytes: Uint8ArrayList | Uint8Array,
		message: DataMessage
	) {
		const seenBefore = await this.modifySeenCache(
			messageBytes instanceof Uint8ArrayList
				? messageBytes.subarray()
				: messageBytes
		);

		return this.onDataMessage(from, peerStream, message, seenBefore);
	}

	async onAck(publicKey: PublicSignKey, peerStream: PeerStreams, message: ACK) {
		if (!(await message.verify(true))) {
			logger.warn(`Recieved ACK message that did not verify`);
			return false;
		}

		if (this.publicKey.equals(publicKey)) {
			const q = 123;
		}

		const messageIdString = await sha256Base64(message.messageIdToAcknowledge);

		const next = this.traces.get(messageIdString);
		const nextStream = next ? this.peers.get(next) : undefined;

		this._ackCallbacks
			.get(messageIdString)
			?.callback(message, peerStream, nextStream);

		/* 	console.log("RECEIVED ACK", {
				me: this.publicKeyHash, from: publicKey.hashcode(), signer: message.header.signatures?.publicKeys[0].hashcode(), msgAckId: messageIdString, acc: !!
					this._ackCallbacks
						.get(messageIdString),
				next: nextStream,
				last: message.header.to.includes(this.publicKeyHash)
			},); */

		// relay ACK ?
		// send exactly backwards same route we got this message
		if (!message.header.to.includes(this.publicKeyHash)) {
			// if not end destination
			if (nextStream) {
				await this.publishMessage(this.publicKey, message, [nextStream], true);
			}
		} else {
			if (message.header.origin && this.connectionManagerOptions.autoDial) {
				this.maybeConnectDirectly(
					message.header.signatures!.publicKeys[0].hashcode(),
					message.header.origin
				);
			}
		}
	}

	async onGoodBye(
		publicKey: PublicSignKey,
		peerStream: PeerStreams,
		message: Goodbye
	) {
		if (!(await message.verify(true))) {
			logger.warn(`Recieved ACK message that did not verify`);
			return false;
		}

		const filteredLeaving = message.leaving.filter((x) =>
			this.routes.hasTarget(x)
		);
		if (filteredLeaving.length > 0) {
			this.publish(new Uint8Array(0), {
				to: filteredLeaving,
				mode: new SeekDelivery(2)
			}).catch((e) => {
				if (e instanceof TimeoutError || e instanceof AbortError) {
					// peer left or closed
				} else {
					throw e;
				}
			}); // this will remove the target if it is still not reable
		}

		for (const target of message.leaving) {
			// relay message to every one who previously talked to this peer
			const dependent = this.routes.getDependent(target);
			message.header.to = [...message.header.to, ...dependent];
			message.header.to = message.header.to.filter(
				(x) => x !== this.publicKeyHash
			);

			if (message.header.to.length > 0) {
				await this.publishMessage(publicKey, message, undefined, true);
			}
		}
	}

	async createMessage(
		data: Uint8Array | Uint8ArrayList | undefined,
		options: {
			to?: (string | PublicSignKey | PeerId)[] | Set<string>;
			mode?: DeliveryMode;
		}
	) {
		// dispatch the event if we are interested
		let toHashes: string[];
		let deliveryMode: DeliveryMode = options.mode || new SilentDelivery(1);
		if (options?.to) {
			if (options.to instanceof Set) {
				toHashes = new Array(options.to.size);
			} else {
				toHashes = new Array(options.to.length);
			}

			let i = 0;
			for (const to of options.to) {
				const hash =
					to instanceof PublicSignKey
						? to.hashcode()
						: typeof to === "string"
						? to
						: getPublicKeyFromPeerId(to).hashcode();

				if (
					deliveryMode instanceof SeekDelivery == false &&
					!this.routes.isReachable(this.publicKeyHash, hash)
				) {
					deliveryMode = new SeekDelivery(DEFAULT_MESSAGE_REDUDANCY);
				}

				toHashes[i++] = hash;
			}
		} else {
			deliveryMode = new SeekDelivery(DEFAULT_MESSAGE_REDUDANCY);
			toHashes = [];
		}
		const message = new DataMessage({
			data: data instanceof Uint8ArrayList ? data.subarray() : data,
			deliveryMode: deliveryMode,
			header: new MessageHeader({ to: toHashes })
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
		data: Uint8Array | Uint8ArrayList | undefined,
		options: {
			to?: (string | PublicSignKey | PeerId)[] | Set<string>;
			mode?: DeliveryMode;
		} = { mode: new SeekDelivery(DEFAULT_MESSAGE_REDUDANCY) }
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

		await this.publishMessage(this.publicKey, message, undefined);
		return message.id;
	}

	public async relayMessage(
		from: PublicSignKey,
		message: Message,
		to?: PeerStreams[] | PeerMap<PeerStreams>
	) {
		if (this.canRelayMessage) {
			if (message instanceof DataMessage) {
				/* if (message.deliveryMode instanceof AcknowledgeDelivery || message.deliveryMode instanceof SilentDelivery) {
					message.to = message.to.filter(
						(x) => !this.badRoutes.get(fromHash)?.has(x)
					);
					if (message.to.length === 0) {
						logger.debug(
							"Received a message to relay but canRelayMessage is false"
						);
						return;
					}
				} */

				if (
					message.deliveryMode instanceof AcknowledgeDelivery ||
					message.deliveryMode instanceof SeekDelivery
				) {
					const messageId = await sha256Base64(message.id);
					this.traces.add(messageId, from.hashcode());
				}
			}

			return this.publishMessage(from, message, to, true);
		} else {
			logger.debug("Received a message to relay but canRelayMessage is false");
		}
	}

	// for all tos if
	private resolveSendFanout(
		from: PublicSignKey,
		tos: string[],
		redundancy: number
	): Map<string, string[]> | undefined {
		if (tos.length === 0) {
			return undefined;
		}

		const fanoutMap = new Map<string, string[]>();

		const fromKey = from.hashcode();

		// Message to > 0
		if (tos.length > 0) {
			for (const to of tos) {
				if (to === this.publicKeyHash || fromKey === to) {
					continue; // don't send to me or backwards
				}

				const neighbour = this.routes.findNeighbor(fromKey, to);
				if (neighbour) {
					let foundClosest = false;
					for (
						let i = 0;
						i < Math.min(neighbour.list.length, redundancy);
						i++
					) {
						const distance = neighbour.list[i].distance;
						if (distance >= redundancy) {
							break; // because neighbour listis sorted
						}
						if (distance <= 0) {
							foundClosest = true;
						}
						const fanout = fanoutMap.get(neighbour.list[i].hash);
						if (!fanout) {
							fanoutMap.set(neighbour.list[i].hash, [to]);
						} else {
							fanout.push(to);
						}
					}
					if (!foundClosest && from.equals(this.publicKey)) {
						return undefined; // we dont have the shortest path to our target (yet). Send to all
					}

					continue;
				}

				// we can't find path, send message to all peers
				return undefined;
			}
		}
		return fanoutMap;
	}

	public async publishMessage(
		from: PublicSignKey,
		message: Message,
		to?: PeerStreams[] | PeerMap<PeerStreams>,
		relayed?: boolean
	): Promise<void> {
		let deliveryDeferredPromiseFn: (() => DeferredPromise<void>) | undefined =
			undefined;

		/**
		 * Logic for handling acknowledge messages when we receive them (later)
		 */

		if (
			message instanceof DataMessage &&
			message.deliveryMode instanceof SeekDelivery
		) {
			to = this.peers; // seek delivery will not work unless we try all possible paths
		}

		if (
			message instanceof DataMessage &&
			(message.deliveryMode instanceof SeekDelivery ||
				message.deliveryMode instanceof AcknowledgeDelivery)
		) {
			const idString = await sha256Base64(message.id);
			const allAckS: ACK[] = [];
			deliveryDeferredPromiseFn = () => {
				const deliveryDeferredPromise = pDefer<void>();
				const fastestNodesReached = new Map<string, ACK[]>();
				const messageToSet: Set<string> = new Set(message.header.to);
				const willGetAllAcknowledgements = !relayed; // Only the origin will get all acks

				// Expected to receive at least 'filterMessageForSeenCounter' acknowledgements from each peer
				const filterMessageForSeenCounter = relayed
					? undefined
					: message.deliveryMode instanceof SeekDelivery
					? Math.min(this.peers.size, message.deliveryMode.redundancy)
					: 1; /*  message.deliveryMode instanceof SeekDelivery ? Math.min(this.peers.size - (relayed ? 1 : 0), message.deliveryMode.redundancy) : 1 */

				const timeout = setTimeout(async () => {
					let hasAll = true;
					this._ackCallbacks.delete(idString);

					// peer not reachable (?)!
					for (const to of message.header.to) {
						let foundNode = false;

						if (fastestNodesReached.has(to)) {
							foundNode = true;
							break;
						}

						if (!foundNode && !relayed) {
							// TODO types
							/* console.log("DID NOT FIND PATH TO", filterMessageForSeenCounter, this.publicKeyHash, to, [...this.peers.values()].map(x => x.publicKey.hashcode()), idString);
							 */
							this.removeRouteConnection(to, false);
							hasAll = false;
						}
					}

					if (!hasAll && willGetAllAcknowledgements) {
						deliveryDeferredPromise.reject(
							new TimeoutError(
								`Failed to get message delivery acknowledges from all nodes (${fastestNodesReached.size}/${message.header.to.length})`
							)
						);
					} else {
						deliveryDeferredPromise.resolve();
					}
				}, 5e3);

				const uniqueAcks = new Set();
				const session = +new Date();
				this._ackCallbacks.set(idString, {
					promise: deliveryDeferredPromise.promise,
					callback: (ack, neighbour, backPeer) => {
						allAckS.push(ack);

						// TODO types
						const target = ack.header.signatures!.publicKeys[0];
						const targetHash = target.hashcode();

						// if the target is not inside the original message to, we still ad the target to our routes
						// this because a relay might modify the 'to' list and we might receive more answers than initially set
						if (message.deliveryMode instanceof SeekDelivery) {
							this.addRouteConnection(
								backPeer?.publicKey.hashcode() || this.publicKeyHash,
								neighbour.publicKey.hashcode(),
								target,
								ack.seenCounter,
								session
							); // we assume the seenCounter = distance. The more the message has been seen by the target the longer the path is to the target
						}

						if (messageToSet.has(targetHash)) {
							// Only keep track of relevant acks
							if (
								filterMessageForSeenCounter == null ||
								ack.seenCounter <= filterMessageForSeenCounter - 1
							) {
								let arr = fastestNodesReached.get(targetHash);
								if (!arr) {
									arr = [];
									fastestNodesReached.set(targetHash, arr);
								}
								arr.push(ack);
								uniqueAcks.add(targetHash + ack.seenCounter);
							}
						}

						if (
							filterMessageForSeenCounter != null
								? uniqueAcks.size >=
								  messageToSet.size * filterMessageForSeenCounter
								: messageToSet.size === fastestNodesReached.size
						) {
							deliveryDeferredPromise?.resolve();

							if (messageToSet.size > 0) {
								// this statement exist beacuse if we do SEEK and have to = [], then it means we try to reach as many as possible hence we never want to delete this ACK callback
								this._ackCallbacks.delete(idString);
								clearTimeout(timeout);
								// only remove callback function if we actually expected a finite amount of responses
							}
						}
					},
					timeout
				});
				return deliveryDeferredPromise;
			};
		}

		const bytes = message.bytes();
		if (!relayed) {
			const bytesArray = bytes instanceof Uint8Array ? bytes : bytes.subarray();
			await this.modifySeenCache(bytesArray);
		}

		/**
		 * For non SEEKing message delivery modes, use routing
		 */
		if (
			message instanceof DataMessage &&
			(message.deliveryMode instanceof AcknowledgeDelivery ||
				message.deliveryMode instanceof SilentDelivery) &&
			!to
		) {
			const fanout = this.resolveSendFanout(
				from,
				message.header.to,
				message.deliveryMode.redundancy
			);

			// update to's
			let sentOnce = false;

			if (fanout) {
				if (fanout.size > 0) {
					const promise = deliveryDeferredPromiseFn?.();
					for (const [neighbour, _distantPeers] of fanout) {
						if (!sentOnce) {
							// if relayed = true, we have already added it to seenCache
							sentOnce = true;
						}

						const stream = this.peers.get(neighbour);

						stream?.waitForWrite(bytes).catch((e) => {
							logger.error("Failed to publish message: " + e.message);
						});
					}
					return promise?.promise; // we are done sending the message in all direction with updates 'to' lists
				}
				if (from.equals(this.publicKey)) {
					console.trace(
						"NO DELIVERY FOR TO",
						message.header.to,
						message.deliveryMode,
						this.routes.routes
							.get(this.publicKeyHash)
							?.get(message.header.to[0]),
						this.routes.isReachable(this.publicKeyHash, message.header.to[0])
					);
				}
				return; // we defintely that we should not forward the message anywhere
			}

			// else send to all (fallthrough to code below)
		}

		// We fils to send the message directly, instead fallback to floodsub
		const peers: PeerStreams[] | PeerMap<PeerStreams> = to || this.peers;
		if (
			peers == null ||
			(Array.isArray(peers) && peers.length === 0) ||
			(peers instanceof Map && peers.size === 0)
		) {
			logger.debug("No peers to send to");
			return;
		}

		let sentOnce = false;
		let promise: Promise<void> | undefined;
		for (const stream of peers.values()) {
			const id = stream as PeerStreams;
			if (id.publicKey.equals(from)) {
				continue;
			}

			if (!sentOnce) {
				sentOnce = true;
				promise = deliveryDeferredPromiseFn?.()?.promise;
			}

			id.waitForWrite(bytes).catch((e) => {
				logger.error("Failed to publish message: " + e.message);
			});
		}

		if (!sentOnce) {
			if (!relayed) {
				throw new Error("Message did not have any valid receivers");
			}
		}
		return promise;
	}

	async maybeConnectDirectly(toHash: string, origin: MultiAddrinfo) {
		if (this.peers.has(toHash)) {
			return; // TODO, is this expected, or are we to dial more addresses?
		}

		const addresses = origin.multiaddrs
			.filter((x) => {
				const ret = !this.recentDials.has(x);
				this.recentDials.add(x);
				return ret;
			})
			.map((x) => multiaddr(x));
		if (addresses.length > 0) {
			try {
				await this.components.connectionManager.openConnection(addresses);
			} catch (error: any) {
				logger.info(
					"Failed to connect directly to: " +
						JSON.stringify(addresses.map((x) => x.toString())) +
						". " +
						error?.message
				);
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
				if (!this.routes.isReachable(this.publicKeyHash, hash)) {
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
					this.routes.isReachable(this.publicKeyHash, hash)
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

	get pending(): boolean {
		return this._ackCallbacks.size > 0;
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
