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
import { MAX_ROUTE_DISTANCE, Routes } from "./routes.js";
import type {
	IncomingStreamData,
	Registrar
} from "@libp2p/interface-internal/registrar";
import type { AddressManager } from "@libp2p/interface-internal/address-manager";
import type { ConnectionManager } from "@libp2p/interface-internal/connection-manager";

import { PeerStore } from "@libp2p/interface/peer-store";
import pDefer from "p-defer";

import { AbortError, delay, TimeoutError, waitFor } from "@peerbit/time";

import {
	getKeypairFromPeerId,
	getPublicKeyFromPeerId,
	PublicSignKey,
	sha256Base64,
	SignatureWithKey,
	toBase64
} from "@peerbit/crypto";

import { multiaddr } from "@multiformats/multiaddr";
import { Components } from "libp2p/components";
import type { TypedEventTarget } from "@libp2p/interface/events";

export type SignaturePolicy = "StictSign" | "StrictNoSign";

import { logger } from "./logger.js";

export { logger };

import { Cache } from "@peerbit/cache";
import type { Libp2pEvents } from "@libp2p/interface";
import { ready } from "@peerbit/crypto";
import {
	Message as Message,
	DataMessage,
	getMsgId,
	WaitForPeer,
	ACK,
	SeekDelivery,
	AcknowledgeDelivery,
	SilentDelivery,
	MessageHeader,
	Goodbye,
	StreamEvents,
	TracedDelivery,
	AnyWhere
} from "@peerbit/stream-interface";

import { MultiAddrinfo } from "@peerbit/stream-interface";
import { MovingAverageTracker } from "./metrics.js";

const logError = (e?: { message: string }) => {
	return logger.error(e?.message);
};
export interface PeerStreamsInit {
	peerId: PeerId;
	publicKey: PublicSignKey;
	protocol: string;
	connId: string;
}
const DEFAULT_SEEK_MESSAGE_REDUDANCY = 2;
const DEFAULT_SILENT_MESSAGE_REDUDANCY = 1;

const isWebsocketConnection = (c: Connection) =>
	c.remoteAddr.protoNames().find((x) => x === "ws" || x === "wss");

export interface PeerStreamEvents {
	"stream:inbound": CustomEvent<never>;
	"stream:outbound": CustomEvent<never>;
	close: CustomEvent<never>;
}

const SEEK_DELIVERY_TIMEOUT = 15e3;
const MAX_DATA_LENGTH = 1e7 + 1000; // 10 mb and some metadata
const MAX_QUEUED_BYTES = MAX_DATA_LENGTH * 50;

const DEFAULT_PRUNE_CONNECTIONS_INTERVAL = 2e4;
const DEFAULT_MIN_CONNECTIONS = 2;
const DEFAULT_MAX_CONNECTIONS = 300;

const DEFAULT_PRUNED_CONNNECTIONS_TIMEOUT = 30 * 1000;

const ROUTE_UPDATE_DELAY_FACTOR = 1e4;

type WithTo = {
	to?: (string | PublicSignKey | PeerId)[] | Set<string>;
};

type WithMode = {
	mode?: SilentDelivery | SeekDelivery | AcknowledgeDelivery | AnyWhere;
};
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

	public seekedOnce: boolean;

	private usedBandWidthTracker: MovingAverageTracker;
	constructor(init: PeerStreamsInit) {
		super();

		this.peerId = init.peerId;
		this.publicKey = init.publicKey;
		this.protocol = init.protocol;
		this.inboundAbortController = new AbortController();
		this.closed = false;
		this.connId = init.connId;
		this.counter = 1;
		this.usedBandWidthTracker = new MovingAverageTracker();
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

	get usedBandwidth() {
		return this.usedBandWidthTracker.value;
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

		this.usedBandWidthTracker.add(data.byteLength);

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
				lp.decode(source, { maxDataLength: MAX_DATA_LENGTH })
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
		).catch(logError);

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

		this.dispatchEvent(new CustomEvent("close"));
		this._rawOutboundStream = undefined;
		this.outboundStream = undefined;

		this._rawInboundStream = undefined;
		this.inboundStream = undefined;
	}
}

type DialerOptions = {
	retryDelay: number;
};
type PrunerOptions = {
	interval: number; // how often to check for pruning
	bandwidth?: number; // Mbps, unlimited if unset
	maxBuffer?: number; // max queued bytes until pruning
	connectionTimeout: number; // how long a pruned connection should be treated as non wanted
};
type ConnectionManagerOptions = {
	minConnections: number;
	maxConnections: number;
	dialer?: DialerOptions;
	pruner?: PrunerOptions;
};

export type DirectStreamOptions = {
	canRelayMessage?: boolean;
	messageProcessingConcurrency?: number;
	maxInboundStreams?: number;
	maxOutboundStreams?: number;
	signaturePolicy?: SignaturePolicy;
	connectionManager?: ConnectionManagerArguments;
	routeSeekInterval?: number;
	seekTimeout?: number;
};

export interface DirectStreamComponents extends Components {
	peerId: PeerId;
	addressManager: AddressManager;
	registrar: Registrar;
	connectionManager: ConnectionManager;
	peerStore: PeerStore;
	events: TypedEventTarget<Libp2pEvents>;
}

export type ConnectionManagerArguments =
	| (Partial<Pick<ConnectionManagerOptions, "minConnections">> &
			Partial<Pick<ConnectionManagerOptions, "maxConnections">> & {
				pruner?: Partial<PrunerOptions> | false;
			} & { dialer?: Partial<DialerOptions> | false })
	| false;

export abstract class DirectStream<
		Events extends { [s: string]: any } = StreamEvents
	>
	extends EventEmitter<Events>
	implements WaitForPeer
{
	public peerId: PeerId;
	public publicKey: PublicSignKey;
	public publicKeyHash: string;
	public sign: (bytes: Uint8Array) => Promise<SignatureWithKey>;

	public started: boolean;

	/**
	 * Map of peer streams
	 */
	public peers: Map<string, PeerStreams>;
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
	public queue: Queue;
	public multicodecs: string[];
	public seenCache: Cache<number>;
	private _registrarTopologyIds: string[] | undefined;
	private readonly maxInboundStreams?: number;
	private readonly maxOutboundStreams?: number;
	connectionManagerOptions: ConnectionManagerOptions;
	private recentDials?: Cache<string>;
	private healthChecks: Map<string, ReturnType<typeof setTimeout>>;
	private pruneConnectionsTimeout: ReturnType<typeof setInterval>;
	private prunedConnectionsCache?: Cache<string>;
	routeSeekInterval: number;
	seekTimeout: number;
	closeController: AbortController;
	private _ackCallbacks: Map<
		string,
		{
			promise: Promise<void>;
			callback: (
				messageTarget: PublicSignKey,
				seenCounter: number,
				messageThrough: PeerStreams,
				messageFrom?: PeerStreams
			) => void;
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
			messageProcessingConcurrency = 10,
			maxInboundStreams,
			maxOutboundStreams,
			signaturePolicy = "StictSign",
			connectionManager,
			routeSeekInterval = ROUTE_UPDATE_DELAY_FACTOR,
			seekTimeout = SEEK_DELIVERY_TIMEOUT
		} = options || {};

		const signKey = getKeypairFromPeerId(components.peerId);
		this.seekTimeout = seekTimeout;
		this.sign = signKey.sign.bind(signKey);
		this.peerId = components.peerId;
		this.publicKey = signKey.publicKey;
		this.publicKeyHash = signKey.publicKey.hashcode();
		this.multicodecs = multicodecs;
		this.started = false;
		this.peers = new Map<string, PeerStreams>();
		this.routes = new Routes(this.publicKeyHash);
		this.canRelayMessage = canRelayMessage;
		this.healthChecks = new Map();
		this.routeSeekInterval = routeSeekInterval;
		this.queue = new Queue({ concurrency: messageProcessingConcurrency });
		this.maxInboundStreams = maxInboundStreams;
		this.maxOutboundStreams = maxOutboundStreams;
		this.seenCache = new Cache({ max: 1e6, ttl: 10 * 60 * 1e3 });

		this.peerKeyHashToPublicKey = new Map();
		this._onIncomingStream = this._onIncomingStream.bind(this);
		this.onPeerConnected = this.onPeerConnected.bind(this);
		this.onPeerDisconnected = this.onPeerDisconnected.bind(this);
		this.signaturePolicy = signaturePolicy;

		if (connectionManager === false || connectionManager === null) {
			this.connectionManagerOptions = {
				maxConnections: Number.MAX_SAFE_INTEGER,
				minConnections: 0,
				dialer: undefined,
				pruner: undefined
			};
		} else {
			this.connectionManagerOptions = {
				maxConnections: DEFAULT_MAX_CONNECTIONS,
				minConnections: DEFAULT_MIN_CONNECTIONS,
				...connectionManager,
				dialer:
					connectionManager?.dialer !== false &&
					connectionManager?.dialer !== null
						? { retryDelay: 60 * 1000, ...connectionManager?.dialer }
						: undefined,
				pruner:
					connectionManager?.pruner !== false &&
					connectionManager?.pruner !== null
						? {
								connectionTimeout: DEFAULT_PRUNED_CONNNECTIONS_TIMEOUT,
								interval: DEFAULT_PRUNE_CONNECTIONS_INTERVAL,
								maxBuffer: MAX_QUEUED_BYTES,
								...connectionManager?.pruner
						  }
						: undefined
			};
		}

		this.recentDials = this.connectionManagerOptions.dialer
			? new Cache({
					ttl: this.connectionManagerOptions.dialer.retryDelay,
					max: 1e3
			  })
			: undefined;

		this.prunedConnectionsCache = this.connectionManagerOptions.pruner
			? new Cache({
					max: 1e6,
					ttl: this.connectionManagerOptions.pruner.connectionTimeout
			  })
			: undefined;
	}

	async start() {
		if (this.started) {
			return;
		}

		await ready;

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
		if (this.connectionManagerOptions.pruner) {
			const pruneConnectionsLoop = () => {
				this.pruneConnectionsTimeout = setTimeout(() => {
					this.maybePruneConnections().finally(() => {
						if (!this.started) {
							return;
						}
						pruneConnectionsLoop();
					});
				}, this.connectionManagerOptions.pruner!.interval);
			};
			pruneConnectionsLoop();
		}
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

		clearTimeout(this.pruneConnectionsTimeout);

		await Promise.all(
			this.multicodecs.map((x) => this.components.registrar.unhandle(x))
		);

		logger.debug("stopping");
		for (const peerStreams of this.peers.values()) {
			await peerStreams.close();
		}
		for (const [k, v] of this.healthChecks) {
			clearTimeout(v);
		}
		this.healthChecks.clear();
		this.prunedConnectionsCache?.clear();

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

		if (this.prunedConnectionsCache?.has(publicKey.hashcode())) {
			await connection.close();
			return;
		}

		const peer = this.addPeer(
			peerId,
			publicKey,
			stream.protocol,
			connection.id
		);
		const inboundStream = peer.attachInboundStream(stream);
		this.processMessages(peer.publicKey, inboundStream, peer).catch(logError);
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
		const peerKey = getPublicKeyFromPeerId(peerId);

		if (this.prunedConnectionsCache?.has(peerKey.hashcode())) {
			return; // we recently pruned this connect, dont allow it to connect for a while
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
						header: new MessageHeader({
							mode: new SilentDelivery({ to: dependent, redundancy: 2 })
						})
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
		session: number,
		pending = false
	) {
		const targetHash = typeof target === "string" ? target : target.hashcode();
		const wasReachable =
			from === this.publicKeyHash
				? this.routes.isReachable(from, targetHash)
				: true;
		if (pending) {
			this.routes.addPendingRouteConnection(session, {
				distance,
				from,
				neighbour,
				target: targetHash
			});
		} else {
			this.routes.add(from, neighbour, targetHash, distance, session);
		}

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

		this.dispatchEvent(
			// TODO types
			new CustomEvent("peer:unreachable", {
				detail: this.peerKeyHashToPublicKey.get(hash)!
			})
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
		peerId: PublicSignKey,
		stream: AsyncIterable<Uint8ArrayList>,
		peerStreams: PeerStreams
	) {
		try {
			await pipe(stream, async (source) => {
				for await (const data of source) {
					this.processRpc(peerId, peerStreams, data).catch(logError);
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
				.catch(logError);
		}

		return true;
	}

	private async modifySeenCache(
		message: Uint8Array,
		getIdFn: (bytes: Uint8Array) => Promise<string> = getMsgId
	) {
		const msgId = await getIdFn(message);
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
		// Ensure the message is valid before processing it
		const message: Message | undefined = Message.from(msg);
		this.dispatchEvent(
			new CustomEvent("message", {
				detail: message
			})
		);

		if (message instanceof DataMessage) {
			// DONT await this since it might introduce a dead-lock
			this._onDataMessage(from, peerStream, msg, message).catch(logError);
		} else {
			if (message instanceof ACK) {
				this.onAck(from, peerStream, msg, message).catch(logError);
			} else if (message instanceof Goodbye) {
				this.onGoodBye(from, peerStream, msg, message).catch(logError);
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
		const alreadySeen =
			message instanceof SeekDelivery &&
			message.header.signatures?.publicKeys.find((x) =>
				x.equals(this.publicKey)
			);
		if (alreadySeen) {
			return;
		}

		let isForMe = false;
		if (message.header.mode instanceof AnyWhere) {
			isForMe = true;
		} else {
			const isFromSelf = this.publicKey.equals(from);
			if (!isFromSelf) {
				isForMe =
					message.header.mode.to == null ||
					message.header.mode.to.find((x) => x === this.publicKeyHash) != null;
			}
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
			message.header.mode instanceof SilentDelivery ||
			message.header.mode instanceof AcknowledgeDelivery
		) {
			if (
				message.header.mode.to &&
				message.header.mode.to.length === 1 &&
				message.header.mode.to[0] === this.publicKeyHash
			) {
				// dont forward this message anymore because it was meant ONLY for me
				return true;
			}
		}

		// Forward
		if (
			message.header.mode instanceof AnyWhere ||
			message.header.mode instanceof SeekDelivery ||
			seenBefore === 0
		) {
			// DONT await this since it might introduce a dead-lock
			if (message.header.mode instanceof SeekDelivery) {
				if (seenBefore < message.header.mode.redundancy) {
					const to = [...this.peers.values()].filter(
						(x) =>
							!message.header.signatures?.publicKeys.find((y) =>
								y.equals(x.publicKey)
							) && x != peerStream
					);
					if (to.length > 0) {
						this.relayMessage(from, message, to);
					}
				}
			} else {
				this.relayMessage(from, message);
			}
		}
	}

	public async maybeVerifyMessage(message: DataMessage) {
		return message.verify(this.signaturePolicy === "StictSign");
	}

	async acknowledgeMessage(
		peerStream: PeerStreams,
		message: DataMessage,
		seenBefore: number
	) {
		if (
			(message.header.mode instanceof SeekDelivery ||
				message.header.mode instanceof AcknowledgeDelivery) &&
			seenBefore < message.header.mode.redundancy
		) {
			await this.publishMessage(
				this.publicKey,
				await new ACK({
					messageIdToAcknowledge: message.id,
					seenCounter: seenBefore,

					// TODO only give origin info to peers we want to connect to us
					header: new MessageHeader({
						mode: new TracedDelivery(
							message.header.signatures!.publicKeys.map((x) => x.hashcode())
						),

						// include our origin if message is SeekDelivery and we have not recently pruned a connection to this peer
						origin:
							message.header.mode instanceof SeekDelivery &&
							!message.header.signatures!.publicKeys.find(
								(x) => this.prunedConnectionsCache?.has(x.hashcode())
							)
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

	async onAck(
		publicKey: PublicSignKey,
		peerStream: PeerStreams,
		messageBytes: Uint8ArrayList | Uint8Array,
		message: ACK
	) {
		const seenBefore = await this.modifySeenCache(
			messageBytes instanceof Uint8Array
				? messageBytes
				: messageBytes.subarray(),
			(bytes) => sha256Base64(bytes)
		);

		if (seenBefore > 1) {
			logger.debug(
				"Received message already seen of type: " + message.constructor.name
			);
			return false;
		}

		if (!(await message.verify(true))) {
			logger.warn(`Recieved ACK message that did not verify`);
			return false;
		}

		const messageIdString = toBase64(message.messageIdToAcknowledge);
		const myIndex = message.header.mode.trace.findIndex(
			(x) => x === this.publicKeyHash
		);
		const next = message.header.mode.trace[myIndex - 1];
		const nextStream = next ? this.peers.get(next) : undefined;

		this._ackCallbacks
			.get(messageIdString)
			?.callback(
				message.header.signatures!.publicKeys[0],
				message.seenCounter,
				peerStream,
				nextStream
			);

		// relay ACK ?
		// send exactly backwards same route we got this message
		if (nextStream) {
			await this.publishMessage(this.publicKey, message, [nextStream], true);
		} else {
			// if origin exist (we can connect to remote peer) && we have autodialer turned on
			if (message.header.origin && this.connectionManagerOptions.dialer) {
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
		messageBytes: Uint8ArrayList | Uint8Array,
		message: Goodbye
	) {
		const seenBefore = await this.modifySeenCache(
			messageBytes instanceof Uint8Array
				? messageBytes
				: messageBytes.subarray()
		);

		if (seenBefore > 0) {
			logger.debug(
				"Received message already seen of type: " + message.constructor.name
			);
			return;
		}

		if (!(await message.verify(true))) {
			logger.warn(`Recieved ACK message that did not verify`);
			return false;
		}

		const filteredLeaving = message.leaving.filter((x) =>
			this.routes.hasTarget(x)
		);

		if (filteredLeaving.length > 0) {
			this.publish(new Uint8Array(0), {
				mode: new SeekDelivery({
					to: filteredLeaving,
					redundancy: DEFAULT_SEEK_MESSAGE_REDUDANCY
				})
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
			message.header.mode.to = [...message.header.mode.to, ...dependent];
			message.header.mode.to = message.header.mode.to.filter(
				(x) => x !== this.publicKeyHash
			);

			if (message.header.mode.to.length > 0) {
				await this.publishMessage(publicKey, message, undefined, true);
			}
		}
	}

	async createMessage(
		data: Uint8Array | Uint8ArrayList | undefined,
		options: WithTo | WithMode
	) {
		// dispatch the event if we are interested

		let mode: SilentDelivery | SeekDelivery | AcknowledgeDelivery | AnyWhere = (
			options as WithMode
		).mode
			? (options as WithMode).mode!
			: new SilentDelivery({
					to: (options as WithTo).to!,
					redundancy: DEFAULT_SILENT_MESSAGE_REDUDANCY
			  });

		if (mode instanceof AcknowledgeDelivery || mode instanceof SilentDelivery) {
			const now = +new Date();
			for (const hash of mode.to) {
				const neighbourRoutes = this.routes.routes
					.get(this.publicKeyHash)
					?.get(hash);
				if (
					!neighbourRoutes ||
					now - neighbourRoutes.session >
						neighbourRoutes.list.length * this.routeSeekInterval
				) {
					mode = new SeekDelivery({
						to: mode.to,
						redundancy: DEFAULT_SEEK_MESSAGE_REDUDANCY
					});
					break;
				}
			}
		}

		if (
			mode instanceof AcknowledgeDelivery ||
			mode instanceof SilentDelivery ||
			mode instanceof SeekDelivery
		) {
			if (mode.to?.find((x) => x === this.publicKeyHash)) {
				mode.to = mode.to.filter((x) => x !== this.publicKeyHash);
			}
		}

		const message = new DataMessage({
			data: data instanceof Uint8ArrayList ? data.subarray() : data,
			header: new MessageHeader({ mode })
		});

		if (
			this.signaturePolicy === "StictSign" ||
			mode instanceof SeekDelivery ||
			mode instanceof AcknowledgeDelivery
		) {
			await message.sign(this.sign);
		}

		return message;
	}
	/**
	 * Publishes messages to all peers
	 */
	async publish(
		data: Uint8Array | Uint8ArrayList | undefined,
		options: WithMode | WithTo = {
			mode: new SeekDelivery({ redundancy: DEFAULT_SEEK_MESSAGE_REDUDANCY })
		}
	): Promise<Uint8Array> {
		if (!this.started) {
			throw new Error("Not started");
		}

		const message = await this.createMessage(data, options);

		await this.publishMessage(this.publicKey, message, undefined);
		return message.id;
	}

	public async relayMessage(
		from: PublicSignKey,
		message: Message,
		to?: PeerStreams[] | Map<string, PeerStreams>
	) {
		if (this.canRelayMessage) {
			if (message instanceof DataMessage) {
				if (
					message.header.mode instanceof AcknowledgeDelivery ||
					message.header.mode instanceof SeekDelivery
				) {
					await message.sign(this.sign);
				}
			}

			return this.publishMessage(from, message, to, true);
		} else {
			logger.debug("Received a message to relay but canRelayMessage is false");
		}
	}

	private async createDeliveryPromise(
		from: PublicSignKey,
		message: DataMessage,
		relayed?: boolean
	): Promise<{ promise: Promise<void> }> {
		if (message.header.mode instanceof AnyWhere) {
			return { promise: Promise.resolve() };
		}
		const idString = toBase64(message.id);

		const existing = this._ackCallbacks.get(idString);
		if (existing) {
			return { promise: existing.promise };
		}

		const deliveryDeferredPromise = pDefer<void>();
		const fastestNodesReached = new Map<string, number[]>();
		const messageToSet: Set<string> = new Set();
		if (message.header.mode.to) {
			for (const to of message.header.mode.to) {
				if (to === from.hashcode()) {
					continue;
				}
				messageToSet.add(to);

				if (!relayed && !this.healthChecks.has(to)) {
					this.healthChecks.set(
						to,
						setTimeout(() => {
							this.removeRouteConnection(to, false);
						}, this.seekTimeout + 5000)
					);
				}
			}
		}

		if (messageToSet.size === 0) {
			deliveryDeferredPromise.resolve(); // we dont know how many answer to expect, just resolve immediately
		}

		const willGetAllAcknowledgements = !relayed; // Only the origin will get all acks

		// Expected to receive at least 'filterMessageForSeenCounter' acknowledgements from each peer
		const filterMessageForSeenCounter = relayed
			? undefined
			: message.header.mode instanceof SeekDelivery
			? Math.min(this.peers.size, message.header.mode.redundancy)
			: 1; /*  message.deliveryMode instanceof SeekDelivery ? Math.min(this.peers.size - (relayed ? 1 : 0), message.deliveryMode.redundancy) : 1 */

		const finalize = () => {
			this._ackCallbacks.delete(idString);
			if (message.header.mode instanceof SeekDelivery) {
				this.routes.commitPendingRouteConnection(session);
			}
		};

		const uniqueAcks = new Set();
		const session = +new Date();
		const timeout = setTimeout(async () => {
			let hasAll = true;
			finalize();

			// peer not reachable (?)!
			for (const to of messageToSet) {
				let foundNode = false;

				if (fastestNodesReached.has(to)) {
					foundNode = true;
					break;
				}

				if (!foundNode && !relayed) {
					hasAll = false;
				}
			}

			if (!hasAll && willGetAllAcknowledgements) {
				deliveryDeferredPromise.reject(
					new TimeoutError(
						`${
							this.publicKeyHash
						} Failed to get message ${idString} ${filterMessageForSeenCounter} ${[
							...messageToSet
						]} delivery acknowledges from all nodes (${
							fastestNodesReached.size
						}/${messageToSet.size})`
					)
				);
			} else {
				deliveryDeferredPromise.resolve();
			}
		}, this.seekTimeout);

		this._ackCallbacks.set(idString, {
			promise: deliveryDeferredPromise.promise,
			callback: (messageTarget, seenCounter, messageThrough, messageFrom) => {
				const messageTargetHash = messageTarget.hashcode();
				// remove the automatic removal of route timeout since we have observed lifesigns of a peer
				const timer = this.healthChecks.get(messageTargetHash);
				clearTimeout(timer);
				this.healthChecks.delete(messageTargetHash);

				// if the target is not inside the original message to, we still ad the target to our routes
				// this because a relay might modify the 'to' list and we might receive more answers than initially set
				if (message.header.mode instanceof SeekDelivery) {
					this.addRouteConnection(
						messageFrom?.publicKey.hashcode() || this.publicKeyHash,
						messageThrough.publicKey.hashcode(),
						messageTarget,
						seenCounter,
						session,
						true
					); // we assume the seenCounter = distance. The more the message has been seen by the target the longer the path is to the target
				}

				if (messageToSet.has(messageTargetHash)) {
					// Only keep track of relevant acks
					if (
						filterMessageForSeenCounter == null ||
						seenCounter < filterMessageForSeenCounter
					) {
						// TODO set limit correctly
						if (seenCounter < MAX_ROUTE_DISTANCE) {
							let arr = fastestNodesReached.get(messageTargetHash);
							if (!arr) {
								arr = [];
								fastestNodesReached.set(messageTargetHash, arr);
							}
							arr.push(seenCounter);

							uniqueAcks.add(messageTargetHash + seenCounter);
						}
					}
				}

				if (
					filterMessageForSeenCounter != null
						? uniqueAcks.size >= messageToSet.size * filterMessageForSeenCounter
						: messageToSet.size === fastestNodesReached.size
				) {
					if (messageToSet.size > 0) {
						// this statement exist beacuse if we do SEEK and have to = [], then it means we try to reach as many as possible hence we never want to delete this ACK callback
						clearTimeout(timeout);
						finalize();
						// only remove callback function if we actually expected a finite amount of responses
					}

					deliveryDeferredPromise.resolve();
				}
			},
			timeout
		});
		return deliveryDeferredPromise;
	}

	public async publishMessage(
		from: PublicSignKey,
		message: Message,
		to?: PeerStreams[] | Map<string, PeerStreams>,
		relayed?: boolean
	): Promise<void> {
		let delivereyPromise: Promise<void> | undefined = undefined as any;

		if (
			(!message.header.signatures ||
				message.header.signatures.publicKeys.length === 0) &&
			message instanceof DataMessage &&
			message.header.mode instanceof SilentDelivery === false
		) {
			throw new Error("Missing signature");
		}

		/**
		 * Logic for handling acknowledge messages when we receive them (later)
		 */

		if (
			message instanceof DataMessage &&
			message.header.mode instanceof SeekDelivery &&
			!relayed
		) {
			to = this.peers; // seek delivery will not work unless we try all possible paths
		}

		if (message instanceof AcknowledgeDelivery) {
			to = undefined;
		}

		if (
			message instanceof DataMessage &&
			(message.header.mode instanceof SeekDelivery ||
				message.header.mode instanceof AcknowledgeDelivery)
		) {
			delivereyPromise = (
				await this.createDeliveryPromise(from, message, relayed)
			).promise;
		}

		const bytes = message.bytes();

		if (!relayed) {
			const bytesArray = bytes instanceof Uint8Array ? bytes : bytes.subarray();
			await this.modifySeenCache(bytesArray);
		}

		/**
		 * For non SEEKing message delivery modes, use routing
		 */

		if (message instanceof DataMessage) {
			if (
				(message.header.mode instanceof AcknowledgeDelivery ||
					message.header.mode instanceof SilentDelivery) &&
				!to &&
				message.header.mode.to
			) {
				const fanout = this.routes.getFanout(
					from,
					message.header.mode.to,
					message.header.mode.redundancy
				);

				if (fanout) {
					if (fanout.size > 0) {
						for (const [neighbour, _distantPeers] of fanout) {
							const stream = this.peers.get(neighbour);
							stream?.waitForWrite(bytes).catch((e) => {
								logger.error("Failed to publish message: " + e.message);
							});
						}
						return delivereyPromise; // we are done sending the message in all direction with updates 'to' lists
					}

					return; // we defintely that we should not forward the message anywhere
				}

				return;

				// else send to all (fallthrough to code below)
			}
		}

		// We fils to send the message directly, instead fallback to floodsub
		const peers: PeerStreams[] | Map<string, PeerStreams> = to || this.peers;
		if (
			peers == null ||
			(Array.isArray(peers) && peers.length === 0) ||
			(peers instanceof Map && peers.size === 0)
		) {
			logger.debug("No peers to send to");
			return;
		}

		let sentOnce = false;
		for (const stream of peers.values()) {
			const id = stream as PeerStreams;
			if (id.publicKey.equals(from)) {
				continue;
			}

			sentOnce = true;

			id.waitForWrite(bytes).catch((e) => {
				logger.error("Failed to publish message: " + e.message);
			});
		}

		if (!sentOnce) {
			if (!relayed) {
				throw new Error("Message did not have any valid receivers");
			}
		}
		return delivereyPromise;
	}

	async maybeConnectDirectly(toHash: string, origin: MultiAddrinfo) {
		if (this.peers.has(toHash) || this.prunedConnectionsCache?.has(toHash)) {
			return; // TODO, is this expected, or are we to dial more addresses?
		}

		const addresses = origin.multiaddrs
			.filter((x) => {
				const ret = !this.recentDials!.has(x);
				this.recentDials!.add(x);
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

	async waitFor(
		peer: PeerId | PublicSignKey,
		options?: { signal: AbortSignal }
	) {
		const hash = (
			peer instanceof PublicSignKey ? peer : getPublicKeyFromPeerId(peer)
		).hashcode();
		try {
			await waitFor(
				() => {
					if (!this.peers.has(hash)) {
						return false;
					}
					if (!this.routes.isReachable(this.publicKeyHash, hash)) {
						return false;
					}

					return true;
				},
				{ signal: options?.signal }
			);
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
			await waitFor(() => stream.isReadable && stream.isWritable, {
				signal: options?.signal
			});
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

	lastQueuedBytes = 0;

	// make this into a job? run every few ms
	maybePruneConnections(): Promise<void> {
		if (this.connectionManagerOptions.pruner!.bandwidth != null) {
			let usedBandwidth = 0;
			for (const [_k, v] of this.peers) {
				usedBandwidth += v.usedBandwidth;
			}
			usedBandwidth /= this.peers.size;

			if (usedBandwidth > this.connectionManagerOptions.pruner!.bandwidth) {
				// prune
				return this.pruneConnections();
			}
		} else if (this.connectionManagerOptions.pruner!.maxBuffer != null) {
			const queuedBytes = this.getQueuedBytes();
			if (queuedBytes > this.connectionManagerOptions.pruner!.maxBuffer) {
				// prune
				return this.pruneConnections();
			}
		}
		return Promise.resolve();
	}

	async pruneConnections(): Promise<void> {
		// TODO sort by bandwidth
		if (this.peers.size <= this.connectionManagerOptions.minConnections) {
			return;
		}
		const sorted = [...this.peers.values()]
			.sort((x, y) => x.usedBandwidth - y.usedBandwidth)
			.map((x) => x.publicKey.hashcode());
		const prunables = this.routes.getPrunable(sorted);
		if (prunables.length === 0) {
			return;
		}

		const stream = this.peers.get(prunables[0])!;
		this.prunedConnectionsCache!.add(stream.publicKey.hashcode());

		await this.onPeerDisconnected(stream.peerId);
		return this.components.connectionManager.closeConnections(stream.peerId);
	}

	getQueuedBytes(): number {
		let sum = 0;
		for (const peer of this.peers) {
			sum += peer[1].outboundStream?.readableLength || 0;
		}
		return sum;
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
