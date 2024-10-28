import {
	MuxerClosedError,
	StreamResetError,
	TypedEventEmitter,
	UnsupportedProtocolError,
} from "@libp2p/interface";
import type {
	Connection,
	Libp2pEvents,
	PeerId,
	PeerStore,
	Stream,
	TypedEventTarget,
} from "@libp2p/interface";
import type {
	AddressManager,
	ConnectionManager,
	IncomingStreamData,
	Registrar,
} from "@libp2p/interface-internal";
import { multiaddr } from "@multiformats/multiaddr";
import { Cache } from "@peerbit/cache";
import {
	PublicSignKey,
	type SignatureWithKey,
	getKeypairFromPrivateKey,
	getPublicKeyFromPeerId,
	ready,
	sha256Base64,
	toBase64,
} from "@peerbit/crypto";
import {
	ACK,
	AcknowledgeDelivery,
	AnyWhere,
	DataMessage,
	DeliveryError,
	Goodbye,
	type IdentificationOptions,
	Message,
	MessageHeader,
	MultiAddrinfo,
	NotStartedError,
	type PriorityOptions,
	type PublicKeyFromHashResolver,
	SeekDelivery,
	SilentDelivery,
	type StreamEvents,
	TracedDelivery,
	type WaitForPeer,
	type WithMode,
	type WithTo,
	deliveryModeHasReceiver,
	getMsgId,
} from "@peerbit/stream-interface";
import { AbortError, TimeoutError, delay } from "@peerbit/time";
import { abortableSource } from "abortable-iterator";
import * as lp from "it-length-prefixed";
import { pipe } from "it-pipe";
import { type Pushable, pushable } from "it-pushable";
import type { Components } from "libp2p/components";
import pDefer, { type DeferredPromise } from "p-defer";
import Queue from "p-queue";
import { Uint8ArrayList } from "uint8arraylist";
import { logger } from "./logger.js";
import { type PushableLanes, pushableLanes } from "./pushable-lanes.js";
import { MAX_ROUTE_DISTANCE, Routes } from "./routes.js";
import { BandwidthTracker } from "./stats.js";
import { waitForEvent } from "./wait-for-event.js";

export { logger };

export { BandwidthTracker }; // might be useful for others

const logError = (e?: { message: string }) => {
	if (e?.message === "Cannot push value onto an ended pushable") {
		return; // ignore since we are trying to push to a closed stream
	}
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
	"peer:reachable": CustomEvent<PublicSignKey>;
	"peer:unreachable": CustomEvent<PublicSignKey>;
}

const SEEK_DELIVERY_TIMEOUT = 10e3;
const ROUTE_MAX_RETANTION_PERIOD = 5e4;
const MAX_DATA_LENGTH_IN = 15e6 + 1000; // 15 mb and some metadata
const MAX_DATA_LENGTH_OUT = 1e7 + 1000; // 10 mb and some metadata

const MAX_QUEUED_BYTES = MAX_DATA_LENGTH_IN * 50;

const DEFAULT_PRUNE_CONNECTIONS_INTERVAL = 2e4;
const DEFAULT_MIN_CONNECTIONS = 2;
const DEFAULT_MAX_CONNECTIONS = 300;

const DEFAULT_PRUNED_CONNNECTIONS_TIMEOUT = 30 * 1000;

const ROUTE_UPDATE_DELAY_FACTOR = 3e4;

const getLaneFromPriority = (priority: number) => {
	if (priority > 0) {
		return 0;
	}
	return 1;
};
/**
 * Thin wrapper around a peer's inbound / outbound pubsub streams
 */
export class PeerStreams extends TypedEventEmitter<PeerStreamEvents> {
	public readonly peerId: PeerId;
	public readonly publicKey: PublicSignKey;
	public readonly protocol: string;
	/**
	 * Write stream - it's preferable to use the write method
	 */
	public outboundStream?: PushableLanes<Uint8Array>;

	/**
	 * Read stream
	 */
	public inboundStream?: AsyncIterable<Uint8ArrayList>;
	/**
	 * The raw outbound stream, as retrieved from conn.newStream
	 */
	public rawOutboundStream?: Stream;
	/**
	 * The raw inbound stream, as retrieved from the callback from libp2p.handle
	 */
	public rawInboundStream?: Stream;
	/**
	 * An AbortController for controlled shutdown of the  treams
	 */
	private inboundAbortController: AbortController;
	private outboundAbortController: AbortController;

	private closed: boolean;

	public connId: string;

	public seekedOnce: boolean;

	private usedBandWidthTracker: BandwidthTracker;
	constructor(init: PeerStreamsInit) {
		super();

		this.peerId = init.peerId;
		this.publicKey = init.publicKey;
		this.protocol = init.protocol;
		this.inboundAbortController = new AbortController();
		this.outboundAbortController = new AbortController();

		this.closed = false;
		this.connId = init.connId;
		this.usedBandWidthTracker = new BandwidthTracker(10);
		this.usedBandWidthTracker.start();
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
	write(data: Uint8Array | Uint8ArrayList, priority: number) {
		if (data.length > MAX_DATA_LENGTH_OUT) {
			throw new Error(
				`Message too large (${data.length * 1e-6}) mb). Needs to be less than ${
					MAX_DATA_LENGTH_OUT * 1e-6
				} mb`,
			);
		}
		if (this.outboundStream == null) {
			logger.error("No writable connection to " + this.peerId.toString());
			throw new Error("No writable connection to " + this.peerId.toString());
		}

		this.usedBandWidthTracker.add(data.byteLength);

		this.outboundStream.push(
			data instanceof Uint8Array ? data : data.subarray(),
			this.outboundStream.getReadableLength(0) === 0
				? 0
				: getLaneFromPriority(priority), // TODO use more lanes
		);
	}

	async waitForWrite(bytes: Uint8Array | Uint8ArrayList, priority: number = 0) {
		if (this.closed) {
			logger.error(
				"Failed to send to stream: " + this.peerId.toString() + ". Closed",
			);
			return;
		}

		if (!this.isWritable) {
			// Catch the event where the outbound stream is attach, but also abort if we shut down
			const outboundPromise = new Promise<void>((resolve, reject) => {
				const resolveClear = () => {
					this.removeEventListener("stream:outbound", listener);
					clearTimeout(timer);
					resolve();
				};
				const rejectClear = (err: Error) => {
					this.removeEventListener("stream:outbound", listener);
					clearTimeout(timer);
					reject(err);
				};
				const timer = setTimeout(() => {
					rejectClear(new Error("Timed out"));
				}, 3 * 1000); // TODO if this timeout > 10s we run into issues in the tests when running in CI
				const abortHandler = () => {
					this.removeEventListener("close", abortHandler);
					rejectClear(new AbortError("Closed"));
				};
				this.addEventListener("close", abortHandler);

				const listener = () => {
					resolveClear();
				};
				this.addEventListener("stream:outbound", listener);
				if (this.isWritable) {
					resolveClear();
				}
			});

			await outboundPromise
				.then(() => {
					this.write(bytes, priority);
				})
				.catch((error) => {
					if (this.closed) {
						return; // ignore
					}
					if (error instanceof AbortError) {
						//return;
					}
					throw error;
				});
		} else {
			this.write(bytes, priority);
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
		this.rawInboundStream = stream;
		this.inboundStream = abortableSource(
			pipe(this.rawInboundStream, (source) =>
				lp.decode(source, { maxDataLength: MAX_DATA_LENGTH_IN }),
			),
			this.inboundAbortController.signal,
			{
				returnOnAbort: true,
				onReturnError: (err) => {
					logger.error("Inbound stream error", err?.message);
				},
			},
		);

		/* this.rawInboundStream = stream
		this.inboundAbortController = new AbortController()
		this.inboundAbortController.signal.addEventListener('abort', () => {
			this.rawInboundStream!.close()
				.catch(err => {
					this.rawInboundStream?.abort(err)
				})
		})

		this.inboundStream = pipe(
			this.rawInboundStream!,
			(source) => lp.decode(source, { maxDataLength: MAX_DATA_LENGTH_IN }),
		)
 */
		this.dispatchEvent(new CustomEvent("stream:inbound"));
		return this.inboundStream;
	}

	/**
	 * Attach a raw outbound stream and setup a write stream
	 */

	async attachOutboundStream(stream: Stream) {
		// If an outbound stream already exists, gently close it
		const _prevStream = this.outboundStream;
		if (_prevStream) {
			logger.info(
				`Stream already exist. This can be due to that you are opening two or more connections to ${this.peerId.toString()}. A stream will only be created for the first succesfully created connection`,
			);
			return;
		}

		this.rawOutboundStream = stream;
		this.outboundStream = pushableLanes({ lanes: 2 });

		this.outboundAbortController.signal.addEventListener("abort", () => {
			this.rawOutboundStream?.close().catch((err) => {
				this.rawOutboundStream?.abort(err);
			});
		});

		pipe(
			this.outboundStream,
			(source) => lp.encode(source),
			this.rawOutboundStream,
		).catch(logError);

		// Emit if the connection is new
		this.dispatchEvent(new CustomEvent("stream:outbound"));
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
			this.rawOutboundStream?.abort(new AbortError("Closed"));
			this.outboundAbortController.abort();
		}

		// End the inbound stream
		if (this.inboundStream != null) {
			this.inboundAbortController.abort();
			await this.rawInboundStream?.close();
		}

		this.usedBandWidthTracker.stop();

		this.dispatchEvent(new CustomEvent("close"));

		this.rawOutboundStream = undefined;
		this.outboundStream = undefined;

		this.rawInboundStream = undefined;
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

export type ConnectionManagerArguments =
	| (Partial<Pick<ConnectionManagerOptions, "minConnections">> &
			Partial<Pick<ConnectionManagerOptions, "maxConnections">> & {
				pruner?: Partial<PrunerOptions> | false;
			} & { dialer?: Partial<DialerOptions> | false })
	| false;

export type DirectStreamOptions = {
	canRelayMessage?: boolean;
	messageProcessingConcurrency?: number;
	maxInboundStreams?: number;
	maxOutboundStreams?: number;
	connectionManager?: ConnectionManagerArguments;
	routeSeekInterval?: number;
	seekTimeout?: number;
	routeMaxRetentionPeriod?: number;
};

export interface DirectStreamComponents extends Components {
	peerId: PeerId;
	addressManager: AddressManager;
	registrar: Registrar;
	connectionManager: ConnectionManager;
	peerStore: PeerStore;
	events: TypedEventTarget<Libp2pEvents>;
}

export abstract class DirectStream<
		Events extends { [s: string]: any } = StreamEvents,
	>
	extends TypedEventEmitter<Events>
	implements WaitForPeer, PublicKeyFromHashResolver
{
	public peerId: PeerId;
	public publicKey: PublicSignKey;
	public publicKeyHash: string;
	public sign: (bytes: Uint8Array) => Promise<SignatureWithKey>;

	public started: boolean;
	public stopping: boolean;
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
	private routeMaxRetentionPeriod: number;

	// for sequential creation of outbound streams
	private outboundInflightQueue: Pushable<{
		connection: Connection;
		peerId: PeerId;
	}>;

	routeSeekInterval: number;
	seekTimeout: number;
	closeController: AbortController;
	session: number;

	private _ackCallbacks: Map<
		string,
		{
			promise: Promise<void>;
			callback: (
				ack: ACK,
				messageThrough: PeerStreams,
				messageFrom?: PeerStreams,
			) => void;
			clear: () => void;
		}
	>;

	constructor(
		readonly components: DirectStreamComponents,
		multicodecs: string[],
		options?: DirectStreamOptions,
	) {
		super();
		const {
			canRelayMessage = true,
			messageProcessingConcurrency = 10,
			maxInboundStreams,
			maxOutboundStreams,
			connectionManager,
			routeSeekInterval = ROUTE_UPDATE_DELAY_FACTOR,
			seekTimeout = SEEK_DELIVERY_TIMEOUT,
			routeMaxRetentionPeriod = ROUTE_MAX_RETANTION_PERIOD,
		} = options || {};

		const signKey = getKeypairFromPrivateKey(components.privateKey);
		this.seekTimeout = seekTimeout;
		this.sign = signKey.sign.bind(signKey);
		this.peerId = components.peerId;
		this.publicKey = signKey.publicKey;
		this.publicKeyHash = signKey.publicKey.hashcode();
		this.multicodecs = multicodecs;
		this.started = false;
		this.peers = new Map<string, PeerStreams>();
		this.canRelayMessage = canRelayMessage;
		this.healthChecks = new Map();
		this.routeSeekInterval = routeSeekInterval;
		this.queue = new Queue({ concurrency: messageProcessingConcurrency });
		this.maxInboundStreams = maxInboundStreams;
		this.maxOutboundStreams = maxOutboundStreams;
		this.seenCache = new Cache({ max: 1e6, ttl: 10 * 60 * 1e3 });
		this.routeMaxRetentionPeriod = routeMaxRetentionPeriod;
		this.peerKeyHashToPublicKey = new Map();
		this._onIncomingStream = this._onIncomingStream.bind(this);
		this.onPeerConnected = this.onPeerConnected.bind(this);
		this.onPeerDisconnected = this.onPeerDisconnected.bind(this);

		this._ackCallbacks = new Map();

		if (connectionManager === false || connectionManager === null) {
			this.connectionManagerOptions = {
				maxConnections: Number.MAX_SAFE_INTEGER,
				minConnections: 0,
				dialer: undefined,
				pruner: undefined,
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
								...connectionManager?.pruner,
							}
						: undefined,
			};
		}

		this.recentDials = this.connectionManagerOptions.dialer
			? new Cache({
					ttl: this.connectionManagerOptions.dialer.retryDelay,
					max: 1e3,
				})
			: undefined;

		this.prunedConnectionsCache = this.connectionManagerOptions.pruner
			? new Cache({
					max: 1e6,
					ttl: this.connectionManagerOptions.pruner.connectionTimeout,
				})
			: undefined;
	}

	async start() {
		if (this.started) {
			return;
		}

		this.session = +new Date();
		await ready;

		this.closeController = new AbortController();

		this.outboundInflightQueue = pushable({ objectMode: true });
		pipe(this.outboundInflightQueue, async (source) => {
			for await (const { peerId, connection } of source) {
				if (this.stopping || this.started === false) {
					return;
				}
				await this.createOutboundStream(peerId, connection);
			}
		}).catch((e) => {
			logger.error("outbound inflight queue error: " + e?.toString());
		});

		this.closeController.signal.addEventListener("abort", () => {
			this.outboundInflightQueue.return();
		});

		this.routes = new Routes(this.publicKeyHash, {
			routeMaxRetentionPeriod: this.routeMaxRetentionPeriod,
			signal: this.closeController.signal,
		});

		this.started = true;
		this.stopping = false;
		logger.debug("starting");

		// Incoming streams
		// Called after a peer dials us
		await Promise.all(
			this.multicodecs.map((multicodec) =>
				this.components.registrar.handle(multicodec, this._onIncomingStream, {
					maxInboundStreams: this.maxInboundStreams,
					maxOutboundStreams: this.maxOutboundStreams,
					runOnLimitedConnection: false,
				}),
			),
		);

		// register protocol with topology
		// Topology callbacks called on connection manager changes
		this._registrarTopologyIds = await Promise.all(
			this.multicodecs.map((multicodec) =>
				this.components.registrar.register(multicodec, {
					onConnect: this.onPeerConnected.bind(this),
					onDisconnect: this.onPeerDisconnected.bind(this),
					notifyOnLimitedConnection: false,
				}),
			),
		);

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

			await this.onPeerConnected(conn.remotePeer, conn);
		}
		if (this.connectionManagerOptions.pruner) {
			const pruneConnectionsLoop = () => {
				if (!this.connectionManagerOptions.pruner) {
					return;
				}
				this.pruneConnectionsTimeout = setTimeout(() => {
					this.maybePruneConnections().finally(() => {
						if (!this.started) {
							return;
						}
						pruneConnectionsLoop();
					});
				}, this.connectionManagerOptions.pruner.interval);
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

		clearTimeout(this.pruneConnectionsTimeout);

		await Promise.all(
			this.multicodecs.map((x) => this.components.registrar.unhandle(x)),
		);

		// unregister protocol and handlers
		if (this._registrarTopologyIds != null) {
			this._registrarTopologyIds?.map((id) =>
				this.components.registrar.unregister(id),
			);
		}

		// reset and clear up
		this.started = false;
		this.outboundInflightQueue.end();
		this.closeController.abort();

		logger.debug("stopping");
		for (const peerStreams of this.peers.values()) {
			await peerStreams.close();
		}

		for (const [_k, v] of this.healthChecks) {
			clearTimeout(v);
		}
		this.healthChecks.clear();
		this.prunedConnectionsCache?.clear();

		this.queue.clear();
		this.peers.clear();
		this.seenCache.clear();
		this.routes.clear();
		this.peerKeyHashToPublicKey.clear();

		for (const [_k, v] of this._ackCallbacks) {
			v.clear();
		}

		this._ackCallbacks.clear();
		logger.debug("stopped");
		this.stopping = false;
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
			await this.components.peerStore.delete(peerId);
			return;
		}

		const peer = this.addPeer(
			peerId,
			publicKey,
			stream.protocol,
			connection.id,
		);

		// handle inbound
		const inboundStream = peer.attachInboundStream(stream);
		this.processMessages(peer.publicKey, inboundStream, peer).catch(logError);

		// try to create outbound stream
		await this.outboundInflightQueue.push({ peerId, connection });
	}

	protected async createOutboundStream(peerId: PeerId, connection: Connection) {
		for (const existingStreams of connection.streams) {
			if (
				existingStreams.protocol &&
				this.multicodecs.includes(existingStreams.protocol) &&
				existingStreams.direction === "outbound"
			) {
				return;
			}
		}

		let stream: Stream = undefined as any; // TODO types
		let tries = 0;
		let peer: PeerStreams = undefined as any;
		const peerKey = getPublicKeyFromPeerId(peerId);
		while (tries <= 3) {
			tries++;
			if (!this.started) {
				return;
			}

			try {
				stream = await connection.newStream(this.multicodecs, {
					// TODO this property seems necessary, together with waitFor isReadable when making sure two peers are conencted before talking.
					// research whether we can do without this so we can push data without beeing able to send
					// more info here https://github.com/libp2p/js-libp2p/issues/2321
					negotiateFully: true,
				});
				if (stream.protocol == null) {
					stream.abort(new Error("Stream was not multiplexed"));
					return;
				}

				if (!this.started) {
					// we closed before we could create the stream
					stream.abort(new Error("Closed"));
					return;
				}
				peer = this.addPeer(peerId, peerKey, stream.protocol!, connection.id); // TODO types
				await peer.attachOutboundStream(stream);
			} catch (error: any) {
				if (error.code === "ERR_UNSUPPORTED_PROTOCOL") {
					await delay(100);
					continue; // Retry
				}

				if (error instanceof UnsupportedProtocolError) {
					await delay(100);
					continue; // Retry
				}

				if (
					connection.status !== "open" ||
					error?.message === "Muxer already closed" ||
					error.code === "ERR_STREAM_RESET" ||
					error instanceof StreamResetError ||
					error instanceof MuxerClosedError
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
	}

	/**
	 * Registrar notifies an established connection with protocol
	 */
	public async onPeerConnected(peerId: PeerId, connection: Connection) {
		if (
			!this.isStarted() ||
			connection.limits ||
			connection.status !== "open"
		) {
			return;
		}
		const peerKey = getPublicKeyFromPeerId(peerId);

		if (this.prunedConnectionsCache?.has(peerKey.hashcode())) {
			await connection.close();
			await this.components.peerStore.delete(peerId);
			return; // we recently pruned this connect, dont allow it to connect for a while
		}

		this.outboundInflightQueue.push({ peerId, connection });
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
					(x) => x.id === conn.id,
				) /* TODO this should work but does not? peer?.connId !== conn.id */
		) {
			return;
		}

		if (!this.peers.has(peerKeyHash)) {
			// TODO remove when https://github.com/libp2p/js-libp2p/issues/2369 fixed
			// TODO this code should work even if onPeerDisconnected events are emitted wrongfully. i.e. disconnection should occur and rediscover should be smooth?
			return;
		}
		if (!this.publicKey.equals(peerKey)) {
			await this._removePeer(peerKey);

			// tell dependent peers that there is a node that might have left
			const dependent = this.routes.getDependent(peerKeyHash);

			// make neighbour unreachables
			this.removePeerFromRoutes(peerKeyHash, true);

			if (dependent.length > 0) {
				await this.publishMessage(
					this.publicKey,
					await new Goodbye({
						leaving: [peerKeyHash],
						header: new MessageHeader({
							session: this.session,
							mode: new SilentDelivery({ to: dependent, redundancy: 2 }),
						}),
					}).sign(this.sign),
				);
			}

			this.checkIsAlive([peerKeyHash]);
		}

		logger.debug("connection ended:" + peerKey.toString());
	}

	public removePeerFromRoutes(hash: string, deleteIfNeighbour = false) {
		if (this.peers.has(hash) && !deleteIfNeighbour) {
			return;
		}

		const unreachable = this.routes.remove(hash);
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
		remoteSession: number,
	) {
		const targetHash = typeof target === "string" ? target : target.hashcode();

		const update = this.routes.add(
			from,
			neighbour,
			targetHash,
			distance,
			session,
			remoteSession,
		);

		// second condition is that we don't want to emit 'reachable' events for routes where we act only as a relay
		// in this case, from is != this.publicKeyhash
		if (from === this.publicKeyHash) {
			if (update === "new") {
				this.peerKeyHashToPublicKey.set(target.hashcode(), target);
				this.onPeerReachable(target);
			}
		}
	}

	public onPeerReachable(publicKey: PublicSignKey) {
		// override this fn
		this.dispatchEvent(
			new CustomEvent("peer:reachable", { detail: publicKey }),
		);
	}

	public onPeerUnreachable(hash: string) {
		// override this fn

		this.dispatchEvent(
			// TODO types
			new CustomEvent("peer:unreachable", {
				detail: this.peerKeyHashToPublicKey.get(hash)!,
			}),
		);
	}

	public updateSession(key: PublicSignKey, session?: number) {
		if (this.routes.updateSession(key.hashcode(), session)) {
			return this.onPeerSession(key, session!);
		}
	}
	public invalidateSession(key: string) {
		this.routes.updateSession(key, undefined);
	}

	public onPeerSession(key: PublicSignKey, session: number) {
		this.dispatchEvent(
			// TODO types
			new CustomEvent("peer:session", {
				detail: key,
			}),
		);
	}

	/**
	 * Notifies the router that a peer has been connected
	 */
	addPeer(
		peerId: PeerId,
		publicKey: PublicSignKey,
		protocol: string,
		connId: string,
	): PeerStreams {
		const publicKeyHash = publicKey.hashcode();

		this.clearHealthcheckTimer(publicKeyHash);

		const existing = this.peers.get(publicKeyHash);

		// If peer streams already exists, do nothing
		if (existing != null) {
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
			connId,
		});

		this.peers.set(publicKeyHash, peerStreams);
		this.updateSession(publicKey, -1);

		peerStreams.addEventListener("close", () => this._removePeer(publicKey), {
			once: true,
		});

		this.addRouteConnection(
			this.publicKeyHash,
			publicKey.hashcode(),
			publicKey,
			-1,
			+new Date(),
			-1,
		);

		return peerStreams;
	}

	/**
	 * Notifies the router that a peer has been disconnected
	 */
	protected async _removePeer(publicKey: PublicSignKey) {
		const hash = publicKey.hashcode();
		const peerStreams = this.peers.get(hash);
		this.clearHealthcheckTimer(hash);

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
		peerStreams: PeerStreams,
	) {
		try {
			await pipe(stream, async (source) => {
				for await (const data of source) {
					this.processRpc(peerId, peerStreams, data).catch((e) => {
						logError(e);
					});
				}
			});
		} catch (err: any) {
			if (err?.code === "ERR_STREAM_RESET") {
				// only send stream reset messages to info
				logger.info(
					"Failed processing messages to id: " +
						peerStreams.peerId.toString() +
						". " +
						err?.message,
				);
			} else {
				logger.warn(
					"Failed processing messages to id: " +
						peerStreams.peerId.toString() +
						". " +
						err?.message,
				);
			}
			this.onPeerDisconnected(peerStreams.peerId);
		}
	}

	/**
	 * Handles an rpc request from a peer
	 */
	async processRpc(
		from: PublicSignKey,
		peerStreams: PeerStreams,
		message: Uint8ArrayList,
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
		getIdFn: (bytes: Uint8Array) => Promise<string> = getMsgId,
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
		msg: Uint8ArrayList,
	) {
		if (!this.started) {
			return;
		}

		// Ensure the message is valid before processing it
		const message: Message | undefined = Message.from(msg);
		this.dispatchEvent(
			new CustomEvent("message", {
				detail: message,
			}),
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

	public shouldIgnore(message: DataMessage, seenBefore: number) {
		const fromMe = message.header.signatures?.publicKeys.find((x) =>
			x.equals(this.publicKey),
		);

		if (fromMe) {
			return true;
		}

		if (
			(seenBefore > 0 &&
				message.header.mode instanceof SeekDelivery === false) ||
			(message.header.mode instanceof SeekDelivery &&
				seenBefore >= message.header.mode.redundancy)
		) {
			return true;
		}

		return false;
	}

	public async onDataMessage(
		from: PublicSignKey,
		peerStream: PeerStreams,
		message: DataMessage,
		seenBefore: number,
	) {
		if (this.shouldIgnore(message, seenBefore)) {
			return false;
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
			if (!(await this.verifyAndProcess(message))) {
				// we don't verify messages we don't dispatch because of the performance penalty // TODO add opts for this
				logger.warn("Recieved message with invalid signature or timestamp");
				return false;
			}

			await this.maybeAcknowledgeMessage(peerStream, message, seenBefore);

			if (seenBefore === 0 && message.data) {
				this.dispatchEvent(
					new CustomEvent("data", {
						detail: message,
					}),
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
		if (message.header.mode instanceof SeekDelivery || seenBefore === 0) {
			// DONT await this since it might introduce a dead-lock
			if (message.header.mode instanceof SeekDelivery) {
				if (seenBefore < message.header.mode.redundancy) {
					const to = [...this.peers.values()].filter(
						(x) =>
							!message.header.signatures?.publicKeys.find((y) =>
								y.equals(x.publicKey),
							) && x !== peerStream,
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

	public async verifyAndProcess(message: Message<any>) {
		const verified = await message.verify(true);
		if (!verified) {
			return false;
		}

		const from = message.header.signatures!.publicKeys[0];
		if (this.peers.has(from.hashcode())) {
			// do nothing
		} else {
			this.updateSession(from, Number(message.header.session));
		}
		return true;
	}

	async maybeAcknowledgeMessage(
		peerStream: PeerStreams,
		message: DataMessage | Goodbye,
		seenBefore: number,
	) {
		if (
			(message.header.mode instanceof SeekDelivery ||
				message.header.mode instanceof AcknowledgeDelivery) &&
			seenBefore < message.header.mode.redundancy
		) {
			const shouldAcknowldege =
				message.header.mode.to == null ||
				message.header.mode.to.includes(this.publicKeyHash);
			if (!shouldAcknowldege) {
				return;
			}
			const signers = message.header.signatures!.publicKeys.map((x) =>
				x.hashcode(),
			);

			await this.publishMessage(
				this.publicKey,
				await new ACK({
					messageIdToAcknowledge: message.id,
					seenCounter: seenBefore,
					// TODO only give origin info to peers we want to connect to us
					header: new MessageHeader({
						mode: new TracedDelivery(signers),
						session: this.session,

						// include our origin if message is SeekDelivery and we have not recently pruned a connection to this peer
						origin:
							message.header.mode instanceof SeekDelivery &&
							!message.header.signatures!.publicKeys.find((x) =>
								this.prunedConnectionsCache?.has(x.hashcode()),
							)
								? new MultiAddrinfo(
										this.components.addressManager
											.getAddresses()
											.map((x) => x.toString()),
									)
								: undefined,
					}),
				}).sign(this.sign),
				[peerStream],
			);
		}
	}

	private async _onDataMessage(
		from: PublicSignKey,
		peerStream: PeerStreams,
		messageBytes: Uint8ArrayList | Uint8Array,
		message: DataMessage,
	) {
		const seenBefore = await this.modifySeenCache(
			messageBytes instanceof Uint8ArrayList
				? messageBytes.subarray()
				: messageBytes,
		);

		return this.onDataMessage(from, peerStream, message, seenBefore);
	}

	async onAck(
		publicKey: PublicSignKey,
		peerStream: PeerStreams,
		messageBytes: Uint8ArrayList | Uint8Array,
		message: ACK,
	) {
		const seenBefore = await this.modifySeenCache(
			messageBytes instanceof Uint8Array
				? messageBytes
				: messageBytes.subarray(),
			(bytes) => sha256Base64(bytes),
		);

		if (seenBefore > 0) {
			logger.debug(
				"Received message already seen of type: " + message.constructor.name,
			);
			return false;
		}

		if (!(await this.verifyAndProcess(message))) {
			logger.warn(`Recieved ACK message that did not verify`);
			return false;
		}

		const messageIdString = toBase64(message.messageIdToAcknowledge);
		const myIndex = message.header.mode.trace.findIndex(
			(x) => x === this.publicKeyHash,
		);
		const next = message.header.mode.trace[myIndex - 1];
		const nextStream = next ? this.peers.get(next) : undefined;

		this._ackCallbacks
			.get(messageIdString)
			?.callback(message, peerStream, nextStream);

		// relay ACK ?
		// send exactly backwards same route we got this message
		if (nextStream) {
			await this.publishMessage(this.publicKey, message, [nextStream], true);
		} else {
			if (myIndex !== 0) {
				// TODO should we throw something, or log?
				// we could arrive here if the ACK target does not exist any more...
				return;
			}
			// if origin exist (we can connect to remote peer) && we have autodialer turned on
			if (message.header.origin && this.connectionManagerOptions.dialer) {
				this.maybeConnectDirectly(
					message.header.signatures!.publicKeys[0].hashcode(),
					message.header.origin,
				);
			}
		}
	}

	async onGoodBye(
		publicKey: PublicSignKey,
		peerStream: PeerStreams,
		messageBytes: Uint8ArrayList | Uint8Array,
		message: Goodbye,
	) {
		const seenBefore = await this.modifySeenCache(
			messageBytes instanceof Uint8Array
				? messageBytes
				: messageBytes.subarray(),
		);

		if (seenBefore > 0) {
			logger.debug(
				"Received message already seen of type: " + message.constructor.name,
			);
			return;
		}

		if (!(await this.verifyAndProcess(message))) {
			logger.warn(`Recieved ACK message that did not verify`);
			return false;
		}

		await this.maybeAcknowledgeMessage(peerStream, message, seenBefore);

		const filteredLeaving = message.leaving.filter((x) =>
			this.routes.hasTarget(x),
		);

		// Forward to all dependent
		for (const target of message.leaving) {
			// relay message to every one who previously talked to this peer
			const dependent = this.routes.getDependent(target);

			let newTo = [...message.header.mode.to, ...dependent];
			newTo = newTo.filter((x) => x !== this.publicKeyHash);
			message.header.mode = new SilentDelivery({ to: newTo, redundancy: 1 });

			if (message.header.mode.to.length > 0) {
				await this.publishMessage(publicKey, message, undefined, true);
			}
		}

		await this.maybeDeleteRemoteRoutes(filteredLeaving);
	}

	private maybeDeleteRemoteRoutes(remotes: string[]) {
		// Handle deletion (if message is sign by the peer who left)
		// or invalidation followed up with an attempt to re-establish a connection
		for (const remote of remotes) {
			this.invalidateSession(remote);
		}
		this.checkIsAlive(remotes);
	}
	private async checkIsAlive(remotes: string[]) {
		if (this.peers.size === 0) {
			return false;
		}
		if (remotes.length > 0) {
			return this.publish(undefined, {
				mode: new SeekDelivery({
					to: remotes,
					redundancy: DEFAULT_SEEK_MESSAGE_REDUDANCY,
				}),
			})
				.then(() => true)
				.catch((e) => {
					if (e instanceof DeliveryError) {
						return false;
					} else if (e instanceof NotStartedError) {
						return false;
					} else if (e instanceof TimeoutError) {
						return false;
					} else if (e instanceof AbortError) {
						return false;
					} else {
						throw e;
					}
				}); // this will remove the target if it is still not reable
		}
		return false;
	}

	async createMessage(
		data: Uint8Array | Uint8ArrayList | undefined,
		options: (WithTo | WithMode) & PriorityOptions & IdentificationOptions,
	) {
		// dispatch the event if we are interested

		let mode: SilentDelivery | SeekDelivery | AcknowledgeDelivery | AnyWhere = (
			options as WithMode
		).mode
			? (options as WithMode).mode!
			: new SilentDelivery({
					to: (options as WithTo).to!,
					redundancy: DEFAULT_SILENT_MESSAGE_REDUDANCY,
				});

		if (
			mode instanceof AcknowledgeDelivery ||
			mode instanceof SilentDelivery ||
			mode instanceof SeekDelivery
		) {
			if (mode.to?.find((x) => x === this.publicKeyHash)) {
				mode.to = mode.to.filter((x) => x !== this.publicKeyHash);
			}
		}

		if (mode instanceof AcknowledgeDelivery || mode instanceof SilentDelivery) {
			const now = +new Date();
			for (const hash of mode.to) {
				const neighbourRoutes = this.routes.routes
					.get(this.publicKeyHash)
					?.get(hash);

				if (
					!neighbourRoutes ||
					now - neighbourRoutes.session >
						neighbourRoutes.list.length * this.routeSeekInterval ||
					!this.routes.isUpToDate(hash, neighbourRoutes)
				) {
					mode = new SeekDelivery({
						to: mode.to,
						redundancy: DEFAULT_SEEK_MESSAGE_REDUDANCY,
					});
					break;
				}
			}
		}

		const message = new DataMessage({
			data: data instanceof Uint8ArrayList ? data.subarray() : data,
			header: new MessageHeader({
				id: options.id,
				mode,
				session: this.session,
				priority: options.priority,
			}),
		});

		// TODO allow messages to also be sent unsigned (signaturePolicy property)
		await message.sign(this.sign);
		return message;
	}
	/**
	 * Publishes messages to all peers
	 */
	async publish(
		data: Uint8Array | Uint8ArrayList | undefined,
		options: (WithMode | WithTo) & PriorityOptions = {
			mode: new SeekDelivery({ redundancy: DEFAULT_SEEK_MESSAGE_REDUDANCY }),
		},
	): Promise<Uint8Array> {
		if (!this.started) {
			throw new NotStartedError();
		}

		if ((options as WithMode).mode && (options as WithTo).to) {
			throw new Error(
				"Expecting either 'to' or 'mode' to be provided not both",
			);
		}

		const message = await this.createMessage(data, options);
		await this.publishMessage(this.publicKey, message, undefined);
		return message.id;
	}

	public async relayMessage(
		from: PublicSignKey,
		message: Message,
		to?: PeerStreams[] | Map<string, PeerStreams>,
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
			if (deliveryModeHasReceiver(message.header.mode)) {
				message.header.mode.to = message.header.mode.to.filter(
					(x) => x !== this.publicKeyHash,
				);
				if (message.header.mode.to.length === 0) {
					return; // non to send to
				}
			}

			return this.publishMessage(from, message, to, true);
		} else {
			logger.debug("Received a message to relay but canRelayMessage is false");
		}
	}

	private clearHealthcheckTimer(to: string) {
		const timer = this.healthChecks.get(to);
		clearTimeout(timer);
		this.healthChecks.delete(to);
	}

	private async createDeliveryPromise(
		from: PublicSignKey,
		message: DataMessage | Goodbye,
		relayed?: boolean,
	): Promise<{ promise: Promise<void> }> {
		if (message.header.mode instanceof AnyWhere) {
			return { promise: Promise.resolve() };
		}
		const idString = toBase64(message.id);

		const existing = this._ackCallbacks.get(idString);
		if (existing) {
			return { promise: existing.promise };
		}

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
							this.removePeerFromRoutes(to);
						}, this.seekTimeout),
					);
				}
			}
		}
		const haveReceivers = messageToSet.size > 0;

		if (haveReceivers && this.peers.size === 0) {
			return {
				promise: Promise.reject(
					new DeliveryError(
						"Cannnot deliver message to peers because there are no peers to deliver to",
					),
				),
			};
		}

		const deliveryDeferredPromise = pDefer<void>();
		if (!haveReceivers) {
			deliveryDeferredPromise.resolve(); // we dont know how many answer to expect, just resolve immediately
		}

		const willGetAllAcknowledgements = !relayed; // Only the origin will get all acks

		// Expected to receive at least 'filterMessageForSeenCounter' acknowledgements from each peer
		const filterMessageForSeenCounter = relayed
			? undefined
			: message.header.mode instanceof SeekDelivery
				? Math.min(this.peers.size, message.header.mode.redundancy)
				: 1; /*  message.deliveryMode instanceof SeekDelivery ? Math.min(this.peers.size - (relayed ? 1 : 0), message.deliveryMode.redundancy) : 1 */

		const uniqueAcks = new Set();
		const session = +new Date();

		const onUnreachable =
			!relayed &&
			((ev: any) => {
				const deletedReceiver = messageToSet.delete(ev.detail.hashcode());
				if (deletedReceiver) {
					// Only reject if we are the sender
					clear();
					deliveryDeferredPromise.reject(
						new DeliveryError(
							`At least one recipent became unreachable while delivering messsage of type ${message.constructor.name}} to ${ev.detail.hashcode()}`,
						),
					);
				}
			});

		onUnreachable && this.addEventListener("peer:unreachable", onUnreachable);

		const clear = () => {
			clearTimeout(timeout);
			onUnreachable &&
				this.removeEventListener("peer:unreachable", onUnreachable);
			this._ackCallbacks.delete(idString);
		};

		const timeout = setTimeout(async () => {
			clear();

			let hasAll = true;

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
					new DeliveryError(
						`Failed to get message ${idString} ${filterMessageForSeenCounter} ${[
							...messageToSet,
						]} delivery acknowledges from all nodes (${
							fastestNodesReached.size
						}/${messageToSet.size}). Mode: ${
							message.header.mode.constructor.name
						}. Redundancy: ${(message.header.mode as any)["redundancy"]}`,
					),
				);
			} else {
				deliveryDeferredPromise.resolve();
			}
		}, this.seekTimeout);

		const checkDone = () => {
			// This if clause should never enter for relayed connections, since we don't
			// know how many ACKs we will get
			if (
				filterMessageForSeenCounter != null &&
				uniqueAcks.size >= messageToSet.size * filterMessageForSeenCounter
			) {
				if (haveReceivers) {
					// this statement exist beacuse if we do SEEK and have to = [], then it means we try to reach as many as possible hence we never want to delete this ACK callback
					// only remove callback function if we actually expected a expected amount of responses
					clear();
				}
				deliveryDeferredPromise.resolve();
				return true;
			}
			return false;
		};

		this._ackCallbacks.set(idString, {
			promise: deliveryDeferredPromise.promise,
			callback: (ack: ACK, messageThrough, messageFrom) => {
				const messageTarget = ack.header.signatures!.publicKeys[0];
				const messageTargetHash = messageTarget.hashcode();
				const seenCounter = ack.seenCounter;

				// remove the automatic removal of route timeout since we have observed lifesigns of a peer
				this.clearHealthcheckTimer(messageTargetHash);

				// if the target is not inside the original message to, we still ad the target to our routes
				// this because a relay might modify the 'to' list and we might receive more answers than initially set
				if (message.header.mode instanceof SeekDelivery) {
					this.addRouteConnection(
						messageFrom?.publicKey.hashcode() || this.publicKeyHash,
						messageThrough.publicKey.hashcode(),
						messageTarget,
						seenCounter,
						session,
						Number(ack.header.session),
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

				checkDone();
			},
			clear: () => {
				clear();
				deliveryDeferredPromise.resolve();
			},
		});
		return deliveryDeferredPromise;
	}

	public async publishMessage(
		from: PublicSignKey,
		message: Message,
		to?: PeerStreams[] | Map<string, PeerStreams>,
		relayed?: boolean,
	): Promise<void> {
		if (this.stopping || !this.started) {
			throw new NotStartedError();
		}

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

		if (message.header.mode instanceof AcknowledgeDelivery) {
			to = undefined;
		}

		if (
			(message instanceof DataMessage || message instanceof Goodbye) &&
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
					from.hashcode(),
					message.header.mode.to,
					message.header.mode.redundancy,
				);

				if (fanout) {
					if (fanout.size > 0) {
						const promises: Promise<any>[] = [];
						for (const [neighbour, _distantPeers] of fanout) {
							const stream = this.peers.get(neighbour);
							stream &&
								promises.push(
									stream.waitForWrite(bytes, message.header.priority),
								);
						}
						await Promise.all(promises);
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
			return delivereyPromise;
		}

		let sentOnce = false;
		const promises: Promise<any>[] = [];
		for (const stream of peers.values()) {
			const id = stream as PeerStreams;

			// Dont sent back to the sender
			if (id.publicKey.equals(from)) {
				continue;
			}
			// Dont send message back to any of the signers (they have already seen the message)
			if (
				message.header.signatures?.publicKeys.find((x) =>
					x.equals(id.publicKey),
				)
			) {
				continue;
			}

			sentOnce = true;
			promises.push(id.waitForWrite(bytes, message.header.priority));
		}
		await Promise.all(promises);

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
				await this.components.connectionManager.openConnection([
					addresses[addresses.length - 1],
				]);
			} catch (error: any) {
				logger.info(
					"Failed to connect directly to: " +
						JSON.stringify(addresses.map((x) => x.toString())) +
						". " +
						error?.message,
				);
			}
		}
	}

	async waitFor(
		peer: PeerId | PublicSignKey | string,
		options?: { timeout?: number; signal?: AbortSignal; neighbour?: boolean },
	) {
		const hash =
			typeof peer === "string"
				? peer
				: (peer instanceof PublicSignKey
						? peer
						: getPublicKeyFromPeerId(peer)
					).hashcode();
		const checkIsReachable = (deferred: DeferredPromise<void>) => {
			if (options?.neighbour && !this.peers.has(hash)) {
				return;
			}

			if (!this.routes.isReachable(this.publicKeyHash, hash, 0)) {
				return;
			}

			deferred.resolve();
		};
		const abortSignals = [this.closeController.signal];
		if (options?.signal) {
			abortSignals.push(options.signal);
		}

		try {
			await waitForEvent(this, ["peer:reachable"], checkIsReachable, {
				signals: abortSignals,
				timeout: options?.timeout,
			});
		} catch (error) {
			throw new Error(
				"Stream to " +
					hash +
					" from " +
					this.publicKeyHash +
					" does not exist. Connection exist: " +
					this.peers.has(hash) +
					". Route exist: " +
					this.routes.isReachable(this.publicKeyHash, hash, 0),
			);
		}

		if (options?.neighbour) {
			const stream = this.peers.get(hash)!;
			try {
				let checkIsWritable = (pDefer: DeferredPromise<void>) => {
					if (stream.isReadable && stream.isWritable) {
						pDefer.resolve();
					}
				};
				await waitForEvent(
					stream,
					["stream:outbound", "stream:inbound"],
					checkIsWritable,
					{
						signals: abortSignals,
						timeout: options?.timeout,
					},
				);
			} catch (error) {
				throw new Error(
					"Stream to " +
						stream.publicKey.hashcode() +
						" not ready. Readable: " +
						stream.isReadable +
						". Writable " +
						stream.isWritable,
				);
			}
		}
	}

	getPublicKey(hash: string): PublicSignKey | undefined {
		return this.peerKeyHashToPublicKey.get(hash);
	}

	get pending(): boolean {
		return this._ackCallbacks.size > 0;
	}

	// make this into a job? run every few ms
	maybePruneConnections(): Promise<void> {
		if (this.connectionManagerOptions.pruner) {
			if (this.connectionManagerOptions.pruner.bandwidth != null) {
				let usedBandwidth = 0;
				for (const [_k, v] of this.peers) {
					usedBandwidth += v.usedBandwidth;
				}
				usedBandwidth /= this.peers.size;

				if (usedBandwidth > this.connectionManagerOptions.pruner.bandwidth) {
					// prune
					return this.pruneConnections();
				}
			} else if (this.connectionManagerOptions.pruner.maxBuffer != null) {
				const queuedBytes = this.getQueuedBytes();
				if (queuedBytes > this.connectionManagerOptions.pruner.maxBuffer) {
					// prune
					return this.pruneConnections();
				}
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
			const out = peer[1].outboundStream;
			sum += out ? out.readableLength : 0;
		}
		return sum;
	}
}

export const waitForReachable = async (
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
			await libs[j].waitFor(libs[i].peerId);
		}
	}
};

export const waitForNeighbour = async (
	...libs: {
		waitFor: (
			peer: PeerId | PublicSignKey,
			options?: { neighbour?: boolean },
		) => Promise<void>;
		peerId: PeerId;
	}[]
) => {
	for (let i = 0; i < libs.length; i++) {
		for (let j = 0; j < libs.length; j++) {
			if (i === j) {
				continue;
			}
			await libs[i].waitFor(libs[j].peerId, { neighbour: true });
			await libs[j].waitFor(libs[i].peerId, { neighbour: true });
		}
	}
};
