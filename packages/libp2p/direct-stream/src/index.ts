import { EventEmitter, CustomEvent } from "@libp2p/interfaces/events";
import { pipe } from "it-pipe";
import Queue from "p-queue";
import { createTopology } from "@libp2p/topology";
import type { PeerId } from "@libp2p/interface-peer-id";
import type { IncomingStreamData } from "@libp2p/interface-registrar";
import type { Connection } from "@libp2p/interface-connection";
import type { Pushable } from "it-pushable";
import { pushable } from "it-pushable";
import type { Stream } from "@libp2p/interface-connection";
import { Uint8ArrayList } from "uint8arraylist";
import type { PeerStreamEvents } from "@libp2p/interface-pubsub";
import { abortableSource } from "abortable-iterator";
import * as lp from "it-length-prefixed";
import { Libp2p } from "libp2p";
import { Routes } from "./routes.js";

import { PeerMap } from "./peer-map.js";
import { Hello, DataMessage, Message, Goodbye } from "./messages.js";
import { waitFor } from "@dao-xyz/peerbit-time";
import {
	getKeypairFromPeerId,
	getPublicKeyFromPeerId,
	PublicSignKey,
	SignatureWithKey,
} from "@dao-xyz/peerbit-crypto";
export {
	Message as Message,
	Goodbye,
	Hello,
	DataMessage,
	MessageHeader,
} from "./messages.js";
export type SignaturePolicy = "StictSign" | "StrictNoSign";

import { sha256Base64 } from "@dao-xyz/peerbit-crypto";
import { logger } from "./logger.js";
import { Cache } from "./cache.js";
export { logger };
export interface PeerStreamsInit {
	peerId: PeerId;
	publicKey: PublicSignKey;
	protocol: string;
}

/**
 * Thin wrapper around a peer's inbound / outbound pubsub streams
 */
export class PeerStreams extends EventEmitter<PeerStreamEvents> {
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
	private _rawOutboundStream?: Stream;
	/**
	 * The raw inbound stream, as retrieved from the callback from libp2p.handle
	 */
	private _rawInboundStream?: Stream;
	/**
	 * An AbortController for controlled shutdown of the  treams
	 */
	private readonly inboundAbortController: AbortController;

	private closed: boolean;

	constructor(init: PeerStreamsInit) {
		super();

		this.peerId = init.peerId;
		this.publicKey = init.publicKey;
		this.protocol = init.protocol;
		this.inboundAbortController = new AbortController();
		this.closed = false;
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
			pipe(this._rawInboundStream, lp.decode({ maxDataLength: 100001000 })),
			this.inboundAbortController.signal,
			{
				returnOnAbort: true,
				onReturnError: (err) => {
					logger.error("Inbound stream error", err?.message);
				},
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
		if (this.outboundStream != null) {
			/* 		if (fromExisting) {
						return;
					} */
			// End the stream without emitting a close event
			await this.outboundStream!.end();
			//await this._rawOutboundStream?.close();
		}

		this._rawOutboundStream = stream;
		this.outboundStream = pushable<Uint8ArrayList>({
			objectMode: true,
			onEnd: () => {
				if (this._rawOutboundStream) {
					this._rawOutboundStream.close();
					this.dispatchEvent(new CustomEvent("close"));
				}
				this._rawOutboundStream = undefined;
				this.outboundStream = undefined;
			},
		});

		pipe(this.outboundStream, lp.encode(), this._rawOutboundStream).catch(
			(err: Error) => {
				logger.error(err);
			}
		);

		// Only emit if the connection is new
		if (_prevStream == null) {
			this.dispatchEvent(new CustomEvent("stream:outbound"));
		}

		return this.outboundStream;
	}

	/**
	 * Closes the open connection to peer
	 */
	close() {
		if (this.closed) {
			return;
		}

		this.closed = true;
		// End the outbound stream
		if (this.outboundStream != null) {
			this.outboundStream.return();
			this._rawOutboundStream?.close();
		}
		// End the inbound stream
		if (this.inboundStream != null) {
			this.inboundAbortController.abort();
			this._rawInboundStream?.close();
		}

		//this.dispatchEvent(new CustomEvent('close'))
		this._rawOutboundStream = undefined;
		this.outboundStream = undefined;
		this._rawInboundStream = undefined;
		this.inboundStream = undefined;
	}
}

export interface PeerEvents {
	"peer:reachable": CustomEvent<PublicSignKey>;
	"peer:unreachable": CustomEvent<PublicSignKey>;
}
export interface MessageEvents {
	message: CustomEvent<Message>;
}

export interface StreamEvents extends PeerEvents, MessageEvents {
	data: CustomEvent<DataMessage>;
}

export type DirectStreamOptions = {
	canRelayMessage?: boolean;
	emitSelf?: boolean;
	messageProcessingConcurrency?: number;
	maxInboundStreams?: number;
	maxOutboundStreams?: number;
	signaturePolicy?: SignaturePolicy;
};

export abstract class DirectStream<
	Events extends { [s: string]: any } = StreamEvents
> extends EventEmitter<Events> {
	public libp2p: Libp2p;
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
	public seenCache: Cache;
	public earlyGoodbyes: Map<string, Goodbye>;
	public hellosToReplay: Map<string, Map<string, Hello>>; // key is hash of publicKey, value is map whey key is hash of signature bytes, and value is latest Hello
	private _registrarTopologyIds: string[] | undefined;
	private readonly maxInboundStreams: number;
	private readonly maxOutboundStreams: number;
	private topology: any;
	private startConnections: Set<string> = new Set();
	constructor(
		libp2p: Libp2p,
		multicodecs: string[],
		props?: DirectStreamOptions
	) {
		super();
		const {
			canRelayMessage = false,
			emitSelf = false,
			messageProcessingConcurrency = 10,
			maxInboundStreams = 1, // TODO, should this be 1, why can't this be one (tests fail)
			maxOutboundStreams = 1, // TODO, should this be 1, why can't this be one (tests fail)
			signaturePolicy = "StictSign",
		} = props || {};

		this.libp2p = libp2p;
		const signKey = getKeypairFromPeerId(this.libp2p.peerId);
		this.sign = (async (bytes) =>
			new SignatureWithKey({
				publicKey: signKey.publicKey,
				signature: await signKey.sign(bytes),
			})).bind(this);

		this.peerIdStr = libp2p.peerId.toString();
		this.publicKey = signKey.publicKey;
		this.publicKeyHash = signKey.publicKey.hashcode();
		this.multicodecs = multicodecs;
		this.started = false;
		this.peers = new Map<string, PeerStreams>();
		this.hellosToReplay = new Map();
		this.routes = new Routes(this.publicKeyHash);
		this.canRelayMessage = canRelayMessage;
		this.emitSelf = emitSelf;
		this.queue = new Queue({ concurrency: messageProcessingConcurrency });
		this.earlyGoodbyes = new Map();
		this.maxInboundStreams = maxInboundStreams;
		this.maxOutboundStreams = maxOutboundStreams;
		this.seenCache = new Cache({ max: 1e3, ttl: 10 * 60 });
		this.peerKeyHashToPublicKey = new Map();
		this._onIncomingStream = this._onIncomingStream.bind(this);
		this.onPeerConnected = this.onPeerConnected.bind(this);
		this.onPeerDisconnected = this.onPeerDisconnected.bind(this);
		this.signaturePolicy = signaturePolicy;
	}

	async start() {
		if (this.started) {
			return;
		}

		logger.debug("starting");

		// Incoming streams
		// Called after a peer dials us
		await Promise.all(
			this.multicodecs.map((multicodec) =>
				this.libp2p.handle(multicodec, this._onIncomingStream, {
					maxInboundStreams: this.maxInboundStreams,
					maxOutboundStreams: this.maxOutboundStreams,
				})
			)
		);

		// register protocol with topology
		// Topology callbacks called on connection manager changes
		this.startConnections = new Set([
			...this.libp2p.getConnections().map((conn) => conn.id),
		]);

		this.topology = createTopology({
			onConnect: this.onPeerConnected,
			onDisconnect: this.onPeerDisconnected,
		});
		this._registrarTopologyIds = await Promise.all(
			this.multicodecs.map((multicodec) =>
				this.libp2p.register(multicodec, this.topology)
			)
		);

		this.started = true;

		// All existing connections are like new ones for us
		const multicodecsSet = new Set(this.multicodecs);
		this.libp2p.getConnections().forEach(async (conn) => {
			const has = (
				await this.libp2p.peerStore.get(conn.remotePeer)
			).protocols.find((x) => multicodecsSet.has(x));
			if (has) {
				this.onPeerConnected(conn.remotePeer, conn);
			}
		});
	}

	/**
	 * Unregister the pubsub protocol and the streams with other peers will be closed.
	 */
	async stop() {
		if (!this.started) {
			return;
		}

		await this.libp2p.unhandle(this.multicodecs);

		// unregister protocol and handlers
		if (this._registrarTopologyIds != null) {
			this._registrarTopologyIds?.map((id) => this.libp2p.unregister(id));
		}

		await Promise.all(
			this.multicodecs.map((multicodec) => this.libp2p.unhandle(multicodec))
		);

		logger.debug("stopping");
		for (const peerStreams of this.peers.values()) {
			peerStreams.close();
		}

		this.queue.clear();
		this.hellosToReplay.clear();
		this.earlyGoodbyes.clear();
		this.peers.clear();
		this.seenCache.clear();
		this.started = false;
		this.routes.clear();
		this.peerKeyHashToPublicKey.clear();
		logger.debug("stopped");
	}

	isStarted() {
		return this.started;
	}

	/**
	 * On an inbound stream opened
	 */
	protected async _onIncomingStream(data: IncomingStreamData) {
		const { stream, connection } = data;
		const peerId = connection.remotePeer;
		if (stream.stat.protocol == null) {
			stream.abort(new Error("Stream was not multiplexed"));
			console.error("Recieved non multiplexed stream");
			return;
		}

		const publicKey = getPublicKeyFromPeerId(peerId);
		const peer = this.addPeer(peerId, publicKey, stream.stat.protocol);
		const inboundStream = peer.attachInboundStream(stream);
		this.processMessages(peerId, inboundStream, peer).catch((err) => {
			logger.error(err);
		});
	}

	/**
	 * Registrar notifies an established connection with protocol
	 */
	public async onPeerConnected(peerId: PeerId, conn: Connection) {
		logger.debug("connected " + peerId);
		try {
			const peerKey = getPublicKeyFromPeerId(peerId);
			const peerKeyHash = peerKey.hashcode();

			// let ok = false;
			for (const existingStreams of conn.streams) {
				if (
					existingStreams.stat.protocol &&
					this.multicodecs.includes(existingStreams.stat.protocol) &&
					existingStreams.stat.direction === "outbound"
				) {
					return;
				}
			}

			// This condition seem to work better than the one above, for some reason. The rea
			// The reason we need this at all is because we will connect to existing connection and recieve connection that
			// some times, yields a race connections where connection drop each other by reset
			let stream: Stream;

			try {
				stream = await conn.newStream(this.multicodecs);
				if (stream.stat.protocol == null) {
					stream.abort(new Error("Stream was not multiplexed"));

					console.log("here");
					return;
				}
			} catch (error) {
				if (conn.stat.status !== "OPEN") {
					return; // fail silenty, stream was never intended to be created
				}
				throw error;
			}

			const peer = this.addPeer(peerId, peerKey, stream.stat.protocol);
			await peer.attachOutboundStream(stream);
			this.addRouteConnection(this.publicKey, peerKey);

			const promises: Promise<any>[] = [];

			// Say hello
			promises.push(
				this.publishMessage(
					this.libp2p.peerId,
					await new Hello().sign(this.sign),
					[peer]
				)
			);

			// Send my goodbye early if I disconnect for some reason, (so my peer can say goodbye for me)
			// TODO add custom condition fn for doing below
			promises.push(
				this.publishMessage(
					this.libp2p.peerId,
					await new Goodbye({ early: true }).sign(this.sign),
					[peer]
				)
			);

			// replay all hellos
			for (const [sender, hellos] of this.hellosToReplay) {
				if (sender === peerKeyHash) {
					// Don't say hellos from sender to same sender (uneccessary)
					continue;
				}
				for (const [key, hello] of hellos) {
					if (!(await hello.header.verify())) {
						hellos.delete(key);
					}
					promises.push(this.publishMessage(this.libp2p.peerId, hello, [peer]));
				}
			}

			const resolved = await Promise.all(promises);
			return resolved;
		} catch (err: any) {
			logger.error(err);

			if (err.code === "ERR_UNSUPPORTED_PROTOCOL") {
				return;
			}
		}
	}

	private addRouteConnection(from: PublicSignKey, to: PublicSignKey) {
		this.peerKeyHashToPublicKey.set(from.hashcode(), from);
		this.peerKeyHashToPublicKey.set(to.hashcode(), to);
		this.routes.addLink(from.hashcode(), to.hashcode()).forEach((added) => {
			const key = this.peerKeyHashToPublicKey.get(added);
			if (key?.equals(this.publicKey) === false) {
				this.onPeerReachable(key!);
			} else {
				const x = 123;
			}
		});
	}

	removeRouteConnection(from: PublicSignKey, to: PublicSignKey) {
		this.routes
			.deleteLink(from.hashcode(), to.hashcode())
			.forEach((deleted) => {
				const key = this.peerKeyHashToPublicKey.get(deleted)!;
				this.peerKeyHashToPublicKey.delete(deleted);
				if (key?.equals(this.publicKey) === false) {
					this.onPeerUnreachable(key!);
				}
			});
	}

	/**
	 * Registrar notifies a closing connection with pubsub protocol
	 */
	protected async onPeerDisconnected(peerId: PeerId) {
		// PeerId could be me, if so, it means that I am disconnecting
		const peerKey = getPublicKeyFromPeerId(peerId);
		const peerKeyHash = peerKey.hashcode();
		logger.debug("connection ended", peerKey.toString());
		this._removePeer(peerKey);
		if (!this.publicKey.equals(peerKey)) {
			this.removeRouteConnection(this.publicKey, peerKey);
		}

		this.startConnections.clear();
		// Notify network
		const earlyGoodBye = this.earlyGoodbyes.get(peerKeyHash);
		if (earlyGoodBye) {
			earlyGoodBye.early = false;
			await earlyGoodBye.sign(this.sign);
			await this.publishMessage(this.libp2p.peerId, earlyGoodBye);
			this.earlyGoodbyes.delete(peerKeyHash);
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
	public onPeerUnreachable(publicKey: PublicSignKey) {
		// override this fn
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
		protocol: string
	): PeerStreams {
		const publicKeyHash = publicKey.hashcode();
		const existing = this.peers.get(publicKeyHash);

		// If peer streams already exists, do nothing
		if (existing != null) {
			return existing;
		}

		// else create a new peer streams
		const peerIdStr = peerId.toString();
		logger.debug("new peer" + peerIdStr);

		const peerStreams: PeerStreams = new PeerStreams({
			peerId,
			publicKey,
			protocol,
		});

		this.peers.set(publicKeyHash, peerStreams);
		peerStreams.addEventListener("close", () => this._removePeer(publicKey), {
			once: true,
		});
		return peerStreams;
	}

	/**
	 * Notifies the router that a peer has been disconnected
	 */
	protected _removePeer(publicKey: PublicSignKey) {
		const hash = publicKey.hashcode();
		const peerStreams = this.peers.get(hash);

		if (peerStreams == null) {
			return;
		}

		// close peer streams
		peerStreams.close();

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
					const msgId = await this.getMsgId(data);

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

		logger.debug("rpc from " + from + ", " + this.peerIdStr);

		if (message.length > 0) {
			logger.debug("messages from " + from);
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

		if (this.libp2p.peerId.equals(from) && !this.emitSelf) {
			return;
		}

		// Ensure the message is valid before processing it
		const message: Message | undefined = Message.deserialize(msg);

		this.dispatchEvent(
			new CustomEvent("message", {
				detail: message,
			})
		);

		if (message instanceof DataMessage) {
			await this.onDataMessage(from, peerStream, message);
		} else if (message instanceof Hello) {
			await this.onHello(from, peerStream, message);
		} else if (message instanceof Goodbye) {
			await this.onGoodbye(from, peerStream, message);
		}
	}

	async onDataMessage(
		from: PeerId,
		peerStream: PeerStreams,
		message: DataMessage
	) {
		const isFromSelf = this.libp2p.peerId.equals(from);
		if (!isFromSelf || this.emitSelf) {
			const isForAll = message.to.length === 0;
			const isForMe =
				!isForAll && message.to.find((x) => x === this.publicKeyHash);
			if (isForAll || isForMe) {
				if (
					this.signaturePolicy === "StictSign" &&
					(!(await message.verify(this.signaturePolicy === "StictSign"))
						? true
						: false)
				) {
					// we don't verify messages we don't dispatch because of the performance penalty // TODO add opts for this
					logger.warn("Recieved message with invalid signature or timestamp");
					return false;
				}

				this.dispatchEvent(
					new CustomEvent("data", {
						detail: message,
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
		if (!(await message.verify(true))) {
			logger.warn("Recieved message with invalid signature or timestamp");
			return false;
		}

		const sender = message.sender?.hashcode();
		if (!sender) {
			logger.warn("Recieved hello without sender");
			return false;
		}

		const signatures = message.signatures;
		for (let i = 0; i < signatures.signatures.length - 1; i++) {
			//	console.log('add route', this.publicKeyHash, signatures.signatures[i].publicKey.hashcode(), signatures.signatures[i + 1].publicKey.hashcode())
			this.addRouteConnection(
				signatures.signatures[i].publicKey,
				signatures.signatures[i + 1].publicKey
			);
		}

		await message.sign(this.sign); // sign it so othere peers can now I have seen it (and can build a network graph from trace info)

		let hellos = this.hellosToReplay.get(sender);
		if (!hellos) {
			hellos = new Map();
			this.hellosToReplay.set(sender, hellos);
		}

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
		if (!(await message.verify(true))) {
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
			for (let i = 1; i < signatures.signatures.length - 1; i++) {
				this.addRouteConnection(
					signatures.signatures[i].publicKey,
					signatures.signatures[i + 1].publicKey
				);
			}

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

			const hellos = this.hellosToReplay.get(sender);
			if (hellos) {
				const helloSignaturHash = await message.signatures.hashPublicKeys();
				hellos.delete(helloSignaturHash);
			}

			// Forward
			await this.relayMessage(from, message);
		}
		return true;
	}

	/**
	 * The default msgID implementation
	 * Child class can override this.
	 */
	public async getMsgId(msg: Uint8ArrayList | Uint8Array) {
		// first bytes is discriminator,
		// next 32 bytes should be an id
		//return  Buffer.from(msg.slice(0, 33)).toString('base64');

		return sha256Base64(msg.subarray(0, 33)); // base64EncArr(msg, 0, ID_LENGTH + 1);
	}

	/**
	 * Whether to accept a message from a peer
	 * Override to create a graylist
	 */
	acceptFrom(id: PeerId) {
		return true;
	}

	/**
	 * Publishes messages to all peers
	 */
	async publish(
		data: Uint8Array | Uint8ArrayList,
		options?: { to?: (string | PublicSignKey | PeerId)[] | Set<string> }
	): Promise<void> {
		if (!this.started) {
			throw new Error("Not started");
		}

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
			to: toHashes,
		});
		if (this.signaturePolicy === "StictSign") {
			await message.sign(this.sign);
		}

		if (this.emitSelf) {
			super.dispatchEvent(
				new CustomEvent("data", {
					detail: message,
				})
			);
		}

		// send to all the other peers
		await this.publishMessage(this.libp2p.peerId, message, undefined);
	}

	public async hello(data?: Uint8Array): Promise<void> {
		if (!this.started) {
			throw new Error("Not started");
		}

		// send to all the other peers
		await this.publishMessage(
			this.libp2p.peerId,
			await new Hello({ data }).sign(this.sign.bind(this))
		);
	}

	public async relayMessage(
		from: PeerId,
		message: Message,
		to?: PeerStreams[] | PeerMap<PeerStreams>
	) {
		if (this.canRelayMessage) {
			return this.publishMessage(from, message, to);
		} else {
			logger.debug("received message we didn't subscribe to. Dropping.");
		}
	}
	public async publishMessage(
		from: PeerId,
		message: Message,
		to?: PeerStreams[] | PeerMap<PeerStreams>
	): Promise<void> {
		let peers: PeerStreams[] | PeerMap<PeerStreams>;
		if (!to) {
			if (message instanceof DataMessage && message.to.length > 0) {
				peers = [];
				for (const to of message.to) {
					try {
						const path = this.routes.getPath(this.publicKeyHash, to);
						if (path && path.length > 0) {
							const stream = this.peers.get(path[1].id.toString());
							if (stream) {
								peers.push(stream);
								continue;
							}
						}
					} catch (error) {
						// Can't find path
					}

					// we can't find path, send message to all peers
					peers = this.peers;
					break;
				}
			} else {
				peers = this.peers;
			}
		} else {
			peers = to;
		}
		if (message instanceof DataMessage) {
			const meTo = message.to.findIndex(
				(value) => value === this.publicKeyHash
			);
			if (meTo >= 0) {
				message.to.splice(meTo, 1); // Delete me from to list
			}
		}

		if (
			peers == null ||
			(Array.isArray(peers) && peers.length === 0) ||
			(peers instanceof Map && peers.size === 0)
		) {
			logger.info("no peers are subscribed");
			return;
		}

		const bytes = message.serialize();
		this.seenCache.add(await this.getMsgId(bytes));
		const promises: Promise<any>[] = [];
		for (const stream of peers.values()) {
			const id = stream as PeerStreams;

			if (this.libp2p.peerId.equals(id.peerId)) {
				logger.trace("not sending message to myself");
				continue;
			}

			if (id.peerId.equals(from)) {
				continue;
			}

			logger.debug("publish msgs on: " + id.peerId + " from " + this.peerIdStr);
			if (!id.isWritable) {
				// Catch the event where the outbound stream is attach, but also abort if we shut down
				const outboundPromise = new Promise<void>((rs, rj) => {
					const resolve = () => {
						id.removeEventListener("stream:outbound", listener);
						clearTimeout(timer);
						rs();
					};
					const reject = () => {
						id.removeEventListener("stream:outbound", listener);
						clearTimeout(timer);
						rj();
					};
					const timer = setTimeout(() => {
						reject();
					}, 10 * 1000);
					const abortHandler = () => {
						id.removeEventListener("close", abortHandler);
						reject();
					};
					id.addEventListener("close", abortHandler);

					const listener = () => {
						resolve();
					};
					id.addEventListener("stream:outbound", listener);
					if (id.isWritable) {
						resolve();
					}
				});

				outboundPromise
					.then(() => {
						id.write(bytes);
					})
					.catch((error) => {
						logger.error(
							"Failed to send to stream: " + id.peerId + ". " + error.message
						);
					});
			} else {
				id.write(bytes);
			}
		}
		await Promise.all(promises);
	}
}

export const waitForPeers = async (...libs: DirectStream<any>[]) => {
	for (let i = 0; i < libs.length; i++) {
		await waitFor(() => {
			for (let j = 0; j < libs.length; j++) {
				if (i === j) {
					continue;
				}
				if (!libs[i].peers.has(libs[j].publicKeyHash)) {
					return false;
				}
			}
			return true;
		});
		const peers = libs[i].peers;
		for (const peer of peers.values()) {
			await waitFor(() => peer.isReadable && peer.isWritable);
		}
	}
};
