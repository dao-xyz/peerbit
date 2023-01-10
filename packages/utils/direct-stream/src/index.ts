import { EventEmitter, CustomEvent } from '@libp2p/interfaces/events'
import { pipe } from 'it-pipe'
import Queue from 'p-queue'
import { createTopology } from '@libp2p/topology'
import type { PeerId } from '@libp2p/interface-peer-id'
import type { IncomingStreamData } from '@libp2p/interface-registrar'
import type { Connection } from '@libp2p/interface-connection'
import type { Pushable } from 'it-pushable'
import { pushable } from 'it-pushable'
import type { Stream } from '@libp2p/interface-connection'
import { Uint8ArrayList } from 'uint8arraylist'
import type { PeerStreamEvents } from '@libp2p/interface-pubsub'
import { logger as logFn } from '@dao-xyz/peerbit-logger'
import { abortableSource } from 'abortable-iterator'
import * as lp from 'it-length-prefixed'
import { Libp2p } from 'libp2p'
import { Routes } from './routes.js'
import LRU from "lru-cache";
import { PeerMap } from './peer-map.js'
import { Hello, DataMessage, Message, ID_LENGTH, NetworkInfo, Goodbye } from './encoding.js'
import { Uint8ArrayView, viewAsArray, viewFromBytes } from './view.js';
import { waitFor } from '@dao-xyz/peerbit-time'
import { getKeypairFromPeerId, getPublicKeyFromPeerId, PublicSignKey, SignatureWithKey } from '@dao-xyz/peerbit-crypto'

export type { Uint8ArrayView };
export { Message as Message, Goodbye as ConnectionClosed, Hello as ConnectionOpen, NetworkInfo as Connections, DataMessage } from './encoding.js';
export { viewAsArray, viewFromBytes };

export * as utf8 from './utf8.js';
const logger = logFn({ module: 'trace-messages', level: 'warn' })
function uint6ToB64(nUint6) {
	return nUint6 < 26
		? nUint6 + 65
		: nUint6 < 52
			? nUint6 + 71
			: nUint6 < 62
				? nUint6 - 4
				: nUint6 === 62
					? 43
					: nUint6 === 63
						? 47
						: 65;
}

function base64EncArr(aBytes: Uint8ArrayList | Uint8Array, offset: number, length: number) {

	let nMod3 = 2;
	let sB64Enc = "";

	const nLen = length;
	let nUint24 = 0;
	const get = aBytes instanceof Uint8ArrayList ? (i: number) => aBytes.get(i) : (i: number) => aBytes[i]
	for (let nIdx = 0; nIdx < nLen - offset; nIdx++) {
		nMod3 = nIdx % 3;
		if (nIdx > 0 && ((nIdx * 4) / 3) % 76 === 0) {
			sB64Enc += "\r\n";
		}
		nUint24 |= get(nIdx + offset) << ((16 >>> nMod3) & 24);
		if (nMod3 === 2 || nLen - nIdx === 1) {
			sB64Enc += String.fromCodePoint(
				uint6ToB64((nUint24 >>> 18) & 63),
				uint6ToB64((nUint24 >>> 12) & 63),
				uint6ToB64((nUint24 >>> 6) & 63),
				uint6ToB64(nUint24 & 63)
			);
			nUint24 = 0;
		}
	}
	return (
		sB64Enc.substr(0, sB64Enc.length - 2 + nMod3) +
		(nMod3 === 2 ? "" : nMod3 === 1 ? "=" : "==")
	);
}



export interface PeerStreamsInit {
	peerId: PeerId
	publicKey: PublicSignKey
	protocol: string
}



/**
 * Thin wrapper around a peer's inbound / outbound pubsub streams
 */
export class PeerStreams extends EventEmitter<PeerStreamEvents> {
	public readonly peerId: PeerId
	public readonly publicKey: PublicSignKey
	public readonly protocol: string
	/**
	 * Write stream - it's preferable to use the write method
	 */
	public outboundStream?: Pushable<Uint8ArrayList>
	/**
	 * Read stream
	 */
	public inboundStream?: AsyncIterable<Uint8ArrayList>
	/**
	 * The raw outbound stream, as retrieved from conn.newStream
	 */
	private _rawOutboundStream?: Stream
	/**
	 * The raw inbound stream, as retrieved from the callback from libp2p.handle
	 */
	private _rawInboundStream?: Stream
	/**
	 * An AbortController for controlled shutdown of the inbound stream
	 */
	private readonly _inboundAbortController: AbortController
	private closed: boolean

	constructor(init: PeerStreamsInit) {
		super()

		this.peerId = init.peerId;
		this.publicKey = init.publicKey
		this.protocol = init.protocol
		this._inboundAbortController = new AbortController()
		this.closed = false
	}

	/**
	 * Do we have a connection to read from?
	 */
	get isReadable() {
		return Boolean(this.inboundStream)
	}

	/**
	 * Do we have a connection to write on?
	 */
	get isWritable() {
		return Boolean(this.outboundStream)
	}

	/**
	 * Send a message to this peer.
	 * Throws if there is no `stream` to write to available.
	 */
	write(data: Uint8Array | Uint8ArrayList) {
		if (this.outboundStream == null) {
			throw new Error('No writable connection to ' + this.peerId.toString())
		}
		this.outboundStream.push(data instanceof Uint8Array ? new Uint8ArrayList(data) : data)
	}

	/**
	 * Attach a raw inbound stream and setup a read stream
	 */
	attachInboundStream(stream: Stream) {
		// Create and attach a new inbound stream
		// The inbound stream is:
		// - abortable, set to only return on abort, rather than throw
		// - transformed with length-prefix transform
		this._rawInboundStream = stream
		this.inboundStream = abortableSource(
			pipe(
				this._rawInboundStream,
				lp.decode({ maxDataLength: 100001000 })
			),
			this._inboundAbortController.signal,
			{
				returnOnAbort: true,
				onReturnError: (err) => {
					logger.error('Inbound stream error', err?.message)
				}
			}
		)

		this.dispatchEvent(new CustomEvent('stream:inbound'))
		return this.inboundStream
	}

	/**
	 * Attach a raw outbound stream and setup a write stream
	 */
	async attachOutboundStream(stream: Stream) {
		// If an outbound stream already exists, gently close it
		const _prevStream = this.outboundStream
		if (this.outboundStream != null) {
			// End the stream without emitting a close event
			await this.outboundStream.end()
		}

		this._rawOutboundStream = stream
		this.outboundStream = pushable<Uint8ArrayList>({
			objectMode: true,
			onEnd: () => {
				this._rawOutboundStream?.close?.()
				this._rawOutboundStream = undefined
				this.outboundStream = undefined
				// this.dispatchEvent(new CustomEvent('close'))

			}
		})

		pipe(
			this.outboundStream,
			lp.encode(),
			this._rawOutboundStream
		).catch((err: Error) => {
			logger.error(err)
		})

		// Only emit if the connection is new
		if (_prevStream == null) {
			this.dispatchEvent(new CustomEvent('stream:outbound'))
		}

		return this.outboundStream
	}

	/**
	 * Closes the open connection to peer
	 */
	close() {
		if (this.closed) {
			return
		}

		this.closed = true
		// End the outbound stream
		if (this.outboundStream != null) {
			this.outboundStream.return();
			this._rawOutboundStream?.close();

		}
		// End the inbound stream
		if (this.inboundStream != null) {
			this._inboundAbortController.abort()
			this._rawInboundStream?.close();
		}

		this._rawOutboundStream = undefined
		this.outboundStream = undefined
		this._rawInboundStream = undefined
		this.inboundStream = undefined

	}
}


export interface StreamEvents {
	'message': CustomEvent<Message>,
	'data': CustomEvent<DataMessage>
}

export type DirectStreamOptions = { canRelayMessage?: boolean, emitSelf?: boolean, messageProcessingConcurrency?: number, maxInboundStreams?: number, maxOutboundStreams?: number };
export abstract class DirectStream<Events extends { [s: string]: any } = StreamEvents> extends EventEmitter<Events> {

	public libp2p: Libp2p;
	public peerIdStr: string;
	public publicKey: PublicSignKey;
	public publicKeyHash: string;
	public sign: (bytes: Uint8Array) => SignatureWithKey

	public started: boolean
	/**
	 * Map of peer streams
	 */
	public peers: PeerMap<PeerStreams>
	public routes: Routes;
	/**
	 * If router can relay received messages, even if not subscribed
	 */
	public canRelayMessage: boolean
	/**
	 * if publish should emit to self, if subscribed
	 */

	public emitSelf: boolean
	public queue: Queue
	public multicodecs: string[]
	public seenCache: LRU<string, boolean>
	public earlyGoodbyes: Map<string, Goodbye>;
	private _registrarTopologyIds: string[] | undefined
	private readonly maxInboundStreams: number
	private readonly maxOutboundStreams: number

	constructor(libp2p: Libp2p, multicodecs: string[], props?: DirectStreamOptions) {
		super()
		const {
			canRelayMessage = false,
			emitSelf = false,
			messageProcessingConcurrency = 10,
			maxInboundStreams = 6,
			maxOutboundStreams = 6,
		} = props || {}

		this.libp2p = libp2p
		const signKey = getKeypairFromPeerId(this.libp2p.peerId)
		this.sign = ((bytes) => new SignatureWithKey({
			publicKey: signKey.publicKey,
			signature: signKey.sign(bytes)
		})).bind(this)

		this.peerIdStr = libp2p.peerId.toString();
		this.publicKey = signKey.publicKey;
		this.publicKeyHash = signKey.publicKey.hashcode();
		this.multicodecs = multicodecs
		this.started = false
		this.peers = new PeerMap<PeerStreams>()
		this.routes = new Routes(this.publicKeyHash, { ttl: 10000 * 1000 })
		this.canRelayMessage = canRelayMessage
		this.emitSelf = emitSelf
		this.queue = new Queue({ concurrency: messageProcessingConcurrency })
		this.earlyGoodbyes = new Map();
		this.maxInboundStreams = maxInboundStreams
		this.maxOutboundStreams = maxOutboundStreams
		this.seenCache = new LRU({ ttl: 60 * 1000 })
		this._onIncomingStream = this._onIncomingStream.bind(this)
		this._onPeerConnected = this._onPeerConnected.bind(this)
		this._onPeerDisconnected = this._onPeerDisconnected.bind(this)


	}

	async start() {
		if (this.started) {
			return
		}

		logger.info('starting')

		const registrar = this.libp2p.registrar
		// Incoming streams
		// Called after a peer dials us
		await Promise.all(this.multicodecs.map(multicodec => registrar.handle(multicodec, this._onIncomingStream, {
			maxInboundStreams: this.maxInboundStreams,
			maxOutboundStreams: this.maxOutboundStreams
		})))

		// register protocol with topology
		// Topology callbacks called on connection manager changes
		const topology = createTopology({
			onConnect: this._onPeerConnected,
			onDisconnect: this._onPeerDisconnected
		})
		this._registrarTopologyIds = await Promise.all(this.multicodecs.map(async multicodec => await registrar.register(multicodec, topology)))


		logger.info('started')
		this.started = true


		// All existing connections are like new ones for us
		this.libp2p.getConnections().forEach(conn => this._onPeerConnected(conn.remotePeer, conn))


	}

	/**
	 * Unregister the pubsub protocol and the streams with other peers will be closed.
	 */
	async stop() {
		if (!this.started) {
			return
		}

		/* this.heartbeat && clearInterval(this.heartbeat) */

		const registrar = this.libp2p.registrar

		await this.libp2p.unhandle(this.multicodecs)

		// unregister protocol and handlers
		if (this._registrarTopologyIds != null) {
			this._registrarTopologyIds?.map(id => registrar.unregister(id))
		}

		await Promise.all(this.multicodecs.map(async multicodec => await registrar.unhandle(multicodec)))

		logger.info('stopping')
		for (const peerStreams of this.peers.values()) {
			peerStreams.close()

		}

		this.earlyGoodbyes.clear();
		this.peers.clear()
		this.seenCache.clear();
		this.started = false
		this.routes.clear();
		logger.info('stopped')
	}

	isStarted() {
		return this.started
	}

	/**
	 * On an inbound stream opened
	 */
	protected _onIncomingStream(data: IncomingStreamData) {
		const { stream, connection } = data
		const peerId = connection.remotePeer

		if (stream.stat.protocol == null) {
			stream.abort(new Error('Stream was not multiplexed'))
			return
		}

		const publicKey = getPublicKeyFromPeerId(peerId);
		const peer = this.addPeer(peerId, publicKey, stream.stat.protocol)
		const inboundStream = peer.attachInboundStream(stream)

		this.processMessages(peerId, inboundStream, peer)
			.catch(err => {
				logger.error(err)
			})
	}

	/**
	 * Registrar notifies an established connection with pubsub protocol
	 */
	protected async _onPeerConnected(peerId: PeerId, conn: Connection) {
		logger.info('connected %p', peerId)
		try {

			for (const existingStreams of conn.streams) {
				if (this.multicodecs.find(x => x === existingStreams.stat.protocol) && existingStreams.stat.direction === 'outbound') {
					return;
				}
			}
			const stream = await conn.newStream(this.multicodecs)

			if (stream.stat.protocol == null) {
				stream.abort(new Error('Stream was not multiplexed'))
				return
			}

			const peerKey = getPublicKeyFromPeerId(peerId);
			const peer = this.addPeer(peerId, peerKey, stream.stat.protocol)
			await peer.attachOutboundStream(stream)
			this.routes.add(this.publicKeyHash, peerKey.hashcode());

			const promises: Promise<any>[] = [];




			// Notify network (other peers than the one we are connecting to)
			/* 		const others = [...this.peers.values()].filter(peer => !peer.id.equals(peerId));
					if (others.length > 0) {
					} */

			// Say hello
			promises.push(this.publishMessage(this.libp2p.peerId, new Hello().sign(this.sign), [peer]))


			// Send my goodbye early if I disconnect for some reason, (so my peer can say goodbye for me)
			// TODO add custom condition fn for doing below
			promises.push(this.publishMessage(this.libp2p.peerId, new Goodbye({ early: true }).sign(this.sign), [peer]))



			const routesToShare = this.routes.links; /* .filter(arr => arr[0] !== peerIdStr && arr[1] !== peerIdStr) */
			if (routesToShare.length > 0) {
				// Share my connections with this peer, except info about the connection that exist between me and 'peer' (its already known)
				// TODO add custom condition fn for doing below
				promises.push(this.publishMessage(this.libp2p.peerId, new NetworkInfo(routesToShare).sign(this.sign), [peer]));
			}



			const resolved = await Promise.all(promises)
			return resolved;

		} catch (err: any) {
			if (err.code === 'ERR_UNSUPPORTED_PROTOCOL') {
				return;
			}
			logger.error(err)
		}
	}

	/**
	 * Registrar notifies a closing connection with pubsub protocol
	 */
	protected async _onPeerDisconnected(peerId: PeerId) {
		// PeerId could be me, if so, it means that I am disconnecting
		const peerKey = getPublicKeyFromPeerId(peerId);
		const peerKeyHash = peerKey.hashcode();
		logger.info('connection ended', peerKey.toString())
		this._removePeer(peerKey)
		if (!this.publicKey.equals(peerKey)) {
			this.routes.deleteLink(this.publicKeyHash, peerKeyHash);
		}

		// Notify network 
		const earlyGoodBye = this.earlyGoodbyes.get(peerKeyHash)
		if (earlyGoodBye) {
			earlyGoodBye.sign(this.sign);
			await this.publishMessage(this.libp2p.peerId, earlyGoodBye);
			this.earlyGoodbyes.delete(peerKeyHash);
		}
	}

	/**
	 * Notifies the router that a peer has been connected
	 */
	addPeer(peerId: PeerId, publicKey: PublicSignKey, protocol: string): PeerStreams {

		const publicKeyHash = publicKey.hashcode();
		const existing = this.peers.get(publicKeyHash)

		// If peer streams already exists, do nothing
		if (existing != null) {
			return existing
		}

		// else create a new peer streams
		const peerIdStr = peerId.toString();
		logger.info('new peer' + peerIdStr)


		const peerStreams: PeerStreams = new PeerStreams({
			peerId,
			publicKey,
			protocol
		})

		this.peers.set(publicKeyHash, peerStreams)
		peerStreams.addEventListener('close', () => this._removePeer(publicKey), {
			once: true
		})
		return peerStreams
	}

	/**
	 * Notifies the router that a peer has been disconnected
	 */
	protected _removePeer(publicKey: PublicSignKey) {
		const hash = publicKey.hashcode();
		const peerStreams = this.peers.get(hash)

		if (peerStreams == null) {
			return
		}

		// close peer streams
		peerStreams.close()


		// delete peer streams
		logger.info('delete peer' + publicKey.toString())
		this.peers.delete(hash)
		return peerStreams
	}

	// MESSAGE METHODS

	/**
	 * Responsible for processing each RPC message received by other peers.
	 */
	async processMessages(peerId: PeerId, stream: AsyncIterable<Uint8ArrayList>, peerStreams: PeerStreams) {
		try {
			await pipe(
				stream,
				async (source) => {
					for await (const data of source) {

						const msgId = this.getMsgId(data)
						if (this.seenCache.has(msgId)) {

							return
						}
						this.seenCache.set(msgId, true)
						this.processRpc(peerId, peerStreams, data)
							.catch(err => logger.info(err))

					}
				}
			)
		} catch (err: any) {
			logger.error('error on processing messages to id: ' + peerStreams.peerId.toString() + ". " + err?.message)
			this._onPeerDisconnected(peerStreams.peerId)
		}
	}

	/**
	 * Handles an rpc request from a peer
	 */
	async processRpc(from: PeerId, peerStreams: PeerStreams, message: Uint8ArrayList): Promise<boolean> {
		if (!this.acceptFrom(from)) {
			logger.info('received message from unacceptable peer %p', from)
			return false
		}

		logger.info('rpc from %p', from)

		if (message.length > 0) {
			logger.info('messages from %p', from)
			if (!this.canRelayMessage) {
				logger.info('received message we didn\'t subscribe to. Dropping.')
				return false
			}

			await this.queue.add(async () => {
				try {
					await this.processMessage(from, message)
				} catch (err: any) {
					logger.error(err)
				}
			})
				.catch(err => logger.info(err))
		}

		return true
	}


	/**
	 * Handles a message from a peer
	 */
	async processMessage(from: PeerId, msg: Uint8ArrayList) {
		if (!from.publicKey) {
			return;
		}

		if (this.libp2p.peerId.equals(from) && !this.emitSelf) {
			return
		}

		// Ensure the message is valid before processing it
		const message: Message | undefined = Message.deserialize(msg);

		this.dispatchEvent(new CustomEvent('message', {
			detail: message
		}))

		if (!message.verify()) {
			logger.warn("Recieved message with invalid signature or timestamp")
			return;
		}

		if (message instanceof DataMessage) {
			await this.onDataMessage(from, message);
		}
		else if (message instanceof Hello) {
			await this.onHello(from, message)
		}
		else if (message instanceof Goodbye) {
			await this.onGoodbye(from, message)
		}
		else if (message instanceof NetworkInfo) {
			await this.onNetworkInfo(from, message)
		}
	}

	async onDataMessage(from: PeerId, message: DataMessage) {
		const isFromSelf = this.libp2p.peerId.equals(from)
		if (!isFromSelf || this.emitSelf) {
			const isForAll = message.to.length === 0;
			const isForMe = !isForAll && message.to.find(x => x.equals(this.publicKey))
			if (isForAll || isForMe)
				this.dispatchEvent(new CustomEvent('data', {
					detail: message
				}))

			if (isForMe && message.to.length === 1) {
				// dont forward this message anymore because it was meant ONLY for me
				return;
			}
		}
		// Forward
		await this.publishMessage(from, message)
	}
	async onHello(from: PeerId, message: Hello) {
		const signatures = message.signatures;
		for (let i = 0; i < signatures.signatures.length - 1; i++) {
			this.routes.add(signatures.signatures[i].publicKey.hashcode(), signatures.signatures[i + 1].publicKey.hashcode());
		}

		message.sign(this.sign) // sign it so othere peers can now I have seen it (and can build a network graph from trace info)

		// Forward
		await this.publishMessage(from, message)
	}

	async onGoodbye(from: PeerId, message: Goodbye) {
		const peerKey = getPublicKeyFromPeerId(from);
		const peerKeyHash = peerKey.hashcode();
		if (message.early) {
			this.earlyGoodbyes.set(peerKeyHash, message)
		}
		else {

			const signatures = message.signatures;
			for (let i = 1; i < signatures.signatures.length - 1; i++) {
				this.routes.add(signatures.signatures[i].publicKey.hashcode(), signatures.signatures[i + 1].publicKey.hashcode());
			}

			//let neighbour = message.trace[1] || this.peerIdStr;
			this.routes.deleteLink(signatures.signatures[0].publicKey.hashcode(), signatures.signatures[1].publicKey.hashcode() || this.publicKeyHash);

			message.sign(this.sign) // sign it so othere peers can now I have seen it (and can build a network graph from trace info)

			// Forward
			await this.publishMessage(from, message)
		}
	}

	async onNetworkInfo(from: PeerId, message: NetworkInfo) {
		message.connections.connections.forEach(([a, b]) => {
			this.routes.add(a, b);
		})

		// Forward
		await this.publishMessage(from, message)
	}


	/**
	 * The default msgID implementation
	 * Child class can override this.
	 */
	getMsgId(msg: Uint8ArrayList | Uint8Array) {

		// first bytes is discriminator, 
		// next 32 bytes should be an id
		//return  Buffer.from(msg.slice(0, 33)).toString('base64');

		return base64EncArr(msg, 0, ID_LENGTH + 1);
	}

	/**
	 * Whether to accept a message from a peer
	 * Override to create a graylist
	 */
	acceptFrom(id: PeerId) {
		return true
	}

	/**
	 * Publishes messages to all peers
	 */
	async publish(data: Uint8Array | Uint8ArrayList, options?: { to?: (PublicSignKey | PeerId)[] }): Promise<void> {
		if (!this.started) {
			throw new Error('Not started')
		}

		// dispatch the event if we are interested
		const message = new DataMessage({ data, to: options?.to?.map(x => (x instanceof PublicSignKey ? x : getPublicKeyFromPeerId(x))) });
		if (this.emitSelf) {
			super.dispatchEvent(new CustomEvent('data', {
				detail: message
			}))

		}

		// send to all the other peers
		await this.publishMessage(this.libp2p.peerId, message)

	}

	public async hello(data?: Uint8Array): Promise<void> {
		if (!this.started) {
			throw new Error('Not started')
		}

		// send to all the other peers
		await this.publishMessage(this.libp2p.peerId, new Hello({ data }).sign(this.sign.bind(this)))
	}


	public async publishMessage(from: PeerId, message: Message, to?: PeerStreams[] | PeerMap<PeerStreams>): Promise<void> {

		let peers: PeerStreams[] | PeerMap<PeerStreams>;
		if (!to) {
			if (message instanceof DataMessage && message.to.length > 0) {
				peers = [];
				for (const to of message.to) {
					try {
						const path = this.routes.getPath(this.publicKeyHash, to.hashcode());
						if (path && path.length > 0) {
							const stream = this.peers.get(path[1].id.toString());
							if (stream) {
								peers.push(stream)
								continue
							}
						}

					} catch (error) {
						// Can't find path
					}

					// we can't find path, send message to all peers
					peers = this.peers;
					break;
				}
			}
			else {
				peers = this.peers;
			}

		}
		else {
			peers = to;
		}
		if (peers == null || peers.length === 0) {
			logger.info('no peers are subscribed')
			return
		}

		const bytes = message.serialize()

		const msgId = this.getMsgId(bytes);
		this.seenCache.set(msgId, true);

		peers.forEach(stream => {
			const id = stream as PeerStreams;
			if (this.libp2p.peerId.equals(id.peerId)) {
				logger.info('not sending message to myself')
				return
			}

			if (id.peerId.equals(from)) {
				logger.info('not sending messageto sender', id.peerId)
				return
			}

			logger.info('publish msgs on %p', id)
			if (!id.isWritable) {
				logger.error('Cannot send RPC to %p as there is no open stream to it available', id.peerId)
				return
			}

			id.write(bytes)
		})
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
			await waitFor(() => peer.isReadable && peer.isWritable)
		}
	}
}
