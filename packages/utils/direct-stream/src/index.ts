import { EventEmitter, CustomEvent } from '@libp2p/interfaces/events'
import { pipe } from 'it-pipe'
import Queue from 'p-queue'
import { createTopology } from '@libp2p/topology'
import type { PeerId } from '@libp2p/interface-peer-id'
import type { IncomingStreamData, Registrar } from '@libp2p/interface-registrar'
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
import crypto from 'crypto';
import { PeerMap } from './peer-map.js'
import { Heartbeat, DataMessage, Message, ID_LENGTH } from './encoding.js'
export { Heartbeat, DataMessage, Message }

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
	id: PeerId
	protocol: string
}



/**
 * Thin wrapper around a peer's inbound / outbound pubsub streams
 */
export class PeerStreams extends EventEmitter<PeerStreamEvents> {
	public readonly id: PeerId
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

		this.id = init.id
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
			const id = this.id.toString()
			throw new Error('No writable connection to ' + id)
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

export abstract class DirectStream<Events extends { [s: string]: any } = StreamEvents> extends EventEmitter<Events> {

	public libp2p: Libp2p;
	public peerIdStr: string;
	private session: Uint8Array

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
	private _registrarTopologyIds: string[] | undefined
	private readonly maxInboundStreams: number
	private readonly maxOutboundStreams: number

	public heartbeatInterval: number
	private heartbeat: any


	constructor(libp2p: Libp2p, multicodecs: string[], props: { canRelayMessage?: boolean, heartbeatInterval?: number, emitSelf?: boolean, messageProcessingConcurrency?: number, maxInboundStreams?: number, maxOutboundStreams?: number }) {
		super()
		const {
			canRelayMessage = false,
			emitSelf = false,
			messageProcessingConcurrency = 10,
			maxInboundStreams = 6,
			maxOutboundStreams = 6,
			heartbeatInterval = 10 * 1000
		} = props

		this.heartbeatInterval = heartbeatInterval;
		this.libp2p = libp2p
		this.peerIdStr = libp2p.peerId.toString();
		this.session = crypto.randomBytes(32);
		this.multicodecs = multicodecs
		this.started = false
		this.peers = new PeerMap<PeerStreams>()
		this.routes = new Routes(this.peerIdStr, { ttl: this.routeTTL });
		this.canRelayMessage = canRelayMessage
		this.emitSelf = emitSelf
		this.queue = new Queue({ concurrency: messageProcessingConcurrency })
		this.maxInboundStreams = maxInboundStreams
		this.maxOutboundStreams = maxOutboundStreams
		this.seenCache = new LRU({ ttl: 60 * 1000 })
		this._onIncomingStream = this._onIncomingStream.bind(this)
		this._onPeerConnected = this._onPeerConnected.bind(this)
		this._onPeerDisconnected = this._onPeerDisconnected.bind(this)
	}

	get routeTTL(): number {
		return this.heartbeatInterval * 2;
	}

	// LIFECYCLE METHODS

	/**
	 * Register the pubsub protocol onto the libp2p node.
	 *
	 * @returns {void}
	 */
	async start() {
		if (this.started) {
			return
		}

		logger.info('starting')

		const registrar = this.libp2p.registrar
		// Incoming streams
		// Called after a peer dials us
		await Promise.all(this.multicodecs.map(async multicodec => await registrar.handle(multicodec, this._onIncomingStream, {
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

		this.heartbeat = setInterval(() => {
			this.publishMessage(this.libp2p.peerId, new Heartbeat())
		}, this.heartbeatInterval)


	}

	/**
	 * Unregister the pubsub protocol and the streams with other peers will be closed.
	 */
	async stop() {
		if (!this.started) {
			return
		}

		this.heartbeat && clearInterval(this.heartbeat)

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

		this.peers.clear()
		this.seenCache.clear();
		this.started = false
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

		const peer = this.addPeer(peerId, stream.stat.protocol)
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

			const peer = this.addPeer(peerId, stream.stat.protocol)
			await peer.attachOutboundStream(stream)
			const peerIdStr = peerId.toString();
			this.routes.add(this.peerIdStr, peerIdStr);

			/* const promises: Promise<any>[] = [];

			const routesToShare = [...this.routes.map.values()].map(p => [p.a, p.b]).filter(arr => arr[0] !== peerIdStr && arr[1] !== peerIdStr) as [string, string][];
			if (routesToShare.length > 0) {
				// Share my connections with this peer, except info about the connection that exist between me and 'peer' (its already known)
				promises.push(this.publishMessage(this.libp2p.peerId, new Connections(routesToShare), [peer]));
			}
			
			// Notify network (other peers than the one we are connecting to)
			const others = [...this.peers.values()].filter(peer => !peer.id.equals(peerId));
			if (others.length > 0) {
				promises.push(this.publishMessage(this.libp2p.peerId, new Heartbeat(), others))
			}
			return Promise.all(promises)*/

			await this.publishMessage(this.libp2p.peerId, new Heartbeat(), [peer]) // Sent heartbeat to new peer to notifiy peers network of me

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
	protected _onPeerDisconnected(peerId: PeerId) {
		const idB58Str = peerId.toString()
		logger.info('connection ended', idB58Str)
		this._removePeer(peerId)
		this.routes.add(this.peerIdStr, peerId.toString());

		// Notify network 
		/* return this.publishMessage(this.libp2p.peerId, new ConnectionClosed(peerId, this.session)) */
	}

	/**
	 * Notifies the router that a peer has been connected
	 */
	addPeer(peerId: PeerId, protocol: string): PeerStreams {
		const existing = this.peers.get(peerId)

		// If peer streams already exists, do nothing
		if (existing != null) {
			return existing
		}

		// else create a new peer streams
		const peerIdStr = peerId.toString();
		logger.info('new peer' + peerIdStr)

		const peerStreams: PeerStreams = new PeerStreams({
			id: peerId,
			protocol
		})

		this.peers.set(peerId, peerStreams)

		peerStreams.addEventListener('close', () => this._removePeer(peerId), {
			once: true
		})

		return peerStreams
	}

	/**
	 * Notifies the router that a peer has been disconnected
	 */
	protected _removePeer(peerId: PeerId) {
		const peerStreams = this.peers.get(peerId)

		if (peerStreams == null) {
			return
		}

		// close peer streams
		peerStreams.close()


		// delete peer streams
		const peerIdStr = peerId.toString();
		logger.info('delete peer' + peerIdStr)
		this.peers.delete(peerId)
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
			this._onPeerDisconnected(peerStreams.id)
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
		const message: DataMessage | Heartbeat | undefined = Message.deserialize(msg);

		this.dispatchEvent(new CustomEvent('message', {
			detail: message
		}))
		if (message instanceof DataMessage) {
			const isFromSelf = this.libp2p.peerId.equals(from)
			if (!isFromSelf || this.emitSelf) {
				let isForAll = message.to.length === 0;
				let isForMe = !isForAll && message.to.find(x => x === this.peerIdStr)
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
		/* else if (message instanceof Heartbeat || message instanceof ConnectionClosed) {
			message.trace.push(from.toString());
			for (let i = message instanceof ConnectionClosed ? 1 : 0; i < message.trace.length - 1; i++) {
				this.routes.add(message.trace[i], message.trace[i + 1])
			}

			if (message instanceof ConnectionClosed) {
				let neighbour = message.trace[1] || this.peerIdStr;
				this.routes.delete(message.trace[0], neighbour);
			}

			// Forward
			await this.publishMessage(from, message)
		} */
		else if (message instanceof Heartbeat) {
			message.trace.push(from.toString());
			for (let i = 0; i < message.trace.length - 1; i++) {
				this.routes.add(message.trace[i], message.trace[i + 1])
			}

			// Forward
			await this.publishMessage(from, message)
		}
		/* else if (message instanceof Connections) {
			message.connections.forEach(([a, b]) => {
				this.routes.add(a, b);
			})
		}
 */

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
	async publish(data: Uint8Array, options?: { to?: (string | PeerId)[] }): Promise<void> {
		if (!this.started) {
			throw new Error('Not started')
		}

		const msgId = this.getMsgId(data);
		this.seenCache.set(msgId, true);


		// dispatch the event if we are interested
		const message = new DataMessage({ data, to: options?.to });
		if (this.emitSelf) {
			super.dispatchEvent(new CustomEvent('data', {
				detail: message
			}))

		}

		// send to all the other peers
		await this.publishMessage(this.libp2p.peerId, message)

	}

	private async publishMessage(from: PeerId, message: Message, to?: PeerStreams[] | PeerMap<PeerStreams>): Promise<void> {

		let peers: PeerStreams[] | PeerMap<PeerStreams>;
		if (!to) {
			if (message instanceof DataMessage && message.to.length > 0) {
				peers = [];
				for (const to of message.to) {
					try {
						const path = this.routes.getPath(this.peerIdStr, to);
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
		peers.forEach(_id => {
			let id = _id as PeerStreams;
			if (this.libp2p.peerId.equals(id.id)) {
				logger.info('not sending message to myself')
				return
			}

			if (id.id.equals(from)) {
				logger.info('not sending messageto sender', id.id)
				return
			}

			logger.info('publish msgs on %p', id)
			if (!id.isWritable) {
				logger.error('Cannot send RPC to %p as there is no open stream to it available', id.id)
				return
			}

			id.write(bytes)
		})
	}

}