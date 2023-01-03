import { EventEmitter, CustomEvent } from '@libp2p/interfaces/events'
import { pipe } from 'it-pipe'
import Queue from 'p-queue'
import { createTopology } from '@libp2p/topology'
import type { PeerId } from '@libp2p/interface-peer-id'
import type { IncomingStreamData, Registrar } from '@libp2p/interface-registrar'
import type { Connection } from '@libp2p/interface-connection'
import { PeerMap } from '@libp2p/peer-collections'
import type { Pushable } from 'it-pushable'
import { pushable } from 'it-pushable'
import type { Stream } from '@libp2p/interface-connection'
import { Uint8ArrayList } from 'uint8arraylist'
import type { PeerStreamEvents } from '@libp2p/interface-pubsub'
import { logger as logFn } from '@dao-xyz/peerbit-logger'
import { abortableSource } from 'abortable-iterator'
import * as lp from 'it-length-prefixed'
import { Libp2p } from 'libp2p'
import { variant, vec, field, serialize, deserialize, fixedArray } from "@dao-xyz/borsh";
import LRU from "lru-cache";
import crypto from 'crypto';

const logger = logFn({ module: 'peer-streams', level: 'warn' })

export const ProtocolId = '/blocksub/1.0.0'

export class BlockMessage { }

@variant(0)
export class BlockRequest extends BlockMessage {
	@field({ type: fixedArray('u8', 32) })
	id: number[] | Uint8Array;

	@field({ type: "string" })
	cid: string;

	constructor(cid: string) {
		super();
		this.id = crypto.randomBytes(32);
		this.cid = cid;
	}
}

@variant(1)
export class BlockResponse extends BlockMessage {

	@field({ type: fixedArray('u8', 32) })
	id: number[] | Uint8Array;

	@field({ type: "string" })
	cid: string;

	@field({ type: Uint8Array })
	bytes: Uint8Array;

	constructor(cid: string, bytes: Uint8Array) {
		super();
		this.id = crypto.randomBytes(32);
		this.cid = cid;
		this.bytes = bytes;
	}
}

class Message {
	@field({ type: fixedArray('u32', 32) })
	id: number[] | Uint8Array

	@field({ type: vec('string') })
	from: string[]

	@field({ type: Uint8Array })
	data: Uint8Array

	constructor(data: Uint8Array) {
		this.data = data;
		this.id = crypto.randomBytes(32);
		this.from = [];
	}
}



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

function base64EncArr(aBytes: Uint8Array) {
	let nMod3 = 2;
	let sB64Enc = "";

	const nLen = aBytes.length;
	let nUint24 = 0;
	for (let nIdx = 0; nIdx < nLen; nIdx++) {
		nMod3 = nIdx % 3;
		if (nIdx > 0 && ((nIdx * 4) / 3) % 76 === 0) {
			sB64Enc += "\r\n";
		}

		nUint24 |= aBytes[nIdx] << ((16 >>> nMod3) & 24);
		if (nMod3 === 2 || aBytes.length - nIdx === 1) {
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

const toBase64 = typeof Buffer !== 'undefined' ? (x: Uint8Array) => Buffer.from(x).toString('base64') : (x: Uint8Array) => base64EncArr(x)


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
				lp.decode()
			),
			this._inboundAbortController.signal,
			{ returnOnAbort: true }
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


/**
 * PubSubBaseProtocol handles the peers and connections logic for pubsub routers
 * and specifies the API that pubsub routers should have.
 */
export class BlockSub<Events extends { [s: string]: any }> extends EventEmitter<Events> {

	public libp2p: Libp2p;
	public started: boolean
	/**
	 * Map of peer streams
	 */
	public peers: PeerMap<PeerStreams>
	/**
	 * If router can relay received messages, even if not subscribed
	 */
	public canRelayMessage: boolean
	/**
	 * if publish should emit to self, if subscribed
	 */

	public emitSelf: boolean
	public queue: Queue
	private connectionQueue: Queue
	public multicodecs: string[]
	public seenCache: LRU<string, boolean>
	private _registrarTopologyIds: string[] | undefined
	private readonly maxInboundStreams: number
	private readonly maxOutboundStreams: number


	constructor(libp2p: Libp2p, props: { canRelayMessage?: boolean, emitSelf?: boolean, messageProcessingConcurrency?: number, maxInboundStreams?: number, maxOutboundStreams?: number }) {
		super()

		const {
			canRelayMessage = false,
			emitSelf = false,
			messageProcessingConcurrency = 10,
			maxInboundStreams = 1,
			maxOutboundStreams = 1
		} = props
		this.libp2p = libp2p
		this.multicodecs = [ProtocolId]
		this.started = false
		this.peers = new PeerMap<PeerStreams>()
		this.canRelayMessage = canRelayMessage
		this.emitSelf = emitSelf
		this.queue = new Queue({ concurrency: messageProcessingConcurrency })
		this.connectionQueue = new Queue({ concurrency: 1 })
		this.maxInboundStreams = maxInboundStreams
		this.maxOutboundStreams = maxOutboundStreams
		this.seenCache = new LRU({ ttl: 60 * 1000 })
		this._onIncomingStream = this._onIncomingStream.bind(this)
		this._onPeerConnected = this._onPeerConnected.bind(this)
		this._onPeerDisconnected = this._onPeerDisconnected.bind(this)


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
	}

	/**
	 * Unregister the pubsub protocol and the streams with other peers will be closed.
	 */
	async stop() {
		if (!this.started) {
			return
		}

		const registrar = this.libp2p.registrar

		await this.libp2p.unhandle(this.multicodecs)

		// unregister protocol and handlers
		if (this._registrarTopologyIds != null) {
			this._registrarTopologyIds?.map(id => registrar.unregister(id))
		}

		await Promise.all(this.multicodecs.map(async multicodec => await registrar.unhandle(multicodec)))

		await this.connectionQueue.onIdle();

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
		logger.info('new peer' + peerId.toString())

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
		logger.info('delete peer' + peerId.toString())
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
						const rpcBytes = data.subarray()
						const msgId = this.getMsgId(rpcBytes)
						if (this.seenCache.has(msgId)) {
							return
						}
						this.seenCache.set(msgId, true)
						this.processRpc(peerId, peerStreams, rpcBytes)
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
	async processRpc(from: PeerId, peerStreams: PeerStreams, message: Uint8Array): Promise<boolean> {
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

			this.queue.add(async () => {
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
	async processMessage(from: PeerId, msg: Uint8Array) {
		if (!from.publicKey) {
			return;
		}

		if (this.libp2p.peerId.equals(from) && !this.emitSelf) {
			return
		}

		// Ensure the message is valid before processing it
		const message: Message = deserialize(msg, Message);
		message.from.push(from.toString());

		const isFromSelf = this.libp2p.peerId.equals(from)
		if (!isFromSelf || this.emitSelf) {
			super.dispatchEvent(new CustomEvent('message', {
				detail: message.data
			}))
		}
		await this.publishMessage(from, message)
	}

	/**
	 * The default msgID implementation
	 * Child class can override this.
	 */
	getMsgId(msg: Uint8Array) {

		// first bytes is discriminator, 
		// next 32 bytes should be an id
		//return  Buffer.from(msg.slice(0, 33)).toString('base64');

		return toBase64(msg.slice(0, 33));
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
	async publish(data: Uint8Array): Promise<void> {
		if (!this.started) {
			throw new Error('Not started')
		}

		const msgId = this.getMsgId(data);
		this.seenCache.set(msgId, true);


		// dispatch the event if we are interested
		if (this.emitSelf) {
			super.dispatchEvent(new CustomEvent('message', {
				detail: data
			}))
		}

		// send to all the other peers
		await this.publishMessage(this.libp2p.peerId, new Message(data))


	}

	async publishMessage(from: PeerId, message: Message): Promise<void> {
		const peers = this.peers;

		if (peers == null || peers.size === 0) {
			logger.info('no peers are subscribed')
			return
		}

		const bytes = serialize(message);
		peers.forEach(id => {
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

			id.write(bytes);
		})
		return
	}


	getPeers(): PeerId[] {
		if (!this.started) {
			throw new Error('Pubsub is not started')
		}

		return Array.from(this.peers.keys())
	}
}