import {
	UnsupportedProtocolError,
	type AbortOptions,
	type Connection,
	type Libp2pEvents,
	type PeerId,
	type PrivateKey,
	type Stream,
	type TypedEventTarget,
} from "@libp2p/interface";
import { type Multiaddr, multiaddr } from "@multiformats/multiaddr";
import { getPublicKeyFromPeerId } from "@peerbit/crypto";
import { pushable, type Pushable } from "it-pushable";

type ProtocolHandler = (stream: Stream, connection: Connection) => Promise<void>;

type Topology = {
	onConnect: (peerId: PeerId, connection: Connection) => Promise<void> | void;
	onDisconnect: (peerId: PeerId, connection: Connection) => Promise<void> | void;
	notifyOnLimitedConnection?: boolean;
};

export type InMemoryNetworkMetrics = {
	dials: number;
	connectionsOpened: number;
	connectionsClosed: number;
	streamsOpened: number;
	framesSent: number;
	bytesSent: number;
	dataFramesSent: number;
	ackFramesSent: number;
	goodbyeFramesSent: number;
	otherFramesSent: number;
};

export type InMemoryFrameType = "data" | "ack" | "goodbye" | "other";

// Minimal "byte view" interface that works for both Uint8Array and Uint8ArrayList
// without importing extra types into the browser app.
type ByteView = {
	byteLength: number;
	length: number;
	subarray: (start?: number, end?: number) => Uint8Array;
	get?: (index: number) => number;
};

export type InMemoryFrameSentEvent = {
	from: PeerId;
	to: PeerId;
	protocol: string;
	type: InMemoryFrameType;
	bytes: number;
	encodedFrame: ByteView;
	payloadOffset: number;
	payloadLength: number;
};

const dispatchPeerEvent = (
	events: TypedEventTarget<Libp2pEvents>,
	type: "peer:connect" | "peer:disconnect",
	peerId: PeerId,
) => {
	try {
		const CustomEventCtor = (globalThis as any).CustomEvent as
			| (new (type: string, init?: { detail?: any }) => Event)
			| undefined;
		if (CustomEventCtor) {
			events.dispatchEvent(new CustomEventCtor(type, { detail: peerId }) as any);
			return;
		}
	} catch {
		// ignore and try fallback
	}

	try {
		const ev = new Event(type) as any;
		ev.detail = peerId;
		events.dispatchEvent(ev);
	} catch {
		// ignore (best-effort shim)
	}
};

const parseTcpPort = (addr: Multiaddr): number | undefined => {
	const str = addr.toString();
	const m = str.match(/\/tcp\/(\d+)/);
	if (!m) return undefined;
	return Number(m[1]);
};

const isPeerId = (value: unknown): value is PeerId => {
	return (
		typeof value === "object" &&
		value != null &&
		typeof (value as any).toString === "function" &&
		(value as any).type != null &&
		(value as any).publicKey != null
	);
};

const MAX_FRAME_LENGTH = 8 * 1024 * 1024; // keep browser demos safe

type OutboundFrameParser = {
	streamId: string;
	varintValue: number;
	varintShift: number;
	varintBytes: number;
	currentPrefixBytes: number;
	payload: Uint8Array | null;
	payloadWritten: number;
};

export class InMemoryRegistrar {
	private handlers = new Map<string, ProtocolHandler>();
	private topologies = new Map<string, { protocol: string; topology: Topology }>();
	private topologySeq = 0;

	async handle(
		protocol: string,
		handler: ProtocolHandler,
		_opts?: unknown,
	): Promise<void> {
		this.handlers.set(protocol, handler);
	}

	async unhandle(protocol: string): Promise<void> {
		this.handlers.delete(protocol);
	}

	async register(protocol: string, topology: Topology): Promise<string> {
		const id = `topology-${++this.topologySeq}`;
		this.topologies.set(id, { protocol, topology });
		return id;
	}

	async unregister(id: string): Promise<void> {
		this.topologies.delete(id);
	}

	getHandler(protocol: string): ProtocolHandler | undefined {
		return this.handlers.get(protocol);
	}

	getTopologies(): Iterable<{ protocol: string; topology: Topology }> {
		return this.topologies.values();
	}
}

class InMemoryStream extends EventTarget {
	public protocol?: string;
	public direction: "inbound" | "outbound";
	public id: string;

	private readonly inbound: Pushable<Uint8Array>;
	private readonly readable: AsyncIterable<Uint8Array>;
	private rxDelayMs: number;

	private closed = false;

	private bufferedBytes = 0;
	private readonly highWaterMark: number;
	private readonly lowWaterMark: number;
	private backpressured = false;

	peer?: InMemoryStream;
	private readonly recordSend?: (encodedFrame: ByteView) => void;

	constructor(opts: {
		id: string;
		protocol: string;
		direction: "inbound" | "outbound";
		highWaterMarkBytes?: number;
		recordSend?: (encodedFrame: ByteView) => void;
		rxDelayMs?: number;
	}) {
		super();
		this.id = opts.id;
		this.protocol = opts.protocol;
		this.direction = opts.direction;
		this.recordSend = opts.recordSend;
		this.rxDelayMs = opts.rxDelayMs ?? 0;
		this.highWaterMark = opts.highWaterMarkBytes ?? 256 * 1024;
		this.lowWaterMark = Math.floor(this.highWaterMark / 2);

		this.inbound = pushable<Uint8Array>({ objectMode: true });

		const self = this;
		this.readable = (async function* () {
			for await (const chunk of self.inbound) {
				if (self.rxDelayMs > 0) {
					await new Promise<void>((resolve) =>
						setTimeout(resolve, self.rxDelayMs),
					);
				}
				self.bufferedBytes = Math.max(0, self.bufferedBytes - chunk.byteLength);
				if (self.backpressured && self.bufferedBytes <= self.lowWaterMark) {
					self.backpressured = false;
					self.peer?.dispatchEvent(new Event("drain"));
				}
				yield chunk;
			}
		})();
	}

	setRxDelayMs(ms: number) {
		this.rxDelayMs = Math.max(0, Math.floor(ms));
	}

	[Symbol.asyncIterator](): AsyncIterator<any> {
		return this.readable[Symbol.asyncIterator]();
	}

	send(data: Uint8Array): boolean {
		if (this.closed) {
			throw new Error("Cannot send on closed stream");
		}
		this.recordSend?.(data);
		const remote = this.peer;
		if (!remote) {
			throw new Error("Missing remote stream endpoint");
		}
		remote.bufferedBytes += data.byteLength;
		remote.inbound.push(data);
		if (remote.bufferedBytes > remote.highWaterMark) {
			remote.backpressured = true;
			return false;
		}
		return true;
	}

	abort(_err?: any): void {
		if (this.closed) return;
		this.closed = true;
		try {
			this.inbound.end();
		} catch {}
		this.dispatchEvent(new Event("close"));
	}

	close(_opts?: AbortOptions): Promise<void> {
		this.abort();
		return Promise.resolve();
	}
}

class InMemoryConnection {
	public readonly id: string;
	public readonly remotePeer: PeerId;
	public readonly remoteAddr?: Multiaddr;
	public readonly streams: Stream[] = [];

	public status: "open" | "closed" = "open";
	public limits: any = undefined;
	public timeline: any = { open: Date.now(), close: undefined };

	private readonly getLocalRegistrar: () => InMemoryRegistrar;
	private readonly getRemoteRegistrar: () => InMemoryRegistrar;
	private readonly getRemoteConnection: () => InMemoryConnection;
	private readonly recordSend: (chunk: ByteView, protocol: string, streamId: string) => void;
	private readonly recordStreamOpen: () => void;
	private readonly streamHighWaterMarkBytes: number;
	private readonly streamRxDelayMs: number;

	private streamSeq = 0;
	private pair?: InMemoryConnectionPair;

	constructor(opts: {
		id: string;
		remotePeer: PeerId;
		remoteAddr: Multiaddr;
		getLocalRegistrar: () => InMemoryRegistrar;
		getRemoteRegistrar: () => InMemoryRegistrar;
		getRemoteConnection: () => InMemoryConnection;
		recordSend: (chunk: ByteView, protocol: string, streamId: string) => void;
		recordStreamOpen: () => void;
		streamHighWaterMarkBytes: number;
		streamRxDelayMs: number;
	}) {
		this.id = opts.id;
		this.remotePeer = opts.remotePeer;
		this.remoteAddr = opts.remoteAddr;
		this.getLocalRegistrar = opts.getLocalRegistrar;
		this.getRemoteRegistrar = opts.getRemoteRegistrar;
		this.getRemoteConnection = opts.getRemoteConnection;
		this.recordSend = opts.recordSend;
		this.recordStreamOpen = opts.recordStreamOpen;
		this.streamHighWaterMarkBytes = opts.streamHighWaterMarkBytes;
		this.streamRxDelayMs = opts.streamRxDelayMs;
	}

	_setPair(pair: InMemoryConnectionPair) {
		this.pair = pair;
	}

	async newStream(
		protocols: string | string[],
		opts?: { signal?: AbortSignal; negotiateFully?: boolean },
	): Promise<Stream> {
		if (this.status !== "open") {
			throw new Error("Connection is not open");
		}
		if (opts?.signal?.aborted) {
			throw opts.signal.reason ?? new Error("Stream open aborted");
		}
		const list = Array.isArray(protocols) ? protocols : [protocols];
		const protocol = list[0];
		if (!protocol) {
			throw new Error("Missing protocol");
		}

		const handler = this.getRemoteRegistrar().getHandler(protocol);
		if (!handler) {
			const err = new UnsupportedProtocolError(
				`No handler registered for protocol ${protocol}`,
			) as any;
			err.code = "ERR_UNSUPPORTED_PROTOCOL";
			throw err;
		}

		this.recordStreamOpen();

		const remoteConn = this.getRemoteConnection();
		const localStreamNo = ++this.streamSeq;
		const remoteStreamNo = ++remoteConn.streamSeq;
		const localStreamId = `${this.id}:out:${localStreamNo}`;
		const remoteStreamId = `${remoteConn.id}:in:${remoteStreamNo}`;

		const outbound = new InMemoryStream({
			id: localStreamId,
			protocol,
			direction: "outbound",
			recordSend: (chunk) => this.recordSend(chunk, protocol, localStreamId),
			highWaterMarkBytes: this.streamHighWaterMarkBytes,
		});
		const inbound = new InMemoryStream({
			id: remoteStreamId,
			protocol,
			direction: "inbound",
			highWaterMarkBytes: this.streamHighWaterMarkBytes,
			rxDelayMs: this.streamRxDelayMs,
		});

		outbound.peer = inbound;
		inbound.peer = outbound;

		this.streams.push(outbound as any);
		remoteConn.streams.push(inbound as any);

		const invokeHandler = () =>
			handler(inbound as any, remoteConn as any).catch(() => {
				// ignore handler errors in the transport shim
			});

		if (opts?.negotiateFully === false) {
			// Fire handler async to better match real libp2p behavior.
			queueMicrotask(() => {
				invokeHandler();
			});
		} else {
			// When negotiateFully=true, invoke handler synchronously so the remote has
			// a chance to attach inbound handlers before the caller starts sending.
			invokeHandler();
		}

		return outbound as any;
	}

	async _closeLocal(): Promise<void> {
		if (this.status !== "open") return;
		this.status = "closed";
		this.timeline.close = Date.now();
		for (const s of this.streams) {
			try {
				s.close?.();
			} catch {}
		}
	}

	async close(_opts?: AbortOptions): Promise<void> {
		if (this.pair) {
			await this.pair.close(this);
			return;
		}
		await this._closeLocal();
	}
}

class InMemoryConnectionPair {
	private closePromise?: Promise<void>;

	constructor(
		private readonly opts: {
			network: InMemoryNetwork;
			aOwner: PeerRuntime;
			bOwner: PeerRuntime;
			aManager: InMemoryConnectionManager;
			bManager: InMemoryConnectionManager;
			aConn: InMemoryConnection;
			bConn: InMemoryConnection;
		},
	) {}

	async close(_initiator: InMemoryConnection): Promise<void> {
		if (this.closePromise) return this.closePromise;
		this.closePromise = (async () => {
			const { aConn, bConn, aOwner, bOwner, aManager, bManager, network } =
				this.opts;

			await Promise.all([aConn._closeLocal(), bConn._closeLocal()]);

			aManager._removeConnectionInstance(bOwner.peerId.toString(), aConn);
			bManager._removeConnectionInstance(aOwner.peerId.toString(), bConn);

			network.metrics.connectionsClosed += 1;
			network.notifyDisconnect(aOwner, bOwner.peerId, [aConn]);
			network.notifyDisconnect(bOwner, aOwner.peerId, [bConn]);
		})();
		return this.closePromise;
	}
}

export class InMemoryAddressManager {
	constructor(private readonly addr: Multiaddr) {}
	getAddresses(): Multiaddr[] {
		return [this.addr];
	}
}

export class InMemoryPeerStore {
	constructor(
		private readonly getAddressesForPeerId: (peerId: PeerId) => Multiaddr[],
	) {}

	async get(peerId: PeerId): Promise<{ addresses: Array<{ multiaddr: Multiaddr }> }> {
		const addrs = this.getAddressesForPeerId(peerId) ?? [];
		return { addresses: addrs.map((multiaddr) => ({ multiaddr })) };
	}

	async delete(_peerId: PeerId): Promise<void> {
		// noop
	}
}

type PeerRuntime = {
	peerId: PeerId;
	privateKey: PrivateKey;
	registrar: InMemoryRegistrar;
	addressManager: InMemoryAddressManager;
	peerStore: InMemoryPeerStore;
	events: TypedEventTarget<Libp2pEvents>;
	connectionManager: InMemoryConnectionManager;
};

export class InMemoryConnectionManager {
	private readonly connectionsByRemote = new Map<string, InMemoryConnection[]>();
	private readonly dialQueue: Array<{ peerId?: PeerId }> = [];
	private readonly connSeqBase: string;

	constructor(
		private readonly network: InMemoryNetwork,
		readonly owner: PeerRuntime,
		opts?: { connSeqBase?: string },
	) {
		this.connSeqBase = opts?.connSeqBase ?? owner.peerId.toString();
	}

	getConnections(peerId?: PeerId): Connection[] {
		const all = !peerId
			? ([...this.connectionsByRemote.values()].flat() as any[])
			: ((this.connectionsByRemote.get(peerId.toString()) ?? []) as any[]);
		// Some callers only check `length > 0`. Ensure closed connections don't keep
		// peers looking "connected" during churn.
		return all.filter((c) => c?.status !== "closed") as any;
	}

	getConnectionsMap(): { get(peer: PeerId): Connection[] | undefined } {
		return {
			get: (peer: PeerId) => this.getConnections(peer),
		};
	}

	getDialQueue(): Array<{ peerId?: PeerId }> {
		return this.dialQueue;
	}

	async isDialable(addr: Multiaddr | Multiaddr[]): Promise<boolean> {
		const list = Array.isArray(addr) ? addr : [addr];
		for (const a of list) {
			const port = parseTcpPort(a);
			if (port == null) continue;
			const remote = this.network.getPeerByPort(port);
			if (!remote) continue;
			if (remote.peerId.toString() === this.owner.peerId.toString()) continue;
			return true;
		}
		return false;
	}

	async openConnection(peer: PeerId | Multiaddr | Multiaddr[]): Promise<Connection> {
		const remote = (() => {
			if (isPeerId(peer)) {
				return this.network.getPeerById(peer.toString());
			}

			const list = Array.isArray(peer) ? peer : [peer];
			for (const addr of list) {
				const port = parseTcpPort(addr);
				if (port == null) continue;
				const found = this.network.getPeerByPort(port);
				if (!found) continue;
				if (found.peerId.toString() === this.owner.peerId.toString()) continue;
				return found;
			}
			return undefined;
		})();

		if (!remote) {
			throw new Error("No dialable address");
		}

		const remoteKey = remote.peerId.toString();
		const existing = this.connectionsByRemote.get(remoteKey)?.find(
			(c) => c.status === "open",
		);
		if (existing) return existing as any;

		const dialEntry = { peerId: remote.peerId };
		this.dialQueue.push(dialEntry);
		try {
			if (this.network.dialDelayMs > 0) {
				await new Promise<void>((resolve) =>
					setTimeout(resolve, this.network.dialDelayMs),
				);
			}
			return this._connectTo(remote);
		} finally {
			const idx = this.dialQueue.indexOf(dialEntry);
			if (idx !== -1) this.dialQueue.splice(idx, 1);
		}
	}

	async closeConnections(peer: PeerId, _options?: AbortOptions): Promise<void> {
		const remoteId = peer.toString();
		const conns = this.connectionsByRemote.get(remoteId) ?? [];
		if (conns.length === 0) return;
		// Closing one side will close the paired remote connection and notify both sides.
		await Promise.all([...conns].map((c) => c.close()));
	}

	_removeConnections(remoteId: string): InMemoryConnection[] {
		const conns = this.connectionsByRemote.get(remoteId) ?? [];
		this.connectionsByRemote.delete(remoteId);
		return conns;
	}

	_removeConnectionInstance(remoteId: string, conn: InMemoryConnection) {
		const conns = this.connectionsByRemote.get(remoteId);
		if (!conns) return;
		const idx = conns.indexOf(conn);
		if (idx === -1) return;
		conns.splice(idx, 1);
		if (conns.length === 0) this.connectionsByRemote.delete(remoteId);
	}

	_connectTo(remote: PeerRuntime): Connection {
		const remoteKey = remote.peerId.toString();
		const existing = this.connectionsByRemote.get(remoteKey)?.find(
			(c) => c.status === "open",
		);
		if (existing) return existing as any;

		const addr = remote.addressManager.getAddresses()[0]!;
		const connIdA = `${this.connSeqBase}->${remoteKey}:${Date.now()}:${Math.random()
			.toString(16)
			.slice(2)}`;
		const connIdB = `${remoteKey}->${this.connSeqBase}:${Date.now()}:${Math.random()
			.toString(16)
			.slice(2)}`;

		let connA!: InMemoryConnection;
		let connB!: InMemoryConnection;

			connA = new InMemoryConnection({
			id: connIdA,
			remotePeer: remote.peerId,
			remoteAddr: addr,
				getLocalRegistrar: () => this.owner.registrar,
				getRemoteRegistrar: () => remote.registrar,
				getRemoteConnection: () => connB,
				recordSend: (chunk, protocol, streamId) =>
					this.network.recordSend({
						from: this.owner.peerId,
						to: remote.peerId,
						protocol,
						streamId,
						chunk,
					}),
				recordStreamOpen: () => {
					this.network.metrics.streamsOpened += 1;
				},
				streamHighWaterMarkBytes: this.network.streamHighWaterMarkBytes,
				streamRxDelayMs: this.network.streamRxDelayMs,
			});
			connB = new InMemoryConnection({
			id: connIdB,
			remotePeer: this.owner.peerId,
			remoteAddr: this.owner.addressManager.getAddresses()[0]!,
				getLocalRegistrar: () => remote.registrar,
				getRemoteRegistrar: () => this.owner.registrar,
				getRemoteConnection: () => connA,
				recordSend: (chunk, protocol, streamId) =>
					this.network.recordSend({
						from: remote.peerId,
						to: this.owner.peerId,
						protocol,
						streamId,
						chunk,
					}),
				recordStreamOpen: () => {
					this.network.metrics.streamsOpened += 1;
				},
				streamHighWaterMarkBytes: this.network.streamHighWaterMarkBytes,
			streamRxDelayMs: this.network.streamRxDelayMs,
		});

		const pair = new InMemoryConnectionPair({
			network: this.network,
			aOwner: this.owner,
			bOwner: remote,
			aManager: this,
			bManager: remote.connectionManager,
			aConn: connA,
			bConn: connB,
		});
		connA._setPair(pair);
		connB._setPair(pair);

		{
			const arr = this.connectionsByRemote.get(remoteKey) ?? [];
			arr.push(connA);
			this.connectionsByRemote.set(remoteKey, arr);
		}
		{
			const arr = remote.connectionManager.connectionsByRemote.get(
				this.owner.peerId.toString(),
			) ?? [];
			arr.push(connB);
			remote.connectionManager.connectionsByRemote.set(
				this.owner.peerId.toString(),
				arr,
			);
		}

		this.network.notifyConnect(this.owner, remote.peerId, connA as any);
		this.network.notifyConnect(remote, this.owner.peerId, connB as any);

		this.network.metrics.dials += 1;
		this.network.metrics.connectionsOpened += 1;

		return connA as any;
	}
}

export class InMemoryNetwork {
	private peersById = new Map<string, PeerRuntime>();
	private peersByPort = new Map<number, PeerRuntime>();
	private outboundParsers = new Map<string, OutboundFrameParser>();
	public readonly metrics: InMemoryNetworkMetrics = {
		dials: 0,
		connectionsOpened: 0,
		connectionsClosed: 0,
		streamsOpened: 0,
		framesSent: 0,
		bytesSent: 0,
		dataFramesSent: 0,
		ackFramesSent: 0,
		goodbyeFramesSent: 0,
		otherFramesSent: 0,
	};
	public readonly streamHighWaterMarkBytes: number;
	public streamRxDelayMs: number;
	public readonly dialDelayMs: number;
	public readonly onFrameSent?: (ev: InMemoryFrameSentEvent) => void;

	constructor(opts?: {
		streamHighWaterMarkBytes?: number;
		streamRxDelayMs?: number;
		dialDelayMs?: number;
		onFrameSent?: (ev: InMemoryFrameSentEvent) => void;
	}) {
		this.streamHighWaterMarkBytes = opts?.streamHighWaterMarkBytes ?? 256 * 1024;
		this.streamRxDelayMs = opts?.streamRxDelayMs ?? 0;
		this.dialDelayMs = opts?.dialDelayMs ?? 0;
		this.onFrameSent = opts?.onFrameSent;
	}

	setStreamRxDelayMs(ms: number) {
		const next = Math.max(0, Math.floor(ms));
		this.streamRxDelayMs = next;
		for (const peer of this.peersById.values()) {
			const conns = (peer.connectionManager?.getConnections?.() ?? []) as any[];
			for (const conn of conns) {
				const streams = (conn as any)?.streams as any[] | undefined;
				if (!Array.isArray(streams)) continue;
				for (const stream of streams) {
					if (stream instanceof InMemoryStream) {
						stream.setRxDelayMs(next);
						continue;
					}
					if (stream && typeof stream.setRxDelayMs === "function") {
						stream.setRxDelayMs(next);
						continue;
					}
					if (stream && typeof stream === "object" && "rxDelayMs" in stream) {
						(stream as any).rxDelayMs = next;
					}
				}
			}
		}
	}

	registerPeer(peer: PeerRuntime, port: number) {
		this.peersById.set(peer.peerId.toString(), peer);
		this.peersByPort.set(port, peer);
	}

	unregisterPeer(peerId: PeerId) {
		const id = peerId.toString();
		this.peersById.delete(id);
		for (const [port, runtime] of this.peersByPort) {
			if (runtime.peerId.toString() === id) {
				this.peersByPort.delete(port);
				break;
			}
		}
	}

	getPeerById(id: string): PeerRuntime | undefined {
		return this.peersById.get(id);
	}

	getPeerByPort(port: number): PeerRuntime | undefined {
		return this.peersByPort.get(port);
	}

	recordSend(ev: {
		from: PeerId;
		to: PeerId;
		protocol: string;
		streamId: string;
		chunk: ByteView;
	}) {
		const { from, to, protocol, streamId } = ev;
		const bytes = ev.chunk instanceof Uint8Array ? ev.chunk : ev.chunk.subarray();
		this.metrics.bytesSent += bytes.byteLength;

		let parser = this.outboundParsers.get(streamId);
		if (!parser) {
			parser = {
				streamId,
				varintValue: 0,
				varintShift: 0,
				varintBytes: 0,
				currentPrefixBytes: 0,
				payload: null,
				payloadWritten: 0,
			};
			this.outboundParsers.set(streamId, parser);
		}

		let offset = 0;
		while (offset < bytes.length) {
			// Read length prefix (uvarint)
			if (!parser.payload) {
				const b = bytes[offset++]!;
				parser.varintValue |= (b & 0x7f) << parser.varintShift;
				parser.varintShift += 7;
				parser.varintBytes += 1;
				if (parser.varintBytes > 10) {
					// Reset on malformed varint.
					parser.varintValue = 0;
					parser.varintShift = 0;
					parser.varintBytes = 0;
					parser.currentPrefixBytes = 0;
					parser.payload = null;
					parser.payloadWritten = 0;
					continue;
				}
				if (b < 0x80) {
					const length = parser.varintValue >>> 0;
					const prefixBytes = parser.varintBytes;
					parser.varintValue = 0;
					parser.varintShift = 0;
					parser.varintBytes = 0;
					parser.currentPrefixBytes = prefixBytes;

					if (length > MAX_FRAME_LENGTH) {
						// Skip absurd frames to avoid OOM in the browser.
						parser.currentPrefixBytes = 0;
						parser.payload = null;
						parser.payloadWritten = 0;
						continue;
					}

					parser.payload = new Uint8Array(length);
					parser.payloadWritten = 0;
					if (length === 0) {
						// Empty frame.
						this.metrics.framesSent += 1;
						this.metrics.otherFramesSent += 1;
						this.onFrameSent?.({
							from,
							to,
							protocol,
							type: "other",
							bytes: prefixBytes,
							encodedFrame: new Uint8Array(0),
							payloadOffset: 0,
							payloadLength: 0,
						});
						parser.currentPrefixBytes = 0;
						parser.payload = null;
						parser.payloadWritten = 0;
					}
				}
				continue;
			}

			// Read payload bytes
			const payload = parser.payload;
			const remaining = payload.length - parser.payloadWritten;
			const available = bytes.length - offset;
			const take = Math.min(remaining, available);
			if (take > 0) {
				payload.set(bytes.subarray(offset, offset + take), parser.payloadWritten);
				parser.payloadWritten += take;
				offset += take;
			}

			if (parser.payloadWritten === payload.length) {
				const frameBytes = parser.currentPrefixBytes + payload.length;
				parser.currentPrefixBytes = 0;
				parser.payload = null;
				parser.payloadWritten = 0;

				const variant = payload[0];
				let type: InMemoryFrameType = "other";
				if (variant === 0) type = "data";
				else if (variant === 1) type = "ack";
				else if (variant === 3) type = "goodbye";

				this.metrics.framesSent += 1;
				if (type === "data") this.metrics.dataFramesSent += 1;
				else if (type === "ack") this.metrics.ackFramesSent += 1;
				else if (type === "goodbye") this.metrics.goodbyeFramesSent += 1;
				else this.metrics.otherFramesSent += 1;

				this.onFrameSent?.({
					from,
					to,
					protocol,
					type,
					bytes: frameBytes,
					encodedFrame: payload,
					payloadOffset: 0,
					payloadLength: payload.length,
				});
			}
		}
	}

	notifyConnect(owner: PeerRuntime, peerId: PeerId, connection: Connection) {
		const remote = this.getPeerById(peerId.toString());
		for (const { protocol, topology } of owner.registrar.getTopologies()) {
			if (remote && !remote.registrar.getHandler(protocol)) continue;
			topology.onConnect(peerId, connection);
		}
		dispatchPeerEvent(owner.events, "peer:connect", peerId);
	}

	notifyDisconnect(
		owner: PeerRuntime,
		peerId: PeerId,
		connections: InMemoryConnection[],
	) {
		const remote = this.getPeerById(peerId.toString());
		for (const conn of connections) {
			for (const { protocol, topology } of owner.registrar.getTopologies()) {
				if (remote && !remote.registrar.getHandler(protocol)) continue;
				topology.onDisconnect(peerId, conn as any);
			}
		}
		dispatchPeerEvent(owner.events, "peer:disconnect", peerId);
	}

	static createPeer(opts: {
		index: number;
		port: number;
		network?: InMemoryNetwork;
		mockKeyBytes?: { publicKey: Uint8Array; privateKey: Uint8Array };
	}): {
		runtime: PeerRuntime;
		port: number;
	} {
		// Deterministic + fast mock "keys" (no heavy crypto), but must still be
		// unique for large `index` values. The previous byte pattern used `& 0xff`
		// on `index`, causing collisions once `index >= 256` which breaks large sims.
		const pub =
			opts.mockKeyBytes?.publicKey ??
			(() => {
				const out = new Uint8Array(32);
				const x = opts.index >>> 0;
				for (let i = 0; i < out.length; i++) {
					const shift = (i % 4) * 8;
					out[i] = (((x >>> shift) + i * 17 + 0x9e) & 0xff) >>> 0;
				}
				return out;
			})();
		const priv =
			opts.mockKeyBytes?.privateKey ??
			(() => {
				const out = new Uint8Array(64);
				const x = Math.imul(opts.index >>> 0, 0x9e3779b1) >>> 0;
				for (let i = 0; i < 32; i++) {
					const shift = (i % 4) * 8;
					out[i] = (((x >>> shift) + i * 29 + 0x7f) & 0xff) >>> 0;
				}
				// Mirror the ed25519 "secretKey = seed||publicKey" layout.
				out.set(pub, 32);
				return out;
			})();

		const peerId = {
			type: "Ed25519",
			publicKey: { raw: pub },
			toString: () => `sim-${opts.index}`,
			equals: (other: any) => other?.toString?.() === `sim-${opts.index}`,
		} as any as PeerId;

		const privateKey = {
			type: "Ed25519",
			raw: priv,
			publicKey: { raw: pub },
		} as any as PrivateKey;

		const addr = multiaddr(`/ip4/127.0.0.1/tcp/${opts.port}`);
		const registrar = new InMemoryRegistrar();
		const addressManager = new InMemoryAddressManager(addr);
		const peerStore = new InMemoryPeerStore((peerId) => {
			const network = opts.network;
			const runtime = network?.getPeerById(peerId.toString());
			return runtime?.addressManager.getAddresses() ?? [];
		});
		const events = new EventTarget() as any as TypedEventTarget<Libp2pEvents>;

		// placeholder connectionManager; caller wires it after creation
		const runtime = {
			peerId,
			privateKey,
			registrar,
			addressManager,
			peerStore,
			events,
			connectionManager: undefined as any,
		} as PeerRuntime;

		return { runtime: runtime as any, port: opts.port };
	}
}

export const publicKeyHash = (peerId: PeerId) =>
	getPublicKeyFromPeerId(peerId).hashcode();

export type InMemoryServiceFactories<T extends Record<string, unknown>> = {
	[K in keyof T]: (components: any) => T[K];
};

export class InMemoryLibp2p<TServices extends Record<string, unknown> = {}> {
	public status: "started" | "stopped" = "stopped";
	public readonly peerId: PeerId;
	public readonly services: TServices;

	constructor(
		public readonly runtime: PeerRuntime,
		services?: InMemoryServiceFactories<TServices>,
	) {
		this.peerId = runtime.peerId;
		const components = {
			peerId: runtime.peerId,
			privateKey: runtime.privateKey,
			addressManager: runtime.addressManager as any,
			registrar: runtime.registrar as any,
			connectionManager: runtime.connectionManager as any,
			peerStore: runtime.peerStore as any,
			events: runtime.events,
		};

		const out: Record<string, unknown> = {};
		if (services) {
			for (const [name, factory] of Object.entries(services)) {
				out[name] = (factory as any)(components);
			}
		}
		this.services = out as any as TServices;
	}

	getMultiaddrs(): Multiaddr[] {
		return this.runtime.addressManager.getAddresses();
	}

	dial(addresses: Multiaddr | Multiaddr[]): Promise<Connection> {
		return this.runtime.connectionManager.openConnection(addresses as any);
	}

	hangUp(peerId: PeerId, options?: AbortOptions): Promise<void> {
		return this.runtime.connectionManager.closeConnections(peerId, options);
	}

	getConnections(peerId?: PeerId): Connection[] {
		return this.runtime.connectionManager.getConnections(peerId);
	}

	async start(): Promise<void> {
		if (this.status === "started") return;
		this.status = "started";
		await Promise.all(
			Object.values(this.services as any).map((svc: any) => svc?.start?.()),
		);
	}

	async stop(): Promise<void> {
		if (this.status === "stopped") return;
		this.status = "stopped";

		await Promise.all(
			Object.values(this.services as any).map((svc: any) => svc?.stop?.()),
		);

		const conns = this.runtime.connectionManager.getConnections();
		await Promise.all(conns.map((c) => c.close()));
	}
}

export class InMemorySession<TServices extends Record<string, unknown> = {}> {
	public readonly peers: Array<InMemoryLibp2p<TServices>>;
	public readonly network: InMemoryNetwork;

	constructor(opts: {
		peers: Array<InMemoryLibp2p<TServices>>;
		network: InMemoryNetwork;
	}) {
		this.peers = opts.peers;
		this.network = opts.network;
	}

	static async disconnected<TServices extends Record<string, unknown>>(
		n: number,
		opts?: {
			basePort?: number;
			start?: boolean;
			network?: InMemoryNetwork;
			networkOpts?: ConstructorParameters<typeof InMemoryNetwork>[0];
			services?: InMemoryServiceFactories<TServices>;
		},
	): Promise<InMemorySession<TServices>> {
		const network = opts?.network ?? new InMemoryNetwork(opts?.networkOpts);
		const basePort = opts?.basePort ?? 30_000;

		const peers: Array<InMemoryLibp2p<TServices>> = [];
		for (let i = 0; i < n; i++) {
			const port = basePort + i;
			const { runtime } = InMemoryNetwork.createPeer({ index: i, port, network });
			runtime.connectionManager = new InMemoryConnectionManager(network, runtime);
			network.registerPeer(runtime, port);
			peers.push(new InMemoryLibp2p(runtime, opts?.services));
		}

		const session = new InMemorySession<TServices>({ peers, network });
		if (opts?.start !== false) {
			await Promise.all(session.peers.map((p) => p.start()));
		}
		return session;
	}

	async connectFully(
		groups?: Array<
			Array<{
				getMultiaddrs(): Multiaddr[];
				dial(addresses: Multiaddr[]): Promise<any>;
			}>
		>,
	): Promise<this> {
		const peers = groups ?? [this.peers];
		const connectPromises: Promise<any>[] = [];
		for (const group of peers) {
			for (let i = 0; i < group.length - 1; i++) {
				for (let j = i + 1; j < group.length; j++) {
					connectPromises.push(group[i]!.dial(group[j]!.getMultiaddrs()));
				}
			}
		}
		await Promise.all(connectPromises);
		return this;
	}

	async stop(): Promise<void> {
		await Promise.all(this.peers.map((p) => p.stop()));
	}
}
