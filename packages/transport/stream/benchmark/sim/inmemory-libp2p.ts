import type {
	AbortOptions,
	Connection,
	PeerId,
	PrivateKey,
	Stream,
	TypedEventTarget,
} from "@libp2p/interface";
import { type Multiaddr, multiaddr } from "@multiformats/multiaddr";
import { getPublicKeyFromPeerId } from "@peerbit/crypto";
import type { Libp2pEvents } from "@libp2p/interface";
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

const decodeUVarint = (buf: Uint8Array): { value: number; bytes: number } => {
	let x = 0;
	let s = 0;
	for (let i = 0; i < buf.length; i++) {
		const b = buf[i]!;
		if (b < 0x80) {
			if (i > 9 || (i === 9 && b > 1)) throw new Error("varint overflow");
			return { value: x | (b << s), bytes: i + 1 };
		}
		x |= (b & 0x7f) << s;
		s += 7;
	}
	throw new Error("unexpected eof decoding varint");
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
	private readonly rxDelayMs: number;

	private closed = false;

	private bufferedBytes = 0;
	private readonly highWaterMark: number;
	private readonly lowWaterMark: number;
	private backpressured = false;

	peer?: InMemoryStream;
	private readonly recordSend?: (encodedFrame: Uint8Array) => void;

	constructor(opts: {
		id: string;
		protocol: string;
		direction: "inbound" | "outbound";
		highWaterMarkBytes?: number;
		recordSend?: (encodedFrame: Uint8Array) => void;
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
	private readonly recordSend: (encodedFrame: Uint8Array) => void;
	private readonly recordStreamOpen: () => void;
	private readonly streamHighWaterMarkBytes: number;
	private readonly streamRxDelayMs: number;

	private streamSeq = 0;

	constructor(opts: {
		id: string;
		remotePeer: PeerId;
		remoteAddr: Multiaddr;
		getLocalRegistrar: () => InMemoryRegistrar;
		getRemoteRegistrar: () => InMemoryRegistrar;
		getRemoteConnection: () => InMemoryConnection;
		recordSend: (encodedFrame: Uint8Array) => void;
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

	async newStream(
		protocols: string | string[],
		_opts?: { signal?: AbortSignal; negotiateFully?: boolean },
	): Promise<Stream> {
		if (this.status !== "open") {
			throw new Error("Connection is not open");
		}
		const list = Array.isArray(protocols) ? protocols : [protocols];
		const protocol = list[0];
		if (!protocol) {
			throw new Error("Missing protocol");
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
			recordSend: this.recordSend,
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

		const handler = this.getRemoteRegistrar().getHandler(protocol);
		if (!handler) {
			throw new Error(`No handler registered for protocol ${protocol}`);
		}

		// Fire handler async to better match real libp2p behavior.
		queueMicrotask(() => {
			handler(inbound as any, remoteConn as any).catch(() => {
				// ignore handler errors in the transport shim
			});
		});

		return outbound as any;
	}

	async close(): Promise<void> {
		if (this.status !== "open") return;
		this.status = "closed";
		this.timeline.close = Date.now();
		for (const s of this.streams) {
			try {
				s.close?.();
			} catch {}
		}
	}
}

export class InMemoryAddressManager {
	constructor(private readonly addr: Multiaddr) {}
	getAddresses(): Multiaddr[] {
		return [this.addr];
	}
}

export class InMemoryPeerStore {
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
		if (!peerId) {
			return [...this.connectionsByRemote.values()].flat() as any;
		}
		return (this.connectionsByRemote.get(peerId.toString()) ?? []) as any;
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
		if (isPeerId(peer)) {
			const peerId = peer;
			const remote = this.network.getPeerById(peerId.toString());
			if (!remote) throw new Error("Unknown peerId");
			return this._connectTo(remote);
		}

		const list = Array.isArray(peer) ? peer : [peer];
		for (const addr of list) {
			const port = parseTcpPort(addr);
			if (port == null) continue;
			const remote = this.network.getPeerByPort(port);
			if (!remote) continue;
			if (remote.peerId.toString() === this.owner.peerId.toString()) continue;
			return this._connectTo(remote);
		}
		throw new Error("No dialable address");
	}

	async closeConnections(peer: PeerId, _options?: AbortOptions): Promise<void> {
		const remoteId = peer.toString();
		const localConns = this._removeConnections(remoteId);
		if (localConns.length === 0) return;

		for (const conn of localConns) {
			await conn.close();
		}

		const remote = this.network.getPeerById(remoteId);
		const remoteConns = remote
			? remote.connectionManager._removeConnections(this.owner.peerId.toString())
			: [];
		for (const conn of remoteConns) {
			await conn.close();
		}

		this.network.metrics.connectionsClosed += 1;
		this.network.notifyDisconnect(this.owner, peer, localConns);
		if (remote) {
			this.network.notifyDisconnect(remote, this.owner.peerId, remoteConns);
		}
	}

	_removeConnections(remoteId: string): InMemoryConnection[] {
		const conns = this.connectionsByRemote.get(remoteId) ?? [];
		this.connectionsByRemote.delete(remoteId);
		return conns;
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
			recordSend: (encoded) => this.network.recordSend(encoded),
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
			recordSend: (encoded) => this.network.recordSend(encoded),
			recordStreamOpen: () => {
				this.network.metrics.streamsOpened += 1;
			},
			streamHighWaterMarkBytes: this.network.streamHighWaterMarkBytes,
			streamRxDelayMs: this.network.streamRxDelayMs,
		});

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
	public readonly streamRxDelayMs: number;

	constructor(opts?: { streamHighWaterMarkBytes?: number; streamRxDelayMs?: number }) {
		this.streamHighWaterMarkBytes = opts?.streamHighWaterMarkBytes ?? 256 * 1024;
		this.streamRxDelayMs = opts?.streamRxDelayMs ?? 0;
	}

	registerPeer(peer: PeerRuntime, port: number) {
		this.peersById.set(peer.peerId.toString(), peer);
		this.peersByPort.set(port, peer);
	}

	getPeerById(id: string): PeerRuntime | undefined {
		return this.peersById.get(id);
	}

	getPeerByPort(port: number): PeerRuntime | undefined {
		return this.peersByPort.get(port);
	}

	recordSend(encodedFrame: Uint8Array) {
		this.metrics.framesSent += 1;
		this.metrics.bytesSent += encodedFrame.byteLength;
		try {
			const { bytes } = decodeUVarint(encodedFrame);
			const variant = encodedFrame[bytes];
			if (variant === 0) this.metrics.dataFramesSent += 1;
			else if (variant === 1) this.metrics.ackFramesSent += 1;
			else if (variant === 3) this.metrics.goodbyeFramesSent += 1;
			else this.metrics.otherFramesSent += 1;
		} catch {
			this.metrics.otherFramesSent += 1;
		}
	}

	notifyConnect(owner: PeerRuntime, peerId: PeerId, connection: Connection) {
		for (const { topology } of owner.registrar.getTopologies()) {
			topology.onConnect(peerId, connection);
		}
	}

	notifyDisconnect(
		owner: PeerRuntime,
		peerId: PeerId,
		connections: InMemoryConnection[],
	) {
		for (const conn of connections) {
			for (const { topology } of owner.registrar.getTopologies()) {
				topology.onDisconnect(peerId, conn as any);
			}
		}
	}

	static createPeer(opts: {
		index: number;
		port: number;
		mockKeyBytes?: { publicKey: Uint8Array; privateKey: Uint8Array };
	}): {
		runtime: PeerRuntime;
		port: number;
	} {
		const pub =
			opts.mockKeyBytes?.publicKey ??
			Uint8Array.from({ length: 32 }, (_, i) => (opts.index + i) & 0xff);
		const priv =
			opts.mockKeyBytes?.privateKey ??
			Uint8Array.from({ length: 64 }, (_, i) => (opts.index * 31 + i) & 0xff);

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
		const peerStore = new InMemoryPeerStore();
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
