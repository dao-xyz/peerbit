import { deserialize, field, serialize, variant } from "@dao-xyz/borsh";
import {
	CanonicalBootstrapRequest,
	CanonicalConnection,
	CanonicalControlRequest,
	CanonicalControlResponse,
	CanonicalLoadProgramRequest,
	CanonicalSignRequest,
	createMessagePortTransport as createControlTransport,
	createRpcTransport as createRpcTransportBase,
} from "@peerbit/canonical-transport";
import type {
	CanonicalChannel,
	CanonicalRpcTransport,
	CanonicalTransport,
} from "@peerbit/canonical-transport";
import {
	type Identity,
	PreHash,
	PublicSignKey,
	SignatureWithKey,
} from "@peerbit/crypto";
import type { CanonicalOpenAdapter, CanonicalOpenMode } from "./auto.js";
import type { PeerbitCanonicalClient } from "./peerbit.js";

export type { CanonicalChannel };

export type { CanonicalRpcTransport };

abstract class RpcMessage {}

class RpcRequestHeader {
	@field({ type: "u32" })
	id: number;

	@field({ type: "string" })
	method: string;

	constructor(properties: { id: number; method: string }) {
		this.id = properties.id;
		this.method = properties.method;
	}
}

class RpcResponseHeader {
	@field({ type: "u32" })
	id: number;

	@field({ type: "string" })
	method: string;

	constructor(properties: { id: number; method: string }) {
		this.id = properties.id;
		this.method = properties.method;
	}
}

@variant(0)
class RpcRequest extends RpcMessage {
	@field({ type: RpcRequestHeader })
	header: RpcRequestHeader;

	@field({ type: Uint8Array })
	payload: Uint8Array;

	constructor(properties: { header: RpcRequestHeader; payload: Uint8Array }) {
		super();
		this.header = properties.header;
		this.payload = properties.payload;
	}
}

@variant(1)
class RpcOk extends RpcMessage {
	@field({ type: RpcResponseHeader })
	header: RpcResponseHeader;

	@field({ type: Uint8Array })
	payload: Uint8Array;

	constructor(properties: { header: RpcResponseHeader; payload: Uint8Array }) {
		super();
		this.header = properties.header;
		this.payload = properties.payload;
	}
}

@variant(2)
class RpcErr extends RpcMessage {
	@field({ type: RpcResponseHeader })
	header: RpcResponseHeader;

	@field({ type: "string" })
	message: string;

	constructor(properties: { header: RpcResponseHeader; message: string }) {
		super();
		this.header = properties.header;
		this.message = properties.message;
	}
}

@variant(3)
class RpcStream extends RpcMessage {
	@field({ type: RpcResponseHeader })
	header: RpcResponseHeader;

	@field({ type: Uint8Array })
	payload: Uint8Array;

	constructor(properties: { header: RpcResponseHeader; payload: Uint8Array }) {
		super();
		this.header = properties.header;
		this.payload = properties.payload;
	}
}

@variant(4)
class RpcStreamEnd extends RpcMessage {
	@field({ type: RpcResponseHeader })
	header: RpcResponseHeader;

	constructor(properties: { header: RpcResponseHeader }) {
		super();
		this.header = properties.header;
	}
}

@variant(5)
class RpcStreamErr extends RpcMessage {
	@field({ type: RpcResponseHeader })
	header: RpcResponseHeader;

	@field({ type: "string" })
	message: string;

	constructor(properties: { header: RpcResponseHeader; message: string }) {
		super();
		this.header = properties.header;
		this.message = properties.message;
	}
}

type RpcTransportLike = {
	send: (data: Uint8Array) => void;
	onMessage: (handler: (data: Uint8Array) => void) => () => void;
};

export type RpcRequestTimeoutMs =
	| number
	| ((method: string) => number | undefined);

export type CanonicalIdentity = Identity<PublicSignKey> & {
	signer: (
		prehash?: PreHash,
	) => (data: Uint8Array) => Promise<SignatureWithKey>;
};

const isMessagePort = (
	value: MessagePort | CanonicalChannel,
): value is MessagePort =>
	typeof (value as MessagePort).postMessage === "function";

const toRpcMessage = (data: Uint8Array): RpcMessage | undefined => {
	try {
		return deserialize(data, RpcMessage) as RpcMessage;
	} catch {
		return undefined;
	}
};

const createCloseAwareRpcTransport = (
	base: RpcTransportLike,
	options: {
		onClose?: (handler: () => void) => () => void;
		closeErrorMessage?: string;
		requestTimeoutMs?: RpcRequestTimeoutMs;
		ignoreTimeoutForMethod?: (method: string) => boolean;
	},
): CanonicalRpcTransport => {
	const closeErrorMessage = options.closeErrorMessage ?? "RPC transport closed";
	const handlers = new Set<(data: Uint8Array) => void>();
	const inFlight = new Map<
		number,
		{
			method: string;
			timeout?: ReturnType<typeof setTimeout>;
		}
	>();
	let closed = false;
	let offClose: (() => void) | undefined;

	const ignoreTimeoutForMethod =
		options.ignoreTimeoutForMethod ??
		((method: string) =>
			method.startsWith("$events:") ||
			method.startsWith("$watch:") ||
			method.startsWith("$presentWatch:"));

	const resolveTimeoutMs = (method: string): number | undefined => {
		if (ignoreTimeoutForMethod(method)) return undefined;
		const configured = options.requestTimeoutMs;
		if (configured == null) return undefined;
		const value =
			typeof configured === "function" ? configured(method) : configured;
		if (value == null || value <= 0) return undefined;
		return value;
	};

	const emit = (data: Uint8Array) => {
		for (const handler of handlers) {
			try {
				handler(data);
			} catch {}
		}
	};

	const unsubscribeBase = base.onMessage((data) => {
		const msg = toRpcMessage(data);
		if (
			msg instanceof RpcOk ||
			msg instanceof RpcErr ||
			msg instanceof RpcStream ||
			msg instanceof RpcStreamEnd ||
			msg instanceof RpcStreamErr
		) {
			const id = msg.header.id;
			const entry = inFlight.get(id);
			if (entry?.timeout) {
				clearTimeout(entry.timeout);
				entry.timeout = undefined;
			}
			if (
				msg instanceof RpcOk ||
				msg instanceof RpcErr ||
				msg instanceof RpcStreamEnd ||
				msg instanceof RpcStreamErr
			) {
				inFlight.delete(id);
			}
		}
		emit(data);
	});

	const close = (message: string = closeErrorMessage) => {
		if (closed) return;
		closed = true;
		if (offClose) offClose();
		unsubscribeBase();
		for (const [id, entry] of inFlight) {
			if (entry.timeout) clearTimeout(entry.timeout);
			emit(
				serialize(
					new RpcErr({
						header: new RpcResponseHeader({ id, method: entry.method }),
						message,
					}),
				),
			);
		}
		inFlight.clear();
	};

	if (options.onClose) {
		offClose = options.onClose(() => close(closeErrorMessage));
	}

	return {
		send: (data) => {
			const msg = toRpcMessage(data);
			if (closed) {
				if (msg instanceof RpcRequest) {
					emit(
						serialize(
							new RpcErr({
								header: new RpcResponseHeader({
									id: msg.header.id,
									method: msg.header.method,
								}),
								message: closeErrorMessage,
							}),
						),
					);
				}
				return;
			}
			if (msg instanceof RpcRequest) {
				const method = msg.header.method;
				const timeoutMs = resolveTimeoutMs(method);
				const id = msg.header.id;
				const entry = {
					method,
					timeout: timeoutMs
						? setTimeout(() => {
								const current = inFlight.get(id);
								if (!current) return;
								inFlight.delete(id);
								emit(
									serialize(
										new RpcErr({
											header: new RpcResponseHeader({
												id,
												method: current.method,
											}),
											message: `RPC request timeout (${current.method})`,
										}),
									),
								);
							}, timeoutMs)
						: undefined,
				};
				inFlight.set(id, entry);
			}
			base.send(data);
		},
		onMessage: (handler) => {
			handlers.add(handler);
			return () => {
				handlers.delete(handler);
			};
		},
		close,
	} as CanonicalRpcTransport;
};

export const createMessagePortTransport = (
	port: MessagePort | CanonicalChannel,
	options?: {
		requestTimeoutMs?: RpcRequestTimeoutMs;
		ignoreTimeoutForMethod?: (method: string) => boolean;
		closeErrorMessage?: string;
	},
): CanonicalRpcTransport => {
	const base = createRpcTransportBase(port);
	const shouldWrap =
		typeof options?.requestTimeoutMs !== "undefined" ||
		typeof options?.ignoreTimeoutForMethod === "function" ||
		typeof options?.closeErrorMessage === "string";
	if (isMessagePort(port)) {
		if (!shouldWrap) return base;
		return createCloseAwareRpcTransport(base, {
			requestTimeoutMs: options?.requestTimeoutMs,
			ignoreTimeoutForMethod: options?.ignoreTimeoutForMethod,
			closeErrorMessage: options?.closeErrorMessage,
		});
	}

	const channel = port as CanonicalChannel;
	if (typeof channel.onClose !== "function") {
		if (!shouldWrap) return base;
		return createCloseAwareRpcTransport(base, {
			requestTimeoutMs: options?.requestTimeoutMs,
			ignoreTimeoutForMethod: options?.ignoreTimeoutForMethod,
			closeErrorMessage: options?.closeErrorMessage,
		});
	}

	return createCloseAwareRpcTransport(base, {
		onClose: (handler) => channel.onClose?.(handler) ?? (() => {}),
		requestTimeoutMs: options?.requestTimeoutMs,
		ignoreTimeoutForMethod: options?.ignoreTimeoutForMethod,
		closeErrorMessage: options?.closeErrorMessage,
	});
};

type Pending<T> = {
	resolve: (value: T) => void;
	reject: (error: Error) => void;
	timeout?: ReturnType<typeof setTimeout>;
};

export class CanonicalClient {
	private nextId = 1;
	private readonly pending = new Map<number, Pending<any>>();
	private readonly connection: CanonicalConnection;
	private readonly unsubscribe: () => void;
	private keepAliveTimer?: ReturnType<typeof setInterval>;
	private keepAliveInFlight?: Promise<void>;
	private readonly requestTimeoutMs?: number;
	private cachedPeerId?: string;
	private cachedPublicKey?: PublicSignKey;
	private cachedMultiaddrs?: string[];
	private cachedIdentity?: CanonicalIdentity;

	constructor(
		control: MessagePort | CanonicalTransport,
		options?: { requestTimeoutMs?: number },
	) {
		const transport =
			typeof (control as MessagePort).postMessage === "function"
				? createControlTransport(control as MessagePort)
				: (control as CanonicalTransport);
		this.connection = new CanonicalConnection(transport);
		this.requestTimeoutMs = options?.requestTimeoutMs;
		this.unsubscribe = this.connection.onControl((frame) => {
			if (frame instanceof CanonicalControlResponse) {
				this.onMessage(frame);
			}
		});
	}

	static async create(
		control: MessagePort | CanonicalTransport,
		options?: { requestTimeoutMs?: number },
	): Promise<CanonicalClient> {
		const client = new CanonicalClient(control, options);
		await client.init();
		return client;
	}

	async init(): Promise<void> {
		if (this.cachedPublicKey) return;
		await this.peerInfo();
	}

	close(): void {
		if (this.keepAliveTimer) {
			clearInterval(this.keepAliveTimer);
			this.keepAliveTimer = undefined;
		}
		this.keepAliveInFlight = undefined;
		this.unsubscribe();
		this.connection.close();
		for (const [id, p] of this.pending) {
			if (p.timeout) clearTimeout(p.timeout);
			p.reject(new Error("CanonicalClient closed"));
			this.pending.delete(id);
		}
	}

	async peerId(): Promise<string> {
		if (this.cachedPeerId) return this.cachedPeerId;
		const resp = await this.request({ op: "peerId" });
		if (!resp.peerId) {
			throw new Error("Canonical peerId response missing peerId");
		}
		this.cachedPeerId = resp.peerId;
		return resp.peerId;
	}

	async peerInfo(): Promise<{
		peerId: string;
		publicKey: PublicSignKey;
		multiaddrs: string[];
	}> {
		const resp = await this.request({ op: "peerInfo" });
		if (!resp.peerId) {
			throw new Error("Canonical peerInfo response missing peerId");
		}
		if (!resp.payload) {
			throw new Error("Canonical peerInfo response missing payload");
		}
		const publicKey = deserialize(resp.payload, PublicSignKey) as PublicSignKey;
		const multiaddrs = resp.strings ?? [];
		this.cachedPeerId = resp.peerId;
		this.cachedPublicKey = publicKey;
		this.cachedMultiaddrs = multiaddrs;
		this.cachedIdentity = undefined;
		return { peerId: resp.peerId, publicKey, multiaddrs };
	}

	async multiaddrs(): Promise<string[]> {
		if (this.cachedMultiaddrs) return this.cachedMultiaddrs;
		const info = await this.peerInfo();
		return info.multiaddrs;
	}

	getMultiaddrs(): Promise<string[]> {
		return this.multiaddrs();
	}

	async dial(address: string | { toString(): string }): Promise<boolean> {
		const value = typeof address === "string" ? address : address.toString();
		await this.request({ op: "dial", name: value });
		return true;
	}

	async hangUp(address: string | { toString(): string }): Promise<void> {
		const value = typeof address === "string" ? address : address.toString();
		await this.request({ op: "hangUp", name: value });
	}

	async start(): Promise<void> {
		await this.request({ op: "start" });
	}

	async stop(): Promise<void> {
		await this.request({ op: "stop" });
	}

	async bootstrap(addresses?: string[]): Promise<void> {
		const payload =
			addresses && addresses.length > 0
				? serialize(new CanonicalBootstrapRequest({ addresses }))
				: undefined;
		await this.request({ op: "bootstrap", payload });
	}

	async loadProgram(
		address: string,
		options?: { timeoutMs?: number },
	): Promise<Uint8Array> {
		if (!address) {
			throw new Error("Canonical loadProgram requires address to be set");
		}
		const payload =
			options?.timeoutMs != null
				? serialize(
						new CanonicalLoadProgramRequest({ timeoutMs: options.timeoutMs }),
					)
				: undefined;
		const resp = await this.request({
			op: "loadProgram",
			name: address,
			payload,
		});
		if (!resp.payload) {
			throw new Error("Canonical loadProgram response missing payload");
		}
		return resp.payload;
	}

	async sign(
		data: Uint8Array,
		prehash: PreHash = PreHash.NONE,
	): Promise<SignatureWithKey> {
		const resp = await this.request({
			op: "sign",
			payload: serialize(
				new CanonicalSignRequest({
					data,
					prehash,
				}),
			),
		});
		if (!resp.payload) {
			throw new Error("Canonical sign response missing payload");
		}
		return deserialize(resp.payload, SignatureWithKey) as SignatureWithKey;
	}

	get identity(): CanonicalIdentity {
		if (!this.cachedPublicKey) {
			throw new Error(
				"CanonicalClient not initialized (missing publicKey); call await CanonicalClient.create(...) or await client.init()",
			);
		}
		if (this.cachedIdentity) return this.cachedIdentity;
		const publicKey = this.cachedPublicKey;
		this.cachedIdentity = {
			publicKey,
			sign: (data, prehash) => this.sign(data, prehash ?? PreHash.NONE),
			signer:
				(prehash = PreHash.NONE) =>
				(data) =>
					this.sign(data, prehash),
		};
		return this.cachedIdentity;
	}

	async openPort(name: string, payload: Uint8Array): Promise<CanonicalChannel> {
		const resp = await this.request({
			op: "open",
			name,
			payload,
		});
		if (resp.channelId == null) {
			throw new Error("Canonical open response missing channelId");
		}
		return this.connection.createChannel(resp.channelId);
	}

	async ping(): Promise<void> {
		await this.request({ op: "ping" });
	}

	startKeepAlive(options?: {
		intervalMs?: number;
		timeoutMs?: number;
		closeOnFail?: boolean;
		maxFailures?: number;
	}): () => void {
		if (this.keepAliveTimer) {
			clearInterval(this.keepAliveTimer);
		}
		const interval = Math.max(1000, options?.intervalMs ?? 30_000);
		const timeoutMs = Math.max(
			1000,
			options?.timeoutMs ?? this.requestTimeoutMs ?? 10_000,
		);
		const closeOnFail = options?.closeOnFail ?? true;
		const maxFailures = Math.max(1, options?.maxFailures ?? 2);
		let failures = 0;
		this.keepAliveTimer = setInterval(() => {
			if (this.keepAliveInFlight) return;
			this.keepAliveInFlight = this.request({ op: "ping" }, { timeoutMs })
				.then(() => {
					failures = 0;
				})
				.catch(() => {
					failures += 1;
					if (closeOnFail && failures >= maxFailures) {
						this.close();
					}
				})
				.finally(() => {
					this.keepAliveInFlight = undefined;
				});
		}, interval);
		return () => {
			if (this.keepAliveTimer) {
				clearInterval(this.keepAliveTimer);
				this.keepAliveTimer = undefined;
			}
			this.keepAliveInFlight = undefined;
		};
	}

	private onMessage(message: CanonicalControlResponse) {
		const entry = this.pending.get(message.id);
		if (!entry) return;
		this.pending.delete(message.id);
		if (entry.timeout) clearTimeout(entry.timeout);

		if (message.ok === false) {
			entry.reject(new Error(message.error ?? "Unknown error"));
			return;
		}

		entry.resolve(message);
	}

	private request(
		message:
			| {
					op: "peerId";
			  }
			| {
					op: "peerInfo";
			  }
			| {
					op: "dial";
					name: string;
			  }
			| {
					op: "hangUp";
					name: string;
			  }
			| {
					op: "start";
			  }
			| {
					op: "stop";
			  }
			| {
					op: "bootstrap";
					payload?: Uint8Array;
			  }
			| {
					op: "loadProgram";
					name: string;
					payload?: Uint8Array;
			  }
			| {
					op: "sign";
					payload: Uint8Array;
			  }
			| {
					op: "ping";
			  }
			| {
					op: "open";
					name: string;
					payload: Uint8Array;
			  },
		options?: { timeoutMs?: number },
	): Promise<CanonicalControlResponse> {
		const id = this.nextId++;
		const timeoutMs = options?.timeoutMs ?? this.requestTimeoutMs;
		return new Promise((resolve, reject) => {
			const entry: Pending<CanonicalControlResponse> = { resolve, reject };
			if (timeoutMs && timeoutMs > 0) {
				entry.timeout = setTimeout(() => {
					if (!this.pending.has(id)) return;
					this.pending.delete(id);
					reject(new Error(`Canonical request timeout (${message.op})`));
				}, timeoutMs);
			}
			this.pending.set(id, entry);
			try {
				this.connection.sendControl(
					new CanonicalControlRequest({
						id,
						op: message.op,
						...("name" in message ? { name: message.name } : {}),
						...("payload" in message ? { payload: message.payload } : {}),
					}),
				);
			} catch (e: any) {
				if (entry.timeout) clearTimeout(entry.timeout);
				this.pending.delete(id);
				reject(e);
			}
		});
	}
}

export const connectSharedWorker = async (
	worker: SharedWorker,
	options?: { requestTimeoutMs?: number },
): Promise<CanonicalClient> => {
	return CanonicalClient.create(worker.port, options);
};

export const connectSharedWorkerPeerbit = async (
	worker: SharedWorker,
	options?: { adapters?: CanonicalOpenAdapter[]; mode?: CanonicalOpenMode },
): Promise<PeerbitCanonicalClient> => {
	const canonical = await connectSharedWorker(worker);
	const { PeerbitCanonicalClient } = await import("./peerbit.js");
	return PeerbitCanonicalClient.create(canonical, options);
};
