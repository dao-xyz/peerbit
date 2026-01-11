import { deserialize, serialize } from "@dao-xyz/borsh";
import {
	CanonicalBootstrapRequest,
	CanonicalChannelClose,
	CanonicalChannelMessage,
	CanonicalControlRequest,
	CanonicalControlResponse,
	CanonicalFrame,
	CanonicalLoadProgramRequest,
	CanonicalSignRequest,
} from "./protocol.js";

export type CanonicalTransport = {
	send: (data: Uint8Array, transfer?: Transferable[]) => void;
	onMessage: (handler: (data: Uint8Array) => void) => () => void;
	close?: () => void;
};

export type CanonicalChannel = {
	send: (data: Uint8Array) => void;
	onMessage: (handler: (data: Uint8Array) => void) => () => void;
	close?: () => void;
	onClose?: (handler: () => void) => () => void;
};

export type CanonicalRpcTransport = {
	send: (data: Uint8Array) => void;
	onMessage: (handler: (data: Uint8Array) => void) => () => void;
};

const coerceToTransferableUint8Array = (bytes: Uint8Array): Uint8Array => {
	if (bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength) {
		return bytes;
	}
	return bytes.slice();
};

const toUint8Array = (data: unknown): Uint8Array | undefined => {
	if (data instanceof Uint8Array) return data;
	if (data instanceof ArrayBuffer) return new Uint8Array(data);
	return undefined;
};

const isMessagePort = (
	value: MessagePort | CanonicalChannel,
): value is MessagePort => {
	return typeof (value as MessagePort).postMessage === "function";
};

export const createMessagePortTransport = (
	port: MessagePort,
): CanonicalTransport => {
	port.start();
	return {
		send: (data, transfer) => {
			const transferable = coerceToTransferableUint8Array(data);
			if (transfer && transfer.length > 0) {
				port.postMessage(transferable, transfer);
				return;
			}
			port.postMessage(transferable);
		},
		onMessage: (handler) => {
			const onMessage = (ev: MessageEvent) => {
				const data = toUint8Array(ev.data);
				if (data) handler(data);
			};
			port.addEventListener("message", onMessage);
			return () => port.removeEventListener("message", onMessage);
		},
		close: () => {
			try {
				port.close();
			} catch {}
		},
	};
};

export const createRpcTransport = (
	port: MessagePort | CanonicalChannel,
): CanonicalRpcTransport => {
	if (!isMessagePort(port)) {
		return {
			send: port.send,
			onMessage: port.onMessage,
		};
	}

	port.start();
	return {
		send: (data) => {
			const transferable = coerceToTransferableUint8Array(data);
			port.postMessage(transferable, [transferable.buffer]);
		},
		onMessage: (handler) => {
			const onMessage = (ev: MessageEvent) => {
				const data = toUint8Array(ev.data);
				if (data) handler(data);
			};
			port.addEventListener("message", onMessage);
			return () => port.removeEventListener("message", onMessage);
		},
	};
};

const WINDOW_MARKER = "__peerbit_canonical_transport__";

export const createWindowTransport = (
	targetWindow: Window,
	options?: { targetOrigin?: string; source?: Window; channel?: string },
): CanonicalTransport => {
	const targetOrigin = options?.targetOrigin ?? "*";
	const channel = options?.channel ?? "peerbit-canonical";
	return {
		send: (data, transfer) => {
			const payload = coerceToTransferableUint8Array(data);
			targetWindow.postMessage(
				{ [WINDOW_MARKER]: true, channel, data: payload },
				targetOrigin,
				transfer as any,
			);
		},
		onMessage: (handler) => {
			const onMessage = (event: MessageEvent) => {
				if (options?.source && event.source !== options.source) return;
				const msg = event.data as any;
				if (!msg || msg[WINDOW_MARKER] !== true) return;
				if (msg.channel !== channel) return;
				const data = toUint8Array(msg.data);
				if (data) handler(data);
			};
			globalThis.addEventListener("message", onMessage);
			return () => globalThis.removeEventListener("message", onMessage);
		},
	};
};

type ChannelState = {
	channel: CanonicalChannel;
	handlers: Set<(data: Uint8Array) => void>;
	closeHandlers: Set<() => void>;
	closed: boolean;
};

export class CanonicalConnection {
	private readonly channels = new Map<number, ChannelState>();
	private readonly controlHandlers = new Set<
		(frame: CanonicalControlRequest | CanonicalControlResponse) => void
	>();
	private readonly activityHandlers = new Set<() => void>();
	private readonly unsubscribe: () => void;

	constructor(readonly transport: CanonicalTransport) {
		this.unsubscribe = transport.onMessage((data) => {
			this.onMessage(data);
		});
	}

	onControl(
		handler: (
			frame: CanonicalControlRequest | CanonicalControlResponse,
		) => void,
	): () => void {
		this.controlHandlers.add(handler);
		return () => this.controlHandlers.delete(handler);
	}

	onActivity(handler: () => void): () => void {
		this.activityHandlers.add(handler);
		return () => this.activityHandlers.delete(handler);
	}

	sendControl(frame: CanonicalControlRequest | CanonicalControlResponse): void {
		this.sendFrame(frame);
	}

	createChannel(channelId: number): CanonicalChannel {
		const existing = this.channels.get(channelId);
		if (existing) return existing.channel;

		const handlers = new Set<(data: Uint8Array) => void>();
		const closeHandlers = new Set<() => void>();
		const state: ChannelState = {
			channel: {
				send: (payload) => {
					this.sendFrame(
						new CanonicalChannelMessage({
							channelId,
							payload,
						}),
					);
				},
				onMessage: (handler) => {
					handlers.add(handler);
					return () => handlers.delete(handler);
				},
				close: () => {
					this.closeChannel(channelId, true);
				},
				onClose: (handler) => {
					closeHandlers.add(handler);
					return () => closeHandlers.delete(handler);
				},
			},
			handlers,
			closeHandlers,
			closed: false,
		};

		this.channels.set(channelId, state);
		return state.channel;
	}

	releaseChannel(channelId: number): void {
		this.closeChannel(channelId, false);
	}

	close(): void {
		this.unsubscribe();
		const ids = [...this.channels.keys()];
		for (const id of ids) {
			this.closeChannel(id, true);
		}
		this.transport.close?.();
	}

	private closeChannel(channelId: number, emitClose: boolean): void {
		const state = this.channels.get(channelId);
		if (!state || state.closed) return;
		state.closed = true;
		this.channels.delete(channelId);
		if (emitClose) {
			try {
				this.sendFrame(new CanonicalChannelClose({ channelId }));
			} catch {}
		}
		for (const handler of state.closeHandlers) {
			handler();
		}
	}

	private sendFrame(frame: CanonicalFrame): void {
		const bytes = serialize(frame);
		const transferable = coerceToTransferableUint8Array(bytes);
		this.transport.send(transferable);
	}

	private onMessage(data: Uint8Array): void {
		for (const handler of this.activityHandlers) {
			handler();
		}
		const frame = deserialize(data, CanonicalFrame) as CanonicalFrame;
		if (frame instanceof CanonicalChannelMessage) {
			const state = this.channels.get(frame.channelId);
			if (!state || state.closed) return;
			for (const handler of state.handlers) {
				handler(frame.payload);
			}
			return;
		}

		if (frame instanceof CanonicalChannelClose) {
			this.closeChannel(frame.channelId, false);
			return;
		}

		if (
			frame instanceof CanonicalControlRequest ||
			frame instanceof CanonicalControlResponse
		) {
			for (const handler of this.controlHandlers) {
				handler(frame);
			}
		}
	}
}

export {
	CanonicalChannelClose,
	CanonicalChannelMessage,
	CanonicalControlRequest,
	CanonicalControlResponse,
	CanonicalFrame,
	CanonicalSignRequest,
	CanonicalBootstrapRequest,
	CanonicalLoadProgramRequest,
};
