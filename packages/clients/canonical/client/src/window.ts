import { createWindowTransport } from "@peerbit/canonical-transport";
import type { CanonicalOpenAdapter, CanonicalOpenMode } from "./auto.js";
import { CanonicalClient } from "./client.js";
import { PeerbitCanonicalClient } from "./peerbit.js";

export const CANONICAL_WINDOW_CONNECT_OP =
	"__peerbit_canonical_window_connect__";
export const CANONICAL_WINDOW_READY_KEY = "__peerbit_canonical_window_ready__";

export type ConnectWindowOptions = {
	target?: Window;
	channel?: string;
	targetOrigin?: string;
	timeoutMs?: number;
};

const randomId = (): string => {
	if (
		typeof crypto !== "undefined" &&
		typeof crypto.randomUUID === "function"
	) {
		return crypto.randomUUID();
	}
	return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
};

const waitForReady = async (options: {
	target: Window;
	channel: string;
	requestId: string;
	timeoutMs: number;
	expectedOrigin?: string;
}): Promise<void> => {
	return new Promise<void>((resolve, reject) => {
		let timeout: ReturnType<typeof setTimeout> | undefined;
		const cleanup = () => {
			if (timeout) clearTimeout(timeout);
			globalThis.removeEventListener("message", onMessage);
		};
		const onMessage = (event: MessageEvent) => {
			if (event.source !== options.target) return;
			if (options.expectedOrigin && event.origin !== options.expectedOrigin)
				return;
			const data = event.data as any;
			if (data?.[CANONICAL_WINDOW_READY_KEY] == null) return;
			if (data.channel && String(data.channel) !== options.channel) return;
			if (data.requestId && String(data.requestId) !== options.requestId)
				return;
			cleanup();
			if (data[CANONICAL_WINDOW_READY_KEY] === false) {
				reject(new Error(data.error ?? "Window canonical host failed"));
				return;
			}
			resolve();
		};
		timeout = setTimeout(() => {
			cleanup();
			reject(new Error("Window canonical connect timeout"));
		}, options.timeoutMs);
		globalThis.addEventListener("message", onMessage);
	});
};

export const connectWindow = async (
	options: ConnectWindowOptions = {},
): Promise<CanonicalClient> => {
	const parent = window.parent;
	const opener = window.opener;
	const inferredTarget = parent && parent !== window ? parent : opener;
	const target = options.target ?? inferredTarget;
	if (!target || target === window) {
		throw new Error(
			"connectWindow requires a parent/opener window or options.target",
		);
	}

	const channel = options.channel ?? "peerbit-canonical";
	const targetOrigin = options.targetOrigin ?? "*";
	const timeoutMs = options.timeoutMs ?? 10_000;
	const requestId = randomId();

	const expectedOrigin = targetOrigin !== "*" ? targetOrigin : undefined;
	const ready = waitForReady({
		target,
		channel,
		requestId,
		timeoutMs,
		expectedOrigin,
	});

	target.postMessage(
		{ op: CANONICAL_WINDOW_CONNECT_OP, channel, requestId },
		targetOrigin,
	);

	await ready;

	const transport = createWindowTransport(target, {
		channel,
		source: target,
		targetOrigin,
	});
	return CanonicalClient.create(transport);
};

export const connectWindowPeerbit = async (
	options: ConnectWindowOptions = {},
	peerOptions?: { adapters?: CanonicalOpenAdapter[]; mode?: CanonicalOpenMode },
): Promise<PeerbitCanonicalClient> => {
	const canonical = await connectWindow(options);
	return PeerbitCanonicalClient.create(canonical, peerOptions);
};
