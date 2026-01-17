import type { CanonicalOpenAdapter, CanonicalOpenMode } from "./auto.js";
import { CanonicalClient } from "./client.js";
import { PeerbitCanonicalClient } from "./peerbit.js";

export const CANONICAL_SERVICE_WORKER_CONNECT_OP =
	"__peerbit_canonical_connect__";
export const CANONICAL_SERVICE_WORKER_READY_KEY = "__peerbit_canonical_ready__";

export type ConnectServiceWorkerOptions = {
	serviceWorker?: ServiceWorker;
	registration?: ServiceWorkerRegistration;
	url?: string | URL;
	scope?: string;
	type?: RegistrationOptions["type"];
	timeoutMs?: number;
};

const waitForActivated = async (
	registration: ServiceWorkerRegistration,
	timeoutMs: number,
): Promise<ServiceWorker> => {
	const candidate =
		registration.active ?? registration.installing ?? registration.waiting;
	if (!candidate) {
		throw new Error("Service worker not installing");
	}
	if (candidate.state === "activated") {
		return candidate;
	}

	return new Promise<ServiceWorker>((resolve, reject) => {
		let timeout: ReturnType<typeof setTimeout> | undefined;
		const onState = () => {
			if (candidate.state !== "activated") return;
			if (timeout) clearTimeout(timeout);
			candidate.removeEventListener("statechange", onState);
			resolve(candidate);
		};
		timeout = setTimeout(() => {
			candidate.removeEventListener("statechange", onState);
			reject(new Error("Service worker activation timeout"));
		}, timeoutMs);
		candidate.addEventListener("statechange", onState);
	});
};

const waitForReady = async (
	port: MessagePort,
	timeoutMs: number,
): Promise<void> => {
	port.start();
	return new Promise<void>((resolve, reject) => {
		let timeout: ReturnType<typeof setTimeout> | undefined;
		const cleanup = () => {
			if (timeout) clearTimeout(timeout);
			port.removeEventListener("message", onMessage);
		};
		const onMessage = (event: MessageEvent) => {
			const data = event.data as any;
			if (data?.[CANONICAL_SERVICE_WORKER_READY_KEY] == null) return;
			cleanup();
			if (data[CANONICAL_SERVICE_WORKER_READY_KEY] === false) {
				reject(new Error(data.error ?? "Service worker canonical host failed"));
			} else {
				resolve();
			}
		};
		timeout = setTimeout(() => {
			cleanup();
			reject(new Error("Service worker canonical connect timeout"));
		}, timeoutMs);
		port.addEventListener("message", onMessage);
	});
};

export const connectServiceWorker = async (
	options: ConnectServiceWorkerOptions,
): Promise<CanonicalClient> => {
	if (!("serviceWorker" in navigator)) {
		throw new Error("Service workers not supported");
	}

	const timeoutMs = options.timeoutMs ?? 10_000;
	let target = options.serviceWorker;

	if (!target) {
		const registration =
			options.registration ??
			(options.url
				? await navigator.serviceWorker.register(options.url, {
						scope: options.scope,
						type: options.type ?? "module",
					})
				: undefined);
		if (!registration) {
			throw new Error(
				"connectServiceWorker requires url, registration, or serviceWorker",
			);
		}
		target = await waitForActivated(registration, timeoutMs);
	}

	const channel = new MessageChannel();
	const ready = waitForReady(channel.port1, timeoutMs);
	target.postMessage({ op: CANONICAL_SERVICE_WORKER_CONNECT_OP }, [
		channel.port2,
	]);
	await ready;
	return CanonicalClient.create(channel.port1);
};

export const connectServiceWorkerPeerbit = async (
	options: ConnectServiceWorkerOptions,
	peerOptions?: { adapters?: CanonicalOpenAdapter[]; mode?: CanonicalOpenMode },
): Promise<PeerbitCanonicalClient> => {
	const canonical = await connectServiceWorker(options);
	return PeerbitCanonicalClient.create(canonical, peerOptions);
};
