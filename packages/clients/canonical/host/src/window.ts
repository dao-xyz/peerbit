import { createWindowTransport } from "@peerbit/canonical-transport";
import {
	CanonicalHost,
	type CanonicalHostOptions,
	type CanonicalModule,
	type CanonicalRuntimeOptions,
	PeerbitCanonicalRuntime,
} from "./index.js";

export const CANONICAL_WINDOW_CONNECT_OP =
	"__peerbit_canonical_window_connect__";
export const CANONICAL_WINDOW_READY_KEY = "__peerbit_canonical_window_ready__";

export type InstallWindowHostOptions = CanonicalRuntimeOptions & {
	modules?: CanonicalModule[];
	hostOptions?: CanonicalHostOptions;
	host?: CanonicalHost;
	createHost?: () => CanonicalHost | Promise<CanonicalHost>;
	channel?: string;
	targetOrigin?: string;
	allow?: (event: MessageEvent) => boolean;
	onError?: (error: unknown) => void;
};

export type WindowHost = {
	connect: (childWindow: Window, options?: { origin?: string }) => void;
	disconnect: (childWindow: Window) => void;
	close: () => void;
};

let hostPromise: Promise<CanonicalHost> | undefined;

const getHost = async (options?: InstallWindowHostOptions) => {
	if (hostPromise) return hostPromise;

	const provided = options?.host;
	const create = options?.createHost;
	const { modules, hostOptions, ...runtimeOptions } = options ?? {};

	const host = provided
		? provided
		: create
			? await create()
			: new CanonicalHost(
					new PeerbitCanonicalRuntime(runtimeOptions),
					hostOptions,
				);

	if (modules?.length) {
		host.registerModules(modules);
	}

	hostPromise = Promise.resolve(host);
	return hostPromise;
};

const coerceErrorMessage = (error: unknown): string => {
	if (error instanceof Error) return error.message;
	return String((error as any)?.message ?? error);
};

const coerceOrigin = (origin: unknown): string | undefined => {
	if (typeof origin !== "string") return undefined;
	if (origin.length === 0 || origin === "null") return undefined;
	return origin;
};

export const installWindowHost = (
	options?: InstallWindowHostOptions,
): WindowHost => {
	const channel = options?.channel ?? "peerbit-canonical";
	const defaultTargetOrigin = options?.targetOrigin ?? "*";
	const connections = new Map<Window, () => void>();

	const connect = (
		childWindow: Window,
		connectOptions?: { origin?: string },
	) => {
		if (connections.has(childWindow)) return;
		const targetOrigin = connectOptions?.origin ?? defaultTargetOrigin;
		const transport = createWindowTransport(childWindow, {
			channel,
			source: childWindow,
			targetOrigin,
		});
		getHost(options)
			.then((host) => {
				const detach = host.attachControlTransport(transport);
				connections.set(childWindow, detach);
			})
			.catch((error) => {
				options?.onError?.(error);
			});
	};

	const disconnect = (childWindow: Window) => {
		const detach = connections.get(childWindow);
		if (!detach) return;
		connections.delete(childWindow);
		detach();
	};

	const onMessage = (event: MessageEvent) => {
		if (options?.allow && !options.allow(event)) {
			return;
		}

		const data = event.data as any;
		const isConnect =
			data?.op === CANONICAL_WINDOW_CONNECT_OP ||
			data?.__peerbit_canonical_window_connect__ === true;
		if (!isConnect) return;
		if (data?.channel && String(data.channel) !== channel) return;

		const source = event.source as Window | null;
		if (!source || typeof (source as any).postMessage !== "function") {
			return;
		}

		const requestId =
			data?.requestId != null ? String(data.requestId) : undefined;
		const origin = coerceOrigin(event.origin) ?? defaultTargetOrigin;

		connect(source, { origin });

		getHost(options)
			.then(() => {
				try {
					source.postMessage(
						{
							[CANONICAL_WINDOW_READY_KEY]: true,
							channel,
							requestId,
						},
						origin === "*" ? "*" : origin,
					);
				} catch {}
			})
			.catch((error) => {
				options?.onError?.(error);
				const message = coerceErrorMessage(error);
				try {
					source.postMessage(
						{
							[CANONICAL_WINDOW_READY_KEY]: false,
							channel,
							requestId,
							error: message,
						},
						origin === "*" ? "*" : origin,
					);
				} catch {}
			});
	};

	globalThis.addEventListener("message", onMessage);

	const close = () => {
		globalThis.removeEventListener("message", onMessage);
		for (const detach of connections.values()) {
			detach();
		}
		connections.clear();
	};

	return { connect, disconnect, close };
};
